"""Temporary diagnostic: rolling ring buffer of serial Modbus exchanges.

TEMPORARY DIAGNOSTIC — REMOVE BEFORE MERGE.

This module exists solely to diagnose an intermittent serial comms failure
between a Fronius inverter and the downstream TS65A bridge. The inverter goes
quiet for ~2 minutes, then issues a read of the compatibility register 50000
(an apparent error-state probe), faults, disconnects, and recovers minutes
later. Register 50000 is the *symptom*; the cause is whatever happened in the
exchanges immediately before it.

Strategy: capture every serial exchange (both raw bytes and decoded PDUs, both
directions) into a fixed-size in-memory ring. When register 50000 is read, dump
the last N records to the log as a single CSV block so the lead-up timeline can
be reconstructed and analysed for latency / framing / timeout patterns.

Design constraints (see AGENTS.md):
- The capture path runs inline on the downstream server event loop's trace
  hooks, so it must be trivially cheap and non-blocking: two clock reads plus a
  ``deque.append`` (atomic under CPython, ``maxlen`` evicts the oldest for free).
  No locks, no formatting, no logging on the capture path.
- The dump (formatting + a single ``logger.info`` call) is expensive relative to
  the serving path, so it is performed OFF the server loop on a dedicated daemon
  worker thread, triggered via a bounded ``queue.Queue(maxsize=1)`` (newest wins).
  A momentary disturbance while the dump is prepared is acceptable and expected.

Timestamps: every record carries both ``time.monotonic()`` (primary — stable,
immune to NTP/DST steps, used for latency deltas) and ``time.time()`` wall-clock
(secondary — to correlate against external logs). Both are captured in-hook at
the moment of the event, never at dump time.
"""

from __future__ import annotations

import logging
import queue
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from pymodbus.pdu import ModbusPDU

# Default ring depth. 400 comfortably covers both directions (rx packet, rx pdu,
# tx pdu per request) for well over a hundred request/response cycles. Trivial
# memory on the target 8GB CM5.
DEFAULT_RING_SIZE: int = 400

# Minimum seconds between dumps, so a burst of trigger reads yields one dump.
DEFAULT_DUMP_COOLDOWN_S: float = 60.0

# Cap on raw-packet hex captured per record (bytes). Modbus RTU frames are
# small; this only guards against a pathological oversized buffer.
_MAX_PACKET_BYTES: int = 64

# Sentinel enqueued to ask the worker to stop.
_STOP = object()

_CSV_HEADER = "seq,mono_ts,wall_utc,dir,layer,fc,dev_id,addr,count,exc,payload_hex,delta_ms"


@dataclass(frozen=True, slots=True)
class TraceRecord:
    """A single captured exchange event.

    Kept as a small immutable record of primitives so the capture path only
    allocates one lightweight object and never touches shared mutable state.
    """

    seq: int
    mono_ts: float
    wall_ts: float
    direction: str  # "rx" | "tx"
    layer: str  # "packet" | "pdu"
    fc: Optional[int] = None
    dev_id: Optional[int] = None
    addr: Optional[int] = None
    count: Optional[int] = None
    exc: Optional[int] = None
    payload_hex: str = ""


class PduTraceRing:
    """Capture serial Modbus exchanges into a ring and dump them off-loop.

    Capture is lock-free and inline-cheap; the dump runs on a dedicated daemon
    worker so no formatting or logging happens on the server event loop.
    """

    def __init__(
        self,
        logger: logging.Logger,
        ring_size: int = DEFAULT_RING_SIZE,
        dump_cooldown_s: float = DEFAULT_DUMP_COOLDOWN_S,
        worker_name: str = "ts65a-trace-dump",
    ) -> None:
        self._logger = logger
        # deque with maxlen: append is atomic under the GIL and evicting the
        # oldest element on overflow is free. Readers snapshot via list(deque),
        # also atomic. No lock is needed between the single capture path and the
        # single dump worker.
        self._ring: deque[TraceRecord] = deque(maxlen=ring_size)
        self._seq: int = 0
        self._dump_cooldown_s = dump_cooldown_s
        self._last_dump_mono: float = 0.0

        # Bounded trigger queue: newest dump request wins on overflow so a storm
        # of trigger reads cannot back up the worker.
        self._trigger_q: queue.Queue[object] = queue.Queue(maxsize=1)
        self._worker = threading.Thread(target=self._dump_worker, name=worker_name, daemon=True)
        self._worker.start()

    # --- capture path (inline on the server loop trace hooks; must stay cheap) ---

    def record_packet(self, sending: bool, data: bytes) -> None:
        """Capture a raw byte-layer event (pre-decode for rx, post-encode for tx)."""
        mono = time.monotonic()
        wall = time.time()
        seq = self._seq
        self._seq = seq + 1
        payload = data[:_MAX_PACKET_BYTES].hex(" ") if data else ""
        self._ring.append(
            TraceRecord(
                seq=seq,
                mono_ts=mono,
                wall_ts=wall,
                direction="tx" if sending else "rx",
                layer="packet",
                payload_hex=payload,
            )
        )

    def record_pdu(self, sending: bool, pdu: ModbusPDU) -> None:
        """Capture a decoded-PDU event."""
        mono = time.monotonic()
        wall = time.time()
        seq = self._seq
        self._seq = seq + 1
        # Attributes vary by PDU type; read defensively without raising on the
        # capture path. Response register values (if any) go into payload_hex.
        registers = getattr(pdu, "registers", None)
        payload = ""
        if registers:
            payload = " ".join(f"{int(r) & 0xFFFF:04x}" for r in registers[:_MAX_PACKET_BYTES])
        self._ring.append(
            TraceRecord(
                seq=seq,
                mono_ts=mono,
                wall_ts=wall,
                direction="tx" if sending else "rx",
                layer="pdu",
                fc=getattr(pdu, "function_code", None),
                dev_id=getattr(pdu, "dev_id", None),
                addr=getattr(pdu, "address", None),
                count=getattr(pdu, "count", None),
                exc=getattr(pdu, "exception_code", None),
                payload_hex=payload,
            )
        )

    # --- trigger (inline; cheap: cooldown check + non-blocking queue put) ---

    def request_dump(self, reason: str) -> bool:
        """Ask the worker to dump the ring. Non-blocking; honours the cooldown.

        Returns True if a dump was scheduled, False if suppressed by cooldown or
        because a dump is already pending in the (maxsize=1) trigger queue.
        """
        now = time.monotonic()
        if now - self._last_dump_mono < self._dump_cooldown_s:
            return False
        self._last_dump_mono = now
        try:
            self._trigger_q.put_nowait(reason)
            return True
        except queue.Full:
            return False

    # --- dump worker (off-loop; formatting + single logging call live here) ---

    def _dump_worker(self) -> None:
        while True:
            reason = self._trigger_q.get()
            if reason is _STOP:
                return
            try:
                self._emit_dump(str(reason))
            except Exception:  # never let the diagnostic take down the worker
                self._logger.debug("PduTraceRing dump failed", exc_info=True)

    def _emit_dump(self, reason: str) -> None:
        # Snapshot atomically. list(deque) is a single C-level copy under the GIL,
        # so the capture path can keep appending concurrently without a lock.
        records = list(self._ring)
        lines = [
            f"=== TS65A serial PDU trace dump (trigger={reason}, {len(records)} records) ===",
            _CSV_HEADER,
        ]
        prev_mono: Optional[float] = None
        for r in records:
            delta_ms = "" if prev_mono is None else f"{(r.mono_ts - prev_mono) * 1000.0:.3f}"
            prev_mono = r.mono_ts
            lines.append(self._format_row(r, delta_ms))
        lines.append("=== end dump ===")
        # Single logging call => one atomic emit through the handler; the CSV
        # block never interleaves with other log traffic and is copy-paste ready.
        self._logger.info("\n".join(lines))

    @staticmethod
    def _format_row(r: TraceRecord, delta_ms: str) -> str:
        wall_iso = datetime.fromtimestamp(r.wall_ts, tz=timezone.utc).isoformat(timespec="milliseconds")
        return ",".join(
            (
                str(r.seq),
                f"{r.mono_ts:.6f}",
                wall_iso,
                r.direction,
                r.layer,
                "" if r.fc is None else str(r.fc),
                "" if r.dev_id is None else str(r.dev_id),
                "" if r.addr is None else str(r.addr),
                "" if r.count is None else str(r.count),
                "" if r.exc is None else str(r.exc),
                r.payload_hex,
                delta_ms,
            )
        )

    def stop(self) -> None:
        """Stop the dump worker (best-effort; for clean shutdown/tests)."""
        try:
            self._trigger_q.put_nowait(_STOP)
        except queue.Full:
            # A pending dump request occupies the slot; drain and replace it.
            try:
                self._trigger_q.get_nowait()
            except queue.Empty:
                pass
            try:
                self._trigger_q.put_nowait(_STOP)
            except queue.Full:
                pass
