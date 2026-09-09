#!/usr/bin/env python3
"""Passive RS485 / Modbus-RTU bus sniffer.

TEMPORARY DIAGNOSTIC SERVICE — not part of the main application.

Listens on a spare RS485 adapter wired (read-only) onto the existing bus between
the Fronius inverter (Modbus master) and this host's TS65A bridge (slave). It
records EVERYTHING seen on the wire so a comms dropout can be reconstructed from
the physical layer, independently of what the bridge's own pymodbus stack does
with the bytes.

Because RS485 is a shared multi-drop bus, a passive listener sees BOTH directions
(master requests and slave responses) as a single interleaved byte stream. There
is no hardware tx/rx distinction for a tap, so "direction" is *inferred* from the
Modbus frame structure (a read request has a 2-byte count that matches a plausible
range; a response carries a byte-count header). The raw log never depends on that
inference — every byte is captured whether or not it forms a valid frame.

Two outputs (paths configurable via env):

  * RAW CSV  — one row per received chunk/frame boundary: local timestamp,
    monotonic delta since previous row, byte length, and full hex. Nothing is
    dropped: bad-CRC, partial, or garbage bytes are all logged here.
  * PARSED CSV — one row per decoded Modbus frame: local timestamp, delta,
    inferred direction, device id, function code, address, count/byte-count,
    CRC ok/bad, and a short decoded summary. Frames that fail to parse are still
    emitted with kind=unparsed so gaps/corruption are visible.

Framing: Modbus RTU delimits frames by an idle gap of >= 3.5 character times. We
reconstruct that from read timing — bytes arriving within the inter-char gap are
one frame; a longer idle flushes the current frame. The gap is derived from the
baud rate, with a small floor so USB-serial latency jitter does not split frames.

Config via environment variables (see rs485_sniffer/README.md):
  SNIFFER_PORT       serial device (e.g. /dev/ttyUSB3)   [required]
  SNIFFER_BAUDRATE   default 9600
  SNIFFER_PARITY     N | E | O   default N
  SNIFFER_BYTESIZE   default 8
  SNIFFER_STOPBITS   default 1
  SNIFFER_RAW_CSV    default /data/rs485_raw.csv
  SNIFFER_PARSED_CSV default /data/rs485_parsed.csv
  SNIFFER_TZ         Olson tz name for local timestamps (else system local time)
"""

from __future__ import annotations

import csv
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import serial

# Largest legal Modbus RTU ADU: 1 addr + 253 PDU + 2 CRC.
_MAX_RTU_ADU: int = 256

# Hard cap on the frame-assembly buffer. A continuous/noisy stream may never
# reach an idle gap; without this bound the buffer would grow until the process
# OOMs. When exceeded we force-flush whatever we have so it still reaches the
# raw CSV. Generously above one max ADU to give the CRC splitter room to work.
_MAX_BUFFER_BYTES: int = 4096

# --- CRC16 (Modbus) -------------------------------------------------------

_CRC16_TABLE: list[int] = []


def _build_crc16_table() -> None:
    for byte in range(256):
        crc = 0x0000
        b = byte
        for _ in range(8):
            if (b ^ crc) & 0x0001:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
            b >>= 1
        _CRC16_TABLE.append(crc)


_build_crc16_table()


def modbus_crc(data: bytes) -> int:
    """Compute the Modbus RTU CRC16 (returned as the on-wire little-endian int)."""
    crc = 0xFFFF
    for ch in data:
        crc = ((crc >> 8) & 0xFF) ^ _CRC16_TABLE[(crc ^ ch) & 0xFF]
    return crc


def crc_ok(frame: bytes) -> bool:
    """True if a frame's trailing 2 bytes are a valid Modbus CRC of the body."""
    if len(frame) < 4:
        return False
    body, crc = frame[:-2], frame[-2:]
    calc = modbus_crc(body)
    # On the wire the CRC is sent low-byte first.
    return crc[0] == (calc & 0xFF) and crc[1] == ((calc >> 8) & 0xFF)


# --- Config ---------------------------------------------------------------


@dataclass
class Config:
    port: str
    baudrate: int
    parity: str
    bytesize: int
    stopbits: int
    raw_csv: str
    parsed_csv: str
    tz: ZoneInfo | None

    @classmethod
    def from_env(cls) -> "Config":
        port = os.environ.get("SNIFFER_PORT")
        if not port:
            raise SystemExit("SNIFFER_PORT is required (e.g. /dev/ttyUSB3)")
        tz_name = os.environ.get("SNIFFER_TZ") or os.environ.get("TZ")
        tz = None
        if tz_name:
            try:
                tz = ZoneInfo(tz_name)
            except Exception:
                print(f"WARNING: unknown timezone {tz_name!r}; using system local time", file=sys.stderr)
        return cls(
            port=port,
            baudrate=int(os.environ.get("SNIFFER_BAUDRATE", "9600")),
            parity=os.environ.get("SNIFFER_PARITY", "N"),
            bytesize=int(os.environ.get("SNIFFER_BYTESIZE", "8")),
            stopbits=int(os.environ.get("SNIFFER_STOPBITS", "1")),
            raw_csv=os.environ.get("SNIFFER_RAW_CSV", "/data/rs485_raw.csv"),
            parsed_csv=os.environ.get("SNIFFER_PARSED_CSV", "/data/rs485_parsed.csv"),
            tz=tz,
        )

    def char_time_s(self) -> float:
        """Duration of one serial character in seconds (start+data+parity+stop)."""
        bits = 1 + self.bytesize + (0 if self.parity == "N" else 1) + self.stopbits
        return bits / float(self.baudrate)

    def interframe_gap_s(self) -> float:
        """RTU inter-frame idle threshold (>= 3.5 char times), with a jitter floor.

        USB-serial adapters batch bytes, so a strict 3.5-char gap (~4ms at 9600)
        would split frames. Use max(3.5 char times, 2ms) as the flush threshold.
        """
        return max(3.5 * self.char_time_s(), 0.002)


# --- Modbus frame decode (best-effort, for the parsed CSV) ----------------

_READ_FCS = {1, 2, 3, 4}
# Register reads (16-bit words). For these an 8-byte frame is unambiguously a
# request; for bit reads (FC1/FC2) an 8-byte frame could be a 3-data-byte
# response, so direction is left ambiguous there.
_REGISTER_READ_FCS = {3, 4}
_WRITE_SINGLE_FCS = {5, 6}
_WRITE_MULTI_FCS = {15, 16}


def expected_frame_len(buf: bytes) -> int | None:
    """Infer the total on-wire length (incl. CRC) of the frame starting at buf.

    Used only to decide whether a partially-received frame should keep waiting
    for more bytes rather than being split by an idle gap (drivers deliver large
    responses in several packets). Returns None when the shape is not recognised
    (so the caller treats an idle gap as a real boundary).

    Disambiguation with as few bytes as possible:
      * exception (fc & 0x80): 5 bytes (id, fc, code, CRC*2)
      * read response (FC1-4): 3 + bytecount + 2  — needs byte[2]
      * read register request (FC3/FC4): 8 bytes
      * write single (FC5/FC6): 8 bytes
      * write multiple request (FC15/FC16): 7 + bytecount + 2 — needs byte[6]
      * write multiple response (FC15/FC16): 8 bytes

    Where a shape is ambiguous from the header alone (a read FC can be either an
    8-byte request or a 3+N response), we return the LARGER plausible length so a
    genuine multi-packet response is not prematurely flushed; a shorter complete
    frame is still recovered by the CRC splitter.
    """
    if len(buf) < 2:
        return None
    fc = buf[1]

    if fc & 0x80:
        return 5  # exception response

    if fc in _READ_FCS:
        # Could be an 8-byte request or a (3 + bytecount + 2) response. If byte[2]
        # is present and looks like a response byte-count that yields a length of
        # at least 8, prefer the response length (the large-response case we must
        # not split). Otherwise assume the 8-byte request.
        if len(buf) >= 3:
            resp_len = 3 + buf[2] + 2
            if resp_len >= 8:
                return resp_len
        return 8

    if fc in _WRITE_SINGLE_FCS:
        return 8

    if fc in _WRITE_MULTI_FCS:
        # Request: 7 + bytecount + 2 (needs byte[6]); response: 8. Prefer the
        # larger request length when byte[6] is available.
        if len(buf) >= 7:
            req_len = 7 + buf[6] + 2
            if req_len >= 8:
                return req_len
        return 8

    return None


def decode_frame(frame: bytes) -> dict:
    """Best-effort decode of a single RTU frame into named fields.

    Returns a dict with whatever could be determined. Never raises. Direction is
    inferred structurally: a request to a read FC is 8 bytes (id, fc, addr_hi,
    addr_lo, cnt_hi, cnt_lo, crc, crc); a read response is (id, fc, bytecount,
    ...data..., crc, crc) with bytecount == len-5.
    """
    out: dict = {
        "dev_id": "",
        "fc": "",
        "addr": "",
        "count": "",
        "bytecount": "",
        "exception": "",
        "direction": "",
        "crc_ok": crc_ok(frame),
        "summary": "",
    }
    if len(frame) < 2:
        out["summary"] = "runt"
        return out

    dev_id = frame[0]
    fc = frame[1]
    out["dev_id"] = dev_id
    out["fc"] = fc

    # Exception response: fc has high bit set.
    if fc & 0x80:
        out["direction"] = "response"
        out["exception"] = frame[2] if len(frame) >= 3 else ""
        out["fc"] = fc & 0x7F
        out["summary"] = f"exception fc={fc & 0x7F} code={out['exception']}"
        return out

    if fc in _READ_FCS:
        # Response form for all read FCs: id, fc, bytecount, data..., CRC — so
        # bytecount == len - 5. Check this first (it is unambiguous).
        if len(frame) >= 5 and frame[2] == len(frame) - 5:
            out["direction"] = "response"
            out["bytecount"] = frame[2]
            out["summary"] = f"read-response fc={fc} bytes={frame[2]}"
        elif len(frame) == 8 and fc in _REGISTER_READ_FCS:
            # An 8-byte request (id, fc, addr_hi, addr_lo, cnt_hi, cnt_lo, CRC).
            # Only unambiguous for register reads (FC3/FC4): an FC1/FC2 (bit)
            # response carrying 3 data bytes is ALSO 8 bytes, so we do not assert
            # a direction for those.
            addr = (frame[2] << 8) | frame[3]
            count = (frame[4] << 8) | frame[5]
            out["direction"] = "request"
            out["addr"] = addr
            out["count"] = count
            out["summary"] = f"read fc={fc} addr={addr} count={count}"
        elif len(frame) == 8:
            # FC1/FC2, 8 bytes: could be a request or a 3-data-byte response.
            out["summary"] = f"read fc={fc} (ambiguous 8-byte FC1/FC2 frame)"
        else:
            out["summary"] = f"read fc={fc} (ambiguous len={len(frame)})"
        return out

    if fc in _WRITE_SINGLE_FCS and len(frame) == 8:
        addr = (frame[2] << 8) | frame[3]
        val = (frame[4] << 8) | frame[5]
        out["addr"] = addr
        out["count"] = 1
        # Request and echo response are identical for single writes.
        out["direction"] = "request/response"
        out["summary"] = f"write-single fc={fc} addr={addr} value={val}"
        return out

    if fc in _WRITE_MULTI_FCS and len(frame) >= 7:
        addr = (frame[2] << 8) | frame[3]
        count = (frame[4] << 8) | frame[5]
        out["addr"] = addr
        out["count"] = count
        if len(frame) == 8:
            out["direction"] = "response"
            out["summary"] = f"write-multi-response fc={fc} addr={addr} count={count}"
        else:
            out["direction"] = "request"
            out["summary"] = f"write-multi fc={fc} addr={addr} count={count}"
        return out

    out["summary"] = f"fc={fc} len={len(frame)}"
    return out


# --- Sniffer --------------------------------------------------------------


class Sniffer:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._running = True
        self._raw_file = None
        self._parsed_file = None
        self._raw_w = None
        self._parsed_w = None
        self._seq = 0
        self._prev_mono: float | None = None
        # Frame-assembly state (see _feed).
        self._buf = bytearray()
        self._last_byte_mono: float | None = None
        self._gap = cfg.interframe_gap_s()

    def _open_outputs(self) -> None:
        for path in (self.cfg.raw_csv, self.cfg.parsed_csv):
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._raw_file = open(self.cfg.raw_csv, "a", newline="", buffering=1)
        self._parsed_file = open(self.cfg.parsed_csv, "a", newline="", buffering=1)
        self._raw_w = csv.writer(self._raw_file)
        self._parsed_w = csv.writer(self._parsed_file)
        if self._raw_file.tell() == 0:
            self._raw_w.writerow(["seq", "wall_local", "mono", "delta_ms", "n_bytes", "hex"])
        if self._parsed_file.tell() == 0:
            self._parsed_w.writerow(
                [
                    "seq",
                    "wall_local",
                    "mono",
                    "delta_ms",
                    "direction",
                    "dev_id",
                    "fc",
                    "addr",
                    "count",
                    "bytecount",
                    "exception",
                    "crc_ok",
                    "n_bytes",
                    "summary",
                    "hex",
                ]
            )

    def _now_local_iso(self) -> str:
        tz = self.cfg.tz or datetime.now(timezone.utc).astimezone().tzinfo
        return datetime.now(tz).isoformat(timespec="milliseconds")

    def _emit(self, frame: bytes) -> None:
        mono = time.monotonic()
        wall = self._now_local_iso()
        delta_ms = "" if self._prev_mono is None else f"{(mono - self._prev_mono) * 1000.0:.3f}"
        self._prev_mono = mono
        seq = self._seq
        self._seq += 1
        hexs = frame.hex(" ")

        self._raw_w.writerow([seq, wall, f"{mono:.6f}", delta_ms, len(frame), hexs])

        d = decode_frame(frame)
        # "unparsed" whenever the frame is unreliable as decoded: a failed CRC
        # (corruption) OR a valid-CRC frame we could not structurally classify.
        # This keeps `direction=unparsed` a dependable filter for suspect traffic.
        if not d["crc_ok"] or not d["direction"]:
            direction = "unparsed"
        else:
            direction = d["direction"]
        self._parsed_w.writerow(
            [
                seq,
                wall,
                f"{mono:.6f}",
                delta_ms,
                direction,
                d["dev_id"],
                d["fc"],
                d["addr"],
                d["count"],
                d["bytecount"],
                d["exception"],
                "1" if d["crc_ok"] else "0",
                len(frame),
                d["summary"],
                hexs,
            ]
        )

    def run(self) -> None:
        cfg = self.cfg
        self._open_outputs()
        gap = self._gap
        # A short read timeout (well under the idle gap) lets us detect the
        # inter-frame idle even when the bus falls silent mid-capture.
        ser = serial.Serial(
            port=cfg.port,
            baudrate=cfg.baudrate,
            parity=cfg.parity,
            bytesize=cfg.bytesize,
            stopbits=cfg.stopbits,
            timeout=gap / 4.0,
        )
        print(
            f"RS485 sniffer listening on {cfg.port} @ {cfg.baudrate} {cfg.bytesize}{cfg.parity}{cfg.stopbits} "
            f"(interframe gap {gap * 1000:.2f} ms)\n"
            f"  raw    -> {cfg.raw_csv}\n  parsed -> {cfg.parsed_csv}",
            file=sys.stderr,
        )

        try:
            while self._running:
                # Read whatever is currently available (at least one byte, blocking
                # up to the short timeout). Reading the available batch lets the
                # CRC-based splitter in _feed separate driver-coalesced frames;
                # the idle-gap path still delimits cleanly-spaced traffic. `now`
                # is the observation time for this batch — the best available
                # timestamp without hardware RX timestamping (see _feed docstring
                # for the residual batching limitation).
                waiting = getattr(ser, "in_waiting", 0) or 1
                b = ser.read(waiting)
                now = time.monotonic()
                for frame in self._feed(b, now):
                    self._emit(frame)
        finally:
            for frame in self._feed(b"", time.monotonic(), force_flush=True):
                self._emit(frame)
            try:
                ser.close()
            except Exception:
                pass
            for f in (self._raw_file, self._parsed_file):
                if f:
                    f.close()

    def _feed(self, data: bytes, now: float, force_flush: bool = False):
        """Frame-assembly state machine. Yields completed frames.

        ``data`` is the bytes observed at monotonic time ``now`` (empty on a read
        timeout). Frames are delimited by three mechanisms, in order:

        1. **Idle gap** — the primary RTU delimiter: once the bus has been idle
           for >= the inter-frame threshold since the last byte, the buffer is a
           complete frame. Using a ``>= gap`` boundary avoids merging a response
           that follows the minimum 3.5-char silence into the preceding request.

        2. **CRC-based split (fallback)** — USB/UART drivers often batch bytes, so
           a request and its response can be delivered back-to-back with no
           observable idle gap between them; the idle rule alone would then
           coalesce two valid frames and report a false bad CRC. To defend the
           diagnostic against manufacturing that exact signal, whenever the buffer
           *starts* with a structurally-complete, CRC-valid frame we split it off
           immediately rather than waiting for an idle gap. This is best-effort:
           it recovers cleanly-framed back-to-back traffic but cannot recover
           genuinely corrupted bytes (which is the signal we want to keep).

        3. **Size cap (safety)** — a continuous or noisy stream may never idle;
           without a bound the buffer would grow until the process OOMs, which is
           especially likely while investigating corruption. Once the buffer
           exceeds a cap we force-flush what we have so it always reaches the CSV.

        Extracted from the read loop so the framing logic is unit-testable
        without a real serial port.

        Batching limitation: if the driver batches bytes AND the traffic is not
        cleanly CRC-framed (real corruption), the idle gap is the only delimiter
        available and adjacent bursts may still be merged. This residual limit is
        documented in the README; a logic analyzer is the authoritative fallback.
        """
        # (1) Idle-gap flush — but only if the buffer is not a still-incomplete
        # frame. USB/UART drivers deliver a large response (e.g. the ~121-byte
        # 40071 block) in smaller packets spaced further apart than the RTU
        # inter-frame gap; a naive idle flush would split one response into many
        # fragments and mis-decode each. If the buffer starts with a recognisable
        # Modbus header whose declared length is not yet satisfied, we keep
        # waiting for the remainder instead of flushing (unless forced on
        # shutdown). ``force_flush`` always flushes.
        if self._buf and self._last_byte_mono is not None:
            idle = now - self._last_byte_mono
            if force_flush or idle >= self._gap:
                if force_flush or self._buf_is_flushable():
                    yield bytes(self._buf)
                    self._buf.clear()

        if data:
            self._buf.extend(data)
            self._last_byte_mono = now

            # (2) CRC-based split: peel off any leading CRC-valid frames so
            # driver-batched back-to-back frames are separated correctly.
            yield from self._split_leading_crc_frames()

            # (3) Size cap: never let the buffer grow without bound.
            if len(self._buf) >= _MAX_BUFFER_BYTES:
                yield bytes(self._buf)
                self._buf.clear()

    def _buf_is_flushable(self) -> bool:
        """True if the buffer should be idle-flushed rather than kept growing.

        Returns False only when the buffer looks like the *start* of a Modbus
        frame whose declared length has not yet been fully received — i.e. more
        bytes for this frame are still expected (arriving in later driver
        packets). In every other case (unknown/garbage shape, or a complete/
        overrun frame) the idle gap is a valid delimiter and we flush.
        """
        expected = expected_frame_len(bytes(self._buf))
        if expected is None:
            return True  # unrecognised shape — treat idle as a real boundary
        return len(self._buf) >= expected  # complete (or more) — safe to flush

    def _split_leading_crc_frames(self):
        """Yield and remove any CRC-valid frame(s) at the start of the buffer.

        Scans increasing prefix lengths (from the RTU minimum of 4 bytes) for a
        valid Modbus CRC. When found, that prefix is emitted as a complete frame
        and removed, then scanning continues on the remainder. Leaves a trailing
        partial/unrecognised remainder in the buffer for the idle-gap path.
        """
        made_progress = True
        while made_progress and len(self._buf) >= 4:
            made_progress = False

            # Prefer the structurally-expected length: if the header declares a
            # length we have fully received and its CRC is valid, emit exactly
            # that frame. This avoids splitting a large response at a coincidental
            # shorter CRC match.
            expected = expected_frame_len(bytes(self._buf))
            if expected is not None and len(self._buf) >= expected and crc_ok(bytes(self._buf[:expected])):
                frame = bytes(self._buf[:expected])
                del self._buf[:expected]
                yield frame
                made_progress = True
                continue

            # Fallback: scan increasing prefixes for any valid CRC (handles
            # shapes we cannot length-predict, and cleanly-batched back-to-back
            # frames). Cap the scan to the max legal ADU to bound cost.
            limit = min(len(self._buf), _MAX_RTU_ADU)
            for end in range(4, limit + 1):
                if crc_ok(bytes(self._buf[:end])):
                    frame = bytes(self._buf[:end])
                    del self._buf[:end]
                    yield frame
                    made_progress = True
                    break

    def stop(self, *_args) -> None:
        self._running = False


def main() -> None:
    cfg = Config.from_env()
    sniffer = Sniffer(cfg)
    signal.signal(signal.SIGTERM, sniffer.stop)
    signal.signal(signal.SIGINT, sniffer.stop)
    sniffer.run()


if __name__ == "__main__":
    main()
