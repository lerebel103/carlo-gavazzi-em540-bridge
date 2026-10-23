"""Serial-only frame-latching for downstream Modbus slave bridges.

Downstream Modbus-over-SERIAL clients can poll the bridge faster than the
upstream EM540/EM530 master refreshes, so they receive DUPLICATE frames. This
module latches a serial client's effective read rate to the upstream refresh
rate: when a serial read arrives and no NEW frame has been published into the
downstream registers since that serial line was last served, the read BLOCKS
until the next fresh frame lands, then releases and responds IMMEDIATELY.

The mechanism is structural serial-only scoping: only the serial server's
``context`` is swapped for :class:`SerialPacingContext`; the TCP and
RTU-over-TCP servers keep the plain shared ``SimCore`` and are byte-for-byte
unaffected.

Design basis (verified against pymodbus 3.15.0):
  - ``ReadHoldingRegistersRequest.datastore_update`` awaits
    ``context.async_getValues(...)`` and maps an ``ExcCodes`` return straight to
    an ``ExceptionResponse`` (identical on the FC4 read-input path). So awaiting
    inside the proxy defers the serial response, and returning ``ExcCodes`` is a
    clean fail-closed path.
  - The only ``context`` methods on the server request path are
    ``device_ids()``, ``async_getValues(...)`` and ``async_setValues(...)``; the
    proxy forwards exactly those.

The latch sits BEFORE the datastore read and DEFERS to the tier-1 staleness
breaker (``PduHelper.circuit_open``): a doomed request never blocks, it fails
closed with ``DEVICE_BUSY`` immediately. The whole staleness ladder is
preserved.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from pymodbus.constants import ExcCodes

from app.utils.pdu_helper import PduHelper


@dataclass(frozen=True)
class SerialWireBudget:
    """Per-request serialization-budget parameters for a serial line.

    Finding 2 — the frame latch must not spend so long waiting for a fresh frame
    that the subsequent on-wire response serialization pushes the serial client
    past its read deadline. ``available_wait_s(count)`` returns how long a read of
    ``count`` registers may latch and still fit its response inside
    ``client_read_timeout_ms`` with a turnaround margin, computed from the serial
    line settings (8N1 = 10 bits/byte) and the RTU response framing.

    Budgeting is done PER REQUEST (using the request's own ``count``) so a small
    read keeps the full configured latch wait while only large reads — whose
    response takes longer to clock out — are clamped. This directly bounds the
    worst case the finding identified without forbidding otherwise-valid configs.
    """

    baudrate: int
    bytesize: int
    parity: str
    stopbits: float
    client_read_timeout_ms: float
    turnaround_margin_s: float = 0.02

    def _byte_time_s(self) -> float:
        parity_bits = 0 if self.parity == "N" else 1
        bits_per_byte = 1 + self.bytesize + parity_bits + self.stopbits
        return bits_per_byte / self.baudrate

    def serialization_s(self, count: int) -> float:
        """Worst-case serialization time (s) for a ``count``-register FC3/FC4 response.

        RTU response framing: 1 addr + 1 func + 1 bytecount + count*2 data + 2 CRC.
        """
        resp_bytes = count * 2 + 5
        return resp_bytes * self._byte_time_s()

    def available_wait_s(self, count: int) -> float:
        """Latch-wait budget (s) for a ``count``-register read under the deadline.

        ``client_read_timeout_ms/1000 - serialization_s(count) - turnaround_margin``.
        May be <= 0 when the response alone (plus margin) already exceeds the
        deadline; the caller clamps the effective wait to max(0, ...).
        """
        return self.client_read_timeout_ms / 1000.0 - self.serialization_s(count) - self.turnaround_margin_s


class FrameGate:
    """Cross-thread wake primitive carrying an atomic generation + snapshot.

    The upstream listener thread builds an immutable register snapshot from the
    bytes it just committed, bumps a per-bridge monotonic generation counter, and
    schedules :meth:`publish` onto the dedicated server loop via
    ``call_soon_threadsafe``, passing BOTH the generation and that snapshot. The
    serial read coroutine (running on that same server loop) awaits
    :meth:`wait_for_generation_after` for a newer generation, then samples the
    stored ``(generation, snapshot)`` pair as ONE unit via :meth:`current_frame`.

    Finding 1 (torn/duplicate frame) fix — the generation and the register bytes
    are published together as a single immutable pair and sampled together, so
    the generation a serial line records as served is ALWAYS the generation of the
    exact bytes it returns. There is no second read of live register state between
    recording the generation and producing the bytes, which is what previously let
    a read record generation N while returning N+1 content (and then serve N+1
    again as a duplicate).

    The ``asyncio.Condition`` is created LAZILY on first use so it binds to the
    running server loop (an asyncio primitive binds to the loop at first await,
    not construction; creating/awaiting on the wrong loop raises
    "got Future attached to a different loop"). Both :meth:`publish` and
    :meth:`wait_for_generation_after` are only ever invoked on the server loop.
    """

    def __init__(self) -> None:
        # current_generation and _snapshot are updated together under the single
        # synchronous assignment in publish() (both run on the server loop), so a
        # reader on that same loop always observes a consistent pair.
        self.current_generation: int = 0
        self._snapshot: tuple[int, ...] | None = None
        self._condition: asyncio.Condition | None = None

    def _get_condition(self) -> asyncio.Condition:
        if self._condition is None:
            # Created on the running (server) loop on first use.
            self._condition = asyncio.Condition()
        return self._condition

    def publish(self, generation: int, snapshot: tuple[int, ...]) -> None:
        """Store the latest generation + its register snapshot and wake any awaiter.

        Runs on the server loop (scheduled via ``call_soon_threadsafe``). The
        ``snapshot`` is the IMMUTABLE register image produced on the listener
        thread from the exact bytes that were committed for ``generation`` — the
        generation and the bytes are one unit. Because a serial server is
        single-request/half-duplex there is at most one awaiter, so ``notify_all``
        vs ``notify(1)`` is immaterial.
        """
        if generation > self.current_generation:
            self.current_generation = generation
            self._snapshot = snapshot
        condition = self._get_condition()

        async def _notify() -> None:
            async with condition:
                condition.notify_all()

        # publish() is a sync callback invoked on the server loop; schedule the
        # async notify on that same loop.
        asyncio.ensure_future(_notify())

    def current_frame(self) -> tuple[int, tuple[int, ...] | None]:
        """Sample the stored ``(generation, snapshot)`` pair as a single read.

        Returns the current generation and the snapshot published with it. Called
        on the server loop; because ``publish`` assigns both fields together on
        that same loop, the returned pair is always consistent.
        """
        return self.current_generation, self._snapshot

    async def wait_for_generation_after(self, generation: int) -> None:
        """Await until ``current_generation`` is strictly greater than ``generation``.

        Returns immediately if a newer generation already exists at entry (client
        slower than upstream).
        """
        if self.current_generation > generation:
            return
        condition = self._get_condition()
        async with condition:
            await condition.wait_for(lambda: self.current_generation > generation)


class SerialPacingContext:
    """Serial-only async context proxy wrapping the real ``SimCore``.

    Forwards ``device_ids()`` and ``async_setValues(...)`` as straight
    pass-throughs (writes are NEVER gated) and implements the read latch in
    ``async_getValues(...)``.

    A single per-instance ``_last_served_gen`` cursor models "has this serial
    line been served the current frame yet?" — correct because a serial server
    is single-handler/half-duplex (one awaiter at a time), so no per-connection
    state is needed.

    If ``max_wait_s <= 0`` latching is disabled and ``async_getValues`` is a pure
    pass-through (today's behaviour, duplicate frames possible).

    Finding 1 — atomic generation-stamped serial snapshot
    ------------------------------------------------------
    On the GATED read path the proxy no longer re-reads the live ``SimCore``
    register array (which the listener thread can overwrite mid-read). Instead it
    samples the gate's ``(generation, snapshot)`` pair as ONE unit (the snapshot
    is the immutable register image the listener thread committed for that exact
    generation) and slices the requested window out of THAT snapshot. Invariants
    held by construction:
      1. recorded generation == generation of the bytes returned (both come from
         the same sampled pair — there is no second read in between);
      2. no torn/duplicate frame across the listener-write / server-publish hop
         (the bytes are committed into the immutable snapshot BEFORE the
         generation is announced, and a serial line only advances its cursor when
         it consumes a strictly-newer generation);
      3. the listener thread does only a non-blocking snapshot copy + int bump +
         ``call_soon_threadsafe`` — no locks/awaits are added on that path.
    TCP/RTU-over-TCP are untouched: they keep the plain shared ``SimCore``.
    """

    def __init__(
        self,
        core: object,
        gate: FrameGate,
        pdu_helper: PduHelper,
        max_wait_s: float,
        reg_start_address: int = 0,
        wire_budget: SerialWireBudget | None = None,
    ) -> None:
        self._core = core
        self._gate = gate
        self._pdu_helper = pdu_helper
        self._max_wait_s = max_wait_s
        # Base Modbus protocol address of the served register block, used to map a
        # requested address onto an index into the published snapshot tuple.
        self._reg_start_address = reg_start_address
        # Per-request serialization budget (Finding 2). When set, the effective
        # latch wait for a read is min(max_wait_s, budget_for_this_count) so the
        # response still fits the serial client's read deadline. When None (e.g.
        # in unit tests) the flat max_wait_s is used unchanged.
        self._wire_budget = wire_budget
        self._last_served_gen: int = 0

    def _effective_wait_s(self, count: int) -> float:
        """Effective latch wait for a ``count``-register read.

        ``min(max_wait_s, per-request wire budget)``, never negative. With no wire
        budget configured this is just ``max_wait_s``.
        """
        if self._wire_budget is None:
            return self._max_wait_s
        budget = self._wire_budget.available_wait_s(count)
        return max(0.0, min(self._max_wait_s, budget))

    def device_ids(self) -> list[int]:
        return self._core.device_ids()

    async def async_setValues(self, device_id, func_code, address, values):
        # Writes are never gated — straight pass-through.
        return await self._core.async_setValues(device_id, func_code, address, values)

    def _slice_snapshot(self, snapshot: tuple[int, ...], address: int, count: int) -> list[int] | None:
        """Slice the requested window out of the published snapshot.

        Returns ``None`` (triggering a live-core fallthrough) if the snapshot is
        missing or does not cover the requested window, so an out-of-range or
        not-yet-populated read behaves exactly as before.
        """
        if snapshot is None:
            return None
        offset = address - self._reg_start_address
        end = offset + count
        if offset < 0 or end > len(snapshot):
            return None
        return list(snapshot[offset:end])

    async def async_getValues(self, device_id, func_code, address, count: int = 1):
        # (a) STALE-BREAKER FIRST: if the upstream staleness breaker is open, fail
        # closed immediately with DEVICE_BUSY — never block a doomed request up to
        # max_wait_s.
        if self._pdu_helper.circuit_open:
            return ExcCodes.DEVICE_BUSY

        # (b) DISABLED: latching off — pure pass-through, no wait.
        if self._max_wait_s <= 0:
            return await self._core.async_getValues(device_id, func_code, address, count)

        # (c) NORMAL WAIT: bounded wait for a generation strictly newer than the
        # one this serial line was last served. The timeout is the per-request
        # effective wait (Finding 2): min(configured cap, the budget that leaves
        # room to serialize this read's response before the client's read
        # deadline). A large read gets a shorter latch window so its longer
        # on-wire response still fits. Returns immediately if a newer generation
        # already exists at entry.
        effective_wait_s = self._effective_wait_s(count)
        if effective_wait_s <= 0:
            # No room to latch this read without risking the client deadline:
            # serve the freshest available frame immediately (still fail-closed on
            # an open breaker, handled above) rather than waiting.
            generation, snapshot = self._gate.current_frame()
            self._last_served_gen = generation
            sliced = self._slice_snapshot(snapshot, address, count)
            if sliced is not None:
                return sliced
            return await self._core.async_getValues(device_id, func_code, address, count)
        try:
            await asyncio.wait_for(
                self._gate.wait_for_generation_after(self._last_served_gen),
                timeout=effective_wait_s,
            )
        except (asyncio.TimeoutError, TimeoutError):
            # (d) JITTER FALLBACK: the bounded wait timed out. If the breaker
            # OPENED during the wait, fail closed (DEVICE_BUSY). Otherwise serve
            # the current frame (one possible duplicate, acceptable) rather than
            # tripping a breaker on mere jitter.
            if self._pdu_helper.circuit_open:
                return ExcCodes.DEVICE_BUSY

        # (e) After a successful wait OR jitter fallthrough: sample the gate's
        # (generation, snapshot) pair as ONE unit, record THAT generation as
        # served, and return the window sliced from THAT snapshot. Because the
        # generation and the bytes come from the same sampled pair, the served
        # generation always matches the bytes returned (Finding 1). If the
        # snapshot cannot satisfy the request (missing / out of range) fall back
        # to the live core so odd reads behave exactly as before.
        generation, snapshot = self._gate.current_frame()
        self._last_served_gen = generation
        sliced = self._slice_snapshot(snapshot, address, count)
        if sliced is not None:
            return sliced
        return await self._core.async_getValues(device_id, func_code, address, count)
