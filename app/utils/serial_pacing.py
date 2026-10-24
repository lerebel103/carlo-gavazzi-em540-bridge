"""Serial-only frame-latching for downstream Modbus slave bridges.

Downstream Modbus-over-SERIAL clients can poll the bridge faster than the
upstream EM540/EM530 master refreshes, so they receive DUPLICATE frames. This
module latches a serial client's effective read rate to the upstream refresh
rate.

ANCHOR-BASED PACING
-------------------
Real serial clients split one polling cycle into MULTIPLE separate reads (power
block, then voltage/current, then energy). A naive per-read latch that advances
a cursor on EVERY read is wrong for those clients: read 1 gets frame N, read 2
waits for N+1, read 3 for N+2 — the blocks are torn across frames AND the full
sweep rate is throttled to one picture per (number-of-blocks) upstream ticks.

So pacing keys off a single cycle-start ANCHOR register instead. Each bridge
supplies a SINGLE ``primary_anchor`` start address — its per-cycle heartbeat read
(the system-power register the real client polls FIRST every cycle; see the
bridge modules for the Victron / Fronius rationale) — plus the full ``anchors``
set (the complete power-register anchor set the overlap/follow-on logic still
recognises). On each gated read:

  - If the requested window OVERLAPS the SINGLE primary cycle-start anchor, it is
    the start of a new polling cycle: bounded-wait for a generation STRICTLY
    NEWER than the one currently PINNED, then PIN that (generation, snapshot)
    pair and serve from it. This paces the sweep to the upstream rate and
    advances the pin AT MOST ONCE per client cycle.
  - Any other read — a SECONDARY power anchor (EM540 0x0012; TS65A 40099/40101/
    40103) or a non-anchor block — is a follow-on read in the same cycle: serve
    IMMEDIATELY from the pinned (generation, snapshot) pair with NO wait and NO
    re-pin, so every block of a sweep is coherent with the one pinned frame.

A cycle that reads BOTH the primary and the secondary power anchors therefore
advances the pin EXACTLY once (on the primary) and returns every block from a
single generation (whole-cycle coherence).

The first-ever read before any pin exists behaves like today's first read: wait
once for a fresh frame and pin it.

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
from collections.abc import Callable, Iterable
from dataclasses import dataclass

from pymodbus.constants import ExcCodes

from app.utils.pdu_helper import PduHelper

# Register-read function codes the snapshot fast-path applies to: FC3 (read
# holding registers) and FC4 (read input registers). Any other function code
# (FC1 read coils, FC2 read discrete inputs, etc.) carries bit addressing and
# different datastore semantics, so it is NOT served from the register snapshot
# and falls through to the real core (see async_getValues, Finding B1).
_REGISTER_READ_FUNC_CODES: frozenset[int] = frozenset({3, 4})


def valid_register_addresses(sim_runtime, block_key: str) -> frozenset[int]:
    """Return the set of DEFINED register addresses for a SimRuntime block.

    The pymodbus SimRuntime block is ``(start_address, register_count, registers,
    flags)``. An address is only a real register when its ``flags`` entry is not
    marked ``DataType.INVALID`` (the low nibble of the flag byte is the type; a
    value of ``DataType.INVALID`` == 1 means "no register here"). The backing
    register array can be contiguous across map gaps (gap addresses hold 0), so
    the flags — not the array length — are the authority on validity (Finding B2).

    Imported lazily so the module has no hard pymodbus-simulator-internals
    dependency at import time for the unit-tested pacing logic itself.
    """
    from pymodbus.simulator.simutils import DataType, SimUtils

    start_address, _count, _registers, flags = sim_runtime.block[block_key]
    return frozenset(
        start_address + i for i, flag in enumerate(flags) if (flag & SimUtils.RunTimeFlag_TYPE) != DataType.INVALID
    )


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

    SYNCHRONOUS, LOST-WAKEUP-FREE WAKE (set-then-replace Event)
    ----------------------------------------------------------
    ``publish`` wakes a parked waiter SYNCHRONOUSLY within the same loop turn. It
    updates ``current_generation``/``_snapshot`` and then, in the SAME call,
    ``set()`` s the current :class:`asyncio.Event` and swaps in a FRESH unset Event
    for the next generation. A waiter that parked on the old Event before the
    advance is released immediately (its ``wait()`` future is already resolved);
    the next waiter awaits the fresh Event. There is NO ``asyncio.ensure_future``
    and NO deferred notify task, so:

      - the wake is not deferred to a later loop turn (no extra scheduling hop /
        frame of latency), and
      - there is no lost-wakeup window: because ``current_generation`` is advanced
        BEFORE (synchronously, same turn) the Event is swapped, a waiter that
        enters between the fast-path check and parking re-checks the generation on
        the next loop iteration and takes the fast-path instead of sleeping on a
        stale Event (see :meth:`wait_for_generation_after`). A ``set()`` that lands
        with no parked waiter is harmless — the fresh Event the next waiter sees is
        unset, but that waiter first re-checks the (already advanced) generation.

    The :class:`asyncio.Event` is created LAZILY on first use so it binds to the
    running server loop (an asyncio primitive binds to the loop at first await, not
    construction; creating/awaiting on the wrong loop raises "got Future attached
    to a different loop"). Both :meth:`publish` and
    :meth:`wait_for_generation_after` are only ever invoked on the server loop.
    """

    def __init__(self) -> None:
        # current_generation and _snapshot are updated together under the single
        # synchronous assignment in publish() (both run on the server loop), so a
        # reader on that same loop always observes a consistent pair.
        self.current_generation: int = 0
        self._snapshot: tuple[int, ...] | None = None
        # The Event the CURRENT generation's waiters park on. Lazily created on
        # the server loop; set-then-replaced on every advancing publish().
        self._event: asyncio.Event | None = None

    def _get_event(self) -> asyncio.Event:
        if self._event is None:
            # Created on the running (server) loop on first use.
            self._event = asyncio.Event()
        return self._event

    def publish(self, generation: int, snapshot: tuple[int, ...]) -> None:
        """Store the latest generation + its register snapshot and wake any awaiter.

        Runs on the server loop (scheduled via ``call_soon_threadsafe``). The
        ``snapshot`` is the IMMUTABLE register image produced on the listener
        thread from the exact bytes that were committed for ``generation`` — the
        generation and the bytes are one unit.

        The generation/snapshot advance happens FIRST, then the wake: ``set()``
        the current Event and swap in a fresh unset Event for the next generation.
        A parked waiter is released SYNCHRONOUSLY (within this same loop turn, no
        deferred task), and the next waiter awaits the fresh Event. Because the
        generation is advanced before the Event swap, a waiter racing between its
        fast-path check and parking cannot lose the wakeup: it re-checks the
        advanced generation and takes the fast-path. A monotonic guard keeps the
        generation only ever advancing.
        """
        if generation <= self.current_generation:
            # Stale / duplicate publish: nothing advanced, so there is nothing to
            # wake. Do not touch the Event (no spurious set).
            return
        self.current_generation = generation
        self._snapshot = snapshot
        # Wake synchronously: release everyone parked on the current Event, then
        # install a fresh unset Event for the next generation's waiters.
        event = self._get_event()
        event.set()
        self._event = asyncio.Event()

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
        slower than upstream). Otherwise it parks on the CURRENT Event, then
        RE-CHECKS the generation in a loop (re-capturing the Event each iteration):
        a waiter that entered just after an advance takes the fast-path and never
        blocks, and a spurious/early ``set()`` is absorbed by the re-check — so no
        wakeup is ever lost.
        """
        while self.current_generation <= generation:
            event = self._get_event()
            await event.wait()


class SerialPacingContext:
    """Serial-only async context proxy wrapping the real ``SimCore``.

    Forwards ``device_ids()`` and ``async_setValues(...)`` as straight
    pass-throughs (writes are NEVER gated) and implements ANCHOR-BASED pacing in
    ``async_getValues(...)``.

    A single per-instance ``_pinned_generation`` cursor models "which frame is the
    current polling cycle pinned to?" — correct because a serial server is
    single-handler/half-duplex (one awaiter at a time), so no per-connection state
    is needed. A PRIMARY cycle-start anchor read advances the pin by
    bounded-waiting for a strictly-newer generation; every other read (secondary
    power anchors and non-anchor blocks) serves coherently from the existing pin
    without waiting.

    The ``primary_anchor`` is the SINGLE served START address of this bridge's
    per-cycle heartbeat read (the system-power register the real client polls
    FIRST every cycle; EM540 0x0028, TS65A 40097). The ``anchors`` set holds the
    FULL set of power-register anchor start addresses (primary + secondary),
    expressed in the same address space the bridge serves and the client requests
    — it is retained so the overlap/follow-on logic recognises the whole power
    block, but ONLY the primary anchor advances the pin. A read is a PRIMARY
    anchor read when the primary anchor falls in ``range(address, address +
    count)`` — overlap on the start address is sufficient.

    If ``max_wait_s <= 0`` latching is disabled and ``async_getValues`` is a pure
    pass-through (today's behaviour, duplicate frames possible).

    The snapshot fast-path applies ONLY to register reads (FC3 read-holding, FC4
    read-input). Bit reads (FC1 read-coils, FC2 read-discrete-inputs) and any
    other func code fall through to the real core so they keep their correct
    datastore semantics/exception (Finding B1) — the proxy replaces the whole
    serial context, so those requests reach it too. A register read into an
    address the datastore does not define falls through to the core as well, so
    it raises the correct ILLEGAL_ADDRESS rather than serving snapshot zeros for
    a map gap (Finding B2).

    Finding 1 — atomic generation-stamped serial snapshot
    ------------------------------------------------------
    On the GATED read path the proxy no longer re-reads the live ``SimCore``
    register array (which the listener thread can overwrite mid-read). Instead it
    samples the gate's ``(generation, snapshot)`` pair as ONE unit (the snapshot
    is the immutable register image the listener thread committed for that exact
    generation), PINS that pair on an anchor read, and slices every read of the
    cycle out of the pinned snapshot. Invariants held by construction:
      1. pinned generation == generation of the bytes returned (both come from
         the same sampled pair — there is no second read in between);
      2. no torn/duplicate frame across the listener-write / server-publish hop
         (the bytes are committed into the immutable snapshot BEFORE the
         generation is announced, and the pin only advances when an anchor read
         consumes a strictly-newer generation);
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
        anchors: Iterable[int] | None = None,
        primary_anchor: int | None = None,
        valid_addresses: Iterable[int] | None = None,
        on_cycle_advance: Callable[[], None] | None = None,
    ) -> None:
        self._core = core
        self._gate = gate
        self._pdu_helper = pdu_helper
        self._max_wait_s = max_wait_s
        # Base Modbus protocol address of the served register block, used to map a
        # requested address onto an index into the published snapshot tuple.
        self._reg_start_address = reg_start_address
        # Addresses the real datastore actually DEFINES (Finding B2). The snapshot
        # tuple is a flat copy of the backing register array and carries no
        # validity flags, so a window spanning an UNDEFINED gap (e.g. a TS65A read
        # at 49999) would be served real-looking zeros where the core raises
        # ILLEGAL_ADDRESS. When this set is supplied, a snapshot slice is only
        # served if EVERY requested address is defined; otherwise the read falls
        # through to the real core so it raises the correct ILLEGAL_ADDRESS. When
        # None (unit tests / no map supplied) validity is deferred to the core via
        # the existing bounds check only.
        self._valid_addresses: frozenset[int] | None = (
            frozenset(valid_addresses) if valid_addresses is not None else None
        )
        # Per-request serialization budget (Finding 2). When set, the effective
        # latch wait for a read is min(max_wait_s, budget_for_this_count) so the
        # response still fits the serial client's read deadline. When None (e.g.
        # in unit tests) the flat max_wait_s is used unchanged.
        self._wire_budget = wire_budget
        # FULL power-anchor address set (0-based Modbus protocol start addresses).
        # Retained so the overlap/follow-on logic recognises the whole per-cycle
        # power block, but it no longer decides pin advances — see _primary_anchor.
        self._anchors: frozenset[int] = frozenset(anchors or ())
        # The SINGLE primary cycle-start anchor. ONLY a read overlapping this
        # address advances the pin (at most one advance per client cycle). When
        # None (unit tests / no primary supplied) the context falls back to
        # treating ANY anchor in ``anchors`` as a cycle start — the degenerate
        # per-read latch the earlier anchor model used — so existing no-primary
        # tests keep their behaviour.
        self._primary_anchor: int | None = primary_anchor
        # The (generation, snapshot) pair the current polling cycle is pinned to.
        # Pin 0 / None means "no frame pinned yet" (startup), handled like today's
        # first read: the first anchor (or, with no anchors configured, the first
        # read) waits once for a fresh frame and pins it.
        self._pinned_generation: int = 0
        self._pinned_snapshot: tuple[int, ...] | None = None
        # Lock-free single-writer counter of PRIMARY cycle-start pin advances —
        # incremented ONCE each time a primary cycle-start anchor read advances
        # the pin to a strictly-NEWER generation (one tick per client polling
        # cycle). This is the Hz 'Serial Read Rate' source: it tracks the real
        # per-cycle poll rate, unlike a per-inbound-request count which overcounts
        # the sparse multi-read cycles a Victron/Fronius client issues. Written
        # only on the serial server loop (inside async_getValues, which runs
        # there) and read lock-free by the diagnostics thread — a GIL-atomic int,
        # so it never blocks the request path.
        self.cycle_start_advances: int = 0
        # Optional sink notified ONCE per primary cycle-start pin advance (the Hz
        # 'Serial Read Rate' event). The bridge wires this to its
        # SerialActivityTracker.record_cycle_read so the diagnostics layer can
        # derive the windowed read-rate from the cycle count instead of the raw
        # inbound-request count. Called only on the serial server loop (inside
        # async_getValues), so it stays on the lock-free single-writer path.
        self._on_cycle_advance = on_cycle_advance
        # Whether a PRIMARY cycle-start anchor read has ever advanced the pin for
        # this client. A client that drives pacing via the primary anchor sets
        # this True on its first cycle start, and its follow-on reads then stay
        # coherent with the pinned frame for the rest of each cycle. A client that
        # NEVER reads the primary cycle-start anchor (non-anchor-only) leaves this
        # False; its reads get a bounded refresh to the latest available
        # generation so it is not frozen on one frame forever (COMMIT 2 /
        # pin-age bound). The bound is upstream generation AVAILABILITY — the
        # refresh only jumps to a generation the gate has already published, never
        # a configured wall-clock cadence.
        self._cycle_start_seen: bool = False

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
        # Finding B2: the snapshot carries register values but no address-validity
        # flags. If any requested address is NOT defined by the real datastore,
        # return None so the caller falls through to the core and it raises the
        # correct ILLEGAL_ADDRESS instead of serving snapshot zeros for a gap.
        if self._valid_addresses is not None and not all(
            addr in self._valid_addresses for addr in range(address, address + count)
        ):
            return None
        return list(snapshot[offset:end])

    def _is_cycle_start_read(self, address: int, count: int) -> bool:
        """True when the requested window starts a new polling cycle.

        A cycle start is a read overlapping the SINGLE primary cycle-start anchor
        (``_primary_anchor``); overlap on the start address is sufficient
        (``address <= primary < address + count``). ONLY a cycle-start read
        advances the pin, so the pin advances AT MOST once per client cycle even
        when the cycle also reads the secondary power anchors.

        When no primary anchor is configured (unit tests / no-primary case) this
        falls back to overlapping ANY address in the full ``anchors`` set — the
        degenerate per-read latch the earlier anchor model used.
        """
        window_end = address + count
        if self._primary_anchor is not None:
            return address <= self._primary_anchor < window_end
        if not self._anchors:
            return False
        return any(address <= anchor < window_end for anchor in self._anchors)

    async def async_getValues(self, device_id, func_code, address, count: int = 1):
        # (a) STALE-BREAKER FIRST: if the upstream staleness breaker is open, fail
        # closed immediately with DEVICE_BUSY — never block a doomed request up to
        # max_wait_s.
        if self._pdu_helper.circuit_open:
            return ExcCodes.DEVICE_BUSY

        # (a2) UNSERVED DEVICE-ID DELEGATION (COMMIT 3): downstream controllers
        # (e.g. Fronius inverters) scan a RANGE of Modbus unit ids to discover
        # devices. A request for an id this bridge does not serve must be answered
        # by the real core (which returns the correct scan exception) and must
        # NEVER advance the pacing pin or wait for a frame — otherwise discovery
        # scan noise would re-pace the served client. Checked BEFORE any wait or
        # snapshot serve, but AFTER the stale-breaker so a doomed request still
        # fails closed first. Writes/device_ids() are already plain pass-throughs.
        if device_id not in self._core.device_ids():
            return await self._core.async_getValues(device_id, func_code, address, count)

        # (b) DISABLED: latching off — pure pass-through, no wait.
        if self._max_wait_s <= 0:
            return await self._core.async_getValues(device_id, func_code, address, count)

        # (b2) NON-REGISTER READ (Finding B1): the proxy replaces the serial
        # server's ENTIRE context, so bit reads (FC1 read-coils / FC2
        # read-discrete-inputs) and any non-register func code also reach here.
        # The snapshot fast-path treats addresses/counts as REGISTER
        # addresses/counts and cannot reproduce bit-read semantics. Serve those
        # from the real core so they keep their correct behaviour/exception;
        # only FC3/FC4 register reads are paced from the snapshot.
        if func_code not in _REGISTER_READ_FUNC_CODES:
            return await self._core.async_getValues(device_id, func_code, address, count)

        is_cycle_start = self._is_cycle_start_read(address, count)
        have_pin = self._pinned_generation > 0 and self._pinned_snapshot is not None

        # (c) FOLLOW-ON READ: any non-cycle-start read in an established cycle —
        # a SECONDARY power anchor (EM540 0x0012; TS65A 40099/40101/40103) or a
        # non-anchor block — serves IMMEDIATELY and coherently from the pinned
        # (generation, snapshot) pair: no wait, no pin advance. Every block of the
        # sweep comes from the SAME frame the primary anchor pinned, so no
        # cross-frame tear and no sweep-rate throttling by block count. If the pin
        # cannot cover the window (odd / out-of-range read) fall through to the
        # live core so it behaves exactly as before.
        if not is_cycle_start and have_pin:
            # (c1) PIN-AGE BOUND (COMMIT 2): a client that NEVER reads the primary
            # cycle-start anchor would otherwise be frozen on the frame its first
            # read happened to pin. For such a non-anchor-only client (one that has
            # never advanced the pin via the primary anchor), refresh the pin to
            # the LATEST ALREADY-PUBLISHED generation when a strictly-newer one is
            # available — bounded purely by upstream generation availability (NOT a
            # configured wall-clock cadence, and with no wait: the refresh only
            # jumps to a frame the gate has already announced). A client that DOES
            # drive the pin via the primary anchor keeps whole-cycle coherence: its
            # follow-on reads never refresh here, so every block of a cycle comes
            # from the one pinned frame.
            if not self._cycle_start_seen:
                latest_generation, latest_snapshot = self._gate.current_frame()
                if latest_generation > self._pinned_generation and latest_snapshot is not None:
                    self._pinned_generation = latest_generation
                    self._pinned_snapshot = latest_snapshot
            sliced = self._slice_snapshot(self._pinned_snapshot, address, count)
            if sliced is not None:
                return sliced
            return await self._core.async_getValues(device_id, func_code, address, count)

        # (d) CYCLE-START (primary anchor) READ, or first-ever read before any pin
        # exists: start of a new polling cycle. A genuine cycle-start read marks
        # this client as primary-anchor-driven so its follow-on reads stay
        # coherent (and never take the pin-age refresh above). A first-ever read
        # that is NOT a cycle start (a non-anchor-only client's opening read) does
        # NOT set the flag, so that client keeps getting the pin-age refresh.
        if is_cycle_start:
            self._cycle_start_seen = True

        # Bounded-wait for a generation strictly newer than the currently PINNED
        # generation, then pin and serve from that pair (the single pin advance
        # for this cycle). The
        # timeout is the per-request effective wait (Finding 2): min(configured
        # cap, the budget that leaves room to serialize this read's response
        # before the client's read deadline). Returns immediately if a newer
        # generation already exists at entry.
        effective_wait_s = self._effective_wait_s(count)
        if effective_wait_s <= 0:
            # No room to latch this read without risking the client deadline:
            # pin the freshest available frame immediately (still fail-closed on
            # an open breaker, handled above) rather than waiting.
            return await self._pin_and_serve(device_id, func_code, address, count)
        try:
            await asyncio.wait_for(
                self._gate.wait_for_generation_after(self._pinned_generation),
                timeout=effective_wait_s,
            )
        except (asyncio.TimeoutError, TimeoutError):
            # (e) JITTER FALLBACK: the bounded wait timed out. If the breaker
            # OPENED during the wait, fail closed (DEVICE_BUSY). Otherwise pin and
            # serve the current frame (one possible duplicate, acceptable) rather
            # than tripping a breaker on mere jitter.
            if self._pdu_helper.circuit_open:
                return ExcCodes.DEVICE_BUSY

        previous_generation = self._pinned_generation
        result = await self._pin_and_serve(device_id, func_code, address, count)
        # Hz 'Serial Read Rate' source: count ONE tick when a PRIMARY cycle-start
        # anchor read advances the pin to a strictly-newer generation (one per
        # client polling cycle). Not bumped on a first-ever non-anchor read, a
        # repeat of the same generation (jitter), or any follow-on read. Written
        # only here on the serial server loop (lock-free, GIL-atomic int).
        if is_cycle_start and self._pinned_generation > previous_generation:
            self.cycle_start_advances += 1
            if self._on_cycle_advance is not None:
                self._on_cycle_advance()
        return result

    async def _pin_and_serve(self, device_id, func_code, address, count):
        """Sample the gate's (generation, snapshot) pair, PIN it, and serve.

        Samples the gate's ``(generation, snapshot)`` pair as ONE unit, pins THAT
        pair as the current cycle, and returns the window sliced from THAT
        snapshot. Because the generation and the bytes come from the same sampled
        pair, the pinned generation always matches the bytes returned (Finding 1).
        If the snapshot cannot satisfy the request (missing / out of range) fall
        back to the live core so odd reads behave exactly as before.
        """
        generation, snapshot = self._gate.current_frame()
        self._pinned_generation = generation
        self._pinned_snapshot = snapshot
        sliced = self._slice_snapshot(snapshot, address, count)
        if sliced is not None:
            return sliced
        return await self._core.async_getValues(device_id, func_code, address, count)
