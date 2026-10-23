"""Unit tests for serial frame-latching (FrameGate + SerialPacingContext).

The gate is driven deterministically on a single test event loop: publish() and
the proxy coroutines all run on the same loop, matching the production topology
(the listener-thread publish is hopped onto the server loop via
call_soon_threadsafe, so by the time publish() executes it is on the server
loop — exactly what we simulate here by calling publish() directly on the loop).

Finding 1 (atomic generation-stamped snapshot): publish() now carries BOTH a
generation and the immutable register snapshot committed for it, and the proxy
returns the window sliced from the sampled snapshot — so the generation a serial
line records as served always matches the bytes it returns. The gated read path
no longer calls the live core, so these tests assert on the returned bytes.

Finding 2 (per-request serialization budget): the effective latch wait is
min(configured cap, SerialWireBudget.available_wait_s(count)), so a large read
gets a shorter wait than a small one; asserted directly below.
"""

from __future__ import annotations

import asyncio
import unittest

from pymodbus.constants import ExcCodes

from app.utils.serial_pacing import FrameGate, SerialPacingContext, SerialWireBudget


class _FakePduHelper:
    """Minimal stand-in exposing a settable circuit_open bool."""

    def __init__(self, circuit_open: bool = False) -> None:
        self.circuit_open = circuit_open


class _FakeCore:
    """Minimal SimCore stand-in recording calls and returning a sentinel.

    Only the fallthrough paths (latching disabled, snapshot missing/out of range,
    writes) reach the core now; the gated read path returns a snapshot slice.
    """

    def __init__(self) -> None:
        self.get_calls: list[tuple] = []
        self.set_calls: list[tuple] = []
        self.sentinel = [1, 2, 3]

    def device_ids(self) -> list[int]:
        return [1, 7]

    async def async_getValues(self, device_id, func_code, address, count=1):
        self.get_calls.append((device_id, func_code, address, count))
        return self.sentinel

    async def async_setValues(self, device_id, func_code, address, values):
        self.set_calls.append((device_id, func_code, address, values))
        return None


def _snapshot(gen: int, length: int = 8) -> tuple[int, ...]:
    """A distinct register image whose values encode the generation.

    Register i holds (gen * 100 + i), so a sliced window is a function of the
    generation — distinct generations yield distinct bytes, which lets a test
    prove the bytes returned belong to the recorded generation.
    """
    return tuple(gen * 100 + i for i in range(length))


class TestSerialPacing(unittest.TestCase):
    def _make(self, *, circuit_open=False, max_wait_s=0.12, wire_budget=None, anchors=None):
        core = _FakeCore()
        helper = _FakePduHelper(circuit_open=circuit_open)
        gate = FrameGate()
        ctx = SerialPacingContext(
            core, gate, helper, max_wait_s, reg_start_address=0, wire_budget=wire_budget, anchors=anchors
        )
        return core, helper, gate, ctx

    def test_blocks_until_generation_advances_then_serves_snapshot(self):
        async def scenario():
            core, _helper, gate, ctx = self._make()
            task = asyncio.ensure_future(ctx.async_getValues(1, 3, 0x0000, 2))
            # Let the task reach the gate wait.
            await asyncio.sleep(0)
            self.assertFalse(task.done(), "read must block until a fresh frame lands")
            self.assertEqual(core.get_calls, [], "datastore must not be read while blocked")

            # A fresh frame lands (on this same loop — mirrors call_soon_threadsafe).
            gate.publish(1, _snapshot(1))
            result = await asyncio.wait_for(task, timeout=1.0)

            # The returned window is the snapshot slice for generation 1, and the
            # recorded generation matches those bytes.
            self.assertEqual(result, [100, 101])
            self.assertEqual(ctx._pinned_generation, 1)
            self.assertEqual(core.get_calls, [], "gated read must not touch the live core")

        asyncio.run(scenario())

    def test_newer_generation_already_present_returns_immediately(self):
        async def scenario():
            core, _helper, gate, ctx = self._make()
            gate.publish(5, _snapshot(5))  # generation already ahead of last-served (0)
            await asyncio.sleep(0)  # let publish's notify settle
            result = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0000, 2), timeout=0.05)
            self.assertEqual(result, [500, 501])
            self.assertEqual(ctx._pinned_generation, 5)
            self.assertEqual(core.get_calls, [])

        asyncio.run(scenario())

    def test_breaker_open_returns_device_busy_without_waiting(self):
        async def scenario():
            core, _helper, _gate, ctx = self._make(circuit_open=True, max_wait_s=10.0)
            # Despite a huge max_wait_s, an open breaker returns immediately.
            result = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0000, 2), timeout=0.1)
            self.assertIs(result, ExcCodes.DEVICE_BUSY)
            self.assertEqual(core.get_calls, [], "datastore must not be read when breaker open")

        asyncio.run(scenario())

    def test_timeout_with_breaker_closed_serves_current_frame(self):
        async def scenario():
            # Anchor at 0 so the read bounded-waits; with no newer generation the
            # wait times out and the jitter fallback re-pins the current snapshot.
            core, _helper, gate, ctx = self._make(max_wait_s=0.02, anchors={0})
            # A frame exists but no NEWER generation arrives → wait_for times out;
            # breaker stays closed → jitter fallback serves the current snapshot.
            gate.publish(3, _snapshot(3))
            await asyncio.sleep(0)
            # Pin the current generation so it is not "newer" than the pin.
            ctx._pinned_generation = 3
            ctx._pinned_snapshot = _snapshot(3)
            result = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0000, 2), timeout=1.0)
            self.assertEqual(result, [300, 301])
            self.assertEqual(core.get_calls, [])

        asyncio.run(scenario())

    def test_timeout_with_no_snapshot_falls_through_to_core(self):
        async def scenario():
            core, _helper, _gate, ctx = self._make(max_wait_s=0.02)
            # No publish ever arrives → no snapshot → jitter fallback falls through
            # to the live core.
            result = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0000, 2), timeout=1.0)
            self.assertEqual(result, core.sentinel)
            self.assertEqual(core.get_calls, [(1, 3, 0x0000, 2)])

        asyncio.run(scenario())

    def test_breaker_opens_during_wait_returns_device_busy(self):
        async def scenario():
            core, helper, _gate, ctx = self._make(max_wait_s=0.02)

            # Breaker opens before the bounded wait elapses; on timeout the
            # jitter path re-checks and fails closed.
            async def _open_soon():
                await asyncio.sleep(0.005)
                helper.circuit_open = True

            opener = asyncio.ensure_future(_open_soon())
            result = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0000, 2), timeout=1.0)
            await opener
            self.assertIs(result, ExcCodes.DEVICE_BUSY)
            self.assertEqual(core.get_calls, [])

        asyncio.run(scenario())

    def test_pacing_disabled_never_blocks(self):
        async def scenario():
            core, _helper, _gate, ctx = self._make(max_wait_s=0.0)
            result = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0000, 2), timeout=0.05)
            self.assertEqual(result, core.sentinel)
            self.assertEqual(core.get_calls, [(1, 3, 0x0000, 2)])

        asyncio.run(scenario())

    def test_set_values_passthrough_not_gated(self):
        async def scenario():
            core, _helper, _gate, ctx = self._make(circuit_open=True, max_wait_s=10.0)
            # Writes are never gated, even with an open breaker and large wait.
            result = await asyncio.wait_for(ctx.async_setValues(1, 3, 0x0000, [9, 9]), timeout=0.1)
            self.assertIsNone(result)
            self.assertEqual(core.set_calls, [(1, 3, 0x0000, [9, 9])])

        asyncio.run(scenario())

    def test_device_ids_passthrough(self):
        core, _helper, _gate, ctx = self._make()
        self.assertEqual(ctx.device_ids(), core.device_ids())

    def test_consecutive_anchor_reads_each_wait_for_a_new_generation(self):
        """Consecutive ANCHOR reads (start of a new cycle each) each block for a
        strictly-newer generation and advance the pin — no duplicate serve."""

        async def scenario():
            # Address 0 is an anchor, so each read here is a new-cycle anchor read.
            core, _helper, gate, ctx = self._make(anchors={0})
            # First read: block, publish gen 1, serve.
            t1 = asyncio.ensure_future(ctx.async_getValues(1, 3, 0, 2))
            await asyncio.sleep(0)
            gate.publish(1, _snapshot(1))
            r1 = await asyncio.wait_for(t1, timeout=1.0)
            self.assertEqual(ctx._pinned_generation, 1)
            self.assertEqual(r1, [100, 101])

            # Second anchor read with no new frame must block again (no duplicate).
            t2 = asyncio.ensure_future(ctx.async_getValues(1, 3, 0, 2))
            await asyncio.sleep(0)
            self.assertFalse(t2.done(), "second anchor read must block until gen advances")
            gate.publish(2, _snapshot(2))
            r2 = await asyncio.wait_for(t2, timeout=1.0)
            self.assertEqual(ctx._pinned_generation, 2)
            self.assertEqual(r2, [200, 201], "second anchor read must serve the NEW generation's bytes")
            self.assertEqual(core.get_calls, [], "gated reads never touch the live core")

        asyncio.run(scenario())

    # -- Finding 1 regression ------------------------------------------------

    def test_served_generation_matches_returned_bytes_when_wake_follows_update(self):
        """Finding 1: delivering the wake AFTER the snapshot update must still
        yield bytes that belong to the recorded generation, and a single
        published frame must be served to exactly one read (no duplicate).
        """

        async def scenario():
            # Address 0 is an anchor so each read is a new-cycle anchor read.
            core, _helper, gate, ctx = self._make(anchors={0})

            # A read is waiting for a new generation.
            t1 = asyncio.ensure_future(ctx.async_getValues(1, 3, 0, 3))
            await asyncio.sleep(0)
            self.assertFalse(t1.done())

            # The snapshot for gen 7 is committed and announced together.
            gate.publish(7, _snapshot(7))
            r1 = await asyncio.wait_for(t1, timeout=1.0)

            # Invariant 1: pinned generation == generation of the bytes returned.
            self.assertEqual(ctx._pinned_generation, 7)
            self.assertEqual(r1, [700, 701, 702])

            # Invariant (no duplicate): a second ANCHOR read for the SAME published
            # frame must block — one frame is served to at most one new cycle.
            t2 = asyncio.ensure_future(ctx.async_getValues(1, 3, 0, 3))
            await asyncio.sleep(0)
            self.assertFalse(t2.done(), "no second serve for one published frame")

            # Only when a strictly newer frame lands does the second read proceed,
            # and it serves THAT frame's bytes (never a stale repeat of gen 7).
            gate.publish(8, _snapshot(8))
            r2 = await asyncio.wait_for(t2, timeout=1.0)
            self.assertEqual(ctx._pinned_generation, 8)
            self.assertEqual(r2, [800, 801, 802])
            self.assertNotEqual(r1, r2, "two reads for two frames must differ (no duplicate)")

        asyncio.run(scenario())

    def test_out_of_range_request_falls_through_to_core(self):
        """A request the snapshot cannot cover falls back to the live core so odd
        reads behave exactly as before (snapshot has 8 regs; ask for 10)."""

        async def scenario():
            core, _helper, gate, ctx = self._make()
            gate.publish(1, _snapshot(1, length=8))
            await asyncio.sleep(0)
            result = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0000, 10), timeout=0.2)
            self.assertEqual(result, core.sentinel)
            self.assertEqual(core.get_calls, [(1, 3, 0x0000, 10)])

        asyncio.run(scenario())

    # -- Finding 2 regression ------------------------------------------------

    def test_effective_wait_is_min_of_configured_and_wire_budget(self):
        # 9600 8N1 -> 10 bits/byte -> byte_time = 10/9600 s.
        # available_wait(count) = 0.2 - (count*2+5)*byte_time - 0.02.
        budget = SerialWireBudget(baudrate=9600, bytesize=8, parity="N", stopbits=1, client_read_timeout_ms=200)
        _core, _helper, _gate, ctx = self._make(max_wait_s=0.12, wire_budget=budget)

        byte_time = 10 / 9600

        # Small read (2 regs): budget is large, so the configured cap wins.
        small_budget = 0.2 - (2 * 2 + 5) * byte_time - 0.02
        self.assertGreater(small_budget, 0.12)
        self.assertAlmostEqual(ctx._effective_wait_s(2), 0.12, places=6)

        # Large read (60 regs): budget is tighter than the cap, so budget wins.
        large_budget = 0.2 - (60 * 2 + 5) * byte_time - 0.02
        self.assertLess(large_budget, 0.12)
        self.assertAlmostEqual(ctx._effective_wait_s(60), large_budget, places=6)

        # Very large read (90 regs): budget goes non-positive -> clamped to 0 so
        # the proxy serves the freshest frame immediately (no latch wait).
        self.assertLessEqual(0.2 - (90 * 2 + 5) * byte_time - 0.02, 0.0)
        self.assertEqual(ctx._effective_wait_s(90), 0.0)

    def test_zero_budget_read_serves_current_frame_without_waiting(self):
        """A read whose per-request budget is non-positive must not block: it
        serves the freshest published snapshot immediately."""

        async def scenario():
            budget = SerialWireBudget(baudrate=9600, bytesize=8, parity="N", stopbits=1, client_read_timeout_ms=200)
            core, _helper, gate, ctx = self._make(max_wait_s=0.12, wire_budget=budget)
            gate.publish(4, _snapshot(4, length=100))
            await asyncio.sleep(0)
            # 90-register read -> zero budget -> immediate serve from snapshot.
            result = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0000, 90), timeout=0.05)
            self.assertEqual(result, [4 * 100 + i for i in range(90)])
            self.assertEqual(core.get_calls, [])

        asyncio.run(scenario())

    def test_no_wire_budget_uses_flat_max_wait(self):
        _core, _helper, _gate, ctx = self._make(max_wait_s=0.12, wire_budget=None)
        self.assertEqual(ctx._effective_wait_s(2), 0.12)
        self.assertEqual(ctx._effective_wait_s(1000), 0.12)

    # -- Anchor-based pacing -------------------------------------------------

    def test_anchor_read_waits_for_newer_generation_then_pins_and_serves(self):
        """An anchor-overlapping read waits for a generation strictly newer than
        the current pin, then pins that generation and serves from its snapshot."""

        async def scenario():
            # Anchor at address 0x0028; the read window [0x0028, 0x002A) overlaps it.
            core, _helper, gate, ctx = self._make(anchors={0x0028})
            # Pin an initial frame (gen 3) so we prove the anchor advances PAST it.
            gate.publish(3, _snapshot(3, length=0x0030))
            await asyncio.sleep(0)
            ctx._pinned_generation = 3
            ctx._pinned_snapshot = _snapshot(3, length=0x0030)

            task = asyncio.ensure_future(ctx.async_getValues(1, 3, 0x0028, 2))
            await asyncio.sleep(0)
            self.assertFalse(task.done(), "anchor read must block until a strictly-newer frame lands")

            gate.publish(4, _snapshot(4, length=0x0030))
            result = await asyncio.wait_for(task, timeout=1.0)

            self.assertEqual(ctx._pinned_generation, 4)
            self.assertEqual(result, [4 * 100 + 0x0028, 4 * 100 + 0x0029])
            self.assertEqual(core.get_calls, [], "gated anchor read must not touch the live core")

        asyncio.run(scenario())

    def test_non_anchor_read_serves_pinned_frame_immediately_without_waiting(self):
        """A non-anchor read serves IMMEDIATELY from the pinned snapshot with NO
        wait — even when no new generation has arrived — and returns the pinned
        generation's bytes."""

        async def scenario():
            core, _helper, gate, ctx = self._make(anchors={0x0028})
            # Pin generation 5; NO newer generation will arrive during this read.
            gate.publish(5, _snapshot(5, length=0x0030))
            await asyncio.sleep(0)
            ctx._pinned_generation = 5
            ctx._pinned_snapshot = _snapshot(5, length=0x0030)

            # A follow-on (non-anchor) read at 0x0000 must return at once from the
            # pinned frame. asyncio.wait_for with a tiny timeout proves no blocking.
            result = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0000, 2), timeout=0.01)
            self.assertEqual(result, [500, 501], "non-anchor read must serve the pinned generation's bytes")
            self.assertEqual(ctx._pinned_generation, 5, "non-anchor read must not advance the pin")
            self.assertEqual(core.get_calls, [], "non-anchor read must not touch the live core")

        asyncio.run(scenario())

    def test_full_sweep_coherent_from_one_pin_and_advances_per_anchor(self):
        """A full multi-block sweep (anchor read, then several non-anchor reads)
        returns ALL blocks from the SAME pinned generation; the NEXT anchor read
        advances to the next generation (sweep rate tracks upstream, not divided
        by block count)."""

        async def scenario():
            core, _helper, gate, ctx = self._make(anchors={0x0028, 0x0012})

            # --- Sweep 1: anchor read pins gen 1, follow-on reads serve from it ---
            t_anchor = asyncio.ensure_future(ctx.async_getValues(1, 3, 0x0028, 2))
            await asyncio.sleep(0)
            self.assertFalse(t_anchor.done())
            gate.publish(1, _snapshot(1, length=0x0200))
            power = await asyncio.wait_for(t_anchor, timeout=1.0)
            self.assertEqual(ctx._pinned_generation, 1)

            # A newer frame lands mid-sweep; it must NOT be observed by the
            # follow-on blocks (they stay coherent with the pinned gen 1).
            gate.publish(2, _snapshot(2, length=0x0200))
            await asyncio.sleep(0)

            voltage = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0000, 4), timeout=0.01)
            energy = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0100, 4), timeout=0.01)

            self.assertEqual(power, [100 + 0x0028, 100 + 0x0029])
            self.assertEqual(voltage, [100 + 0, 100 + 1, 100 + 2, 100 + 3])
            self.assertEqual(energy, [100 + 0x0100, 100 + 0x0101, 100 + 0x0102, 100 + 0x0103])
            self.assertEqual(ctx._pinned_generation, 1, "follow-on reads must not advance the pin")

            # --- Sweep 2: the next anchor read advances exactly one pin step ------
            power2 = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0028, 2), timeout=1.0)
            self.assertEqual(ctx._pinned_generation, 2, "sweep rate tracks upstream (one pin step per sweep)")
            self.assertEqual(power2, [200 + 0x0028, 200 + 0x0029])
            self.assertEqual(core.get_calls, [], "gated sweep must never touch the live core")

        asyncio.run(scenario())

    def test_first_ever_read_before_any_pin_waits_once_and_pins(self):
        """First-ever read before any pin exists waits once for a fresh frame and
        pins it — even a non-anchor first read (startup is covered)."""

        async def scenario():
            core, _helper, gate, ctx = self._make(anchors={0x0028})
            # A non-anchor address, but no pin exists yet -> must wait once.
            task = asyncio.ensure_future(ctx.async_getValues(1, 3, 0x0000, 2))
            await asyncio.sleep(0)
            self.assertFalse(task.done(), "first-ever read must block until a frame is published")
            self.assertEqual(ctx._pinned_generation, 0)

            gate.publish(1, _snapshot(1))
            result = await asyncio.wait_for(task, timeout=1.0)
            self.assertEqual(ctx._pinned_generation, 1)
            self.assertEqual(result, [100, 101])
            self.assertEqual(core.get_calls, [])

        asyncio.run(scenario())

    def test_anchor_path_breaker_open_returns_device_busy(self):
        """Stale-breaker-first still holds on the anchor path: an open breaker
        returns DEVICE_BUSY immediately without waiting."""

        async def scenario():
            core, _helper, _gate, ctx = self._make(circuit_open=True, max_wait_s=10.0, anchors={0x0028})
            result = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0028, 2), timeout=0.1)
            self.assertIs(result, ExcCodes.DEVICE_BUSY)
            self.assertEqual(core.get_calls, [])

        asyncio.run(scenario())

    def test_anchor_path_respects_wire_budget(self):
        """The per-request wire budget still bounds the anchor-read wait: a read
        whose budget is non-positive pins the freshest frame immediately."""

        async def scenario():
            budget = SerialWireBudget(baudrate=9600, bytesize=8, parity="N", stopbits=1, client_read_timeout_ms=200)
            core, _helper, gate, ctx = self._make(max_wait_s=0.12, wire_budget=budget, anchors={0x0000})
            # 90-register read starting on the anchor -> zero budget -> immediate
            # pin+serve from the freshest snapshot (no latch wait).
            gate.publish(4, _snapshot(4, length=100))
            await asyncio.sleep(0)
            self.assertEqual(ctx._effective_wait_s(90), 0.0)
            result = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0000, 90), timeout=0.05)
            self.assertEqual(result, [4 * 100 + i for i in range(90)])
            self.assertEqual(ctx._pinned_generation, 4)
            self.assertEqual(core.get_calls, [])

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
