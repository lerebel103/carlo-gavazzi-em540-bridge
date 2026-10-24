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

    def __init__(self, result=None) -> None:
        self.get_calls: list[tuple] = []
        self.set_calls: list[tuple] = []
        self.sentinel = [1, 2, 3] if result is None else result

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
    def _make(self, *, circuit_open=False, max_wait_s=0.12, wire_budget=None, anchors=None, valid_addresses=None):
        core = _FakeCore()
        helper = _FakePduHelper(circuit_open=circuit_open)
        gate = FrameGate()
        ctx = SerialPacingContext(
            core,
            gate,
            helper,
            max_wait_s,
            reg_start_address=0,
            wire_budget=wire_budget,
            anchors=anchors,
            valid_addresses=valid_addresses,
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

    # -- Synchronous, lost-wakeup-free wake (FrameGate) ----------------------

    def test_publish_before_wait_returns_via_generation_fast_path(self):
        """(a) A publish that lands BEFORE a waiter parks is observed via the
        generation fast-path: wait_for_generation_after returns immediately
        without ever blocking on the Event (no second publish needed)."""

        async def scenario():
            gate = FrameGate()
            gate.publish(1, _snapshot(1))
            # current_generation is already ahead of 0, so this must not block.
            await asyncio.wait_for(gate.wait_for_generation_after(0), timeout=0.01)
            self.assertEqual(gate.current_generation, 1)

        asyncio.run(scenario())

    def test_parked_waiter_released_in_same_loop_turn_as_publish(self):
        """(b) A waiter already parked on the gate is released PROMPTLY in the
        same loop turn as publish(): after publish() a single `await
        asyncio.sleep(0)` lets the parked wait complete — no deferred notify
        task, no second publish."""

        async def scenario():
            gate = FrameGate()
            waiter = asyncio.ensure_future(gate.wait_for_generation_after(0))
            await asyncio.sleep(0)  # let the waiter park on the Event
            self.assertFalse(waiter.done(), "waiter must be parked before publish")

            gate.publish(1, _snapshot(1))  # synchronous wake, no ensure_future
            await asyncio.sleep(0)  # single turn to let the resolved wait run
            self.assertTrue(waiter.done(), "parked waiter must wake in the same loop turn")
            await waiter  # must not raise / re-block
            self.assertEqual(gate.current_generation, 1)

        asyncio.run(scenario())

    def test_advance_between_fast_path_and_park_is_not_a_lost_wakeup(self):
        """(c) A publish that advances the generation in the window between a
        waiter's fast-path check and its parking still releases the waiter — the
        post-wake re-check takes the generation fast-path, so the wakeup is not
        lost even though the set() happened against the Event the waiter had not
        yet awaited."""

        async def scenario():
            gate = FrameGate()
            # Simulate the race: the generation advances (publish) between when a
            # waiter would read current_generation (0, not yet > target) and when
            # it awaits the Event. Publishing first, THEN parking, exercises the
            # re-check: the waiter must observe the advanced generation and return
            # without sleeping forever on a fresh (unset) Event.
            gate.publish(1, _snapshot(1))
            # A fresh unset Event is now installed; a naive waiter that blindly
            # awaited it would hang. The re-check must save it.
            await asyncio.wait_for(gate.wait_for_generation_after(0), timeout=0.05)
            self.assertEqual(gate.current_generation, 1)

        asyncio.run(scenario())

    def test_publish_contains_no_deferred_notify_task(self):
        """The wake must be synchronous: publish() must not schedule any task on
        the loop (no asyncio.ensure_future / deferred notify)."""

        async def scenario():
            gate = FrameGate()
            before = len(asyncio.all_tasks())
            gate.publish(1, _snapshot(1))
            after = len(asyncio.all_tasks())
            self.assertEqual(after, before, "publish() must not create a deferred notify task")

        asyncio.run(scenario())

    def test_stale_publish_does_not_advance_or_wake(self):
        """A publish whose generation is not strictly newer leaves the pinned
        generation/snapshot unchanged and does not release a waiter."""

        async def scenario():
            gate = FrameGate()
            gate.publish(5, _snapshot(5))
            waiter = asyncio.ensure_future(gate.wait_for_generation_after(5))
            await asyncio.sleep(0)
            self.assertFalse(waiter.done(), "waiter targets > 5, must be parked")

            # A stale (<=5) publish must not wake the waiter nor change state.
            gate.publish(5, _snapshot(99))
            await asyncio.sleep(0)
            self.assertFalse(waiter.done(), "stale publish must not release the waiter")
            self.assertEqual(gate.current_generation, 5)
            self.assertEqual(gate.current_frame()[1], _snapshot(5), "snapshot must not change on stale publish")

            # A genuinely newer publish releases it.
            gate.publish(6, _snapshot(6))
            await asyncio.sleep(0)
            self.assertTrue(waiter.done())
            await waiter

        asyncio.run(scenario())

    def test_wake_suppressed_stalls_negative_control(self):
        """Negative control: with NO publish advancing the generation, a parked
        waiter never wakes (proves the tests above assert a real wake, not a
        trivially-satisfied condition)."""

        async def scenario():
            gate = FrameGate()
            waiter = asyncio.ensure_future(gate.wait_for_generation_after(0))
            await asyncio.sleep(0)
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(waiter), timeout=0.05)
            waiter.cancel()

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

    # -- Finding B1: bit reads (FC1/FC2) fall through to the real core ---------

    def test_bit_read_func_codes_fall_through_to_core_not_snapshot(self):
        """FC1 (read coils) and FC2 (read discrete inputs) reach the proxy because
        it replaces the whole serial context. The snapshot fast-path must NOT
        handle them (it would return raw register words); they fall through to the
        real core so the datastore's bit-read semantics/exception are preserved."""

        async def scenario():
            core, _helper, gate, ctx = self._make(anchors={0})
            # A fresh frame exists and would satisfy a register read at this window.
            gate.publish(1, _snapshot(1))
            await asyncio.sleep(0)

            for func_code in (1, 2):
                core.get_calls.clear()
                result = await asyncio.wait_for(ctx.async_getValues(1, func_code, 0, 2), timeout=0.2)
                # Served by the core, NOT a snapshot slice.
                self.assertEqual(result, core.sentinel)
                self.assertEqual(core.get_calls, [(1, func_code, 0, 2)])

        asyncio.run(scenario())

    def test_register_read_func_codes_still_use_snapshot_fast_path(self):
        """FC3 (read holding) and FC4 (read input) remain paced from the snapshot
        — the B1 fix must not disturb register reads."""

        async def scenario():
            for func_code in (3, 4):
                core, _helper, gate, ctx = self._make(anchors={0})
                gate.publish(2, _snapshot(2))
                await asyncio.sleep(0)
                result = await asyncio.wait_for(ctx.async_getValues(1, func_code, 0, 2), timeout=0.2)
                self.assertEqual(result, [200, 201], "register read must serve the snapshot slice")
                self.assertEqual(core.get_calls, [], "register read must not touch the live core")

        asyncio.run(scenario())

    # -- Finding B2: undefined-gap reads defer ILLEGAL_ADDRESS to the core ------

    def test_paced_read_into_undefined_gap_falls_through_to_core(self):
        """A paced register read whose window covers an address the datastore does
        NOT define must fall through to the core so it raises ILLEGAL_ADDRESS,
        instead of serving snapshot zeros for a map gap (reviewer's TS65A 49999
        example)."""

        async def scenario():
            # Datastore defines addresses 0..3 and 6..7, with 4 and 5 as a gap.
            core = _FakeCore(result=ExcCodes.ILLEGAL_ADDRESS)
            helper = _FakePduHelper(circuit_open=False)
            gate = FrameGate()
            ctx = SerialPacingContext(
                core,
                gate,
                helper,
                0.12,
                reg_start_address=0,
                anchors={0},
                valid_addresses={0, 1, 2, 3, 6, 7},
            )
            gate.publish(1, _snapshot(1, length=8))
            await asyncio.sleep(0)

            # Window [3, 5) touches the undefined gap at 4 -> fall through to core,
            # which returns the same ILLEGAL_ADDRESS an unpaced read would.
            result = await asyncio.wait_for(ctx.async_getValues(1, 3, 3, 2), timeout=0.2)
            self.assertIs(result, ExcCodes.ILLEGAL_ADDRESS)
            self.assertEqual(core.get_calls, [(1, 3, 3, 2)])

        asyncio.run(scenario())

    # -- Single primary cycle-start anchor (COMMIT 1) ------------------------

    def test_both_power_anchors_in_one_cycle_advance_pin_exactly_once(self):
        """A cycle that reads BOTH power anchors (primary 0x0028 then secondary
        0x0012) advances the pin EXACTLY once — on the primary — and both reads
        return the SAME generation's bytes (whole-cycle coherence)."""

        async def scenario():
            # Full anchor set recognised, but only 0x0028 is the primary.
            core, _helper, gate, ctx = self._make(anchors={0x0028, 0x0012}, max_wait_s=0.12)
            ctx._primary_anchor = 0x0028

            # Pin an initial frame (gen 3) so we can prove the primary advances it.
            gate.publish(3, _snapshot(3, length=0x0040))
            await asyncio.sleep(0)
            ctx._pinned_generation = 3
            ctx._pinned_snapshot = _snapshot(3, length=0x0040)

            # Primary-anchor read: must block for a strictly-newer generation.
            t_primary = asyncio.ensure_future(ctx.async_getValues(1, 3, 0x0028, 2))
            await asyncio.sleep(0)
            self.assertFalse(t_primary.done(), "primary-anchor read must wait for a newer frame")

            # A newer frame lands; the primary read pins it (the single advance).
            gate.publish(4, _snapshot(4, length=0x0040))
            primary = await asyncio.wait_for(t_primary, timeout=1.0)
            self.assertEqual(ctx._pinned_generation, 4, "primary anchor advances the pin once")
            self.assertEqual(primary, [4 * 100 + 0x0028, 4 * 100 + 0x0029])

            # A still-newer frame lands mid-cycle; the SECONDARY power anchor read
            # must NOT observe it — it is a follow-on that serves from the pin.
            gate.publish(5, _snapshot(5, length=0x0040))
            await asyncio.sleep(0)

            secondary = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0012, 2), timeout=0.01)
            self.assertEqual(ctx._pinned_generation, 4, "secondary power anchor must NOT advance the pin")
            self.assertEqual(
                secondary,
                [4 * 100 + 0x0012, 4 * 100 + 0x0013],
                "secondary power anchor must serve the SAME generation as the primary (coherent cycle)",
            )
            self.assertEqual(core.get_calls, [], "gated reads never touch the live core")

        asyncio.run(scenario())

    def test_secondary_and_non_anchor_reads_serve_from_pin_without_advance(self):
        """Secondary power + non-anchor reads serve immediately from the pin with
        NO wait and NO advance, even while newer generations are available."""

        async def scenario():
            core, _helper, gate, ctx = self._make(anchors={0x0028, 0x0012}, max_wait_s=0.12)
            ctx._primary_anchor = 0x0028

            # Pin gen 6; newer generations exist but must be ignored by follow-ons.
            # This client drives pacing via the primary anchor (cycle-start seen),
            # so its follow-on reads stay coherent and never take the pin-age
            # refresh — mark it as such.
            ctx._cycle_start_seen = True
            gate.publish(6, _snapshot(6, length=0x0200))
            await asyncio.sleep(0)
            ctx._pinned_generation = 6
            ctx._pinned_snapshot = _snapshot(6, length=0x0200)
            gate.publish(7, _snapshot(7, length=0x0200))
            await asyncio.sleep(0)

            # Secondary power anchor (0x0012): follow-on, serves from pin.
            secondary = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0012, 2), timeout=0.01)
            # Non-anchor block (0x0100): follow-on, serves from pin.
            energy = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0100, 4), timeout=0.01)

            self.assertEqual(secondary, [600 + 0x0012, 600 + 0x0013])
            self.assertEqual(energy, [600 + 0x0100, 600 + 0x0101, 600 + 0x0102, 600 + 0x0103])
            self.assertEqual(ctx._pinned_generation, 6, "no follow-on read may advance the pin")
            self.assertEqual(core.get_calls, [], "follow-on reads never touch the live core")

        asyncio.run(scenario())

    # -- Pin-age bound for non-anchor-only clients (COMMIT 2) ----------------

    def test_non_anchor_only_client_gets_bounded_refresh_as_generations_advance(self):
        """A client that NEVER reads the primary cycle-start anchor is not frozen
        on one frame: its follow-on reads refresh the pin to the LATEST
        already-published generation as upstream generations advance (bounded by
        availability, no wall-clock clamp), while still serving coherently within
        a single frame."""

        async def scenario():
            # Primary anchor is 0x0028; this client only ever reads 0x0000/0x0100.
            core, _helper, gate, ctx = self._make(anchors={0x0028, 0x0012}, max_wait_s=0.12)
            ctx._primary_anchor = 0x0028

            # First-ever (non-anchor) read pins the first available frame.
            gate.publish(1, _snapshot(1, length=0x0200))
            t_first = asyncio.ensure_future(ctx.async_getValues(1, 3, 0x0000, 2))
            await asyncio.sleep(0)
            r_first = await asyncio.wait_for(t_first, timeout=1.0)
            self.assertEqual(ctx._pinned_generation, 1)
            self.assertEqual(r_first, [100 + 0, 100 + 1])
            self.assertFalse(ctx._cycle_start_seen, "a non-anchor read must not mark the client primary-driven")

            # Upstream advances; a later non-anchor read must track the NEW
            # generation (pin-age bound), not stay frozen on gen 1.
            gate.publish(2, _snapshot(2, length=0x0200))
            await asyncio.sleep(0)
            r_second = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0000, 2), timeout=0.01)
            self.assertEqual(ctx._pinned_generation, 2, "non-anchor-only client must refresh to the latest generation")
            self.assertEqual(r_second, [200 + 0, 200 + 1])

            # Within a frame the client is still coherent: two blocks read with no
            # new generation between them come from the SAME (gen 2) frame.
            block_a = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0000, 2), timeout=0.01)
            block_b = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0100, 2), timeout=0.01)
            self.assertEqual(block_a, [200 + 0, 200 + 1])
            self.assertEqual(block_b, [200 + 0x0100, 200 + 0x0101])
            self.assertEqual(ctx._pinned_generation, 2, "no new generation -> pin unchanged within the frame")
            self.assertEqual(core.get_calls, [], "bounded refresh never touches the live core")

        asyncio.run(scenario())

    def test_primary_anchor_client_keeps_whole_cycle_coherence_despite_pin_age_bound(self):
        """The pin-age bound must NOT affect a client that drives pacing via the
        primary anchor: once a cycle-start read is seen, follow-on reads stay
        coherent with the pinned frame even when newer generations are available."""

        async def scenario():
            core, _helper, gate, ctx = self._make(anchors={0x0028, 0x0012}, max_wait_s=0.12)
            ctx._primary_anchor = 0x0028

            # Primary-anchor read pins gen 1 and marks the client primary-driven.
            gate.publish(1, _snapshot(1, length=0x0200))
            power = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0028, 2), timeout=1.0)
            self.assertEqual(ctx._pinned_generation, 1)
            self.assertTrue(ctx._cycle_start_seen)
            self.assertEqual(power, [100 + 0x0028, 100 + 0x0029])

            # Newer frames land mid-cycle; follow-on reads must NOT refresh.
            gate.publish(2, _snapshot(2, length=0x0200))
            gate.publish(3, _snapshot(3, length=0x0200))
            await asyncio.sleep(0)
            energy = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0100, 2), timeout=0.01)
            self.assertEqual(ctx._pinned_generation, 1, "primary-driven follow-on must not take the pin-age refresh")
            self.assertEqual(energy, [100 + 0x0100, 100 + 0x0101])
            self.assertEqual(core.get_calls, [])

        asyncio.run(scenario())

    # -- Unserved device-id delegation (COMMIT 3) ----------------------------

    def test_unserved_device_id_delegates_to_core_and_does_not_advance_pin(self):
        """A read for an unserved device_id delegates straight to the core (so a
        discovery scan gets the correct exception) and must NOT advance the pin
        or wait — even when it overlaps the primary anchor."""

        async def scenario():
            # Core serves ids {1, 7}; 9 is unserved. 0x0028 is the primary anchor.
            core, _helper, gate, ctx = self._make(anchors={0x0028}, max_wait_s=0.12)
            ctx._primary_anchor = 0x0028
            core.sentinel = ExcCodes.ILLEGAL_ADDRESS
            # A fresh frame exists and WOULD satisfy a served anchor read.
            gate.publish(5, _snapshot(5, length=0x0030))
            await asyncio.sleep(0)

            # Unserved id 9, anchor-overlapping window: must delegate, not pace.
            result = await asyncio.wait_for(ctx.async_getValues(9, 3, 0x0028, 2), timeout=0.05)
            self.assertIs(result, ExcCodes.ILLEGAL_ADDRESS, "unserved id must get the core's scan exception")
            self.assertEqual(core.get_calls, [(9, 3, 0x0028, 2)], "unserved id must be delegated to the core")
            self.assertEqual(ctx._pinned_generation, 0, "unserved id must not advance the pin")
            self.assertFalse(ctx._cycle_start_seen, "unserved id must not mark the client primary-driven")

        asyncio.run(scenario())

    def test_served_device_id_still_paces_after_unserved_scan(self):
        """A served device_id still paces normally (pins a fresh frame) even after
        unserved discovery-scan reads passed through — scan noise does not disturb
        the served client's pacing."""

        async def scenario():
            core, _helper, gate, ctx = self._make(anchors={0x0028}, max_wait_s=0.12)
            ctx._primary_anchor = 0x0028
            gate.publish(5, _snapshot(5, length=0x0030))
            await asyncio.sleep(0)

            # Discovery scan noise for unserved ids first.
            await asyncio.wait_for(ctx.async_getValues(8, 3, 0x0028, 2), timeout=0.05)
            await asyncio.wait_for(ctx.async_getValues(9, 3, 0x0028, 2), timeout=0.05)
            self.assertEqual(ctx._pinned_generation, 0, "scan noise must not advance the pin")

            # The served id (1) paces: its anchor read pins the fresh frame.
            core.get_calls.clear()
            result = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0028, 2), timeout=1.0)
            self.assertEqual(ctx._pinned_generation, 5, "served id must pace and pin a fresh frame")
            self.assertEqual(result, [500 + 0x0028, 500 + 0x0029])
            self.assertEqual(core.get_calls, [], "served gated read must not touch the live core")

        asyncio.run(scenario())

    # -- Hz read-rate counter pinned to primary-anchor pin advance (COMMIT 4) --

    def test_cycle_counter_increments_once_per_primary_anchor_pin_advance(self):
        """The cycle-start counter (and the on_cycle_advance sink) must tick EXACTLY
        once per primary cycle-start pin advance to a NEW generation — never on a
        follow-on/non-anchor read, never on a repeat of the same generation."""

        async def scenario():
            core = _FakeCore()
            helper = _FakePduHelper(circuit_open=False)
            gate = FrameGate()
            ticks: list[int] = []
            ctx = SerialPacingContext(
                core,
                gate,
                helper,
                0.12,
                reg_start_address=0,
                anchors={0x0028, 0x0012},
                primary_anchor=0x0028,
                on_cycle_advance=lambda: ticks.append(1),
            )

            # --- Cycle 1: primary anchor pins gen 1 -> exactly one tick ---------
            gate.publish(1, _snapshot(1, length=0x0200))
            await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0028, 2), timeout=1.0)
            self.assertEqual(ctx.cycle_start_advances, 1)
            self.assertEqual(len(ticks), 1)

            # Follow-on reads in the same cycle (secondary power anchor + energy)
            # must NOT tick, even though a newer generation is available.
            gate.publish(2, _snapshot(2, length=0x0200))
            await asyncio.sleep(0)
            await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0012, 2), timeout=0.01)
            await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0100, 2), timeout=0.01)
            self.assertEqual(ctx.cycle_start_advances, 1, "follow-on reads must not tick the cycle counter")
            self.assertEqual(len(ticks), 1)

            # --- Cycle 2: next primary anchor advances to gen 2 -> one more tick -
            await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0028, 2), timeout=1.0)
            self.assertEqual(ctx.cycle_start_advances, 2)
            self.assertEqual(len(ticks), 2)

            # A primary read with NO newer generation (jitter) must NOT tick.
            await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0028, 2), timeout=1.0)
            self.assertEqual(ctx.cycle_start_advances, 2, "same-generation re-read must not tick")
            self.assertEqual(len(ticks), 2)

        asyncio.run(scenario())

    def test_cycle_counter_not_bumped_by_unserved_scan_or_bit_reads(self):
        """Unserved device-id scans and bit reads delegate to the core and must
        NOT tick the cycle counter."""

        async def scenario():
            core, _helper, gate, ctx = self._make(anchors={0x0028}, max_wait_s=0.12)
            ctx._primary_anchor = 0x0028
            gate.publish(5, _snapshot(5, length=0x0030))
            await asyncio.sleep(0)

            # Unserved id scan at the anchor address.
            await asyncio.wait_for(ctx.async_getValues(9, 3, 0x0028, 2), timeout=0.05)
            # Bit read at the anchor address.
            await asyncio.wait_for(ctx.async_getValues(1, 1, 0x0028, 2), timeout=0.05)
            self.assertEqual(ctx.cycle_start_advances, 0, "scan/bit reads must not tick the cycle counter")

        asyncio.run(scenario())

    def test_paced_read_within_defined_range_still_served_from_snapshot(self):
        """A valid paced read (all addresses defined) is still served atomically
        from the pinned snapshot — the B2 fix must not force a live-core read for
        valid windows (which would break the atomic generation/snapshot guarantee)."""

        async def scenario():
            core, _helper, gate, ctx = self._make(anchors={0}, valid_addresses={0, 1, 2, 3, 6, 7})
            gate.publish(5, _snapshot(5, length=8))
            await asyncio.sleep(0)

            # Window [0, 4) is entirely within the defined range -> snapshot slice.
            result = await asyncio.wait_for(ctx.async_getValues(1, 3, 0, 4), timeout=0.2)
            self.assertEqual(result, [500, 501, 502, 503])
            self.assertEqual(core.get_calls, [], "valid paced read must stay atomic (no live-core read)")

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
