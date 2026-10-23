"""Unit tests for serial frame-latching (FrameGate + SerialPacingContext).

The gate is driven deterministically on a single test event loop: publish() and
the proxy coroutines all run on the same loop, matching the production topology
(the listener-thread publish is hopped onto the server loop via
call_soon_threadsafe, so by the time publish() executes it is on the server
loop — exactly what we simulate here by calling publish() directly on the loop).
"""

from __future__ import annotations

import asyncio
import unittest

from pymodbus.constants import ExcCodes

from app.utils.serial_pacing import FrameGate, SerialPacingContext


class _FakePduHelper:
    """Minimal stand-in exposing a settable circuit_open bool."""

    def __init__(self, circuit_open: bool = False) -> None:
        self.circuit_open = circuit_open


class _FakeCore:
    """Minimal SimCore stand-in recording calls and returning sentinels."""

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


class TestSerialPacing(unittest.TestCase):
    def _make(self, *, circuit_open=False, max_wait_s=0.12):
        core = _FakeCore()
        helper = _FakePduHelper(circuit_open=circuit_open)
        gate = FrameGate()
        ctx = SerialPacingContext(core, gate, helper, max_wait_s)
        return core, helper, gate, ctx

    def test_blocks_until_generation_advances_then_serves(self):
        async def scenario():
            core, _helper, gate, ctx = self._make()
            task = asyncio.ensure_future(ctx.async_getValues(1, 3, 0x0000, 2))
            # Let the task reach the gate wait.
            await asyncio.sleep(0)
            self.assertFalse(task.done(), "read must block until a fresh frame lands")
            self.assertEqual(core.get_calls, [], "datastore must not be read while blocked")

            # A fresh frame lands (on this same loop — mirrors call_soon_threadsafe).
            gate.publish(1)
            result = await asyncio.wait_for(task, timeout=1.0)

            self.assertEqual(result, core.sentinel)
            self.assertEqual(core.get_calls, [(1, 3, 0x0000, 2)])
            self.assertEqual(ctx._last_served_gen, 1)

        asyncio.run(scenario())

    def test_newer_generation_already_present_returns_immediately(self):
        async def scenario():
            core, _helper, gate, ctx = self._make()
            gate.publish(5)  # generation already ahead of last-served (0)
            await asyncio.sleep(0)  # let publish's notify settle
            result = await asyncio.wait_for(ctx.async_getValues(1, 3, 0x0000, 2), timeout=0.05)
            self.assertEqual(result, core.sentinel)
            self.assertEqual(ctx._last_served_gen, 5)

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
            core, _helper, _gate, ctx = self._make(max_wait_s=0.02)
            # No publish ever arrives → wait_for times out; breaker stays closed →
            # jitter fallback serves the current frame.
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

    def test_consecutive_reads_each_wait_for_a_new_generation(self):
        async def scenario():
            core, _helper, gate, ctx = self._make()
            # First read: block, publish gen 1, serve.
            t1 = asyncio.ensure_future(ctx.async_getValues(1, 3, 0, 1))
            await asyncio.sleep(0)
            gate.publish(1)
            await asyncio.wait_for(t1, timeout=1.0)
            self.assertEqual(ctx._last_served_gen, 1)

            # Second read with no new frame must block again (no duplicate serve).
            t2 = asyncio.ensure_future(ctx.async_getValues(1, 3, 0, 1))
            await asyncio.sleep(0)
            self.assertFalse(t2.done(), "second read must block until gen advances")
            gate.publish(2)
            await asyncio.wait_for(t2, timeout=1.0)
            self.assertEqual(ctx._last_served_gen, 2)
            self.assertEqual(len(core.get_calls), 2)

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
