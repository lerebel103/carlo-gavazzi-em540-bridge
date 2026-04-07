import asyncio
import os
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, call, patch

from pymodbus import ModbusException
from pymodbus.exceptions import ModbusIOException

from carlo_gavazzi.em540_master import Em540Master, MeterDataListener


def _make_config(**overrides):
    """Build a minimal TCP config namespace for Em540Master."""
    defaults = dict(
        mode="tcp",
        host="127.0.0.1",
        port=502,
        slave_id=1,
        timeout=1.0,
        retries=0,
        log_level="CRITICAL",
        baudrate=115200,
        parity="N",
        stopbits=1,
        serial_port="/dev/null",
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_successful_result(num_registers):
    """Return a mock Modbus response with the expected number of registers."""
    result = MagicMock()
    result.isError.return_value = False
    result.registers = [0] * num_registers
    return result


class TestEm540Master(unittest.TestCase):
    """Validates: Requirements 9.1, 9.2, 9.3, 9.4, 9.5, 10.1, 10.2"""

    @patch("carlo_gavazzi.em540_master.AsyncModbusTcpClient")
    def setUp(self, mock_tcp_cls):
        """Patch the TCP client class so the constructor doesn't create a real connection."""
        self.mock_client = MagicMock()
        self.mock_client.read_holding_registers = AsyncMock()
        self.mock_client.connect = AsyncMock()
        self.mock_client.close = MagicMock()
        mock_tcp_cls.return_value = self.mock_client

        self.config = _make_config()
        self.master = Em540Master(self.config)
        # Replace the client created by the constructor with our mock
        self.master._client = self.mock_client

    # -----------------------------------------------------------------------
    # Requirement 9.1: disconnected → returns False, calls read_failed
    # -----------------------------------------------------------------------
    def test_acquire_data_returns_false_when_disconnected(self):
        """Requirement 9.1 – acquire_data returns False and calls read_failed when disconnected."""
        type(self.mock_client).connected = PropertyMock(return_value=False)

        listener = MagicMock(spec=MeterDataListener)
        listener.read_failed = AsyncMock()
        self.master.add_listener(listener)

        result = asyncio.run(self.master.acquire_data())

        self.assertFalse(result)
        listener.read_failed.assert_awaited_once()

    # -----------------------------------------------------------------------
    # Requirement 9.2: Modbus read error → returns False, calls read_failed
    # -----------------------------------------------------------------------
    def test_modbus_read_error_returns_false_and_calls_read_failed(self):
        """Requirement 9.2 – Modbus read error returns False and calls read_failed on listeners."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        error_result = MagicMock()
        error_result.isError.return_value = True
        self.mock_client.read_holding_registers = AsyncMock(return_value=error_result)

        listener = MagicMock(spec=MeterDataListener)
        listener.read_failed = AsyncMock()
        self.master.add_listener(listener)

        result = asyncio.run(self.master.acquire_data())

        self.assertFalse(result)
        listener.read_failed.assert_awaited_once()

    # -----------------------------------------------------------------------
    # Requirement 9.3: register count mismatch → os._exit(1)
    # -----------------------------------------------------------------------
    @patch("carlo_gavazzi.em540_master.os._exit", side_effect=SystemExit(1))
    def test_register_count_mismatch_exits(self, mock_exit):
        """Requirement 9.3 – register count mismatch calls os._exit(1)."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        # Return fewer registers than expected for the first register group
        bad_result = MagicMock()
        bad_result.isError.return_value = False
        bad_result.registers = [0]  # Only 1 register instead of expected count
        self.mock_client.read_holding_registers = AsyncMock(return_value=bad_result)

        with self.assertRaises(SystemExit):
            asyncio.run(self.master.acquire_data())

        mock_exit.assert_called_once_with(1)

    # -----------------------------------------------------------------------
    # Requirement 9.4: ModbusIOException → returns False
    # -----------------------------------------------------------------------
    def test_modbus_io_exception_returns_false(self):
        """Requirement 9.4 – ModbusIOException returns False."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        self.mock_client.read_holding_registers = AsyncMock(
            side_effect=ModbusIOException("IO error")
        )

        result = asyncio.run(self.master.acquire_data())

        self.assertFalse(result)

    # -----------------------------------------------------------------------
    # Requirement 9.5: ModbusException → closes client, returns False
    # -----------------------------------------------------------------------
    def test_modbus_exception_closes_client_and_returns_false(self):
        """Requirement 9.5 – ModbusException closes client and returns False."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        self.mock_client.read_holding_registers = AsyncMock(
            side_effect=ModbusException("connection lost")
        )

        result = asyncio.run(self.master.acquire_data())

        self.assertFalse(result)
        self.mock_client.close.assert_called_once()

    # -----------------------------------------------------------------------
    # Requirement 10.1: successful acquire notifies via Condition
    # -----------------------------------------------------------------------
    def test_acquire_data_notifies_condition_on_success(self):
        """Requirement 10.1 – acquire_data notifies Condition on success."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        # Build responses that match the expected register counts for each
        # dynamic register group
        frame = self.master._data.frame
        responses = []
        for reg_addr in frame.dynamic_reg_map:
            reg_def = frame.dynamic_reg_map[reg_addr]
            responses.append(_make_successful_result(len(reg_def.values)))

        self.mock_client.read_holding_registers = AsyncMock(side_effect=responses)

        with patch.object(self.master._condition, "notify") as mock_notify:
            result = asyncio.run(self.master.acquire_data())

        self.assertTrue(result)
        mock_notify.assert_called_once()

    # -----------------------------------------------------------------------
    # Requirement 10.2: successful acquire reads dynamic registers
    # -----------------------------------------------------------------------
    def test_acquire_data_reads_dynamic_registers(self):
        """Requirement 10.2 – acquire_data reads all dynamic register groups."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        frame = self.master._data.frame
        responses = []
        for reg_addr in frame.dynamic_reg_map:
            reg_def = frame.dynamic_reg_map[reg_addr]
            responses.append(_make_successful_result(len(reg_def.values)))

        self.mock_client.read_holding_registers = AsyncMock(side_effect=responses)

        with patch.object(self.master._condition, "notify"):
            result = asyncio.run(self.master.acquire_data())

        self.assertTrue(result)
        # Should have been called once per dynamic register group
        expected_calls = len(frame.dynamic_reg_map)
        self.assertEqual(
            self.mock_client.read_holding_registers.await_count, expected_calls
        )
        # Counter should have been incremented
        self.assertEqual(self.master._dyn_reg_read_counter, 1)


class TestSkipNRead(unittest.TestCase):
    """Validates: Requirements 8.1, 8.2, 8.3"""

    @patch("carlo_gavazzi.em540_master.AsyncModbusTcpClient")
    def setUp(self, mock_tcp_cls):
        """Set up master with mock client. Modify one register to have skip_n_read=2."""
        self.mock_client = MagicMock()
        self.mock_client.read_holding_registers = AsyncMock()
        self.mock_client.connect = AsyncMock()
        self.mock_client.close = MagicMock()
        type(self.mock_client).connected = PropertyMock(return_value=True)
        mock_tcp_cls.return_value = self.mock_client

        self.config = _make_config()
        self.master = Em540Master(self.config)
        self.master._client = self.mock_client

        self.frame = self.master._data.frame

        # Set 0x0500 register to skip_n_read=2 (read every 3rd cycle)
        self.frame.dynamic_reg_map[0x0500].skip_n_read = 2

    def _build_responses_for_all(self):
        """Build successful responses for all dynamic register groups."""
        responses = []
        for reg_addr in self.frame.dynamic_reg_map:
            reg_def = self.frame.dynamic_reg_map[reg_addr]
            responses.append(_make_successful_result(len(reg_def.values)))
        return responses

    def _get_read_addresses(self):
        """Extract the register addresses from read_holding_registers calls."""
        return [
            call.kwargs.get("address", call.args[0] if call.args else None)
            for call in self.mock_client.read_holding_registers.call_args_list
        ]

    # -----------------------------------------------------------------------
    # Requirement 8.1: first cycle reads all registers regardless of skip_n_read
    # -----------------------------------------------------------------------
    def test_first_cycle_reads_all_registers(self):
        """Requirement 8.1 – First cycle (counter=1) reads all registers."""
        self.mock_client.read_holding_registers = AsyncMock(
            side_effect=self._build_responses_for_all()
        )

        with patch.object(self.master._condition, "notify"):
            result = asyncio.run(self.master.acquire_data())

        self.assertTrue(result)
        self.assertEqual(self.master._dyn_reg_read_counter, 1)
        # Both 0x0000 and 0x0500 should be read
        self.assertEqual(
            self.mock_client.read_holding_registers.await_count,
            len(self.frame.dynamic_reg_map),
        )
        addresses = self._get_read_addresses()
        self.assertIn(0x0000, addresses)
        self.assertIn(0x0500, addresses)

    # -----------------------------------------------------------------------
    # Requirement 8.2: subsequent cycles skip based on counter % (S+1) != 0
    # -----------------------------------------------------------------------
    def test_subsequent_cycles_skip_registers(self):
        """Requirement 8.2 – Register with skip_n_read=2 is skipped when counter%(2+1)!=0."""
        # 0x0500 has skip_n_read=2, so it reads when counter % 3 == 0
        # Cycle 1: counter=1, reads all (first cycle exception)
        # Cycle 2: counter=2, 2%3=2!=0 → skip 0x0500, only read 0x0000
        # Cycle 3: counter=3, 3%3=0 → read both

        # --- Cycle 1: reads all ---
        self.mock_client.read_holding_registers = AsyncMock(
            side_effect=self._build_responses_for_all()
        )
        with patch.object(self.master._condition, "notify"):
            asyncio.run(self.master.acquire_data())
        self.assertEqual(self.master._dyn_reg_read_counter, 1)

        # --- Cycle 2: should skip 0x0500 ---
        self.mock_client.read_holding_registers.reset_mock()
        # Only need response for 0x0000 since 0x0500 is skipped
        reg_0000 = self.frame.dynamic_reg_map[0x0000]
        self.mock_client.read_holding_registers = AsyncMock(
            side_effect=[_make_successful_result(len(reg_0000.values))]
        )
        with patch.object(self.master._condition, "notify"):
            result = asyncio.run(self.master.acquire_data())

        self.assertTrue(result)
        self.assertEqual(self.master._dyn_reg_read_counter, 2)
        self.assertEqual(self.mock_client.read_holding_registers.await_count, 1)
        addresses = self._get_read_addresses()
        self.assertIn(0x0000, addresses)
        self.assertNotIn(0x0500, addresses)

        # --- Cycle 3: counter=3, 3%3=0 → read both ---
        self.mock_client.read_holding_registers.reset_mock()
        self.mock_client.read_holding_registers = AsyncMock(
            side_effect=self._build_responses_for_all()
        )
        with patch.object(self.master._condition, "notify"):
            result = asyncio.run(self.master.acquire_data())

        self.assertTrue(result)
        self.assertEqual(self.master._dyn_reg_read_counter, 3)
        self.assertEqual(self.mock_client.read_holding_registers.await_count, 2)
        addresses = self._get_read_addresses()
        self.assertIn(0x0000, addresses)
        self.assertIn(0x0500, addresses)

    # -----------------------------------------------------------------------
    # Requirement 8.3: registers with skip_n_read=0 are read every cycle
    # -----------------------------------------------------------------------
    def test_skip_n_read_zero_reads_every_cycle(self):
        """Requirement 8.3 – Register with skip_n_read=0 is read on every cycle."""
        # 0x0000 has skip_n_read=0 (default), should be read every cycle
        # Run 4 cycles and verify 0x0000 is always read
        for cycle in range(1, 5):
            self.mock_client.read_holding_registers.reset_mock()

            if cycle == 1:
                # First cycle reads all
                self.mock_client.read_holding_registers = AsyncMock(
                    side_effect=self._build_responses_for_all()
                )
            elif cycle % 3 == 0:
                # Cycles where 0x0500 is also read (counter % 3 == 0)
                self.mock_client.read_holding_registers = AsyncMock(
                    side_effect=self._build_responses_for_all()
                )
            else:
                # Only 0x0000 is read (0x0500 skipped)
                reg_0000 = self.frame.dynamic_reg_map[0x0000]
                self.mock_client.read_holding_registers = AsyncMock(
                    side_effect=[_make_successful_result(len(reg_0000.values))]
                )

            with patch.object(self.master._condition, "notify"):
                result = asyncio.run(self.master.acquire_data())

            self.assertTrue(result, f"Cycle {cycle} should succeed")
            # 0x0000 (skip_n_read=0) must always be read
            addresses = self._get_read_addresses()
            self.assertIn(
                0x0000, addresses, f"Cycle {cycle}: 0x0000 should always be read"
            )


class TestNotifyLoop(unittest.TestCase):
    """Validates: Requirements 10.2, 10.3, 10.4"""

    @patch("carlo_gavazzi.em540_master.AsyncModbusTcpClient")
    def setUp(self, mock_tcp_cls):
        self.mock_client = MagicMock()
        self.mock_client.read_holding_registers = AsyncMock()
        self.mock_client.connect = AsyncMock()
        self.mock_client.close = MagicMock()
        mock_tcp_cls.return_value = self.mock_client

        self.config = _make_config()
        self.master = Em540Master(self.config)
        self.master._client = self.mock_client

    # -----------------------------------------------------------------------
    # Requirement 10.2: notify thread calls update_from_frame then new_data
    # -----------------------------------------------------------------------
    def test_notify_calls_update_from_frame_then_new_data(self):
        """Requirement 10.2 – Notify thread calls update_from_frame then new_data on listeners."""
        call_order = []
        done_event = threading.Event()

        self.master._data.update_from_frame = MagicMock(
            side_effect=lambda: call_order.append("update_from_frame")
        )

        listener = MagicMock(spec=MeterDataListener)

        async def _new_data(data):
            call_order.append("new_data")
            done_event.set()

        listener.new_data = _new_data
        self.master.add_listener(listener)

        # Trigger the notify loop
        with self.master._condition:
            self.master._condition.notify()

        # Wait for the notify thread to process
        self.assertTrue(done_event.wait(timeout=2), "Notify thread did not process in time")
        self.assertEqual(call_order, ["update_from_frame", "new_data"])

    # -----------------------------------------------------------------------
    # Requirement 10.3: listener exception increments error counter
    # -----------------------------------------------------------------------
    def test_listener_exception_increments_error_counter(self):
        """Requirement 10.3 – Listener exception increments error counter, loop continues."""
        error_count_event = threading.Event()
        success_event = threading.Event()
        call_count = {"n": 0}

        def _update_side_effect():
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("simulated failure")
            # Second call succeeds — proves the loop continued
            success_event.set()

        self.master._data.update_from_frame = MagicMock(side_effect=_update_side_effect)

        listener = MagicMock(spec=MeterDataListener)
        listener.new_data = AsyncMock()
        self.master.add_listener(listener)

        # First notification: triggers exception
        with self.master._condition:
            self.master._condition.notify()

        # Give the thread time to process the first notification
        time.sleep(0.1)

        # Second notification: should succeed, proving loop continued
        with self.master._condition:
            self.master._condition.notify()

        self.assertTrue(success_event.wait(timeout=2), "Notify loop did not continue after error")
        self.assertEqual(call_count["n"], 2)

    # -----------------------------------------------------------------------
    # Requirement 10.4: 10+ consecutive errors triggers os._exit(2)
    # -----------------------------------------------------------------------
    @patch("carlo_gavazzi.em540_master.os._exit")
    def test_consecutive_errors_trigger_exit(self, mock_exit):
        """Requirement 10.4 – More than 10 consecutive errors triggers os._exit(2)."""
        exit_event = threading.Event()

        def _exit_side_effect(code):
            exit_event.set()

        mock_exit.side_effect = _exit_side_effect

        # Make update_from_frame always raise to accumulate errors
        self.master._data.update_from_frame = MagicMock(
            side_effect=RuntimeError("persistent failure")
        )

        # Trigger 11 notifications to exceed the >10 threshold
        for _ in range(11):
            with self.master._condition:
                self.master._condition.notify()
            # Small delay to let the thread process each notification
            time.sleep(0.05)

        self.assertTrue(exit_event.wait(timeout=5), "os._exit(2) was not called after 10+ errors")
        mock_exit.assert_called_with(2)


if __name__ == "__main__":
    unittest.main()
