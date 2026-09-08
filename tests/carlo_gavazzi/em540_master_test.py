import asyncio
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

from pymodbus import ModbusException
from pymodbus.exceptions import ModbusIOException

from app.carlo_gavazzi.em540_master import Em540Master, MeterDataListener


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
        ensure_bidirectional_mode=False,
        ensure_3phase_measuring_system=False,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_successful_result(num_registers):
    """Return a mock Modbus response with the expected number of registers."""
    result = MagicMock()
    result.isError.return_value = False
    result.registers = [0] * num_registers
    return result


def _build_first_tick_responses(frame):
    """Build responses for one tick: primary block + full energy block."""
    from app.carlo_gavazzi.em540_data import ENERGY_BLOCK_TOTAL_SIZE

    primary_reg = frame.dynamic_reg_map[0x0000]
    return [
        _make_successful_result(len(primary_reg.values)),  # primary block
        _make_successful_result(ENERGY_BLOCK_TOTAL_SIZE),  # full energy block
    ]


class TestEm540Master(unittest.TestCase):
    """Validates: Requirements 9.1, 9.2, 9.3, 9.4, 9.5, 10.1, 10.2"""

    @patch("app.carlo_gavazzi.em540_master.AsyncModbusTcpClient")
    def setUp(self, mock_tcp_cls):
        """Patch the TCP client class so the constructor doesn't create a real connection."""
        self.mock_client = MagicMock()
        # Default: return a well-formed, request-sized successful response so the
        # connect-time config refresh (which reads a few registers on every
        # connect) has a valid awaitable result. Tests that need specific read
        # behaviour override this.
        self.mock_client.read_holding_registers = AsyncMock(
            side_effect=lambda address, count=1, device_id=None: _make_successful_result(count)
        )
        self.mock_client.connect = AsyncMock()
        self.mock_client.close = MagicMock()
        mock_tcp_cls.return_value = self.mock_client

        self.config = _make_config()
        self.master = Em540Master(self.config)
        # Replace the client created by the constructor with our mock
        self.master._client = self.mock_client
        # Bypass initial energy-read gate for tests not focused on energy chunking
        self.master._static_data_valid = True
        self.master._energy_initial_read_complete = True

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
        # close() is NOT called on read errors — pymodbus owns connection lifecycle
        self.mock_client.close.assert_not_called()
        listener.read_failed.assert_awaited_once()

    # -----------------------------------------------------------------------
    # Read-failure counter: every failure mode increments read_failed_total
    # -----------------------------------------------------------------------
    def test_disconnected_increments_read_failed_total(self):
        """A disconnected tick counts as a read failure."""
        type(self.mock_client).connected = PropertyMock(return_value=False)

        self.assertEqual(self.master._stats.read_failed_total, 0)
        asyncio.run(self.master.acquire_data())
        self.assertEqual(self.master._stats.read_failed_total, 1)

    def test_primary_error_response_increments_read_failed_total(self):
        """A primary block Modbus error response counts as a read failure."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        error_result = MagicMock()
        error_result.isError.return_value = True
        self.mock_client.read_holding_registers = AsyncMock(return_value=error_result)

        asyncio.run(self.master.acquire_data())
        self.assertEqual(self.master._stats.read_failed_total, 1)

    def test_primary_io_exception_increments_read_failed_total(self):
        """A primary block ModbusIOException counts as a read failure."""
        type(self.mock_client).connected = PropertyMock(return_value=True)
        self.mock_client.read_holding_registers = AsyncMock(side_effect=ModbusIOException("no response"))

        asyncio.run(self.master.acquire_data())
        self.assertEqual(self.master._stats.read_failed_total, 1)

    def test_energy_chunk_failure_increments_read_failed_total_without_aborting_tick(self):
        """An energy chunk read failure is counted but does NOT fail the tick.

        The primary block read succeeds and publishes; only the energy chunk fails.
        The read-failure counter must still increment, and listeners must NOT be
        told read_failed (which would flap their circuit breakers on a partial miss).
        """
        type(self.mock_client).connected = PropertyMock(return_value=True)

        primary_reg = self.master._back_data.frame.dynamic_reg_map[0x0000]
        primary_count = len(primary_reg.values)

        # First tick reads primary (success) then energy chunk 0 (error).
        good_primary = MagicMock()
        good_primary.isError.return_value = False
        good_primary.registers = [0] * primary_count

        energy_error = MagicMock()
        energy_error.isError.return_value = True

        self.mock_client.read_holding_registers = AsyncMock(side_effect=[good_primary, energy_error])

        listener = MagicMock(spec=MeterDataListener)
        listener.new_data = AsyncMock()
        listener.read_failed = AsyncMock()
        self.master.add_listener(listener)

        result = asyncio.run(self.master.acquire_data())

        # Primary data was read successfully, so the tick succeeds.
        self.assertTrue(result)
        # The energy chunk failure was still counted.
        self.assertEqual(self.master._stats.read_failed_total, 1)
        # Listeners are NOT told the read failed for an energy-only miss.
        listener.read_failed.assert_not_awaited()

    # -----------------------------------------------------------------------
    # Requirement 9.3: register count mismatch → os._exit(1)
    # -----------------------------------------------------------------------
    def test_register_count_mismatch_discards_and_returns_false(self):
        """A single register count mismatch discards the read and returns False without exiting."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        # Return fewer registers than expected for the primary block read
        bad_result = MagicMock()
        bad_result.isError.return_value = False
        bad_result.registers = [0]  # Only 1 register instead of expected count
        self.mock_client.read_holding_registers = AsyncMock(return_value=bad_result)

        result = asyncio.run(self.master.acquire_data())

        self.assertFalse(result)
        self.assertEqual(self.master._consecutive_reg_mismatch, 1)

    @patch("app.carlo_gavazzi.em540_master.os._exit", side_effect=SystemExit(1))
    def test_register_count_mismatch_exits_after_consecutive_threshold(self, mock_exit):
        """Persistent register count mismatches trigger os._exit(1) after threshold."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        bad_result = MagicMock()
        bad_result.isError.return_value = False
        bad_result.registers = [0]  # Wrong count
        self.mock_client.read_holding_registers = AsyncMock(return_value=bad_result)

        # Pre-set the counter to just below threshold
        self.master._consecutive_reg_mismatch = self.master._MAX_CONSECUTIVE_REG_MISMATCH - 1

        with self.assertRaises(SystemExit):
            asyncio.run(self.master.acquire_data())

        mock_exit.assert_called_once_with(1)

    def test_register_count_mismatch_resets_on_successful_read(self):
        """A successful read resets the consecutive mismatch counter."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        # Simulate prior mismatches
        self.master._consecutive_reg_mismatch = 5

        # Return correct responses for both primary and energy chunk reads
        def side_effect(addr, count, device_id=None):
            result = MagicMock()
            result.isError.return_value = False
            result.registers = [0] * count
            return result

        self.mock_client.read_holding_registers = AsyncMock(side_effect=side_effect)

        asyncio.run(self.master.acquire_data())

        self.assertEqual(self.master._consecutive_reg_mismatch, 0)

    # -----------------------------------------------------------------------
    # Requirement 9.4: ModbusIOException → returns False
    # -----------------------------------------------------------------------
    def test_modbus_io_exception_returns_false(self):
        """Requirement 9.4 – ModbusIOException returns False."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        self.mock_client.read_holding_registers = AsyncMock(side_effect=ModbusIOException("IO error"))

        result = asyncio.run(self.master.acquire_data())

        self.assertFalse(result)

    # -----------------------------------------------------------------------
    # Requirement 9.5: ModbusException → returns False (no close)
    # -----------------------------------------------------------------------
    def test_modbus_exception_returns_false_without_closing(self):
        """Requirement 9.5 – ModbusException returns False without closing client."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        self.mock_client.read_holding_registers = AsyncMock(side_effect=ModbusException("connection lost"))

        result = asyncio.run(self.master.acquire_data())

        self.assertFalse(result)
        # close() is NOT called — pymodbus owns connection lifecycle via timeouts
        self.mock_client.close.assert_not_called()

    # -----------------------------------------------------------------------
    # Requirement 10.1: successful acquire notifies via Condition
    # -----------------------------------------------------------------------
    def test_acquire_data_notifies_condition_on_success(self):
        """Requirement 10.1 – acquire_data notifies Condition on success."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        # One tick: primary block + full energy block
        frame = self.master.data.frame
        responses = _build_first_tick_responses(frame)
        self.mock_client.read_holding_registers = AsyncMock(side_effect=responses)

        with patch.object(self.master._condition, "notify") as mock_notify:
            result = asyncio.run(self.master.acquire_data())

        self.assertTrue(result)
        mock_notify.assert_called_once()

    def test_acquire_data_updates_last_frame_monotonic_on_success(self):
        """A successful acquire records a monotonic timestamp for the freshness watchdog."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        # No frame yet -> monotonic stamp starts at 0.0.
        self.assertEqual(self.master.last_frame_monotonic, 0.0)

        frame = self.master.data.frame
        responses = _build_first_tick_responses(frame)
        self.mock_client.read_holding_registers = AsyncMock(side_effect=responses)

        with patch("app.carlo_gavazzi.em540_master.time.monotonic", return_value=1234.5):
            with patch.object(self.master._condition, "notify"):
                result = asyncio.run(self.master.acquire_data())

        self.assertTrue(result)
        self.assertEqual(self.master.last_frame_monotonic, 1234.5)

    def test_acquire_data_does_not_update_last_frame_monotonic_on_failure(self):
        """A failed primary read must not advance the freshness watchdog timestamp."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        error = MagicMock()
        error.isError.return_value = True
        self.mock_client.read_holding_registers = AsyncMock(return_value=error)

        with patch.object(self.master._condition, "notify"):
            result = asyncio.run(self.master.acquire_data())

        self.assertFalse(result)
        self.assertEqual(self.master.last_frame_monotonic, 0.0)

    # -----------------------------------------------------------------------
    # Requirement 10.2: successful acquire reads dynamic registers
    # -----------------------------------------------------------------------
    def test_acquire_data_reads_dynamic_registers(self):
        """Requirement 10.2 – acquire_data reads primary + full energy block each tick."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        frame = self.master.data.frame
        responses = _build_first_tick_responses(frame)
        self.mock_client.read_holding_registers = AsyncMock(side_effect=responses)

        with patch.object(self.master._condition, "notify"):
            result = asyncio.run(self.master.acquire_data())

        self.assertTrue(result)
        # One tick reads primary + full energy block = 2 calls
        self.assertEqual(self.mock_client.read_holding_registers.await_count, 2)
        # Counter should have been incremented
        self.assertEqual(self.master._dyn_reg_read_counter, 1)

    def test_connect_failure_does_not_raise_and_closes_client(self):
        """Transport connect failures should not escape connect() and should close the client."""
        self.mock_client.connect = AsyncMock(side_effect=RuntimeError("dial failed"))

        asyncio.run(self.master.connect())

        self.mock_client.close.assert_called_once()

    def test_connect_failure_records_failure_time_after_connect_attempt(self):
        """Failure timestamps should be captured after the awaited connect attempt completes."""
        import app.carlo_gavazzi.em540_master as _master_mod

        async def _connect():
            _master_mod.time.perf_counter()
            raise RuntimeError("dial failed")

        self.mock_client.connect = AsyncMock(side_effect=_connect)

        with (
            patch.object(self.master, "_record_connect_failure") as mock_record_failure,
            patch("app.carlo_gavazzi.em540_master.time.perf_counter", side_effect=[1.0, 4.5]),
        ):
            asyncio.run(self.master.connect())

        mock_record_failure.assert_called_once_with(4.5)

    def test_connect_success_logs_current_outage_duration(self):
        """Recovery logging should use the current time after connect completes."""
        import app.carlo_gavazzi.em540_master as _master_mod

        self.master._consecutive_connect_failures = 2
        self.master._first_failure_time = 1.0
        self.master._static_data_valid = True
        type(self.mock_client).connected = PropertyMock(return_value=True)

        async def _connect():
            _master_mod.time.perf_counter()

        self.mock_client.connect = AsyncMock(side_effect=_connect)

        with (
            patch("app.carlo_gavazzi.em540_master.logger.info") as mock_logger_info,
            patch("app.carlo_gavazzi.em540_master.time.perf_counter", side_effect=[2.0, 5.5]),
        ):
            asyncio.run(self.master.connect())

        mock_logger_info.assert_any_call(
            "Connected to EM540 after %.1fs (%d failed attempt%s).",
            4.5,
            2,
            "s",
        )

    def test_static_read_is_retried_until_success(self):
        """Static register reads should retry on later connects until they succeed."""
        # Reset static data valid to False to test startup behavior
        self.master._static_data_valid = False

        type(self.mock_client).connected = PropertyMock(return_value=True)

        # First read (first connect) fails; every subsequent read succeeds with a
        # response sized to the request. Using a function keeps the test robust to
        # the extra config-register refresh reads performed after the static read.
        state = {"first": True}

        async def _read(address, count=1, device_id=None):
            if state["first"]:
                state["first"] = False
                result = MagicMock()
                result.isError.return_value = True
                return result
            return _make_successful_result(count)

        self.mock_client.read_holding_registers = AsyncMock(side_effect=_read)

        asyncio.run(self.master.connect())
        self.assertFalse(self.master._static_data_valid)

        asyncio.run(self.master.connect())
        self.assertTrue(self.master._static_data_valid)

    def test_timing_stats_notifies_listeners_each_cycle(self):
        """Timing stats should be pushed every cycle for diagnostics consumers."""
        observed = []

        def _on_stats(stats):
            observed.append(
                (
                    stats.acquisition_duration_ms_min,
                    stats.acquisition_duration_ms_max,
                    stats.acquisition_duration_samples,
                )
            )

        self.master.add_stats_listener(_on_stats)

        cycle_start = time.perf_counter() - 0.02
        self.master._update_timing_stats(
            cycle_start=cycle_start,
        )

        self.assertTrue(observed)
        dur_min, dur_max, sample_count = observed[-1]
        self.assertGreaterEqual(dur_min, 0.0)
        self.assertEqual(dur_min, dur_max)
        self.assertEqual(sample_count, 1)

    def test_tick_overrun_not_counted_when_headroom_remains(self):
        """A cycle that still leaves time before the next tick must not count as an overrun."""
        self.config.update_interval = 0.1  # 100ms budget

        # 70ms cycle: headroom remains before the following tick.
        cycle_start = time.perf_counter() - 0.070
        self.master._update_timing_stats(
            cycle_start=cycle_start,
        )

        self.assertEqual(self.master._stats.tick_overrun_count, 0)

    def test_tick_overrun_counts_when_beyond_margin(self):
        """A cycle that misses by more than the jitter margin must count as an overrun."""
        self.config.update_interval = 0.1  # 100ms budget; 50ms jitter margin

        # 180ms cycle: signed headroom ~= -80ms, beyond the 50ms margin.
        cycle_start = time.perf_counter() - 0.180
        self.master._update_timing_stats(
            cycle_start=cycle_start,
        )

        self.assertEqual(self.master._stats.tick_overrun_count, 1)

    def test_tick_headroom_can_be_negative_when_cycle_finishes_late(self):
        """Signed headroom should be negative when the cycle misses the immediate next boundary."""
        now = time.perf_counter()
        cycle_start = now - 0.120
        tick_deadline = now - 0.200

        self.master._update_timing_stats(
            cycle_start=cycle_start,
            tick_deadline_mono=tick_deadline,
            tick_interval_s=0.1,
        )

        self.assertLess(self.master._stats.acquisition_headroom_ms_min, 0.0)

    def test_refresh_client_runtime_config_uses_live_shared_config_values(self):
        self.mock_client.timeout = 1.0
        self.mock_client.retries = 0

        self.config.timeout = 0.25
        self.config.retries = 3
        self.master._refresh_client_runtime_config()

        self.assertEqual(self.mock_client.timeout, 0.25)
        self.assertEqual(self.mock_client.retries, 3)


class TestSkipNRead(unittest.TestCase):
    """Validates temporary baseline mode with full dynamic reads every tick."""

    @patch("app.carlo_gavazzi.em540_master.AsyncModbusTcpClient")
    def setUp(self, mock_tcp_cls):
        """Set up master with mock client."""
        self.mock_client = MagicMock()
        self.mock_client.read_holding_registers = AsyncMock()
        self.mock_client.connect = AsyncMock()
        self.mock_client.close = MagicMock()
        type(self.mock_client).connected = PropertyMock(return_value=True)
        mock_tcp_cls.return_value = self.mock_client

        self.config = _make_config()
        self.master = Em540Master(self.config)
        self.master._client = self.mock_client
        # Bypass initial startup gates for testing read mechanics
        self.master._static_data_valid = True
        self.master._energy_initial_read_complete = True

        self.frame = self.master.data.frame

    def _get_read_addresses(self):
        """Extract the register addresses from read_holding_registers calls."""
        return [
            call.kwargs.get("address", call.args[0] if call.args else None)
            for call in self.mock_client.read_holding_registers.call_args_list
        ]

    # -----------------------------------------------------------------------
    # First tick reads primary + full energy block
    # -----------------------------------------------------------------------
    def test_first_tick_reads_primary_and_full_energy_block(self):
        """First tick reads primary block and full energy block."""
        responses = _build_first_tick_responses(self.frame)
        self.mock_client.read_holding_registers = AsyncMock(side_effect=responses)

        with patch.object(self.master._condition, "notify"):
            result = asyncio.run(self.master.acquire_data())

        self.assertTrue(result)
        self.assertEqual(self.mock_client.read_holding_registers.await_count, 2)
        addresses = self._get_read_addresses()
        self.assertIn(0x0000, addresses)
        self.assertIn(0x0500, addresses)

    # -----------------------------------------------------------------------
    # Dynamic blocks are always read on every cycle
    # -----------------------------------------------------------------------
    def test_dynamic_blocks_read_every_cycle(self):
        """Primary and energy blocks are both read on every cycle."""
        for cycle in range(1, 6):
            self.mock_client.read_holding_registers.reset_mock()
            self.mock_client.read_holding_registers = AsyncMock(side_effect=_build_first_tick_responses(self.frame))
            with patch.object(self.master._condition, "notify"):
                result = asyncio.run(self.master.acquire_data())

            self.assertTrue(result, f"Cycle {cycle} should succeed")
            addresses = self._get_read_addresses()
            self.assertIn(0x0000, addresses, f"Cycle {cycle}: 0x0000 should always be read")
            self.assertIn(0x0500, addresses, f"Cycle {cycle}: 0x0500 should always be read")

    # -----------------------------------------------------------------------
    # Startup gate: energy_initial_read_complete opens after first full read
    # -----------------------------------------------------------------------
    def test_energy_initial_read_complete_tracks_full_cycle(self):
        """_energy_initial_read_complete becomes True after first successful full energy read."""
        # Reset the gate to False (simulating fresh startup)
        self.master._energy_initial_read_complete = False
        self.master._dyn_reg_read_counter = 0

        initial_seq = self.master._data_seq

        # Tick 1: primary + full energy block
        self.mock_client.read_holding_registers = AsyncMock(side_effect=_build_first_tick_responses(self.frame))
        asyncio.run(self.master.acquire_data())

        # Gate should now be open
        self.assertTrue(self.master._energy_initial_read_complete)
        self.assertGreater(self.master._data_seq, initial_seq)

    # -----------------------------------------------------------------------
    # Energy values are written correctly into the register map
    # -----------------------------------------------------------------------
    def test_energy_values_written_to_register_map(self):
        """Full energy block values are correctly stored in the register map."""
        from app.carlo_gavazzi.em540_data import ENERGY_BLOCK_TOTAL_SIZE

        primary_reg = self.frame.dynamic_reg_map[0x0000]
        primary_result = _make_successful_result(len(primary_reg.values))

        energy_values = list(range(300, 300 + ENERGY_BLOCK_TOTAL_SIZE))
        energy_result = MagicMock()
        energy_result.isError.return_value = False
        energy_result.registers = energy_values

        self.mock_client.read_holding_registers = AsyncMock(side_effect=[primary_result, energy_result])
        with patch("app.carlo_gavazzi.meter_data.MeterData.update_from_frame", return_value=None):
            with patch.object(self.master._condition, "notify"):
                result = asyncio.run(self.master.acquire_data())

        self.assertTrue(result)
        stored = self.master.data.frame.dynamic_reg_map[0x0500].values
        self.assertEqual(stored, energy_values)

    # -----------------------------------------------------------------------
    # Full energy read failure does not abort the tick (primary still publishes)
    # -----------------------------------------------------------------------
    def test_energy_read_failure_does_not_abort_tick(self):
        """If the full energy read fails, primary data still publishes."""
        primary_reg = self.frame.dynamic_reg_map[0x0000]
        primary_result = _make_successful_result(len(primary_reg.values))

        # Energy read returns an error
        energy_error = MagicMock()
        energy_error.isError.return_value = True

        self.mock_client.read_holding_registers = AsyncMock(side_effect=[primary_result, energy_error])

        with patch("app.carlo_gavazzi.meter_data.MeterData.update_from_frame", return_value=None):
            with patch.object(self.master._condition, "notify"):
                result = asyncio.run(self.master.acquire_data())

        # Tick still succeeds (primary was OK)
        self.assertTrue(result)


class TestListenerWorker(unittest.TestCase):
    """Validates listener workers and consumer missed-update diagnostics."""

    @patch("app.carlo_gavazzi.em540_master.AsyncModbusTcpClient")
    def setUp(self, mock_tcp_cls):
        self.mock_client = MagicMock()
        self.mock_client.read_holding_registers = AsyncMock()
        self.mock_client.connect = AsyncMock()
        self.mock_client.close = MagicMock()
        type(self.mock_client).connected = PropertyMock(return_value=True)
        mock_tcp_cls.return_value = self.mock_client

        self.config = _make_config()
        self.master = Em540Master(self.config)
        self.master._client = self.mock_client
        # Bypass initial startup gates for listener tests
        self.master._static_data_valid = True
        self.master._energy_initial_read_complete = True

    def test_listener_worker_receives_latest_snapshot(self):
        """Listener worker should process new snapshots from successful acquisitions."""
        frame = self.master.data.frame
        self.mock_client.read_holding_registers = AsyncMock(side_effect=_build_first_tick_responses(frame))

        done_event = threading.Event()

        listener = MagicMock(spec=MeterDataListener)

        async def _new_data(_data):
            done_event.set()

        listener.new_data = _new_data
        listener.read_failed = AsyncMock()
        self.master.add_listener(listener)

        result = asyncio.run(self.master.acquire_data())
        self.assertTrue(result)
        self.assertTrue(done_event.wait(timeout=2), "Listener worker did not process data")

    def test_missed_update_stats_increment_for_slow_consumer(self):
        """Slow consumers should increment missed-update metrics when sequence jumps occur."""
        frame = self.master.data.frame

        responses = _build_first_tick_responses(frame) * 3
        self.mock_client.read_holding_registers = AsyncMock(side_effect=responses)

        stats_updates = []
        stats_event = threading.Event()

        def _on_stats(stats):
            stats_updates.append((stats.consumer_missed_updates_total, stats.consumer_max_seq_gap))
            stats_event.set()

        self.master.add_stats_listener(_on_stats)

        listener = MagicMock(spec=MeterDataListener)

        async def _slow_new_data(_data):
            await asyncio.sleep(0.2)

        listener.new_data = _slow_new_data
        listener.read_failed = AsyncMock()
        self.master.add_listener(listener)

        with patch("app.carlo_gavazzi.meter_data.MeterData.update_from_frame", return_value=None):
            self.assertTrue(asyncio.run(self.master.acquire_data()))
            self.assertTrue(asyncio.run(self.master.acquire_data()))
            self.assertTrue(asyncio.run(self.master.acquire_data()))

        self.assertTrue(stats_event.wait(timeout=2), "Expected stats callback")

        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            if any(missed_total >= 1 and max_gap >= 2 for missed_total, max_gap in stats_updates):
                break
            time.sleep(0.01)

        self.assertTrue(
            any(missed_total >= 1 and max_gap >= 2 for missed_total, max_gap in stats_updates),
            f"Expected missed update stats, got snapshots={stats_updates}",
        )


class TestEm540MasterMeterConfig(unittest.TestCase):
    """Meter-config read/log/write on connect (measurement mode, measuring system,
    wiring check) and the best-effort corrective write path."""

    @patch("app.carlo_gavazzi.em540_master.AsyncModbusTcpClient")
    def _build(self, mock_tcp_cls, **config_overrides):
        mock_client = MagicMock()
        mock_client.write_register = AsyncMock()
        mock_tcp_cls.return_value = mock_client
        master = Em540Master(_make_config(**config_overrides))
        master._client = mock_client

        # The refresh-on-connect and post-write read-back both call
        # read_holding_registers. Echo back the current static-map values for the
        # requested address so reads reflect whatever the test seeded / a write set.
        async def _echo_read(address, count=1, device_id=None):
            values = master._front_data.frame.static_reg_map[address].values
            result = MagicMock()
            result.isError.return_value = False
            result.registers = list(values[:count])
            return result

        mock_client.read_holding_registers = AsyncMock(side_effect=_echo_read)
        return master, mock_client

    @staticmethod
    def _seed_registers(master, *, mode, system, wrong):
        from app.carlo_gavazzi.em540_data import (
            REG_MEASUREMENT_MODE,
            REG_MEASURING_SYSTEM,
            REG_WRONG_CONNECTION_BLOCK,
        )

        # Seed BOTH buffers so refresh mirroring and read-back stay consistent.
        for data in (master._front_data, master._back_data):
            static = data.frame.static_reg_map
            static[REG_MEASUREMENT_MODE].values = [mode]
            static[REG_MEASURING_SYSTEM].values = [system]
            # Block is [enable, status]; status is the second word (0x1105).
            static[REG_WRONG_CONNECTION_BLOCK].values = [1, wrong]

    def _ok_write(self):
        result = MagicMock()
        result.isError.return_value = False
        return result

    def _err_write(self):
        result = MagicMock()
        result.isError.return_value = True
        return result

    @staticmethod
    def _accepting_write(master):
        """A write mock that applies the value to both buffers' static maps,
        simulating a meter that accepts the write (so read-back observes it)."""

        async def _write(address, value, device_id=None):
            for data in (master._front_data, master._back_data):
                data.frame.static_reg_map[address].values = [value]
            result = MagicMock()
            result.isError.return_value = False
            return result

        return AsyncMock(side_effect=_write)

    # --- read + cache + log -------------------------------------------------
    def test_reads_and_caches_all_three_registers(self):
        master, client = self._build()
        self._seed_registers(master, mode=2, system=0, wrong=0)

        asyncio.run(master._apply_meter_config(refresh_from_device=True))

        self.assertEqual(
            master.meter_config,
            {"measurement_mode": 2, "measuring_system": 0, "wrong_connection": 0},
        )
        client.write_register.assert_not_awaited()

    def test_wrong_connection_status_surfaced(self):
        master, _ = self._build()
        self._seed_registers(master, mode=2, system=0, wrong=1)

        asyncio.run(master._apply_meter_config(refresh_from_device=True))

        self.assertEqual(master.meter_config["wrong_connection"], 1)

    def test_reconnect_refreshes_stale_config_from_meter(self):
        """On reconnect the static read is skipped, so _apply_meter_config must
        re-read the config registers rather than trust the stale cache."""
        from app.carlo_gavazzi.em540_data import REG_WRONG_CONNECTION_BLOCK

        master, client = self._build()
        # Cached (stale) snapshot says wiring is fine.
        self._seed_registers(master, mode=2, system=0, wrong=0)
        asyncio.run(master._apply_meter_config(refresh_from_device=True))
        self.assertEqual(master.meter_config["wrong_connection"], 0)

        # Meter now reports a wiring error; the refresh read must observe it even
        # though _static_data_valid stayed True across the reconnect.
        for data in (master._front_data, master._back_data):
            data.frame.static_reg_map[REG_WRONG_CONNECTION_BLOCK].values = [1, 1]

        asyncio.run(master._apply_meter_config(refresh_from_device=True))
        self.assertEqual(master.meter_config["wrong_connection"], 1)

    def test_config_refresh_failure_falls_back_to_cache(self):
        """A failed refresh read is non-fatal: cached values are logged/used."""
        master, client = self._build()
        self._seed_registers(master, mode=2, system=0, wrong=0)

        error = MagicMock()
        error.isError.return_value = True
        client.read_holding_registers = AsyncMock(return_value=error)

        # Must not raise; cache retains the seeded values.
        asyncio.run(master._apply_meter_config(refresh_from_device=True))
        self.assertEqual(master.meter_config["measurement_mode"], 2)
        self.assertEqual(master.meter_config["wrong_connection"], 0)

    def test_config_refresh_is_atomic_on_partial_failure(self):
        """If a later register read fails mid-refresh, NO register is updated
        (no mixed old/new snapshot); the buffers keep their prior values."""
        from app.carlo_gavazzi.em540_data import (
            REG_MEASUREMENT_MODE,
            REG_MEASURING_SYSTEM,
        )

        # Flags off: this test isolates refresh atomicity, not the write path.
        master, client = self._build()
        self._seed_registers(master, mode=0, system=0, wrong=0)

        # First read (measurement mode) returns a NEW value; a later read fails.
        calls = {"n": 0}

        async def _read(address, count=1, device_id=None):
            calls["n"] += 1
            result = MagicMock()
            if calls["n"] == 1:
                result.isError.return_value = False
                result.registers = [2]  # would-be new mode
            else:
                result.isError.return_value = True
                result.registers = []
            return result

        client.read_holding_registers = AsyncMock(side_effect=_read)

        asyncio.run(master._apply_meter_config(refresh_from_device=True))

        # Refresh aborted: the successfully-read first register was NOT committed.
        self.assertEqual(master._front_data.frame.static_reg_map[REG_MEASUREMENT_MODE].values, [0])
        self.assertEqual(master._back_data.frame.static_reg_map[REG_MEASUREMENT_MODE].values, [0])
        self.assertEqual(master._front_data.frame.static_reg_map[REG_MEASURING_SYSTEM].values, [0])

    def test_first_connect_does_not_refresh(self):
        """On first connect the static read already populated the registers, so
        _apply_meter_config performs no extra reads."""
        master, client = self._build()
        self._seed_registers(master, mode=2, system=0, wrong=0)

        asyncio.run(master._apply_meter_config(refresh_from_device=False))

        client.read_holding_registers.assert_not_awaited()
        self.assertEqual(master.meter_config["measurement_mode"], 2)

    def test_failed_reconnect_refresh_skips_corrective_writes(self):
        """If the reconnect refresh read fails, no corrective write is attempted
        even when a flag is on and the STALE value differs from the target —
        write decisions must be based on a value confirmed this connect."""
        master, client = self._build(ensure_bidirectional_mode=True, ensure_3phase_measuring_system=True)
        # Stale cache differs from both targets (would trigger writes if trusted).
        self._seed_registers(master, mode=0, system=2, wrong=0)

        error = MagicMock()
        error.isError.return_value = True
        client.read_holding_registers = AsyncMock(return_value=error)

        asyncio.run(master._apply_meter_config(refresh_from_device=True))

        # Refresh failed -> no write issued off stale data.
        client.write_register.assert_not_awaited()

    def test_readback_failure_keeps_last_known_value(self):
        """A successful write whose read-back fails must NOT cache the optimistic
        requested value; the last known value is kept (unverified)."""
        from app.carlo_gavazzi.em540_data import REG_MEASUREMENT_MODE

        master, client = self._build(ensure_bidirectional_mode=True)
        self._seed_registers(master, mode=0, system=0, wrong=0)

        # Write ACKs successfully; every read (refresh + read-back) fails.
        error = MagicMock()
        error.isError.return_value = True
        client.read_holding_registers = AsyncMock(return_value=error)
        ok = MagicMock()
        ok.isError.return_value = False
        client.write_register = AsyncMock(return_value=ok)

        # Refresh fails first, which already short-circuits writes; to exercise
        # the read-back path specifically, call _apply_meter_config on first
        # connect (no refresh) so the write path runs, then fail the read-back.
        asyncio.run(master._apply_meter_config(refresh_from_device=False))

        # Write was attempted, read-back failed -> cache keeps prior value (0).
        client.write_register.assert_awaited_once()
        self.assertEqual(master.meter_config["measurement_mode"], 0)
        self.assertEqual(master._front_data.frame.static_reg_map[REG_MEASUREMENT_MODE].values, [0])

    # --- flags off: never write --------------------------------------------
    def test_no_write_when_flags_disabled_even_if_values_differ(self):
        master, client = self._build(ensure_bidirectional_mode=False, ensure_3phase_measuring_system=False)
        self._seed_registers(master, mode=0, system=2, wrong=0)

        asyncio.run(master._apply_meter_config(refresh_from_device=True))

        client.write_register.assert_not_awaited()

    # --- flag on, value already correct: no write --------------------------
    def test_no_write_when_mode_already_correct(self):
        master, client = self._build(ensure_bidirectional_mode=True)
        self._seed_registers(master, mode=2, system=0, wrong=0)

        asyncio.run(master._apply_meter_config(refresh_from_device=True))

        client.write_register.assert_not_awaited()

    # --- flag on, value wrong: write + cache/refresh -----------------------
    def test_writes_mode_when_flag_on_and_value_wrong(self):
        master, client = self._build(ensure_bidirectional_mode=True)
        self._seed_registers(master, mode=0, system=0, wrong=0)
        client.write_register = self._accepting_write(master)

        asyncio.run(master._apply_meter_config(refresh_from_device=True))

        from app.carlo_gavazzi.em540_data import REG_MEASUREMENT_MODE

        client.write_register.assert_awaited_once_with(REG_MEASUREMENT_MODE, 2, device_id=1)
        self.assertEqual(master.meter_config["measurement_mode"], 2)
        # Read-back value propagated to BOTH buffers so a tick swap can't revert it.
        self.assertEqual(master._front_data.frame.static_reg_map[REG_MEASUREMENT_MODE].values, [2])
        self.assertEqual(master._back_data.frame.static_reg_map[REG_MEASUREMENT_MODE].values, [2])

    def test_writes_measuring_system_when_flag_on_and_value_wrong(self):
        master, client = self._build(ensure_3phase_measuring_system=True)
        self._seed_registers(master, mode=2, system=2, wrong=0)
        client.write_register = self._accepting_write(master)

        asyncio.run(master._apply_meter_config(refresh_from_device=True))

        from app.carlo_gavazzi.em540_data import REG_MEASURING_SYSTEM

        client.write_register.assert_awaited_once_with(REG_MEASURING_SYSTEM, 0, device_id=1)
        self.assertEqual(master.meter_config["measuring_system"], 0)
        self.assertEqual(master._back_data.frame.static_reg_map[REG_MEASURING_SYSTEM].values, [0])

    def test_write_accepted_but_meter_normalizes_value_caches_actual(self):
        """If the meter ignores/normalizes a write, the read-back value (not the
        requested one) is cached and published."""
        master, client = self._build(ensure_bidirectional_mode=True)
        self._seed_registers(master, mode=0, system=0, wrong=0)

        from app.carlo_gavazzi.em540_data import REG_MEASUREMENT_MODE

        # Write "succeeds" at the transport level but the meter keeps its own
        # value (e.g. read-only MID model that ACKs but does not change).
        async def _write_ignored(address, value, device_id=None):
            result = MagicMock()
            result.isError.return_value = False
            return result

        client.write_register = AsyncMock(side_effect=_write_ignored)

        asyncio.run(master._apply_meter_config(refresh_from_device=True))

        # Read-back sees the unchanged meter value (0), so the cache reflects
        # reality, not the optimistic requested 2.
        self.assertEqual(master.meter_config["measurement_mode"], 0)
        self.assertEqual(master._front_data.frame.static_reg_map[REG_MEASUREMENT_MODE].values, [0])

    # --- MID read-only meter: rejected write is non-fatal, cache unchanged --
    def test_rejected_write_is_non_fatal_and_leaves_cache_unchanged(self):
        master, client = self._build(ensure_bidirectional_mode=True)
        client.write_register.return_value = self._err_write()
        self._seed_registers(master, mode=1, system=0, wrong=0)

        # Should not raise; cached value stays at the meter's actual value.
        asyncio.run(master._apply_meter_config(refresh_from_device=True))

        self.assertEqual(master.meter_config["measurement_mode"], 1)

    def test_write_modbus_exception_is_swallowed(self):
        master, client = self._build(ensure_bidirectional_mode=True)
        client.write_register = AsyncMock(side_effect=ModbusException("boom"))
        self._seed_registers(master, mode=0, system=0, wrong=0)

        asyncio.run(master._apply_meter_config(refresh_from_device=True))

        self.assertEqual(master.meter_config["measurement_mode"], 0)

    def test_write_io_exception_is_swallowed(self):
        master, client = self._build(ensure_3phase_measuring_system=True)
        client.write_register = AsyncMock(side_effect=ModbusIOException("no response"))
        self._seed_registers(master, mode=2, system=2, wrong=0)

        asyncio.run(master._apply_meter_config(refresh_from_device=True))

        self.assertEqual(master.meter_config["measuring_system"], 2)

    def test_write_register_returns_false_on_error_response(self):
        master, client = self._build()
        client.write_register.return_value = self._err_write()

        ok = asyncio.run(master._write_register(0x1103, 2))

        self.assertFalse(ok)

    def test_write_register_returns_true_on_success(self):
        master, client = self._build()
        client.write_register.return_value = self._ok_write()

        ok = asyncio.run(master._write_register(0x1103, 2))

        self.assertTrue(ok)


if __name__ == "__main__":
    unittest.main()
