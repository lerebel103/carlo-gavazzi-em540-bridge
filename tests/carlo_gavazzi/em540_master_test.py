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
    """Build responses for the first tick: primary block + energy chunk 0.

    On the first tick, primary is always read first, then energy_skip_fires=True
    so chunk 0 is read.
    """
    from app.carlo_gavazzi.em540_data import ENERGY_BLOCK_CHUNK_SIZE

    primary_reg = frame.dynamic_reg_map[0x0000]
    return [
        _make_successful_result(len(primary_reg.values)),  # primary block
        _make_successful_result(ENERGY_BLOCK_CHUNK_SIZE),  # energy chunk 0
    ]


def _build_continuation_tick_responses(frame, chunk_index):
    """Build responses for a tick reading primary block + a pending energy chunk."""
    from app.carlo_gavazzi.em540_data import ENERGY_BLOCK_CHUNK_SIZE, ENERGY_BLOCK_TOTAL_SIZE

    primary_reg = frame.dynamic_reg_map[0x0000]
    chunk_offset = chunk_index * ENERGY_BLOCK_CHUNK_SIZE
    chunk_size = min(ENERGY_BLOCK_CHUNK_SIZE, ENERGY_BLOCK_TOTAL_SIZE - chunk_offset)
    return [
        _make_successful_result(len(primary_reg.values)),  # primary block
        _make_successful_result(chunk_size),  # energy chunk N
    ]


def _build_primary_only_responses(frame):
    """Build responses for a tick with only the primary block read."""
    primary_reg = frame.dynamic_reg_map[0x0000]
    return [_make_successful_result(len(primary_reg.values))]


class TestEm540Master(unittest.TestCase):
    """Validates: Requirements 9.1, 9.2, 9.3, 9.4, 9.5, 10.1, 10.2"""

    @patch("app.carlo_gavazzi.em540_master.AsyncModbusTcpClient")
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

        # First tick: energy chunk 0 + primary block
        frame = self.master.data.frame
        responses = _build_first_tick_responses(frame)
        self.mock_client.read_holding_registers = AsyncMock(side_effect=responses)

        with patch.object(self.master._condition, "notify") as mock_notify:
            result = asyncio.run(self.master.acquire_data())

        self.assertTrue(result)
        mock_notify.assert_called_once()

    # -----------------------------------------------------------------------
    # Requirement 10.2: successful acquire reads dynamic registers
    # -----------------------------------------------------------------------
    def test_acquire_data_reads_dynamic_registers(self):
        """Requirement 10.2 – acquire_data reads energy chunk 0 + primary on first tick."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        frame = self.master.data.frame
        responses = _build_first_tick_responses(frame)
        self.mock_client.read_holding_registers = AsyncMock(side_effect=responses)

        with patch.object(self.master._condition, "notify"):
            result = asyncio.run(self.master.acquire_data())

        self.assertTrue(result)
        # First tick reads energy chunk 0 + primary block = 2 calls
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
        bad_result = MagicMock()
        bad_result.isError.return_value = True

        good_results = []
        for reg_addr in self.master._static_read_plan:
            static_reg = self.master.data.frame.static_reg_map[reg_addr]
            good_results.append(_make_successful_result(len(static_reg.values)))

        self.mock_client.read_holding_registers = AsyncMock(side_effect=[bad_result, *good_results])

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
                    stats.modbus_read_duration_ms_last,
                    stats.post_read_processing_ms_last,
                    stats.non_read_processing_ms_last,
                )
            )

        self.master.add_stats_listener(_on_stats)

        cycle_start = time.perf_counter() - 0.02
        self.master._update_timing_stats(
            cycle_start=cycle_start,
            modbus_read_ms=12.0,
            post_read_processing_ms=3.0,
        )

        self.assertTrue(observed)
        modbus_ms, post_ms, non_read_ms = observed[-1]
        self.assertEqual(modbus_ms, 12.0)
        self.assertEqual(post_ms, 3.0)
        self.assertGreaterEqual(non_read_ms, 0.0)

    def test_tick_overrun_ignores_jitter_within_margin(self):
        """A cycle exceeding the budget but within the jitter margin must not count as an overrun."""
        self.config.update_interval = 0.1  # 100ms budget; margin fraction 0.5 -> 50ms slack

        # 130ms cycle: over budget (100ms) but under budget + margin (150ms).
        cycle_start = time.perf_counter() - 0.130
        self.master._update_timing_stats(
            cycle_start=cycle_start,
            modbus_read_ms=120.0,
            post_read_processing_ms=5.0,
        )

        self.assertEqual(self.master._stats.tick_overrun_count, 0)

    def test_tick_overrun_counts_when_beyond_margin(self):
        """A cycle exceeding the budget plus jitter margin must count as an overrun."""
        self.config.update_interval = 0.1  # 100ms budget; margin fraction 0.5 -> 50ms slack

        # 180ms cycle: beyond budget + margin (150ms).
        cycle_start = time.perf_counter() - 0.180
        self.master._update_timing_stats(
            cycle_start=cycle_start,
            modbus_read_ms=170.0,
            post_read_processing_ms=5.0,
        )

        self.assertEqual(self.master._stats.tick_overrun_count, 1)

    def test_refresh_client_runtime_config_uses_live_shared_config_values(self):
        self.mock_client.timeout = 1.0
        self.mock_client.retries = 0

        self.config.timeout = 0.25
        self.config.retries = 3
        self.master._refresh_client_runtime_config()

        self.assertEqual(self.mock_client.timeout, 0.25)
        self.assertEqual(self.mock_client.retries, 3)


class TestSkipNRead(unittest.TestCase):
    """Validates: energy block read behaviour with current chunk configuration.

    With ENERGY_BLOCK_CHUNK_SIZE=32 (2 chunks) and skip_n_read=9, the energy
    block is read across 3 ticks every ~1s: chunk0, rest, chunk1. The rest tick
    ensures each energy chunk read is separated by a primary-only tick.
    """

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
    # First tick reads primary + energy chunk 0
    # -----------------------------------------------------------------------
    def test_first_tick_reads_primary_and_energy_chunk0(self):
        """First tick reads primary block and energy chunk 0."""
        responses = _build_first_tick_responses(self.frame)
        self.mock_client.read_holding_registers = AsyncMock(side_effect=responses)

        with patch.object(self.master._condition, "notify"):
            result = asyncio.run(self.master.acquire_data())

        self.assertTrue(result)
        self.assertEqual(self.mock_client.read_holding_registers.await_count, 2)
        addresses = self._get_read_addresses()
        self.assertIn(0x0000, addresses)
        self.assertIn(0x0500, addresses)
        # With 2 chunks, chunk 1 is pending after chunk 0
        self.assertEqual(self.master._energy_chunk_pending, 1)

    # -----------------------------------------------------------------------
    # Primary block is always read on every cycle
    # -----------------------------------------------------------------------
    def test_primary_block_reads_every_cycle(self):
        """Primary block (skip_n_read=0) is read on every cycle."""
        # With 2 chunks: tick1=primary+chunk0, tick2=rest(primary), tick3=primary+chunk1, tick4+=primary
        for cycle in range(1, 6):
            self.mock_client.read_holding_registers.reset_mock()
            if cycle == 1:
                # First cycle: energy fires, read primary + chunk 0
                self.mock_client.read_holding_registers = AsyncMock(side_effect=_build_first_tick_responses(self.frame))
            elif not self.master._energy_chunk_rest and self.master._energy_chunk_pending > 0:
                # Chunk read tick: primary + pending chunk
                self.mock_client.read_holding_registers = AsyncMock(
                    side_effect=_build_continuation_tick_responses(self.frame, self.master._energy_chunk_pending)
                )
            else:
                # Rest tick or idle: primary only
                self.mock_client.read_holding_registers = AsyncMock(
                    side_effect=_build_primary_only_responses(self.frame)
                )
            with patch.object(self.master._condition, "notify"):
                result = asyncio.run(self.master.acquire_data())

            self.assertTrue(result, f"Cycle {cycle} should succeed")
            addresses = self._get_read_addresses()
            self.assertIn(0x0000, addresses, f"Cycle {cycle}: 0x0000 should always be read")

    # -----------------------------------------------------------------------
    # Startup gate: energy_initial_read_complete opens after first full read
    # -----------------------------------------------------------------------
    def test_energy_initial_read_complete_tracks_full_cycle(self):
        """_energy_initial_read_complete becomes True after all energy chunks are read.

        With 2 chunks: chunk 0 on tick 1, rest on tick 2, chunk 1 on tick 3.
        Gate opens after tick 3.
        """
        # Reset the gate to False (simulating fresh startup)
        self.master._energy_initial_read_complete = False
        self.master._dyn_reg_read_counter = 0

        initial_seq = self.master._data_seq

        # Tick 1: primary + chunk 0
        self.mock_client.read_holding_registers = AsyncMock(side_effect=_build_first_tick_responses(self.frame))
        asyncio.run(self.master.acquire_data())
        self.assertFalse(self.master._energy_initial_read_complete)

        # Tick 2: rest (primary only)
        self.mock_client.read_holding_registers = AsyncMock(side_effect=_build_primary_only_responses(self.frame))
        asyncio.run(self.master.acquire_data())
        self.assertFalse(self.master._energy_initial_read_complete)

        # Tick 3: primary + chunk 1 (final chunk)
        self.mock_client.read_holding_registers = AsyncMock(
            side_effect=_build_continuation_tick_responses(self.frame, 1)
        )
        asyncio.run(self.master.acquire_data())

        # Gate should now be open
        self.assertTrue(self.master._energy_initial_read_complete)
        self.assertGreater(self.master._data_seq, initial_seq)

    # -----------------------------------------------------------------------
    # Energy values are written correctly into the register map
    # -----------------------------------------------------------------------
    def test_energy_values_written_to_register_map(self):
        """Energy block read values are correctly stored in the register map."""
        from app.carlo_gavazzi.em540_data import ENERGY_BLOCK_CHUNK_SIZE, ENERGY_BLOCK_TOTAL_SIZE

        primary_reg = self.frame.dynamic_reg_map[0x0000]
        primary_result = _make_successful_result(len(primary_reg.values))

        # Build distinct values for each chunk
        chunk0_values = list(range(300, 300 + ENERGY_BLOCK_CHUNK_SIZE))
        chunk1_size = ENERGY_BLOCK_TOTAL_SIZE - ENERGY_BLOCK_CHUNK_SIZE
        chunk1_values = list(range(400, 400 + chunk1_size))

        chunk0_result = MagicMock()
        chunk0_result.isError.return_value = False
        chunk0_result.registers = chunk0_values

        chunk1_result = MagicMock()
        chunk1_result.isError.return_value = False
        chunk1_result.registers = chunk1_values

        # Tick 1: primary + chunk 0
        self.mock_client.read_holding_registers = AsyncMock(side_effect=[primary_result, chunk0_result])
        with patch("app.carlo_gavazzi.meter_data.MeterData.update_from_frame", return_value=None):
            with patch.object(self.master._condition, "notify"):
                asyncio.run(self.master.acquire_data())

        # Tick 2: rest (primary only)
        self.mock_client.read_holding_registers = AsyncMock(
            side_effect=[_make_successful_result(len(primary_reg.values))]
        )
        with patch("app.carlo_gavazzi.meter_data.MeterData.update_from_frame", return_value=None):
            with patch.object(self.master._condition, "notify"):
                asyncio.run(self.master.acquire_data())

        # Tick 3: primary + chunk 1
        self.mock_client.read_holding_registers = AsyncMock(
            side_effect=[_make_successful_result(len(primary_reg.values)), chunk1_result]
        )
        with patch("app.carlo_gavazzi.meter_data.MeterData.update_from_frame", return_value=None):
            with patch.object(self.master._condition, "notify"):
                result = asyncio.run(self.master.acquire_data())

        self.assertTrue(result)
        stored = self.master.data.frame.dynamic_reg_map[0x0500].values
        self.assertEqual(stored[:ENERGY_BLOCK_CHUNK_SIZE], chunk0_values)
        self.assertEqual(stored[ENERGY_BLOCK_CHUNK_SIZE:], chunk1_values)

    # -----------------------------------------------------------------------
    # Energy chunk failure does not abort the tick (primary still publishes)
    # -----------------------------------------------------------------------
    def test_energy_chunk_failure_does_not_abort_tick(self):
        """If the energy read fails, primary data still publishes."""
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

        # With 2 chunks and skip_n_read=9: tick 1 = primary+chunk0, tick 2 = rest(primary),
        # tick 3 = primary+chunk1.
        responses = (
            _build_first_tick_responses(frame)
            + _build_primary_only_responses(frame)
            + _build_continuation_tick_responses(frame, 1)
        )
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


if __name__ == "__main__":
    unittest.main()
