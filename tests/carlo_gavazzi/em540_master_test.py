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
    """Build responses for the first tick: energy chunk 0 (16 regs) + primary block.

    On the first tick, energy_skip_fires=True so chunk 0 is read, then the primary block.
    """
    from app.carlo_gavazzi.em540_data import ENERGY_BLOCK_CHUNK_SIZE

    primary_reg = frame.dynamic_reg_map[0x0000]
    return [
        _make_successful_result(ENERGY_BLOCK_CHUNK_SIZE),  # energy chunk 0
        _make_successful_result(len(primary_reg.values)),  # primary block
    ]


def _build_continuation_tick_responses(frame, chunk_index):
    """Build responses for a tick reading a pending energy chunk + primary block."""
    from app.carlo_gavazzi.em540_data import ENERGY_BLOCK_CHUNK_SIZE, ENERGY_BLOCK_TOTAL_SIZE

    primary_reg = frame.dynamic_reg_map[0x0000]
    chunk_offset = chunk_index * ENERGY_BLOCK_CHUNK_SIZE
    chunk_size = min(ENERGY_BLOCK_CHUNK_SIZE, ENERGY_BLOCK_TOTAL_SIZE - chunk_offset)
    return [
        _make_successful_result(chunk_size),  # energy chunk N
        _make_successful_result(len(primary_reg.values)),  # primary block
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
        self.mock_client.close.assert_called()
        listener.read_failed.assert_awaited_once()

    # -----------------------------------------------------------------------
    # Requirement 9.3: register count mismatch → os._exit(1)
    # -----------------------------------------------------------------------
    @patch("app.carlo_gavazzi.em540_master.os._exit", side_effect=SystemExit(1))
    def test_register_count_mismatch_exits(self, mock_exit):
        """Requirement 9.3 – register count mismatch calls os._exit(1)."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        # Return fewer registers than expected for the energy chunk read
        bad_result = MagicMock()
        bad_result.isError.return_value = False
        bad_result.registers = [0]  # Only 1 register instead of expected 16
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

        self.mock_client.read_holding_registers = AsyncMock(side_effect=ModbusIOException("IO error"))

        result = asyncio.run(self.master.acquire_data())

        self.assertFalse(result)

    # -----------------------------------------------------------------------
    # Requirement 9.5: ModbusException → closes client, returns False
    # -----------------------------------------------------------------------
    def test_modbus_exception_closes_client_and_returns_false(self):
        """Requirement 9.5 – ModbusException closes client and returns False."""
        type(self.mock_client).connected = PropertyMock(return_value=True)

        self.mock_client.read_holding_registers = AsyncMock(side_effect=ModbusException("connection lost"))

        result = asyncio.run(self.master.acquire_data())

        self.assertFalse(result)
        self.mock_client.close.assert_called_once()

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

    def test_refresh_client_runtime_config_uses_live_shared_config_values(self):
        self.mock_client.timeout = 1.0
        self.mock_client.retries = 0

        self.config.timeout = 0.25
        self.config.retries = 3
        self.master._refresh_client_runtime_config()

        self.assertEqual(self.mock_client.timeout, 0.25)
        self.assertEqual(self.mock_client.retries, 3)


class TestSkipNRead(unittest.TestCase):
    """Validates: Requirements 8.1, 8.2, 8.3 (chunked energy read variant)"""

    @patch("app.carlo_gavazzi.em540_master.AsyncModbusTcpClient")
    def setUp(self, mock_tcp_cls):
        """Set up master with mock client. Energy block has skip_n_read=2."""
        self.mock_client = MagicMock()
        self.mock_client.read_holding_registers = AsyncMock()
        self.mock_client.connect = AsyncMock()
        self.mock_client.close = MagicMock()
        type(self.mock_client).connected = PropertyMock(return_value=True)
        mock_tcp_cls.return_value = self.mock_client

        self.config = _make_config()
        self.master = Em540Master(self.config)
        self.master._client = self.mock_client
        # Bypass initial energy-read gate for testing chunked read mechanics
        self.master._energy_initial_read_complete = True

        self.frame = self.master.data.frame

        # Set 0x0500 register to skip_n_read=2 (read every 3rd cycle)
        self.frame.dynamic_reg_map[0x0500].skip_n_read = 2

    def _get_read_addresses(self):
        """Extract the register addresses from read_holding_registers calls."""
        return [
            call.kwargs.get("address", call.args[0] if call.args else None)
            for call in self.mock_client.read_holding_registers.call_args_list
        ]

    # -----------------------------------------------------------------------
    # Requirement 8.1: first cycle reads energy chunk 0 + primary
    # -----------------------------------------------------------------------
    def test_first_cycle_reads_energy_chunk_and_primary(self):
        """Requirement 8.1 – First cycle reads energy chunk 0 + primary block."""
        responses = _build_first_tick_responses(self.frame)
        self.mock_client.read_holding_registers = AsyncMock(side_effect=responses)

        with patch.object(self.master._condition, "notify"):
            result = asyncio.run(self.master.acquire_data())

        self.assertTrue(result)
        self.assertEqual(self.master._dyn_reg_read_counter, 1)
        # Energy chunk 0 (at 0x0500) + primary (at 0x0000) = 2 calls
        self.assertEqual(self.mock_client.read_holding_registers.await_count, 2)
        addresses = self._get_read_addresses()
        self.assertIn(0x0000, addresses)
        self.assertIn(0x0500, addresses)
        # Next chunk should be pending
        self.assertEqual(self.master._energy_chunk_pending, 1)

    # -----------------------------------------------------------------------
    # Requirement 8.2: continuation ticks interlace chunks with rest ticks
    # -----------------------------------------------------------------------
    def test_continuation_ticks_read_remaining_chunks(self):
        """Requirement 8.2 – Chunks are interlaced with primary-only rest ticks."""
        from app.carlo_gavazzi.em540_data import ENERGY_BLOCK_CHUNK_SIZE

        # Cycle 1: energy chunk 0 + primary
        self.mock_client.read_holding_registers = AsyncMock(side_effect=_build_first_tick_responses(self.frame))
        with patch.object(self.master._condition, "notify"):
            asyncio.run(self.master.acquire_data())
        self.assertEqual(self.master._energy_chunk_pending, 1)
        self.assertTrue(self.master._energy_chunk_rest)

        # Cycle 2: REST tick — primary only
        self.mock_client.read_holding_registers.reset_mock()
        self.mock_client.read_holding_registers = AsyncMock(side_effect=_build_primary_only_responses(self.frame))
        with patch.object(self.master._condition, "notify"):
            result = asyncio.run(self.master.acquire_data())
        self.assertTrue(result)
        self.assertEqual(self.mock_client.read_holding_registers.await_count, 1)
        self.assertEqual(self.master._energy_chunk_pending, 1)  # still pending
        self.assertFalse(self.master._energy_chunk_rest)  # rest consumed

        # Cycle 3: chunk 1 + primary
        self.mock_client.read_holding_registers.reset_mock()
        self.mock_client.read_holding_registers = AsyncMock(
            side_effect=_build_continuation_tick_responses(self.frame, 1)
        )
        with patch.object(self.master._condition, "notify"):
            result = asyncio.run(self.master.acquire_data())
        self.assertTrue(result)
        addresses = self._get_read_addresses()
        self.assertIn(0x0500 + ENERGY_BLOCK_CHUNK_SIZE, addresses)
        self.assertEqual(self.master._energy_chunk_pending, 2)
        self.assertTrue(self.master._energy_chunk_rest)

        # Continue through remaining chunks with rest ticks in between
        from app.carlo_gavazzi.em540_data import ENERGY_BLOCK_TOTAL_SIZE

        num_chunks = (ENERGY_BLOCK_TOTAL_SIZE + ENERGY_BLOCK_CHUNK_SIZE - 1) // ENERGY_BLOCK_CHUNK_SIZE
        for chunk_idx in range(2, num_chunks):
            # REST tick
            self.mock_client.read_holding_registers.reset_mock()
            self.mock_client.read_holding_registers = AsyncMock(side_effect=_build_primary_only_responses(self.frame))
            with patch.object(self.master._condition, "notify"):
                asyncio.run(self.master.acquire_data())
            self.assertFalse(self.master._energy_chunk_rest)

            # Chunk read tick
            self.mock_client.read_holding_registers.reset_mock()
            self.mock_client.read_holding_registers = AsyncMock(
                side_effect=_build_continuation_tick_responses(self.frame, chunk_idx)
            )
            with patch.object(self.master._condition, "notify"):
                result = asyncio.run(self.master.acquire_data())
            self.assertTrue(result)

            if chunk_idx < num_chunks - 1:
                self.assertEqual(self.master._energy_chunk_pending, chunk_idx + 1)
                self.assertTrue(self.master._energy_chunk_rest)
            else:
                # Last chunk done
                self.assertEqual(self.master._energy_chunk_pending, -1)
                self.assertFalse(self.master._energy_chunk_rest)

    # -----------------------------------------------------------------------
    # Requirement 8.2: energy reads are skipped when counter doesn't fire
    # -----------------------------------------------------------------------
    def test_subsequent_cycles_skip_energy_reads(self):
        """Requirement 8.2 – After all chunks complete, energy reads are skipped until skip counter fires."""
        from app.carlo_gavazzi.em540_data import ENERGY_BLOCK_CHUNK_SIZE, ENERGY_BLOCK_TOTAL_SIZE

        num_chunks = (ENERGY_BLOCK_TOTAL_SIZE + ENERGY_BLOCK_CHUNK_SIZE - 1) // ENERGY_BLOCK_CHUNK_SIZE

        # Run through a full interlaced chunk sequence
        # Pattern: chunk0, rest, chunk1, rest, ..., chunkN-1
        tick_responses = [_build_first_tick_responses(self.frame)]  # chunk 0
        for chunk_idx in range(1, num_chunks):
            tick_responses.append(_build_primary_only_responses(self.frame))  # rest
            tick_responses.append(_build_continuation_tick_responses(self.frame, chunk_idx))  # chunk N

        for responses in tick_responses:
            self.mock_client.read_holding_registers.reset_mock()
            self.mock_client.read_holding_registers = AsyncMock(side_effect=responses)
            with patch.object(self.master._condition, "notify"):
                asyncio.run(self.master.acquire_data())

        self.assertEqual(self.master._energy_chunk_pending, -1)

        # Next tick: skip counter hasn't fired again → primary only
        self.mock_client.read_holding_registers.reset_mock()
        self.mock_client.read_holding_registers = AsyncMock(side_effect=_build_primary_only_responses(self.frame))
        with patch.object(self.master._condition, "notify"):
            result = asyncio.run(self.master.acquire_data())
        self.assertTrue(result)
        self.assertEqual(self.mock_client.read_holding_registers.await_count, 1)
        addresses = self._get_read_addresses()
        self.assertIn(0x0000, addresses)
        self.assertEqual(self.master._energy_chunk_pending, -1)

    # -----------------------------------------------------------------------
    # Requirement 8.3: primary block (skip_n_read=0) is read every cycle
    # -----------------------------------------------------------------------
    def test_primary_block_reads_every_cycle(self):
        """Requirement 8.3 – Primary block (skip_n_read=0) is read on every cycle."""
        # Run 8 cycles and verify 0x0000 is always read (covers chunk + rest + idle ticks)
        for cycle in range(1, 9):
            self.mock_client.read_holding_registers.reset_mock()

            if cycle == 1:
                # First cycle: energy chunk 0 + primary
                self.mock_client.read_holding_registers = AsyncMock(side_effect=_build_first_tick_responses(self.frame))
            elif self.master._energy_chunk_pending > 0 and not self.master._energy_chunk_rest:
                # Chunk read tick
                self.mock_client.read_holding_registers = AsyncMock(
                    side_effect=_build_continuation_tick_responses(self.frame, self.master._energy_chunk_pending)
                )
            else:
                # Primary only (rest tick or idle)
                self.mock_client.read_holding_registers = AsyncMock(
                    side_effect=_build_primary_only_responses(self.frame)
                )

            with patch.object(self.master._condition, "notify"):
                result = asyncio.run(self.master.acquire_data())

            self.assertTrue(result, f"Cycle {cycle} should succeed")
            addresses = self._get_read_addresses()
            self.assertIn(0x0000, addresses, f"Cycle {cycle}: 0x0000 should always be read")

    # -----------------------------------------------------------------------
    # Regression: chunk 0 values must persist in the frame after chunk 1 is read
    # -----------------------------------------------------------------------
    def test_chunk0_values_persist_after_chunk1_read(self):
        """Chunk 0 values written in tick N must still be present after chunk 1 read (rest tick in between)."""
        from app.carlo_gavazzi.em540_data import ENERGY_BLOCK_CHUNK_SIZE

        # Use distinct non-zero values for each chunk so we can verify they persist
        chunk0_values = list(range(100, 100 + ENERGY_BLOCK_CHUNK_SIZE))
        chunk1_values = list(range(200, 200 + ENERGY_BLOCK_CHUNK_SIZE))

        primary_reg = self.frame.dynamic_reg_map[0x0000]
        primary_result = _make_successful_result(len(primary_reg.values))

        # Cycle 1: chunk 0 returns distinctive values
        chunk0_result = MagicMock()
        chunk0_result.isError.return_value = False
        chunk0_result.registers = chunk0_values
        self.mock_client.read_holding_registers = AsyncMock(side_effect=[chunk0_result, primary_result])

        with patch("app.carlo_gavazzi.meter_data.MeterData.update_from_frame", return_value=None):
            with patch.object(self.master._condition, "notify"):
                result = asyncio.run(self.master.acquire_data())
        self.assertTrue(result)
        self.assertEqual(self.master._energy_chunk_pending, 1)
        self.assertTrue(self.master._energy_chunk_rest)

        # Cycle 2: rest tick — primary only
        self.mock_client.read_holding_registers.reset_mock()
        self.mock_client.read_holding_registers = AsyncMock(
            side_effect=[_make_successful_result(len(primary_reg.values))]
        )
        with patch("app.carlo_gavazzi.meter_data.MeterData.update_from_frame", return_value=None):
            with patch.object(self.master._condition, "notify"):
                result = asyncio.run(self.master.acquire_data())
        self.assertTrue(result)

        # Cycle 3: chunk 1 returns different distinctive values
        chunk1_result = MagicMock()
        chunk1_result.isError.return_value = False
        chunk1_result.registers = chunk1_values
        self.mock_client.read_holding_registers.reset_mock()
        self.mock_client.read_holding_registers = AsyncMock(
            side_effect=[chunk1_result, _make_successful_result(len(primary_reg.values))]
        )

        with patch("app.carlo_gavazzi.meter_data.MeterData.update_from_frame", return_value=None):
            with patch.object(self.master._condition, "notify"):
                result = asyncio.run(self.master.acquire_data())
        self.assertTrue(result)

        # Verify: the front buffer's energy block should have chunk 0 + chunk 1 values
        energy_values = self.master.data.frame.dynamic_reg_map[0x0500].values
        self.assertEqual(energy_values[:ENERGY_BLOCK_CHUNK_SIZE], chunk0_values)
        self.assertEqual(energy_values[ENERGY_BLOCK_CHUNK_SIZE : 2 * ENERGY_BLOCK_CHUNK_SIZE], chunk1_values)


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
        # Bypass initial energy-read gate for listener tests
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

        # 3 cycles: tick 1 (chunk0+primary), tick 2 (rest=primary only), tick 3 (chunk1+primary)
        # With default skip_n_read=4 for energy: fires on counter=1 (first cycle exception)
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
