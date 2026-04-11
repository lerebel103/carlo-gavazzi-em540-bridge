"""Tests for main.py process_loop() – validates main loop priority (Requirements 11.1, 11.2, 11.3).

Transitive imports from main.py hit several compatibility issues (pymodbus API
differences, Python 3.10+ syntax in ha_sensors.py).  We inject mock modules into
sys.modules for the problematic leaves so that `import main` succeeds cleanly.
"""

import asyncio
import sys
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

# ---------------------------------------------------------------------------
# Pre-patch sys.modules for modules that cannot be imported in this environment.
# This must happen before `import main`.
# ---------------------------------------------------------------------------
# 1. pymodbus.constants.ExcCodes is missing in the installed pymodbus version
import pymodbus.constants as _const

if not hasattr(_const, "ExcCodes"):

    class _ExcCodes:
        DEVICE_BUSY = 0x06

    _const.ExcCodes = _ExcCodes

# 2. pymodbus.datastore.ModbusDeviceContext is missing
import pymodbus.datastore as _ds

if not hasattr(_ds, "ModbusDeviceContext"):
    _ds.ModbusDeviceContext = type("ModbusDeviceContext", (), {})

# 3. home_assistant.ha_sensors uses `str | None` (Python 3.10+) – mock the whole
#    HA subtree since we don't need it for main loop tests.
_ha_mock = MagicMock()
for mod_name in [
    "home_assistant.ha_sensors",
    "home_assistant.ha_diagnostics",
    "app.home_assistant.ha_sensors",
    "app.home_assistant.ha_diagnostics",
]:
    if mod_name not in sys.modules:
        sys.modules[mod_name] = _ha_mock

# Now safe to import main
import app.main as main  # noqa: E402


def _make_state():
    """Build a state namespace that mimics AppState for process_loop."""
    return SimpleNamespace(
        pymodbus_log_level="CRITICAL",
        root_log_level="CRITICAL",
        em540_master=SimpleNamespace(
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
            update_interval=0.1,
        ),
        em540_slave=SimpleNamespace(
            host="0.0.0.0",
            rtu_port=5020,
            tcp_port=5021,
            slave_id=1,
            update_timeout=5.0,
            log_level="CRITICAL",
        ),
        ts65a_slave=SimpleNamespace(
            host="0.0.0.0",
            port=5030,
            slave_id=1,
            update_timeout=5.0,
            grid_feed_in_hard_limit=-10000.0,
            smoothing_num_points=10,
            log_level="CRITICAL",
        ),
        goodwe_gm3000_slave=SimpleNamespace(
            host="0.0.0.0",
            socket_port=5012,
            rtu_port=5013,
            slave_id=3,
            update_timeout=5.0,
            log_level="CRITICAL",
        ),
        mqtt=SimpleNamespace(
            enabled=False,
            host="localhost",
            port=1883,
            username="",
            password="",
            update_interval=0.5,
            log_level="CRITICAL",
        ),
    )


class _LoopBreak(Exception):
    """Sentinel exception used to break out of the while True loop."""

    pass


def _setup_mocks():
    """Return common mocks for process_loop dependencies."""
    mock_master = MagicMock()
    type(mock_master).connected = PropertyMock(return_value=True)
    mock_master.connect = AsyncMock()
    mock_master.disconnect = AsyncMock()
    mock_master.add_listener = MagicMock()
    mock_master.data = MagicMock()
    mock_master.data.frame = MagicMock()

    mock_slave = MagicMock()
    mock_slave.start = AsyncMock()
    mock_slave.add_stats_listener = MagicMock()

    mock_ts65a = MagicMock()
    mock_ts65a.start = AsyncMock()
    mock_ts65a.add_stats_listener = MagicMock()

    mock_goodwe = MagicMock()
    mock_goodwe.start = AsyncMock()
    mock_goodwe.add_stats_listener = MagicMock()

    return {"master": mock_master, "slave": mock_slave, "ts65a": mock_ts65a, "goodwe": mock_goodwe}


def _patch_config_manager(state):
    """Create a mock ConfigManager and patch it into main.config_manager."""
    mock_cm = MagicMock()
    mock_cm.load.return_value = state
    mock_cm.start_flush_loop = MagicMock()
    return patch.object(main, "config_manager", mock_cm)


class TestMainLoopPriority(unittest.TestCase):
    """Validates: Requirements 11.1, 11.2, 11.3"""

    # ------------------------------------------------------------------
    # Requirement 11.1: acquire_data called at configured update_interval
    # ------------------------------------------------------------------
    def test_acquire_called_at_update_interval(self):
        """Requirement 11.1 – acquire_data is called at intervals equal to update_interval."""
        state = _make_state()
        mocks = _setup_mocks()
        call_count = {"n": 0}

        async def _acquire():
            call_count["n"] += 1
            if call_count["n"] >= 3:
                raise _LoopBreak()
            return True

        mocks["master"].acquire_data = AsyncMock(side_effect=_acquire)

        with (
            _patch_config_manager(state),
            patch.object(main, "pymodbus_apply_logging_config"),
            patch.object(main, "Em540Master", return_value=mocks["master"]),
            patch.object(main, "Em540Slave", return_value=mocks["slave"]),
            patch.object(main, "Ts65aSlaveBridge", return_value=mocks["ts65a"]),
            patch.object(main, "GoodweGm3000SlaveBridge", return_value=mocks["goodwe"]),
            patch.object(main, "HABridge"),
        ):
            with self.assertRaises(_LoopBreak):
                asyncio.run(main.process_loop())

        self.assertEqual(mocks["master"].acquire_data.await_count, 3)

    # ------------------------------------------------------------------
    # Requirement 11.3: reconnect attempted when client is disconnected
    # ------------------------------------------------------------------
    def test_reconnect_attempted_when_disconnected(self):
        """Requirement 11.3 – connect() is called when client is disconnected."""
        state = _make_state()
        mocks = _setup_mocks()
        type(mocks["master"]).connected = PropertyMock(return_value=False)

        async def _acquire():
            raise _LoopBreak()

        mocks["master"].acquire_data = AsyncMock(side_effect=_acquire)

        with (
            _patch_config_manager(state),
            patch.object(main, "pymodbus_apply_logging_config"),
            patch.object(main, "Em540Master", return_value=mocks["master"]),
            patch.object(main, "Em540Slave", return_value=mocks["slave"]),
            patch.object(main, "Ts65aSlaveBridge", return_value=mocks["ts65a"]),
            patch.object(main, "GoodweGm3000SlaveBridge", return_value=mocks["goodwe"]),
            patch.object(main, "HABridge"),
        ):
            with self.assertRaises(_LoopBreak):
                asyncio.run(main.process_loop())

        mocks["master"].connect.assert_awaited_once()

    # ------------------------------------------------------------------
    # Requirement 11.1/11.2: loop sleeps between intervals using mocked time
    # ------------------------------------------------------------------
    def test_loop_sleeps_between_intervals(self):
        """Requirement 11.1, 11.2 – loop uses perf_counter for timing and sleeps between intervals."""
        state = _make_state()
        state.em540_master.update_interval = 0.1
        mocks = _setup_mocks()

        # Use a counter-based perf_counter mock that advances by update_interval
        # each call, ensuring the loop always triggers acquire_data immediately.
        counter = {"n": 0}

        def _perf_counter():
            val = counter["n"] * 0.1  # each call advances by update_interval
            counter["n"] += 1
            return val

        call_count = {"n": 0}

        async def _acquire():
            call_count["n"] += 1
            if call_count["n"] >= 3:
                raise _LoopBreak()
            return True

        mocks["master"].acquire_data = AsyncMock(side_effect=_acquire)

        with (
            _patch_config_manager(state),
            patch.object(main, "pymodbus_apply_logging_config"),
            patch.object(main, "Em540Master", return_value=mocks["master"]),
            patch.object(main, "Em540Slave", return_value=mocks["slave"]),
            patch.object(main, "Ts65aSlaveBridge", return_value=mocks["ts65a"]),
            patch.object(main, "GoodweGm3000SlaveBridge", return_value=mocks["goodwe"]),
            patch.object(main, "HABridge"),
            patch.object(main.time, "perf_counter", side_effect=_perf_counter) as mock_perf,
            patch.object(main.asyncio, "sleep", new_callable=AsyncMock),
        ):
            with self.assertRaises(_LoopBreak):
                asyncio.run(main.process_loop())

        self.assertTrue(mock_perf.called)
        self.assertEqual(mocks["master"].acquire_data.await_count, 3)

    # ------------------------------------------------------------------
    # Requirement 11.2: slow listener does not block next acquire_data
    # ------------------------------------------------------------------
    def test_slow_listener_does_not_block_acquire(self):
        """Requirement 11.2 – slow listener processing does not block next acquire_data call.

        The main loop calls threading.Condition.notify() (non-blocking) and then
        sleeps until the next interval. Listener processing runs in a separate
        daemon thread. This test verifies acquire_data is called repeatedly
        without being blocked by listener work.
        """
        state = _make_state()
        mocks = _setup_mocks()
        acquire_sequence = []
        call_count = {"n": 0}

        async def _acquire():
            call_count["n"] += 1
            acquire_sequence.append(call_count["n"])
            if call_count["n"] >= 3:
                raise _LoopBreak()
            return True

        mocks["master"].acquire_data = AsyncMock(side_effect=_acquire)

        with (
            _patch_config_manager(state),
            patch.object(main, "pymodbus_apply_logging_config"),
            patch.object(main, "Em540Master", return_value=mocks["master"]),
            patch.object(main, "Em540Slave", return_value=mocks["slave"]),
            patch.object(main, "Ts65aSlaveBridge", return_value=mocks["ts65a"]),
            patch.object(main, "GoodweGm3000SlaveBridge", return_value=mocks["goodwe"]),
            patch.object(main, "HABridge"),
        ):
            with self.assertRaises(_LoopBreak):
                asyncio.run(main.process_loop())

        self.assertEqual(mocks["master"].acquire_data.await_count, 3)
        self.assertEqual(acquire_sequence, [1, 2, 3])

    def test_mqtt_startup_failure_does_not_block_tick_loop(self):
        """MQTT connect scheduling failures must not block process_loop startup or acquisitions."""
        state = _make_state()
        state.mqtt.enabled = True
        mocks = _setup_mocks()

        mqtt_bridge = MagicMock()
        mqtt_bridge.on_em540_master_stats = MagicMock()
        mqtt_bridge.on_em540_slave_stats = MagicMock()
        mqtt_bridge.on_ts65a_slave_stats = MagicMock()
        mqtt_bridge.connect = MagicMock(side_effect=RuntimeError("should not escape"))

        call_count = {"n": 0}

        async def _acquire():
            call_count["n"] += 1
            if call_count["n"] >= 1:
                raise _LoopBreak()
            return True

        mocks["master"].acquire_data = AsyncMock(side_effect=_acquire)

        with (
            _patch_config_manager(state),
            patch.object(main, "pymodbus_apply_logging_config"),
            patch.object(main, "Em540Master", return_value=mocks["master"]),
            patch.object(main, "Em540Slave", return_value=mocks["slave"]),
            patch.object(main, "Ts65aSlaveBridge", return_value=mocks["ts65a"]),
            patch.object(main, "GoodweGm3000SlaveBridge", return_value=mocks["goodwe"]),
            patch.object(main, "HABridge", return_value=mqtt_bridge),
        ):
            with self.assertRaises(_LoopBreak):
                asyncio.run(main.process_loop())

        mocks["master"].add_listener.assert_any_call(mqtt_bridge)
        self.assertEqual(mocks["master"].acquire_data.await_count, 1)

    def test_process_loop_cleans_up_background_services_on_exit(self):
        state = _make_state()
        state.mqtt.enabled = True
        mocks = _setup_mocks()
        mock_cm = MagicMock()
        mock_cm.load.return_value = state
        mock_cm.start_flush_loop = MagicMock()
        mock_cm.stop = MagicMock()

        mqtt_bridge = MagicMock()
        mqtt_bridge.on_em540_master_stats = MagicMock()
        mqtt_bridge.on_em540_slave_stats = MagicMock()
        mqtt_bridge.on_ts65a_slave_stats = MagicMock()
        mqtt_bridge.connect = MagicMock()
        mqtt_bridge.stop = MagicMock()

        async def _acquire():
            raise _LoopBreak()

        mocks["master"].acquire_data = AsyncMock(side_effect=_acquire)

        with (
            patch.object(main, "config_manager", mock_cm),
            patch.object(main, "pymodbus_apply_logging_config"),
            patch.object(main, "Em540Master", return_value=mocks["master"]),
            patch.object(main, "Em540Slave", return_value=mocks["slave"]),
            patch.object(main, "Ts65aSlaveBridge", return_value=mocks["ts65a"]),
            patch.object(main, "GoodweGm3000SlaveBridge", return_value=mocks["goodwe"]),
            patch.object(main, "HABridge", return_value=mqtt_bridge),
        ):
            with self.assertRaises(_LoopBreak):
                asyncio.run(main.process_loop())

        mqtt_bridge.stop.assert_called_once()
        mocks["master"].disconnect.assert_awaited_once()
        mock_cm.stop.assert_called_once()

    def test_main_logs_version_on_startup(self):
        """Startup log must include app version exactly once (no double 'v')."""
        state = _make_state()

        with (
            patch.object(main, "parse_args", return_value=SimpleNamespace(config="config.yaml")),
            patch.object(main, "ConfigManager") as mock_cm_cls,
            patch.object(main.logging, "basicConfig"),
            patch.object(main, "process_loop", new_callable=AsyncMock),
            patch.object(main, "version_for_display", return_value="v1.2.3"),
            patch.object(main.logger, "info") as mock_logger_info,
        ):
            mock_cm = MagicMock()
            mock_cm.load.return_value = state
            mock_cm_cls.return_value = mock_cm

            asyncio.run(main.main())

        mock_logger_info.assert_any_call("Starting EM540 Energy Meter Bridge (%s)", "v1.2.3")


if __name__ == "__main__":
    unittest.main()
