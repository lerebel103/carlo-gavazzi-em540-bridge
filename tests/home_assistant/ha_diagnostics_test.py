import json
from types import SimpleNamespace
from unittest.mock import patch

from app.carlo_gavazzi.em540_master import Em540MasterStats
from app.carlo_gavazzi.em540_slave_stats import EM540SlaveStats
from app.home_assistant.ha_diagnostics import HADiagnostics


def test_em540_tcp_client_stats_are_published_in_diagnostics_payload():
    diagnostics = HADiagnostics(topic_prefix="test")

    slave_stats = EM540SlaveStats()
    slave_stats.tcp_client_count = 3
    slave_stats.tcp_client_disconnect_count = 11
    diagnostics.set_em540_slave_stats(slave_stats)

    master_stats = Em540MasterStats()
    master_stats.read_duration_ms_last = 9.5
    diagnostics.set_em540_master_stats(master_stats)

    with patch.dict("sys.modules", {"uptime": SimpleNamespace(uptime=lambda: 1)}):
        _, payload = diagnostics.mqtt_data()
    payload_obj = json.loads(payload)

    assert payload_obj["em540_tcp_client_count"] == 3
    assert payload_obj["em540_tcp_client_disconnect_count"] == 11


def test_diagnostics_payload_contains_all_declared_sensor_keys():
    diagnostics = HADiagnostics(topic_prefix="test")

    with patch.dict("sys.modules", {"uptime": SimpleNamespace(uptime=lambda: 1)}):
        _, payload = diagnostics.mqtt_data()

    payload_obj = json.loads(payload)
    expected_keys = {sensor.safe_name for sensor in diagnostics._all_sensors()}

    assert set(payload_obj.keys()) == expected_keys


def test_master_read_rate_is_derived_from_master_stats_callback_timing():
    diagnostics = HADiagnostics(topic_prefix="test")
    stats = Em540MasterStats()

    with patch("app.home_assistant.ha_diagnostics.time.time", side_effect=[100.0, 106.0]):
        diagnostics.set_em540_master_stats(stats)
        diagnostics.set_em540_master_stats(stats)

    assert abs(diagnostics.update_rate.value - (1 / 6)) < 1e-9


def test_mqtt_update_rate_is_derived_from_actual_publish_timing():
    diagnostics = HADiagnostics(topic_prefix="test")

    diagnostics.record_mqtt_publish(200.0)
    diagnostics.record_mqtt_publish(206.0)

    assert abs(diagnostics.mqtt_update_rate.value - (1 / 6)) < 1e-9


def test_only_selected_diagnostics_are_enabled_by_default():
    diagnostics = HADiagnostics(topic_prefix="test")

    enabled = {sensor.safe_name for sensor in diagnostics._all_sensors() if sensor.enabled_by_default}

    assert enabled == {
        "sys_uptime",
        "bridge_uptime",
        "rs485_master_read_rate",
        "rs485_tick_overrun_count",
        "min_power_w",
        "max_power_w",
        "em540_rtu_client_count",
        "em540_rtu_client_disconnect_count",
        "em540_tcp_client_count",
        "em540_tcp_client_disconnect_count",
        "em540_stale_data_age",
        "em540_dropped_stale_requests",
        "ts65a_tcp_client_count",
        "ts65a_tcp_client_disconnect_count",
        "overfeed_limit_count",
        "overfeed_limit_max_duration",
        "ts65a_stale_data_age",
        "ts65a_dropped_stale_requests",
    }
