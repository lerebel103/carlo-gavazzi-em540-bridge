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
    master_stats.acquisition_duration_ms_min = 9.5
    master_stats.acquisition_duration_ms_max = 10.5
    master_stats.acquisition_duration_ms_sum = 20.0
    master_stats.acquisition_duration_samples = 2
    diagnostics.set_em540_master_stats(master_stats)

    with patch.dict("sys.modules", {"uptime": SimpleNamespace(uptime=lambda: 1)}):
        _, payload = diagnostics.mqtt_data()
    payload_obj = json.loads(payload)

    assert payload_obj["em540_tcp_client_count"] == 3
    assert payload_obj["em540_tcp_client_disconnect_count"] == 11


def test_read_failed_count_is_published_from_master_stats():
    diagnostics = HADiagnostics(topic_prefix="test")

    master_stats = Em540MasterStats()
    master_stats.read_failed_total = 7
    diagnostics.set_em540_master_stats(master_stats)

    with patch.dict("sys.modules", {"uptime": SimpleNamespace(uptime=lambda: 1)}):
        _, payload = diagnostics.mqtt_data()
    payload_obj = json.loads(payload)

    assert payload_obj["rs485_master_read_failures"] == 7


def test_master_timing_payload_prefers_worst_case_values_over_last_cycle_values():
    diagnostics = HADiagnostics(topic_prefix="test")

    master_stats = Em540MasterStats()
    master_stats.acquisition_duration_ms_min = 8.0
    master_stats.acquisition_duration_ms_max = 14.0
    master_stats.acquisition_duration_ms_sum = 33.0
    master_stats.acquisition_duration_samples = 3
    master_stats.acquisition_headroom_ms_min = -6.0
    master_stats.acquisition_headroom_ms_max = 21.0
    master_stats.acquisition_headroom_ms_sum = 18.0
    master_stats.acquisition_headroom_samples = 3
    master_stats.tick_overrun_count = 7
    diagnostics.set_em540_master_stats(master_stats)

    with patch.dict("sys.modules", {"uptime": SimpleNamespace(uptime=lambda: 1)}):
        _, payload = diagnostics.mqtt_data()
    payload_obj = json.loads(payload)

    assert payload_obj["acq_dur_min"] == 8.0
    assert payload_obj["acq_dur_max"] == 14.0
    assert payload_obj["acq_dur_mean"] == 11.0
    assert payload_obj["acq_headroom_min"] == -6.0
    assert payload_obj["acq_headroom_max"] == 21.0
    assert payload_obj["acq_headroom_mean"] == 6.0
    assert payload_obj["tick_overruns"] == 7


def test_master_timing_extrema_reset_after_diagnostics_emit():
    diagnostics = HADiagnostics(topic_prefix="test")

    master_stats = Em540MasterStats()
    master_stats.acquisition_duration_ms_min = 10.0
    master_stats.acquisition_duration_ms_max = 12.0
    master_stats.acquisition_duration_ms_sum = 22.0
    master_stats.acquisition_duration_samples = 2
    master_stats.acquisition_headroom_ms_min = -5.0
    master_stats.acquisition_headroom_ms_max = 4.0
    master_stats.acquisition_headroom_ms_sum = -1.0
    master_stats.acquisition_headroom_samples = 2
    diagnostics.set_em540_master_stats(master_stats)

    with patch.dict("sys.modules", {"uptime": SimpleNamespace(uptime=lambda: 1)}):
        diagnostics.mqtt_data()

    assert master_stats.acquisition_duration_ms_min == 0.0
    assert master_stats.acquisition_duration_ms_max == 0.0
    assert master_stats.acquisition_duration_ms_sum == 0.0
    assert master_stats.acquisition_duration_samples == 0
    assert master_stats.acquisition_headroom_ms_min == 0.0
    assert master_stats.acquisition_headroom_ms_max == 0.0
    assert master_stats.acquisition_headroom_ms_sum == 0.0
    assert master_stats.acquisition_headroom_samples == 0


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

    with patch("app.home_assistant.ha_diagnostics.time.monotonic", side_effect=[100.0, 106.0]):
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
        "acq_rate",
        "tick_overruns",
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


def test_diagnostics_display_names_are_migrated_without_changing_entity_keys():
    diagnostics = HADiagnostics(topic_prefix="test")

    payloads = {topic: json.loads(payload) for topic, payload in diagnostics.advertise_data()}

    assert payloads["homeassistant/sensor/em540_bridge_test_acq_rate/config"]["name"] == "Acq Rate"
    assert payloads["homeassistant/sensor/em540_bridge_test_acq_dur_mean/config"]["name"] == "Acq Dur Mean"
    assert payloads["homeassistant/sensor/em540_bridge_test_acq_headroom_mean/config"]["name"] == "Acq Headroom Mean"


def test_daily_power_extrema_keep_legacy_entity_keys_with_daily_display_names():
    diagnostics = HADiagnostics(topic_prefix="test")

    payloads = {topic: json.loads(payload) for topic, payload in diagnostics.advertise_data()}

    # Legacy system-power extrema keep their historical entity keys (unique_id
    # derives from safe_name) but display as "Daily ...".
    min_cfg = payloads["homeassistant/sensor/em540_bridge_test_min_power_w/config"]
    max_cfg = payloads["homeassistant/sensor/em540_bridge_test_max_power_w/config"]
    assert min_cfg["name"] == "Daily Power Min"
    assert max_cfg["name"] == "Daily Power Max"
    assert min_cfg["enabled_by_default"] is False
    assert max_cfg["enabled_by_default"] is False


def test_all_32_daily_extrema_sensors_are_declared_and_disabled_by_default():
    diagnostics = HADiagnostics(topic_prefix="test")

    extrema = diagnostics._daily_extrema_sensors
    assert len(extrema) == 32
    assert all(not s.enabled_by_default for s in extrema.values())
    assert all(s.entity_category == "diagnostic" for s in extrema.values())

    expected_keys = set()
    for quantity in ("power", "current", "voltage_ln", "voltage_ll"):
        for scope in ("", "_l1", "_l2", "_l3"):
            for bound in ("_min", "_max"):
                expected_keys.add(f"{quantity}{scope}{bound}")
    assert set(extrema.keys()) == expected_keys


def test_daily_extrema_values_are_pulled_from_master_snapshot():
    diagnostics = HADiagnostics(topic_prefix="test")

    class _FakeExtrema:
        def snapshot(self):
            return {
                "power_min": -1500.0,
                "power_max": 3200.0,
                "current_l1_max": 12.5,
                "voltage_ln_min": 228.4,
                # unset extrema report None and must be skipped
                "voltage_ll_l3_min": None,
            }

    diagnostics.set_daily_extrema_source(_FakeExtrema())

    with patch.dict("sys.modules", {"uptime": SimpleNamespace(uptime=lambda: 1)}):
        _, payload = diagnostics.mqtt_data()
    payload_obj = json.loads(payload)

    # Payload keys are the sensor safe_names. System power keeps its legacy
    # keys; the rest are namespaced under "daily_".
    assert payload_obj["min_power_w"] == -1500.0
    assert payload_obj["max_power_w"] == 3200.0
    assert payload_obj["daily_current_l1_max"] == 12.5
    assert payload_obj["daily_voltage_l_n_min"] == 228.4


def test_daily_extrema_source_absent_does_not_error():
    diagnostics = HADiagnostics(topic_prefix="test")

    with patch.dict("sys.modules", {"uptime": SimpleNamespace(uptime=lambda: 1)}):
        # No source registered; mqtt_data must still succeed.
        _, payload = diagnostics.mqtt_data()

    assert json.loads(payload) is not None
