"""Tests for HABridge — validates sensor/config entity integration."""

import unittest
from unittest.mock import MagicMock

from app.carlo_gavazzi.meter_data import MeterData
from app.config import AppState, ConfigManager, MqttConfig
from app.home_assistant.ha_bridge import HABridge
from app.home_assistant.ha_sensors import HA_AVAILABILITY_TOPIC


def _make_conf(topic_prefix: str = "") -> MqttConfig:
    return MqttConfig(
        host="localhost",
        port=1883,
        username="user",
        password="pass",
        update_interval=10,
        ha_topic_prefix=topic_prefix,
        log_level="DEBUG",
    )


class TestHABridge(unittest.TestCase):
    def setUp(self):
        self.bridge = HABridge(_make_conf())
        self.bridge.client.connect_async = MagicMock()
        self.bridge.client.loop_start = MagicMock()

    def tearDown(self):
        self.bridge.stop()

    def test_connect_schedules_background_connection(self):
        self.bridge.connect()

        self.bridge.client.loop_start.assert_called_once()
        self.bridge.client.connect_async.assert_called_once_with("localhost", 1883)

    def test_connect_swallow_initial_connection_error(self):
        self.bridge.client.connect_async.side_effect = RuntimeError("broker unavailable")

        self.bridge.connect()

        self.bridge.client.loop_start.assert_called_once()

    def test_publish_when_connected(self):
        self.bridge.connected = True
        self.bridge.client.publish = MagicMock()
        self.bridge.publish("test/topic", "payload", retain=True)
        self.bridge.client.publish.assert_called_with("test/topic", "payload", retain=True)

    def test_publish_when_not_connected(self):
        self.bridge.connected = False
        self.bridge.client.publish = MagicMock()
        self.bridge.publish("test/topic", "payload", retain=True)
        self.bridge.client.publish.assert_not_called()

    def test_disconnect_callback_does_not_attempt_blocking_reconnect(self):
        client = MagicMock()
        self.bridge._last_payload_by_topic["test/topic"] = "payload"

        HABridge.on_disconnect(client, self.bridge, None, 1, None)

        self.assertFalse(self.bridge.connected)
        self.assertEqual(self.bridge._last_payload_by_topic, {})
        client.reconnect.assert_not_called()

    def test_stop_stops_background_mqtt_loop(self):
        self.bridge._loop_started = True
        self.bridge.client.disconnect = MagicMock()
        self.bridge.client.loop_stop = MagicMock()

        self.bridge.stop()

        self.bridge.client.disconnect.assert_called_once()
        self.bridge.client.loop_stop.assert_called_once()
        self.assertFalse(self.bridge._loop_started)

    def test_advertise_calls_publish(self):
        self.bridge.sensors.advertise_data = MagicMock(return_value=[("topic1", "msg1")])
        self.bridge._diagnostics.advertise_data = MagicMock(return_value=[("topic2", "msg2")])
        self.bridge.publish = MagicMock()
        self.bridge.advertise()
        self.bridge.publish.assert_any_call("topic1", "msg1", retain=True)
        self.bridge.publish.assert_any_call("topic2", "msg2", retain=True)

    def test_on_connect_publishes_current_availability_state(self):
        self.bridge.sensors.advertise_data = MagicMock(return_value=[])
        self.bridge._diagnostics.advertise_data = MagicMock(return_value=[])
        self.bridge._data_available = False

        mock_client = MagicMock()

        HABridge.on_connect(mock_client, self.bridge, None, 0, None)

        mock_client.publish.assert_any_call(HA_AVAILABILITY_TOPIC, "offline", retain=True)

    def test_set_daily_extrema_source_forwards_to_diagnostics(self):
        self.bridge._diagnostics.set_daily_extrema_source = MagicMock()
        source = MagicMock()

        self.bridge.set_daily_extrema_source(source)

        self.bridge._diagnostics.set_daily_extrema_source.assert_called_once_with(source)

    def test_new_data_triggers_update(self):
        data = MeterData()
        data._timestamp = 100
        data.system.frequency = 49.9
        data.system.power = 1234.0
        data.phases[0].current = 1.2
        data.other_energies.kwh_plus_total = 12.3
        # Set valid serial number to make static_data_valid return True
        data.frame.static_reg_map[0x5000].values = [0x3142, 0x3344, 0x3546, 0x3748, 0x3949, 0x4141, 0x4242, 0x4343]
        self.bridge.sensors.update = MagicMock()
        self.bridge._diagnostics.new_data = MagicMock()
        self.bridge._condition = MagicMock()
        import asyncio

        asyncio.run(self.bridge.new_data(data))

        self.assertEqual(self.bridge._front_snapshot.timestamp, 100)
        self.assertEqual(self.bridge._front_snapshot.system.frequency, 49.9)
        self.assertEqual(self.bridge._front_snapshot.system.power, 1234.0)
        self.assertEqual(self.bridge._front_snapshot.phases[0].current, 1.2)
        self.assertEqual(self.bridge._front_snapshot.other_energies.kwh_plus_total, 12.3)
        self.assertTrue(self.bridge._snapshot_pending)
        self.assertTrue(self.bridge._data_available)
        self.assertTrue(self.bridge._availability_dirty)
        self.bridge.sensors.update.assert_not_called()
        self.bridge._diagnostics.new_data.assert_not_called()
        self.assertEqual(self.bridge._condition.notify_all.call_count, 2)

    def test_sensor_update_interval_reads_live_config_value(self):
        self.bridge._mqtt_config.update_interval = 0.75

        self.assertEqual(self.bridge._sensor_update_interval(), 0.75)

    def test_advance_publish_deadline_skips_missed_intervals_without_drift(self):
        next_deadline = self.bridge._advance_publish_deadline(10.0, 0.5, 11.3)

        self.assertEqual(next_deadline, 11.5)

    def test_next_due_uses_immediate_publish_until_deadlines_are_initialized(self):
        self.bridge._data_available = True
        self.bridge._front_snapshot.timestamp = 123.0

        self.assertEqual(self.bridge._next_due_monotonic(50.0), 50.0)

    def test_set_data_available_false_resets_publish_schedules(self):
        self.bridge._data_available = True
        self.bridge._next_sensor_publish_monotonic = 12.0
        self.bridge._next_diagnostics_publish_monotonic = 17.0
        self.bridge._condition = MagicMock()

        self.bridge._set_data_available(False)

        self.assertEqual(self.bridge._next_sensor_publish_monotonic, 0.0)
        self.assertEqual(self.bridge._next_diagnostics_publish_monotonic, 0.0)

    def test_read_failed_marks_data_unavailable(self):
        import asyncio

        self.bridge._data_available = True
        self.bridge._condition = MagicMock()
        self.bridge._diagnostics.read_failed = MagicMock()

        asyncio.run(self.bridge.read_failed())

        self.bridge._diagnostics.read_failed.assert_called_once()
        self.assertFalse(self.bridge._data_available)
        self.assertTrue(self.bridge._availability_dirty)
        self.bridge._condition.notify_all.assert_called_once()

    # -- HA/MQTT wall-clock freshness watchdog --

    def test_freshness_watchdog_goes_offline_when_snapshot_too_old(self):
        # Data available, a frame was received, but its wall-clock age exceeds
        # ha_max_stale_s: the watchdog must mark the bridge offline WITHOUT any
        # read_failed() call. Mock the condition so _set_data_available doesn't
        # need a real lock; capture that the offline transition path fires.
        self.bridge._mqtt_config.ha_max_stale_s = 3.0
        self.bridge._data_available = True
        self.bridge._condition = MagicMock()
        snapshot = self.bridge._front_snapshot
        snapshot.timestamp = 100.0

        # now is 4s later -> age 4.0 > 3.0
        self.bridge._evaluate_freshness_locked(snapshot, 104.0)

        self.assertFalse(self.bridge._data_available)
        self.assertTrue(self.bridge._availability_dirty)
        self.bridge._condition.notify_all.assert_called_once()

    def test_freshness_watchdog_does_not_trip_when_fresh(self):
        self.bridge._mqtt_config.ha_max_stale_s = 3.0
        self.bridge._data_available = True
        self.bridge._condition = MagicMock()
        snapshot = self.bridge._front_snapshot
        snapshot.timestamp = 100.0

        # age 2.0 <= 3.0 -> stays online
        self.bridge._evaluate_freshness_locked(snapshot, 102.0)

        self.assertTrue(self.bridge._data_available)
        self.assertFalse(self.bridge._availability_dirty)

    def test_freshness_watchdog_does_not_spam_offline(self):
        # Repeated evaluations while already stale must not re-publish offline:
        # _set_data_available(False) early-returns once already offline, so no
        # further notify_all / availability_dirty churn occurs after the first.
        self.bridge._mqtt_config.ha_max_stale_s = 3.0
        self.bridge._data_available = True
        self.bridge._condition = MagicMock()
        snapshot = self.bridge._front_snapshot
        snapshot.timestamp = 100.0

        self.bridge._evaluate_freshness_locked(snapshot, 104.0)
        self.assertFalse(self.bridge._data_available)
        self.assertEqual(self.bridge._condition.notify_all.call_count, 1)

        # Clear the dirty flag as the notify loop would after publishing, then
        # evaluate again while still stale: must be a no-op (idempotent).
        self.bridge._availability_dirty = False
        self.bridge._evaluate_freshness_locked(snapshot, 110.0)

        self.assertFalse(self.bridge._data_available)
        self.assertFalse(self.bridge._availability_dirty)
        self.assertEqual(self.bridge._condition.notify_all.call_count, 1)

    def test_freshness_watchdog_recovers_on_fresh_frame(self):
        import asyncio

        # Start stale/offline.
        self.bridge._mqtt_config.ha_max_stale_s = 3.0
        self.bridge._data_available = True
        self.bridge._condition = MagicMock()
        snapshot = self.bridge._front_snapshot
        snapshot.timestamp = 100.0
        self.bridge._evaluate_freshness_locked(snapshot, 104.0)
        self.assertFalse(self.bridge._data_available)

        # A genuinely fresh frame restores availability and publishing resumes.
        data = MeterData()
        data._timestamp = 200.0
        data.frame.static_reg_map[0x5000].values = [
            0x3142,
            0x3344,
            0x3546,
            0x3748,
            0x3949,
            0x4141,
            0x4242,
            0x4343,
        ]
        asyncio.run(self.bridge.new_data(data))
        self.assertTrue(self.bridge._data_available)

        # Evaluating the fresh snapshot shortly after keeps it online.
        self.bridge._evaluate_freshness_locked(self.bridge._front_snapshot, 200.5)
        self.assertTrue(self.bridge._data_available)

    def test_freshness_watchdog_disabled_when_threshold_non_positive(self):
        # ha_max_stale_s <= 0 disables the watchdog: never trips regardless of age.
        self.bridge._mqtt_config.ha_max_stale_s = 0
        self.bridge._data_available = True
        self.bridge._condition = MagicMock()
        snapshot = self.bridge._front_snapshot
        snapshot.timestamp = 100.0

        self.bridge._evaluate_freshness_locked(snapshot, 1_000_000.0)

        self.assertTrue(self.bridge._data_available)
        self.assertFalse(self.bridge._availability_dirty)

    def test_freshness_watchdog_ignores_pre_first_frame(self):
        # Before any frame (timestamp == 0) the watchdog must not trip, preserving
        # startup semantics.
        self.bridge._mqtt_config.ha_max_stale_s = 3.0
        self.bridge._data_available = True
        self.bridge._condition = MagicMock()
        snapshot = self.bridge._front_snapshot
        snapshot.timestamp = 0.0

        self.bridge._evaluate_freshness_locked(snapshot, 1_000_000.0)

        self.assertTrue(self.bridge._data_available)
        self.assertFalse(self.bridge._availability_dirty)

    # -- Finding 3: a concurrent recovery must win over a late offline flip --

    def test_freshness_watchdog_recovery_wins_over_stale_capture(self):
        # Reproduce the interleaving the loop must survive: a fresh new_data()
        # swaps in a current snapshot around the same iteration the watchdog would
        # otherwise expire an OLDER snapshot. Because the loop now evaluates the
        # LATEST front snapshot under the lock (not a copy captured earlier), the
        # fresh frame wins and the bridge stays ONLINE.
        import asyncio

        self.bridge._mqtt_config.ha_max_stale_s = 3.0
        self.bridge._data_available = True
        self.bridge._condition = MagicMock()

        # An older stale snapshot is the front buffer; capturing THIS copy and
        # evaluating it after release would flip the bridge offline.
        stale_copy = self.bridge._front_snapshot
        stale_copy.timestamp = 100.0

        # A genuinely fresh frame arrives (swapped into _front_snapshot, sets
        # _data_available True) before the watchdog evaluation runs.
        data = MeterData()
        data._timestamp = 200.0
        data.frame.static_reg_map[0x5000].values = [
            0x3142,
            0x3344,
            0x3546,
            0x3748,
            0x3949,
            0x4141,
            0x4242,
            0x4343,
        ]
        asyncio.run(self.bridge.new_data(data))
        self.assertTrue(self.bridge._data_available)

        # The loop evaluates the CURRENT front snapshot (fresh, ts=200) at a
        # wall-clock only slightly later: it must NOT trip, so recovery wins.
        self.bridge._evaluate_freshness_locked(self.bridge._front_snapshot, 200.5)
        self.assertTrue(self.bridge._data_available)

        # Sanity: evaluating the STALE captured copy at the same instant WOULD
        # have tripped — confirming the recovery-wins behaviour comes precisely
        # from re-reading the latest snapshot rather than the captured one.
        self.bridge._evaluate_freshness_locked(stale_copy, 200.5)
        self.assertFalse(self.bridge._data_available)

    def test_freshness_watchdog_still_trips_when_genuinely_stale(self):
        # The recovery-wins fix must not weaken genuine staleness detection: when
        # the latest front snapshot really is too old, the watchdog still flips
        # offline.
        self.bridge._mqtt_config.ha_max_stale_s = 3.0
        self.bridge._data_available = True
        self.bridge._condition = MagicMock()
        self.bridge._front_snapshot.timestamp = 100.0

        self.bridge._evaluate_freshness_locked(self.bridge._front_snapshot, 105.0)

        self.assertFalse(self.bridge._data_available)
        self.assertTrue(self.bridge._availability_dirty)

    # -- Finding 4: breaker/health diagnostics publish even while offline --

    def _run_one_notify_pass(self):
        """Drive exactly one iteration of the real _notify_loop body on THIS thread.

        First stops the auto-started background notify thread so it cannot race
        for the pending snapshot, then re-arms a fresh stop event that fires after
        a single publish pass and pre-sets _snapshot_pending so the inner wait
        breaks immediately.
        """
        import threading

        # Quiesce the background thread started in setUp so it does not consume
        # the pending flag or publish concurrently with this deterministic pass.
        self.bridge._stop_event.set()
        with self.bridge._condition:
            self.bridge._condition.notify_all()
        self.bridge._notify_thread.join(timeout=1)

        self.bridge._snapshot_pending = True
        calls = {"n": 0}

        def fake_is_set():
            # is_set() is checked three times before the publish block runs:
            # the outer-loop guard, the inner wait-break guard, and the
            # post-wait early-return guard. Keep all three False so the single
            # publish pass runs, then return True on the next outer-loop guard so
            # the loop exits cleanly.
            calls["n"] += 1
            return calls["n"] > 3

        self.bridge._stop_event = threading.Event()
        self.bridge._stop_event.is_set = fake_is_set  # type: ignore[assignment]
        self.bridge._notify_loop()

    def test_diagnostics_publish_while_offline_carries_breaker_open(self):
        # Finding 4: the master sets stale_frame_breaker_open=1 immediately before
        # read_failed() flips the bridge offline. Diagnostics must keep flowing
        # while offline so the breaker-open sensor is visible exactly when it
        # matters, but stale MEASUREMENT values must NOT be published as fresh.
        # Disable the wall-clock watchdog so the real time.time() in the loop
        # does not independently flip availability during this deterministic pass.
        self.bridge._mqtt_config.ha_max_stale_s = 0
        self.bridge._data_available = False  # offline (breaker tripped upstream)
        self.bridge._front_snapshot.timestamp = 500.0  # a frame has been seen
        self.bridge._next_diagnostics_publish_monotonic = 0.0  # due immediately
        self.bridge._next_sensor_publish_monotonic = 0.0

        self.bridge._diagnostics.mqtt_data = MagicMock(return_value=("diag/topic", '{"stale_frame_breaker_open": 1}'))
        self.bridge.sensors.mqtt_data = MagicMock(return_value=("sensor/topic", "{}"))
        published: list[tuple] = []
        self.bridge.publish = lambda topic, payload, retain=False: published.append((topic, payload))

        self._run_one_notify_pass()

        # Diagnostics WERE published while offline, carrying the breaker-open flag.
        self.assertIn(("diag/topic", '{"stale_frame_breaker_open": 1}'), published)
        self.bridge._diagnostics.mqtt_data.assert_called()
        # Measurement sensor values were NOT published as fresh while offline.
        self.bridge.sensors.mqtt_data.assert_not_called()
        self.assertNotIn(("sensor/topic", "{}"), published)

    def test_measurements_publish_again_after_recovery(self):
        # Once online again, measurement values resume publishing alongside
        # diagnostics (confirms offline-only suppression of measurements).
        self.bridge._mqtt_config.ha_max_stale_s = 0  # watchdog off for determinism
        self.bridge._data_available = True
        self.bridge._front_snapshot.timestamp = 500.0
        self.bridge._next_diagnostics_publish_monotonic = 0.0
        self.bridge._next_sensor_publish_monotonic = 0.0

        self.bridge._diagnostics.mqtt_data = MagicMock(return_value=("diag/topic", "{}"))
        self.bridge.sensors.mqtt_data = MagicMock(return_value=("sensor/topic", "{}"))
        published: list[tuple] = []
        self.bridge.publish = lambda topic, payload, retain=False: published.append((topic, payload))

        self._run_one_notify_pass()

        self.assertIn(("sensor/topic", "{}"), published)
        self.assertIn(("diag/topic", "{}"), published)


class TestHABridgeConfigEntities(unittest.TestCase):
    """Tests for HAConfigEntities integration in HABridge."""

    def test_no_config_entities_without_state(self):
        """Config entities are None when state/config_manager not provided."""
        bridge = HABridge(_make_conf())
        self.assertIsNone(bridge._config_entities)

    def test_config_entities_created_with_state(self):
        """Config entities are created when state and config_manager provided."""
        state = AppState()
        cm = MagicMock(spec=ConfigManager)
        bridge = HABridge(_make_conf(), state=state, config_manager=cm)
        self.assertIsNotNone(bridge._config_entities)

    def test_advertise_includes_config_entity_payloads(self):
        """advertise() includes config entity discovery payloads."""
        state = AppState()
        cm = MagicMock(spec=ConfigManager)
        bridge = HABridge(_make_conf(), state=state, config_manager=cm)
        bridge.sensors.advertise_data = MagicMock(return_value=[])
        bridge._diagnostics.advertise_data = MagicMock(return_value=[])
        bridge.publish = MagicMock()

        bridge.advertise()

        # Should have published config entity payloads (one per PERSISTED_FIELD)
        self.assertTrue(bridge.publish.call_count > 0)

    def test_on_connect_subscribes_config_entities(self):
        """on_connect subscribes config entities and publishes state values."""
        state = AppState()
        cm = MagicMock(spec=ConfigManager)
        bridge = HABridge(_make_conf(), state=state, config_manager=cm)
        bridge.sensors.advertise_data = MagicMock(return_value=[])
        bridge._diagnostics.advertise_data = MagicMock(return_value=[])
        bridge._config_entities.subscribe = MagicMock()

        mock_client = MagicMock()
        # Simulate on_connect callback
        HABridge.on_connect(mock_client, bridge, None, 0, None)

        bridge._config_entities.subscribe.assert_called_once()
        # Should have published state values for each entity
        self.assertTrue(mock_client.publish.called)

    def test_on_connect_publishes_master_interval_config_state_in_ms(self):
        state = AppState()
        state.em540_master.update_interval = 0.125
        state.em540_master.timeout = 0.25
        cm = MagicMock(spec=ConfigManager)
        bridge = HABridge(_make_conf(), state=state, config_manager=cm)
        bridge.sensors.advertise_data = MagicMock(return_value=[])
        bridge._diagnostics.advertise_data = MagicMock(return_value=[])

        mock_client = MagicMock()

        HABridge.on_connect(mock_client, bridge, None, 0, None)

        mock_client.publish.assert_any_call(
            "lerebel/config/em540_bridge/em540_master_update_interval/state",
            "125.0",
            retain=True,
        )
        mock_client.publish.assert_any_call(
            "lerebel/config/em540_bridge/em540_master_timeout/state",
            "250.0",
            retain=True,
        )

    def test_on_connect_republishes_online_availability_after_recovery(self):
        state = AppState()
        cm = MagicMock(spec=ConfigManager)
        bridge = HABridge(_make_conf(), state=state, config_manager=cm)
        bridge.sensors.advertise_data = MagicMock(return_value=[])
        bridge._diagnostics.advertise_data = MagicMock(return_value=[])
        bridge._data_available = True

        mock_client = MagicMock()

        HABridge.on_connect(mock_client, bridge, None, 0, None)

        mock_client.publish.assert_any_call(HA_AVAILABILITY_TOPIC, "online", retain=True)
        bridge.stop()

    def test_on_connect_uses_prefixed_availability_topic(self):
        state = AppState()
        cm = MagicMock(spec=ConfigManager)
        bridge = HABridge(_make_conf(topic_prefix="qa/test-stack"), state=state, config_manager=cm)
        bridge.sensors.advertise_data = MagicMock(return_value=[])
        bridge._diagnostics.advertise_data = MagicMock(return_value=[])
        bridge._data_available = True

        mock_client = MagicMock()

        HABridge.on_connect(mock_client, bridge, None, 0, None)

        mock_client.publish.assert_any_call(
            "qa/test-stack/lerebel/sensor/em540_energy_meter_bridge/availability",
            "online",
            retain=True,
        )
        bridge.stop()


if __name__ == "__main__":
    unittest.main()
