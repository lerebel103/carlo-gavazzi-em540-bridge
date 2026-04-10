"""Tests for HABridge — validates sensor/config entity integration."""

import unittest
from unittest.mock import MagicMock

from app.carlo_gavazzi.meter_data import MeterData
from app.config import AppState, ConfigManager, MqttConfig
from app.home_assistant.ha_bridge import HABridge
from app.home_assistant.ha_sensors import HA_AVAILABILITY_TOPIC


def _make_conf() -> MqttConfig:
    return MqttConfig(
        host="localhost",
        port=1883,
        username="user",
        password="pass",
        update_interval=10,
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

    def test_new_data_triggers_update(self):
        data = MeterData()
        data._timestamp = 100
        data.system.frequency = 49.9
        data.system.power = 1234.0
        data.phases[0].current = 1.2
        data.other_energies.kwh_plus_total = 12.3
        self.bridge._last_update = 80
        self.bridge._update_interval = 10
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


if __name__ == "__main__":
    unittest.main()
