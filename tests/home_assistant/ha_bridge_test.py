"""Tests for HABridge — validates sensor/config entity integration.

Pre-patches sys.modules for ha_sensors (uses Python 3.10+ syntax) so that
`import home_assistant.ha_bridge` succeeds on Python 3.9.
"""
import sys
import unittest
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Pre-patch modules that use Python 3.10+ syntax (str | None)
# ---------------------------------------------------------------------------
_ha_mock = MagicMock()
for _mod in [
    "home_assistant.ha_sensors",
    "home_assistant.ha_diagnostics",
    "app.home_assistant.ha_sensors",
    "app.home_assistant.ha_diagnostics",
]:
    if _mod not in sys.modules:
        sys.modules[_mod] = _ha_mock

from app.carlo_gavazzi.meter_data import MeterData  # noqa: E402
from app.config import AppState, ConfigManager, MqttConfig  # noqa: E402
from app.home_assistant.ha_bridge import HABridge  # noqa: E402


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
        self.bridge.client.connect = MagicMock()
        self.bridge.client.loop_start = MagicMock()

    def test_publish_when_connected(self):
        self.bridge.connected = True
        self.bridge.client.publish = MagicMock()
        self.bridge.publish("test/topic", "payload", retain=True)
        self.bridge.client.publish.assert_called_with(
            "test/topic", "payload", retain=True
        )

    def test_publish_when_not_connected(self):
        self.bridge.connected = False
        self.bridge.client.publish = MagicMock()
        self.bridge.publish("test/topic", "payload", retain=True)
        self.bridge.client.publish.assert_not_called()

    def test_advertise_calls_publish(self):
        self.bridge.sensors.advertise_data = MagicMock(
            return_value=[("topic1", "msg1")]
        )
        self.bridge._diagnostics.advertise_data = MagicMock(
            return_value=[("topic2", "msg2")]
        )
        self.bridge.publish = MagicMock()
        self.bridge.advertise()
        self.bridge.publish.assert_any_call("topic1", "msg1", retain=True)
        self.bridge.publish.assert_any_call("topic2", "msg2", retain=True)

    def test_new_data_triggers_update(self):
        data = MeterData()
        data._timestamp = 100
        self.bridge._last_update = 80
        self.bridge._update_interval = 10
        self.bridge.sensors.update = MagicMock()
        self.bridge._condition = MagicMock()
        import asyncio

        asyncio.run(self.bridge.new_data(data))
        self.bridge.sensors.update.assert_called_with(data)
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


if __name__ == "__main__":
    unittest.main()
