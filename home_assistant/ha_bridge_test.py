import unittest
from unittest.mock import MagicMock

from carlo_gavazzi.meter_data import MeterData
from home_assistant.ha_bridge import HABridge


class TestHABridge(unittest.TestCase):
    def setUp(self):
        conf = MagicMock()
        conf.host = "localhost"
        conf.port = 1883
        conf.username = "user"
        conf.password = "pass"
        conf.update_interval = 10
        conf.log_level = "DEBUG"
        self.bridge = HABridge(conf)

        # Mock the connect call on paho mqtt client
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


if __name__ == "__main__":
    unittest.main()
