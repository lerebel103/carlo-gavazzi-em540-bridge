import logging
import sys

from paho.mqtt import client as mqtt_client
import random
import time

from paho.mqtt.enums import CallbackAPIVersion

from em540_master import MeterDataListener
from em540_slave_bridge import EM540SlaveStats
from ha_diagnostics import HADiagnostics
from ha_sensors import EnergyMeterSensor
from meter_data import MeterData
from ts65a_slave_bridge import Ts65aSlaveStats

FIRST_RECONNECT_DELAY = 1
RECONNECT_RATE = 2
MAX_RECONNECT_COUNT = sys.maxsize
MAX_RECONNECT_DELAY = 60

logger = logging.getLogger('ha-bridge')


class HABridge(MeterDataListener):
    """ Represents a MQTT bridge to Home Assistant
    Sensors are defined in ha_sensors.py
    """
    def __init__(self, conf):
        client_id = f'publish-{random.randint(0, 1000)}'
        self.host = conf.host
        self.port = conf.port
        self.client = mqtt_client.Client(CallbackAPIVersion.VERSION2, client_id, userdata=self)
        self.client.username_pw_set(conf.username, conf.password)
        self.client.on_connect = HABridge.on_connect
        self.client.on_disconnect = HABridge.on_disconnect
        self.connected = False
        self._update_interval = conf.update_interval
        self._last_update = 0
        self.sensors = EnergyMeterSensor()
        self._diagnostics = HADiagnostics()

        logger.setLevel(conf.log_level)


    def connect(self):
        if len(self.host):
            self.client.connect(self.host, self.port)
            self.client.loop_start()

    @staticmethod
    def on_disconnect(client, userdata, flags, rc, props):
        userdata.connected = False
        logger.warning(f"Disconnected with result code: {rc}")
        reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY
        while reconnect_count < MAX_RECONNECT_COUNT:
            logger.info(f"Reconnecting in {reconnect_delay} seconds...")
            time.sleep(reconnect_delay)

            try:
                client.reconnect()
                logger.info("Reconnected successfully!")
                return
            except Exception as err:
                logger.info(f"{err}. Reconnect failed. Retrying...")

            reconnect_delay *= RECONNECT_RATE
            reconnect_delay = min(reconnect_delay, MAX_RECONNECT_DELAY)
            reconnect_count += 1
        logger.info(f"Reconnect failed after {reconnect_count} attempts. Exiting...")

    @staticmethod
    def on_connect(client, userdata, flags, rc, props):
        if rc == 0:
            logger.info("Connected to MQTT Broker")
            userdata.connected = True

            # Great advertise sensors
            userdata.advertise()
        else:
            logger.info("Failed to connect, return code %d\n", rc)

    def publish(self, topic, msg, retain=False):
        if self.connected:
            self.client.publish(topic, msg, retain=retain)

    async def new_data(self, data: MeterData):
        self._diagnostics.new_data(data)

        # Update sensor if enough time has passed
        if data.timestamp - self._last_update > self._update_interval:
            self._last_update = data.timestamp

            self.sensors.update(data)

            # Now publish all sensor data
            topic, payload = self.sensors.mqtt_data()
            try:
               self.publish(topic, payload)
            except Exception as err:
                logger.error(f"Failed to publish sensor data on topic {topic}: {err}")

            # Do the same with diagnostics
            topic, payload = self._diagnostics.mqtt_data()
            try:
               self.publish(topic, payload)
            except Exception as err:
                logger.error(f"Failed to publish diagnostics data on topic {topic}: {err}")

    async def read_failed(self):
        self._diagnostics.read_failed()

    def advertise(self):
        payloads = self.sensors.advertise_data()
        payloads.extend(self._diagnostics.advertise_data())

        for payload in payloads:
            topic, msg = payload
            logger.debug(f"Advertising sensor on topic {topic} with payload {msg}")

            try:
                # We want retained messages for discovery
                self.publish(topic, msg, retain=True)
            except Exception as err:
                logger.error(f"Failed to advertise sensor on topic {topic}: {err}")


    def on_ts65a_slave_stats(self, stats: Ts65aSlaveStats):
        self._diagnostics.set_ts_65a_slave_stats(stats)

    def on_em540_slave_stats(self, stats: EM540SlaveStats):
        self._diagnostics.set_em540_slave_stats(stats)