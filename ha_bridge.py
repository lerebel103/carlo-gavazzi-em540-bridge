import logging
import sys

from paho.mqtt import client as mqtt_client
import random
import time

from paho.mqtt.enums import CallbackAPIVersion

from em540_master import MeterDataListener
from ha_sensors import EnergyMeterSensor
from meter_data import MeterData

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
        logger.setLevel(conf.log_level)

        self.sensors = EnergyMeterSensor()

    def connect(self):
        if len(self.host):
            self.client.connect(self.host, self.port)
            self.client.loop_start()

    @staticmethod
    def on_disconnect(client, userdata, flags, rc, props):
        userdata.connected = False
        print("Disconnected with result code: %s", rc)
        reconnect_count, reconnect_delay = 0, FIRST_RECONNECT_DELAY
        while reconnect_count < MAX_RECONNECT_COUNT:
            print("Reconnecting in %d seconds...", reconnect_delay)
            time.sleep(reconnect_delay)

            try:
                client.reconnect()
                print("Reconnected successfully!")
                return
            except Exception as err:
                print("%s. Reconnect failed. Retrying...", err)

            reconnect_delay *= RECONNECT_RATE
            reconnect_delay = min(reconnect_delay, MAX_RECONNECT_DELAY)
            reconnect_count += 1
        print("Reconnect failed after %s attempts. Exiting...", reconnect_count)

    @staticmethod
    def on_connect(client, userdata, flags, rc, props):
        if rc == 0:
            print("Connected to MQTT Broker")
            userdata.connected = True

            # Great advertise sensors
            userdata.advertise()
        else:
            print("Failed to connect, return code %d\n", rc)

    def publish(self, topic, msg):
        if self.connected:
            self.client.publish(topic, msg)

    async def new_data(self, data: MeterData):
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

    async def read_failed(self):
        pass

    def advertise(self):
        payloads = self.sensors.advertise_data()
        for payload in payloads:
            topic, msg = payload
            logger.info(f"Advertising sensor on topic {topic} with payload {msg}")

            try:
                self.publish(topic, msg)
            except Exception as err:
                logger.error(f"Failed to advertise sensor on topic {topic}: {err}")

