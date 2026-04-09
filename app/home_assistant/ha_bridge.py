from __future__ import annotations

import logging
import random
import sys
import threading
import time
from datetime import datetime

from paho.mqtt import client as mqtt_client
from paho.mqtt.enums import CallbackAPIVersion

from app.carlo_gavazzi.em540_master import MeterDataListener
from app.carlo_gavazzi.em540_slave_stats import EM540SlaveStats
from app.carlo_gavazzi.meter_data import MeterData
from app.config import AppState, ConfigManager, MqttConfig
from app.fronius.ts65a_slave_stats import Ts65aSlaveStats
from app.home_assistant.ha_config_entities import HAConfigEntities
from app.home_assistant.ha_diagnostics import DIAGNOSTICS_INTERVAL, HADiagnostics
from app.home_assistant.ha_sensors import EnergyMeterSensor

FIRST_RECONNECT_DELAY = 1
RECONNECT_RATE = 2
MAX_RECONNECT_COUNT = sys.maxsize
MAX_RECONNECT_DELAY = 60

logger = logging.getLogger("ha-bridge")


class HABridge(MeterDataListener):
    """Represents a MQTT bridge to Home Assistant
    Sensors are defined in ha_sensors.py
    """

    def __init__(
        self,
        conf: MqttConfig,
        state: AppState | None = None,
        config_manager: ConfigManager | None = None,
    ):
        client_id = f"publish-{random.randint(0, 1000)}"
        self.host = conf.host
        self.port = conf.port
        self.client = mqtt_client.Client(
            CallbackAPIVersion.VERSION2, client_id, userdata=self
        )
        self.client.username_pw_set(conf.username, conf.password)
        self.client.on_connect = HABridge.on_connect
        self.client.on_disconnect = HABridge.on_disconnect
        self.connected = False
        self._update_interval = conf.update_interval
        self._last_update = 0
        self.sensors = EnergyMeterSensor()
        self._diagnostics = HADiagnostics()
        self._last_stats_update: float = 0
        self._config_entities: HAConfigEntities | None = None

        if state is not None and config_manager is not None:
            self._config_entities = HAConfigEntities(
                state, self.client, config_manager
            )

        # Background thread to handle updates outside of asyncio loop
        self._condition: threading.Condition = threading.Condition()
        self._notify_thread: threading.Thread = threading.Thread(
            target=self._notify_loop, daemon=True
        )
        self._notify_thread.start()

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

            # Subscribe config entities and publish current state values
            if userdata._config_entities is not None:
                userdata._config_entities.subscribe()
                for entity in userdata._config_entities._entities:
                    value = getattr(entity.config_section, entity.field_name)
                    state_topic = f"lerebel/config/em540_bridge/{entity.safe_name}/state"
                    client.publish(state_topic, str(value), retain=True)
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
            with self._condition:
                self._condition.notify_all()

    async def read_failed(self):
        self._diagnostics.read_failed()

    def advertise(self):
        payloads = self.sensors.advertise_data()
        payloads.extend(self._diagnostics.advertise_data())

        if self._config_entities is not None:
            payloads.extend(self._config_entities.advertise())

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

    def _notify_loop(self) -> None:
        """Background thread to publish sensor data when notified of new data"""
        while True:
            with self._condition:
                self._condition.wait()

                # Now publish all sensor data
                topic, payload = self.sensors.mqtt_data()
                try:
                    self.publish(topic, payload)
                except Exception as err:
                    logger.error(
                        f"Failed to publish sensor data on topic {topic}: {err}"
                    )

                # Do the same with diagnostics, if we are ready for an update
                now = datetime.now().timestamp()
                if (
                    self._last_stats_update == 0
                    or (now - self._last_stats_update) > DIAGNOSTICS_INTERVAL
                ):
                    self._last_stats_update = now
                    topic, payload = self._diagnostics.mqtt_data()
                    try:
                        self.publish(topic, payload)
                    except Exception as err:
                        logger.error(
                            f"Failed to publish diagnostics data on topic {topic}: {err}"
                        )
