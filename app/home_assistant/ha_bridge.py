from __future__ import annotations

import logging
import random
import threading
from datetime import datetime

from paho.mqtt import client as mqtt_client
from paho.mqtt.enums import CallbackAPIVersion

from app.carlo_gavazzi.em540_master import Em540MasterStats, MeterDataListener
from app.carlo_gavazzi.em540_slave_stats import EM540SlaveStats
from app.carlo_gavazzi.meter_data import MeterData
from app.config import AppState, ConfigManager, MqttConfig
from app.fronius.ts65a_slave_stats import Ts65aSlaveStats
from app.home_assistant.ha_config_entities import HAConfigEntities
from app.home_assistant.ha_diagnostics import DIAGNOSTICS_INTERVAL, HADiagnostics
from app.home_assistant.ha_sensors import EnergyMeterSensor

FIRST_RECONNECT_DELAY = 1
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
        self.client = mqtt_client.Client(CallbackAPIVersion.VERSION2, client_id, userdata=self)
        self.client.username_pw_set(conf.username, conf.password)
        self.client.on_connect = HABridge.on_connect
        self.client.on_disconnect = HABridge.on_disconnect
        self.client.reconnect_delay_set(
            min_delay=FIRST_RECONNECT_DELAY,
            max_delay=MAX_RECONNECT_DELAY,
        )
        self.connected = False
        self._loop_started = False
        self._update_interval = conf.update_interval
        self._last_update = 0
        self.sensors = EnergyMeterSensor()
        self._diagnostics = HADiagnostics()
        self._last_stats_update: float = 0
        self._config_entities: HAConfigEntities | None = None
        self._last_payload_by_topic: dict[str, str] = {}

        if state is not None and config_manager is not None:
            self._config_entities = HAConfigEntities(state, self.client, config_manager)

        # Background thread to handle updates outside of asyncio loop
        self._condition: threading.Condition = threading.Condition()
        self._notify_thread: threading.Thread = threading.Thread(target=self._notify_loop, daemon=True)
        self._notify_thread.start()

        logger.setLevel(conf.log_level)

    def connect(self):
        if not self.host:
            return

        if not self._loop_started:
            self.client.loop_start()
            self._loop_started = True

        try:
            self.client.connect_async(self.host, self.port)
        except Exception as err:
            logger.warning("Failed to schedule MQTT connection: %s", err)

    @staticmethod
    def on_disconnect(client, userdata, flags, rc, props):
        userdata.connected = False
        userdata._last_payload_by_topic.clear()
        if rc != 0:
            logger.warning(
                "Disconnected from MQTT broker with result code: %s. Reconnect will continue in background.",
                rc,
            )
        else:
            logger.info("Disconnected from MQTT broker.")

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
                    # Format state value based on entity type
                    if entity.entity_type == "switch":
                        state_value = "on" if value else "off"
                    else:
                        state_value = str(value)
                    client.publish(state_topic, state_value, retain=True)
        else:
            logger.info("Failed to connect, return code %d\n", rc)

    def publish(self, topic, msg, retain=False):
        msg_str = str(msg)
        if self._last_payload_by_topic.get(topic) == msg_str:
            return

        if self.connected:
            self.client.publish(topic, msg_str, retain=retain)
            self._last_payload_by_topic[topic] = msg_str

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

    def on_em540_master_stats(self, stats: Em540MasterStats):
        self._diagnostics.set_em540_master_stats(stats)

    def _notify_loop(self) -> None:
        """Background thread to publish sensor data when notified of new data"""
        while True:
            with self._condition:
                self._condition.wait()

                # Only publish sensor data if enable_ha_publish is True
                if self._state.mqtt.enable_ha_publish:
                    # Now publish all sensor data
                    topic, payload = self.sensors.mqtt_data()
                    try:
                        self.publish(topic, payload)
                    except Exception as err:
                        logger.error(f"Failed to publish sensor data on topic {topic}: {err}")

                    # Do the same with diagnostics, if we are ready for an update
                    now = datetime.now().timestamp()
                    if self._last_stats_update == 0 or (now - self._last_stats_update) > DIAGNOSTICS_INTERVAL:
                        self._last_stats_update = now
                        topic, payload = self._diagnostics.mqtt_data()
                        try:
                            self.publish(topic, payload)
                        except Exception as err:
                            logger.error(f"Failed to publish diagnostics data on topic {topic}: {err}")
