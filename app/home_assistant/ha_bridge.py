from __future__ import annotations

import logging
import random
import threading
import time
from dataclasses import dataclass, field

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


@dataclass(slots=True)
class _HASnapshotPhase:
    line_line_voltage: float = 0.0
    line_neutral_voltage: float = 0.0
    current: float = 0.0
    power: float = 0.0
    apparent_power: float = 0.0
    reactive_power: float = 0.0
    power_factor: float = 0.0


@dataclass(slots=True)
class _HASnapshotSystem:
    line_neutral_voltage: float = 0.0
    line_line_voltage: float = 0.0
    power: float = 0.0
    apparent_power: float = 0.0
    reactive_power: float = 0.0
    power_factor: float = 0.0
    frequency: float = 0.0
    An: float = 0.0


@dataclass(slots=True)
class _HASnapshotOtherEnergies:
    kwh_plus_total: float = 0.0
    kvarh_plus_total: float = 0.0
    kwh_neg_total: float = 0.0
    kvarh_neg_total: float = 0.0
    kvah_total: float = 0.0
    run_hour_meter: float = 0.0


@dataclass(slots=True)
class _HASnapshot:
    timestamp: float = 0.0
    system: _HASnapshotSystem = field(default_factory=_HASnapshotSystem)
    phases: list[_HASnapshotPhase] = field(
        default_factory=lambda: [_HASnapshotPhase(), _HASnapshotPhase(), _HASnapshotPhase()]
    )
    other_energies: _HASnapshotOtherEnergies = field(default_factory=_HASnapshotOtherEnergies)


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
        self._mqtt_config = conf
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
        self._topic_prefix = conf.ha_topic_prefix
        self.sensors = EnergyMeterSensor(topic_prefix=self._topic_prefix)
        self._diagnostics = HADiagnostics(topic_prefix=self._topic_prefix)
        self._next_sensor_publish_monotonic: float = 0.0
        self._next_diagnostics_publish_monotonic: float = 0.0
        self._config_entities: HAConfigEntities | None = None
        self._last_payload_by_topic: dict[str, str] = {}
        self._stop_event = threading.Event()
        self._front_snapshot = _HASnapshot()
        self._back_snapshot = _HASnapshot()
        self._snapshot_pending = False
        self._data_available = False
        self._availability_dirty = False
        self._static_data_set = False

        if state is not None and config_manager is not None:
            self._config_entities = HAConfigEntities(
                state,
                self.client,
                config_manager,
                topic_prefix=self._topic_prefix,
            )

        # Background thread to handle updates outside of asyncio loop
        self._condition: threading.Condition = threading.Condition()
        self._notify_thread: threading.Thread = threading.Thread(
            target=self._notify_loop,
            daemon=True,
            name="ha-bridge-notify",
        )
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

    def stop(self) -> None:
        self._stop_event.set()
        with self._condition:
            self._condition.notify_all()

        if self._loop_started:
            try:
                self.client.disconnect()
            except Exception:
                logger.debug("Failed to disconnect MQTT client cleanly", exc_info=True)
            try:
                self.client.loop_stop()
            except Exception:
                logger.debug("Failed to stop MQTT client loop cleanly", exc_info=True)
            self._loop_started = False

        self._notify_thread.join(timeout=1)

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
            userdata._publish_availability(client)

            # Subscribe config entities and publish current state values
            if userdata._config_entities is not None:
                userdata._config_entities.subscribe()
                for entity in userdata._config_entities._entities:
                    value = getattr(entity.config_section, entity.field_name)
                    state_topic = userdata._config_entities.state_topic_for(entity)
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
        # Only set static device info if static registers have been read
        if data.static_data_valid and not self._static_data_set:
            if data.serial_number:
                self.sensors.set_device_serial_number(data.serial_number)
            if data.model_number:
                self.sensors.set_device_model_number(data.model_number)
            self._static_data_set = True

        self._set_data_available(data.static_data_valid)
        with self._condition:
            self._copy_snapshot(data, self._back_snapshot)
            self._front_snapshot, self._back_snapshot = self._back_snapshot, self._front_snapshot
            self._snapshot_pending = True
            self._condition.notify_all()

    async def read_failed(self):
        self._diagnostics.read_failed()
        self._set_data_available(False)

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
        while not self._stop_event.is_set():
            with self._condition:
                while not self._stop_event.is_set():
                    now = time.monotonic()
                    next_due = self._next_due_monotonic(now)
                    if self._snapshot_pending or self._availability_dirty:
                        break
                    if next_due is not None and next_due <= now:
                        break
                    timeout = None if next_due is None else max(0.0, next_due - now)
                    self._condition.wait(timeout=timeout)

                if self._stop_event.is_set():
                    return

                snapshot = self._front_snapshot
                snapshot_pending = self._snapshot_pending
                availability_dirty = self._availability_dirty
                self._snapshot_pending = False
                self._availability_dirty = False

            if availability_dirty:
                self._publish_availability()

            if snapshot_pending and snapshot.timestamp > 0:
                self._diagnostics.new_data(snapshot)
                self.sensors.update(snapshot)

            # Only publish sensor data when upstream data is available
            if snapshot.timestamp > 0 and self._data_available:
                now = time.monotonic()

                if self._next_sensor_publish_monotonic == 0:
                    self._next_sensor_publish_monotonic = now
                if self._next_diagnostics_publish_monotonic == 0:
                    self._next_diagnostics_publish_monotonic = now

                if now >= self._next_sensor_publish_monotonic:
                    topic, payload = self.sensors.mqtt_data()
                    try:
                        self.publish(topic, payload)
                    except Exception as err:
                        logger.error(f"Failed to publish sensor data on topic {topic}: {err}")
                    else:
                        self._diagnostics.record_mqtt_publish(now)
                    self._next_sensor_publish_monotonic = self._advance_publish_deadline(
                        self._next_sensor_publish_monotonic,
                        self._sensor_update_interval(),
                        now,
                    )

                if now >= self._next_diagnostics_publish_monotonic:
                    topic, payload = self._diagnostics.mqtt_data()
                    try:
                        self.publish(topic, payload)
                    except Exception as err:
                        logger.error(f"Failed to publish diagnostics data on topic {topic}: {err}")
                    self._next_diagnostics_publish_monotonic = self._advance_publish_deadline(
                        self._next_diagnostics_publish_monotonic,
                        DIAGNOSTICS_INTERVAL,
                        now,
                    )

    def _next_due_monotonic(self, now: float) -> float | None:
        if not self._data_available or self._front_snapshot.timestamp <= 0:
            return None

        sensor_due = self._next_sensor_publish_monotonic or now
        diagnostics_due = self._next_diagnostics_publish_monotonic or now
        return min(sensor_due, diagnostics_due)

    def _sensor_update_interval(self) -> float:
        return max(0.001, float(self._mqtt_config.update_interval))

    @staticmethod
    def _advance_publish_deadline(deadline: float, interval: float, now: float) -> float:
        next_deadline = deadline + interval
        if next_deadline > now:
            return next_deadline

        skipped_intervals = int((now - deadline) // interval) + 1
        return deadline + skipped_intervals * interval

    def _set_data_available(self, available: bool) -> None:
        if self._data_available == available:
            return

        self._data_available = available
        if not available:
            self._next_sensor_publish_monotonic = 0.0
            self._next_diagnostics_publish_monotonic = 0.0
        with self._condition:
            self._availability_dirty = True
            self._condition.notify_all()

    def _publish_availability(self, client: mqtt_client.Client | None = None) -> None:
        payload = "online" if self._data_available else "offline"
        availability_topic = self.sensors.availability_topic
        try:
            if client is not None:
                client.publish(availability_topic, payload, retain=True)
                self._last_payload_by_topic[availability_topic] = payload
            else:
                self.publish(availability_topic, payload, retain=True)
        except Exception as err:
            logger.error(f"Failed to publish availability on topic {availability_topic}: {err}")

    def _copy_snapshot(self, data: MeterData, target: _HASnapshot) -> None:
        target.timestamp = data.timestamp

        target.system.line_neutral_voltage = data.system.line_neutral_voltage
        target.system.line_line_voltage = data.system.line_line_voltage
        target.system.power = data.system.power
        target.system.apparent_power = data.system.apparent_power
        target.system.reactive_power = data.system.reactive_power
        target.system.power_factor = data.system.power_factor
        target.system.frequency = data.system.frequency
        target.system.An = data.system.An

        for source_phase, target_phase in zip(data.phases, target.phases):
            target_phase.line_line_voltage = source_phase.line_line_voltage
            target_phase.line_neutral_voltage = source_phase.line_neutral_voltage
            target_phase.current = source_phase.current
            target_phase.power = source_phase.power
            target_phase.apparent_power = source_phase.apparent_power
            target_phase.reactive_power = source_phase.reactive_power
            target_phase.power_factor = source_phase.power_factor

        target.other_energies.kwh_plus_total = data.other_energies.kwh_plus_total
        target.other_energies.kvarh_plus_total = data.other_energies.kvarh_plus_total
        target.other_energies.kwh_neg_total = data.other_energies.kwh_neg_total
        target.other_energies.kvarh_neg_total = data.other_energies.kvarh_neg_total
        target.other_energies.kvah_total = data.other_energies.kvah_total
        target.other_energies.run_hour_meter = data.other_energies.run_hour_meter
