import json

from app.carlo_gavazzi.meter_data import MeterData
from app.home_assistant.ha_topics import discovery_model_name, prefix_topic, topic_namespace
from app.version import __version__

BASE_STATE_TOPIC = "lerebel/sensor/em540_energy_meter_bridge/state"
HA_AVAILABILITY_TOPIC = "lerebel/sensor/em540_energy_meter_bridge/availability"


def configure_sensor_topic_metadata(
    sensors: list["Sensor"],
    namespace: str,
    topic_prefix: str,
    availability_topic: str,
) -> None:
    for sensor in sensors:
        sensor.unique_id = f"{namespace}_{sensor.safe_name}"
        # Discovery must remain under Home Assistant's configured discovery prefix.
        sensor.advertisement_topic = f"homeassistant/sensor/{namespace}_{sensor.safe_name}/config"
        sensor.availability_topic = availability_topic
        sensor.device["identifiers"] = [namespace]
        sensor.device["model"] = discovery_model_name(topic_prefix)


class Sensor:
    def __init__(
        self,
        name,
        unit,
        device_class,
        state_class,
        state_topic,
        precision=1,
        entity_category: str | None = None,
        enabled_by_default: bool = True,
    ):
        self.value: float = 0
        self.name = name
        self.state_topic = state_topic
        self.device_class = device_class
        self.state_class = state_class
        self.value_template = (
            "{% if value_json." + self.safe_name + " is defined %} {{ value_json." + self.safe_name + " }} {% endif %}"
        )
        self.unit_of_measurement = unit
        self.suggested_display_precision = precision
        self.enabled_by_default = enabled_by_default
        self.unique_id = f"em540_bridge_{self.safe_name}"
        self.advertisement_topic = f"homeassistant/sensor/em540_bridge_{self.safe_name}/config"
        self.entity_category = entity_category
        self.availability_topic = HA_AVAILABILITY_TOPIC
        self.device = {
            "name": "EM540 Energy Meter Bridge",
            "identifiers": ["em540_bridge"],
            "manufacturer": "LeRebel",
            "model": "EM540 Bridge",
            "sw_version": __version__,
        }

    @property
    def safe_name(self):
        return self.name.replace(" ", "_").replace("-", "_").lower()

    def update_value(self, new_value):
        self.value = new_value

    def __repr__(self):
        return (
            f"Sensor(name={self.name}, state_topic={self.state_topic}, "
            f"device_class={self.device_class}, value_template={self.value_template}, "
            f"unique_id={self.unique_id}, device={self.device}, value={self.value}), "
            f"entity_category={self.entity_category}"
        )

    def discovery(self):
        obj = {
            "name": self.name,
            "state_topic": self.state_topic,
            "device_class": self.device_class,
            "state_class": self.state_class,
            "value_template": self.value_template,
            "unit_of_measurement": self.unit_of_measurement,
            "suggested_display_precision": self.suggested_display_precision,
            "unique_id": self.unique_id,
            "device": self.device,
            "entity_category": self.entity_category,
            "enabled_by_default": self.enabled_by_default,
            "availability_topic": self.availability_topic,
            "payload_available": "online",
            "payload_not_available": "offline",
        }
        # Remove device_class, state_class, unit_of_measurement if None
        if self.device_class is None:
            del obj["device_class"]
        if self.state_class is None:
            del obj["state_class"]
        if self.unit_of_measurement is None:
            del obj["unit_of_measurement"]
        if self.entity_category is None:
            del obj["entity_category"]

        return self.advertisement_topic, json.dumps(obj, indent=2)


class EnergyMeterSensor:
    def __init__(self, topic_prefix: str = ""):
        self._topic_prefix = topic_prefix
        self.namespace = topic_namespace(topic_prefix)
        self.state_topic = prefix_topic(BASE_STATE_TOPIC, topic_prefix)
        self.availability_topic = prefix_topic(HA_AVAILABILITY_TOPIC, topic_prefix)

        # Grouped sensor definitions
        self.frequency = Sensor("Frequency", "Hz", "frequency", "measurement", self.state_topic, precision=2)

        self.voltage_sensors = [
            Sensor(
                "Mean Voltage L-N",
                "V",
                "voltage",
                "measurement",
                self.state_topic,
                enabled_by_default=False,
            ),
            Sensor("Voltage L1-N", "V", "voltage", "measurement", self.state_topic),
            Sensor("Voltage L2-N", "V", "voltage", "measurement", self.state_topic),
            Sensor("Voltage L3-N", "V", "voltage", "measurement", self.state_topic),
            Sensor(
                "Mean Voltage L-L",
                "V",
                "voltage",
                "measurement",
                self.state_topic,
                enabled_by_default=False,
            ),
            Sensor("Voltage L1-L2", "V", "voltage", "measurement", self.state_topic),
            Sensor("Voltage L2-L3", "V", "voltage", "measurement", self.state_topic),
            Sensor("Voltage L3-L1", "V", "voltage", "measurement", self.state_topic),
        ]

        self.current_sensors = [
            Sensor("Current", "A", "current", "measurement", self.state_topic),
            Sensor("Current L1", "A", "current", "measurement", self.state_topic),
            Sensor("Current L2", "A", "current", "measurement", self.state_topic),
            Sensor("Current L3", "A", "current", "measurement", self.state_topic),
        ]

        self.power_sensors = [
            Sensor("Power", "W", "power", "measurement", self.state_topic, precision=0),
            Sensor("Power L1", "W", "power", "measurement", self.state_topic, precision=0),
            Sensor("Power L2", "W", "power", "measurement", self.state_topic, precision=0),
            Sensor("Power L3", "W", "power", "measurement", self.state_topic, precision=0),
        ]

        self.reactive_power_sensors = [
            Sensor(
                "Reactive Power",
                "var",
                "reactive_power",
                "measurement",
                self.state_topic,
                precision=0,
            ),
            Sensor(
                "Reactive Power L1",
                "var",
                "reactive_power",
                "measurement",
                self.state_topic,
                precision=0,
            ),
            Sensor(
                "Reactive Power L2",
                "var",
                "reactive_power",
                "measurement",
                self.state_topic,
                precision=0,
            ),
            Sensor(
                "Reactive Power L3",
                "var",
                "reactive_power",
                "measurement",
                self.state_topic,
                precision=0,
            ),
        ]

        self.apparent_power_sensors = [
            Sensor(
                "Apparent Power",
                "VA",
                "apparent_power",
                "measurement",
                self.state_topic,
                precision=0,
            ),
            Sensor(
                "Apparent Power L1",
                "VA",
                "apparent_power",
                "measurement",
                self.state_topic,
                precision=0,
            ),
            Sensor(
                "Apparent Power L2",
                "VA",
                "apparent_power",
                "measurement",
                self.state_topic,
                precision=0,
            ),
            Sensor(
                "Apparent Power L3",
                "VA",
                "apparent_power",
                "measurement",
                self.state_topic,
                precision=0,
            ),
        ]

        self.power_factor_sensors = [
            Sensor(
                "Mean Power Factor",
                "",
                "power_factor",
                "measurement",
                self.state_topic,
                precision=2,
            ),
            Sensor(
                "Power Factor L1",
                "",
                "power_factor",
                "measurement",
                self.state_topic,
                precision=2,
            ),
            Sensor(
                "Power Factor L2",
                "",
                "power_factor",
                "measurement",
                self.state_topic,
                precision=2,
            ),
            Sensor(
                "Power Factor L3",
                "",
                "power_factor",
                "measurement",
                self.state_topic,
                precision=2,
            ),
        ]

        self.energy_import = Sensor(
            "Energy Import",
            "kWh",
            "energy",
            "total_increasing",
            self.state_topic,
            precision=2,
        )
        self.energy_export = Sensor(
            "Energy Export",
            "kWh",
            "energy",
            "total_increasing",
            self.state_topic,
            precision=2,
        )
        self.kvarh_neg_total = Sensor(
            "Reactive Energy Export",
            "kvarh",
            "reactive_energy",
            "total_increasing",
            self.state_topic,
            precision=2,
            enabled_by_default=False,
        )
        self.kvarh_plus_total = Sensor(
            "Reactive Energy Import",
            "kvarh",
            "reactive_energy",
            "total_increasing",
            self.state_topic,
            precision=2,
            enabled_by_default=False,
        )
        self.kvah_total = Sensor(
            "Apparent Energy kvah",
            "kVAh",
            "energy",
            "total_increasing",
            self.state_topic,
            precision=2,
            enabled_by_default=False,
        )

        self.run_hour_meter = Sensor(
            "Run Hours",
            "h",
            "duration",
            "total_increasing",
            self.state_topic,
            precision=1,
            enabled_by_default=False,
        )

        sensors = (
            [self.frequency]
            + self.voltage_sensors
            + self.current_sensors
            + self.power_sensors
            + self.reactive_power_sensors
            + self.apparent_power_sensors
            + self.power_factor_sensors
            + [
                self.energy_import,
                self.energy_export,
                self.kvarh_neg_total,
                self.kvarh_plus_total,
                self.run_hour_meter,
                self.kvah_total,
            ]
        )

        configure_sensor_topic_metadata(
            sensors=sensors,
            namespace=self.namespace,
            topic_prefix=self._topic_prefix,
            availability_topic=self.availability_topic,
        )

    def update(self, data: MeterData):
        self.frequency.update_value(data.system.frequency)

        # Voltage sensors
        voltage_values = [
            data.system.line_neutral_voltage,
            data.phases[0].line_neutral_voltage,
            data.phases[1].line_neutral_voltage,
            data.phases[2].line_neutral_voltage,
            data.system.line_line_voltage,
            data.phases[0].line_line_voltage,
            data.phases[1].line_line_voltage,
            data.phases[2].line_line_voltage,
        ]
        for sensor, value in zip(self.voltage_sensors, voltage_values):
            sensor.update_value(value)

        # Current sensors
        current_values = [
            data.system.An,
            data.phases[0].current,
            data.phases[1].current,
            data.phases[2].current,
        ]
        for sensor, value in zip(self.current_sensors, current_values):
            sensor.update_value(value)

        # Power sensors
        power_values = [
            data.system.power,
            data.phases[0].power,
            data.phases[1].power,
            data.phases[2].power,
        ]
        for sensor, value in zip(self.power_sensors, power_values):
            sensor.update_value(value)

        # Reactive power sensors
        reactive_power_values = [
            data.system.reactive_power,
            data.phases[0].reactive_power,
            data.phases[1].reactive_power,
            data.phases[2].reactive_power,
        ]
        for sensor, value in zip(self.reactive_power_sensors, reactive_power_values):
            sensor.update_value(value)

        # Apparent power sensors
        apparent_power_values = [
            data.system.apparent_power,
            data.phases[0].apparent_power,
            data.phases[1].apparent_power,
            data.phases[2].apparent_power,
        ]
        for sensor, value in zip(self.apparent_power_sensors, apparent_power_values):
            sensor.update_value(value)

        # Power factor sensors
        power_factor_values = [
            data.system.power_factor,
            data.phases[0].power_factor,
            data.phases[1].power_factor,
            data.phases[2].power_factor,
        ]
        for sensor, value in zip(self.power_factor_sensors, power_factor_values):
            sensor.update_value(value)

        # Energy sensors
        self.energy_import.update_value(data.other_energies.kwh_plus_total)
        self.energy_export.update_value(data.other_energies.kwh_neg_total)
        self.kvarh_neg_total.update_value(data.other_energies.kvarh_neg_total)
        self.kvarh_plus_total.update_value(data.other_energies.kvarh_plus_total)
        self.kvah_total.update_value(data.other_energies.kvah_total)
        self.run_hour_meter.update_value(data.other_energies.run_hour_meter)

    def advertise_data(self):
        sensors = (
            [self.frequency]
            + self.voltage_sensors
            + self.current_sensors
            + self.power_sensors
            + self.reactive_power_sensors
            + self.apparent_power_sensors
            + self.power_factor_sensors
            + [
                self.energy_import,
                self.energy_export,
                self.kvarh_neg_total,
                self.kvarh_plus_total,
                self.run_hour_meter,
                self.kvah_total,
            ]
        )
        return [sensor.discovery() for sensor in sensors]

    def mqtt_data(self):
        sensors = (
            [self.frequency]
            + self.voltage_sensors
            + self.current_sensors
            + self.power_sensors
            + self.reactive_power_sensors
            + self.apparent_power_sensors
            + self.power_factor_sensors
            + [
                self.energy_import,
                self.energy_export,
                self.kvarh_neg_total,
                self.kvarh_plus_total,
                self.run_hour_meter,
                self.kvah_total,
            ]
        )
        payload = {sensor.safe_name: sensor.value for sensor in sensors}
        return self.state_topic, json.dumps(payload)
