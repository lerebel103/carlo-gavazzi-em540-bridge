import json

from meter_data import MeterData


class Sensor:
    def __init__(self, name, unit, device_class, state_class, state_topic, precision=1):
        self.value: float = None
        self.name = name
        self.state_topic = state_topic
        self.device_class = device_class
        self.state_class = state_class
        self.value_template = "{{ value_json." + self.safe_name + " }}"
        self.unit_of_measurement = unit
        self.suggested_display_precision = precision
        self.unique_id = f'em540_bridge_{self.safe_name}'
        self.advertisement_topic = f"homeassistant/sensor/em540_bridge_{self.safe_name}/config"
        self.device = {
            "name": "EM540 Energy Meter Bridge",
            "identifiers": ["em540_bridge"],
            "manufacturer": "LeRebel",
            "model": "EM540 Bridge",
            "sw_version": "1.0"
        }

    @property
    def safe_name(self):
        return self.name.replace(" ", "_").replace('-', '_').lower()

    def update_value(self, new_value):
        self.value = new_value

    def __repr__(self):
        return (f"Sensor(name={self.name}, state_topic={self.state_topic}, "
                f"device_class={self.device_class}, value_template={self.value_template}, "
                f"unique_id={self.unique_id}, device={self.device}, value={self.value})")

    def discovery(self):
        return self.advertisement_topic, json.dumps({
            "name": self.name,
            "state_topic": self.state_topic,
            "device_class": self.device_class,
            "state_class": self.state_class,
            "value_template": self.value_template,
            "unit_of_measurement": self.unit_of_measurement,
            "suggested_display_precision": self.suggested_display_precision,
            "unique_id": self.unique_id,
            "device": self.device,
        }, indent=2)


class EnergyMeterSensor:
    def __init__(self):
        self.state_topic = "lerebel/sensor/em540_energy_meter_bridge/state"

        # Grouped sensor definitions
        self.frequency = Sensor('Frequency', 'Hz', 'frequency', 'measurement', self.state_topic, precision=2)

        self.voltage_sensors = [
            Sensor('Mean Voltage L-N', 'V', 'voltage', 'measurement', self.state_topic),
            Sensor('Voltage L1-N', 'V', 'voltage', 'measurement', self.state_topic),
            Sensor('Voltage L2-N', 'V', 'voltage', 'measurement', self.state_topic),
            Sensor('Voltage L3-N', 'V', 'voltage', 'measurement', self.state_topic),
            Sensor('Mean Voltage L-L', 'V', 'voltage', 'measurement', self.state_topic),
            Sensor('Voltage L1-L2', 'V', 'voltage', 'measurement', self.state_topic),
            Sensor('Voltage L2-L3', 'V', 'voltage', 'measurement', self.state_topic),
            Sensor('Voltage L3-L1', 'V', 'voltage', 'measurement', self.state_topic),
        ]

        self.current_sensors = [
            Sensor('Current', 'A', 'current', 'measurement', self.state_topic),
            Sensor('Current L1', 'A', 'current', 'measurement', self.state_topic),
            Sensor('Current L2', 'A', 'current', 'measurement', self.state_topic),
            Sensor('Current L3', 'A', 'current', 'measurement', self.state_topic),
        ]

        self.power_sensors = [
            Sensor('Power', 'W', 'power', 'measurement', self.state_topic),
            Sensor('Power L1', 'W', 'power', 'measurement', self.state_topic),
            Sensor('Power L2', 'W', 'power', 'measurement', self.state_topic),
            Sensor('Power L3', 'W', 'power', 'measurement', self.state_topic),
        ]

        self.reactive_power_sensors = [
            Sensor('Reactive Power', 'var', 'reactive_power', 'measurement', self.state_topic),
            Sensor('Reactive Power L1', 'var', 'reactive_power', 'measurement', self.state_topic),
            Sensor('Reactive Power L2', 'var', 'reactive_power', 'measurement', self.state_topic),
            Sensor('Reactive Power L3', 'var', 'reactive_power', 'measurement', self.state_topic),
        ]

        self.apparent_power_sensors = [
            Sensor('Apparent Power', 'VA', 'apparent_power', 'measurement', self.state_topic),
            Sensor('Apparent Power L1', 'VA', 'apparent_ower', 'measurement', self.state_topic),
            Sensor('Apparent Power L2', 'VA', 'apparent_power', 'measurement', self.state_topic),
            Sensor('Apparent Power L3', 'VA', 'apparent_power', 'measurement', self.state_topic),
        ]

        self.power_factor_sensors = [
            Sensor('Mean Power Factor', '', 'power_factor', 'measurement', self.state_topic, precision=2),
            Sensor('Power Factor L1', '', 'power_factor', 'measurement', self.state_topic, precision=2),
            Sensor('Power Factor L2', '', 'power_factor', 'measurement', self.state_topic, precision=2),
            Sensor('Power Factor L3', '', 'power_factor', 'measurement', self.state_topic, precision=2),
        ]

        self.energy_import = Sensor('Energy Import', 'kWh', 'energy', 'total_increasing', self.state_topic, precision=2)
        self.energy_export = Sensor('Energy Export', 'kWh', 'energy', 'total_increasing', self.state_topic, precision=2)

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
        self.energy_import.update_value(data.other_energies.kwh_plus_total / 1000)
        self.energy_export.update_value(data.other_energies.kwh_neg_total / 1000)

    def advertise_data(self):
        sensors = (
                [self.frequency] +
                self.voltage_sensors +
                self.current_sensors +
                self.power_sensors +
                self.reactive_power_sensors +
                self.apparent_power_sensors +
                self.power_factor_sensors +
                [self.energy_import, self.energy_export]
        )
        return [sensor.discovery() for sensor in sensors]

    def mqtt_data(self):
        sensors = (
                [self.frequency] +
                self.voltage_sensors +
                self.current_sensors +
                self.power_sensors +
                self.reactive_power_sensors +
                self.apparent_power_sensors +
                self.power_factor_sensors +
                [self.energy_import, self.energy_export]
        )
        payload = {sensor.safe_name: sensor.value for sensor in sensors}
        return self.state_topic, json.dumps(payload)
