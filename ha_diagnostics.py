import json

from meter_data import MeterData
from ha_sensors import Sensor

diag_interval = 5  # seconds

class HADiagnostics:
    def __init__(self):
        self._last_update_timestamp = 0
        self._last_data_counter = 0

        self._data_counter = 0
        self.state_topic = "lerebel/sensor/em540_energy_meter_bridge/state"

        self.update_rate = Sensor('RS485 Master Read Rate', 'Hz', 'frequency', 'measurement', self.state_topic, precision=2, entity_category='diagnostic')
        self.read_failed_count = Sensor('RS485 Master Read Failures', None, None, None, self.state_topic, precision=0, entity_category='diagnostic')

    def new_data(self, data: MeterData):
        # Keep track of how many updates we have received, so we can calculate an update rate
        if self._last_update_timestamp == 0:
            self._last_update_timestamp = data.timestamp

        self._data_counter += 1

        # Calculate update rate
        if data.timestamp - self._last_update_timestamp > diag_interval:
            update_rate = (self._data_counter - self._last_data_counter) / (data.timestamp - self._last_update_timestamp)
            self.update_rate.update_value(update_rate)

            self._last_data_counter = self._data_counter
            self._last_update_timestamp = data.timestamp

    def read_failed(self):
        self.read_failed_count.update_value(self.read_failed_count.value + 1)

    def advertise_data(self):
        sensors = [self.update_rate, self.read_failed_count]
        return [sensor.discovery() for sensor in sensors]

    def mqtt_data(self):
        sensors = [self.update_rate, self.read_failed_count]
        payload = {sensor.safe_name: sensor.value for sensor in sensors}
        return self.state_topic, json.dumps(payload)