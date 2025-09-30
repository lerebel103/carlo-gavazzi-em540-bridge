import json
import time

from em540_slave_stats import EM540SlaveStats
from meter_data import MeterData
from ha_sensors import Sensor
from ts65a_slave_stats import Ts65aSlaveStats

diag_interval = 5  # seconds

class HADiagnostics:
    def __init__(self):
        self._em540_slave_stats = None
        self._ts65a_slave_stats = None

        self._last_update_timestamp = 0
        self._last_data_counter = 0

        self._start_time = time.time()
        self._data_counter = 0
        self.state_topic = "lerebel/sensor/em540_energy_meter_bridge/state"

        self._uptime = Sensor('Sys Uptime', 's', 'duration', 'measurement', self.state_topic, precision=0, entity_category='diagnostic')
        self._bridge_uptime = Sensor('Bridge Uptime', 's', 'duration', 'measurement', self.state_topic, precision=0, entity_category='diagnostic')

        self.update_rate = Sensor('RS485 Master Read Rate', 'Hz', 'frequency', 'measurement', self.state_topic, precision=2, entity_category='diagnostic')
        self.read_failed_count = Sensor('RS485 Master Read Failures', None, None, 'measurement', self.state_topic, precision=0, entity_category='diagnostic')
        self.min_power_w = Sensor('Min Power W', 'W', 'power', 'measurement', self.state_topic, precision=1, entity_category='diagnostic')
        self.max_power_w = Sensor('Max Power W', 'W', 'power', 'measurement', self.state_topic, precision=1, entity_category='diagnostic')

        # TS65A specific diagnostics
        self.ts65a_tcp_client_count = Sensor('TS65A TCP Client Count', None, None, 'measurement', self.state_topic, precision=0, entity_category='diagnostic')
        self.ts65a_tcp_client_disconnect_count = Sensor('TS65A TCP Client Disconnect Count', None, None, 'measurement', self.state_topic, precision=0, entity_category='diagnostic')
        self.ts65a_power_over_feed_in_limit_count = Sensor('Overfeed Limit Count', None, None, 'measurement', self.state_topic, precision=0, entity_category='diagnostic')
        self.ts65a_power_over_feed_limit_max_duration = Sensor('Overfeed Limit Max Duration', 'ms', 'duration', 'measurement', self.state_topic, precision=2, entity_category='diagnostic')

        self.em540_rtu_client_count = Sensor('EM540 RTU Client Count', None, None, 'measurement', self.state_topic, precision=0, entity_category='diagnostic')
        self.em540_rtu_client_disconnect_count = Sensor('EM540 RTU Client Disconnect Count', None, None, 'measurement', self.state_topic, precision=0, entity_category='diagnostic')
        self.em540_tcp_client_count = Sensor('EM540 TCP Client Count', None, None, 'measurement', self.state_topic, precision=0, entity_category='diagnostic')
        self.em540_tcp_client_disconnect_count = Sensor('EM540 TCP Client Disconnect Count', None, None, 'measurement', self.state_topic, precision=0, entity_category='diagnostic')

    def new_data(self, data: MeterData):
        # Keep track of how many updates we have received, so we can calculate an update rate
        if self._last_update_timestamp == 0:
            self._last_update_timestamp = data.timestamp

        self._data_counter += 1
        
        power = data.system.power
        self.min_power_w.update_value(min(self.min_power_w.value, power) if self.min_power_w.value is not None else power)
        self.max_power_w.update_value(max(self.max_power_w.value, power) if self.max_power_w.value is not None else power)

        # Calculate update rate
        if data.timestamp - self._last_update_timestamp > diag_interval:
            update_rate = (self._data_counter - self._last_data_counter) / (data.timestamp - self._last_update_timestamp)
            self.update_rate.update_value(update_rate)

            self._last_data_counter = self._data_counter
            self._last_update_timestamp = data.timestamp

    def read_failed(self):
        self.read_failed_count.update_value(self.read_failed_count.value + 1)

    def advertise_data(self):
        sensors = [
            self._uptime,
            self._bridge_uptime,
            self.update_rate,
            self.min_power_w,
            self.max_power_w,
            self.read_failed_count,
            self.em540_rtu_client_count,
            self.em540_tcp_client_count,
            self.ts65a_tcp_client_count,
            self.ts65a_tcp_client_disconnect_count,
            self.ts65a_power_over_feed_in_limit_count,
            self.ts65a_power_over_feed_limit_max_duration
        ]
        return [sensor.discovery() for sensor in sensors]

    def mqtt_data(self):
        import uptime

        # Get the system uptime in seconds
        system_uptime_seconds = uptime.uptime()
        self._uptime.update_value(int(system_uptime_seconds))

        # Get the bridge uptime in seconds
        bridge_uptime_seconds = time.time() - self._start_time
        self._bridge_uptime.update_value(int(bridge_uptime_seconds))

        # Update slave stats if available
        if self._em540_slave_stats is not None:
            self.em540_rtu_client_count.update_value(self._em540_slave_stats.rtu_client_count)
            self.em540_tcp_client_count.update_value(self._em540_slave_stats.tcp_client_count)
        if self._ts65a_slave_stats is not None:
            self.ts65a_tcp_client_count.update_value(self._ts65a_slave_stats.tcp_client_count)
            self.ts65a_tcp_client_disconnect_count.update_value(self._ts65a_slave_stats.tcp_client_disconnect_count)
            self.ts65a_power_over_feed_in_limit_count.update_value(self._ts65a_slave_stats.power_over_feed_in_limit_count)
            self.ts65a_power_over_feed_limit_max_duration.update_value(self._ts65a_slave_stats.power_over_feed_limit_max_duration_sec * 1000.0)  # convert to ms


        sensors = [
            self._uptime,
            self.min_power_w,
            self.max_power_w,
            self._bridge_uptime,
            self.update_rate,
            self.read_failed_count,
            self.em540_rtu_client_count,
            self.em540_tcp_client_count,
            self.ts65a_tcp_client_count,
            self.ts65a_tcp_client_disconnect_count,
            self.ts65a_power_over_feed_in_limit_count,
            self.ts65a_power_over_feed_limit_max_duration
        ]

        payload = {sensor.safe_name: sensor.value for sensor in sensors}
        return self.state_topic, json.dumps(payload)

    def set_em540_slave_stats(self, stats: EM540SlaveStats):
        self._em540_slave_stats = stats

    def set_ts_65a_slave_stats(self, stats: Ts65aSlaveStats):
        self._ts65a_slave_stats = stats
