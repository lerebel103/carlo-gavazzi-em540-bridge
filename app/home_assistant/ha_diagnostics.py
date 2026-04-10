import json
import time

from app.carlo_gavazzi.em540_master import Em540MasterStats
from app.carlo_gavazzi.em540_slave_stats import EM540SlaveStats
from app.carlo_gavazzi.meter_data import MeterData
from app.fronius.ts65a_slave_stats import Ts65aSlaveStats
from app.home_assistant.ha_sensors import Sensor

DIAGNOSTICS_INTERVAL: float = 5  # seconds


class HADiagnostics:
    def __init__(self):
        self._em540_slave_stats = None
        self._em540_master_stats = None
        self._ts65a_slave_stats = None

        self._last_update_timestamp = 0
        self._last_data_counter = 0

        self._start_time = time.time()
        self._data_counter = 0
        self.state_topic = "lerebel/sensor/em540_energy_meter_bridge/state"

        self._uptime = Sensor(
            "Sys Uptime",
            "s",
            "duration",
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=False,
        )
        self._bridge_uptime = Sensor(
            "Bridge Uptime",
            "s",
            "duration",
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )

        self.update_rate = Sensor(
            "RS485 Master Read Rate",
            "Hz",
            "frequency",
            "measurement",
            self.state_topic,
            precision=2,
            entity_category="diagnostic",
        )
        self.read_failed_count = Sensor(
            "RS485 Master Read Failures",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )
        self.consumer_missed_updates_total = Sensor(
            "RS485 Consumer Missed Updates",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )
        self.consumer_max_seq_gap = Sensor(
            "RS485 Consumer Max Seq Gap",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )
        self.master_read_duration_ms = Sensor(
            "RS485 Master Read Duration",
            "ms",
            "duration",
            "measurement",
            self.state_topic,
            precision=2,
            entity_category="diagnostic",
        )
        self.master_read_duration_max_ms = Sensor(
            "RS485 Master Read Duration Max",
            "ms",
            "measurement",
            "measurement",
            self.state_topic,
            precision=2,
            entity_category="diagnostic",
        )
        self.master_tick_headroom_ms = Sensor(
            "RS485 Tick Headroom",
            "ms",
            "duration",
            "measurement",
            self.state_topic,
            precision=2,
            entity_category="diagnostic",
        )
        self.master_tick_headroom_min_ms = Sensor(
            "RS485 Tick Headroom Min",
            "ms",
            "measurement",
            "measurement",
            self.state_topic,
            precision=2,
            entity_category="diagnostic",
        )
        self.master_tick_overrun_count = Sensor(
            "RS485 Tick Overrun Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )
        self.min_power_w = Sensor(
            "Min Power W",
            "W",
            "power",
            "measurement",
            self.state_topic,
            precision=1,
            entity_category="diagnostic",
        )
        self.max_power_w = Sensor(
            "Max Power W",
            "W",
            "power",
            "measurement",
            self.state_topic,
            precision=1,
            entity_category="diagnostic",
        )

        # TS65A specific diagnostics
        self.ts65a_tcp_client_count = Sensor(
            "TS65A TCP Client Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )
        self.ts65a_tcp_client_disconnect_count = Sensor(
            "TS65A TCP Client Disconnect Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )
        self.ts65a_power_over_feed_in_limit_count = Sensor(
            "Overfeed Limit Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )
        self.ts65a_power_over_feed_limit_max_duration = Sensor(
            "Overfeed Limit Max Duration",
            "ms",
            "duration",
            "measurement",
            self.state_topic,
            precision=2,
            entity_category="diagnostic",
        )
        self.ts65a_circuit_breaker_open = Sensor(
            "TS65A Circuit Breaker Open",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )
        self.ts65a_circuit_breaker_open_count = Sensor(
            "TS65A Circuit Breaker Open Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )
        self.ts65a_stale_data_age_ms = Sensor(
            "TS65A Stale Data Age",
            "ms",
            "duration",
            "measurement",
            self.state_topic,
            precision=1,
            entity_category="diagnostic",
        )
        self.ts65a_dropped_stale_request_count = Sensor(
            "TS65A Dropped Stale Requests",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )

        self.em540_rtu_client_count = Sensor(
            "EM540 RTU Client Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )
        self.em540_rtu_client_disconnect_count = Sensor(
            "EM540 RTU Client Disconnect Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )
        self.em540_tcp_client_count = Sensor(
            "EM540 TCP Client Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )
        self.em540_tcp_client_disconnect_count = Sensor(
            "EM540 TCP Client Disconnect Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )
        self.em540_circuit_breaker_open = Sensor(
            "EM540 Circuit Breaker Open",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )
        self.em540_circuit_breaker_open_count = Sensor(
            "EM540 Circuit Breaker Open Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )
        self.em540_stale_data_age_ms = Sensor(
            "EM540 Stale Data Age",
            "ms",
            "duration",
            "measurement",
            self.state_topic,
            precision=1,
            entity_category="diagnostic",
        )
        self.em540_dropped_stale_request_count = Sensor(
            "EM540 Dropped Stale Requests",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
        )

    def new_data(self, data: MeterData):
        # Keep track of how many updates we have received, so we can calculate an update rate
        if self._last_update_timestamp == 0:
            self._last_update_timestamp = data.timestamp

        self._data_counter += 1

        power = data.system.power
        self.min_power_w.update_value(
            min(self.min_power_w.value, power) if self.min_power_w.value is not None else power
        )
        self.max_power_w.update_value(
            max(self.max_power_w.value, power) if self.max_power_w.value is not None else power
        )

        # Calculate update rate
        if data.timestamp - self._last_update_timestamp > DIAGNOSTICS_INTERVAL:
            update_rate = (self._data_counter - self._last_data_counter) / (
                data.timestamp - self._last_update_timestamp
            )
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
            self.consumer_missed_updates_total,
            self.consumer_max_seq_gap,
            self.master_read_duration_ms,
            self.master_read_duration_max_ms,
            self.master_tick_headroom_ms,
            self.master_tick_headroom_min_ms,
            self.master_tick_overrun_count,
            self.em540_rtu_client_count,
            self.em540_rtu_client_disconnect_count,
            self.em540_circuit_breaker_open,
            self.em540_circuit_breaker_open_count,
            self.em540_stale_data_age_ms,
            self.em540_dropped_stale_request_count,
            self.ts65a_tcp_client_count,
            self.ts65a_tcp_client_disconnect_count,
            self.ts65a_power_over_feed_in_limit_count,
            self.ts65a_power_over_feed_limit_max_duration,
            self.ts65a_circuit_breaker_open,
            self.ts65a_circuit_breaker_open_count,
            self.ts65a_stale_data_age_ms,
            self.ts65a_dropped_stale_request_count,
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
            self.em540_rtu_client_disconnect_count.update_value(self._em540_slave_stats.rtu_client_disconnect_count)
            self.em540_tcp_client_count.update_value(self._em540_slave_stats.tcp_client_count)
            self.em540_tcp_client_disconnect_count.update_value(self._em540_slave_stats.tcp_client_disconnect_count)
            self.em540_circuit_breaker_open.update_value(1 if self._em540_slave_stats.circuit_breaker_open else 0)
            self.em540_circuit_breaker_open_count.update_value(self._em540_slave_stats.circuit_breaker_open_count)
            self.em540_stale_data_age_ms.update_value(self._em540_slave_stats.stale_data_age_ms)
            self.em540_dropped_stale_request_count.update_value(self._em540_slave_stats.dropped_stale_request_count)
        if self._em540_master_stats is not None:
            self.consumer_missed_updates_total.update_value(self._em540_master_stats.consumer_missed_updates_total)
            self.consumer_max_seq_gap.update_value(self._em540_master_stats.consumer_max_seq_gap)
            self.master_read_duration_ms.update_value(self._em540_master_stats.read_duration_ms_last)
            self.master_read_duration_max_ms.update_value(self._em540_master_stats.read_duration_ms_max)
            self.master_tick_headroom_ms.update_value(self._em540_master_stats.tick_headroom_ms_last)
            self.master_tick_headroom_min_ms.update_value(self._em540_master_stats.tick_headroom_ms_min)
            self.master_tick_overrun_count.update_value(self._em540_master_stats.tick_overrun_count)
        if self._ts65a_slave_stats is not None:
            self.ts65a_tcp_client_count.update_value(self._ts65a_slave_stats.tcp_client_count)
            self.ts65a_tcp_client_disconnect_count.update_value(self._ts65a_slave_stats.tcp_client_disconnect_count)
            self.ts65a_power_over_feed_in_limit_count.update_value(
                self._ts65a_slave_stats.power_over_feed_in_limit_count
            )
            self.ts65a_power_over_feed_limit_max_duration.update_value(
                self._ts65a_slave_stats.power_over_feed_limit_max_duration_sec * 1000.0
            )  # convert to ms
            self.ts65a_circuit_breaker_open.update_value(1 if self._ts65a_slave_stats.circuit_breaker_open else 0)
            self.ts65a_circuit_breaker_open_count.update_value(self._ts65a_slave_stats.circuit_breaker_open_count)
            self.ts65a_stale_data_age_ms.update_value(self._ts65a_slave_stats.stale_data_age_ms)
            self.ts65a_dropped_stale_request_count.update_value(self._ts65a_slave_stats.dropped_stale_request_count)

        sensors = [
            self._uptime,
            self.min_power_w,
            self.max_power_w,
            self._bridge_uptime,
            self.update_rate,
            self.read_failed_count,
            self.consumer_missed_updates_total,
            self.consumer_max_seq_gap,
            self.master_read_duration_ms,
            self.master_read_duration_max_ms,
            self.master_tick_headroom_ms,
            self.master_tick_headroom_min_ms,
            self.master_tick_overrun_count,
            self.em540_rtu_client_count,
            self.em540_rtu_client_disconnect_count,
            self.em540_circuit_breaker_open,
            self.em540_circuit_breaker_open_count,
            self.em540_stale_data_age_ms,
            self.em540_dropped_stale_request_count,
            self.ts65a_tcp_client_count,
            self.ts65a_tcp_client_disconnect_count,
            self.ts65a_power_over_feed_in_limit_count,
            self.ts65a_power_over_feed_limit_max_duration,
            self.ts65a_circuit_breaker_open,
            self.ts65a_circuit_breaker_open_count,
            self.ts65a_stale_data_age_ms,
            self.ts65a_dropped_stale_request_count,
        ]

        payload = {sensor.safe_name: sensor.value for sensor in sensors}
        return self.state_topic, json.dumps(payload)

    def set_em540_slave_stats(self, stats: EM540SlaveStats):
        self._em540_slave_stats = stats

    def set_em540_master_stats(self, stats: Em540MasterStats):
        self._em540_master_stats = stats

    def set_ts_65a_slave_stats(self, stats: Ts65aSlaveStats):
        self._ts65a_slave_stats = stats
