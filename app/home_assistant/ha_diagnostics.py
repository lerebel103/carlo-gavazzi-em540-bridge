import json
import time

from app.carlo_gavazzi.em540_master import Em540MasterStats
from app.carlo_gavazzi.em540_slave_stats import EM540SlaveStats
from app.carlo_gavazzi.meter_data import MeterData
from app.fronius.ts65a_slave_stats import Ts65aSlaveStats
from app.home_assistant.ha_sensors import HA_AVAILABILITY_TOPIC, Sensor, configure_sensor_topic_metadata
from app.home_assistant.ha_topics import prefix_topic, topic_namespace

DIAGNOSTICS_INTERVAL: float = 5  # seconds


class HADiagnostics:
    def __init__(self, topic_prefix: str = ""):
        self._em540_slave_stats = None
        self._em540_master_stats = None
        self._ts65a_slave_stats = None

        self._last_master_rate_timestamp = 0.0
        self._last_master_counter = 0
        self._master_counter = 0
        self._last_mqtt_rate_timestamp = 0.0
        self._last_mqtt_counter = 0
        self._mqtt_counter = 0

        self._start_time = time.time()
        self._data_counter = 0
        self._topic_prefix = topic_prefix
        self._namespace = topic_namespace(topic_prefix)
        self._availability_topic = prefix_topic(HA_AVAILABILITY_TOPIC, topic_prefix)
        self.state_topic = prefix_topic("lerebel/sensor/em540_energy_meter_bridge/state", topic_prefix)

        self._uptime = Sensor(
            "Sys Uptime",
            "s",
            "duration",
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=True,
        )
        self._bridge_uptime = Sensor(
            "Bridge Uptime",
            "s",
            "duration",
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=True,
        )

        self.update_rate = Sensor(
            "Acq Rate",
            "Hz",
            "frequency",
            "measurement",
            self.state_topic,
            precision=2,
            entity_category="diagnostic",
            enabled_by_default=True,
        )
        self.mqtt_update_rate = Sensor(
            "MQTT Data Update Rate",
            "Hz",
            "frequency",
            "measurement",
            self.state_topic,
            precision=2,
            entity_category="diagnostic",
            enabled_by_default=False,
        )
        self.read_failed_count = Sensor(
            "RS485 Master Read Failures",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=False,
        )
        self.consumer_missed_updates_total = Sensor(
            "RS485 Consumer Missed Updates",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=False,
        )
        self.consumer_max_seq_gap = Sensor(
            "RS485 Consumer Max Seq Gap",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=False,
        )
        self.acquisition_duration_min_ms = Sensor(
            "Acq Dur Min",
            "ms",
            "duration",
            "measurement",
            self.state_topic,
            precision=2,
            entity_category="diagnostic",
            enabled_by_default=False,
        )
        self.acquisition_duration_max_ms = Sensor(
            "Acq Dur Max",
            "ms",
            "duration",
            "measurement",
            self.state_topic,
            precision=2,
            entity_category="diagnostic",
            enabled_by_default=False,
        )
        self.acquisition_duration_mean_ms = Sensor(
            "Acq Dur Mean",
            "ms",
            "duration",
            "measurement",
            self.state_topic,
            precision=2,
            entity_category="diagnostic",
            enabled_by_default=False,
        )
        self.acquisition_headroom_min_ms = Sensor(
            "Acq Headroom Min",
            "ms",
            "duration",
            "measurement",
            self.state_topic,
            precision=2,
            entity_category="diagnostic",
            enabled_by_default=False,
        )
        self.acquisition_headroom_max_ms = Sensor(
            "Acq Headroom Max",
            "ms",
            "duration",
            "measurement",
            self.state_topic,
            precision=2,
            entity_category="diagnostic",
            enabled_by_default=False,
        )
        self.acquisition_headroom_mean_ms = Sensor(
            "Acq Headroom Mean",
            "ms",
            "duration",
            "measurement",
            self.state_topic,
            precision=2,
            entity_category="diagnostic",
            enabled_by_default=False,
        )
        self.master_tick_overrun_count = Sensor(
            "Tick Overruns",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=True,
        )
        # Daily per-quantity, per-scope extrema. Values are sourced from the
        # master's DailyExtrema tracker (which observes every upstream frame)
        # and pulled in mqtt_data(). All are disabled by default to avoid
        # cluttering Home Assistant; users can enable the ones they want.
        #
        # The system-power min/max entities intentionally keep their original
        # entity names ("Min Power W" / "Max Power W") so their derived
        # safe_name / unique_id stay stable and existing HA entities are not
        # orphaned. Only their display_name changes to "Daily ...".
        self._daily_extrema_source = None
        self._daily_extrema_sensors: dict[str, Sensor] = self._build_daily_extrema_sensors()
        self.min_power_w = self._daily_extrema_sensors["power_min"]
        self.max_power_w = self._daily_extrema_sensors["power_max"]

        # TS65A specific diagnostics
        self.ts65a_tcp_client_count = Sensor(
            "TS65A TCP Client Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=True,
        )
        self.ts65a_tcp_client_disconnect_count = Sensor(
            "TS65A TCP Client Disconnect Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=True,
        )
        self.ts65a_power_over_feed_in_limit_count = Sensor(
            "Overfeed Limit Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=True,
        )
        self.ts65a_power_over_feed_limit_max_duration = Sensor(
            "Overfeed Limit Max Duration",
            "ms",
            "duration",
            "measurement",
            self.state_topic,
            precision=2,
            entity_category="diagnostic",
            enabled_by_default=True,
        )
        self.ts65a_circuit_breaker_open = Sensor(
            "TS65A Circuit Breaker Open",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=False,
        )
        self.ts65a_circuit_breaker_open_count = Sensor(
            "TS65A Circuit Breaker Open Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=False,
        )
        self.ts65a_stale_data_age_ms = Sensor(
            "TS65A Stale Data Age",
            "ms",
            "duration",
            "measurement",
            self.state_topic,
            precision=1,
            entity_category="diagnostic",
            enabled_by_default=True,
        )
        self.ts65a_dropped_stale_request_count = Sensor(
            "TS65A Dropped Stale Requests",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=True,
        )

        self.em540_rtu_client_count = Sensor(
            "EM540 RTU Client Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=True,
        )
        self.em540_rtu_client_disconnect_count = Sensor(
            "EM540 RTU Client Disconnect Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=True,
        )
        self.em540_tcp_client_count = Sensor(
            "EM540 TCP Client Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=True,
        )
        self.em540_tcp_client_disconnect_count = Sensor(
            "EM540 TCP Client Disconnect Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=True,
        )
        self.em540_circuit_breaker_open = Sensor(
            "EM540 Circuit Breaker Open",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=False,
        )
        self.em540_circuit_breaker_open_count = Sensor(
            "EM540 Circuit Breaker Open Count",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=False,
        )
        self.em540_stale_data_age_ms = Sensor(
            "EM540 Stale Data Age",
            "ms",
            "duration",
            "measurement",
            self.state_topic,
            precision=1,
            entity_category="diagnostic",
            enabled_by_default=True,
        )
        self.em540_dropped_stale_request_count = Sensor(
            "EM540 Dropped Stale Requests",
            None,
            None,
            "measurement",
            self.state_topic,
            precision=0,
            entity_category="diagnostic",
            enabled_by_default=True,
        )

        configure_sensor_topic_metadata(
            sensors=self._all_sensors(),
            namespace=self._namespace,
            topic_prefix=self._topic_prefix,
            availability_topic=self._availability_topic,
        )

    def _build_daily_extrema_sensors(self) -> dict[str, Sensor]:
        """Create the 32 daily extrema sensors keyed by tracker snapshot key.

        Keys match DailyExtrema.snapshot() output ("<quantity>[_<phase>]_<min|max>").
        Entity names (and therefore safe_name / unique_id) are chosen so that
        system power keeps its historical identity, while all display names read
        as "Daily ...".
        """
        # quantity -> (unit, device_class, precision, human label)
        quantity_meta: dict[str, tuple[str, str, int, str]] = {
            "power": ("W", "power", 1, "Power"),
            "current": ("A", "current", 2, "Current"),
            "voltage_ln": ("V", "voltage", 1, "Voltage L-N"),
            "voltage_ll": ("V", "voltage", 1, "Voltage L-L"),
        }
        # scope suffix ("" == system) -> label fragment
        scope_labels: list[tuple[str, str]] = [
            ("", ""),
            ("l1", "L1"),
            ("l2", "L2"),
            ("l3", "L3"),
        ]

        # Preserve the historical entity identity for the system power extrema so
        # existing Home Assistant entities are not orphaned. Maps snapshot key ->
        # (entity name, display name).
        legacy_names: dict[str, tuple[str, str]] = {
            "power_min": ("Min Power W", "Daily Power Min"),
            "power_max": ("Max Power W", "Daily Power Max"),
        }

        sensors: dict[str, Sensor] = {}
        for quantity, (unit, device_class, precision, q_label) in quantity_meta.items():
            for suffix, scope_label in scope_labels:
                scope_key = quantity if suffix == "" else f"{quantity}_{suffix}"
                for bound in ("min", "max"):
                    key = f"{scope_key}_{bound}"
                    if key in legacy_names:
                        name, display_name = legacy_names[key]
                    else:
                        scope_txt = f" {scope_label}" if scope_label else ""
                        name = f"Daily {q_label}{scope_txt} {bound.capitalize()}"
                        display_name = name
                    sensors[key] = Sensor(
                        name,
                        unit,
                        device_class,
                        "measurement",
                        self.state_topic,
                        precision=precision,
                        entity_category="diagnostic",
                        enabled_by_default=False,
                        display_name=display_name,
                    )
        return sensors

    def _all_sensors(self) -> list[Sensor]:
        return [
            self._uptime,
            self._bridge_uptime,
            self.update_rate,
            self.mqtt_update_rate,
            *self._daily_extrema_sensors.values(),
            self.read_failed_count,
            self.consumer_missed_updates_total,
            self.consumer_max_seq_gap,
            self.acquisition_duration_min_ms,
            self.acquisition_duration_max_ms,
            self.acquisition_duration_mean_ms,
            self.acquisition_headroom_min_ms,
            self.acquisition_headroom_max_ms,
            self.acquisition_headroom_mean_ms,
            self.master_tick_overrun_count,
            self.em540_rtu_client_count,
            self.em540_rtu_client_disconnect_count,
            self.em540_tcp_client_count,
            self.em540_tcp_client_disconnect_count,
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

    def new_data(self, data: MeterData):
        # Daily extrema are computed at the master (which sees every frame) and
        # pulled from the DailyExtrema snapshot in mqtt_data(). Nothing to do
        # here; kept for the listener/callback contract.
        pass

    def set_daily_extrema_source(self, source) -> None:
        """Register the master's DailyExtrema tracker to pull snapshots from.

        ``source`` must expose ``snapshot() -> dict[str, float | None]`` whose
        keys match the daily extrema sensor keys.
        """
        self._daily_extrema_source = source

    def _apply_daily_extrema(self) -> None:
        source = getattr(self, "_daily_extrema_source", None)
        if source is None:
            return
        snapshot = source.snapshot()
        for key, sensor in self._daily_extrema_sensors.items():
            value = snapshot.get(key)
            if value is not None:
                sensor.update_value(value)

    def record_mqtt_publish(self, published_at: float | None = None):
        now = time.monotonic() if published_at is None else published_at
        self._mqtt_counter += 1
        if self._last_mqtt_rate_timestamp == 0:
            self._last_mqtt_rate_timestamp = now
            self._last_mqtt_counter = self._mqtt_counter

        if now - self._last_mqtt_rate_timestamp > DIAGNOSTICS_INTERVAL:
            mqtt_rate = (self._mqtt_counter - self._last_mqtt_counter) / (now - self._last_mqtt_rate_timestamp)
            self.mqtt_update_rate.update_value(mqtt_rate)

            self._last_mqtt_counter = self._mqtt_counter
            self._last_mqtt_rate_timestamp = now

    def read_failed(self):
        # Intentionally a no-op. The read-failure count is sourced
        # authoritatively from Em540MasterStats.read_failed_total in mqtt_data(),
        # which captures every failure mode (connect, primary block, corrupt
        # frame, energy block). This method is kept only because HABridge.read_failed()
        # calls it as part of the listener contract; it must not mutate the
        # counter here or it would double-count against the master stat.
        pass

    def advertise_data(self):
        return [sensor.discovery() for sensor in self._all_sensors()]

    def mqtt_data(self):
        import uptime

        # Pull the latest daily extrema snapshot from the master tracker.
        self._apply_daily_extrema()

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
            self.read_failed_count.update_value(self._em540_master_stats.read_failed_total)
            master_stats = self._em540_master_stats.snapshot_and_reset_interval_extrema()
            self.consumer_missed_updates_total.update_value(master_stats["consumer_missed_updates_total"])
            self.consumer_max_seq_gap.update_value(master_stats["consumer_max_seq_gap"])
            self.acquisition_duration_min_ms.update_value(master_stats["acquisition_duration_ms_min"])
            self.acquisition_duration_max_ms.update_value(master_stats["acquisition_duration_ms_max"])
            self.acquisition_duration_mean_ms.update_value(master_stats["acquisition_duration_ms_mean"])
            self.acquisition_headroom_min_ms.update_value(master_stats["acquisition_headroom_ms_min"])
            self.acquisition_headroom_max_ms.update_value(master_stats["acquisition_headroom_ms_max"])
            self.acquisition_headroom_mean_ms.update_value(master_stats["acquisition_headroom_ms_mean"])
            self.master_tick_overrun_count.update_value(master_stats["tick_overrun_count"])
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

        sensors = self._all_sensors()

        payload = {sensor.safe_name: sensor.value for sensor in sensors}
        return self.state_topic, json.dumps(payload)

    def set_em540_slave_stats(self, stats: EM540SlaveStats):
        self._em540_slave_stats = stats

    def set_em540_master_stats(self, stats: Em540MasterStats):
        self._em540_master_stats = stats
        now = time.monotonic()
        self._master_counter += 1
        if self._last_master_rate_timestamp == 0:
            self._last_master_rate_timestamp = now
            self._last_master_counter = self._master_counter

        if now - self._last_master_rate_timestamp > DIAGNOSTICS_INTERVAL:
            master_rate = (self._master_counter - self._last_master_counter) / (now - self._last_master_rate_timestamp)
            self.update_rate.update_value(master_rate)

            self._last_master_counter = self._master_counter
            self._last_master_rate_timestamp = now

    def set_ts_65a_slave_stats(self, stats: Ts65aSlaveStats):
        self._ts65a_slave_stats = stats
