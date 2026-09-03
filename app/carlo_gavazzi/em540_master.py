import asyncio
import logging
import os
import struct
import threading
import time
from threading import Thread
from typing import Callable

from pymodbus import FramerType, ModbusException
from pymodbus.client import AsyncModbusSerialClient, AsyncModbusTcpClient, ModbusBaseClient
from pymodbus.exceptions import ModbusIOException

from app.carlo_gavazzi.em540_data import (
    _DYNAMIC_PRIMARY_BLOCK_ADDR,
    _ENERGY_BLOCK_ADDR,
    ENERGY_BLOCK_TOTAL_SIZE,
)
from app.carlo_gavazzi.meter_data import MeterData

logger = logging.getLogger("Em540Master")


class MeterDataListener:
    async def new_data(self, data: MeterData):
        raise NotImplementedError()

    async def read_failed(self):
        raise NotImplementedError()


class Em540MasterStats:
    def __init__(self) -> None:
        self.lock: threading.Lock = threading.Lock()
        self.consumer_missed_updates_total: int = 0
        self.consumer_max_seq_gap: int = 0
        self.acquisition_duration_ms_min: float = 0.0
        self.acquisition_duration_ms_max: float = 0.0
        self.acquisition_duration_ms_sum: float = 0.0
        self.acquisition_duration_samples: int = 0
        self.acquisition_headroom_ms_min: float = 0.0
        self.acquisition_headroom_ms_max: float = 0.0
        self.acquisition_headroom_ms_sum: float = 0.0
        self.acquisition_headroom_samples: int = 0
        self.tick_overrun_count: int = 0
        # Count of every failed upstream read attempt: transport not connected,
        # primary block read failure, corrupt frame, and energy block chunk read
        # failure. This is the authoritative "RS485 Master Read Failures" metric.
        self.read_failed_total: int = 0
        self._listeners: list[Callable[["Em540MasterStats"], None]] = []

    def snapshot_and_reset_interval_extrema(self) -> dict[str, float | int]:
        """Return a synchronized stats snapshot and reset interval extrema.

        Extrema are reset so subsequent diagnostics emissions reflect only the
        next interval window (DIAGNOSTICS_INTERVAL in HA diagnostics).
        """
        with self.lock:
            if self.acquisition_duration_samples > 0:
                acquisition_duration_ms_mean = self.acquisition_duration_ms_sum / self.acquisition_duration_samples
            else:
                acquisition_duration_ms_mean = 0.0

            if self.acquisition_headroom_samples > 0:
                acquisition_headroom_ms_mean = self.acquisition_headroom_ms_sum / self.acquisition_headroom_samples
            else:
                acquisition_headroom_ms_mean = 0.0

            snapshot = {
                "consumer_missed_updates_total": self.consumer_missed_updates_total,
                "consumer_max_seq_gap": self.consumer_max_seq_gap,
                "acquisition_duration_ms_min": self.acquisition_duration_ms_min,
                "acquisition_duration_ms_max": self.acquisition_duration_ms_max,
                "acquisition_duration_ms_mean": acquisition_duration_ms_mean,
                "acquisition_headroom_ms_min": self.acquisition_headroom_ms_min,
                "acquisition_headroom_ms_max": self.acquisition_headroom_ms_max,
                "acquisition_headroom_ms_mean": acquisition_headroom_ms_mean,
                "tick_overrun_count": self.tick_overrun_count,
            }

            # Reset interval window stats while keeping persistent counters.
            self.acquisition_duration_ms_min = 0.0
            self.acquisition_duration_ms_max = 0.0
            self.acquisition_duration_ms_sum = 0.0
            self.acquisition_duration_samples = 0
            self.acquisition_headroom_ms_min = 0.0
            self.acquisition_headroom_ms_max = 0.0
            self.acquisition_headroom_ms_sum = 0.0
            self.acquisition_headroom_samples = 0

            return snapshot

    def changed(self) -> None:
        for listener in self._listeners:
            try:
                listener(self)
            except Exception:
                logger.debug("Stats listener raised an exception", exc_info=True)

    def add_listener(self, listener: Callable[["Em540MasterStats"], None]) -> None:
        self._listeners.append(listener)


# Quantities tracked for daily extrema. Each entry maps a logical quantity key
# to the attribute name read from SystemData / PhaseData. System current is the
# summed neutral current, exposed as SystemData.An (there is no system.current).
_DAILY_EXTREMA_QUANTITIES: tuple[tuple[str, str, str], ...] = (
    # (quantity key, system attribute, phase attribute)
    ("power", "power", "power"),
    ("current", "An", "current"),
    ("voltage_ln", "line_neutral_voltage", "line_neutral_voltage"),
    ("voltage_ll", "line_line_voltage", "line_line_voltage"),
)

# Scope suffixes: system aggregate plus the three phases (index -> suffix).
_DAILY_EXTREMA_PHASE_SUFFIXES: tuple[str, ...] = ("l1", "l2", "l3")


def _local_day_start(epoch: float) -> float:
    """Return the epoch of the most recent local midnight at or before ``epoch``.

    Uses the process/container local timezone. If TZ is unset, local == UTC.
    """
    lt = time.localtime(epoch)
    # mktime round-trips the local wall-clock struct back to epoch, correctly
    # accounting for the active UTC offset (including DST) at that instant.
    return time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday, 0, 0, 0, 0, 0, -1))


def _local_next_day_start(epoch: float) -> float:
    """Return the epoch of the next local midnight strictly after ``epoch``.

    Computed from the *following* calendar day at 00:00 with is_dst=-1 so
    mktime resolves the correct local offset. This makes the day boundary
    robust to DST transitions where a local day is 23 or 25 hours long; a
    fixed ``+86400`` would drift the boundary by an hour on those days.
    """
    lt = time.localtime(epoch)
    # mktime normalises out-of-range fields, so mday+1 correctly rolls month
    # and year boundaries (e.g. Jan 31 -> Feb 1, Dec 31 -> Jan 1).
    return time.mktime((lt.tm_year, lt.tm_mon, lt.tm_mday + 1, 0, 0, 0, 0, 0, -1))


class DailyExtrema:
    """Tracks per-quantity, per-scope daily min/max, updated on every frame.

    This lives at the master because it must observe *every* upstream frame.
    Downstream consumers (HA notify loop, MQTT publish) are deliberately
    subsampled and would miss intermediate peaks, so extrema cannot be computed
    there. The per-frame cost here is a handful of scalar comparisons and one
    cached float comparison for the day boundary; no allocations occur on the
    steady-state path.

    Extrema are unset (``None``) until the first observed sample and are
    re-seeded from the first sample after each local-midnight rollover. Values
    are signed (power and current can be negative), so resetting to zero would
    corrupt the first post-reset comparison; re-seeding avoids that.
    """

    def __init__(self) -> None:
        self._lock: threading.Lock = threading.Lock()
        # key -> [min, max]; None entries mean "no sample observed yet today".
        self._extrema: dict[str, list[float | None]] = {}
        self._keys: tuple[str, ...] = self._build_keys()
        for key in self._keys:
            self._extrema[key] = [None, None]
        # Local-day boundary cache. next boundary is the next local midnight;
        # the steady-state hot path only compares against it.
        self._day_start: float = 0.0
        self._next_day_start: float = 0.0
        self._initialised: bool = False

    @staticmethod
    def _build_keys() -> tuple[str, ...]:
        keys: list[str] = []
        for quantity, _sys_attr, _phase_attr in _DAILY_EXTREMA_QUANTITIES:
            keys.append(quantity)  # system scope
            for suffix in _DAILY_EXTREMA_PHASE_SUFFIXES:
                keys.append(f"{quantity}_{suffix}")
        return tuple(keys)

    @property
    def keys(self) -> tuple[str, ...]:
        return self._keys

    def _reset_locked(self, wall_clock: float) -> None:
        for pair in self._extrema.values():
            pair[0] = None
            pair[1] = None
        # Anchor to the local-day window containing wall_clock. The next
        # boundary is the following local midnight (DST-aware), not a fixed
        # 24h offset.
        self._day_start = _local_day_start(wall_clock)
        self._next_day_start = _local_next_day_start(wall_clock)
        self._initialised = True

    @staticmethod
    def _accumulate(pair: list[float | None], value: float) -> None:
        if pair[0] is None or value < pair[0]:
            pair[0] = value
        if pair[1] is None or value > pair[1]:
            pair[1] = value

    def update(self, data: MeterData, wall_clock: float) -> None:
        """Fold one frame's samples into today's extrema.

        ``wall_clock`` is the frame's wall-clock time (epoch seconds) used only
        for the local-day rollover check.
        """
        with self._lock:
            # Handle first frame and day rollover. A backwards jump (clock
            # correction) also re-anchors the window.
            if not self._initialised or wall_clock >= self._next_day_start or wall_clock < self._day_start:
                self._reset_locked(wall_clock)

            system = data.system
            phases = data.phases
            for quantity, sys_attr, phase_attr in _DAILY_EXTREMA_QUANTITIES:
                self._accumulate(self._extrema[quantity], getattr(system, sys_attr))
                for idx, suffix in enumerate(_DAILY_EXTREMA_PHASE_SUFFIXES):
                    self._accumulate(
                        self._extrema[f"{quantity}_{suffix}"],
                        getattr(phases[idx], phase_attr),
                    )

    def snapshot(self) -> dict[str, float | None]:
        """Return a flat ``{"<key>_min"/"<key>_max": value}`` mapping.

        Unset extrema (no sample yet today) are reported as ``None``.
        """
        with self._lock:
            result: dict[str, float | None] = {}
            for key, (lo, hi) in self._extrema.items():
                result[f"{key}_min"] = lo
                result[f"{key}_max"] = hi
            return result


class Em540Master:
    """Represents a Modbus master that reads data from an EM540 device.

    This class reads one primary dynamic block and one full energy block back-to-back
    on each acquisition cycle.

    Additionally, a high baud rate of 115200bps should be used on the EM540 to achieve the best performance.

    Asyncio is used to avoid blocking the main thread while waiting for Modbus responses, listeners are notified
    in a separate thread.
    """

    # Interval between repeated "still disconnected" log messages (seconds).
    _RECONNECT_LOG_INTERVAL: float = 30.0

    # Interval between periodic diagnostics debug-log lines (seconds), emitted
    # only when DEBUG logging is enabled for this master.
    _DIAGNOSTICS_LOG_INTERVAL: float = 5.0

    # Fraction of the per-tick budget allowed as jitter before a cycle is counted as an
    # overrun. A single cycle's wall-clock duration includes Modbus I/O round-trip time,
    # which naturally jitters. Without this margin, transient blips that the scheduler
    # fully absorbs on the next tick would inflate the overrun count. Only cycles that
    # exceed budget + margin are counted as genuine overload pressure.
    _TICK_OVERRUN_MARGIN_FRACTION: float = 0.5

    def __init__(self, config) -> None:
        self._config = config
        self._front_data: MeterData = MeterData()
        self._back_data: MeterData = MeterData()
        self.slave_id: int = config.slave_id
        self._dyn_reg_read_counter: int = 0
        self._static_data_valid: bool = False
        self._listeners: list[MeterDataListener] = []
        self._listener_threads: dict[MeterDataListener, Thread] = {}
        self._listener_last_seq: dict[MeterDataListener, int] = {}
        self._listener_stop: bool = False
        self._data_seq: int = 0
        self._condition: threading.Condition = threading.Condition()
        self._stats: Em540MasterStats = Em540MasterStats()
        self._daily_extrema: DailyExtrema = DailyExtrema()
        self._static_read_plan: tuple[int, ...] = tuple(self._front_data.frame.static_reg_map.keys())
        logger.setLevel(config.log_level)
        self._client: ModbusBaseClient

        # Gate: don't publish until the first full energy read has completed.
        self._energy_initial_read_complete: bool = False
        self._fatal_error: threading.Event = threading.Event()

        # Register count mismatch tracking. Transient mismatches (e.g. stale RTU
        # responses after reconnection) are tolerated and discarded. If mismatches
        # persist consecutively, the stream is considered unrecoverably corrupt.
        self._consecutive_reg_mismatch: int = 0
        self._MAX_CONSECUTIVE_REG_MISMATCH: int = 10

        # Reconnect log-spam suppression state
        self._consecutive_connect_failures: int = 0
        self._first_failure_time: float = 0.0
        self._last_reconnect_log_time: float = 0.0

        # Periodic diagnostics debug-logging state. Tracks a windowed read count
        # so a frame rate can be derived and logged alongside the timing stats.
        self._diag_log_last_time: float = 0.0
        self._diag_log_last_tick_count: int = 0

        if config.mode == "serial":
            # Create serial client.
            # reconnect_delay=None disables pymodbus's internal do_reconnect() task.
            self._client = AsyncModbusSerialClient(
                port=config.serial_port,
                framer=FramerType.RTU,
                baudrate=config.baudrate,
                parity=config.parity,
                stopbits=config.stopbits,
                handle_local_echo=config.handle_local_echo,
                timeout=config.timeout,
                retries=config.retries,
                reconnect_delay=None,
            )
        elif config.mode == "tcp":
            # Create Modbus TCP client
            # reconnect_delay=0 disables pymodbus's internal do_reconnect() task.
            # The tick loop in process_loop owns the connection lifecycle exclusively
            # to avoid dual-reconnect races that cause resource exhaustion.
            self._client = AsyncModbusTcpClient(
                host=self._config.host,
                port=self._config.port,
                framer=FramerType.RTU,
                timeout=config.timeout,
                retries=config.retries,
                reconnect_delay=0,
            )
        else:
            raise ValueError(f"Invalid mode '{config.mode}' in configuration, must be 'tcp' or 'serial'")

    def _refresh_client_runtime_config(self) -> None:
        timeout = self._config.timeout
        retries = self._config.retries

        for attr_name, value in (("timeout", timeout), ("retries", retries)):
            if hasattr(self._client, attr_name):
                try:
                    setattr(self._client, attr_name, value)
                except Exception:
                    logger.debug("Failed to update client attribute %s", attr_name, exc_info=True)

        for container_name, nested_attr in (
            ("params", "timeout"),
            ("params", "retries"),
            ("comm_params", "timeout"),
            ("comm_params", "retries"),
        ):
            container = getattr(self._client, container_name, None)
            if container is None or not hasattr(container, nested_attr):
                continue
            try:
                setattr(container, nested_attr, timeout if nested_attr == "timeout" else retries)
            except Exception:
                logger.debug(
                    "Failed to update client nested attribute %s.%s",
                    container_name,
                    nested_attr,
                    exc_info=True,
                )

    async def connect(self) -> None:
        self._refresh_client_runtime_config()

        # Only log the first attempt and periodic reminders to avoid spam during outages.
        is_first_attempt = self._consecutive_connect_failures == 0

        if is_first_attempt:
            if self._config.mode == "serial":
                logger.info("Connecting to EM540 via serial port %s...", self._config.serial_port)
            else:
                logger.info("Connecting to EM540 at %s:%s...", self._config.host, self._config.port)

        try:
            await self._client.connect()
        except Exception as ex:
            if is_first_attempt:
                logger.warning("Failed to connect to EM540 transport: %s", ex)
            else:
                logger.debug("Failed to connect to EM540 transport: %s", ex)
            try:
                self._client.close()
            except Exception:
                logger.debug("Failed to close EM540 client after connect failure", exc_info=True)
            self._record_connect_failure(time.perf_counter())
            return

        if self._client.connected:
            # Successful connection — log recovery summary if we had prior failures.
            if self._consecutive_connect_failures > 0:
                outage_duration = time.perf_counter() - self._first_failure_time
                logger.info(
                    "Connected to EM540 after %.1fs (%d failed attempt%s).",
                    outage_duration,
                    self._consecutive_connect_failures,
                    "s" if self._consecutive_connect_failures != 1 else "",
                )
            else:
                logger.info("Connected to EM540.")
            if not self._static_data_valid:
                logger.debug("Reading static registers from EM540...")
                frame = self._front_data.frame
                if not await self._read_registers(
                    frame.static_reg_map,
                    reg_addrs=self._static_read_plan,
                ):
                    logger.error("Failed to read device info from EM540.")
                    try:
                        self._client.close()
                    except Exception:
                        logger.debug("Failed to close EM540 client after static read failure", exc_info=True)
                    self._record_connect_failure(time.perf_counter())
                    return
                else:
                    self._static_data_valid = True
                    # Keep both buffers aligned so skipped reads in dynamic maps keep prior values.
                    self._copy_meter_data(self._front_data, self._back_data)
            # Connection fully ready (TCP connected and static data available).
            self._consecutive_connect_failures = 0
        else:
            if is_first_attempt:
                logger.warning("Failed to connect to EM540.")
            self._record_connect_failure(time.perf_counter())

    def _record_connect_failure(self, now: float) -> None:
        """Track consecutive connection failures and emit periodic summary logs."""
        if self._consecutive_connect_failures == 0:
            self._first_failure_time = now
            self._last_reconnect_log_time = now
        self._consecutive_connect_failures += 1

        # Emit a periodic "still trying" message so operators know the service is alive.
        elapsed_since_last_log = now - self._last_reconnect_log_time
        if elapsed_since_last_log >= self._RECONNECT_LOG_INTERVAL:
            outage_duration = now - self._first_failure_time
            logger.warning(
                "Still unable to reach EM540 (%d attempts over %.0fs).",
                self._consecutive_connect_failures,
                outage_duration,
            )
            self._last_reconnect_log_time = now

    @property
    def data(self) -> MeterData:
        return self._front_data

    async def disconnect(self) -> None:
        # Simulate disconnecting from the EM540 device
        if self._client.connected:
            logger.info("Disconnecting from EM540...")
            self._client.close()
        else:
            logger.info("Already disconnected.")

    def add_listener(self, listener: MeterDataListener) -> None:
        self._listeners.append(listener)
        self._listener_last_seq[listener] = 0

        thread = Thread(
            target=self._listener_loop,
            args=(listener,),
            daemon=True,
            name=f"em540-listener-{len(self._listener_threads) + 1}",
        )
        self._listener_threads[listener] = thread
        thread.start()

    def add_stats_listener(self, listener: Callable[[Em540MasterStats], None]) -> None:
        self._stats.add_listener(listener)

    @property
    def daily_extrema(self) -> "DailyExtrema":
        return self._daily_extrema

    def remove_listener(self, listener: MeterDataListener) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)
        self._listener_last_seq.pop(listener, None)
        self._listener_threads.pop(listener, None)
        with self._condition:
            self._condition.notify_all()

    def stop_listeners(self) -> None:
        """Signal all listener threads to stop and unblock any threads waiting on the condition."""
        with self._condition:
            self._listener_stop = True
            self._condition.notify_all()

    @property
    def has_fatal_error(self) -> bool:
        return self._fatal_error.is_set()

    @property
    def connected(self) -> bool:
        return self._client.connected

    async def acquire_data(
        self,
        tick_deadline_mono: float | None = None,
        tick_interval_s: float | None = None,
    ) -> bool:
        cycle_start = time.perf_counter()

        # No point reading if we are not connected
        if not self._client.connected:
            await self._notify_listeners_read_failed()
            self._update_timing_stats(
                cycle_start=cycle_start,
                tick_deadline_mono=tick_deadline_mono,
                tick_interval_s=tick_interval_s,
            )
            return False

        # Use back buffer as the mutable working set and keep front buffer immutable for listeners.
        frame = self._back_data.frame

        # Read our dynamic registers
        self._dyn_reg_read_counter += 1

        # --- Primary block read (critical path) ---
        # Read the primary block FIRST. This contains real-time power/voltage/current
        # data that downstream consumers depend on at 10Hz. Energy read failures must
        # never prevent primary data from reaching listeners.
        is_ok = await self._read_primary_block(frame)

        if not is_ok:
            await self._notify_listeners_read_failed()
            self._update_timing_stats(
                cycle_start=cycle_start,
                tick_deadline_mono=tick_deadline_mono,
                tick_interval_s=tick_interval_s,
            )
            return False

        # --- Temporary baseline mode: full energy block every tick ---
        # Read the whole energy block immediately after the primary block so
        # each tick measures two back-to-back Modbus reads.
        energy_read_ok = await self._read_full_energy_block(frame)
        if energy_read_ok:
            if not self._energy_initial_read_complete:
                self._energy_initial_read_complete = True
                logger.info("Initial full energy register read complete.")
        else:
            # Energy read failed — primary data still published this tick, so we
            # do NOT abort or signal listeners read_failed (that would flap their
            # circuit breakers on a partial miss). Still count it so the
            # read-failure metric accounts for every upstream read error, and
            # preserve previously-known energy values.
            self._count_read_failure()
            self._backfill_energy_from_front(frame)

        # --- Post-read processing and publication ---
        try:
            self._back_data.update_from_frame()
        except (struct.error, ValueError, OverflowError) as e:
            logger.warning("Corrupt frame data, dropping cycle: %s", e)
            is_ok = False
            await self._notify_listeners_read_failed()

        if is_ok:
            # Fold this frame into daily extrema BEFORE the buffer swap. This is
            # the only point that observes every upstream frame; downstream
            # consumers are subsampled and would miss intermediate peaks. The
            # primary block (power/voltage/current) is always read above, so
            # these quantities are valid on every successful frame regardless of
            # the energy-read publish gate below.
            self._daily_extrema.update(self._back_data, self._back_data.timestamp)

            # Swap buffers under the condition lock so the front buffer stays immutable
            # for listener threads (which read _front_data under _condition). The swap is
            # always performed so previous energy values can be preserved across failed reads.
            # Only advance the sequence and wake listeners
            # once we have static data AND a complete initial energy read, so downstream
            # consumers never observe a frame with zero energy values on startup.
            with self._condition:
                self._front_data, self._back_data = self._back_data, self._front_data
                if self._static_data_valid and self._energy_initial_read_complete:
                    self._data_seq += 1
                    self._condition.notify_all()

        self._update_timing_stats(
            cycle_start=cycle_start,
            tick_deadline_mono=tick_deadline_mono,
            tick_interval_s=tick_interval_s,
        )

        return is_ok

    def _count_read_failure(self) -> None:
        """Record a failed upstream read attempt in diagnostics.

        Covers every read failure mode: not connected, primary block failure,
        corrupt frame, and energy block chunk failure. This keeps the
        "RS485 Master Read Failures" metric authoritative regardless of whether
        the failure aborts the tick (primary/connect/corrupt) or is tolerated
        without aborting (energy chunk).

        Does not notify stats listeners here: every tick — including failure
        ticks — calls _update_timing_stats() afterwards, which fires
        _stats.changed() exactly once. Notifying here too would double-notify on
        failure ticks and add avoidable work in the 10Hz path.
        """
        with self._stats.lock:
            self._stats.read_failed_total += 1

    async def _notify_listeners_read_failed(self) -> None:
        """Signal listeners that the tick produced no usable data.

        Also counts the failure. Used for whole-cycle failures (not connected,
        primary block failure, corrupt frame) where downstream consumers must
        fail closed. Energy chunk failures do NOT use this path: they count the
        failure but keep publishing primary data, so listeners are not told the
        cycle failed (which would flap their circuit breakers needlessly).
        """
        self._count_read_failure()
        for listener in self._listeners:
            await listener.read_failed()

    async def _read_primary_block(self, frame) -> bool:
        """Read the primary dynamic register block (0x0000)."""
        reg_desc = frame.dynamic_reg_map[_DYNAMIC_PRIMARY_BLOCK_ADDR]
        num_registers = len(reg_desc.values)

        self._refresh_client_runtime_config()
        try:
            result = await self._client.read_holding_registers(
                _DYNAMIC_PRIMARY_BLOCK_ADDR, count=num_registers, device_id=self.slave_id
            )

            if result.isError():
                logger.warning(
                    "Modbus error reading register %s, count=%s: %s",
                    hex(_DYNAMIC_PRIMARY_BLOCK_ADDR),
                    num_registers,
                    result,
                )
                return False

            if len(result.registers) != num_registers:
                self._consecutive_reg_mismatch += 1
                logger.warning(
                    "Register count mismatch: expected %d but got %d for address %s (consecutive: %d/%d)",
                    num_registers,
                    len(result.registers),
                    hex(_DYNAMIC_PRIMARY_BLOCK_ADDR),
                    self._consecutive_reg_mismatch,
                    self._MAX_CONSECUTIVE_REG_MISMATCH,
                )
                if self._consecutive_reg_mismatch >= self._MAX_CONSECUTIVE_REG_MISMATCH:
                    logger.critical(
                        "Persistent register count mismatch (%d consecutive), stream unrecoverable — exiting.",
                        self._consecutive_reg_mismatch,
                    )
                    os._exit(1)
                return False

            self._consecutive_reg_mismatch = 0
            reg_desc.values = result.registers
        except ModbusIOException as ex:
            logger.warning("Modbus IO error reading primary registers from EM540: %s", ex)
            return False
        except ModbusException as ex:
            logger.warning("Modbus error reading primary registers from EM540: %s", ex)
            return False

        return True

    async def _read_full_energy_block(self, frame) -> bool:
        """Read the full energy register block (0x0500) in one Modbus request."""
        reg_desc = frame.dynamic_reg_map[_ENERGY_BLOCK_ADDR]
        start_addr = _ENERGY_BLOCK_ADDR
        num_registers = ENERGY_BLOCK_TOTAL_SIZE

        self._refresh_client_runtime_config()
        try:
            result = await self._client.read_holding_registers(start_addr, count=num_registers, device_id=self.slave_id)

            if result.isError():
                logger.warning(
                    "Modbus error reading full energy block at %s, count=%s: %s",
                    hex(start_addr),
                    num_registers,
                    result,
                )
                return False

            if len(result.registers) != num_registers:
                self._consecutive_reg_mismatch += 1
                logger.warning(
                    "Register count mismatch: expected %d but got %d for full energy block "
                    "at address %s (consecutive: %d/%d)",
                    num_registers,
                    len(result.registers),
                    hex(start_addr),
                    self._consecutive_reg_mismatch,
                    self._MAX_CONSECUTIVE_REG_MISMATCH,
                )
                if self._consecutive_reg_mismatch >= self._MAX_CONSECUTIVE_REG_MISMATCH:
                    logger.critical(
                        "Persistent register count mismatch (%d consecutive), stream unrecoverable — exiting.",
                        self._consecutive_reg_mismatch,
                    )
                    os._exit(1)
                return False

            self._consecutive_reg_mismatch = 0
            reg_desc.values = result.registers
        except ModbusIOException as ex:
            logger.warning("Modbus IO error reading full energy block from EM540: %s", ex)
            return False
        except ModbusException as ex:
            logger.warning("Modbus error reading full energy block from EM540: %s", ex)
            return False

        return True

    def _backfill_energy_from_front(self, frame) -> None:
        """Copy energy register values from the front buffer when a full energy read fails."""
        front_energy = self._front_data.frame.dynamic_reg_map.get(_ENERGY_BLOCK_ADDR)
        if front_energy is not None:
            frame.dynamic_reg_map[_ENERGY_BLOCK_ADDR].values = list(front_energy.values)

    def _update_timing_stats(
        self,
        cycle_start: float,
        tick_deadline_mono: float | None = None,
        tick_interval_s: float | None = None,
    ) -> None:
        acquisition_end = time.perf_counter()
        acquisition_duration_ms = (acquisition_end - cycle_start) * 1000.0

        if tick_interval_s is None:
            tick_interval_s = float(getattr(self._config, "update_interval", 0.1))

        if tick_deadline_mono is not None and tick_interval_s > 0:
            # Signed slack against the immediate following tick boundary.
            # Positive means we completed before that boundary; negative means late.
            first_following_tick = tick_deadline_mono + tick_interval_s
            headroom_ms = (first_following_tick - acquisition_end) * 1000.0
        elif tick_interval_s > 0:
            headroom_ms = tick_interval_s * 1000.0 - acquisition_duration_ms
        else:
            headroom_ms = 0.0

        with self._stats.lock:
            self._stats.acquisition_duration_ms_sum += acquisition_duration_ms
            self._stats.acquisition_duration_samples += 1
            if self._stats.acquisition_duration_samples == 1:
                self._stats.acquisition_duration_ms_min = acquisition_duration_ms
                self._stats.acquisition_duration_ms_max = acquisition_duration_ms
            else:
                self._stats.acquisition_duration_ms_min = min(
                    self._stats.acquisition_duration_ms_min,
                    acquisition_duration_ms,
                )
                self._stats.acquisition_duration_ms_max = max(
                    self._stats.acquisition_duration_ms_max,
                    acquisition_duration_ms,
                )

            self._stats.acquisition_headroom_ms_sum += headroom_ms
            self._stats.acquisition_headroom_samples += 1
            if self._stats.acquisition_headroom_samples == 1:
                self._stats.acquisition_headroom_ms_min = headroom_ms
                self._stats.acquisition_headroom_ms_max = headroom_ms
            else:
                self._stats.acquisition_headroom_ms_min = min(
                    self._stats.acquisition_headroom_ms_min,
                    headroom_ms,
                )
                self._stats.acquisition_headroom_ms_max = max(
                    self._stats.acquisition_headroom_ms_max,
                    headroom_ms,
                )

            # Overrun means we missed the immediate following tick boundary.
            overrun_threshold_ms = tick_interval_s * 1000.0 * self._TICK_OVERRUN_MARGIN_FRACTION
            if tick_interval_s > 0 and headroom_ms < -overrun_threshold_ms:
                self._stats.tick_overrun_count += 1

        # Timing stats are expected to update continuously for diagnostics consumers.
        self._stats.changed()

        self._maybe_log_diagnostics()

    def _maybe_log_diagnostics(self) -> None:
        """Emit a periodic diagnostics summary when DEBUG logging is enabled.

        Logs the master's frame rate (reads/second, measured over the elapsed
        window) alongside the min/max timing stats. Rate-limited to
        _DIAGNOSTICS_LOG_INTERVAL so it stays readable at 10Hz. Cheap when DEBUG
        is disabled (a single isEnabledFor check).
        """
        if not logger.isEnabledFor(logging.DEBUG):
            return

        now = time.perf_counter()
        if self._diag_log_last_time == 0.0:
            self._diag_log_last_time = now
            self._diag_log_last_tick_count = self._dyn_reg_read_counter
            return

        elapsed = now - self._diag_log_last_time
        if elapsed < self._DIAGNOSTICS_LOG_INTERVAL:
            return

        ticks = self._dyn_reg_read_counter - self._diag_log_last_tick_count
        frame_rate = ticks / elapsed if elapsed > 0 else 0.0

        # Capture the current window's extrema under the lock, then release it
        # before formatting/logging. We read directly (rather than calling
        # snapshot_and_reset_interval_extrema()) so this debug logging does not
        # steal/reset the interval stats that HA diagnostics consumes, and we do
        # not hold the stats lock across string formatting or handler I/O.
        with self._stats.lock:
            s = self._stats
            dur_min = s.acquisition_duration_ms_min
            dur_max = s.acquisition_duration_ms_max
            dur_mean = (
                s.acquisition_duration_ms_sum / s.acquisition_duration_samples
                if s.acquisition_duration_samples
                else 0.0
            )
            head_min = s.acquisition_headroom_ms_min
            head_max = s.acquisition_headroom_ms_max
            head_mean = (
                s.acquisition_headroom_ms_sum / s.acquisition_headroom_samples
                if s.acquisition_headroom_samples
                else 0.0
            )
            overruns = s.tick_overrun_count
            read_failures = s.read_failed_total
            missed_updates = s.consumer_missed_updates_total
            max_seq_gap = s.consumer_max_seq_gap

        logger.debug(
            "Master diagnostics: frame_rate=%.2f Hz | "
            "acquisition_ms min=%.2f max=%.2f mean=%.2f | "
            "headroom_ms min=%.2f max=%.2f mean=%.2f | "
            "overruns=%d | read_failures=%d | missed_updates=%d max_seq_gap=%d",
            frame_rate,
            dur_min,
            dur_max,
            dur_mean,
            head_min,
            head_max,
            head_mean,
            overruns,
            read_failures,
            missed_updates,
            max_seq_gap,
        )

        self._diag_log_last_time = now
        self._diag_log_last_tick_count = self._dyn_reg_read_counter

    def _copy_meter_data(self, source: MeterData, target: MeterData) -> None:
        """Copy frame register values between buffers while keeping object allocation stable."""
        source_frame = source.frame
        target_frame = target.frame

        for addr, reg in source_frame.static_reg_map.items():
            target_frame.static_reg_map[addr].values = list(reg.values)
            target_frame.static_reg_map[addr].skip_n_read = reg.skip_n_read

        for addr, reg in source_frame.dynamic_reg_map.items():
            target_frame.dynamic_reg_map[addr].values = list(reg.values)
            target_frame.dynamic_reg_map[addr].skip_n_read = reg.skip_n_read

        for addr, reg in source_frame.remapped_reg_map.items():
            target_frame.remapped_reg_map[addr].values = list(reg.values)
            target_frame.remapped_reg_map[addr].skip_n_read = reg.skip_n_read

    def _listener_loop(self, listener: MeterDataListener) -> None:
        num_errors = 0
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            while True:
                snapshot: MeterData | None = None
                gap: int = 0

                with self._condition:
                    if self._listener_stop:
                        return

                    if listener not in self._listener_last_seq:
                        return

                    last_seq = self._listener_last_seq.get(listener, 0)
                    while self._data_seq == last_seq and not self._listener_stop:
                        self._condition.wait()
                        if listener not in self._listener_last_seq:
                            return

                    if self._listener_stop:
                        return

                    current_seq = self._data_seq
                    gap = current_seq - last_seq
                    self._listener_last_seq[listener] = current_seq
                    snapshot = self._front_data

                if gap > 1:
                    missed = gap - 1
                    with self._stats.lock:
                        self._stats.consumer_missed_updates_total += missed
                        self._stats.consumer_max_seq_gap = max(self._stats.consumer_max_seq_gap, gap)
                    self._stats.changed()

                try:
                    loop.run_until_complete(listener.new_data(snapshot))
                    num_errors = 0
                except Exception:
                    num_errors += 1
                    if num_errors <= 3 or num_errors % 10 == 0:
                        logger.critical("Listener worker failure (%d consecutive errors)", num_errors, exc_info=True)

                if num_errors > 10:
                    logger.critical("Too many successive listener errors, restarting.")
                    break
        except Exception:
            logger.critical("Listener thread crashed unexpectedly", exc_info=True)
        finally:
            loop.close()

        # Only reached via break (too many errors) or except (crash), never via
        # the clean return paths (stop_listeners / listener removal).
        if not self._listener_stop:
            logger.critical("Listener thread terminated unrecoverably, signalling process shutdown.")
            self._fatal_error.set()

    async def _read_registers(
        self,
        reg_map: dict,
        reg_addrs: tuple[int, ...] | None = None,
    ) -> bool:
        self._refresh_client_runtime_config()
        try:
            if reg_addrs is None:
                reg_addrs = tuple(reg_map.keys())

            for reg_addr in reg_addrs:
                reg_desc = reg_map[reg_addr]

                num_registers: int = len(reg_desc.values)
                logger.debug(
                    "Reading '%s' from start register address %s, count=%s",
                    reg_desc.description,
                    hex(reg_addr),
                    num_registers,
                )
                result = await self._client.read_holding_registers(
                    reg_addr, count=num_registers, device_id=self.slave_id
                )

                if result.isError():
                    logger.warning(
                        "Modbus error reading register %s, count=%s: %s",
                        hex(reg_addr),
                        num_registers,
                        result,
                    )
                    return False

                # Check if we received the expected number of registers
                # Force quit to be safe, as it seems at that stage the client is in a bad state and further reads will
                # fail with out-of-order responses. Resetting the client could be better, but for now just exit.
                if len(result.registers) != num_registers:
                    logger.fatal(
                        f"Expected {num_registers} registers but got {len(result.registers)} "
                        f"for address {hex(reg_addr)}"
                    )
                    os._exit(1)

                # Store the read values
                reg_map[reg_addr].values = result.registers
        except ModbusIOException as ex:
            logger.warning("Modbus IO error reading registers from EM540: %s", ex)
            return False
        except ModbusException as ex:
            logger.warning("Modbus error reading registers from EM540: %s", ex)
            return False

        return True
