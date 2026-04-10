import asyncio
import logging
import os
import threading
import time
from threading import Thread
from typing import Callable

from pymodbus import FramerType, ModbusException
from pymodbus.client import AsyncModbusSerialClient, AsyncModbusTcpClient, ModbusBaseClient
from pymodbus.exceptions import ModbusIOException

from app.carlo_gavazzi.meter_data import MeterData

logger = logging.getLogger("Em540Master")


class MeterDataListener:
    async def new_data(self, data: MeterData):
        raise NotImplementedError()

    async def read_failed(self):
        raise NotImplementedError()


class Em540MasterStats:
    def __init__(self) -> None:
        self.consumer_missed_updates_total: int = 0
        self.consumer_max_seq_gap: int = 0
        self.read_duration_ms_last: float = 0.0
        self.read_duration_ms_max: float = 0.0
        self.modbus_read_duration_ms_last: float = 0.0
        self.modbus_read_duration_ms_max: float = 0.0
        self.post_read_processing_ms_last: float = 0.0
        self.post_read_processing_ms_max: float = 0.0
        self.non_read_processing_ms_last: float = 0.0
        self.non_read_processing_ms_max: float = 0.0
        self.tick_headroom_ms_last: float = 0.0
        self.tick_headroom_ms_min: float = 0.0
        self.tick_overrun_count: int = 0
        self._listeners: list[Callable[["Em540MasterStats"], None]] = []

    def changed(self) -> None:
        for listener in self._listeners:
            listener(self)

    def add_listener(self, listener: Callable[["Em540MasterStats"], None]) -> None:
        self._listeners.append(listener)


class Em540Master:
    """Represents a Modbus master that reads data from an EM540 device.

    This class will do its best to read data up to a 10Hz rate which the EM540 is able to provide. However, this can
    only be achieved if only a subset of the available registers are read. The more registers are read, the slower the
    update rate will be. The read rate of non-critical data can be configured by setting the 'skip_n_read' parameter in
    the register definitions in em540_data.py.

    Additionally, a high baud rate of 115200bps should be used on the EM540 to achieve the best performance.

    Asyncio is used to avoid blocking the main thread while waiting for Modbus responses, listeners are notified
    in a separate thread.
    """

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
        self._stats_lock: threading.Lock = threading.Lock()
        self._static_read_plan: tuple[int, ...] = tuple(self._front_data.frame.static_reg_map.keys())
        self._dynamic_read_plan: tuple[int, ...] = tuple(self._front_data.frame.dynamic_reg_map.keys())
        logger.setLevel(config.log_level)
        self._client: ModbusBaseClient

        if config.mode == "serial":
            # Create serial client
            self._client = AsyncModbusSerialClient(
                port=config.serial_port,
                framer=FramerType.RTU,
                baudrate=config.baudrate,
                parity=config.parity,
                stopbits=config.stopbits,
                timeout=config.timeout,
                retries=config.retries,
            )
        elif config.mode == "tcp":
            # Create Modbus TCP client
            self._client = AsyncModbusTcpClient(
                host=self._config.host,
                port=self._config.port,
                framer=FramerType.RTU,
                timeout=config.timeout,
                retries=config.retries,
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
        # Simulate connecting to the EM540 device
        if self._config.mode == "serial":
            logger.info("Connecting to EM540 via serial port " + self._config.serial_port + "...")
        else:
            logger.info("Connecting to EM540 at " + self._config.host + ":" + str(self._config.port) + "...")

        try:
            await self._client.connect()
        except Exception as ex:
            logger.warning("Failed to connect to EM540 transport: %s", ex)
            try:
                self._client.close()
            except Exception:
                logger.debug("Failed to close EM540 client after connect failure", exc_info=True)
            return

        if self._client.connected:
            logger.info("Connected to EM540.")
            if not self._static_data_valid:
                logger.debug("Reading static registers from EM540...")
                frame = self._front_data.frame
                if not await self._read_registers(
                    frame.static_reg_map,
                    reg_addrs=self._static_read_plan,
                ):
                    logger.error("Failed to read device info from EM540.")
                    self._client.close()
                else:
                    self._static_data_valid = True
                    # Keep both buffers aligned so skipped reads in dynamic maps keep prior values.
                    self._copy_meter_data(self._front_data, self._back_data)
        else:
            logger.info("Failed to connect to EM540.")

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

    def remove_listener(self, listener: MeterDataListener) -> None:
        if listener in self._listeners:
            self._listeners.remove(listener)
        self._listener_last_seq.pop(listener, None)
        self._listener_threads.pop(listener, None)
        with self._condition:
            self._condition.notify_all()

    @property
    def connected(self) -> bool:
        return self._client.connected

    async def acquire_data(self) -> bool:
        cycle_start = time.perf_counter()
        modbus_read_ms = 0.0
        post_read_processing_ms = 0.0

        # No point reading if we are not connected
        if not self._client.connected:
            for listener in self._listeners:
                await listener.read_failed()
            self._update_timing_stats(cycle_start, modbus_read_ms, post_read_processing_ms)
            return False

        # Use back buffer as the mutable working set and keep front buffer immutable for listeners.
        # To avoid per-tick full-frame copies, skipped dynamic register groups are backfilled from front.
        frame = self._back_data.frame
        self._sync_dynamic_reg_meta(self._front_data.frame.dynamic_reg_map, frame.dynamic_reg_map)

        # Read our dynamic registers
        self._dyn_reg_read_counter += 1
        read_start = time.perf_counter()
        is_ok: bool = await self._read_registers(
            frame.dynamic_reg_map,
            dyn_reg=True,
            fallback_reg_map=self._front_data.frame.dynamic_reg_map,
            reg_addrs=self._dynamic_read_plan,
        )
        modbus_read_ms = (time.perf_counter() - read_start) * 1000.0
        if is_ok:
            process_start = time.perf_counter()
            self._back_data.update_from_frame()

            # Atomic swap so listeners always read a coherent, latest snapshot.
            with self._condition:
                self._front_data, self._back_data = self._back_data, self._front_data
                self._data_seq += 1
                self._condition.notify_all()
            post_read_processing_ms = (time.perf_counter() - process_start) * 1000.0
        else:
            # Now notify listeners
            for listener in self._listeners:
                await listener.read_failed()

        self._update_timing_stats(cycle_start, modbus_read_ms, post_read_processing_ms)

        return is_ok

    def _update_timing_stats(self, cycle_start: float, modbus_read_ms: float, post_read_processing_ms: float) -> None:
        elapsed_ms = (time.perf_counter() - cycle_start) * 1000.0
        non_read_processing_ms = max(0.0, elapsed_ms - modbus_read_ms - post_read_processing_ms)
        tick_budget_ms = float(getattr(self._config, "update_interval", 0.1)) * 1000.0
        headroom_ms = tick_budget_ms - elapsed_ms

        with self._stats_lock:
            self._stats.read_duration_ms_last = elapsed_ms
            self._stats.read_duration_ms_max = max(self._stats.read_duration_ms_max, elapsed_ms)
            self._stats.modbus_read_duration_ms_last = modbus_read_ms
            self._stats.modbus_read_duration_ms_max = max(self._stats.modbus_read_duration_ms_max, modbus_read_ms)
            self._stats.post_read_processing_ms_last = post_read_processing_ms
            self._stats.post_read_processing_ms_max = max(
                self._stats.post_read_processing_ms_max,
                post_read_processing_ms,
            )
            self._stats.non_read_processing_ms_last = non_read_processing_ms
            self._stats.non_read_processing_ms_max = max(
                self._stats.non_read_processing_ms_max,
                non_read_processing_ms,
            )
            self._stats.tick_headroom_ms_last = headroom_ms

            if self._stats.tick_headroom_ms_min == 0:
                self._stats.tick_headroom_ms_min = headroom_ms
            else:
                self._stats.tick_headroom_ms_min = min(self._stats.tick_headroom_ms_min, headroom_ms)

            if headroom_ms < 0:
                self._stats.tick_overrun_count += 1

        # Timing stats are expected to update continuously for diagnostics consumers.
        self._stats.changed()

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

    def _sync_dynamic_reg_meta(self, source_reg_map: dict, target_reg_map: dict) -> None:
        """Keep dynamic register scheduling metadata aligned across buffers."""
        for addr, reg in source_reg_map.items():
            target_reg_map[addr].skip_n_read = reg.skip_n_read

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
                    with self._stats_lock:
                        self._stats.consumer_missed_updates_total += missed
                        self._stats.consumer_max_seq_gap = max(self._stats.consumer_max_seq_gap, gap)
                    self._stats.changed()

                try:
                    loop.run_until_complete(listener.new_data(snapshot))
                    num_errors = 0
                except Exception as e:
                    logger.critical("Listener worker failure, starting error counting", exc_info=True)
                    logger.exception(e)
                    num_errors += 1

                if num_errors > 10:
                    logger.critical("Too many successive listener errors, restarting.")
                    break
        finally:
            loop.close()

        os._exit(2)

    async def _read_registers(
        self,
        reg_map: dict,
        dyn_reg: bool = False,
        fallback_reg_map: dict | None = None,
        reg_addrs: tuple[int, ...] | None = None,
    ) -> bool:
        self._refresh_client_runtime_config()
        try:
            # Read dynamic registers
            # Only read the primary register every cycle, the rest are read less often
            # This is because we can't keep up a 10Hz read rate if we read all registers.
            if reg_addrs is None:
                reg_addrs = tuple(reg_map.keys())

            for reg_addr in reg_addrs:
                reg_desc = reg_map[reg_addr]
                skip_n_read: int = reg_desc.skip_n_read

                # Always perform the first read on all registers
                # Then skip reads as configured
                if dyn_reg and self._dyn_reg_read_counter > 1 and skip_n_read > 0:
                    if (self._dyn_reg_read_counter % (skip_n_read + 1)) != 0:
                        if fallback_reg_map is not None and reg_addr in fallback_reg_map:
                            reg_map[reg_addr].values = list(fallback_reg_map[reg_addr].values)
                        logger.debug(
                            ">>>> Skipping read of '%s' register at %s, read counter=%s, skip_n_read=%s",
                            reg_desc.description,
                            hex(reg_addr),
                            self._dyn_reg_read_counter,
                            skip_n_read,
                        )
                        continue

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
                    logger.error(f"Error reading register {hex(reg_addr)}, count={num_registers}")
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

                self._bad_read_count = 0

                # Store the read values
                reg_map[reg_addr].values = result.registers
        except ModbusIOException as ex:
            logger.error("Modbus IO error reading registers from EM540: %s", ex)
            self._client.close()
            return False
        except ModbusException as ex:
            logger.error("Could not read dynamic registers from EM540: %s", ex)
            self._client.close()
            return False

        return True
