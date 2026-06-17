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
    ENERGY_BLOCK_CHUNK_SIZE,
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

    # Interval between repeated "still disconnected" log messages (seconds).
    _RECONNECT_LOG_INTERVAL: float = 30.0

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

        # Energy block chunked-read state.
        # When the energy block's skip counter fires, we read chunk 0 (first 16 regs) on
        # that tick, then chunks 1, 2, 3 on alternate ticks with primary-only rest ticks
        # in between. This interlacing lets shorter primary-only ticks absorb jitter.
        self._energy_chunk_pending: int = -1  # -1 = no chunk pending, 1..N = next chunk index to read
        self._energy_chunk_rest: bool = False  # True = skip this tick's chunk read (rest tick)

        # Reconnect log-spam suppression state
        self._consecutive_connect_failures: int = 0
        self._first_failure_time: float = 0.0
        self._last_reconnect_log_time: float = 0.0

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
            self._consecutive_connect_failures = 0

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
                else:
                    self._static_data_valid = True
                    # Keep both buffers aligned so skipped reads in dynamic maps keep prior values.
                    self._copy_meter_data(self._front_data, self._back_data)
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

        # --- Chunked energy block read logic ---
        # The energy block (0x0500, 64 regs) is split into four 16-register reads
        # interlaced with primary-only rest ticks to allow jitter recovery.
        #
        # Scheduling:
        #   - When skip_n_read fires, we read chunk 0 on that tick.
        #   - After each chunk read, we rest for one tick (primary-only) before
        #     reading the next chunk. This gives lighter ticks room to absorb jitter.
        #   - Pattern: chunk0, rest, chunk1, rest, chunk2, rest, chunk3, then idle.

        energy_reg_desc = frame.dynamic_reg_map[_ENERGY_BLOCK_ADDR]
        skip_n_read = energy_reg_desc.skip_n_read
        num_chunks = (ENERGY_BLOCK_TOTAL_SIZE + ENERGY_BLOCK_CHUNK_SIZE - 1) // ENERGY_BLOCK_CHUNK_SIZE

        # Determine whether the energy block's skip counter would fire this tick
        energy_skip_fires = (
            self._dyn_reg_read_counter == 1 or skip_n_read == 0 or (self._dyn_reg_read_counter % (skip_n_read + 1)) == 0
        )

        # If a chunk is pending from a previous tick, handle rest/read alternation
        if self._energy_chunk_pending > 0:
            if self._energy_chunk_rest:
                # Rest tick — skip the chunk read, just backfill and let primary run alone
                self._energy_chunk_rest = False
                energy_read_ok = True
                self._backfill_energy_from_front(frame)
            else:
                # Read tick — seed from front then read the pending chunk
                self._backfill_energy_from_front(frame)
                chunk_idx = self._energy_chunk_pending
                energy_read_ok = await self._read_energy_chunk(frame, chunk_index=chunk_idx)
                if energy_read_ok:
                    next_chunk = chunk_idx + 1
                    if next_chunk < num_chunks:
                        self._energy_chunk_pending = next_chunk
                        self._energy_chunk_rest = True  # rest before next chunk
                    else:
                        self._energy_chunk_pending = -1
                        self._energy_chunk_rest = False
                else:
                    self._energy_chunk_pending = -1
                    self._energy_chunk_rest = False
        elif energy_skip_fires:
            # Start a new chunked energy read: read chunk 0 this tick
            energy_read_ok = await self._read_energy_chunk(frame, chunk_index=0)
            if energy_read_ok:
                if num_chunks > 1:
                    self._energy_chunk_pending = 1
                    self._energy_chunk_rest = True  # rest before chunk 1
                else:
                    self._energy_chunk_pending = -1
            else:
                self._energy_chunk_pending = -1
                self._backfill_energy_from_front(frame)
        else:
            # No energy read this tick — backfill energy values from front buffer
            energy_read_ok = True
            self._backfill_energy_from_front(frame)

        # If the energy chunk read failed (connection closed), abort the tick early.
        if not energy_read_ok:
            modbus_read_ms = (time.perf_counter() - read_start) * 1000.0
            for listener in self._listeners:
                await listener.read_failed()
            self._update_timing_stats(cycle_start, modbus_read_ms, post_read_processing_ms)
            return False

        # Always read the primary block
        is_ok: bool = await self._read_primary_block(frame)
        modbus_read_ms = (time.perf_counter() - read_start) * 1000.0

        if is_ok:
            process_start = time.perf_counter()
            try:
                self._back_data.update_from_frame()
            except (struct.error, ValueError, OverflowError) as e:
                logger.warning("Corrupt frame data, dropping cycle: %s", e)
                is_ok = False
                for listener in self._listeners:
                    await listener.read_failed()

            if is_ok:
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
                try:
                    self._client.close()
                except Exception:
                    logger.debug("Failed to close EM540 client after read error", exc_info=True)
                return False

            if len(result.registers) != num_registers:
                logger.fatal(
                    f"Expected {num_registers} registers but got {len(result.registers)} "
                    f"for address {hex(_DYNAMIC_PRIMARY_BLOCK_ADDR)}"
                )
                os._exit(1)

            self._bad_read_count = 0
            reg_desc.values = result.registers
        except ModbusIOException as ex:
            logger.warning("Modbus IO error reading primary registers from EM540: %s", ex)
            try:
                self._client.close()
            except Exception:
                logger.debug("Failed to close EM540 client after ModbusIOException", exc_info=True)
            return False
        except ModbusException as ex:
            logger.warning("Modbus error reading primary registers from EM540: %s", ex)
            try:
                self._client.close()
            except Exception:
                logger.debug("Failed to close EM540 client after ModbusException", exc_info=True)
            return False

        return True

    async def _read_energy_chunk(self, frame, chunk_index: int) -> bool:
        """Read a single chunk of the energy register block.

        chunk_index 0: registers 0x0500 + 0 .. 0x0500 + 31  (first 32)
        chunk_index 1: registers 0x0500 + 32 .. 0x0500 + 63 (second 32)

        The results are written directly into the appropriate slice of the energy
        register's values list in the frame.
        """
        reg_desc = frame.dynamic_reg_map[_ENERGY_BLOCK_ADDR]
        chunk_offset = chunk_index * ENERGY_BLOCK_CHUNK_SIZE
        start_addr = _ENERGY_BLOCK_ADDR + chunk_offset
        num_registers = min(ENERGY_BLOCK_CHUNK_SIZE, ENERGY_BLOCK_TOTAL_SIZE - chunk_offset)

        self._refresh_client_runtime_config()
        try:
            logger.debug(
                "Reading energy chunk %d from address %s, count=%d",
                chunk_index,
                hex(start_addr),
                num_registers,
            )
            result = await self._client.read_holding_registers(start_addr, count=num_registers, device_id=self.slave_id)

            if result.isError():
                logger.warning(
                    "Modbus error reading energy chunk %d at %s, count=%s: %s",
                    chunk_index,
                    hex(start_addr),
                    num_registers,
                    result,
                )
                try:
                    self._client.close()
                except Exception:
                    logger.debug("Failed to close EM540 client after energy chunk read error", exc_info=True)
                return False

            if len(result.registers) != num_registers:
                logger.fatal(
                    f"Expected {num_registers} registers but got {len(result.registers)} "
                    f"for energy chunk {chunk_index} at address {hex(start_addr)}"
                )
                os._exit(1)

            # Write chunk data into the correct slice of the energy register values
            reg_desc.values[chunk_offset : chunk_offset + num_registers] = result.registers
        except ModbusIOException as ex:
            logger.warning("Modbus IO error reading energy chunk %d from EM540: %s", chunk_index, ex)
            try:
                self._client.close()
            except Exception:
                logger.debug("Failed to close EM540 client after ModbusIOException", exc_info=True)
            return False
        except ModbusException as ex:
            logger.warning("Modbus error reading energy chunk %d from EM540: %s", chunk_index, ex)
            try:
                self._client.close()
            except Exception:
                logger.debug("Failed to close EM540 client after ModbusException", exc_info=True)
            return False

        return True

    def _backfill_energy_from_front(self, frame) -> None:
        """Copy energy register values from the front buffer to keep them current when not reading."""
        front_energy = self._front_data.frame.dynamic_reg_map.get(_ENERGY_BLOCK_ADDR)
        if front_energy is not None:
            frame.dynamic_reg_map[_ENERGY_BLOCK_ADDR].values = list(front_energy.values)

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
                    logger.warning(
                        "Modbus error reading register %s, count=%s: %s",
                        hex(reg_addr),
                        num_registers,
                        result,
                    )
                    if dyn_reg:
                        try:
                            self._client.close()
                        except Exception:
                            logger.debug("Failed to close EM540 client after read error", exc_info=True)
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
            logger.warning("Modbus IO error reading registers from EM540: %s", ex)
            if dyn_reg:
                try:
                    self._client.close()
                except Exception:
                    logger.debug("Failed to close EM540 client after ModbusIOException", exc_info=True)
            return False
        except ModbusException as ex:
            logger.warning("Modbus error reading registers from EM540: %s", ex)
            if dyn_reg:
                try:
                    self._client.close()
                except Exception:
                    logger.debug("Failed to close EM540 client after ModbusException", exc_info=True)
            return False

        return True
