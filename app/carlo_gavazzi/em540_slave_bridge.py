import asyncio
import logging
from threading import Event, Lock, Thread
from typing import Callable

from pymodbus import FramerType
from pymodbus.server import ModbusTcpServer
from pymodbus.simulator.simdata import DataType, SimData
from pymodbus.simulator.simdevice import SimDevice

from app.carlo_gavazzi.em540_data import Em540Frame
from app.carlo_gavazzi.em540_master import MeterDataListener
from app.carlo_gavazzi.em540_slave_stats import EM540SlaveStats
from app.carlo_gavazzi.meter_data import MeterData
from app.utils.pdu_helper import PduHelper

REG_OFFSET = 0  # SimDevice uses raw 0-based Modbus protocol addresses directly

# Holding register function code used for async_setValues/async_getValues.
_FC_HOLDING_REGISTER = 3

# Some downstream EM540 clients issue contiguous bulk reads that span sparse gaps.
# Populate these compatibility windows with explicit zero placeholders so reads
# return stable values instead of illegal-address exceptions.
_COMPATIBILITY_RANGES: tuple[tuple[int, int], ...] = ((0x0000, 0x0160),)

logger = logging.getLogger("em540-slave")


def _expanded_addresses(reg_map: dict[int, object]) -> set[int]:
    addrs: set[int] = set()
    for addr, reg in reg_map.items():
        addrs.update(range(addr, addr + len(reg.values)))
    return addrs


def _build_simdata(frame: Em540Frame) -> list[SimData]:
    """Build a flat list of SimData entries (one per register address) from an Em540Frame.

    pymodbus 3.13+ SimDevice enforces strict non-overlapping address validation.
    The EM540 register layout has multi-register dynamic blocks that span into addresses
    where static/remapped registers are independently declared.  To avoid overlap conflicts
    we flatten everything into individual single-register entries.
    """
    values: dict[int, int] = {}

    # Dynamic registers first (large contiguous blocks)
    for addr, reg in frame.dynamic_reg_map.items():
        for i, v in enumerate(reg.values):
            values[addr + REG_OFFSET + i] = v

    # Static registers overlay specific addresses within dynamic ranges
    for addr, reg in frame.static_reg_map.items():
        for i, v in enumerate(reg.values):
            values[addr + REG_OFFSET + i] = v

    # Remapped registers may also overlap dynamic ranges
    for addr, reg in frame.remapped_reg_map.items():
        for i, v in enumerate(reg.values):
            values[addr + REG_OFFSET + i] = v

    # Fill compatibility ranges with zeros where no register exists yet
    for range_start, range_end in _COMPATIBILITY_RANGES:
        for addr in range(range_start, range_end + 1):
            values.setdefault(addr + REG_OFFSET, 0)

    # Build sorted SimData entries — one per address for zero overlap risk
    return [SimData(addr, values=[val], datatype=DataType.UINT16) for addr, val in sorted(values.items())]


def _build_contiguous_runs(reg_map: dict[int, object]) -> list[tuple[int, list[int]]]:
    """Group register map entries into contiguous address runs.

    Returns a list of (start_address, list_of_addr_keys) tuples where each group
    represents registers that can be written in a single async_setValues call.
    """
    if not reg_map:
        return []

    sorted_addrs = sorted(reg_map.keys())
    runs: list[tuple[int, list[int]]] = []
    current_start = sorted_addrs[0]
    current_keys = [current_start]
    current_end = current_start + len(reg_map[current_start].values) - 1

    for addr in sorted_addrs[1:]:
        if addr == current_end + 1:
            current_keys.append(addr)
            current_end = addr + len(reg_map[addr].values) - 1
        else:
            runs.append((current_start, current_keys))
            current_start = addr
            current_keys = [addr]
            current_end = addr + len(reg_map[addr].values) - 1

    runs.append((current_start, current_keys))
    return runs


class Em540Slave(MeterDataListener):
    """Represents a Modbus slave that serves data read from an EM540 master."""

    def __init__(self, config, frame: Em540Frame) -> None:
        self._config = config
        self.host: str = config.host
        self.rtu_port: int = config.rtu_port
        self.tcp_port: int = config.tcp_port
        self.last_pdu: object = None
        self._slave_id: int = config.slave_id
        self._pdu_helper: PduHelper = PduHelper(logger, lambda: self._config.update_timeout)
        self._stats: EM540SlaveStats = EM540SlaveStats()
        logger.setLevel(config.log_level)

        logger.info("Building SimDevice register map...")
        simdata = _build_simdata(frame)

        self._static_addrs = tuple(frame.static_reg_map.keys())
        self._dynamic_addrs = tuple(frame.dynamic_reg_map.keys())
        self._remapped_runs = _build_contiguous_runs(frame.remapped_reg_map)
        dynamic_written_addrs = _expanded_addresses(frame.dynamic_reg_map)
        remapped_written_addrs = _expanded_addresses(frame.remapped_reg_map)
        self._overlapped_static_addrs: tuple[int, ...] = tuple(
            addr for addr in self._static_addrs if addr in dynamic_written_addrs or addr in remapped_written_addrs
        )
        self._last_static_value_ids: dict[int, int] = {
            addr: id(frame.static_reg_map[addr].values) for addr in self._static_addrs
        }
        self._static_synced: bool = any(
            any(value != 0 for value in frame.static_reg_map[addr].values) for addr in self._static_addrs
        )

        device = SimDevice(self._slave_id, simdata=simdata)

        # Modbus RTU over socket server
        self._rtu_server: ModbusTcpServer = ModbusTcpServer(
            framer=FramerType.RTU,
            context=device,
            address=(self.host, self.rtu_port),
            trace_pdu=self._pdu_helper.on_pdu,
            trace_connect=self._rtu_trace_connect,
        )

        # Modbus TCP server — shares the same SimCore context so both protocols
        # serve identical register state.
        self._tcp_server: ModbusTcpServer = ModbusTcpServer(
            framer=FramerType.SOCKET,
            context=device,
            address=(self.host, self.tcp_port),
            trace_pdu=self._pdu_helper.on_pdu,
            trace_connect=self._tcp_trace_connect,
        )
        self._tcp_server.context = self._rtu_server.context
        self._server_loop: asyncio.AbstractEventLoop | None = None

        # Direct access to the SimRuntime register array for lock-based writes.
        # This avoids routing writes through the server event loop (which caused
        # starvation deadlocks when many downstream connections saturated the loop).
        # After construction, server.context is a SimCore wrapping the SimDevice.
        sim_core = self._rtu_server.context
        sim_runtime = sim_core.devices[self._slave_id]
        # SimRuntime uses block key "x" when a single SimDevice is used (non-dict build),
        # otherwise "h" for holding registers.
        block_key = "x" if "x" in sim_runtime.block else "h"
        if block_key not in sim_runtime.block:
            raise RuntimeError(f"SimRuntime has no register block (available keys: {list(sim_runtime.block.keys())})")
        self._reg_start_address: int = sim_runtime.block[block_key][0]
        self._registers: list[int] = sim_runtime.block[block_key][2]
        self._reg_lock: Lock = Lock()

    def _rtu_trace_connect(self, connect: bool) -> None:
        logger.debug("Client connection to RTU server: %s", connect)
        if connect:
            self._stats.rtu_client_count += 1
            logger.info("Downstream RTU client connected (total: %d).", self._stats.rtu_client_count)
        else:
            self._stats.rtu_client_count -= 1
            self._stats.rtu_client_disconnect_count += 1
            logger.info("Downstream RTU client disconnected (total: %d).", self._stats.rtu_client_count)
        self._stats.changed()

    def _tcp_trace_connect(self, connect: bool) -> None:
        logger.debug("Client connection to TCP server: %s", connect)
        if connect:
            self._stats.tcp_client_count += 1
            logger.info("Downstream TCP client connected (total: %d).", self._stats.tcp_client_count)
        else:
            self._stats.tcp_client_count -= 1
            self._stats.tcp_client_disconnect_count += 1
            logger.info("Downstream TCP client disconnected (total: %d).", self._stats.tcp_client_count)
        self._stats.changed()

    def add_stats_listener(self, listener: Callable[[EM540SlaveStats], None]) -> None:
        self._stats.add_listener(listener)

    async def start(self) -> None:
        """Start downstream Modbus servers on a dedicated event loop.

        This isolates downstream server I/O (client connections, request handling)
        from the main event loop where the upstream master reads run at 10Hz.
        Prevents downstream activity from starving the upstream read path.
        """
        self._server_loop = asyncio.new_event_loop()
        ready = Event()
        startup_error: list[BaseException] = []

        async def _run_servers():
            try:
                await self._rtu_server.serve_forever(background=True)
                await self._tcp_server.serve_forever(background=True)
            except Exception as e:
                startup_error.append(e)
            finally:
                ready.set()

        def _server_thread():
            asyncio.set_event_loop(self._server_loop)
            self._server_loop.create_task(_run_servers())
            self._server_loop.run_forever()

        thread = Thread(target=_server_thread, daemon=True, name="em540-slave-servers")
        thread.start()

        signalled = await asyncio.to_thread(ready.wait, 5.0)
        if not signalled:
            raise TimeoutError("EM540 downstream servers failed to start within 5 seconds")
        if startup_error:
            raise startup_error[0]

    def _sync_pdu_stats(self) -> None:
        stale_age = self._pdu_helper.stale_age_seconds()
        self._stats.stale_data_age_ms = 0.0 if stale_age is None else stale_age * 1000.0
        self._stats.circuit_breaker_open = self._pdu_helper.circuit_open
        self._stats.circuit_breaker_open_count = self._pdu_helper.circuit_open_count
        self._stats.dropped_stale_request_count = self._pdu_helper.dropped_request_count
        self._stats.changed()

    async def _flush_writes(self, writes: list[tuple[int, list[int]]]) -> None:
        """Write register values directly into the SimRuntime register array.

        Uses a threading lock instead of scheduling coroutines on the server
        event loop. This eliminates the cross-thread dependency that caused
        starvation deadlocks when many downstream connections saturated the
        server loop — the listener thread would block forever waiting for
        run_coroutine_threadsafe to execute on the overloaded loop.

        The register array is a plain Python list; writes are fast in-memory
        slice assignments. The lock serializes concurrent writers to prevent
        interleaving of multi-register updates. The server-side read path
        (SimRuntime.get_reg_block) is not coordinated by this lock; torn
        reads of a single multi-register update are theoretically possible
        but harmless — the next tick (100ms) will overwrite with fresh data.
        """
        if not writes:
            return

        with self._reg_lock:
            for address, values in writes:
                offset = address - self._reg_start_address
                end = offset + len(values)
                if offset < 0 or end > len(self._registers):
                    logger.error(
                        "Register write out of bounds: address=%s, offset=%d, end=%d, array_len=%d",
                        hex(address),
                        offset,
                        end,
                        len(self._registers),
                    )
                    continue
                self._registers[offset:end] = values

    def _sync_static_registers_if_changed(self, frame: Em540Frame, writes: list[tuple[int, list[int]]]) -> bool:
        if self._static_synced:
            return False

        static_changed = False
        for addr in self._static_addrs:
            current_id = id(frame.static_reg_map[addr].values)
            if self._last_static_value_ids.get(addr) != current_id:
                static_changed = True
                break

        if not static_changed:
            return False

        for addr in self._static_addrs:
            writes.append((addr + REG_OFFSET, frame.static_reg_map[addr].values))
            self._last_static_value_ids[addr] = id(frame.static_reg_map[addr].values)

        self._static_synced = True
        return True

    def _refresh_overlapped_static_registers(self, frame: Em540Frame, writes: list[tuple[int, list[int]]]) -> None:
        for addr in self._overlapped_static_addrs:
            writes.append((addr + REG_OFFSET, frame.static_reg_map[addr].values))

    async def new_data(self, data: MeterData) -> None:
        """Handle new data from the master.

        We update the Modbus datastore with the new register values as is from the master.
        All writes are collected and flushed directly into the register array under a
        lock — no cross-thread event loop scheduling required.
        """
        frame = data.frame
        writes: list[tuple[int, list[int]]] = []

        # Collect dynamic register writes
        for addr in self._dynamic_addrs:
            writes.append((addr + REG_OFFSET, frame.dynamic_reg_map[addr].values))

        # Collect remapped values — batched into contiguous runs
        for run_start, run_keys in self._remapped_runs:
            batch: list[int] = []
            for key in run_keys:
                batch.extend(frame.remapped_reg_map[key].values)
            writes.append((run_start + REG_OFFSET, batch))

        static_synced_this_cycle = self._sync_static_registers_if_changed(frame, writes)

        # Some static registers (e.g. 0x000B device type) overlap dynamic ranges.
        # Re-apply them after dynamic writes so downstream clients always see static metadata.
        if self._static_synced and not static_synced_this_cycle:
            self._refresh_overlapped_static_registers(frame, writes)

        # Single cross-thread flush for all collected writes
        await self._flush_writes(writes)

        # Keep the circuit open until static registers have been synced at least once.
        if self._static_synced:
            self._pdu_helper.data_received(data.timestamp)
        else:
            self._pdu_helper.upstream_failed()
        self._sync_pdu_stats()

    async def read_failed(self) -> None:
        self._pdu_helper.upstream_failed()
        self._sync_pdu_stats()

    def stop(self) -> None:
        """Stop downstream servers and clean up the dedicated event loop."""
        loop = self._server_loop
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
        self._server_loop = None
