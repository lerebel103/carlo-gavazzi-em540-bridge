import logging
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

REG_OFFSET = 1  # Modbus addresses are 1-based, pymodbus uses 0-based

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

        logger.info("Building Modbus sparse datablock...")
        simdata = _build_simdata(frame)

        self._static_addrs = tuple(frame.static_reg_map.keys())
        self._dynamic_addrs = tuple(frame.dynamic_reg_map.keys())
        self._remapped_addrs = tuple(frame.remapped_reg_map.keys())
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
        await self._rtu_server.serve_forever(background=True)
        await self._tcp_server.serve_forever(background=True)

    def _sync_pdu_stats(self) -> None:
        stale_age = self._pdu_helper.stale_age_seconds()
        self._stats.stale_data_age_ms = 0.0 if stale_age is None else stale_age * 1000.0
        self._stats.circuit_breaker_open = self._pdu_helper.circuit_open
        self._stats.circuit_breaker_open_count = self._pdu_helper.circuit_open_count
        self._stats.dropped_stale_request_count = self._pdu_helper.dropped_request_count
        self._stats.changed()

    async def _set_values(self, address: int, values: list[int]) -> None:
        """Write register values to the shared SimCore context."""
        await self._rtu_server.async_setValues(self._slave_id, _FC_HOLDING_REGISTER, address, values)

    async def _sync_static_registers_if_changed(self, frame: Em540Frame) -> bool:
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
            await self._set_values(addr + REG_OFFSET, frame.static_reg_map[addr].values)
            self._last_static_value_ids[addr] = id(frame.static_reg_map[addr].values)

        self._static_synced = True
        return True

    async def _refresh_overlapped_static_registers(self, frame: Em540Frame) -> None:
        for addr in self._overlapped_static_addrs:
            await self._set_values(addr + REG_OFFSET, frame.static_reg_map[addr].values)

    async def new_data(self, data: MeterData) -> None:
        """Handle new data from the master.

        We update the Modbus datastore with the new register values as is from the master.
        Here we are only just resending the same values read upstream to connected clients without needing to do
        any parsing, since we are bridging EM540 to EM540.
        """
        frame = data.frame

        # Update dynamic registers in the datablock
        for addr in self._dynamic_addrs:
            await self._set_values(addr + REG_OFFSET, frame.dynamic_reg_map[addr].values)

        # Update remapped values
        for addr in self._remapped_addrs:
            await self._set_values(addr + REG_OFFSET, frame.remapped_reg_map[addr].values)

        static_synced_this_cycle = await self._sync_static_registers_if_changed(frame)

        # Some static registers (e.g. 0x000B device type) overlap dynamic ranges.
        # Re-apply them after dynamic writes so downstream clients always see static metadata.
        if self._static_synced and not static_synced_this_cycle:
            await self._refresh_overlapped_static_registers(frame)

        # Keep the circuit open until static registers have been synced at least once.
        # This prevents downstream consumers from receiving partially initialized data.
        if self._static_synced:
            self._pdu_helper.data_received(data.timestamp)
        else:
            self._pdu_helper.upstream_failed()
        self._sync_pdu_stats()

    async def read_failed(self) -> None:
        self._pdu_helper.upstream_failed()
        self._sync_pdu_stats()
