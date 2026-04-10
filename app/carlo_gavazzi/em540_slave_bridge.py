import logging
from typing import Callable

from pymodbus import FramerType
from pymodbus.datastore import ModbusDeviceContext, ModbusServerContext, ModbusSparseDataBlock
from pymodbus.server import ModbusTcpServer

from app.carlo_gavazzi.em540_data import Em540Frame
from app.carlo_gavazzi.em540_master import MeterDataListener
from app.carlo_gavazzi.em540_slave_stats import EM540SlaveStats
from app.carlo_gavazzi.meter_data import MeterData
from app.utils.pdu_helper import PduHelper

REG_OFFSET = 1  # Modbus addresses are 1-based, pymodbus uses 0-based

logger = logging.getLogger("em540-slave")


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

        # Build a sparse datablock with the size of the frame registers
        values: dict[int, list[int]] = {}
        logger.info("Building Modbus sparse datablock...")

        for addr in frame.static_reg_map:
            logger.debug("Adding static reg " + hex(addr))
            values[addr + REG_OFFSET] = frame.static_reg_map[addr].values

        for addr in frame.dynamic_reg_map:
            logger.debug("Adding dynamic reg " + hex(addr))
            values[addr + REG_OFFSET] = frame.dynamic_reg_map[addr].values

        for addr in frame.remapped_reg_map:
            logger.debug("Adding remapped reg " + hex(addr))
            values[addr + REG_OFFSET] = frame.remapped_reg_map[addr].values

        self._static_addrs = tuple(frame.static_reg_map.keys())
        self._dynamic_addrs = tuple(frame.dynamic_reg_map.keys())
        self._remapped_addrs = tuple(frame.remapped_reg_map.keys())
        self._last_static_value_ids: dict[int, int] = {
            addr: id(frame.static_reg_map[addr].values) for addr in self._static_addrs
        }
        self._static_synced: bool = any(
            any(value != 0 for value in frame.static_reg_map[addr].values) for addr in self._static_addrs
        )

        self.datablock: ModbusSparseDataBlock = ModbusSparseDataBlock.create(values)

        self._context: ModbusDeviceContext = ModbusDeviceContext(
            di=self.datablock,
            co=self.datablock,
            hr=self.datablock,
            ir=self.datablock,
        )
        context: ModbusServerContext = ModbusServerContext(devices={self._slave_id: self._context}, single=False)

        # Modbus RTU over socket server
        self._rtu_server: ModbusTcpServer = ModbusTcpServer(
            framer=FramerType.RTU,
            context=context,
            address=(self.host, self.rtu_port),
            trace_pdu=self._pdu_helper.on_pdu,
            trace_connect=self._rtu_trace_connect,
        )

        # Modbus TCP server
        self._tcp_server: ModbusTcpServer = ModbusTcpServer(
            framer=FramerType.SOCKET,
            context=context,
            address=(self.host, self.tcp_port),
            trace_pdu=self._pdu_helper.on_pdu,
            trace_connect=self._tcp_trace_connect,
        )

    def _rtu_trace_connect(self, connect: bool) -> None:
        logger.info(f"Client connection to RTU server: {connect}")
        if connect:
            self._stats.rtu_client_count += 1
        else:
            self._stats.rtu_client_count -= 1
            self._stats.rtu_client_disconnect_count += 1
        self._stats.changed()

    def _tcp_trace_connect(self, connect: bool) -> None:
        logger.info(f"Client connection to TCP server: {connect}")
        if connect:
            self._stats.tcp_client_count += 1
        else:
            self._stats.tcp_client_count -= 1
            self._stats.tcp_client_disconnect_count += 1
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

    def _sync_static_registers_if_changed(self, frame: Em540Frame) -> bool:
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
            self.datablock.setValues(addr + REG_OFFSET, frame.static_reg_map[addr].values)
            self._last_static_value_ids[addr] = id(frame.static_reg_map[addr].values)

        self._static_synced = True
        return True

    async def new_data(self, data: MeterData) -> None:
        """Handle new data from the master.

        We update the Modbus datastore with the new register values as is from the master.
        Here we are only just resending the same values read upstream to connected clients without needing to do
        any parsing, since we are bridging EM540 to EM540.
        """
        frame = data.frame

        self._sync_static_registers_if_changed(frame)

        # Update dynamic registers in the datablock
        for addr in self._dynamic_addrs:
            self.datablock.setValues(addr + REG_OFFSET, frame.dynamic_reg_map[addr].values)

        # Update remapped values
        for addr in self._remapped_addrs:
            self.datablock.setValues(addr + REG_OFFSET, frame.remapped_reg_map[addr].values)

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
