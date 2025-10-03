import logging
from typing import Callable

from pymodbus import FramerType
from pymodbus.datastore import (ModbusDeviceContext, ModbusServerContext,
                                ModbusSparseDataBlock)
from pymodbus.server import ModbusTcpServer

from carlo_gavazzi.em540_data import Em540Frame
from carlo_gavazzi.em540_master import MeterDataListener
from carlo_gavazzi.em540_slave_stats import EM540SlaveStats
from carlo_gavazzi.meter_data import MeterData
from utils.pdu_helper import PduHelper

REG_OFFSET = 1  # Modbus addresses are 1-based, pymodbus uses 0-based

logger = logging.getLogger("em540-slave")


class Em540Slave(MeterDataListener):
    """Represents a Modbus slave that serves data read from an EM540 master."""

    def __init__(self, config, frame: Em540Frame) -> None:
        self.host: str = config.host
        self.rtu_port: int = config.rtu_port
        self.tcp_port: int = config.tcp_port
        self.last_pdu: object = None
        self._slave_id: int = config.slave_id
        self._pdu_helper: PduHelper = PduHelper(logger, config.update_timeout)
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

        self.datablock: ModbusSparseDataBlock = ModbusSparseDataBlock.create(values)

        self._context: ModbusDeviceContext = ModbusDeviceContext(
            di=self.datablock,
            co=self.datablock,
            hr=self.datablock,
            ir=self.datablock,
        )
        context: ModbusServerContext = ModbusServerContext(
            devices={self._slave_id: self._context}, single=False
        )

        # Modbus RTU over socket server
        self._rtu_server: ModbusTcpServer = ModbusTcpServer(
            framer=FramerType.RTU,
            context=context,
            address=(self.host, self.rtu_port),
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

    async def new_data(self, data: MeterData) -> None:
        """Handle new data from the master.

        We update the Modbus datastore with the new register values as is from the master.
        Here we are only just resending the same values read upstream to connected clients without needing to do
        any parsing, since we are bridging EM540 to EM540.
        """
        frame = data.frame

        # Update dynamic registers in the datablock
        for addr in frame.dynamic_reg_map:
            self.datablock.setValues(
                addr + REG_OFFSET, frame.dynamic_reg_map[addr].values
            )

        # Update static registers in the datablock (in case they changed)
        for addr in frame.static_reg_map:
            self.datablock.setValues(
                addr + REG_OFFSET, frame.static_reg_map[addr].values
            )

        # Update remapped values
        for addr in frame.remapped_reg_map:
            self.datablock.setValues(
                addr + REG_OFFSET, frame.remapped_reg_map[addr].values
            )

        # Now update our PDU helper with the timestamp of this data
        self._pdu_helper.data_received(data.timestamp)

    async def read_failed(self) -> None:
        pass
