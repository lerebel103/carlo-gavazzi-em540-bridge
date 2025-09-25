import logging

from pymodbus import FramerType
from pymodbus.datastore import (
    ModbusDeviceContext,
    ModbusServerContext,
    ModbusSparseDataBlock,
)
from pymodbus.server import ModbusTcpServer

from em_540_data import Em540Frame
from em540_master import MeterDataListener
from meter_data import MeterData
from pdu_helper import PduHelper

REG_OFFSET = 1  # Modbus addresses are 1-based, pymodbus uses 0-based

logger = logging.getLogger('Em540Slave')


class Em540Slave(MeterDataListener):
    def __init__(self, config, frame: Em540Frame):
        self.host = config.host
        self.port = config.port
        self.last_pdu = None
        self._slave_id: int = config.slave_id
        self._pdu_helper = PduHelper(logger, config.update_timeout)
        logger.setLevel(config.logging)

        # Build a sparse datablock with the size of the frame registers
        values = {}
        logger.info('Building Modbus sparse datablock...')

        for addr in frame.static_reg_map:
            logger.info("Adding static reg " + hex(addr))
            values[addr + REG_OFFSET] = frame.static_reg_map[addr].values

        for addr in frame.dynamic_reg_map:
            logger.info("Adding dynamic reg " + hex(addr))
            values[addr + REG_OFFSET] = frame.dynamic_reg_map[addr].values

        for addr in frame.remapped_reg_map:
            logger.info("Adding remapped reg " + hex(addr))
            values[addr + REG_OFFSET] = frame.remapped_reg_map[addr].values

        self.datablock = ModbusSparseDataBlock.create(values)

        self._context = ModbusDeviceContext(
            di=self.datablock,
            co=self.datablock,
            hr=self.datablock,
            ir=self.datablock,
        )
        context = ModbusServerContext(devices=self._context, single=True)
        self._server = ModbusTcpServer(framer=FramerType.RTU,
                                       context=context,
                                       address=(self.host, self.port),
                                       trace_pdu=self._pdu_helper.on_pdu)


    async def start(self):
        await self._server.serve_forever(background=True)

    async def new_data(self, data: MeterData):
        """Handle new data from the master.

        We update the Modbus datastore with the new register values as is from the master.
        Here we are only just resending the same values read upstream to connected clients without needing to do
        any parsing, since we are bridging EM540 to EM540.
        """
        self._pdu_helper.data_received(data.timestamp)
        frame = data.frame

        # Update dynamic registers in the datablock
        for addr in frame.dynamic_reg_map:
            self.datablock.setValues(addr + REG_OFFSET, frame.dynamic_reg_map[addr].values)

        # Update static registers in the datablock (in case they changed)
        for addr in frame.static_reg_map:
            self.datablock.setValues(addr + REG_OFFSET, frame.static_reg_map[addr].values)

        # Update remapped values
        for addr in frame.remapped_reg_map:
            self.datablock.setValues(addr + REG_OFFSET, frame.remapped_reg_map[addr].values)

    async def read_failed(self):
        pass
