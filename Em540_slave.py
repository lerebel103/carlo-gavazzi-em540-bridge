import logging

from pymodbus import FramerType
from pymodbus.datastore import (
    ModbusDeviceContext,
    ModbusSequentialDataBlock,
    ModbusServerContext,
    ModbusSparseDataBlock,
)
from pymodbus.pdu import ModbusPDU
from pymodbus.server import ModbusTcpServer

from Em540Data import Em540Frame

REG_OFFSET = 1  # Modbus addresses are 1-based, pymodbus uses 0-based

logger = logging.getLogger()

class Em540Slave:
    def __init__(self, host, port, data: Em540Frame):
        self.host = host
        self.port = port
        self.data = data


        # Build a sparse datablock with fixed registers and dynamic registers
        values = {}
        logger.info('Building Modbus sparse datablock...')
        for addr in data.static_reg_map:
            logger.info("Adding static reg " + hex(addr))
            values[addr + REG_OFFSET] = data.static_reg_map[addr].values

        for addr in data.dynamic_reg_map:
            logger.info("Adding dynamic reg " + hex(addr))
            values[addr + REG_OFFSET] = data.dynamic_reg_map[addr].values

        for addr in data.remapped_reg_map:
            logger.info("Adding remapped reg " + hex(addr))
            values[addr + REG_OFFSET] = data.remapped_reg_map[addr].values


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
                                       trace_pdu=self.on_pdu)



    def on_pdu(self, flag: bool, pdu: ModbusPDU) -> ModbusPDU:
        logger.debug("got pdu " + str(pdu))
        return pdu

    async def start(self):
       await self._server.serve_forever(background=True)

    async def data_ready(self):
        # Update dynamic registers in the datablock
        for addr in self.data.dynamic_reg_map:
            self.datablock.setValues(addr + REG_OFFSET, self.data.dynamic_reg_map[addr].values)

        # Update static registers in the datablock (in case they changed)
        for addr in self.data.static_reg_map:
            self.datablock.setValues(addr + REG_OFFSET, self.data.static_reg_map[addr].values)

        # Update remapped values
        for addr in self.data.remapped_reg_map:
            self.datablock.setValues(addr + REG_OFFSET, self.data.remapped_reg_map[addr].values)


    async def data_failed(self):
        pass

        # Deal with this later
