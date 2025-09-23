from pymodbus import FramerType
from pymodbus.datastore import (
    ModbusDeviceContext,
    ModbusSequentialDataBlock,
    ModbusServerContext,
    ModbusSparseDataBlock,
)
from pymodbus.pdu import ModbusPDU
from pymodbus.server import ModbusTcpServer

from em540_master import MeterData


class Em540Slave:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.start_address = 0x00F6

        # Create instance of a Modbus server

        # identity
        from pymodbus import ModbusDeviceIdentification
        identity = ModbusDeviceIdentification(
            info_name={
                "VendorName": "Pymodbus",
                "ProductCode": "PM",
                "VendorUrl": "https://github.com/pymodbus-dev/pymodbus/",
                "ProductName": "Pymodbus Server",
                "ModelName": "Pymodbus Server",
                "MajorMinorRevision": 1.0,
            }
        )

        total_number_of_registers = 108
        values = [0] * total_number_of_registers

        self.datablock = ModbusSparseDataBlock({
            0x000B+1: [1760],  # Device Type
            0x0302+1: [5123],  # Firmware Version
            0x1101+1: [1],  # Application
            0x1103+1: [2],  # Measurement mode
            0x5000+1: [19288, 12592, 12592, 12594, 14128, 12337, 16640, 2023],  # Serial Data
            0x00F6+1: values
        })


        #self.datablock = ModbusSequentialDataBlock(self.start_address, values)

        self._context = ModbusDeviceContext(
            di=self.datablock,
            co=self.datablock,
            hr=self.datablock,
            ir=self.datablock,
        )
        context = ModbusServerContext(devices=self._context, single=True)
        self._server = ModbusTcpServer(identity=identity,
                                       framer=FramerType.RTU,
                                       context=context,
                                       address=(self.host, self.port),
                                       trace_pdu=self.on_pdu)


    async def start(self):
        await self._server.serve_forever(background=True)

    def on_pdu(self, flag: bool, pdu: ModbusPDU) -> ModbusPDU:
        print("got pdu " + str(pdu))
        return pdu


    def set_data(self, data: MeterData):
        if data is not None:
            self.datablock.setValues(0x000B+1, data.device_type)
            self.datablock.setValues(0x0302+1, data.fw_ver)
            self.datablock.setValues(0x1103+1, data.measurement_mode)
            self.datablock.setValues(0x5000+1, data.serial_data)

            self.datablock.setValues(self.start_address+1, data._registers)
            print("device_type " + str(data.device_type) + ", fw_ver " + str(data.fw_ver) + ", measurement_mode " + str(data.measurement_mode))
