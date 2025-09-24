import logging
import threading

from pymodbus import FramerType
from pymodbus.client import ModbusTcpClient
from pymodbus.datastore import ModbusSparseDataBlock, ModbusDeviceContext, ModbusServerContext
from pymodbus.pdu import ModbusPDU
from pymodbus.server import ModbusTcpServer

import MeterData
from Em540_master import MeterDataListener
from PduHelper import PduHelper

logger = logging.getLogger('TS65A')


def _append_registers(registers, values):
    """ Append successive float32 values as big-endian registers to the register list. """
    for value in values:
        registers.extend(
            ModbusTcpClient.convert_to_registers(value, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )


class Ts65aSlaveBridge(MeterDataListener):
    def __init__(self, host, port, bridge_timeout: float):
        self.host = host
        self.port: int = port
        self._slave_id: int = 0x01
        self.bridge_timeout: float = bridge_timeout
        self._pdu_helper = PduHelper(bridge_timeout)

        # Smart Meter TS 65A-3
        datablock = ModbusSparseDataBlock({
            40001: [21365, 28243],
            40003: [1],
            40004: [65],
            40005: [18034, 28526, 26997, 29440, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],  # Manufacturer "Fronius"
            40021: [21357, 24946, 29728, 19813, 29797, 29216, 21587, 8246, 13633, 11571, 0, 0, 0, 0, 0, 0],
            # Device Model "Smart Meter TS 65A-3"
            40037: [15472, 29289, 28001, 29305, 15872, 0, 0, 0],  # Options N/A
            40045: [12590, 14592, 0, 0, 0, 0, 0, 0],  # Software Version  N/A
            40053: [48, 48, 48, 48, 48, 48, 48, 49, 0, 0, 0, 0, 0, 0, 0, 1],  # Serial Number: 000001
            40069: [3],  # Modbus TCP Address:
            40070: [213],  # Meter Type
            40071: [124],  # Modbus Length
            40072: [0, 0],  # Ac Current Total
            40074: [0, 0],  # Ac Current Phase A
            40076: [0, 0],  # Ac Current Phase B
            40078: [0, 0],  # Ac Current Phase C
            40080: [0, 0],  # Ac voltage average phase to neutral value
            40082: [0, 0],  # Ac voltage phase A to neutral value
            40084: [0, 0],  # Ac voltage phase B to neutral value
            40086: [0, 0],  # Ac voltage phase C to neutral value
            40088: [0, 0],  # Ac voltage average phase to phase value
            40090: [0, 0],  # Ac voltage phase ab value
            40092: [0, 0],  # Ac voltage phase bc value
            40094: [0, 0],  # Ac voltage phase ca value
            40096: [0, 0],  # Ac Frequency
            40098: [0, 0],  # Ac power value
            40100: [0, 0],  # Ac power phase A value
            40102: [0, 0],  # Ac power phase B value
            40104: [0, 0],  # Ac power phase C value
            40106: [0, 0],  # Ac apparent power value (VA)
            40108: [0, 0],  # Ac apparent power phase A value (VA)
            40110: [0, 0],  # Ac apparent power phase B value (VA)
            40112: [0, 0],  # Ac apparent power phase C value (VA)
            40114: [0, 0],  # Ac reactive power value (VAr)
            40116: [0, 0],  # Ac reactive power phase A value (VAr)
            40118: [0, 0],  # Ac reactive power phase B value (VAr)
            40120: [0, 0],  # Ac reactive power phase C value (VAr)
            40122: [0, 0],  # Ac power factor value
            40124: [0, 0],  # Ac power factor phase A value
            40126: [0, 0],  # Ac power factor phase B value
            40128: [0, 0],  # Ac power factor phase C value
            40130: [0, 0],  # Total Watt Hours exported (Wh)
            40132: [0, 0],  # Total Watt Hours exported phase A (Wh)
            40134: [0, 0],  # Total Watt Hours exported phase B (Wh)
            40136: [0, 0],  # Total Watt Hours exported phase C (Wh)
            40138: [0, 0],  # Total Watt Hours imported (Wh)
            40140: [0, 0],  # Total Watt Hours imported phase A (Wh)
            40142: [0, 0],  # Total Watt Hours imported phase B (Wh)
            40144: [0, 0],  # Total Watt Hours imported phase C (Wh)
            40146: [0, 0],  # Total VA Hours exported (VAh)
            40148: [0, 0],  # Total VA Hours exported phase A (VAh)
            40150: [0, 0],  # Total VA Hours exported phase B (VAh)
            40152: [0, 0],  # Total VA Hours exported phase C (VAh)
            40154: [0, 0],  # Total VA Hours imported (VAh)
            40156: [0, 0],  # Total VA Hours imported phase A (VAh)
            40158: [0, 0],  # Total VA Hours imported phase B (VAh)
            40160: [0, 0],  # Total VA Hours imported phase C (VAh)

            40162: [32704, 0, 32704, 0, 32704, 0,  # 8
                    32704, 0, 32704, 0, 32704, 0, 32704, 0, 32704, 0,  # 9
                    32704, 0, 32704, 0, 32704, 0, 32704, 0, 32704, 0,  # 10
                    32704, 0, 32704, 0, 32704, 0, 32704, 0, 32704, 0,  # 11
                    32704, 0, 32704, 0, 32704, 0, 32704, 0, 32704, 0,  # 12
                    32704, 0, 32704, 0],
            40194: [0, 0],  # Event
            40196: [65535, 0],  # End Block
        })

        self.datablock = datablock

        slave_store = ModbusDeviceContext(
            di=datablock,
            co=datablock,
            hr=datablock,
            ir=datablock,
        )

        self.context = ModbusServerContext(devices=slave_store, single=True)
        self._server = ModbusTcpServer(framer=FramerType.SOCKET,
                                       context=self.context,
                                       address=(self.host, self.port),
                                       trace_pdu=self._pdu_helper.on_pdu)

    async def start(self):
        await self._server.serve_forever(background=True)

    def stop(self):
        pass

    async def new_data(self, data: MeterData):
        self._pdu_helper.data_received(data.timestamp)

        address = 40072
        registers = list()

        # Current
        _append_registers(registers, [
            data.system.An,
            data.phases[0].current,
            data.phases[1].current,
            data.phases[2].current,
        ])

        # Voltage - L-N
        _append_registers(registers, [
            data.system.line_neutral_voltage,
            data.phases[0].line_neutral_voltage,
            data.phases[1].line_neutral_voltage,
            data.phases[2].line_neutral_voltage,
        ])

        # Voltage - L-L
        _append_registers(registers, [
            data.system.line_line_voltage,
            data.phases[0].line_line_voltage,
            data.phases[1].line_line_voltage,
            data.phases[2].line_line_voltage,
        ])

        # Freguency
        _append_registers(registers, [
            data.system.frequency,
        ])

        # Power
        _append_registers(registers, [
            data.system.power,
            data.phases[0].power,
            data.phases[1].power,
            data.phases[2].power,
        ])

        # Apparent apparent
        _append_registers(registers, [
            data.system.apparent_power,
            data.phases[0].apparent_power,
            data.phases[1].apparent_power,
            data.phases[2].apparent_power,
        ])

        # Reactive power
        _append_registers(registers, [
            data.system.reactive_power,
            data.phases[0].reactive_power,
            data.phases[1].reactive_power,
            data.phases[2].reactive_power,
        ])

        # Power factor
        _append_registers(registers, [
            data.system.power_factor,
            data.phases[0].power_factor,
            data.phases[1].power_factor,
            data.phases[2].power_factor,
        ])

        # Total Watt Hours exported (Wh)
        _append_registers(registers, [
            data.other_energies.kwh_neg_total,
            0,  # phase A
            0,  # phase B
            0,  # phase C
        ])

        # Total Watt Hours imported phase A (Wh)
        _append_registers(registers, [
            data.other_energies.kwh_plus_total,
            data.other_energies.kwh_plus_l1,
            data.other_energies.kwh_plus_l2,
            data.other_energies.kwh_plus_l3,
        ])

        self.datablock.setValues(address, registers)

    async def read_failed(self):
        pass
