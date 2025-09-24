import logging
import threading

from pymodbus import FramerType
from pymodbus.client import ModbusTcpClient
from pymodbus.datastore import ModbusSparseDataBlock, ModbusDeviceContext, ModbusServerContext
from pymodbus.server import ModbusTcpServer

from Em540Data import Em540Frame
from MeterData import MeterData

class SmartMeterEmulator:
    def __init__(self, host, port, data: Em540Frame):
        self.host = host
        self.port = port
        self._data = data
        self._slave_id = 0x01

        self._logger = logging.getLogger('fronius-meter')
        self._logger.setLevel(logging.DEBUG)

        # Smart Meter TS 65A-3
        datablock = ModbusSparseDataBlock({
            40001: [21365, 28243],
            40003: [1],
            40004: [65],
            40005: [18034,28526,26997,29440,0,0,0,0,0,0,0,0,0,0,0,0],                       # Manufacturer "Fronius
            40021: [21357,24946,29728,19813,29797,29216,21587,8246,13633,11571,0,0,0,0,0,0],   # Device Model "Smart Meter TS 65A-3"
            40037: [15472,29289,28001,29305,15872,0,0,0],                                   # Options N/A
            40045: [12590,14592,0,0,0,0,0,0],                                               # Software Version  N/A
            40053: [48,48,48,48,48,48,48,49,0,0,0,0,0,0,0,1],                               # Serial Number: 000001
            40069: [3],                                                                     # Modbus TCP Address:
            40070: [213],                                                                   # Meter Type
            40071: [124],                                                                   # Modbus Length
            40072: [0, 0],                                                                  # Ac Current Total
            40074: [0, 0],                                                                  # Ac Current Phase A
            40076: [0, 0],                                                                  # Ac Current Phase B
            40078: [0, 0],                                                                  # Ac Current Phase C
            40080: [0, 0],                                                                  # Ac voltage average phase to neutral value
            40082: [0, 0],                                                                  # Ac voltage phase A to neutral value
            40084: [0, 0],                                                                  # Ac voltage phase B to neutral value
            40086: [0, 0],                                                                  # Ac voltage phase C to neutral value
            40088: [0, 0],                                                                  # Ac voltage average phase to phase value
            40090: [0, 0],                                                                  # Ac voltage phase ab value
            40092: [0, 0],                                                                  # Ac voltage phase bc value
            40094: [0, 0],                                                                  # Ac voltage phase ca value
            40096: [0, 0],                                                                  # Ac Frequency
            40098: [0,0],                                                                   # Ac power value
            40100: [0, 0],                                                                  # Ac power phase A value
            40102: [0, 0],                                                                  # Ac power phase B value
            40104: [0, 0],                                                                  # Ac power phase C value
            40106: [0, 0],                                                                  # Ac apparent power value (VA)
            40108: [0, 0],                                                                  # Ac apparent power phase A value (VA)
            40110: [0, 0],                                                                  # Ac apparent power phase B value (VA)
            40112: [0, 0],                                                                  # Ac apparent power phase C value (VA)
            40114: [0, 0],                                                                  # Ac reactive power value (VAr)
            40116: [0, 0],                                                                  # Ac reactive power phase A value (VAr)
            40118: [0, 0],                                                                  # Ac reactive power phase B value (VAr)
            40120: [0, 0],                                                                  # Ac reactive power phase C value (VAr)
            40122: [0, 0],                                                                  # Ac power factor value
            40124: [0, 0],                                                                  # Ac power factor phase A value
            40126: [0, 0],                                                                  # Ac power factor phase B value
            40128: [0, 0],                                                                  # Ac power factor phase C value
            40130: [0, 0],                                                                  # Total Watt Hours exported (Wh)
            40132: [0, 0],                                                                  # Total Watt Hours exported phase A (Wh)
            40134: [0, 0],                                                                  # Total Watt Hours exported phase B (Wh)
            40136: [0, 0],                                                                  # Total Watt Hours exported phase C (Wh)
            40138: [0, 0],                                                                  # Total Watt Hours imported (Wh)
            40140: [0, 0],                                                                  # Total Watt Hours imported phase A (Wh)
            40142: [0, 0],                                                                  # Total Watt Hours imported phase B (Wh)
            40144: [0, 0],                                                                  # Total Watt Hours imported phase C (Wh)
            40146: [0, 0],                                                                  # Total VA Hours exported (VAh)
            40148: [0, 0],                                                                  # Total VA Hours exported phase A (VAh)
            40150: [0, 0],                                                                  # Total VA Hours exported phase B (VAh)
            40152: [0, 0],                                                                  # Total VA Hours exported phase C (VAh)
            40154: [0, 0],                                                                  # Total VA Hours imported (VAh)
            40156: [0, 0],                                                                  # Total VA Hours imported phase A (VAh)
            40158: [0, 0],                                                                  # Total VA Hours imported phase B (VAh)
            40160: [0, 0],                                                                  # Total VA Hours imported phase C (VAh)

            40162: [32704,0,32704,0,32704,0,  #8
                    32704,0,32704,0,32704,0,32704,0,32704,0,  #9
                    32704,0,32704,0,32704,0,32704,0,32704,0,  #10
                    32704,0,32704,0,32704,0,32704,0,32704,0,  #11
                    32704,0,32704,0,32704,0,32704,0,32704,0,  #12
                    32704,0,32704,0],
            40194: [0, 0],                                                                   #Event
            40196: [65535, 0],                                                               #End Block
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
                                       address=(self.host, self.port))


    async def start(self):
        await self._server.serve_forever(background=True)


    def stop(self):
        pass

    async def data_failed(self):
        pass

    async def update(self, data: MeterData):
        address = 40072
        registers = list()

        # Current
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.system.An, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[0].current, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[1].current, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[2].current, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        
        # Voltage - L-N
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.system.line_neutral_voltage, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[0].line_neutral_voltage, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[1].line_neutral_voltage, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[2].line_neutral_voltage, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )

        # Voltage - L-L
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.system.line_line_voltage, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[0].line_line_voltage, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[1].line_line_voltage, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[2].line_line_voltage, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        
        # Freguency
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.system.frequency, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )

        # Power
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.system.power, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[0].power, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[1].power, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[2].power, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )

        # Apparent apparent
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.system.apparent_power, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[0].apparent_power, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[1].apparent_power, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[2].apparent_power, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )

        # Reactive reactive
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.system.reactive_power, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[0].reactive_power, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[1].reactive_power, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[2].reactive_power, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )

        # Power factor
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.system.power_factor, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[0].power_factor, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[1].power_factor, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.phases[2].power_factor, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )

        # Total Watt Hours exported (Wh)
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.other_energies.kwh_neg_total, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        # Total Watt Hours exported phase A (Wh)
        registers.extend(
            ModbusTcpClient.convert_to_registers(0, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        # Total Watt Hours exported phase B (Wh)
        registers.extend(
            ModbusTcpClient.convert_to_registers(0, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        # Total Watt Hours exported phase C (Wh)
        registers.extend(
            ModbusTcpClient.convert_to_registers(0, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )

        # Total Watt Hours imported phase A (Wh)
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.other_energies.kwh_plus_total, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        # Total Watt Hours imported phase A (Wh)
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.other_energies.kwh_plus_l1, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        # Total Watt Hours imported phase B (Wh)
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.other_energies.kwh_plus_l2, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )
        # Total Watt Hours imported phase C (Wh)
        registers.extend(
            ModbusTcpClient.convert_to_registers(data.other_energies.kwh_plus_l3, ModbusTcpClient.DATATYPE.FLOAT32, "big")
        )


        self.datablock.setValues(address, registers)

        # self._logger.debug("Updating SmartMeter data:", data)
        # # We are writing FC 3 from Address 40071
        # register = 3
        # slave_id = 0x01
        # address = 0x9C87
        #
        # builder = BinaryPayloadBuilder(byteorder=Endian.big, wordorder=Endian.big)
        #
        # builder.add_32bit_float(data['sum_of_current'])            # Ac Current Total
        # builder.add_32bit_float(data['current_a'])                 # Ac Current Phase A
        # builder.add_32bit_float(data['current_b'])                 # Ac Current Phase B
        # builder.add_32bit_float(data['current_c'])                 # Ac Current Phase C
        # builder.add_32bit_float(data['voltage_mean'])              # Ac voltage average phase to neutral value
        # builder.add_32bit_float(data['voltage_a'])                 # Ac voltage phase A to neutral value
        # builder.add_32bit_float(data['voltage_b'])                 # Ac voltage phase B to neutral value
        # builder.add_32bit_float(data['voltage_c'])                 # Ac voltage phase C to neutral value
        # builder.add_32bit_float(data['voltage_phase_mean'])        # Ac voltage average phase to phase value
        # builder.add_32bit_float(data['voltage_phase_ab'])          # Ac voltage phase ab value
        # builder.add_32bit_float(data['voltage_phase_bc'])          # Ac voltage phase bc value
        # builder.add_32bit_float(data['voltage_phase_ca'])          # Ac voltage phase ca value
        # builder.add_32bit_float(data['frequency'])                 # Ac Frequency
        # builder.add_32bit_float(data['sum_of_power'])              # Ac power value
        # builder.add_32bit_float(data['active_power_a'])            # Ac power phase A value
        # builder.add_32bit_float(data['active_power_b'])            # Ac power phase B value
        # builder.add_32bit_float(data['active_power_c'])            # Ac power phase C value
        # builder.add_32bit_float(data['apparent_power'])            # Ac apparent power value (VA)
        # builder.add_32bit_float(data['apparent_power_a'])          # Ac apparent power phase A value (VA)
        # builder.add_32bit_float(data['apparent_power_b'])          # Ac apparent power phase B value (VA)
        # builder.add_32bit_float(data['apparent_power_c'])          # Ac apparent power phase C value (VA)
        # builder.add_32bit_float(data['sum_of_reactive_power'])     # Ac reactive power value (VAr)
        # builder.add_32bit_float(data['reactive_power_a'])          # Ac reactive power phase A value (VAr)
        # builder.add_32bit_float(data['reactive_power_b'])          # Ac reactive power phase B value (VAr)
        # builder.add_32bit_float(data['reactive_power_c'])          # Ac reactive power phase C value (VAr)
        # builder.add_32bit_float(data['power_factor'])              # Ac power factor value
        # builder.add_32bit_float(data['power_factor_a'])            # Ac power factor phase A value
        # builder.add_32bit_float(data['power_factor_b'])            # Ac power factor phase B value
        # builder.add_32bit_float(data['power_factor_c'])            # Ac power factor phase C value
        # builder.add_32bit_float(data['sum_of_export'])             # Total Watt Hours exported (Wh)
        # builder.add_32bit_float(data['export_energy_a'])           # Total Watt Hours exported phase A (Wh)
        # builder.add_32bit_float(data['export_energy_b'])           # Total Watt Hours exported phase B (Wh)
        # builder.add_32bit_float(data['export_energy_c'])           # Total Watt Hours exported phase C (Wh)
        # builder.add_32bit_float(data['sum_of_import'])             # Total Watt Hours imported (Wh)
        # builder.add_32bit_float(data['import_energy_a'])           # Total Watt Hours imported phase A (Wh)
        # builder.add_32bit_float(data['import_energy_b'])           # Total Watt Hours imported phase B (Wh)
        # builder.add_32bit_float(data['import_energy_c'])           # Total Watt Hours imported phase C (Wh)
        # builder.add_32bit_float(0)                                 # Total VA Hours exported (VAh)
        # builder.add_32bit_float(0)                                 # Total VA Hours exported phase A (VAh)
        # builder.add_32bit_float(0)                                 # Total VA Hours exported phase B (VAh)
        # builder.add_32bit_float(0)                                 # Total VA Hours exported phase C (VAh)
        # builder.add_32bit_float(0)                                 # Total VA Hours imported (VAh)
        # builder.add_32bit_float(0)                                 # Total VA Hours imported phase A (VAh)
        # builder.add_32bit_float(0)                                 # Total VA Hours imported phase B (VAh)
        # builder.add_32bit_float(0)                                 # Total VA Hours imported phase C (VAh)
        #
        #
        # values = builder.to_registers()
        # self.context[slave_id].setValues(register, address, values)
        #
        #
