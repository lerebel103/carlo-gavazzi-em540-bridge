import logging
import sys

from pymodbus import pymodbus_apply_logging_config, FramerType
from pymodbus.client import AsyncModbusTcpClient, ModbusTcpClient
from pymodbus import ModbusException

logger = logging.getLogger()

class SystemData:
    def __init__(self):
        self.run_hour_meter_neg: float = 0.0
        self.An: float = 0.0
        self.run_hour_meter: float = 0.0
        self.line_neutral_voltage: float = 0.0
        self.line_line_voltage: float = 0.0
        self.power: float = 0.0
        self.apparent_power: float = 0.0
        self.reactive_power: float = 0.0
        self.power_factor: float = 0.0
        self.phase_sequence: int = 0
        self.frequency: float = 0.0
        
    def parse(self, client, registers):
        # 300247	00F6h	2	Run hour meter kWh (-)	INT32	Value weight: hours*100
        # 300249	00F8h	2	An	INT32	Value weight: Ampere*1000
        # 300251	OOFAh	2	n.a.	INT32	Not available, value =0
        # 300253	0OFCh	2	n.a.	INT32	Not available, value =0
        # 300255	OOFEh	2	Run hour meter	INT32	Value weight: hours*100
        # 300257	0100h	2	n.a.	INT32	Not available, value =0
        # 300259	0102h	2	V L-N sys	INT32	Value weight: Volt*10
        # 300261	0104h		V L-L sys	INT32	Value weight: Volt*10
        # 300263	0106h		W sys	INT32	Value weight: Watt*10
        # 300265	0108h		VA sys	INT32	Value weight: VA*10
        # 300267	010Ah	2	var sys	INT32	Value weight: var*10
        # 300269	010Ch	2	PF sys	INT32	(See 31h register note). Value weight: PF*1000
        # 300271	010Eh	2	Phase sequence	INT32	The value -1 corResponses to L1-L3-L2 sequence, the value 1 corResponses to L1-L2-L3 sequence. The phase sequence value is meaningful only in a 3-phase system
        # 300273	0110h	2	Hz	INT32	Value weight: Hz*10
        parsed = client.convert_from_registers(registers, ModbusTcpClient.DATATYPE.INT32, word_order="little")

        self.run_hour_meter_neg = parsed[0] / 100
        self.An = parsed[1] / 1000
        self.run_hour_meter = parsed[4] / 100
        self.line_neutral_voltage = parsed[6] / 10
        self.line_line_voltage = parsed[7] / 10
        self.power = parsed[8] / 10
        self.apparent_power = parsed[9] / 10
        self.reactive_power = parsed[10] / 10
        self.power_factor = parsed[11] / 1000
        self.phase_sequence = parsed[12]
        self.frequency = parsed[13] / 10


class PhaseData:

    def __init__(self):
        self.line_to_line_voltage: float = 0.0
        self.line_to_neutral_voltage: float = 0.0
        self.current: float = 0.0
        self.power: float = 0.0
        self.apparent_power: float = 0.0
        self.reactive_power: float = 0.0
        self.power_factor: float = 0.0
        
    def parse(self, client, registers):        # Each block of 7 registers corresponds to a phase:
        # V Line-Line+1 INT32 Value weight: Volt*10
        # V Line-N INT32 Value weight: Volt*10
        # A Line INT32 Value weight: Ampere*1000
        # W Line INT32 Value weight: Watt*10
        # VA Line INT32 Value weight: VA*10
        # var Line INT32 Value weight: var*10
        # PF Line 0130h INT32 (See 2Eh register note). Value weight: PF*1000

        parsed = client.convert_from_registers(registers, ModbusTcpClient.DATATYPE.INT32, word_order="little")
        self.line_to_line_voltage = parsed[0] / 10.0
        self.line_to_neutral_voltage = parsed[1] / 10.0
        self.current = parsed[2] / 1000.0
        self.power = parsed[3] / 10.0
        self.apparent_power = parsed[4] / 10.0
        self.reactive_power = parsed[5] / 10.0
        self.power_factor = parsed[6] / 1000.0


class OtherEnergies:
    def __init__(self):
        self.kwh_plus_partial: float = 0.0
        self.kvarh_plus_partial: float = 0.0
        self.kwh_plus_l1: float = 0.0
        self.kwh_plus_l2: float = 0.0
        self.kwh_plus_l3: float = 0.0
        self.kwh_plus_t1: float = 0.0
        self.kwh_plus_t2: float = 0.0
        self.kwh_minus_partial: float = 0.0
        self.kvarh_minus_partial: float = 0.0
        self.kvah_total: float = 0.0
        self.kvah_partial: float = 0.0

    def parse(self, client, registers):
        # 300329	0148h	2	KWH (+) PARTIAL	INT32	Value weight: kWh*10
        # 300331	014Ah	2	Kvarh (+) PARTIAL	INT32	Value weight: kvarh*10
        # 300333	014Ch	2	kWh (+) L1	INT32	Value weight: kWh*10
        # 300335	014Eh	2	kWh (+) L2	INT32	Value weight: kWh*10
        # 300337	0150h	2	kWh (+) L3	INT32	Value weight: kWh*10
        # 300339	0152h	2	kWh (+) t1	INT32	Value weight: kWh*10
        # 300341	0154h	2	kWh (+) t2	INT32	Value weight: KWh*10
        # 300343	0156h	2	n.a.	INT32	Not available, value =0
        # 300345	0158h	2	n.a.	INT32	Not available, value =0
        # 300347	015Ah	2	kWh (-) PARTIAL	INT32	Value weight: kWh*10
        # 300349	015Ch	2	Kvarh (- PARTIAL	INT32	Value weight: kvarh*10
        # 300351	015Eh	2	kVAh TOT	INT32	Value weight: kVAh*10
        # 300353	0160h	2	KVAh PARTIAL	INT32	Value weight: kVAh*10
        parsed = client.convert_from_registers(registers, ModbusTcpClient.DATATYPE.INT32, word_order="little")
        self.kwh_plus_partial = parsed[0] / 10
        self.kvarh_plus_partial = parsed[1] / 10
        self.kwh_plus_l1 = parsed[2] / 10
        self.kwh_plus_l2 = parsed[3] / 10
        self.kwh_plus_l3 = parsed[4] / 10
        self.kwh_plus_t1 = parsed[5] / 10
        self.kwh_plus_t2 = parsed[6] / 10
        # parsed[7] and parsed[8] are not available, skip
        self.kwh_minus_partial = parsed[9] / 10
        self.kvarh_minus_partial = parsed[10] / 10
        self.kvah_total = parsed[11] / 10
        self.kvah_partial = parsed[12] / 10

class MeterData:
    def __init__(self):
        self.device_type: int = 0
        self.fw_ver: int = 0
        self.measurement_mode: int = 0
        self.application: int = 0
        self.serial_data = None

        self._registers = None
        self.system = SystemData()
        self.phases = [PhaseData(), PhaseData(), PhaseData()]
        self.other_energies = OtherEnergies()
        
    def parse(self, client, start_register, registers):
        self._registers = registers

        # Parse system data
        system_num_registers = 14 * 2
        system_offset = 0x00F6 - start_register
        self.system.parse(client, registers[system_offset:system_offset + system_num_registers])

        # Parse three phases consecutively, starting at 0x011E
        phase_num_registers = 7 * 2
        phase_offset = 0x011E - start_register
        for idx in range(3):
            start_idx = phase_offset + idx * phase_num_registers
            end_idx = start_idx + phase_num_registers
            self.phases[idx].parse(client, registers[start_idx:end_idx])

        # Parse other energies data
        other_energies_num_registers = 13 * 2
        other_energies_offset = 0x0148 - start_register
        self.other_energies.parse(client, registers[other_energies_offset:other_energies_offset + other_energies_num_registers])

        #logger.fine("Total Power: %.1f W, L1: %.1f W, L2: %.1f W, L3: %.1f W",
        #            self.system.power,
        #            self.phases[0].power,
        #            self.phases[1].power,
        #            self.phases[2].power)


class Em540Master:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.data = MeterData()

        # Create Modbus client
        self._client = AsyncModbusTcpClient(host=self.host, port=self.port, framer=FramerType.RTU, timeout=0.15, retries=1)


    async def connect(self):
        # Simulate connecting to the EM540 device
        logger.info(f"Connecting to EM540 at {self.host}:{self.port}...")

        await self._client.connect()
        if self._client.connected:
            logger.info("Connected to EM540.")
            await  self._read_device_info()
        else:
            logger.info("Failed to connect to EM540.")

    async def disconnect(self):
        # Simulate disconnecting from the EM540 device
        if self._client.connected:
            logger.info("Disconnecting from EM540...")
        else:
            logger.info("Already disconnected.")

    @property
    def connected(self):
        return self._client.connected

    async def read_data(self) -> MeterData | None:
        start_register = 0x00F6
        end_register = 0x0161
        count = end_register - start_register + 1
        #print("num registers: " + str(count))

        num_registers = count
        try:
            result = await self._client.read_holding_registers(start_register, count=num_registers, device_id=1)
        except ModbusException:
            logger.error("Not connected to EM540")
            self._client.close()
            return None

        if result.isError():
            logger.error("Received error response from EM540")
            return None

        # Good to parse then
        self.data.parse(self._client, start_register, result.registers)
        return self.data

    async def _read_device_info(self):
        try:
            # Read device id
            resp = await self._client.read_holding_registers(0x000b, count=1, device_id=1)
            if resp.isError():
                logger.error("Error reading device information")
                self._client.close()
                return None
            self.data.device_type = self._client.convert_from_registers(resp.registers, ModbusTcpClient.DATATYPE.UINT16, 'little')

            resp = await self._client.read_holding_registers(0x1101, count=1, device_id=1)
            if resp.isError():
                logger.error("Error reading application")
                self._client.close()
                return None
            self.data.application = self._client.convert_from_registers(resp.registers, ModbusTcpClient.DATATYPE.UINT16, 'little')

            resp = await self._client.read_holding_registers(0x1103, count=1, device_id=1)
            if resp.isError():
                logger.error("Error measurement mode")
                self._client.close()
                return None
            self.data.measurement_mode = self._client.convert_from_registers(resp.registers, ModbusTcpClient.DATATYPE.UINT16, 'little')

            resp = await self._client.read_holding_registers(0x0302, count=1, device_id=1)
            if resp.isError():
                logger.error("Error reading firmware version")
                self._client.close()
                return None
            self.data.fw_ver = self._client.convert_from_registers(resp.registers, ModbusTcpClient.DATATYPE.UINT16, 'little')

            resp = await self._client.read_holding_registers(0x5000, count=8, device_id=1)
            if resp.isError():
                logger.error("Error reading firmware version")
                self._client.close()
                return None
            self.data.serial_data = resp.registers

            #print("device_type " + str(self.data.device_type) + ", fw_ver " + str(self.data.fw_ver) + ", measurement_mode " + str(self.data.measurement_mode))
            #print(self.data.application)
            #sys.exit(0)

        except ModbusException:
            logger.error("Could not get device information")
            self._client.close()
            return None

