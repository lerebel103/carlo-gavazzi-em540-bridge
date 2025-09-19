import logging
import struct

from pymodbus import pymodbus_apply_logging_config, FramerType
from pymodbus.client import AsyncModbusTcpClient

logger = logging.getLogger()

class Em540Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port

        # Create Modbus client
        pymodbus_apply_logging_config("DEBUG")
        self._client = AsyncModbusTcpClient(host=self.host, port=self.port, framer=FramerType.RTU, timeout=3, retries=3)

    async def connect(self):
        # Simulate connecting to the EM540 device
        logger.info(f"Connecting to EM540 at {self.host}:{self.port}...")

        await self._client.connect()
        if self._client.connected:
            logger.info("Connected to EM540.")
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


    async def read_data(self):
        # Phase variables
        start_register = 300287
        count = 7

        # Each block of 7 registers corresponds to a phase:
        # V Line-Line+1 INT32 Value weight: Volt*10
        # V Line-N INT32 Value weight: Volt*10
        # A Line INT32 Value weight: Ampere*1000
        # W Line INT32 Value weight: Watt*10
        # VA Line INT32 Value weight: VA*10
        # var Line INT32 Value weight: var*10
        # PF Line 0130h INT32 (See 2Eh register note). Value weight: PF*1000

        values = []
        num_registers = count * 2
        from pymodbus import ModbusException
        try:
            result = await self._client.read_holding_registers(start_register, count=num_registers)
        except ModbusException:
            logger.error("Not connected to EM540")
            return None

        if not result.isError():
            # Combine every two registers into one INT32
            for i in range(0, num_registers, 2):
                # Modbus returns 16-bit values, combine to 32-bit
                high = result.registers[i]
                low = result.registers[i + 1]
                # Pack as big-endian INT32
                int32 = struct.unpack('>i', struct.pack('>HH', high, low))[0]
                values.append(int32)
        else:
            logger.error(f"Modbus read error: {result}")
            return None

        print(values)

        # Dummy data for illustration purposes
        data = {
            "voltage": 230.0,
            "current": 5.0,
            "power": 1150.0,
            "energy": 12345.6,
            "frequency": 50.0,
            "power_factor": 0.95
        }
        return data