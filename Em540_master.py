import logging
import sys

from pymodbus import pymodbus_apply_logging_config, FramerType
from pymodbus.client import AsyncModbusTcpClient, ModbusTcpClient
from pymodbus import ModbusException

from Em540Data import Em540Frame

logger = logging.getLogger()



class Em540Master:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.data = Em540Frame()
        self.slave_id = 1
        self._first_read = True

        # Create Modbus client
        self._client = AsyncModbusTcpClient(host=self.host, port=self.port, framer=FramerType.RTU, timeout=0.2, retries=1)


    async def connect(self):
        # Simulate connecting to the EM540 device
        logger.info(f"Connecting to EM540 at {self.host}:{self.port}...")

        await self._client.connect()
        if self._client.connected:
            logger.info("Connected to EM540.")
            if self._first_read:
                logger.debug("Reading dynamic registers from EM540...")
                if await self._read_registers(self.data.static_reg_map):
                    self._first_read = False
                else:
                    logger.error("Failed to read device info from EM540.")
                    self._client.close()
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

    async def read_data(self) -> bool:
        is_ok = await self._read_registers(self.data.dynamic_reg_map)
        if is_ok:
            self.data.remap_registers()
        return is_ok

    async def _read_registers(self, reg_map) -> bool:
        try:
            # Read dynamic registers
            for reg_addr in reg_map:
                num_registers = len(reg_map[reg_addr].values)
                logger.debug(f"Reading '{reg_map[reg_addr].description}' from start register address {hex(reg_addr)}, count={num_registers}")
                result = await self._client.read_holding_registers(reg_addr, count=num_registers, device_id=self.slave_id)

                if result.isError():
                    logger.error(f"Error reading register {hex(reg_addr)}, count={num_registers}")
                    return False
                # Check if we received the expected number of registers
                if len(result.registers) != num_registers:
                    logger.error(f"Expected {num_registers} registers but got {len(result.registers)} for address {hex(reg_addr)}")
                    return False

                # Store the read values
                reg_map[reg_addr].values = result.registers
        except ModbusException:
            logger.error("Could not read dynamic registers")
            self._client.close()
            return False

        return True