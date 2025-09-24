import logging

from pymodbus import FramerType
from pymodbus.client import AsyncModbusTcpClient
from pymodbus import ModbusException

from MeterData import MeterData

logger = logging.getLogger('Em540Master')


class MeterDataListener:
    async def new_data(self, data: MeterData):
        raise NotImplementedError()

    async def read_failed(self):
        raise NotImplementedError()


class Em540Master:
    def __init__(self, host, port, slave_id=1):
        self.host = host
        self.port = port
        self._data = MeterData()
        self.slave_id = slave_id
        self._first_read = True
        self._listeners: list[MeterDataListener] = []

        # Create Modbus client
        self._client = AsyncModbusTcpClient(
            host=self.host, port=self.port, framer=FramerType.RTU, timeout=0.2, retries=1)

    async def connect(self):
        # Simulate connecting to the EM540 device
        logger.info(f"Connecting to EM540 at {self.host}:{self.port}...")

        await self._client.connect()
        if self._client.connected:
            logger.info("Connected to EM540.")
            if self._first_read:
                logger.debug("Reading static registers from EM540...")
                frame = self._data.frame
                if await self._read_registers(frame.static_reg_map):
                    self._first_read = False
                else:
                    logger.error("Failed to read device info from EM540.")
                    self._client.close()
        else:
            logger.info("Failed to connect to EM540.")

    @property
    def data(self) -> MeterData:
        return self._data

    async def disconnect(self):
        # Simulate disconnecting from the EM540 device
        if self._client.connected:
            logger.info("Disconnecting from EM540...")
        else:
            logger.info("Already disconnected.")

    def add_listener(self, listener: MeterDataListener):
        self._listeners.append(listener)

    def remove_listener(self, listener: MeterDataListener):
        self._listeners.remove(listener)

    @property
    def connected(self):
        return self._client.connected

    async def read_data(self) -> bool:
        # No point reading if we are not connected
        if not self._client.connected:
            for listener in self._listeners:
                await listener.read_failed()
            return False

        # Retrieve the frame we will be working with
        frame = self._data.frame

        # Read our dynamic registers
        is_ok = await self._read_registers(frame.dynamic_reg_map)
        if is_ok:
            # Now update the MeterData from the frame we have just received
            self._data.update_from_frame()

            # Now notify listeners
            for listener in self._listeners:
                await listener.new_data(self._data)
        else:
            # Now notify listeners
            for listener in self._listeners:
                await listener.read_failed()

        return is_ok

    async def _read_registers(self, reg_map) -> bool:
        try:
            # Read dynamic registers
            for reg_addr in reg_map:
                num_registers = len(reg_map[reg_addr].values)
                logger.debug(
                    f"Reading '{reg_map[reg_addr].description}' from start register address {hex(reg_addr)}, count={num_registers}")
                result = await self._client.read_holding_registers(reg_addr, count=num_registers,
                                                                   device_id=self.slave_id)

                if result.isError():
                    logger.error(f"Error reading register {hex(reg_addr)}, count={num_registers}")
                    return False
                # Check if we received the expected number of registers
                if len(result.registers) != num_registers:
                    logger.error(
                        f"Expected {num_registers} registers but got {len(result.registers)} for address {hex(reg_addr)}")
                    return False

                # Store the read values
                reg_map[reg_addr].values = result.registers
        except ModbusException:
            logger.error("Could not read dynamic registers")
            self._client.close()
            return False

        return True
