import asyncio
import logging
import sys
import threading
from threading import Thread

from pymodbus import FramerType
from pymodbus.client import AsyncModbusTcpClient
from pymodbus import ModbusException
from pymodbus.exceptions import ModbusIOException

from meter_data import MeterData

logger = logging.getLogger('Em540Master')


class MeterDataListener:
    async def new_data(self, data: MeterData):
        raise NotImplementedError()

    async def read_failed(self):
        raise NotImplementedError()


class Em540Master:
    def __init__(self, config):
        self.host = config.host
        self.port = config.port
        self._data = MeterData()
        self.slave_id = config.slave_id
        self._read_counter = 0
        self._listeners: list[MeterDataListener] = []
        logger.setLevel(config.log_level)

        # Create Modbus client
        self._client = AsyncModbusTcpClient(
            host=self.host, port=self.port, framer=FramerType.RTU,
            timeout=config.timeout, retries=config.retries)

        # create notify mutex
        self._condition = threading.Condition()

        self._notify_thread = Thread(target=self._notify_loop, daemon=True)
        self._notify_thread.start()


    async def connect(self):
        # Simulate connecting to the EM540 device
        logger.info(f"Connecting to EM540 at {self.host}:{self.port}...")

        await self._client.connect()
        if self._client.connected:
            logger.info("Connected to EM540.")
            if self._read_counter == 0:
                logger.debug("Reading static registers from EM540...")
                frame = self._data.frame
                if not await self._read_registers(frame.static_reg_map):
                    logger.error("Failed to read device info from EM540.")
                    self._client.close()
        else:
            logger.info("Failed to connect to EM540.")

    def _notify_loop(self):
        while True:
            with self._condition:
                self._condition.wait()

                # Now update the MeterData from the frame we have just received
                self._data.update_from_frame()

                for listener in self._listeners:
                    asyncio.run(listener.new_data(self._data))

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
            # Now notify listeners
            with self._condition:
                self._condition.notify()
        else:
            # Now notify listeners
            for listener in self._listeners:
                await listener.read_failed()

        return is_ok

    async def _read_registers(self, reg_map) -> bool:
        try:
            self._read_counter += 1
            # Read dynamic registers
            # Only read the primary register every cycle, the rest are read less often
            # This is because we can't keep up a 10Hz read rate if we read all registers.
            for reg_addr in reg_map:
                reg_desc = reg_map[reg_addr]
                skip_n_read = reg_desc.skip_n_read

                # Always perform the first read on all registers
                # Then skip reads as configured
                if self._read_counter > 1 and skip_n_read > 0:
                    if (self._read_counter % (skip_n_read + 1)) != 0:
                        logger.debug(f">>>> Skipping read of '{reg_desc.description}' register at {hex(reg_addr)}, read counter={self._read_counter}, skip_n_read={skip_n_read}")
                        continue

                num_registers = len(reg_desc.values)
                logger.debug(
                    f"Reading '{reg_desc.description}' from start register address {hex(reg_addr)}, count={num_registers}")
                result = await self._client.read_holding_registers(reg_addr, count=num_registers,
                                                                   device_id=self.slave_id)

                if result.isError():
                    logger.error(f"Error reading register {hex(reg_addr)}, count={num_registers}")
                    return False

                # Check if we received the expected number of registers
                # Force quit to be safe, as it seems at that stage the client is in a bad state and further reads will fail
                # with out-of-order responses. Resetting the client could be better, but for now just exit.
                if len(result.registers) != num_registers:
                    logger.fatal(
                        f"Expected {num_registers} registers but got {len(result.registers)} for address {hex(reg_addr)}")
                    sys.exit(1)

                self._bad_read_count = 0

                # Store the read values
                reg_map[reg_addr].values = result.registers
        except ModbusIOException as ex:
            logger.error("Modbus IO error reading registers from EM540: %s", ex)
            return False
        except ModbusException as ex:
            logger.error("Could not read dynamic registers from EM540: %s", ex)
            self._client.close()
            return False

        return True
