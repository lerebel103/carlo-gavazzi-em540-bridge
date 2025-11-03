import asyncio
import logging
import sys
import threading
from threading import Thread

from pymodbus import FramerType, ModbusException
from pymodbus.client import (AsyncModbusSerialClient, AsyncModbusTcpClient,
                             ModbusBaseClient)
from pymodbus.exceptions import ModbusIOException

from carlo_gavazzi.meter_data import MeterData

logger = logging.getLogger("Em540Master")


class MeterDataListener:
    async def new_data(self, data: MeterData):
        raise NotImplementedError()

    async def read_failed(self):
        raise NotImplementedError()


class Em540Master:
    """Represents a Modbus master that reads data from an EM540 device.

    This class will do its best to read data up to a 10Hz rate which the EM540 is able to provide. However, this can
    only be achieved if only a subset of the available registers are read. The more registers are read, the slower the
    update rate will be. The read rate of non-critical data can be configured by setting the 'skip_n_read' parameter in
    the register definitions in em540_data.py.

    Additionally, a high baud rate of 115200bps should be used on the EM540 to achieve the best performance.

    Asyncio is used to avoid blocking the main thread while waiting for Modbus responses, listeners are notified
    in a separate thread.
    """

    def __init__(self, config) -> None:
        self._config = config
        self._data: MeterData = MeterData()
        self.slave_id: int = config.slave_id
        self._dyn_reg_read_counter: int = 0
        self._listeners: list[MeterDataListener] = []
        logger.setLevel(config.log_level)
        self._client: ModbusBaseClient

        if config.mode == "serial":
            # Create serial client
            self._client = AsyncModbusSerialClient(
                port=config.serial_port,
                framer=FramerType.RTU,
                baudrate=config.baudrate,
                parity=config.parity,
                stopbits=config.stopbits,
                timeout=config.timeout,
                retries=config.retries,
            )
        elif config.mode == "tcp":
            # Create Modbus TCP client
            self._client = AsyncModbusTcpClient(
                host=self._config.host,
                port=self._config.port,
                framer=FramerType.RTU,
                timeout=config.timeout,
                retries=config.retries,
            )
        else:
            raise ValueError(
                f"Invalid mode '{config.mode}' in configuration, must be 'tcp' or 'serial'"
            )

        # create notify mutex and thread for async notification of listeners
        self._condition: threading.Condition = threading.Condition()
        self._notify_thread: Thread = Thread(target=self._notify_loop, daemon=True)
        self._notify_thread.start()

    async def connect(self) -> None:
        # Simulate connecting to the EM540 device
        if self._config.mode == "serial":
            logger.info(
                "Connecting to EM540 via serial port "
                + self._config.serial_port
                + "..."
            )
        else:
            logger.info(
                "Connecting to EM540 at "
                + self._config.host
                + ":"
                + str(self._config.port)
                + "..."
            )

        await self._client.connect()
        if self._client.connected:
            logger.info("Connected to EM540.")
            if self._dyn_reg_read_counter == 0:
                logger.debug("Reading static registers from EM540...")
                frame = self._data.frame
                if not await self._read_registers(frame.static_reg_map):
                    logger.error("Failed to read device info from EM540.")
                    self._client.close()
        else:
            logger.info("Failed to connect to EM540.")

    def _notify_loop(self) -> None:
        while True:
            with self._condition:
                self._condition.wait()

                # Now update the MeterData from the frame we have just received.
                # Then notify listeners, noting a performance impact as we are holding a lock.
                # However, this will prevent a new data acquire while we are notifying listeners.

                try:
                    self._data.update_from_frame()
                    for listener in self._listeners:
                        asyncio.run(listener.new_data(self._data))
                except Exception as e:
                    # Yeah... this is not a great solution, looking for a safe way to ensure we handle critical errors
                    logger.critical("Notify loop failure, restarting as a safe guard")
                    logger.error(e)

                    # As a docker container, this will cause the container to respawn safely to clear whatever
                    # error state we might be in currently.
                    sys.exit(1)

    @property
    def data(self) -> MeterData:
        return self._data

    async def disconnect(self) -> None:
        # Simulate disconnecting from the EM540 device
        if self._client.connected:
            logger.info("Disconnecting from EM540...")
        else:
            logger.info("Already disconnected.")

    def add_listener(self, listener: MeterDataListener) -> None:
        self._listeners.append(listener)

    def remove_listener(self, listener: MeterDataListener) -> None:
        self._listeners.remove(listener)

    @property
    def connected(self) -> bool:
        return self._client.connected

    async def acquire_data(self) -> bool:
        # No point reading if we are not connected
        if not self._client.connected:
            for listener in self._listeners:
                await listener.read_failed()
            return False

        # Retrieve the frame we will be working with
        frame = self._data.frame

        # Read our dynamic registers
        self._dyn_reg_read_counter += 1
        is_ok: bool = await self._read_registers(frame.dynamic_reg_map, dyn_reg=True)
        if is_ok:
            # Now notify listeners
            with self._condition:
                self._condition.notify()
        else:
            # Now notify listeners
            for listener in self._listeners:
                await listener.read_failed()

        return is_ok

    async def _read_registers(self, reg_map: dict, dyn_reg: bool = False) -> bool:
        try:
            # Read dynamic registers
            # Only read the primary register every cycle, the rest are read less often
            # This is because we can't keep up a 10Hz read rate if we read all registers.
            for reg_addr in reg_map:
                reg_desc = reg_map[reg_addr]
                skip_n_read: int = reg_desc.skip_n_read

                # Always perform the first read on all registers
                # Then skip reads as configured
                if dyn_reg and self._dyn_reg_read_counter > 1 and skip_n_read > 0:
                    if (self._dyn_reg_read_counter % (skip_n_read + 1)) != 0:
                        logger.debug(
                            f">>>> Skipping read of '{reg_desc.description}' register "
                            f"at {hex(reg_addr)}, read counter={self._dyn_reg_read_counter}, skip_n_read={skip_n_read}"
                        )
                        continue

                num_registers: int = len(reg_desc.values)
                logger.debug(
                    f"Reading '{reg_desc.description}' from start register address {hex(reg_addr)}, "
                    f"count={num_registers}"
                )
                result = await self._client.read_holding_registers(
                    reg_addr, count=num_registers, device_id=self.slave_id
                )

                if result.isError():
                    logger.error(
                        f"Error reading register {hex(reg_addr)}, count={num_registers}"
                    )
                    return False

                # Check if we received the expected number of registers
                # Force quit to be safe, as it seems at that stage the client is in a bad state and further reads will
                # fail with out-of-order responses. Resetting the client could be better, but for now just exit.
                if len(result.registers) != num_registers:
                    logger.fatal(
                        f"Expected {num_registers} registers but got {len(result.registers)} "
                        f"for address {hex(reg_addr)}"
                    )
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
