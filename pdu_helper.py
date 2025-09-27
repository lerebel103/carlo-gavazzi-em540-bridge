import logging
from datetime import datetime

from pymodbus import ExceptionResponse
from pymodbus.constants import ExcCodes
from pymodbus.pdu import ModbusPDU


class PduHelper:
    def __init__(self, logger, bridge_timeout: float):
        self.logger = logger
        self.bridge_timeout = bridge_timeout
        self.last_pdu = None
        self._last_rx_timestamp = None

    def on_pdu(self, flag: bool, pdu: ModbusPDU) -> ModbusPDU:
        # Here we deliberately drop requests if we have not received any data from the master
        # within the bridge timeout period.
        now = datetime.now().timestamp()
        if self._last_rx_timestamp is None or (now - self._last_rx_timestamp)  > self.bridge_timeout:
            self.logger.warning("Dropping request since no data received from master within timeout period")

            if flag:
                # Only modify responses to say we are busy
                # Modbus exception code 6 = Slave Device Busy
                return ExceptionResponse(pdu.function_code,
                                         exception_code=ExcCodes.DEVICE_BUSY, device_id=pdu.dev_id, transaction=pdu.transaction_id)

        # Log some exceptions so we can debug any issues with register access not accounted for
        if pdu.exception_code != 0:
            self.logger.error(pdu)
            self.logger.error(f"Prior PDU: {self.last_pdu}")

        self.last_pdu = pdu
        return pdu

    def data_received(self, timestamp):
        self._last_rx_timestamp = timestamp
