import logging
from datetime import datetime
from typing import Optional

from pymodbus import ExceptionResponse
from pymodbus.constants import ExcCodes
from pymodbus.pdu import ModbusPDU


class PduHelper:
    def __init__(self, logger: logging.Logger, bridge_timeout: float) -> None:
        self.logger: logging.Logger = logger
        self.bridge_timeout: float = bridge_timeout
        self.last_pdu: Optional[ModbusPDU] = None
        self._last_rx_timestamp: Optional[float] = None
        self._last_warning_timestamp: float = 0
        self._dropped_request_count: int = 0

    def on_pdu(self, flag: bool, pdu: ModbusPDU) -> ModbusPDU:
        # Here we deliberately drop requests if we have not received any data from the master
        # within the bridge timeout period.
        now: float = datetime.now().timestamp()

        if (self._last_rx_timestamp is None
            or (now - self._last_rx_timestamp) > self.bridge_timeout):
            self._dropped_request_count += 1

            # Only print this warning every 10 seconds
            if (now - self._last_warning_timestamp) > 10:
                self.logger.warning(
                    f"Dropping request, no data received (dropped {self._dropped_request_count} requests so far).")
                self._last_warning_timestamp = now

            if flag:
                # Only modify responses to say we are busy
                # Modbus exception code 6 = Slave Device Busy
                return ExceptionResponse(
                    pdu.function_code,
                    exception_code=ExcCodes.DEVICE_BUSY,
                    device_id=pdu.dev_id,
                    transaction=pdu.transaction_id,
                )

        # Log some exceptions so we can debug any issues with register access not accounted for...
        # For whatever reason, Victron seems to be wanting slave_id 2, just mute this one
        if getattr(pdu, "exception_code", 0) != 0 and getattr(pdu, "dev_id", 2) != 2:
            self.logger.error(pdu)
            self.logger.error(f"Prior PDU: {self.last_pdu}")

        self.last_pdu = pdu
        return pdu

    def data_received(self, timestamp: float) -> None:
        self._last_rx_timestamp = timestamp
