import logging
from datetime import datetime
from typing import Optional

from pymodbus import ExceptionResponse
from pymodbus.constants import ExcCodes
from pymodbus.pdu import ModbusPDU


class PduHelper:
    def __init__(self, logger: logging.Logger, bridge_timeout: float) -> None:
        self.logger: logging.Logger = logger
        self.bridge_timeout = bridge_timeout
        self.last_pdu: Optional[ModbusPDU] = None
        self._last_rx_timestamp: Optional[float] = None
        self._last_warning_timestamp: float = 0
        self._dropped_request_count: int = 0
        self._circuit_open: bool = True
        self._circuit_open_count: int = 1

    @property
    def dropped_request_count(self) -> int:
        return self._dropped_request_count

    @property
    def circuit_open(self) -> bool:
        return self._circuit_open

    @property
    def circuit_open_count(self) -> int:
        return self._circuit_open_count

    def stale_age_seconds(self, now: Optional[float] = None) -> Optional[float]:
        if self._last_rx_timestamp is None:
            return None
        if now is None:
            now = datetime.now().timestamp()
        return now - self._last_rx_timestamp

    def _open_circuit(self, reason: str, now: float) -> None:
        if not self._circuit_open:
            self._circuit_open = True
            self._circuit_open_count += 1
            self.logger.warning("Opening Modbus circuit breaker: %s", reason)

    def _close_circuit(self) -> None:
        if self._circuit_open:
            self._circuit_open = False
            self.logger.info("Closing Modbus circuit breaker: fresh upstream data")

    def upstream_failed(self) -> None:
        now: float = datetime.now().timestamp()
        self._open_circuit("upstream read failure", now)

    def on_pdu(self, flag: bool, pdu: ModbusPDU) -> ModbusPDU:
        # Here we deliberately drop requests if we have not received any data from the master
        # within the bridge timeout period.
        now: float = datetime.now().timestamp()

        stale_age = self.stale_age_seconds(now)
        bridge_timeout = self.bridge_timeout() if callable(self.bridge_timeout) else self.bridge_timeout
        is_stale = stale_age is None or stale_age > bridge_timeout
        if is_stale:
            self._open_circuit("stale upstream data", now)

        if self._circuit_open:
            self._dropped_request_count += 1

            # Only print this warning every 10 seconds
            if (now - self._last_warning_timestamp) > 10:
                self.logger.warning(
                    f"Dropping request, no data received (dropped {self._dropped_request_count} requests so far)."
                )
                self._last_warning_timestamp = now

            # Reply with a clear Modbus exception when the data path is stale or open-circuit.
            try:
                return ExceptionResponse(
                    pdu.function_code,
                    exception_code=ExcCodes.DEVICE_BUSY,
                    device_id=pdu.dev_id,
                    transaction=pdu.transaction_id,
                )
            except TypeError:
                response = ExceptionResponse(
                    pdu.function_code,
                    exception_code=ExcCodes.DEVICE_BUSY,
                )
                if hasattr(response, "dev_id"):
                    response.dev_id = pdu.dev_id
                if hasattr(response, "transaction_id"):
                    response.transaction_id = pdu.transaction_id
                return response

        # Log some exceptions so we can debug any issues with register access not accounted for...
        # For whatever reason, Victron seems to be wanting slave_id 2, just mute this one
        if getattr(pdu, "exception_code", 0) != 0 and getattr(pdu, "dev_id", 2) != 2:
            self.logger.error(pdu)
            self.logger.error(f"Prior PDU: {self.last_pdu}")

        self.last_pdu = pdu
        return pdu

    def data_received(self, timestamp: float) -> None:
        self._last_rx_timestamp = timestamp
        self._close_circuit()
