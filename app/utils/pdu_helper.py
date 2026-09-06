import logging
import time
from typing import Optional

from pymodbus import ExceptionResponse
from pymodbus.constants import ExcCodes
from pymodbus.pdu import ModbusPDU
from pymodbus.pdu.register_message import ReadHoldingRegistersResponse

# Modbus function codes for register reads.
_FC_READ_HOLDING = 3
_FC_READ_INPUT = 4


class PduHelper:
    def __init__(
        self,
        logger: logging.Logger,
        bridge_timeout: float,
        served_device_ids: Optional[set[int]] = None,
        zero_fill_read_addresses: Optional[set[int]] = None,
    ) -> None:
        self.logger: logging.Logger = logger
        self.bridge_timeout = bridge_timeout
        # Device IDs this bridge actually serves. Exception responses to these
        # IDs indicate a genuine downstream failure (illegal address/function,
        # device failure) and are logged at ERROR. Exceptions to any other ID
        # are device-scan probes (see on_pdu) and are logged at DEBUG.
        self.served_device_ids: set[int] = served_device_ids or set()
        # Specific register-read start addresses for which an ILLEGAL_DATA_ADDRESS
        # rejection should be answered with zeros instead of the exception. This is
        # a deliberately narrow allow-list — ONLY these exact addresses are
        # substituted; every other out-of-range read still returns the proper
        # ILLEGAL_DATA_ADDRESS. Used to work around a specific downstream client
        # (Fronius) that faults when a particular unimplemented register (50000)
        # returns an exception. Each substitution is logged so its effect can be
        # compared against the exception behaviour.
        self.zero_fill_read_addresses: set[int] = zero_fill_read_addresses or set()
        self.zero_filled_read_count: int = 0
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
            now = time.time()
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
        now: float = time.time()
        self._open_circuit("upstream read failure", now)

    def on_pdu(self, flag: bool, pdu: ModbusPDU) -> ModbusPDU:
        # Here we deliberately drop requests if we have not received any data from the master
        # within the bridge timeout period.
        now: float = time.time()

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

        # Narrow work-around: substitute a zero-filled response for an
        # ILLEGAL_DATA_ADDRESS rejection, but ONLY for the specific read addresses
        # in the allow-list. Runs on the outgoing response path (flag is True);
        # self.last_pdu holds the request that produced this exception.
        if flag and getattr(pdu, "exception_code", 0) == int(ExcCodes.ILLEGAL_ADDRESS):
            zero_response = self._maybe_zero_fill(pdu)
            if zero_response is not None:
                return zero_response

        # Log exception responses, distinguishing genuine failures from scan noise:
        #  - Exceptions for a device ID we actually serve indicate a real
        #    downstream problem (illegal address/function, device failure) and are
        #    logged at ERROR so they are visible at normal log levels.
        #  - Exceptions for any other ID are device-scan probes: downstream
        #    controllers (e.g. Fronius inverters) sweep a range of unit IDs looking
        #    for devices, and every probe to an ID we don't emulate yields a
        #    legitimate SLAVE_DEVICE_FAILURE. These are logged at DEBUG so the
        #    scanning noise stays silent at INFO.
        # Victron polls unit ID 2 even when we don't emulate it; those exception
        # responses are muted entirely as known noise. But if this bridge is
        # actually configured to serve ID 2, exceptions for it are genuine
        # failures and must not be suppressed.
        dev_id = getattr(pdu, "dev_id", 2)
        served = dev_id in self.served_device_ids
        muted = dev_id == 2 and not served
        if getattr(pdu, "exception_code", 0) != 0 and not muted:
            log = self.logger.error if served else self.logger.debug
            log(pdu)
            log(f"Prior PDU: {self.last_pdu}")

        self.last_pdu = pdu
        return pdu

    def _maybe_zero_fill(self, exception_pdu: ModbusPDU) -> Optional[ModbusPDU]:
        """Return a zero-filled read response, but only for an allow-listed address.

        Returns None (leave the ILLEGAL_DATA_ADDRESS exception unchanged) unless
        the triggering request was a register read (FC 3/4) whose exact start
        address is in ``zero_fill_read_addresses``. This is intentionally narrow:
        no other out-of-range read is affected.
        """
        if not self.zero_fill_read_addresses:
            return None

        request = self.last_pdu
        if request is None:
            return None
        if getattr(request, "function_code", None) not in (_FC_READ_HOLDING, _FC_READ_INPUT):
            return None

        address = getattr(request, "address", None)
        if address not in self.zero_fill_read_addresses:
            return None

        count = getattr(request, "count", 0)
        if not isinstance(count, int) or count <= 0:
            return None

        self.zero_filled_read_count += 1
        self.logger.warning(
            "Returning zeros instead of ILLEGAL_DATA_ADDRESS for allow-listed read: "
            "dev_id=%s address=%s count=%s (zero-filled %d times so far). "
            "Monitoring effect vs. sending the exception.",
            getattr(request, "dev_id", "?"),
            address,
            count,
            self.zero_filled_read_count,
        )

        response = ReadHoldingRegistersResponse(registers=[0] * count)
        # Preserve addressing so the frame routes back to the right client/txn.
        if hasattr(response, "dev_id"):
            response.dev_id = getattr(request, "dev_id", 0)
        if hasattr(response, "transaction_id"):
            response.transaction_id = getattr(request, "transaction_id", 0)
        return response

    def data_received(self, timestamp: float) -> None:
        self._last_rx_timestamp = timestamp
        self._close_circuit()
