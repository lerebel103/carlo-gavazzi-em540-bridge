import logging
import time
from typing import Optional

from pymodbus import ExceptionResponse
from pymodbus.constants import ExcCodes
from pymodbus.pdu import ModbusPDU

# Minimum seconds between repeated "watched address polled" logs, per address.
_WATCH_LOG_INTERVAL_S = 10.0


class PduHelper:
    def __init__(
        self,
        logger: logging.Logger,
        bridge_timeout: float,
        served_device_ids: Optional[set[int]] = None,
        log_read_addresses: Optional[set[int]] = None,
    ) -> None:
        self.logger: logging.Logger = logger
        self.bridge_timeout = bridge_timeout
        # Device IDs this bridge actually serves. Exception responses to these
        # IDs indicate a genuine downstream failure (illegal address/function,
        # device failure) and are logged at ERROR. Exceptions to any other ID
        # are device-scan probes (see on_pdu) and are logged at DEBUG.
        self.served_device_ids: set[int] = served_device_ids or set()
        # Specific register-read start addresses to observe. Whenever an incoming
        # request reads one of these addresses it is counted and logged (rate
        # limited), regardless of which client issued it. Used to monitor a
        # register that a downstream client polls but this emulation serves as a
        # compatibility value (e.g. 50000). Detection is done on the request PDU
        # itself, so it is race-free even with concurrent connections.
        self.log_read_addresses: set[int] = log_read_addresses or set()
        # address -> observed count, and address -> last-logged monotonic time.
        self.watched_read_counts: dict[int, int] = dict.fromkeys(self.log_read_addresses, 0)
        self._watched_last_log: dict[int, float] = {}
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

        # Observe reads of watched addresses. Done on the inbound request pass
        # (flag is False), using the request PDU directly, so it is race-free
        # regardless of which client issued it or how many connections are active.
        if not flag and self.log_read_addresses:
            self._note_watched_read(pdu, now)

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

    def _note_watched_read(self, request: ModbusPDU, now: float) -> None:
        """Count and (rate-limited) log a read of a watched address.

        Uses the incoming request PDU directly, so it is client-agnostic and
        race-free. The count is always incremented; the log is emitted at most
        once per ``_WATCH_LOG_INTERVAL_S`` per address to avoid handler-I/O
        spam on the server loop, since the address is polled continuously.
        """
        address = getattr(request, "address", None)
        if address not in self.log_read_addresses:
            return

        count = self.watched_read_counts.get(address, 0) + 1
        self.watched_read_counts[address] = count

        last_log = self._watched_last_log.get(address, 0.0)
        if (now - last_log) >= _WATCH_LOG_INTERVAL_S:
            self._watched_last_log[address] = now
            self.logger.info(
                "Watched register read observed: address=%s count=%s (served as compatibility value; "
                "polled %d times so far)",
                address,
                getattr(request, "count", "?"),
                count,
            )

    def data_received(self, timestamp: float) -> None:
        self._last_rx_timestamp = timestamp
        self._close_circuit()
