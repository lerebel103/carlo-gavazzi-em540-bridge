import logging
from datetime import datetime

from pymodbus.pdu import ModbusPDU

logger = logging.getLogger('PduHelper')

class PduHelper:
    def __init__(self, bridge_timeout: float):
        self.bridge_timeout = bridge_timeout
        self.last_pdu = None
        self._last_rx_timestamp = None

    def on_pdu(self, flag: bool, pdu: ModbusPDU) -> ModbusPDU:
        # Here we deliberately drop requests if we have not received any data from the master
        # within the bridge timeout period.
        now = datetime.now().timestamp()
        if self._last_rx_timestamp is None or (now - self._last_rx_timestamp)  > self.bridge_timeout:
            logger.warning("Dropping request since no data received from master within timeout period")
            return ModbusPDU()

        # Log some exceptions so we can debug any issues with register access not accounted for
        if pdu.exception_code != 0:
            logger.error(pdu)
            logger.error(f"Prior PDU: {self.last_pdu}")

        self.last_pdu = pdu
        return pdu

    def data_received(self, timestamp):
        self._last_rx_timestamp = timestamp
