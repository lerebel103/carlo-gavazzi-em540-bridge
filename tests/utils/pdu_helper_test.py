import unittest
from types import SimpleNamespace

import pymodbus.constants as _const

if not hasattr(_const, "ExcCodes"):

    class _ExcCodes:
        DEVICE_BUSY = 0x06

    _const.ExcCodes = _ExcCodes

from app.utils.pdu_helper import PduHelper


def _make_pdu(function_code=3, dev_id=1, transaction_id=1):
    return SimpleNamespace(
        function_code=function_code,
        dev_id=dev_id,
        transaction_id=transaction_id,
        exception_code=0,
    )


class TestPduHelperCircuitBreaker(unittest.TestCase):
    def setUp(self):
        self.logger = SimpleNamespace(
            warning=lambda *args, **kwargs: None,
            info=lambda *args, **kwargs: None,
            error=lambda *args, **kwargs: None,
            debug=lambda *args, **kwargs: None,
        )

    def test_stale_data_returns_exception_response(self):
        helper = PduHelper(self.logger, bridge_timeout=0.1)
        response = helper.on_pdu(True, _make_pdu())

        self.assertTrue(helper.circuit_open)
        self.assertEqual(helper.dropped_request_count, 1)
        self.assertEqual(getattr(response, "exception_code", None), 6)

    def test_upstream_failed_opens_circuit(self):
        helper = PduHelper(self.logger, bridge_timeout=10.0)
        helper.data_received(123.0)
        self.assertFalse(helper.circuit_open)

        helper.upstream_failed()
        self.assertTrue(helper.circuit_open)

    def test_fresh_data_closes_circuit(self):
        helper = PduHelper(self.logger, bridge_timeout=10.0)
        helper.upstream_failed()
        self.assertTrue(helper.circuit_open)

        helper.data_received(123.0)
        self.assertFalse(helper.circuit_open)

    def test_callable_bridge_timeout_is_used_dynamically(self):
        timeout_holder = {"value": 10.0}
        helper = PduHelper(self.logger, bridge_timeout=lambda: timeout_holder["value"])
        helper.data_received(100.0)

        timeout_holder["value"] = 0.1
        response = helper.on_pdu(True, _make_pdu())

        self.assertTrue(helper.circuit_open)
        self.assertEqual(getattr(response, "exception_code", None), 6)

    def test_scanning_exception_responses_are_logged_at_debug_not_error(self):
        calls = {"error": 0, "debug": 0}
        logger = SimpleNamespace(
            warning=lambda *a, **k: None,
            info=lambda *a, **k: None,
            error=lambda *a, **k: calls.__setitem__("error", calls["error"] + 1),
            debug=lambda *a, **k: calls.__setitem__("debug", calls["debug"] + 1),
        )
        import time

        helper = PduHelper(logger, bridge_timeout=10.0)
        # Close the circuit with fresh data so we reach the exception-logging
        # branch (not the open-circuit drop path). Use a current timestamp so
        # the staleness check keeps the circuit closed.
        helper.data_received(time.time())
        self.assertFalse(helper.circuit_open)

        # An exception response from an unaddressed device-scan probe.
        exc_pdu = _make_pdu(function_code=131, dev_id=40)
        exc_pdu.exception_code = 4  # SLAVE_DEVICE_FAILURE

        returned = helper.on_pdu(False, exc_pdu)

        self.assertIs(returned, exc_pdu)
        self.assertEqual(calls["error"], 0)
        self.assertEqual(calls["debug"], 2)  # the PDU and the "Prior PDU" line

    def test_exception_for_served_device_id_is_logged_at_error(self):
        calls = {"error": 0, "debug": 0}
        logger = SimpleNamespace(
            warning=lambda *a, **k: None,
            info=lambda *a, **k: None,
            error=lambda *a, **k: calls.__setitem__("error", calls["error"] + 1),
            debug=lambda *a, **k: calls.__setitem__("debug", calls["debug"] + 1),
        )
        import time

        # This bridge serves device_id 1; an exception for it is a genuine
        # downstream failure and must surface at ERROR, not be hidden as scan noise.
        helper = PduHelper(logger, bridge_timeout=10.0, served_device_ids={1})
        helper.data_received(time.time())
        self.assertFalse(helper.circuit_open)

        exc_pdu = _make_pdu(function_code=131, dev_id=1)
        exc_pdu.exception_code = 2  # ILLEGAL_DATA_ADDRESS

        helper.on_pdu(False, exc_pdu)

        self.assertEqual(calls["error"], 2)  # the PDU and the "Prior PDU" line
        self.assertEqual(calls["debug"], 0)

    def test_device_id_2_is_logged_at_error_when_it_is_a_served_id(self):
        # ID 2 is normally muted (Victron noise), but if the bridge is configured
        # to serve ID 2, its exceptions are genuine failures and must surface.
        calls = {"error": 0, "debug": 0}
        logger = SimpleNamespace(
            warning=lambda *a, **k: None,
            info=lambda *a, **k: None,
            error=lambda *a, **k: calls.__setitem__("error", calls["error"] + 1),
            debug=lambda *a, **k: calls.__setitem__("debug", calls["debug"] + 1),
        )
        import time

        helper = PduHelper(logger, bridge_timeout=10.0, served_device_ids={2})
        helper.data_received(time.time())

        exc_pdu = _make_pdu(function_code=131, dev_id=2)
        exc_pdu.exception_code = 4

        helper.on_pdu(False, exc_pdu)

        self.assertEqual(calls["error"], 2)
        self.assertEqual(calls["debug"], 0)

    def test_device_id_2_is_muted_when_not_served(self):
        calls = {"error": 0, "debug": 0}
        logger = SimpleNamespace(
            warning=lambda *a, **k: None,
            info=lambda *a, **k: None,
            error=lambda *a, **k: calls.__setitem__("error", calls["error"] + 1),
            debug=lambda *a, **k: calls.__setitem__("debug", calls["debug"] + 1),
        )
        import time

        helper = PduHelper(logger, bridge_timeout=10.0, served_device_ids={1})
        helper.data_received(time.time())

        exc_pdu = _make_pdu(function_code=131, dev_id=2)
        exc_pdu.exception_code = 4

        helper.on_pdu(False, exc_pdu)

        # Fully muted: neither error nor debug.
        self.assertEqual(calls["error"], 0)
        self.assertEqual(calls["debug"], 0)


class TestPduHelperWatchedReads(unittest.TestCase):
    """Observing (counting + rate-limited logging) reads of watched addresses."""

    def setUp(self):
        import time

        self.time = time
        self.calls = {"info": 0}
        self.logger = SimpleNamespace(
            warning=lambda *a, **k: None,
            info=lambda *a, **k: self.calls.__setitem__("info", self.calls["info"] + 1),
            error=lambda *a, **k: None,
            debug=lambda *a, **k: None,
        )

    def _closed_helper(self, **kwargs):
        helper = PduHelper(self.logger, bridge_timeout=10.0, **kwargs)
        helper.data_received(self.time.time())  # close the circuit
        self.assertFalse(helper.circuit_open)
        # Reset the info counter so the circuit-close log isn't counted.
        self.calls["info"] = 0
        return helper

    def _read(self, helper, address, count=2):
        from pymodbus.pdu.register_message import ReadHoldingRegistersRequest

        request = ReadHoldingRegistersRequest(address=address, count=count, dev_id=1, transaction_id=7)
        helper.on_pdu(False, request)  # inbound request pass

    def test_watched_read_is_counted_and_logged(self):
        helper = self._closed_helper(served_device_ids={1}, log_read_addresses={50000})
        self._read(helper, 50000)

        self.assertEqual(helper.watched_read_counts[50000], 1)
        self.assertEqual(self.calls["info"], 1)

    def test_repeated_watched_reads_are_counted_but_log_is_rate_limited(self):
        helper = self._closed_helper(served_device_ids={1}, log_read_addresses={50000})

        for _ in range(5):
            self._read(helper, 50000)

        # All reads counted, but only the first logs (rate-limited window).
        self.assertEqual(helper.watched_read_counts[50000], 5)
        self.assertEqual(self.calls["info"], 1)

    def test_unwatched_address_is_not_counted_or_logged(self):
        helper = self._closed_helper(served_device_ids={1}, log_read_addresses={50000})
        self._read(helper, 40071)

        self.assertNotIn(40071, helper.watched_read_counts)
        self.assertEqual(self.calls["info"], 0)

    def test_no_watch_addresses_configured_is_a_noop(self):
        helper = self._closed_helper(served_device_ids={1})  # no log_read_addresses
        self._read(helper, 50000)

        self.assertEqual(helper.watched_read_counts, {})
        self.assertEqual(self.calls["info"], 0)


if __name__ == "__main__":
    unittest.main()
