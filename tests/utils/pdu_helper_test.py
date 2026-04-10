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


if __name__ == "__main__":
    unittest.main()
