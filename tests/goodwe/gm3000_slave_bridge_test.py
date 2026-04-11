import asyncio
import time
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pymodbus.datastore as _ds

if not hasattr(_ds, "ModbusDeviceContext"):
    _ds.ModbusDeviceContext = getattr(_ds, "ModbusSlaveContext", MagicMock())

import pymodbus.constants as _const

if not hasattr(_const, "ExcCodes"):
    _const.ExcCodes = SimpleNamespace(DEVICE_BUSY=6)

from app.carlo_gavazzi.meter_data import MeterData
from app.goodwe.gm3000_slave_bridge import (
    GOODWE_CT_RATIO_REGISTER,
    GOODWE_CT_RATIO_VALUE,
    GOODWE_DYNAMIC_START_REGISTER,
    GOODWE_METER_TYPE_REGISTER,
    GOODWE_METER_TYPE_VALUE,
    GoodweGm3000SlaveBridge,
)


def _make_pdu(function_code=3, dev_id=1, transaction_id=1):
    return SimpleNamespace(
        function_code=function_code,
        dev_id=dev_id,
        transaction_id=transaction_id,
        exception_code=0,
    )


class TestGoodweGm3000SlaveBridge(unittest.TestCase):
    def _make_config(self):
        config = MagicMock()
        config.host = "127.0.0.1"
        config.socket_port = 0
        config.rtu_port = 0
        config.slave_id = 3
        config.update_timeout = 5.0
        config.log_level = "WARNING"
        return config

    def _build_bridge(self):
        with (
            patch("app.goodwe.gm3000_slave_bridge.ModbusTcpServer"),
            patch("app.goodwe.gm3000_slave_bridge.ModbusServerContext"),
            patch("app.goodwe.gm3000_slave_bridge.ModbusSparseDataBlock") as mock_block_cls,
        ):
            mock_datablock = MagicMock()
            mock_block_cls.return_value = mock_datablock
            bridge = GoodweGm3000SlaveBridge(self._make_config())

        return bridge, mock_datablock

    def test_static_registers_are_set(self):
        with (
            patch("app.goodwe.gm3000_slave_bridge.ModbusTcpServer"),
            patch("app.goodwe.gm3000_slave_bridge.ModbusServerContext"),
            patch("app.goodwe.gm3000_slave_bridge.ModbusSparseDataBlock") as mock_block_cls,
        ):
            mock_block_cls.return_value = MagicMock()
            GoodweGm3000SlaveBridge(self._make_config())

        static_map = mock_block_cls.call_args.args[0]
        self.assertEqual(static_map[40142], [0])
        self.assertEqual(static_map[GOODWE_CT_RATIO_REGISTER], [GOODWE_CT_RATIO_VALUE])
        self.assertEqual(static_map[GOODWE_METER_TYPE_REGISTER], [GOODWE_METER_TYPE_VALUE])

    def test_circuit_starts_open_and_rejects_requests_until_data_arrives(self):
        bridge, _ = self._build_bridge()

        response = bridge._pdu_helper.on_pdu(True, _make_pdu())

        self.assertTrue(bridge._pdu_helper.circuit_open)
        self.assertEqual(bridge._pdu_helper.dropped_request_count, 1)
        self.assertEqual(getattr(response, "exception_code", None), 6)

    def test_fresh_new_data_closes_circuit_and_allows_requests(self):
        bridge, mock_datablock = self._build_bridge()

        data = MeterData()
        data._timestamp = time.time()
        data.phases = [
            SimpleNamespace(line_neutral_voltage=230.1, current=5.01, power=-1000.0),
            SimpleNamespace(line_neutral_voltage=231.2, current=6.02, power=1500.0),
            SimpleNamespace(line_neutral_voltage=232.3, current=7.03, power=0.0),
        ]

        asyncio.run(bridge.new_data(data))
        response = bridge._pdu_helper.on_pdu(True, _make_pdu())

        mock_datablock.setValues.assert_called_once()
        self.assertFalse(bridge._pdu_helper.circuit_open)
        self.assertIs(response.function_code, 3)
        self.assertEqual(bridge._pdu_helper.dropped_request_count, 0)

    def test_read_failed_reopens_circuit_and_blocks_requests(self):
        bridge, _ = self._build_bridge()

        data = MeterData()
        data._timestamp = time.time()
        data.phases = [
            SimpleNamespace(line_neutral_voltage=230.1, current=5.01, power=-1000.0),
            SimpleNamespace(line_neutral_voltage=231.2, current=6.02, power=1500.0),
            SimpleNamespace(line_neutral_voltage=232.3, current=7.03, power=0.0),
        ]

        asyncio.run(bridge.new_data(data))
        self.assertFalse(bridge._pdu_helper.circuit_open)

        asyncio.run(bridge.read_failed())
        response = bridge._pdu_helper.on_pdu(True, _make_pdu())

        self.assertTrue(bridge._pdu_helper.circuit_open)
        self.assertEqual(getattr(response, "exception_code", None), 6)

    def test_new_data_updates_live_register_block(self):
        bridge, mock_datablock = self._build_bridge()

        data = MeterData()
        data._timestamp = 123.0
        data.phases = [
            SimpleNamespace(line_neutral_voltage=230.1, current=5.01, power=-1000.0),
            SimpleNamespace(line_neutral_voltage=231.2, current=6.02, power=1500.0),
            SimpleNamespace(line_neutral_voltage=232.3, current=7.03, power=0.0),
        ]

        asyncio.run(bridge.new_data(data))

        mock_datablock.setValues.assert_called_once()
        start_addr, regs = mock_datablock.setValues.call_args.args
        self.assertEqual(start_addr, GOODWE_DYNAMIC_START_REGISTER)
        self.assertEqual(len(regs), 17)

        self.assertEqual(regs[0], 2301)
        self.assertEqual(regs[1], 2312)
        self.assertEqual(regs[2], 2323)

        self.assertEqual(regs[3], 0)
        self.assertEqual(regs[4], 501)
        self.assertEqual(regs[5], 0)
        self.assertEqual(regs[6], 602)
        self.assertEqual(regs[7], 0)
        self.assertEqual(regs[8], 703)

        self.assertEqual(regs[9], 0)
        self.assertEqual(regs[10], 1000)
        self.assertEqual(regs[11], 0xFFFF)
        self.assertEqual(regs[12], 64036)
        self.assertEqual(regs[13], 0)
        self.assertEqual(regs[14], 0)
        self.assertEqual(regs[15], 0xFFFF)
        self.assertEqual(regs[16], 65036)

    def test_power_sign_words_follow_export_positive_import_negative_rule(self):
        bridge, mock_datablock = self._build_bridge()

        data = MeterData()
        data._timestamp = 125.0
        data.phases = [
            SimpleNamespace(line_neutral_voltage=230.0, current=1.0, power=-250.0),
            SimpleNamespace(line_neutral_voltage=230.0, current=1.0, power=400.0),
            SimpleNamespace(line_neutral_voltage=230.0, current=1.0, power=-100.0),
        ]

        asyncio.run(bridge.new_data(data))

        _, regs = mock_datablock.setValues.call_args.args
        self.assertEqual(regs[9], 0)
        self.assertEqual(regs[10], 250)
        self.assertEqual(regs[11], 0xFFFF)
        self.assertEqual(regs[12], 65136)
        self.assertEqual(regs[13], 0)
        self.assertEqual(regs[14], 100)
        self.assertEqual(regs[15], 0xFFFF)
        self.assertEqual(regs[16], 65486)

    def test_new_data_clamps_current_and_power_to_register_range(self):
        bridge, mock_datablock = self._build_bridge()

        data = MeterData()
        data._timestamp = 124.0
        data.phases = [
            SimpleNamespace(line_neutral_voltage=-5.0, current=-10.0, power=-100000.0),
            SimpleNamespace(line_neutral_voltage=99999.0, current=99999.0, power=100000.0),
            SimpleNamespace(line_neutral_voltage=230.0, current=0.0, power=32767.0),
        ]

        asyncio.run(bridge.new_data(data))

        _, regs = mock_datablock.setValues.call_args.args

        self.assertEqual(regs[0], 0)
        self.assertEqual(regs[1], 65535)

        self.assertEqual(regs[4], 0)
        self.assertEqual(regs[6], 65535)

        self.assertEqual(regs[9], 0)
        self.assertEqual(regs[10], 32767)
        self.assertEqual(regs[11], 0xFFFF)
        self.assertEqual(regs[12], 32768)
        self.assertEqual(regs[13], 0xFFFF)
        self.assertEqual(regs[14], 32769)
        self.assertEqual(regs[15], 0xFFFF)
        self.assertEqual(regs[16], 32768)


if __name__ == "__main__":
    unittest.main()
