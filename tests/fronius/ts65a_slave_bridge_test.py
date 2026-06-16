import asyncio
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
from app.fronius.ts65a_slave_bridge import Ts65aSlaveBridge


class TestTs65aSlaveBridge(unittest.TestCase):
    def _make_config(self):
        config = MagicMock()
        config.host = "127.0.0.1"
        config.port = 0
        config.slave_id = 1
        config.update_timeout = 5.0
        config.log_level = "WARNING"
        config.smoothing_num_points = 3
        config.grid_feed_in_hard_limit = -1000
        return config

    def _build_bridge(self):
        with (
            patch("app.fronius.ts65a_slave_bridge.ModbusTcpServer"),
            patch("app.fronius.ts65a_slave_bridge.ModbusServerContext"),
            patch("app.fronius.ts65a_slave_bridge.ModbusSparseDataBlock") as mock_block_cls,
        ):
            mock_datablock = MagicMock()
            mock_block_cls.return_value = mock_datablock
            bridge = Ts65aSlaveBridge(self._make_config())

        return bridge, mock_datablock

    def test_dynamic_register_buffer_matches_payload_size(self):
        bridge, _ = self._build_bridge()

        self.assertEqual(len(bridge._dynamic_values()), 45)
        self.assertEqual(len(bridge._dynamic_register_buffer), 90)

    def test_new_data_updates_full_dynamic_register_range(self):
        bridge, mock_datablock = self._build_bridge()
        data = MeterData()
        data._timestamp = 123.0
        data.system.An = 12.3
        data.system.line_neutral_voltage = 240.0
        data.system.line_line_voltage = 415.0
        data.system.frequency = 50.0
        data.system.power = 1000.0
        data.system.apparent_power = 1100.0
        data.system.reactive_power = 100.0
        data.system.power_factor = 0.9
        data.other_energies.kwh_neg_total = 1.0
        data.other_energies.kwh_plus_total = 2.0
        data.other_energies.kwh_plus_l1 = 3.0
        data.other_energies.kwh_plus_l2 = 4.0
        data.other_energies.kwh_plus_l3 = 5.0

        phase_values = [
            SimpleNamespace(
                current=4.1,
                line_neutral_voltage=239.0,
                line_line_voltage=414.0,
                power=300.0,
                apparent_power=330.0,
                reactive_power=30.0,
                power_factor=0.91,
            ),
            SimpleNamespace(
                current=4.2,
                line_neutral_voltage=240.0,
                line_line_voltage=415.0,
                power=333.0,
                apparent_power=366.0,
                reactive_power=33.0,
                power_factor=0.92,
            ),
            SimpleNamespace(
                current=4.0,
                line_neutral_voltage=241.0,
                line_line_voltage=416.0,
                power=367.0,
                apparent_power=404.0,
                reactive_power=37.0,
                power_factor=0.93,
            ),
        ]
        data.phases = phase_values

        def _fake_convert_to_registers(value, _datatype):
            encoded = int(abs(value)) % 65536
            return [encoded, encoded]

        with patch(
            "app.fronius.ts65a_slave_bridge.ModbusTcpClient.convert_to_registers",
            side_effect=_fake_convert_to_registers,
        ):
            asyncio.run(bridge.new_data(data))

        mock_datablock.setValues.assert_called_once()
        start_address, registers = mock_datablock.setValues.call_args.args
        self.assertEqual(start_address, 40072)
        self.assertEqual(len(registers), len(bridge._dynamic_values()) * 2)

    def test_voltage_phase_ca_uses_phase_c_line_line_voltage(self):
        bridge, mock_datablock = self._build_bridge()
        data = MeterData()
        data._timestamp = 123.0
        data.system.An = 12.3
        data.system.line_neutral_voltage = 240.0
        data.system.line_line_voltage = 415.0
        data.system.frequency = 50.0
        data.system.power = 1000.0
        data.system.apparent_power = 1100.0
        data.system.reactive_power = 100.0
        data.system.power_factor = 0.9

        data.phases = [
            SimpleNamespace(
                current=4.1,
                line_neutral_voltage=239.0,
                line_line_voltage=414.0,
                power=300.0,
                apparent_power=330.0,
                reactive_power=30.0,
                power_factor=0.91,
            ),
            SimpleNamespace(
                current=4.2,
                line_neutral_voltage=240.0,
                line_line_voltage=415.0,
                power=333.0,
                apparent_power=366.0,
                reactive_power=33.0,
                power_factor=0.92,
            ),
            SimpleNamespace(
                current=4.0,
                line_neutral_voltage=241.0,
                line_line_voltage=777.0,
                power=367.0,
                apparent_power=404.0,
                reactive_power=37.0,
                power_factor=0.93,
            ),
        ]

        def _fake_convert_to_registers(value, _datatype):
            encoded = int(value)
            return [encoded, encoded + 1]

        with patch(
            "app.fronius.ts65a_slave_bridge.ModbusTcpClient.convert_to_registers",
            side_effect=_fake_convert_to_registers,
        ):
            asyncio.run(bridge.new_data(data))

        start_address, registers = mock_datablock.setValues.call_args.args
        self.assertEqual(start_address, 40072)

        phase_ca_index = 22
        self.assertEqual(registers[phase_ca_index], 777)
        self.assertEqual(registers[phase_ca_index + 1], 778)


if __name__ == "__main__":
    unittest.main()
