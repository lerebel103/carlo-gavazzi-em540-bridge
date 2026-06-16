import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from app.carlo_gavazzi.meter_data import MeterData
from app.fronius.ts65a_slave_bridge import Ts65aSlaveBridge, _build_ts65a_simdata


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
            patch("app.fronius.ts65a_slave_bridge.ModbusTcpServer") as mock_server_cls,
        ):
            mock_server = MagicMock()
            # Provide a backing register list that the bridge writes into directly
            registers = [0] * 65536
            mock_runtime = MagicMock()
            mock_runtime.block = {"x": (0, 65536, registers, [0] * 65536)}
            mock_server.context = MagicMock()
            mock_server.context.devices = {1: mock_runtime}
            mock_server_cls.return_value = mock_server
            bridge = Ts65aSlaveBridge(self._make_config())

        return bridge, registers

    def test_dynamic_register_buffer_matches_payload_size(self):
        bridge, _ = self._build_bridge()

        self.assertEqual(len(bridge._dynamic_values()), 45)
        self.assertEqual(len(bridge._dynamic_register_buffer), 90)

    def test_new_data_updates_full_dynamic_register_range(self):
        bridge, registers = self._build_bridge()
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

        # Verify registers were written at the dynamic start address
        start = bridge._dynamic_start_address
        written = registers[start : start + len(bridge._dynamic_values()) * 2]
        self.assertEqual(len(written), len(bridge._dynamic_values()) * 2)
        # At least some non-zero values should be present
        self.assertTrue(any(v != 0 for v in written))

    def test_voltage_phase_ca_uses_phase_c_line_line_voltage(self):
        bridge, registers = self._build_bridge()
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

        start = bridge._dynamic_start_address
        written = registers[start : start + len(bridge._dynamic_values()) * 2]

        phase_ca_index = 22
        self.assertEqual(written[phase_ca_index], 777)
        self.assertEqual(written[phase_ca_index + 1], 778)


class TestBuildTs65aSimdata(unittest.TestCase):
    """Validates that _build_ts65a_simdata produces a valid, non-overlapping SimDevice."""

    def test_simdevice_creates_without_overlap_errors(self):
        """SimDevice construction must not raise TypeError for overlapping addresses."""
        device = _build_ts65a_simdata(slave_id=1)
        self.assertEqual(device.id, 1)

    def test_key_addresses_are_present(self):
        """Critical SunSpec addresses must be present in the flattened register map."""
        device = _build_ts65a_simdata(slave_id=1)
        all_simdata = device.simdata
        addresses = set()
        if isinstance(all_simdata, list):
            for entry in all_simdata:
                addresses.add(entry.address)
        elif isinstance(all_simdata, tuple):
            for block in all_simdata:
                if isinstance(block, list):
                    for entry in block:
                        addresses.add(entry.address)

        # SunSpec marker (0-based): register 769 → address 768
        self.assertIn(768, addresses)
        # SunSpec Well-Known: register 1707 → address 1706
        self.assertIn(1706, addresses)
        # SunSpec ID: register 40001 → address 40000
        self.assertIn(40000, addresses)
        # Event: register 40194 → address 40193
        self.assertIn(40193, addresses)
        # End Block: register 40196 → address 40195
        self.assertIn(40195, addresses)
        # Scale factors start: register 40162 → address 40161
        self.assertIn(40161, addresses)

    def test_no_duplicate_addresses(self):
        """Flattened entries must have unique addresses (no overlaps)."""
        device = _build_ts65a_simdata(slave_id=1)
        all_simdata = device.simdata
        addresses = []
        if isinstance(all_simdata, list):
            addresses = [entry.address for entry in all_simdata]
        elif isinstance(all_simdata, tuple):
            for block in all_simdata:
                if isinstance(block, list):
                    addresses.extend(entry.address for entry in block)
        self.assertEqual(len(addresses), len(set(addresses)), "Duplicate addresses found")


if __name__ == "__main__":
    unittest.main()
