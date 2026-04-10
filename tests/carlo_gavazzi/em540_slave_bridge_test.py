"""Tests for Em540Slave (slave bridge) datastore updates.

Validates: Requirements 12.1, 12.2, 12.3, 12.4

The production module has transitive imports that may not resolve in every
pymodbus version (e.g. ModbusDeviceContext, ExcCodes).  We patch the
missing names *before* importing the module under test so the test suite
runs cleanly regardless of the installed pymodbus version.
"""

import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Patch pymodbus names that may be missing in the installed version
# ---------------------------------------------------------------------------
import pymodbus.datastore as _ds

if not hasattr(_ds, "ModbusDeviceContext"):
    _ds.ModbusDeviceContext = getattr(_ds, "ModbusSlaveContext", MagicMock())

import pymodbus.constants as _const

if not hasattr(_const, "ExcCodes"):
    _const.ExcCodes = SimpleNamespace(DEVICE_BUSY=6)

# Now safe to import the module under test and its dependencies
from app.carlo_gavazzi.em540_data import Em540Frame
from app.carlo_gavazzi.em540_slave_bridge import REG_OFFSET, Em540Slave
from app.carlo_gavazzi.meter_data import MeterData


class TestEm540Slave(unittest.TestCase):
    """Validates: Requirements 12.1, 12.2, 12.3, 12.4"""

    def _make_config(self):
        config = MagicMock()
        config.host = "127.0.0.1"
        config.rtu_port = 5020
        config.tcp_port = 5021
        config.slave_id = 1
        config.update_timeout = 5.0
        config.log_level = "WARNING"
        return config

    def _build_slave(self, frame=None):
        """Construct Em540Slave with server classes patched out.

        Returns (slave, mock_datablock).
        """
        if frame is None:
            frame = Em540Frame()
        config = self._make_config()

        with (
            patch("app.carlo_gavazzi.em540_slave_bridge.ModbusTcpServer"),
            patch("app.carlo_gavazzi.em540_slave_bridge.ModbusServerContext"),
            patch("app.carlo_gavazzi.em540_slave_bridge.ModbusSparseDataBlock") as mock_block_cls,
        ):
            mock_datablock = MagicMock()
            mock_block_cls.create.return_value = mock_datablock
            slave = Em540Slave(config, frame)

        return slave, mock_datablock

    # --- Requirement 12.4: REG_OFFSET is 1 ---

    def test_reg_offset_is_one(self):
        """Requirement 12.4 – REG_OFFSET constant equals 1."""
        self.assertEqual(REG_OFFSET, 1)

    # --- Requirement 12.4: constructor builds datablock with +1 offset ---

    def test_datablock_built_with_plus_one_offset(self):
        """Requirement 12.4 – all register addresses passed to datablock use +1 offset."""
        frame = Em540Frame()
        config = self._make_config()

        with (
            patch("app.carlo_gavazzi.em540_slave_bridge.ModbusTcpServer"),
            patch("app.carlo_gavazzi.em540_slave_bridge.ModbusServerContext"),
            patch("app.carlo_gavazzi.em540_slave_bridge.ModbusSparseDataBlock") as mock_block_cls,
        ):
            mock_block_cls.create.return_value = MagicMock()
            Em540Slave(config, frame)

            values_dict = mock_block_cls.create.call_args[0][0]

            for addr in frame.static_reg_map:
                self.assertIn(addr + REG_OFFSET, values_dict, f"Static {hex(addr)} missing at {hex(addr + REG_OFFSET)}")
            for addr in frame.dynamic_reg_map:
                self.assertIn(
                    addr + REG_OFFSET, values_dict, f"Dynamic {hex(addr)} missing at {hex(addr + REG_OFFSET)}"
                )
            for addr in frame.remapped_reg_map:
                self.assertIn(
                    addr + REG_OFFSET, values_dict, f"Remapped {hex(addr)} missing at {hex(addr + REG_OFFSET)}"
                )

    def test_datablock_prefills_contiguous_compatibility_range(self):
        """Contiguous EM540 range should be materialized to avoid sparse KeyError reads."""
        frame = Em540Frame()
        config = self._make_config()

        with (
            patch("app.carlo_gavazzi.em540_slave_bridge.ModbusTcpServer"),
            patch("app.carlo_gavazzi.em540_slave_bridge.ModbusServerContext"),
            patch("app.carlo_gavazzi.em540_slave_bridge.ModbusSparseDataBlock") as mock_block_cls,
        ):
            mock_block_cls.create.return_value = MagicMock()
            Em540Slave(config, frame)

            values_dict = mock_block_cls.create.call_args[0][0]

            for addr in range(0x0000, 0x0160 + 1):
                self.assertIn(addr + REG_OFFSET, values_dict, f"Missing compatibility register {hex(addr)}")

    # --- Requirement 12.1: new_data updates dynamic registers ---

    def test_new_data_updates_dynamic_registers(self):
        """Requirement 12.1 – setValues called for every dynamic register with +1 offset."""
        frame = Em540Frame()
        slave, mock_datablock = self._build_slave(frame)

        meter_data = MeterData()
        meter_data.frame = frame
        meter_data.frame.dynamic_reg_map[0x0000].values = [42] * 0x34
        meter_data.frame.dynamic_reg_map[0x0500].values = [7] * 0x40

        asyncio.run(slave.new_data(meter_data))

        calls = {c[0][0]: c[0][1] for c in mock_datablock.setValues.call_args_list}
        for addr in frame.dynamic_reg_map:
            expected_addr = addr + REG_OFFSET
            self.assertIn(expected_addr, calls, f"Dynamic {hex(addr)} not updated at {hex(expected_addr)}")
            self.assertEqual(calls[expected_addr], meter_data.frame.dynamic_reg_map[addr].values)

    # --- Requirement 12.2: new_data does not rewrite static registers ---

    def test_new_data_does_not_update_static_registers_when_unchanged(self):
        """Requirement 12.2 – static register values are not rewritten unless source static data changes."""
        frame = Em540Frame()
        slave, mock_datablock = self._build_slave(frame)

        meter_data = MeterData()
        meter_data.frame = frame

        asyncio.run(slave.new_data(meter_data))

        calls = {c[0][0]: c[0][1] for c in mock_datablock.setValues.call_args_list}
        for addr in frame.static_reg_map:
            expected_addr = addr + REG_OFFSET
            self.assertNotIn(
                expected_addr,
                calls,
                f"Static {hex(addr)} should not be rewritten at {hex(expected_addr)}",
            )

    # --- Requirement 12.3: new_data updates remapped registers ---

    def test_new_data_updates_remapped_registers(self):
        """Requirement 12.3 – setValues called for every remapped register with +1 offset."""
        frame = Em540Frame()
        slave, mock_datablock = self._build_slave(frame)

        meter_data = MeterData()
        meter_data.frame = frame
        for reg in meter_data.frame.remapped_reg_map.values():
            reg.values = [55] * len(reg.values)

        asyncio.run(slave.new_data(meter_data))

        calls = {c[0][0]: c[0][1] for c in mock_datablock.setValues.call_args_list}
        for addr in frame.remapped_reg_map:
            expected_addr = addr + REG_OFFSET
            self.assertIn(expected_addr, calls, f"Remapped {hex(addr)} not updated at {hex(expected_addr)}")
            self.assertEqual(calls[expected_addr], meter_data.frame.remapped_reg_map[addr].values)

    def test_new_data_resyncs_static_registers_when_source_static_changes(self):
        frame = Em540Frame()
        slave, mock_datablock = self._build_slave(frame)

        meter_data = MeterData()
        meter_data.frame = frame
        meter_data._timestamp = 123.0

        asyncio.run(slave.new_data(meter_data))
        mock_datablock.setValues.reset_mock()

        first_static_addr = next(iter(frame.static_reg_map))
        reg = frame.static_reg_map[first_static_addr]
        reg.values = [99] * len(reg.values)

        asyncio.run(slave.new_data(meter_data))

        calls = {c[0][0]: c[0][1] for c in mock_datablock.setValues.call_args_list}
        self.assertIn(first_static_addr + REG_OFFSET, calls)
        self.assertEqual(calls[first_static_addr + REG_OFFSET], frame.static_reg_map[first_static_addr].values)

    def test_new_data_keeps_circuit_open_until_static_sync(self):
        frame = Em540Frame()
        slave, _ = self._build_slave(frame)

        meter_data = MeterData()
        meter_data.frame = frame
        meter_data._timestamp = 123.0

        asyncio.run(slave.new_data(meter_data))

        self.assertTrue(slave._stats.circuit_breaker_open)

    def test_new_data_closes_circuit_once_static_sync_completed(self):
        frame = Em540Frame()
        slave, _ = self._build_slave(frame)

        meter_data = MeterData()
        meter_data.frame = frame
        meter_data._timestamp = 123.0

        first_static_addr = next(iter(frame.static_reg_map))
        reg = frame.static_reg_map[first_static_addr]
        reg.values = [42] * len(reg.values)

        asyncio.run(slave.new_data(meter_data))

        self.assertFalse(slave._stats.circuit_breaker_open)


if __name__ == "__main__":
    unittest.main()
