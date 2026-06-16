"""Tests for Em540Slave (slave bridge) datastore updates.

Validates: Requirements 12.1, 12.2, 12.3, 12.4

The tests patch ModbusTcpServer and SimDevice to verify that the bridge
correctly builds SimData entries and updates registers via async_setValues.
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from app.carlo_gavazzi.em540_data import Em540Frame
from app.carlo_gavazzi.em540_slave_bridge import (
    _FC_HOLDING_REGISTER,
    REG_OFFSET,
    Em540Slave,
    _build_simdata,
)
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
        """Construct Em540Slave with server and SimDevice patched out.

        Returns (slave, mock_rtu_server).
        """
        if frame is None:
            frame = Em540Frame()
        config = self._make_config()

        with (
            patch("app.carlo_gavazzi.em540_slave_bridge.ModbusTcpServer") as mock_server_cls,
            patch("app.carlo_gavazzi.em540_slave_bridge.SimDevice"),
        ):
            mock_server = MagicMock()
            mock_server.async_setValues = AsyncMock()
            mock_server.context = MagicMock()
            mock_server_cls.return_value = mock_server
            slave = Em540Slave(config, frame)

        return slave, mock_server

    # --- Requirement 12.4: REG_OFFSET is 1 ---

    def test_reg_offset_is_one(self):
        """Requirement 12.4 – REG_OFFSET constant equals 1."""
        self.assertEqual(REG_OFFSET, 1)

    # --- Requirement 12.4: constructor builds simdata with +1 offset ---

    def test_simdata_built_with_plus_one_offset(self):
        """Requirement 12.4 – all register addresses in SimData entries use +1 offset."""
        frame = Em540Frame()
        simdata = _build_simdata(frame)
        addresses = {entry.address for entry in simdata}

        for addr in frame.static_reg_map:
            for i in range(len(frame.static_reg_map[addr].values)):
                self.assertIn(
                    addr + REG_OFFSET + i,
                    addresses,
                    f"Static {hex(addr)}+{i} missing at {hex(addr + REG_OFFSET + i)}",
                )
        for addr in frame.dynamic_reg_map:
            for i in range(len(frame.dynamic_reg_map[addr].values)):
                self.assertIn(
                    addr + REG_OFFSET + i,
                    addresses,
                    f"Dynamic {hex(addr)}+{i} missing at {hex(addr + REG_OFFSET + i)}",
                )
        for addr in frame.remapped_reg_map:
            for i in range(len(frame.remapped_reg_map[addr].values)):
                self.assertIn(
                    addr + REG_OFFSET + i,
                    addresses,
                    f"Remapped {hex(addr)}+{i} missing at {hex(addr + REG_OFFSET + i)}",
                )

    def test_simdata_prefills_contiguous_compatibility_range(self):
        """Contiguous EM540 range should be materialized to avoid illegal-address exceptions."""
        frame = Em540Frame()
        simdata = _build_simdata(frame)
        addresses = {entry.address for entry in simdata}

        for addr in range(0x0000, 0x0160 + 1):
            self.assertIn(addr + REG_OFFSET, addresses, f"Missing compatibility register {hex(addr)}")

    def test_simdata_has_no_duplicate_addresses(self):
        """SimDevice requires non-overlapping entries — verify no duplicate addresses."""
        frame = Em540Frame()
        simdata = _build_simdata(frame)
        addresses = [entry.address for entry in simdata]
        self.assertEqual(len(addresses), len(set(addresses)), "Duplicate addresses in simdata")

    # --- Requirement 12.1: new_data updates dynamic registers ---

    def test_new_data_updates_dynamic_registers(self):
        """Requirement 12.1 – async_setValues called for every dynamic register with +1 offset."""
        frame = Em540Frame()
        slave, mock_server = self._build_slave(frame)

        meter_data = MeterData()
        meter_data.frame = frame
        meter_data.frame.dynamic_reg_map[0x0000].values = [42] * 0x34
        meter_data.frame.dynamic_reg_map[0x0500].values = [7] * 0x40

        asyncio.run(slave.new_data(meter_data))

        calls = {
            c.args[2]: c.args[3]
            for c in mock_server.async_setValues.call_args_list
            if c.args[1] == _FC_HOLDING_REGISTER
        }
        for addr in frame.dynamic_reg_map:
            expected_addr = addr + REG_OFFSET
            self.assertIn(expected_addr, calls, f"Dynamic {hex(addr)} not updated at {hex(expected_addr)}")
            self.assertEqual(calls[expected_addr], meter_data.frame.dynamic_reg_map[addr].values)

    # --- Requirement 12.2: new_data does not rewrite static registers ---

    def test_new_data_does_not_update_static_registers_when_unchanged(self):
        """Requirement 12.2 – static register values are not rewritten unless source data changes."""
        frame = Em540Frame()
        slave, mock_server = self._build_slave(frame)

        meter_data = MeterData()
        meter_data.frame = frame

        asyncio.run(slave.new_data(meter_data))

        calls = {
            c.args[2]: c.args[3]
            for c in mock_server.async_setValues.call_args_list
            if c.args[1] == _FC_HOLDING_REGISTER
        }
        for addr in frame.static_reg_map:
            expected_addr = addr + REG_OFFSET
            # Static addrs that are overlapped by dynamic/remapped will be refreshed,
            # but pure static-only addrs should NOT be updated
            if addr not in slave._overlapped_static_addrs:
                self.assertNotIn(
                    expected_addr,
                    calls,
                    f"Static {hex(addr)} should not be rewritten at {hex(expected_addr)}",
                )

    # --- Requirement 12.3: new_data updates remapped registers ---

    def test_new_data_updates_remapped_registers(self):
        """Requirement 12.3 – async_setValues called for every remapped register with +1 offset."""
        frame = Em540Frame()
        slave, mock_server = self._build_slave(frame)

        meter_data = MeterData()
        meter_data.frame = frame
        for reg in meter_data.frame.remapped_reg_map.values():
            reg.values = [55] * len(reg.values)

        asyncio.run(slave.new_data(meter_data))

        calls = {
            c.args[2]: c.args[3]
            for c in mock_server.async_setValues.call_args_list
            if c.args[1] == _FC_HOLDING_REGISTER
        }
        for addr in frame.remapped_reg_map:
            expected_addr = addr + REG_OFFSET
            self.assertIn(expected_addr, calls, f"Remapped {hex(addr)} not updated at {hex(expected_addr)}")
            self.assertEqual(calls[expected_addr], meter_data.frame.remapped_reg_map[addr].values)

    def test_new_data_resyncs_static_registers_when_source_static_changes(self):
        frame = Em540Frame()
        slave, mock_server = self._build_slave(frame)

        meter_data = MeterData()
        meter_data.frame = frame
        meter_data._timestamp = 123.0

        asyncio.run(slave.new_data(meter_data))
        mock_server.async_setValues.reset_mock()

        first_static_addr = next(iter(frame.static_reg_map))
        reg = frame.static_reg_map[first_static_addr]
        reg.values = [99] * len(reg.values)

        asyncio.run(slave.new_data(meter_data))

        calls = {
            c.args[2]: c.args[3]
            for c in mock_server.async_setValues.call_args_list
            if c.args[1] == _FC_HOLDING_REGISTER
        }
        self.assertIn(first_static_addr + REG_OFFSET, calls)
        self.assertEqual(calls[first_static_addr + REG_OFFSET], frame.static_reg_map[first_static_addr].values)

    def test_new_data_preserves_overlapped_static_device_type_register(self):
        frame = Em540Frame()
        frame.static_reg_map[0x000B].values = [1744]
        slave, mock_server = self._build_slave(frame)

        meter_data = MeterData()
        meter_data.frame = frame
        meter_data._timestamp = 123.0
        meter_data.frame.dynamic_reg_map[0x0000].values = [0] * 0x34

        asyncio.run(slave.new_data(meter_data))

        calls = [(c.args[2], c.args[3]) for c in mock_server.async_setValues.call_args_list]
        self.assertIn((0x000B + REG_OFFSET, [1744]), calls)

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
