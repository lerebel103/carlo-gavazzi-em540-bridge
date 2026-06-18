"""Tests for Em540Slave (slave bridge) datastore updates.

Validates: Requirements 12.1, 12.2, 12.3, 12.4

The tests patch ModbusTcpServer to verify that the bridge correctly builds
SimData entries and writes register values directly into the register array.
"""

import asyncio
import unittest
from unittest.mock import MagicMock, patch

from pymodbus.simulator.simdevice import SimDevice

from app.carlo_gavazzi.em540_data import Em540Frame
from app.carlo_gavazzi.em540_slave_bridge import (
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
        """Construct Em540Slave with ModbusTcpServer patched out (but real SimDevice/SimCore).

        Returns (slave, mock_server) — the slave has a real register array that
        new_data() writes into directly.
        """
        if frame is None:
            frame = Em540Frame()
        config = self._make_config()

        # Pre-build the real SimCore so the mock server can expose it via .context
        from pymodbus.simulator.simcore import SimCore

        simdata = _build_simdata(frame)
        real_device = SimDevice(config.slave_id, simdata=simdata)
        real_sim_core = SimCore(real_device)

        with patch("app.carlo_gavazzi.em540_slave_bridge.ModbusTcpServer") as mock_server_cls:
            rtu_mock = MagicMock()
            rtu_mock.context = real_sim_core

            tcp_mock = MagicMock()
            tcp_mock.context = real_sim_core

            mock_server_cls.side_effect = [rtu_mock, tcp_mock]
            slave = Em540Slave(config, frame)

        return slave, rtu_mock

    def test_rtu_and_tcp_servers_share_context(self):
        """RTU and TCP servers must share the same SimCore context for coherent register state.

        Since both servers are constructed with the same sim_core instance,
        and _tcp_server.context is explicitly set to _rtu_server.context,
        they always serve identical register state.
        """
        frame = Em540Frame()
        slave, _ = self._build_slave(frame)

        # Both server references point to the same context object
        self.assertIs(slave._tcp_server.context, slave._rtu_server.context)

    # --- Requirement 12.4: REG_OFFSET is 0 ---

    def test_reg_offset_is_zero(self):
        """Requirement 12.4 – REG_OFFSET constant equals 0 (SimDevice uses 0-based addresses)."""
        self.assertEqual(REG_OFFSET, 0)

    # --- Requirement 12.4: constructor builds simdata with REG_OFFSET applied ---

    def test_simdata_built_with_reg_offset(self):
        """Requirement 12.4 – all register addresses in SimData entries use REG_OFFSET (0-based protocol addresses)."""
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
        """Requirement 12.1 – registers updated for every dynamic register with REG_OFFSET applied."""
        frame = Em540Frame()
        slave, _ = self._build_slave(frame)

        meter_data = MeterData()
        meter_data.frame = frame
        meter_data.frame.dynamic_reg_map[0x0000].values = [42] * 0x34
        meter_data.frame.dynamic_reg_map[0x0500].values = [7] * 0x40

        asyncio.run(slave.new_data(meter_data))

        # Verify a subset of dynamic registers that are not overlapped by remapped registers.
        # The primary block at 0x0000 has remapped registers starting at 0x33, so check 0x00..0x32.
        offset = 0 + REG_OFFSET - slave._reg_start_address
        actual_primary = slave._registers[offset : offset + 0x33]
        self.assertEqual(actual_primary, [42] * 0x33)

        # Energy block at 0x0500 has no overlapping remapped registers
        energy_offset = 0x0500 + REG_OFFSET - slave._reg_start_address
        actual_energy = slave._registers[energy_offset : energy_offset + 0x40]
        self.assertEqual(actual_energy, [7] * 0x40)

    # --- Requirement 12.2: new_data does not rewrite static registers ---

    def test_new_data_does_not_update_static_registers_when_unchanged(self):
        """Requirement 12.2 – static register values are not rewritten unless source data changes."""
        frame = Em540Frame()
        slave, _ = self._build_slave(frame)

        meter_data = MeterData()
        meter_data.frame = frame

        # Snapshot registers before
        pre_snapshot: dict[int, list[int]] = {}
        for addr in frame.static_reg_map:
            offset = addr + REG_OFFSET - slave._reg_start_address
            length = len(frame.static_reg_map[addr].values)
            pre_snapshot[addr] = list(slave._registers[offset : offset + length])

        asyncio.run(slave.new_data(meter_data))

        # Static-only addresses (not overlapped by dynamic/remapped) should be unchanged
        for addr in frame.static_reg_map:
            if addr not in slave._overlapped_static_addrs:
                offset = addr + REG_OFFSET - slave._reg_start_address
                length = len(frame.static_reg_map[addr].values)
                actual = slave._registers[offset : offset + length]
                self.assertEqual(
                    actual,
                    pre_snapshot[addr],
                    f"Static {hex(addr)} should not be rewritten",
                )

    # --- Requirement 12.3: new_data updates remapped registers ---

    def test_new_data_updates_remapped_registers(self):
        """Requirement 12.3 – remapped registers are written correctly."""
        frame = Em540Frame()
        slave, _ = self._build_slave(frame)

        meter_data = MeterData()
        meter_data.frame = frame
        for reg in meter_data.frame.remapped_reg_map.values():
            reg.values = [55] * len(reg.values)

        asyncio.run(slave.new_data(meter_data))

        # Verify each remapped register was written into the register array
        for addr in frame.remapped_reg_map:
            offset = addr + REG_OFFSET - slave._reg_start_address
            reg_values = frame.remapped_reg_map[addr].values
            for i, expected_val in enumerate(reg_values):
                actual = slave._registers[offset + i]
                self.assertEqual(
                    actual,
                    expected_val,
                    f"Remapped {hex(addr)}+{i} not updated at offset {offset + i}",
                )

    def test_new_data_resyncs_static_registers_when_source_static_changes(self):
        frame = Em540Frame()
        slave, _ = self._build_slave(frame)

        meter_data = MeterData()
        meter_data.frame = frame
        meter_data._timestamp = 123.0

        asyncio.run(slave.new_data(meter_data))

        first_static_addr = next(iter(frame.static_reg_map))
        reg = frame.static_reg_map[first_static_addr]
        reg.values = [99] * len(reg.values)

        asyncio.run(slave.new_data(meter_data))

        offset = first_static_addr + REG_OFFSET - slave._reg_start_address
        actual = slave._registers[offset : offset + len(reg.values)]
        self.assertEqual(actual, reg.values)

    def test_new_data_preserves_overlapped_static_device_type_register(self):
        frame = Em540Frame()
        frame.static_reg_map[0x000B].values = [1744]
        slave, _ = self._build_slave(frame)

        meter_data = MeterData()
        meter_data.frame = frame
        meter_data._timestamp = 123.0
        meter_data.frame.dynamic_reg_map[0x0000].values = [0] * 0x34

        asyncio.run(slave.new_data(meter_data))

        offset = 0x000B + REG_OFFSET - slave._reg_start_address
        self.assertEqual(slave._registers[offset], 1744)

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
