import unittest

from pymodbus.client import ModbusTcpClient

from app.carlo_gavazzi.em540_data import ZERO_FILL, Em540Frame, RegisterDefinition, register_remap

# ---------------------------------------------------------------------------
# Test helpers – encode values into Modbus register lists using pymodbus
# ---------------------------------------------------------------------------


def encode_int32_le(value: int) -> list:
    """Encode a signed 32-bit integer as two 16-bit registers in little-endian word order."""
    return ModbusTcpClient.convert_to_registers(value, ModbusTcpClient.DATATYPE.INT32, "little")


def encode_int16_le(value: int) -> list:
    """Encode a signed 16-bit integer as one 16-bit register."""
    return ModbusTcpClient.convert_to_registers(value, ModbusTcpClient.DATATYPE.INT16, "little")


def encode_int64_le(value: int) -> list:
    """Encode a signed 64-bit integer as four 16-bit registers in little-endian word order."""
    return ModbusTcpClient.convert_to_registers(value, ModbusTcpClient.DATATYPE.INT64, "little")


def build_dynamic_registers(
    phase_voltages_ln=(2300, 2310, 2320),  # Volt * 10
    phase_voltages_ll=(3990, 4000, 4010),  # Volt * 10
    phase_currents=(10500, 11000, 10800),  # Ampere * 1000
    phase_powers=(2415, 2530, 2480),  # Watt * 10
    phase_apparent=(2500, 2600, 2550),  # VA * 10
    phase_reactive=(500, 520, 510),  # var * 10
    sys_voltage_ln=2310,  # Volt * 10
    sys_voltage_ll=4000,  # Volt * 10
    sys_power=7425,  # Watt * 10
    sys_apparent=7650,  # VA * 10
    sys_reactive=1530,  # var * 10
    phase_pfs=(980, 970, 975),  # PF * 1000
    sys_pf=975,  # PF * 1000
    phase_seq=1,  # Phase sequence
    frequency=500,  # Hz * 10
) -> list:
    """Build a 0x34-length register array matching the 0x0000 dynamic block layout."""
    regs = [0] * 0x34

    # Phase voltages L-N (0x0000-0x0005)
    for i, v in enumerate(phase_voltages_ln):
        r = encode_int32_le(v)
        regs[i * 2], regs[i * 2 + 1] = r[0], r[1]

    # Phase voltages L-L (0x0006-0x000B)
    for i, v in enumerate(phase_voltages_ll):
        r = encode_int32_le(v)
        regs[0x06 + i * 2], regs[0x07 + i * 2] = r[0], r[1]

    # Phase currents (0x000C-0x0011)
    for i, v in enumerate(phase_currents):
        r = encode_int32_le(v)
        regs[0x0C + i * 2], regs[0x0D + i * 2] = r[0], r[1]

    # Phase powers (0x0012-0x0017)
    for i, v in enumerate(phase_powers):
        r = encode_int32_le(v)
        regs[0x12 + i * 2], regs[0x13 + i * 2] = r[0], r[1]

    # Phase apparent powers (0x0018-0x001D)
    for i, v in enumerate(phase_apparent):
        r = encode_int32_le(v)
        regs[0x18 + i * 2], regs[0x19 + i * 2] = r[0], r[1]

    # Phase reactive powers (0x001E-0x0023)
    for i, v in enumerate(phase_reactive):
        r = encode_int32_le(v)
        regs[0x1E + i * 2], regs[0x1F + i * 2] = r[0], r[1]

    # System voltages and powers (0x0024-0x002D)
    for base, val in [
        (0x24, sys_voltage_ln),
        (0x26, sys_voltage_ll),
        (0x28, sys_power),
        (0x2A, sys_apparent),
        (0x2C, sys_reactive),
    ]:
        r = encode_int32_le(val)
        regs[base], regs[base + 1] = r[0], r[1]

    # Phase power factors (0x002E-0x0030) – INT16
    for i, pf in enumerate(phase_pfs):
        regs[0x2E + i] = encode_int16_le(pf)[0]

    # System PF (0x0031) – INT16
    regs[0x31] = encode_int16_le(sys_pf)[0]

    # Phase sequence (0x0032) – INT16
    regs[0x32] = encode_int16_le(phase_seq)[0]

    # Frequency (0x0033) – INT16
    regs[0x33] = encode_int16_le(frequency)[0]

    return regs


# ---------------------------------------------------------------------------
# TestRegisterDefinition – Requirements 6.1, 6.2, 6.3
# ---------------------------------------------------------------------------


class TestRegisterDefinition(unittest.TestCase):
    """Validates: Requirements 6.1, 6.2, 6.3"""

    def test_values_length_enforcement(self):
        """Requirement 6.1 – setting values with matching length succeeds."""
        reg = RegisterDefinition("test", [0, 0, 0])
        reg.values = [1, 2, 3]
        self.assertEqual(reg.values, [1, 2, 3])

    def test_values_setter_rejects_wrong_length(self):
        """Requirement 6.2 – setting values with mismatched length raises ValueError."""
        reg = RegisterDefinition("test", [0, 0])
        with self.assertRaises(ValueError):
            reg.values = [1, 2, 3]

    def test_skip_n_read_default(self):
        """Requirement 6.3 – skip_n_read defaults to 0."""
        reg = RegisterDefinition("test", [0])
        self.assertEqual(reg.skip_n_read, 0)

    def test_skip_n_read_custom(self):
        """skip_n_read can be set to a custom value."""
        reg = RegisterDefinition("test", [0], skip_n_read=5)
        self.assertEqual(reg.skip_n_read, 5)

    def test_values_getter_returns_current_values(self):
        """Values property returns the stored list."""
        reg = RegisterDefinition("desc", [10, 20])
        self.assertEqual(reg.values, [10, 20])


if __name__ == "__main__":
    unittest.main()

# ---------------------------------------------------------------------------
# Helper to build a 0x0500-block register array (length 0x40)
# ---------------------------------------------------------------------------


def build_energy_registers(
    kwh_plus_tot=0,
    kvarh_plus_tot=0,
    kwh_plus_partial=0,
    kvarh_plus_partial=0,
    kwh_plus_l1=0,
    kwh_plus_l2=0,
    kwh_plus_l3=0,
    kwh_neg_tot=0,
    kwh_neg_partial=0,
    kvarh_neg_tot=0,
    kvarh_neg_partial=0,
    kvah_tot=0,
    kvah_partial=0,
    run_hour_meter=0,
    run_hour_meter_neg=0,
    run_hour_partial=0,
    run_hour_neg_partial=0,
    frequency_hz1000=0,
    run_hour_life=0,
) -> list:
    """Build a 0x40-length register array matching the 0x0500 dynamic block layout."""
    regs = [0] * 0x40
    # 13 consecutive INT64 values (4 regs each) starting at offset 0
    int64_vals = [
        kwh_plus_tot,
        kvarh_plus_tot,
        kwh_plus_partial,
        kvarh_plus_partial,
        kwh_plus_l1,
        kwh_plus_l2,
        kwh_plus_l3,
        kwh_neg_tot,
        kwh_neg_partial,
        kvarh_neg_tot,
        kvarh_neg_partial,
        kvah_tot,
        kvah_partial,
    ]
    for i, val in enumerate(int64_vals):
        encoded = encode_int64_le(val)
        base = i * 4
        regs[base : base + 4] = encoded

    # INT32 values for run hour meters (offsets 0x34, 0x36, 0x38, 0x3A from 0x500 base)
    for offset, val in [
        (0x34, run_hour_meter),
        (0x36, run_hour_meter_neg),
        (0x38, run_hour_partial),
        (0x3A, run_hour_neg_partial),
    ]:
        encoded = encode_int32_le(val)
        regs[offset], regs[offset + 1] = encoded[0], encoded[1]

    # Frequency at offset 0x3C (INT32, Hz*1000)
    encoded = encode_int32_le(frequency_hz1000)
    regs[0x3C], regs[0x3D] = encoded[0], encoded[1]

    # Run hour life counter at offset 0x3E (INT32)
    encoded = encode_int32_le(run_hour_life)
    regs[0x3E], regs[0x3F] = encoded[0], encoded[1]

    return regs


# ---------------------------------------------------------------------------
# TestEm540Frame – Requirements 7.1, 7.2, 7.3, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6
# ---------------------------------------------------------------------------


class TestEm540Frame(unittest.TestCase):
    """Validates: Requirements 7.1, 7.2, 7.3, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6"""

    def setUp(self):
        self.frame = Em540Frame()

    # --- Requirement 7.1: static_reg_map addresses ---

    def test_static_reg_map_contains_all_expected_addresses(self):
        """Requirement 7.1 – static_reg_map has all expected register addresses."""
        expected_addresses = {
            0x0302,
            0x000B,
            0x1002,
            0x1010,
            0x1012,
            0x1101,
            0x1103,
            0x1104,
            0x110B,
            0x1150,
            0x1200,
            0x1600,
            0x4100,
            0x4200,
            0x5000,
            0x5008,
            0x5012,
        }
        self.assertEqual(set(self.frame.static_reg_map.keys()), expected_addresses)

    # --- Requirement 7.2: dynamic_reg_map addresses and sizes ---

    def test_dynamic_reg_map_has_0x0000_with_length_0x34(self):
        """Requirement 7.2 – dynamic_reg_map[0x0000] has length 0x34."""
        self.assertIn(0x0000, self.frame.dynamic_reg_map)
        self.assertEqual(len(self.frame.dynamic_reg_map[0x0000].values), 0x34)

    def test_dynamic_reg_map_has_0x0500_with_length_0x40(self):
        """Requirement 7.2 – dynamic_reg_map[0x0500] has length 0x40."""
        self.assertIn(0x0500, self.frame.dynamic_reg_map)
        self.assertEqual(len(self.frame.dynamic_reg_map[0x0500].values), 0x40)

    # --- Requirement 7.3: remapped_reg_map contains all targets ---

    def test_remapped_reg_map_contains_all_remap_targets(self):
        """Requirement 7.3 – remapped_reg_map has all target addresses from register_remap."""
        for _src, target in register_remap:
            self.assertIn(target, self.frame.remapped_reg_map, f"Target {hex(target)} missing from remapped_reg_map")

    # --- Requirement 5.1: remap copies source to correct targets ---

    def test_remap_registers_phase_voltages(self):
        """Requirement 5.1 – remap copies phase voltage registers correctly."""
        regs = build_dynamic_registers(
            phase_voltages_ln=(2300, 2310, 2320),
            phase_voltages_ll=(3990, 4000, 4010),
        )
        self.frame.dynamic_reg_map[0x0000].values = regs
        self.frame.remap_registers()

        # V L1-N: source 0x0,0x1 → target 0x0120,0x0121
        self.assertEqual(self.frame.remapped_reg_map[0x0120].values[0], regs[0x0])
        self.assertEqual(self.frame.remapped_reg_map[0x0121].values[0], regs[0x1])
        # V L2-N: source 0x2,0x3 → target 0x012E,0x012F
        self.assertEqual(self.frame.remapped_reg_map[0x012E].values[0], regs[0x2])
        self.assertEqual(self.frame.remapped_reg_map[0x012F].values[0], regs[0x3])
        # V L3-N: source 0x4,0x5 → target 0x013C,0x013D
        self.assertEqual(self.frame.remapped_reg_map[0x013C].values[0], regs[0x4])
        self.assertEqual(self.frame.remapped_reg_map[0x013D].values[0], regs[0x5])

    def test_remap_registers_phase_currents(self):
        """Requirement 5.1 – remap copies phase current registers correctly."""
        regs = build_dynamic_registers(phase_currents=(10500, 11000, 10800))
        self.frame.dynamic_reg_map[0x0000].values = regs
        self.frame.remap_registers()

        # A L1: source 0x0C,0x0D → target 0x0122,0x0123
        self.assertEqual(self.frame.remapped_reg_map[0x0122].values[0], regs[0x0C])
        self.assertEqual(self.frame.remapped_reg_map[0x0123].values[0], regs[0x0D])
        # A L2: source 0x0E,0x0F → target 0x0130,0x0131
        self.assertEqual(self.frame.remapped_reg_map[0x0130].values[0], regs[0x0E])
        self.assertEqual(self.frame.remapped_reg_map[0x0131].values[0], regs[0x0F])
        # A L3: source 0x10,0x11 → target 0x013E,0x013F
        self.assertEqual(self.frame.remapped_reg_map[0x013E].values[0], regs[0x10])
        self.assertEqual(self.frame.remapped_reg_map[0x013F].values[0], regs[0x11])

    def test_remap_registers_phase_powers(self):
        """Requirement 5.1 – remap copies phase power registers correctly."""
        regs = build_dynamic_registers(phase_powers=(2415, 2530, 2480))
        self.frame.dynamic_reg_map[0x0000].values = regs
        self.frame.remap_registers()

        # W L1: source 0x12,0x13 → target 0x0124,0x0125
        self.assertEqual(self.frame.remapped_reg_map[0x0124].values[0], regs[0x12])
        self.assertEqual(self.frame.remapped_reg_map[0x0125].values[0], regs[0x13])
        # W L2: source 0x14,0x15 → target 0x0132,0x0133
        self.assertEqual(self.frame.remapped_reg_map[0x0132].values[0], regs[0x14])
        self.assertEqual(self.frame.remapped_reg_map[0x0133].values[0], regs[0x15])
        # W L3: source 0x16,0x17 → target 0x0140,0x0141
        self.assertEqual(self.frame.remapped_reg_map[0x0140].values[0], regs[0x16])
        self.assertEqual(self.frame.remapped_reg_map[0x0141].values[0], regs[0x17])

    # --- Requirement 5.2: ZERO_FILL entries produce zero ---

    def test_remap_zero_fill_entries(self):
        """Requirement 5.2 – ZERO_FILL source entries produce zero in remapped registers."""
        regs = build_dynamic_registers()
        self.frame.dynamic_reg_map[0x0000].values = regs
        self.frame.remap_registers()

        zero_fill_targets = [target for src, target in register_remap if src == ZERO_FILL]
        for target in zero_fill_targets:
            self.assertEqual(
                self.frame.remapped_reg_map[target].values[0], 0, f"ZERO_FILL target {hex(target)} should be 0"
            )

    # --- Requirement 5.3: energy counter INT64→INT32 /100 ---

    def test_remap_energy_counters_int64_to_int32(self):
        """Requirement 5.3 – energy counters converted from INT64 Wh to INT32 Wh/100."""
        kwh_wh = 123456789  # Wh value in INT64
        energy_regs = build_energy_registers(kwh_plus_tot=kwh_wh)
        self.frame.dynamic_reg_map[0x0500].values = energy_regs
        self.frame.remap_registers()

        # Expected: INT32 encoding of kwh_wh / 100
        expected = ModbusTcpClient.convert_to_registers(int(kwh_wh / 100), ModbusTcpClient.DATATYPE.INT32, "little")
        self.assertEqual(self.frame.remapped_reg_map[0x0034].values, expected)

    # --- Requirement 5.4: frequency INT32 Hz*1000 → INT16 Hz*10 ---

    def test_remap_frequency_conversion(self):
        """Requirement 5.4 – frequency converted from INT32 Hz*1000 to INT16 Hz*10."""
        freq_hz1000 = 50000  # 50.000 Hz * 1000
        energy_regs = build_energy_registers(frequency_hz1000=freq_hz1000)
        self.frame.dynamic_reg_map[0x0500].values = energy_regs
        self.frame.remap_registers()

        # Expected: INT16 encoding of 50000 / 100 = 500 (Hz*10)
        expected = ModbusTcpClient.convert_to_registers(
            int(freq_hz1000 / 100), ModbusTcpClient.DATATYPE.INT16, "little"
        )
        self.assertEqual(self.frame.remapped_reg_map[0x0033].values, expected)

    # --- Requirement 5.5: dual-mapped registers contain identical values ---

    def test_dual_mapped_registers_identical(self):
        """Requirement 5.5 – dual-mapped registers contain identical values."""
        kwh_wh = 987654321
        energy_regs = build_energy_registers(kwh_plus_tot=kwh_wh)
        self.frame.dynamic_reg_map[0x0500].values = energy_regs
        self.frame.remap_registers()

        # kWh (+) TOT: 0x0034 and 0x0112 should be identical
        self.assertEqual(
            self.frame.remapped_reg_map[0x0034].values,
            self.frame.remapped_reg_map[0x0112].values,
        )
        # Kvarh (+) TOT: 0x0036 and 0x0114
        self.assertEqual(
            self.frame.remapped_reg_map[0x0036].values,
            self.frame.remapped_reg_map[0x0114].values,
        )
        # kWh (-) TOT: 0x004E and 0x0116
        self.assertEqual(
            self.frame.remapped_reg_map[0x004E].values,
            self.frame.remapped_reg_map[0x0116].values,
        )
        # Run hour meter: 0x005A and 0x00FE
        self.assertEqual(
            self.frame.remapped_reg_map[0x005A].values,
            self.frame.remapped_reg_map[0x00FE].values,
        )
        # Run hour meter kWh(-): 0x005C and 0x00F6
        self.assertEqual(
            self.frame.remapped_reg_map[0x005C].values,
            self.frame.remapped_reg_map[0x00F6].values,
        )

    # --- Requirement 5.6: all targets populated after single invocation ---

    def test_remap_populates_all_register_remap_targets(self):
        """Requirement 5.6 – all register_remap targets populated after remap_registers()."""
        regs = build_dynamic_registers(
            phase_voltages_ln=(2300, 2310, 2320),
            phase_currents=(10500, 11000, 10800),
            phase_powers=(2415, 2530, 2480),
        )
        self.frame.dynamic_reg_map[0x0000].values = regs
        self.frame.remap_registers()

        for src, target in register_remap:
            if src == ZERO_FILL:
                self.assertEqual(
                    self.frame.remapped_reg_map[target].values[0], 0, f"ZERO_FILL target {hex(target)} not zero"
                )
            else:
                self.assertEqual(
                    self.frame.remapped_reg_map[target].values[0],
                    self.frame.dynamic_reg_map[0x0000].values[src],
                    f"Remap {hex(src)} → {hex(target)} mismatch",
                )

    # --- Run hour meter straight copies ---

    def test_remap_run_hour_meters(self):
        """Run hour meters are straight-copied from 0x0500 block."""
        run_hour = 12345
        run_hour_neg = 67890
        run_hour_partial = 11111
        run_hour_neg_partial = 22222
        energy_regs = build_energy_registers(
            run_hour_meter=run_hour,
            run_hour_meter_neg=run_hour_neg,
            run_hour_partial=run_hour_partial,
            run_hour_neg_partial=run_hour_neg_partial,
        )
        self.frame.dynamic_reg_map[0x0500].values = energy_regs
        self.frame.remap_registers()

        expected_run_hour = encode_int32_le(run_hour)
        expected_run_hour_neg = encode_int32_le(run_hour_neg)
        expected_run_hour_partial = encode_int32_le(run_hour_partial)
        expected_run_hour_neg_partial = encode_int32_le(run_hour_neg_partial)

        # 0x005A: Run hour meter
        self.assertEqual(self.frame.remapped_reg_map[0x005A].values, expected_run_hour)
        # 0x005C: Run hour meter kWh(-)
        self.assertEqual(self.frame.remapped_reg_map[0x005C].values, expected_run_hour_neg)
        # 0x006E: Run hour meter PARTIAL
        self.assertEqual(self.frame.remapped_reg_map[0x006E].values, expected_run_hour_partial)
        # 0x0070: Run hour meter kWh(-) PARTIAL
        self.assertEqual(self.frame.remapped_reg_map[0x0070].values, expected_run_hour_neg_partial)
