import unittest
from unittest.mock import patch

from app.carlo_gavazzi.meter_data import MeterData, OtherEnergies, PhaseData, SystemData
from tests.carlo_gavazzi.em540_data_test import (
    build_dynamic_registers,
    build_energy_registers,
)

# ---------------------------------------------------------------------------
# TestPhaseData – Requirements 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8
# ---------------------------------------------------------------------------


class TestPhaseData(unittest.TestCase):
    """Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8"""

    # --- Requirement 1.1: voltage L-N at phase_idx*2 + 0x0000, INT32, /10 ---

    def test_parse_phase_voltages_ln(self):
        """Requirement 1.1 – voltage L-N parsed from correct offset with /10 weight."""
        ln_values = (2300, 2310, 2320)  # Volt * 10 → 230.0, 231.0, 232.0
        regs = build_dynamic_registers(phase_voltages_ln=ln_values)

        for phase_idx, raw in enumerate(ln_values):
            phase = PhaseData()
            phase.parse(phase_idx, regs)
            self.assertAlmostEqual(
                phase.line_neutral_voltage,
                raw / 10.0,
                msg=f"Phase {phase_idx} voltage L-N mismatch",
            )

    # --- Requirement 1.2: voltage L-L at phase_idx*2 + 0x0006, INT32, /10 ---

    def test_parse_phase_voltages_ll(self):
        """Requirement 1.2 – voltage L-L parsed from correct offset with /10 weight."""
        ll_values = (3990, 4000, 4010)  # Volt * 10 → 399.0, 400.0, 401.0
        regs = build_dynamic_registers(phase_voltages_ll=ll_values)

        for phase_idx, raw in enumerate(ll_values):
            phase = PhaseData()
            phase.parse(phase_idx, regs)
            self.assertAlmostEqual(
                phase.line_line_voltage,
                raw / 10.0,
                msg=f"Phase {phase_idx} voltage L-L mismatch",
            )

    # --- Requirement 1.3: current at phase_idx*2 + 0x000C, INT32, /1000 ---

    def test_parse_phase_currents(self):
        """Requirement 1.3 – current parsed from correct offset with /1000 weight."""
        current_values = (10500, 11000, 10800)  # Ampere * 1000 → 10.5, 11.0, 10.8
        regs = build_dynamic_registers(phase_currents=current_values)

        for phase_idx, raw in enumerate(current_values):
            phase = PhaseData()
            phase.parse(phase_idx, regs)
            self.assertAlmostEqual(
                phase.current,
                raw / 1000.0,
                msg=f"Phase {phase_idx} current mismatch",
            )

    # --- Requirement 1.4: power at phase_idx*2 + 0x0012, INT32, /10 ---

    def test_parse_phase_power(self):
        """Requirement 1.4 – power parsed from correct offset with /10 weight."""
        power_values = (2415, 2530, 2480)  # Watt * 10 → 241.5, 253.0, 248.0
        regs = build_dynamic_registers(phase_powers=power_values)

        for phase_idx, raw in enumerate(power_values):
            phase = PhaseData()
            phase.parse(phase_idx, regs)
            self.assertAlmostEqual(
                phase.power,
                raw / 10.0,
                msg=f"Phase {phase_idx} power mismatch",
            )

    # --- Requirement 1.5: apparent power at phase_idx*2 + 0x0018, INT32, /10 ---

    def test_parse_phase_apparent_power(self):
        """Requirement 1.5 – apparent power parsed from correct offset with /10 weight."""
        apparent_values = (2500, 2600, 2550)  # VA * 10 → 250.0, 260.0, 255.0
        regs = build_dynamic_registers(phase_apparent=apparent_values)

        for phase_idx, raw in enumerate(apparent_values):
            phase = PhaseData()
            phase.parse(phase_idx, regs)
            self.assertAlmostEqual(
                phase.apparent_power,
                raw / 10.0,
                msg=f"Phase {phase_idx} apparent power mismatch",
            )

    # --- Requirement 1.6: reactive power at phase_idx*2 + 0x001E, INT32, /10 ---

    def test_parse_phase_reactive_power(self):
        """Requirement 1.6 – reactive power parsed from correct offset with /10 weight."""
        reactive_values = (500, 520, 510)  # var * 10 → 50.0, 52.0, 51.0
        regs = build_dynamic_registers(phase_reactive=reactive_values)

        for phase_idx, raw in enumerate(reactive_values):
            phase = PhaseData()
            phase.parse(phase_idx, regs)
            self.assertAlmostEqual(
                phase.reactive_power,
                raw / 10.0,
                msg=f"Phase {phase_idx} reactive power mismatch",
            )

    # --- Requirement 1.7: power factor at phase_idx + 0x002E, INT16, /1000 ---

    def test_parse_phase_power_factor(self):
        """Requirement 1.7 – power factor parsed from correct offset with /1000 weight."""
        pf_values = (980, 970, 975)  # PF * 1000 → 0.980, 0.970, 0.975
        regs = build_dynamic_registers(phase_pfs=pf_values)

        for phase_idx, raw in enumerate(pf_values):
            phase = PhaseData()
            phase.parse(phase_idx, regs)
            self.assertAlmostEqual(
                phase.power_factor,
                raw / 1000.0,
                msg=f"Phase {phase_idx} power factor mismatch",
            )

    # --- Requirement 1.1-1.7: all fields parsed correctly for all three phases ---

    def test_parse_all_three_phases(self):
        """Requirements 1.1-1.7 – all 7 fields correct for each of the 3 phases."""
        ln = (2300, 2310, 2320)
        ll = (3990, 4000, 4010)
        currents = (10500, 11000, 10800)
        powers = (2415, 2530, 2480)
        apparent = (2500, 2600, 2550)
        reactive = (500, 520, 510)
        pfs = (980, 970, 975)

        regs = build_dynamic_registers(
            phase_voltages_ln=ln,
            phase_voltages_ll=ll,
            phase_currents=currents,
            phase_powers=powers,
            phase_apparent=apparent,
            phase_reactive=reactive,
            phase_pfs=pfs,
        )

        for phase_idx in range(3):
            phase = PhaseData()
            phase.parse(phase_idx, regs)

            self.assertAlmostEqual(phase.line_neutral_voltage, ln[phase_idx] / 10.0)
            self.assertAlmostEqual(phase.line_line_voltage, ll[phase_idx] / 10.0)
            self.assertAlmostEqual(phase.current, currents[phase_idx] / 1000.0)
            self.assertAlmostEqual(phase.power, powers[phase_idx] / 10.0)
            self.assertAlmostEqual(phase.apparent_power, apparent[phase_idx] / 10.0)
            self.assertAlmostEqual(phase.reactive_power, reactive[phase_idx] / 10.0)
            self.assertAlmostEqual(phase.power_factor, pfs[phase_idx] / 1000.0)

    # --- Requirement 1.8: negative power values produce negative output ---

    def test_negative_power_values(self):
        """Requirement 1.8 – negative power values produce negative floating-point output."""
        neg_powers = (-5000, -3200, -4100)  # Watt * 10 → -500.0, -320.0, -410.0
        regs = build_dynamic_registers(phase_powers=neg_powers)

        for phase_idx, raw in enumerate(neg_powers):
            phase = PhaseData()
            phase.parse(phase_idx, regs)
            self.assertAlmostEqual(
                phase.power,
                raw / 10.0,
                msg=f"Phase {phase_idx} negative power mismatch",
            )
            self.assertLess(
                phase.power,
                0,
                msg=f"Phase {phase_idx} power should be negative",
            )


# ---------------------------------------------------------------------------
# TestSystemData – Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7
# ---------------------------------------------------------------------------


class TestSystemData(unittest.TestCase):
    """Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7"""

    # --- Requirement 2.1: system voltage L-N at 0x024, INT32, /10 ---

    def test_parse_system_voltage_ln(self):
        """Requirement 2.1 – system voltage L-N parsed from offset 0x024 with /10 weight."""
        raw = 2310  # Volt * 10 → 231.0
        regs = build_dynamic_registers(sys_voltage_ln=raw)
        system = SystemData()
        system.parse(regs)
        self.assertAlmostEqual(system.line_neutral_voltage, raw / 10.0)

    # --- Requirement 2.2: system voltage L-L at 0x026, INT32, /10 ---

    def test_parse_system_voltage_ll(self):
        """Requirement 2.2 – system voltage L-L parsed from offset 0x026 with /10 weight."""
        raw = 4000  # Volt * 10 → 400.0
        regs = build_dynamic_registers(sys_voltage_ll=raw)
        system = SystemData()
        system.parse(regs)
        self.assertAlmostEqual(system.line_line_voltage, raw / 10.0)

    # --- Requirement 2.3: system power at 0x028, INT32, /10 ---

    def test_parse_system_power(self):
        """Requirement 2.3 – system power parsed from offset 0x028 with /10 weight."""
        raw = 7425  # Watt * 10 → 742.5
        regs = build_dynamic_registers(sys_power=raw)
        system = SystemData()
        system.parse(regs)
        self.assertAlmostEqual(system.power, raw / 10.0)

    # --- Requirement 2.4: system apparent power at 0x02A, INT32, /10 ---

    def test_parse_system_apparent_power(self):
        """Requirement 2.4 – system apparent power parsed from offset 0x02A with /10 weight."""
        raw = 7650  # VA * 10 → 765.0
        regs = build_dynamic_registers(sys_apparent=raw)
        system = SystemData()
        system.parse(regs)
        self.assertAlmostEqual(system.apparent_power, raw / 10.0)

    # --- Requirement 2.5: system reactive power at 0x02C, INT32, /10 ---

    def test_parse_system_reactive_power(self):
        """Requirement 2.5 – system reactive power parsed from offset 0x02C with /10 weight."""
        raw = 1530  # var * 10 → 153.0
        regs = build_dynamic_registers(sys_reactive=raw)
        system = SystemData()
        system.parse(regs)
        self.assertAlmostEqual(system.reactive_power, raw / 10.0)

    # --- Requirement 2.6: system power factor at 0x031, INT16, /1000 ---

    def test_parse_system_power_factor(self):
        """Requirement 2.6 – system power factor parsed from offset 0x031 with /1000 weight."""
        raw = 975  # PF * 1000 → 0.975
        regs = build_dynamic_registers(sys_pf=raw)
        system = SystemData()
        system.parse(regs)
        self.assertAlmostEqual(system.power_factor, raw / 1000.0)

    # --- Requirement 2.7: frequency at 0x033, INT16, /10 ---

    def test_parse_system_frequency(self):
        """Requirement 2.7 – frequency parsed from offset 0x033 with /10 weight."""
        raw = 500  # Hz * 10 → 50.0
        regs = build_dynamic_registers(frequency=raw)
        system = SystemData()
        system.parse(regs)
        self.assertAlmostEqual(system.frequency, raw / 10.0)


# ---------------------------------------------------------------------------
# TestOtherEnergies – Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8
# ---------------------------------------------------------------------------


class TestOtherEnergies(unittest.TestCase):
    """Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8"""

    # --- Requirement 3.1: kWh (+) total at 0x00, INT64, /1000 ---

    def test_parse_kwh_plus_total(self):
        """Requirement 3.1 – kWh (+) total parsed from offset 0x00 as INT64 /1000."""
        raw = 123456789  # Wh → 123456.789 kWh
        regs = build_energy_registers(kwh_plus_tot=raw)
        oe = OtherEnergies()
        oe.parse(regs)
        self.assertAlmostEqual(oe.kwh_plus_total, raw / 1000.0)

    # --- Requirement 3.2: kvarh (+) total at 0x04, INT64, /1000 ---

    def test_parse_kvarh_plus_total(self):
        """Requirement 3.2 – kvarh (+) total parsed from offset 0x04 as INT64 /1000."""
        raw = 98765432
        regs = build_energy_registers(kvarh_plus_tot=raw)
        oe = OtherEnergies()
        oe.parse(regs)
        self.assertAlmostEqual(oe.kvarh_plus_total, raw / 1000.0)

    # --- Requirement 3.3: per-phase kWh (+) at 0x10, 0x14, 0x18, INT64, /1000 ---

    def test_parse_kwh_per_phase(self):
        """Requirement 3.3 – per-phase kWh (+) parsed from offsets 0x10, 0x14, 0x18."""
        l1_raw, l2_raw, l3_raw = 40000000, 41000000, 42000000
        regs = build_energy_registers(kwh_plus_l1=l1_raw, kwh_plus_l2=l2_raw, kwh_plus_l3=l3_raw)
        oe = OtherEnergies()
        oe.parse(regs)
        self.assertAlmostEqual(oe.kwh_plus_l1, l1_raw / 1000.0)
        self.assertAlmostEqual(oe.kwh_plus_l2, l2_raw / 1000.0)
        self.assertAlmostEqual(oe.kwh_plus_l3, l3_raw / 1000.0)

    # --- Requirement 3.4: kWh (-) total at 0x1C, INT64, /1000 ---

    def test_parse_kwh_neg_total(self):
        """Requirement 3.4 – kWh (-) total parsed from offset 0x1C as INT64 /1000."""
        raw = 55555555
        regs = build_energy_registers(kwh_neg_tot=raw)
        oe = OtherEnergies()
        oe.parse(regs)
        self.assertAlmostEqual(oe.kwh_neg_total, raw / 1000.0)

    # --- Requirement 3.5: kvarh (-) total at 0x24, INT64, /1000 ---

    def test_parse_kvarh_neg_total(self):
        """Requirement 3.5 – kvarh (-) total parsed from offset 0x24 as INT64 /1000."""
        raw = 33333333
        regs = build_energy_registers(kvarh_neg_tot=raw)
        oe = OtherEnergies()
        oe.parse(regs)
        self.assertAlmostEqual(oe.kvarh_neg_total, raw / 1000.0)

    # --- Requirement 3.6: kVAh total at 0x2C, INT64, /1000 ---

    def test_parse_kvah_total(self):
        """Requirement 3.6 – kVAh total parsed from offset 0x2C as INT64 /1000."""
        raw = 77777777
        regs = build_energy_registers(kvah_tot=raw)
        oe = OtherEnergies()
        oe.parse(regs)
        self.assertAlmostEqual(oe.kvah_total, raw / 1000.0)

    # --- Requirement 3.7: run hour meters at 0x34, 0x36, INT32, /100 ---

    def test_parse_run_hour_meters(self):
        """Requirement 3.7 – run hour meters parsed from offsets 0x34, 0x36 as INT32 /100."""
        run_hour_raw = 123456  # hours * 100 → 1234.56 hours
        run_hour_neg_raw = 78901
        regs = build_energy_registers(run_hour_meter=run_hour_raw, run_hour_meter_neg=run_hour_neg_raw)
        oe = OtherEnergies()
        oe.parse(regs)
        self.assertAlmostEqual(oe.run_hour_meter, run_hour_raw / 100.0)
        self.assertAlmostEqual(oe.run_hour_meter_neg_kwh, run_hour_neg_raw / 100.0)

    # --- Requirement 3.8: frequency at 0x3C, INT32, /1000 ---

    def test_parse_frequency(self):
        """Requirement 3.8 – frequency parsed from offset 0x3C as INT32 /1000."""
        raw = 50000  # Hz * 1000 → 50.0 Hz
        regs = build_energy_registers(frequency_hz1000=raw)
        oe = OtherEnergies()
        oe.parse(regs)
        self.assertAlmostEqual(oe.frequency, raw / 1000.0)

    # --- Run hour life counter at 0x3E, INT32, /100 ---

    def test_parse_run_hour_life_counter(self):
        """Run hour life counter parsed from offset 0x3E as INT32 /100."""
        raw = 999999  # hours * 100 → 9999.99 hours
        regs = build_energy_registers(run_hour_life=raw)
        oe = OtherEnergies()
        oe.parse(regs)
        self.assertAlmostEqual(oe.run_hour_life_counter, raw / 100.0)


# ---------------------------------------------------------------------------
# TestMeterDataUpdateFromFrame – Requirements 4.1, 4.2, 4.3, 4.4
# ---------------------------------------------------------------------------


class TestMeterDataUpdateFromFrame(unittest.TestCase):
    """Validates: Requirements 4.1, 4.2, 4.3, 4.4"""

    def _prepare_meter_data(self, phase_currents=(10500, 11000, 10800)):
        """Helper: create a MeterData with known dynamic register values."""
        md = MeterData()
        dyn_regs = build_dynamic_registers(phase_currents=phase_currents)
        md.frame.dynamic_reg_map[0x0000].values = dyn_regs
        energy_regs = build_energy_registers()
        md.frame.dynamic_reg_map[0x0500].values = energy_regs
        return md

    # --- Requirement 4.1: update_from_frame parses all three phases ---

    def test_update_parses_all_three_phases(self):
        """Requirement 4.1 – update_from_frame parses all three phases."""
        currents = (10500, 11000, 10800)  # Ampere * 1000
        md = self._prepare_meter_data(phase_currents=currents)
        md.update_from_frame()

        for phase_idx, raw in enumerate(currents):
            self.assertAlmostEqual(
                md.phases[phase_idx].current,
                raw / 1000.0,
                msg=f"Phase {phase_idx} current not parsed correctly",
            )

    # --- Requirement 4.2: An equals sum of three phase currents ---

    def test_total_current_equals_sum_of_phase_currents(self):
        """Requirement 4.2 – total current An equals sum of three phase currents."""
        currents = (10500, 11000, 10800)  # Ampere * 1000
        md = self._prepare_meter_data(phase_currents=currents)
        md.update_from_frame()

        expected_an = sum(c / 1000.0 for c in currents)
        self.assertAlmostEqual(md.system.An, expected_an)

    # --- Requirement 4.3: timestamp is set after update ---

    def test_timestamp_set_after_update(self):
        """Requirement 4.3 – timestamp is set to current time after update."""
        md = self._prepare_meter_data()
        self.assertEqual(md.timestamp, 0)

        fake_ts = 1700000000.123
        with patch("app.carlo_gavazzi.meter_data.datetime") as mock_dt:
            mock_dt.now.return_value.timestamp.return_value = fake_ts
            md.update_from_frame()

        self.assertEqual(md.timestamp, fake_ts)

    # --- Requirement 4.4: remap_registers is called during update ---

    def test_remap_registers_called_during_update(self):
        """Requirement 4.4 – remap_registers is invoked before parsing."""
        md = self._prepare_meter_data()

        with patch.object(md.frame, "remap_registers", wraps=md.frame.remap_registers) as mock_remap:
            md.update_from_frame()
            mock_remap.assert_called_once()


if __name__ == "__main__":
    unittest.main()
