"""Unit tests for the pure spec-grounded frame validator.

Fixtures build a physically realistic primary-block register list (0x34 words)
and parse it through MeterData so the decoded floats and the raw register list
agree, per AGENTS.md "Register Layout Invariants" (L-L voltage ~400 V, L-N
~230 V, current ~10 A, frequency ~50 Hz). This keeps the 0x000B MSW at 0 so no
false Device-Type collision is manufactured.
"""

import unittest

from app.carlo_gavazzi.frame_validation import (
    _PRIMARY_INT32_MSW,
    validate_primary_frame,
)
from app.carlo_gavazzi.meter_data import MeterData

PRIMARY_BLOCK_SIZE = 0x34

# Default kwargs for the validator (datasheet defaults).
BOUNDS = dict(
    max_phase_current_a=130.0,
    max_system_current_a=390.0,
    max_voltage_ln_v=360.0,
    max_voltage_ll_v=625.0,
    min_frequency_hz=40.0,
    max_frequency_hz=70.0,
)

# INT32 LSW offsets (MSW = offset + 1) for each quantity we want to seed.
OFF_V_L1N = 0x00
OFF_V_L2N = 0x02
OFF_V_L3N = 0x04
OFF_V_L1L2 = 0x06
OFF_V_L2L3 = 0x08
OFF_V_L3L1 = 0x0A
OFF_A_L1 = 0x0C
OFF_A_L2 = 0x0E
OFF_A_L3 = 0x10
OFF_W_L1 = 0x12
OFF_VA_L1 = 0x18
OFF_VAR_L1 = 0x1E
OFF_V_LN_SYS = 0x24
OFF_V_LL_SYS = 0x26
OFF_W_SYS = 0x28
# INT16 offsets.
OFF_PF_SYS = 0x31
OFF_HZ = 0x33


def _set_int32(regs: list[int], off: int, value: int) -> None:
    """Encode a signed INT32 as little-endian word order (LSW at off, MSW at off+1)."""
    v = value & 0xFFFFFFFF
    regs[off] = v & 0xFFFF
    regs[off + 1] = (v >> 16) & 0xFFFF


def _set_int16(regs: list[int], off: int, value: int) -> None:
    regs[off] = value & 0xFFFF


def _realistic_registers(
    *,
    v_ln=2300,  # 230.0 V -> *10
    v_ll=4000,  # 400.0 V -> *10
    current=10000,  # 10.000 A -> *1000
    watt=23000,  # 2300.0 W -> *10
    va=23000,
    var=0,
    frequency=500,  # 50.0 Hz -> *10
) -> list[int]:
    """Build a physically realistic primary-block register list."""
    regs = [0] * PRIMARY_BLOCK_SIZE
    # L-N voltages (per phase + system).
    for off in (OFF_V_L1N, OFF_V_L2N, OFF_V_L3N, OFF_V_LN_SYS):
        _set_int32(regs, off, v_ln)
    # L-L voltages (per phase + system).
    for off in (OFF_V_L1L2, OFF_V_L2L3, OFF_V_L3L1, OFF_V_LL_SYS):
        _set_int32(regs, off, v_ll)
    # Currents.
    for off in (OFF_A_L1, OFF_A_L2, OFF_A_L3):
        _set_int32(regs, off, current)
    # Power / apparent / reactive per phase (0x12/0x14/0x16, etc.).
    for off in (0x12, 0x14, 0x16):
        _set_int32(regs, off, watt)
    for off in (0x18, 0x1A, 0x1C):
        _set_int32(regs, off, va)
    for off in (0x1E, 0x20, 0x22):
        _set_int32(regs, off, var)
    # System W/VA/var.
    _set_int32(regs, OFF_W_SYS, watt * 3)
    _set_int32(regs, 0x2A, va * 3)
    _set_int32(regs, 0x2C, var * 3)
    # INT16 PF and frequency.
    _set_int16(regs, OFF_PF_SYS, 1000)  # pf 1.0
    _set_int16(regs, OFF_HZ, frequency)
    return regs


def _parse(regs: list[int]) -> MeterData:
    """Parse a primary-block register list into a MeterData instance.

    Only the primary (0x0000) block is populated here; the validator reads
    phase/system attributes plus the raw primary registers.
    """
    data = MeterData()
    data.frame.dynamic_reg_map[0x0000].values = list(regs)
    values = data.frame.dynamic_reg_map[0x0000].values
    An = 0
    for i in range(3):
        data.phases[i].parse(i, values)
        An += data.phases[i].current
    data.system.An = An
    data.system.parse(values)
    return data


def _validate(regs: list[int], data: MeterData | None = None, **overrides) -> str | None:
    if data is None:
        data = _parse(regs)
    kwargs = dict(BOUNDS)
    kwargs.update(overrides)
    return validate_primary_frame(regs, data, **kwargs)


class TestFrameValidationPure(unittest.TestCase):
    # 1. Normal frame passes.
    def test_normal_frame_passes(self):
        regs = _realistic_registers()
        self.assertIsNone(_validate(regs))

    # 2. Per-phase current MSW sentinel rejected.
    def test_current_sentinel_rejected_each_phase(self):
        for off, slug in ((OFF_A_L1, "a_l1"), (OFF_A_L2, "a_l2"), (OFF_A_L3, "a_l3")):
            regs = _realistic_registers()
            regs[off] = 0xFFFF
            regs[off + 1] = 0x7FFF
            self.assertEqual(_validate(regs), f"sentinel:{slug}")

    # 3. Every INT32 instantaneous variable in the spec mapping is sentinel-checked.
    # Enumerate the full _PRIMARY_INT32_MSW table (the core spec register map) so
    # a missing/mistyped offset or slug fails the suite, not just a sampled subset.
    def test_sentinel_covers_all_int32_instants(self):
        self.assertEqual(len(_PRIMARY_INT32_MSW), 23)
        for msw_idx, slug in _PRIMARY_INT32_MSW:
            regs = _realistic_registers()
            regs[msw_idx] = 0x7FFF
            self.assertEqual(
                _validate(regs),
                f"sentinel:{slug}",
                msg=f"MSW index {hex(msw_idx)} (slug {slug}) not rejected as sentinel",
            )

    # 4. 0x8000 MSW is NOT a sentinel (large negative power must pass).
    def test_negative_power_msw_0x8000_not_rejected(self):
        regs = _realistic_registers()
        # Large negative W L1: -30000 (-3000.0 W) has MSW 0xFFFF, not 0x7FFF.
        _set_int32(regs, OFF_W_L1, -30000)
        self.assertIsNone(_validate(regs))
        # And an explicit MSW of 0x8000 on a power quantity (in-range decoded).
        regs2 = _realistic_registers()
        regs2[OFF_W_L1] = 0x0000
        regs2[OFF_W_L1 + 1] = 0x8000  # 0x80000000 -> most negative, far from sentinel
        # Decoded value is huge but power has no numeric bound; sentinel ignores 0x8000.
        self.assertIsNone(_validate(regs2))

    # 5. Per-phase current > 130 A rejected; boundary.
    def test_phase_current_over_bound_rejected(self):
        regs = _realistic_registers(current=150000)  # 150.0 A
        self.assertEqual(_validate(regs), "range:a_l1")

    def test_phase_current_boundary(self):
        # Exactly 130.0 A passes.
        regs = _realistic_registers(current=130000)
        self.assertIsNone(_validate(regs))
        # 130.001 A fails.
        regs = _realistic_registers(current=130001)
        self.assertEqual(_validate(regs), "range:a_l1")

    # 6. System An: defensive guard semantics.
    def test_an_per_phase_fires_first_on_real_frame(self):
        # A frame that would push An over 390 can only do so with a phase > 130 A,
        # so the per-phase rule fires first (range:a_l1, NOT range:an).
        regs = _realistic_registers(current=200000)  # 200 A each -> An 600
        self.assertEqual(_validate(regs), "range:a_l1")

    def test_an_defensive_guard_via_direct_override(self):
        # Normally unreachable: override system.An to a value the summation
        # cannot produce once per-phase bounds hold, with all phases in range.
        regs = _realistic_registers()
        data = _parse(regs)
        data.system.An = 400.0
        self.assertEqual(_validate(regs, data=data), "range:an")

    def test_an_boundary_passes(self):
        # An exactly 390.0 via phases 130/130/130 passes.
        regs = _realistic_registers(current=130000)
        data = _parse(regs)
        self.assertAlmostEqual(data.system.An, 390.0, places=3)
        self.assertIsNone(_validate(regs, data=data))

    # 7. L-N voltage out of range.
    def test_ln_voltage_over_bound_rejected(self):
        regs = _realistic_registers(v_ln=4000)  # 400.0 V
        self.assertEqual(_validate(regs), "range:v_ln_l1")

    def test_ln_voltage_boundary(self):
        regs = _realistic_registers(v_ln=3600)  # 360.0 V passes
        self.assertIsNone(_validate(regs))
        regs = _realistic_registers(v_ln=3601)  # 360.1 V fails
        self.assertEqual(_validate(regs), "range:v_ln_l1")

    # 8. L-L voltage out of range.
    def test_ll_voltage_over_bound_rejected(self):
        regs = _realistic_registers(v_ll=7000)  # 700.0 V
        self.assertEqual(_validate(regs), "range:v_ll_l1")

    def test_ll_voltage_boundary_passes(self):
        regs = _realistic_registers(v_ll=6250)  # 625.0 V passes
        self.assertIsNone(_validate(regs))

    # 9. Frequency out of range.
    def test_frequency_out_of_range_rejected(self):
        self.assertEqual(_validate(_realistic_registers(frequency=399)), "range:hz")  # 39.9 Hz
        self.assertEqual(_validate(_realistic_registers(frequency=701)), "range:hz")  # 70.1 Hz

    def test_frequency_in_range_passes(self):
        for hz in (400, 500, 700):  # 40.0, 50.0, 70.0
            self.assertIsNone(_validate(_realistic_registers(frequency=hz)))

    # 10. Sentinel toggle isolated on VA L1 (unbounded quantity).
    def test_sentinel_toggle_isolated_on_va(self):
        regs = _realistic_registers()
        regs[OFF_VA_L1] = 0x0000
        regs[OFF_VA_L1 + 1] = 0x7FFF  # MSW sentinel on VA L1
        self.assertEqual(_validate(regs, check_sentinel=True), "sentinel:va_l1")
        self.assertIsNone(_validate(regs, check_sentinel=False))

    def test_current_sentinel_falls_through_to_range_when_disabled(self):
        # A current MSW 0x7FFF is a poor toggle probe: scaled it is ~2.1M A, so
        # with the sentinel off it still fails the numeric bound.
        regs = _realistic_registers()
        regs[OFF_A_L1] = 0xFFFF
        regs[OFF_A_L1 + 1] = 0x7FFF
        self.assertEqual(_validate(regs, check_sentinel=True), "sentinel:a_l1")
        self.assertEqual(_validate(regs, check_sentinel=False), "range:a_l1")


if __name__ == "__main__":
    unittest.main()
