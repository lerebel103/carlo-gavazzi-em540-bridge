"""Spec-grounded upstream frame validation for the EM530/EM540 primary block.

This module provides a *pure* validator that rejects an entire parsed frame when
any instantaneous quantity fails one of two independent, documented criteria:

1. Overflow sentinel — EM500_CPP_Mod_V1.3 "Modbus communication protocol" §3.3
   "Maximum electrical values": the overflow indication is signalled when the
   MSW (most-significant word) of the relevant INT32 variable is ``0x7FFF``.
   Per §3.1 the word order is LSW->MSW, so for an INT32 at primary-block offset
   ``off`` the MSW is register ``off + 1``.
2. Physical plausibility bounds — EM540 datasheet figures (Imax, rated voltages,
   frequency range), widened by the user-confirmed/datasheet-derived margins
   documented alongside each constant below.

The validator reads the parse *output* (decoded floats on ``SystemData`` /
``PhaseData``) plus the raw primary-block registers for the sentinel check. It
never alters how a value is decoded: a valid frame produces byte-for-byte
identical downstream output. The decode path (``_fast_int32_from_regs_le``,
scaling divisors, the ``0x000B`` overlay) is untouched.

Hot path: the accept path is a sequence of integer and float comparisons with no
locks, no I/O and no allocations. The only allocation is the reason string, and
only on the rare reject path.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from app.carlo_gavazzi.meter_data import MeterData

# Overflow sentinel value of the MSW for an INT32 instantaneous variable
# (EM500_CPP_Mod_V1.3 §3.3 "Maximum electrical values": MSW == 0x7FFFh).
_OVERFLOW_SENTINEL_MSW: int = 0x7FFF

# (MSW register index, slug) for every INT32 instantaneous variable in the
# primary block (base 0x0000, so register index == physical address). The MSW
# index is the INT32 offset + 1 (LSW->MSW word order per §3.1). Addresses and
# scaling come from EM500_CPP_Mod_V1.3 §4.1. The slug is the suffix of the
# "sentinel:<slug>" reason string so reason strings are stable and testable.
# Iterated in this listed order (deterministic).
_PRIMARY_INT32_MSW: tuple[tuple[int, str], ...] = (
    (0x01, "v_l1n"),
    (0x03, "v_l2n"),
    (0x05, "v_l3n"),
    (0x07, "v_l1l2"),
    (0x09, "v_l2l3"),
    (0x0B, "v_l3l1"),
    (0x0D, "a_l1"),
    (0x0F, "a_l2"),
    (0x11, "a_l3"),
    (0x13, "w_l1"),
    (0x15, "w_l2"),
    (0x17, "w_l3"),
    (0x19, "va_l1"),
    (0x1B, "va_l2"),
    (0x1D, "va_l3"),
    (0x1F, "var_l1"),
    (0x21, "var_l2"),
    (0x23, "var_l3"),
    (0x25, "v_ln_sys"),
    (0x27, "v_ll_sys"),
    (0x29, "w_sys"),
    (0x2B, "va_sys"),
    (0x2D, "var_sys"),
)

# --- Physical plausibility bounds (EM540 datasheet) ------------------------
# Rejection uses abs(value) so sign (import/export, phase rotation) is ignored;
# only magnitude plausibility is enforced.

# Per-phase current: 2 x Imax(65 A) = 130 A. The 2x factor is a deliberate
# headroom margin over the datasheet maximum current Imax (user-confirmed).
MAX_PHASE_CURRENT_A: float = 130.0  # 2 x Imax(65 A)

# System/neutral current An. On any frame produced by update_from_frame, An is
# the signed sum of the three phase currents, so once each per-phase current has
# passed its 130 A bound, abs(An) <= 3 x MAX_PHASE_CURRENT_A = 390 A is already
# implied. This bound is therefore a DEFENSIVE guard (for a future code path
# that sets An independently of the per-phase sum, or a validator reordering),
# not an independently reachable live-frame check. It is retained per the
# user-confirmed requirement ("system/neutral current An abs>390 A").
MAX_SYSTEM_CURRENT_A: float = 390.0  # defensive: 3 x per-phase bound (3 x 2 x Imax)

# L-N voltage: 1.5 x Un(240 V) = 360 V (datasheet "Overload Continuous:
# 1.5 Un max", Un max = 240 V non-MID).
MAX_VOLTAGE_LN_V: float = 360.0  # 1.5 x Un(240 V)

# L-L voltage: 1.5 x L-L rating(415 V) = 622.5 V, rounded to 625 V.
MAX_VOLTAGE_LL_V: float = 625.0  # ~1.5 x L-L(415 V)

# Frequency plausibility: datasheet spec 45-65 Hz, widened by a +/-5 Hz guard
# band to avoid rejecting valid edge readings during transients.
MIN_FREQUENCY_HZ: float = 40.0  # datasheet 45 Hz - 5 Hz guard
MAX_FREQUENCY_HZ: float = 70.0  # datasheet 65 Hz + 5 Hz guard


def validate_primary_frame(
    registers: list[int],
    data: "MeterData",
    *,
    max_phase_current_a: float,
    max_system_current_a: float,
    max_voltage_ln_v: float,
    max_voltage_ll_v: float,
    min_frequency_hz: float,
    max_frequency_hz: float,
    check_sentinel: bool = True,
) -> str | None:
    """Validate a parsed primary frame. Return ``None`` if acceptable.

    ``registers`` is the live upstream primary block
    (``dynamic_reg_map[_DYNAMIC_PRIMARY_BLOCK_ADDR].values``, pre-overlay);
    ``data`` is the parsed frame (decoded floats on ``system`` / ``phases``).

    Deterministic evaluation order so reason strings are stable for tests:

    1. if ``check_sentinel``: full sweep of ``_PRIMARY_INT32_MSW`` in listed
       order; on the first ``registers[msw_idx] == 0x7FFF`` return
       ``"sentinel:<slug>"``.
    2. physical-range sweep (per-phase current L1/L2/L3, system An, per-phase +
       system L-N voltage, per-phase + system L-L voltage, frequency); on the
       first out-of-bounds value return ``"range:<slug>"``.
    3. otherwise return ``None``.

    Allocation-free on the accept path except the reason string on reject.
    """
    # 1. Overflow-sentinel sweep (spec-exact MSW == 0x7FFF check).
    if check_sentinel:
        for msw_idx, slug in _PRIMARY_INT32_MSW:
            if registers[msw_idx] == _OVERFLOW_SENTINEL_MSW:
                return f"sentinel:{slug}"

    system = data.system
    phases = data.phases

    # 2. Physical-range sweep (abs magnitude vs datasheet-derived bounds).
    # Per-phase current L1/L2/L3.
    if abs(phases[0].current) > max_phase_current_a:
        return "range:a_l1"
    if abs(phases[1].current) > max_phase_current_a:
        return "range:a_l2"
    if abs(phases[2].current) > max_phase_current_a:
        return "range:a_l3"

    # System/neutral current An (defensive; see MAX_SYSTEM_CURRENT_A).
    if abs(system.An) > max_system_current_a:
        return "range:an"

    # L-N voltage: phases then system.
    if abs(phases[0].line_neutral_voltage) > max_voltage_ln_v:
        return "range:v_ln_l1"
    if abs(phases[1].line_neutral_voltage) > max_voltage_ln_v:
        return "range:v_ln_l2"
    if abs(phases[2].line_neutral_voltage) > max_voltage_ln_v:
        return "range:v_ln_l3"
    if abs(system.line_neutral_voltage) > max_voltage_ln_v:
        return "range:v_ln_sys"

    # L-L voltage: phases then system.
    if abs(phases[0].line_line_voltage) > max_voltage_ll_v:
        return "range:v_ll_l1"
    if abs(phases[1].line_line_voltage) > max_voltage_ll_v:
        return "range:v_ll_l2"
    if abs(phases[2].line_line_voltage) > max_voltage_ll_v:
        return "range:v_ll_l3"
    if abs(system.line_line_voltage) > max_voltage_ll_v:
        return "range:v_ll_sys"

    # Frequency (INT16 in the primary block; the sentinel does not apply).
    frequency = system.frequency
    if frequency < min_frequency_hz or frequency > max_frequency_hz:
        return "range:hz"

    return None
