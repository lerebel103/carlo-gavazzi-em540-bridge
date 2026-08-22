"""Test data generation and validation helpers."""

from __future__ import annotations

import struct
from collections import Counter
from typing import Any

from app.carlo_gavazzi.em540_data import Em540Frame
from app.fronius.ts65a_data import Ts65aMeterData


def encode_int32_le(value: int) -> list[int]:
    """Encode a 32-bit signed integer as two 16-bit words (little-endian)."""
    hi, lo = struct.unpack(">2H", struct.pack(">i", value))
    return [lo, hi]


def encode_int64_le(value: int) -> list[int]:
    """Encode a 64-bit signed integer as four 16-bit words (little-endian)."""
    w0, w1, w2, w3 = struct.unpack(">4H", struct.pack(">q", value))
    return [w3, w2, w1, w0]


def make_em540_source_frame(seed: int) -> Em540Frame:
    """Generate a deterministic EM540 frame for testing.

    Args:
        seed: Integer seed for deterministic data generation.

    Returns:
        An Em540Frame with fully populated register maps.
    """
    frame = Em540Frame()

    # Static registers: device type, serial, version
    frame.static_reg_map[0x000B].values = [1744]
    frame.static_reg_map[0x5000].values = [48, 48, 48, 48, 48, 48, 48, 49]
    frame.static_reg_map[0x5008].values = [ord(c) for c in "EM540-01"]

    # Fill remaining static registers with seed-based data
    for addr, reg in frame.static_reg_map.items():
        if addr in (0x000B, 0x5000, 0x5008):
            continue
        reg.values = [((seed + addr + i) & 0xFFFF) for i in range(len(reg.values))]

    # Primary dynamic block (0x0000): current, voltage, frequency
    primary = frame.dynamic_reg_map[0x0000].values
    for idx in range(0, len(primary), 2):
        primary[idx : idx + 2] = encode_int32_le(seed * 100 + idx)

    # Energy blocks (0x0500, 0x0520): cumulative energy values
    energy = frame.dynamic_reg_map[0x0500].values
    for idx in range(len(energy)):
        energy[idx] = 0
    for source_index in range(13):
        offset = source_index * 4
        energy[offset : offset + 4] = encode_int64_le(seed * 1000 + source_index * 100 + 123)
    energy[0x34 : 0x34 + 2] = encode_int32_le(seed * 10 + 1)
    energy[0x36 : 0x36 + 2] = encode_int32_le(seed * 10 + 2)
    energy[0x38 : 0x38 + 2] = encode_int32_le(seed * 10 + 3)
    energy[0x3A : 0x3A + 2] = encode_int32_le(seed * 10 + 4)
    energy[0x3C : 0x3C + 2] = encode_int32_le(seed * 10 + 50)
    energy[0x3E : 0x3E + 2] = encode_int32_le(seed * 10 + 6)

    frame.remap_registers()
    return frame


def expected_em540_blocks(frame: Em540Frame) -> dict[str, list[int]]:
    """Compute the expected register blocks as seen by downstream EM540 slaves.

    The slave bridge overlays static registers (like 0x000B device type)
    into the primary dynamic block for unified client visibility.

    Args:
        frame: Source Em540Frame to extract expected values from.

    Returns:
        Dictionary mapping block names to expected register lists.
    """
    frame_copy = Em540Frame()
    for addr, reg in frame.static_reg_map.items():
        frame_copy.static_reg_map[addr].values = list(reg.values)
    for addr, reg in frame.dynamic_reg_map.items():
        frame_copy.dynamic_reg_map[addr].values = list(reg.values)
    frame_copy.remap_registers()

    primary = list(frame_copy.dynamic_reg_map[0x0000].values)
    primary[0x000B] = frame_copy.static_reg_map[0x000B].values[0]
    primary[0x0033] = frame_copy.remapped_reg_map[0x0033].values[0]

    return {
        "static_000b": list(frame_copy.static_reg_map[0x000B].values),
        "static_5000": list(frame_copy.static_reg_map[0x5000].values),
        "primary": primary,
        "energy": list(frame_copy.dynamic_reg_map[0x0500].values),
        "remapped_0033": list(frame_copy.remapped_reg_map[0x0033].values),
        "remapped_0110": list(frame_copy.remapped_reg_map[0x0110].values),
        "remapped_0034": list(frame_copy.remapped_reg_map[0x0034].values),
        "remapped_0112": list(frame_copy.remapped_reg_map[0x0112].values),
    }


def encode_ts65a_dynamic_values(meter: Ts65aMeterData) -> list[int]:
    """Encode TS65A dynamic meter values as Modbus registers (IEEE 754 floats).

    Args:
        meter: Ts65aMeterData instance to extract values from.

    Returns:
        List of 16-bit register values representing 32-bit floats in Modbus format.
    """
    values = (
        meter.current_an,
        meter.current_a,
        meter.current_b,
        meter.current_c,
        meter.voltage_ln,
        meter.voltage_ln_a,
        meter.voltage_ln_b,
        meter.voltage_ln_c,
        meter.voltage_ll,
        meter.voltage_ll_a,
        meter.voltage_ll_b,
        meter.voltage_ll_c,
        meter.frequency,
        meter.power,
        meter.power_a,
        meter.power_b,
        meter.power_c,
        meter.apparent_power,
        meter.apparent_power_a,
        meter.apparent_power_b,
        meter.apparent_power_c,
        meter.reactive_power,
        meter.reactive_power_a,
        meter.reactive_power_b,
        meter.reactive_power_c,
        meter.power_factor,
        meter.power_factor_a,
        meter.power_factor_b,
        meter.power_factor_c,
        meter.wh_neg_total,
        meter.wh_neg_a,
        meter.wh_neg_b,
        meter.wh_neg_c,
        meter.wh_plus_total,
        meter.wh_plus_l1,
        meter.wh_plus_l2,
        meter.wh_plus_l3,
        meter.vah_neg_total,
        meter.vah_neg_a,
        meter.vah_neg_b,
        meter.vah_neg_c,
        meter.vah_plus_total,
        meter.vah_plus_a,
        meter.vah_plus_b,
        meter.vah_plus_c,
    )
    registers: list[int] = []
    for value in values:
        registers.extend(struct.unpack(">2H", struct.pack(">f", value)))
    return registers


def decode_ts65a_dynamic_values(registers: list[int]) -> list[float]:
    """Decode TS65A dynamic values from Modbus registers.

    Args:
        registers: List of 16-bit register values.

    Returns:
        List of 32-bit float values.
    """
    values: list[float] = []
    for index in range(0, len(registers), 2):
        values.append(struct.unpack(">f", struct.pack(">2H", registers[index], registers[index + 1]))[0])
    return values


def analyze_register_coverage(
    requests: list[tuple[float, int, int]], expected_requests: set[tuple[int, int]]
) -> dict[str, Any]:
    """Analyze register coverage from a list of requests.

    Args:
        requests: List of (timestamp, address, count) tuples from the server.
        expected_requests: Set of (address, count) tuples that should be requested.

    Returns:
        Dictionary with coverage analysis including request counts and missing requests.
    """
    counts = Counter((addr, count) for _, addr, count in requests)
    missing = [item for item in expected_requests if counts.get(item, 0) == 0]
    return {
        "total_requests": len(requests),
        "distinct_requests": len(counts),
        "request_counts": dict(counts),
        "missing_requests": missing,
    }
