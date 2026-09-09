#!/usr/bin/env python3
"""Convert an RS485 sniffer capture into a human-readable, debug-friendly CSV.

TEMPORARY DIAGNOSTIC — companion to the RS485 sniffer (tools/rs485_sniffer/).

The sniffer's raw/parsed CSVs record one row per framed exchange as hex. This
tool reads the sniffer's PARSED CSV, decodes the Modbus payloads into named
energy-meter values (SunSpec model 213 — the TS65A dynamic float block at 40071,
its Events register at 40193, and the energy block at 40129), and writes a wide
CSV with one column per meter field plus timing/consistency columns that are
useful when the dropout recurs.

Usage:
    python3 parse_capture.py <sniffer_parsed.csv> [out.csv]

Input columns expected (from the sniffer's parsed CSV):
    seq,wall_local,mono,delta_ms,direction,dev_id,fc,addr,count,bytecount,
    exception,crc_ok,n_bytes,summary,hex

Output: one row per decoded read RESPONSE (the rows that actually carry meter
values), keyed by the response's timestamp, with:
  * timing: wall_local, mono, delta_ms, and gap_s (seconds since previous
    decoded response — a large gap is the dropout signature)
  * the register block it answers (addr, inferred by pairing with the preceding
    request on the same dev_id)
  * every decoded field for that block (named)
  * consistency helpers for the dynamic block: S_calc = hypot(P,Q),
    S_ratio = S/S_calc, and per-phase V*I
  * crc_ok and a note column flagging anything unusual

Rows that are not decodable meter responses (requests, exceptions, unparsed
fragments) are summarised in a separate companion column so they are not lost.
"""

from __future__ import annotations

import csv
import math
import struct
import sys

# --- SunSpec model 213 field order for the 40071 dynamic block --------------
# Matches Ts65aSlaveBridge._dynamic_values(): 45 float32 fields = 90 registers.
# A read of count=58 registers returns the first 29 fields (through PF phase C).
DYN_FIELDS = [
    "current_total",
    "current_a",
    "current_b",
    "current_c",
    "voltage_ln_avg",
    "voltage_ln_a",
    "voltage_ln_b",
    "voltage_ln_c",
    "voltage_ll_avg",
    "voltage_ll_ab",
    "voltage_ll_bc",
    "voltage_ll_ca",
    "frequency",
    "power_total",
    "power_a",
    "power_b",
    "power_c",
    "va_total",
    "va_a",
    "va_b",
    "va_c",
    "var_total",
    "var_a",
    "var_b",
    "var_c",
    "pf_total",
    "pf_a",
    "pf_b",
    "pf_c",
    "wh_exported_total",
    "wh_exported_a",
    "wh_exported_b",
    "wh_exported_c",
    "wh_imported_total",
    "wh_imported_a",
    "wh_imported_b",
    "wh_imported_c",
    "vah_exported_total",
    "vah_exported_a",
    "vah_exported_b",
    "vah_exported_c",
    "vah_imported_total",
    "vah_imported_a",
    "vah_imported_b",
    "vah_imported_c",
]

# Consistency/timing columns appended after the decoded fields.
DERIVED_FIELDS = ["S_calc_total", "S_ratio_total", "VxI_a", "VxI_b", "VxI_c"]

# Register address -> (name, first field index in DYN_FIELDS).
# 40071 is the dynamic block start; 40129 is the energy sub-block (offset 29).
_ENERGY_BLOCK_FIELD_OFFSET = 29


def _hex_to_bytes(hexstr: str) -> bytes:
    return bytes.fromhex(hexstr.replace(" ", "")) if hexstr else b""


def _response_registers(frame: bytes) -> list[int] | None:
    """Extract 16-bit registers from a read-response frame (id, fc, bytecount, data, CRC)."""
    if len(frame) < 5:
        return None
    bytecount = frame[2]
    if bytecount != len(frame) - 5 or bytecount % 2 != 0:
        return None
    data = frame[3 : 3 + bytecount]
    return [int.from_bytes(data[i : i + 2], "big") for i in range(0, len(data), 2)]


def _registers_to_floats(registers: list[int]) -> list[float]:
    """Decode consecutive register pairs into big-endian float32s."""
    out = []
    for i in range(0, len(registers) - 1, 2):
        hi, lo = registers[i], registers[i + 1]
        out.append(struct.unpack(">f", struct.pack(">HH", hi, lo))[0])
    return out


def _decode_dynamic(registers: list[int], field_offset: int = 0) -> dict:
    """Decode a dynamic/energy block into named fields (starting at field_offset)."""
    floats = _registers_to_floats(registers)
    fields = {}
    for i, val in enumerate(floats):
        idx = field_offset + i
        if idx < len(DYN_FIELDS):
            fields[DYN_FIELDS[idx]] = val
    # Consistency helpers when we have the power quantities.
    if "power_total" in fields and "var_total" in fields and "va_total" in fields:
        p, q, s = fields["power_total"], fields["var_total"], fields["va_total"]
        calc = math.hypot(p, q)
        fields["S_calc_total"] = calc
        fields["S_ratio_total"] = (s / calc) if calc else ""
    for ph in ("a", "b", "c"):
        v, i = fields.get(f"voltage_ln_{ph}"), fields.get(f"current_{ph}")
        if v is not None and i is not None:
            fields[f"VxI_{ph}"] = v * i
    return fields


# Column order of the sniffer's PARSED CSV. Parsing is done by POSITION rather
# than via csv.DictReader so a capture without a header row (the common case —
# the sniffer only writes a header when it creates a fresh file) still parses
# correctly. A header line, if present, is detected and skipped.
_PARSED_COLUMNS = [
    "seq",
    "wall_local",
    "mono",
    "delta_ms",
    "direction",
    "dev_id",
    "fc",
    "addr",
    "count",
    "bytecount",
    "exception",
    "crc_ok",
    "n_bytes",
    "summary",
    "hex",
]


def _read_rows(inp: str) -> list[dict]:
    """Read the sniffer's parsed CSV by column position (header optional).

    The `hex` field is the last column and may itself contain commas? No — the
    sniffer writes hex as space-separated bytes, so a simple split on ',' with a
    maxsplit equal to the number of leading columns is safe and keeps the whole
    hex string intact.
    """
    rows: list[dict] = []
    n_lead = len(_PARSED_COLUMNS) - 1  # everything before the final `hex` column
    with open(inp) as fh:
        for line in fh:
            line = line.rstrip("\n")
            if not line:
                continue
            parts = line.split(",", n_lead)
            if len(parts) < len(_PARSED_COLUMNS):
                continue  # malformed / short line
            # Skip a header row if present.
            if parts[0] == "seq" and parts[4] == "direction":
                continue
            rows.append(dict(zip(_PARSED_COLUMNS, parts)))
    return rows


def main(inp: str, outp: str) -> None:
    rows = _read_rows(inp)

    out_cols = (
        ["seq", "wall_local", "mono", "delta_ms", "gap_s", "block", "crc_ok"]
        + DYN_FIELDS
        + DERIVED_FIELDS
        + ["events_hex", "note", "raw_summary", "hex"]
    )

    records = []
    last_request_addr: dict[str, str] = {}  # dev_id -> last requested addr
    prev_mono: float | None = None

    for r in rows:
        direction = r.get("direction", "")
        dev = r.get("dev_id", "")
        addr = r.get("addr", "")
        frame = _hex_to_bytes(r.get("hex", ""))

        # Track the most recent request address per device so a response (which
        # does not carry the address) can be attributed to a register block.
        if direction == "request" and addr:
            last_request_addr[dev] = addr
            continue

        # Only responses carry meter values.
        if direction != "response":
            # Keep non-response rows visible (exceptions, unparsed) as notes.
            records.append(
                {
                    "seq": r.get("seq", ""),
                    "wall_local": r.get("wall_local", ""),
                    "mono": r.get("mono", ""),
                    "delta_ms": r.get("delta_ms", ""),
                    "gap_s": "",
                    "block": "",
                    "crc_ok": r.get("crc_ok", ""),
                    "note": f"non-response ({direction})",
                    "raw_summary": r.get("summary", ""),
                    "hex": r.get("hex", ""),
                }
            )
            continue

        block = last_request_addr.get(dev, "")
        rec = {
            "seq": r.get("seq", ""),
            "wall_local": r.get("wall_local", ""),
            "mono": r.get("mono", ""),
            "delta_ms": r.get("delta_ms", ""),
            "block": block,
            "crc_ok": r.get("crc_ok", ""),
            "raw_summary": r.get("summary", ""),
            "hex": r.get("hex", ""),
        }

        # gap_s: seconds since the previous decoded response — the dropout shows
        # up here as a large value.
        try:
            mono = float(r.get("mono", ""))
            if prev_mono is not None:
                rec["gap_s"] = round(mono - prev_mono, 3)
            prev_mono = mono
        except (TypeError, ValueError):
            pass

        registers = _response_registers(frame)
        if registers is None:
            rec["note"] = "response payload not decodable"
        elif block == "40071":
            rec.update(_decode_dynamic(registers, field_offset=0))
        elif block == "40129":
            rec.update(_decode_dynamic(registers, field_offset=_ENERGY_BLOCK_FIELD_OFFSET))
        elif block == "40193":
            rec["events_hex"] = frame[3:-2].hex(" ")
            rec["note"] = "events register"
        else:
            rec["note"] = f"response for unmapped block {block!r}"

        records.append(rec)

    with open(outp, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=out_cols, extrasaction="ignore")
        w.writeheader()
        for rec in records:
            w.writerow({k: (round(v, 4) if isinstance(v, float) else v) for k, v in rec.items()})

    dyn = sum(1 for r in records if r.get("block") == "40071" and r.get("power_total") is not None)
    print(f"Parsed {len(records)} rows -> {outp}")
    print(f"  decoded dynamic (40071) responses: {dyn}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    out = sys.argv[2] if len(sys.argv) > 2 else "sniffer_decoded.csv"
    main(sys.argv[1], out)
