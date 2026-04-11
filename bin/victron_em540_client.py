#!/usr/bin/env python3
"""
Victron GX System EM540 Test Client

This script mimics the Victron dbus-cgwacs client behavior, polling EM540/EM530 
Modbus registers as the GX system would. It connects to the EM540 bridge, reads
a single snapshot of the key registers, and dumps them as key/value pairs.

Reference: https://github.com/victronenergy/dbus-cgwacs/
"""

import sys
import logging
from pymodbus import FramerType
from pymodbus.client import ModbusTcpClient

# ============================================================================
# CONFIGURATION - Adjust these for your environment
# ============================================================================

# TCP transport parameters
#MODBUS_HOST = "192.168.102.240"
MODBUS_HOST = "localhost"
MODBUS_PORT = 5002
SLAVE_ID = 1
TIMEOUT = 2.0
RETRIES = 2

# ============================================================================
# REGISTER DEFINITIONS - Victron EM540 polling registers
# ============================================================================

# Registers are organized by functional blocks based on Victron's polling strategy

REGISTER_GROUPS = {
    # Device identification and compatibility checks
    "Device Type": {
        "registers": 0x000B,
        "count": 1,
        "description": "Device type ID (for compatibility check)",
        "static": True
    },
    
    # Firmware version
    "Firmware Version": {
        "registers": 0x0302,
        "count": 1,
        "description": "Firmware version",
        "static": True
    },
    
    # Serial number
    "Serial Number": {
        "registers": 0x5000,
        "count": 8,
        "description": "Device serial number",
        "static": True
    },
    
    # Measurement configuration
    "Measuring System": {
        "registers": 0x1002,
        "count": 1,
        "description": "Measuring system (3-phase, single-phase, etc.)",
        "static": True
    },
    
    # Measurement mode
    "Measurement Mode": {
        "registers": 0x1103,
        "count": 1,
        "description": "Measurement mode (B or C for EM540)",
        "static": True
    },

    # Phase sequence/status (Victron startup check)
    "Phase Sequence": {
        "registers": 0x010E,
        "count": 1,
        "description": "Phase sequence status",
        "static": True
    },
    
    # Basic voltage measurements (per-phase)
    "Voltage Per Phase": {
        "registers": 0x0000,
        "count": 7,
        "description": "Phase voltages block (Victron profile)"
    },

    # System voltage block
    "Voltage Total": {
        "registers": 0x0024,
        "count": 2,
        "description": "System voltage block (Victron profile)"
    },
    
    # Current measurements (per-phase)
    "Current Per Phase": {
        "registers": 0x000C,
        "count": 7,
        "description": "Phase currents block (Victron profile)"
    },
    
    # Power measurements (power, apparent power, reactive power)
    "Power Per Phase": {
        "registers": 0x0012,
        "count": 7,
        "description": "Per-phase power block (Victron profile)"
    },

    # System power
    "Power Total": {
        "registers": 0x0028,
        "count": 2,
        "description": "System total power"
    },
    
    # Power factor
    "Power Factor": {
        "registers": 0x002E,
        "count": 4,
        "description": "Power factor (total and per-phase)"
    },
    
    # Frequency
    "Frequency": {
        "registers": 0x053C,
        "count": 3,
        "description": "Grid frequency block (EM540 high-resolution path)"
    },
    
    # Energy counters (positive)
    "Energy Import Total": {
        "registers": 0x0034,
        "count": 3,
        "description": "kWh+ total block"
    },

    # Per-phase imported energy
    "Energy Import Per Phase": {
        "registers": 0x0040,
        "count": 7,
        "description": "kWh+ per-phase block"
    },

    # Total exported energy
    "Energy Export Total": {
        "registers": 0x004E,
        "count": 3,
        "description": "kWh- total block"
    },
}

# ============================================================================
# VALUE PARSING
# ============================================================================

def convert_int32(registers, offset):
    """Convert two 16-bit registers to a 32-bit signed integer (little-endian word order)."""
    if offset + 1 >= len(registers):
        return None
    low_word = registers[offset] & 0xFFFF
    high_word = registers[offset + 1] & 0xFFFF
    value = (high_word << 16) | low_word
    if value & 0x80000000:
        value -= 0x100000000
    return value

def convert_int64(registers, offset):
    """Convert four 16-bit registers to a 64-bit signed integer (little-endian word order)."""
    if offset + 3 >= len(registers):
        return None
    words = [registers[offset + i] & 0xFFFF for i in range(4)]
    value = 0
    for i, word in enumerate(words):
        value |= word << (16 * i)
    if value & (1 << 63):
        value -= 1 << 64
    return value

def convert_int16(registers, offset):
    """Convert a single 16-bit register to a signed integer."""
    if offset >= len(registers):
        return None
    value = registers[offset] & 0xFFFF
    if value & 0x8000:
        value -= 0x10000
    return value

def parse_voltages(registers):
    """Parse voltage registers (per-phase)."""
    values = {}
    # Assuming first 8 registers contain phase-to-neutral and phase-to-phase voltages
    # Typical layout: V_L1N, V_L2N, V_L3N, V_L1L2, V_L2L3, V_L3L1, etc.
    for i, phase in enumerate(["L1", "L2", "L3"]):
        val = convert_int32(registers, i * 2)
        if val is not None:
            values[f"Voltage_{phase}_N"] = val / 10.0  # Divide by 10 for voltage
    return values

def parse_currents(registers):
    """Parse current registers (per-phase)."""
    values = {}
    for i, phase in enumerate(["L1", "L2", "L3"]):
        val = convert_int32(registers, i * 2)
        if val is not None:
            values[f"Current_{phase}"] = val / 1000.0  # Divide by 1000 for current
    return values

def parse_power(registers):
    """Parse power registers (total and per-phase)."""
    values = {}
    if len(registers) <= 2:
        val = convert_int32(registers, 0)
        if val is not None:
            values["Power_Total"] = val / 10.0
        return values

    # Typical layout: P_TOT (2 regs), P_L1 (2), P_L2 (2), P_L3 (2), S_TOT (2), Q_TOT (2), ...
    val = convert_int32(registers, 0)
    if val is not None:
        values["Power_Total"] = val / 10.0
    val = convert_int32(registers, 2)
    if val is not None:
        values["Power_L1"] = val / 10.0
    val = convert_int32(registers, 4)
    if val is not None:
        values["Power_L2"] = val / 10.0
    val = convert_int32(registers, 6)
    if val is not None:
        values["Power_L3"] = val / 10.0
    val = convert_int32(registers, 8)
    if val is not None:
        values["Apparent_Power_Total"] = val / 10.0
    val = convert_int32(registers, 10)
    if val is not None:
        values["Reactive_Power_Total"] = val / 10.0
    return values

def parse_power_factor(registers):
    """Parse power factor registers."""
    values = {}
    val = convert_int16(registers, 0)
    if val is not None:
        values["Power_Factor_Total"] = val / 1000.0
    for i, phase in enumerate(["L1", "L2", "L3"]):
        val = convert_int16(registers, 1 + i)
        if val is not None:
            values[f"Power_Factor_{phase}"] = val / 1000.0
    return values

def parse_frequency(registers):
    """Parse frequency register."""
    values = {}
    val = convert_int32(registers, 0) if len(registers) >= 2 else convert_int16(registers, 0)
    if val is not None:
        # EM540 0x053C path uses higher resolution than legacy 0x0033.
        values["Frequency"] = val / 1000.0
    return values


def parse_raw_register_block(group_name, base_addr, registers):
    """Dump raw register values so the polling profile can be validated directly."""
    values = {}
    for idx, reg in enumerate(registers):
        addr = base_addr + idx
        values[f"{group_name}_Raw_0x{addr:04X}"] = reg
    return values

def parse_energy_import(registers):
    """Parse positive energy counter registers (kWh)."""
    values = {}
    if len(registers) < 4:
        # Victron profile reads 0x0034 as a short block; expose what is available.
        val32 = convert_int32(registers, 0) if len(registers) >= 2 else None
        if val32 is not None:
            values["Energy_kWh_Import_Total"] = val32 / 100.0
        for i, reg in enumerate(registers):
            values[f"Energy_Import_Total_Raw_Idx{i}"] = reg
        return values

    val = convert_int64(registers, 0)
    if val is not None:
        values["Energy_kWh_Import_Total"] = val / 1000.0
    val = convert_int64(registers, 4)
    if val is not None:
        values["Energy_kWh_Import_L1"] = val / 1000.0
    val = convert_int64(registers, 8) if len(registers) > 8 else None
    if val is not None:
        values["Energy_kWh_Import_L2"] = val / 1000.0
    val = convert_int64(registers, 12) if len(registers) > 12 else None
    if val is not None:
        values["Energy_kWh_Import_L3"] = val / 1000.0
    return values

def parse_run_hours(registers):
    """Parse run hour meter registers."""
    values = {}
    val = convert_int32(registers, 0)
    if val is not None:
        values["Run_Hour_Meter"] = val / 100.0
    val = convert_int32(registers, 2) if len(registers) > 2 else None
    if val is not None:
        values["Run_Hour_Meter_Negative"] = val / 100.0
    return values

def parse_device_type(registers):
    """Parse device type register."""
    values = {}
    val = convert_int16(registers, 0)
    if val is not None:
        # Victron compatibility check: EM540 IDs are 1744-1763, EM530 IDs are 2096, 1956
        device_name = "Unknown"
        if 1744 <= val <= 1763:
            device_name = "EM540"
        elif val in [2096, 1956]:
            device_name = "EM530"
        values["Device_Type_ID"] = val
        values["Device_Type_Name"] = device_name
    return values

def parse_firmware_version(registers):
    """Parse firmware version register."""
    values = {}
    val = convert_int16(registers, 0)
    if val is not None:
        # Firmware is typically stored as a version code
        values["Firmware_Version"] = val
    return values

def parse_serial_number(registers):
    """Parse serial number (8 registers, ASCII)."""
    values = {}

    def decode_ascii(regs, low_byte_first):
        raw_bytes = bytearray()
        for reg in regs:
            high = (reg >> 8) & 0xFF
            low = reg & 0xFF
            if low_byte_first:
                raw_bytes.extend((low, high))
            else:
                raw_bytes.extend((high, low))

        printable = []
        for b in raw_bytes:
            if b == 0:
                break
            if 32 <= b <= 126:
                printable.append(chr(b))
            else:
                break
        return "".join(printable)

    serial_be = decode_ascii(registers[:8], low_byte_first=False)
    serial_le = decode_ascii(registers[:8], low_byte_first=True)

    # Prefer serial-like strings (alnum/hyphen), fallback to longest printable.
    def score(candidate):
        if not candidate:
            return -1
        valid_chars = sum(c.isalnum() or c in "-_" for c in candidate)
        return valid_chars * 100 + len(candidate)

    serial = serial_be if score(serial_be) >= score(serial_le) else serial_le
    if serial:
        values["Serial_Number"] = serial
        values["Serial_Number_Decode"] = "high-byte-first" if serial == serial_be else "low-byte-first"
    return values

def parse_measuring_system(registers):
    """Parse measuring system configuration."""
    values = {}
    val = convert_int16(registers, 0)
    if val is not None:
        system_name = "Unknown"
        if val == 0:
            system_name = "3-Phase"
        elif val == 1:
            system_name = "Single-Phase"
        values["Measuring_System_ID"] = val
        values["Measuring_System_Name"] = system_name
    return values

def parse_measurement_mode(registers):
    """Parse measurement mode (B or C)."""
    values = {}
    val = convert_int16(registers, 0)
    if val is not None:
        mode_name = "A"
        if val == 1:
            mode_name = "Mode B"
        elif val == 2:
            mode_name = "Mode C"
        else:
            mode_name = f"Unknown ({val})"           
        values["Measurement_Mode_ID"] = val
        values["Measurement_Mode_Name"] = mode_name
    return values

PARSERS = {
    "Device Type": parse_device_type,
    "Firmware Version": parse_firmware_version,
    "Serial Number": parse_serial_number,
    "Measuring System": parse_measuring_system,
    "Measurement Mode": parse_measurement_mode,
    "Voltage Per Phase": parse_voltages,
    "Current Per Phase": parse_currents,
    "Power Per Phase": parse_power,
    "Power Total": parse_power,
    "Power Factor": parse_power_factor,
    "Frequency": parse_frequency,
    "Energy Import Total": parse_energy_import,
}

# ============================================================================
# MAIN CLIENT
# ============================================================================

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Victron EM540 Test Client")

    # Keep TCP transport, but use RTU framing (RTU-over-TCP style) to match the bridge.
    logger.info("Connecting via TCP (RTU framer) to %s:%s", MODBUS_HOST, MODBUS_PORT)
    client = ModbusTcpClient(
        host=MODBUS_HOST,
        port=MODBUS_PORT,
        framer=FramerType.RTU,
        timeout=TIMEOUT,
        retries=RETRIES,
    )
    
    try:
        if not client.connect():
            logger.error("Failed to connect to Modbus server")
            return 1
        
        logger.info("Connected successfully")
        
        # Disable defaults on read when supported; error on unexpected responses
        if hasattr(client, "strict"):
            client.strict = True
        
        all_values = {}
        
        # Read register groups (static first for compatibility check)
        static_groups = {k: v for k, v in REGISTER_GROUPS.items() if v.get("static", False)}
        dynamic_groups = {k: v for k, v in REGISTER_GROUPS.items() if not v.get("static", False)}
        
        for group_dict in [static_groups, dynamic_groups]:
            for group_name, group_config in group_dict.items():
                logger.info(f"Reading {group_name} ({group_config['description']})")
                
                try:
                    result = client.read_holding_registers(
                        address=group_config["registers"],
                        count=group_config["count"],
                        slave=SLAVE_ID
                    )
                    
                    if result.isError():
                        logger.warning(f"{group_name}: Modbus error - {result}")
                        continue
                    
                    if not result.registers:
                        logger.warning(f"{group_name}: No registers returned")
                        continue
                    
                    # Parse the values using the appropriate parser
                    if group_name in PARSERS:
                        parser = PARSERS[group_name]
                        parsed = parser(result.registers)
                        all_values.update(parsed)
                        logger.info(f"{group_name}: {len(parsed)} values parsed")
                    else:
                        parsed = parse_raw_register_block(group_name, group_config["registers"], result.registers)
                        all_values.update(parsed)
                        logger.info(f"{group_name}: {len(parsed)} raw registers dumped")
                    
                except Exception as e:
                    logger.error(f"{group_name}: Exception during read - {e}")
                    continue
        
        # Print results as key/value pairs
        print("\n" + "="*70)
        print("EM540 METER DATA - KEY/VALUE PAIRS")
        print("="*70)
        
        if all_values:
            for key in sorted(all_values.keys()):
                value = all_values[key]
                if isinstance(value, (int, float)):
                    print(f"{key:<40} = {value:>15.3f}")
                else:
                    print(f"{key:<40} = {value}")
            print("="*70)
            logger.info(f"Successfully read {len(all_values)} values")
            return 0
        else:
            print("No values could be read from the meter")
            print("="*70)
            logger.error("Failed to read any values from the meter")
            return 1
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1
    
    finally:
        client.close()
        logger.info("Disconnected from Modbus server")

if __name__ == "__main__":
    sys.exit(main())
