import struct
import time

from pymodbus.client import ModbusTcpClient

from app.carlo_gavazzi.em540_data import Em540Frame

# Pre-compiled struct formats for fast register conversion
_STRUCT_INT32_BE = struct.Struct(">i")
_STRUCT_INT16_BE = struct.Struct(">h")
_STRUCT_2H = struct.Struct(">2H")


def _convert_from_registers_little(registers: list[int], data_type: ModbusTcpClient.DATATYPE) -> int | float | str:
    # Pymodbus >=3.8 removed word_order, so we reverse multi-word register groups
    # to preserve the existing little-endian word-order behavior.
    if len(registers) > 1:
        registers = list(reversed(registers))
    return ModbusTcpClient.convert_from_registers(registers, data_type)


def _fast_int32_from_regs_le(registers: list[int], offset: int) -> int:
    """Fast INT32 decode from 2 registers in little-endian word order."""
    r0, r1 = registers[offset], registers[offset + 1]
    return _STRUCT_INT32_BE.unpack(_STRUCT_2H.pack(r1, r0))[0]


def _fast_int16_from_reg(registers: list[int], offset: int) -> int:
    """Fast signed INT16 decode from a single unsigned 16-bit register."""
    val = registers[offset] & 0xFFFF
    return val - 0x10000 if val >= 0x8000 else val


def _fast_int64_from_regs_le(registers: list[int], offset: int) -> int:
    """Fast INT64 decode from 4 registers in little-endian word order."""
    r0, r1, r2, r3 = registers[offset], registers[offset + 1], registers[offset + 2], registers[offset + 3]
    return struct.unpack(">q", struct.pack(">4H", r3, r2, r1, r0))[0]


class SystemData:
    def __init__(self):
        self.line_neutral_voltage: float = 0.0
        self.line_line_voltage: float = 0.0
        self.power: float = 0.0
        self.apparent_power: float = 0.0
        self.reactive_power: float = 0.0
        self.power_factor: float = 0.0
        self.frequency: float = 0.0
        self.run_hour_meter: float = 0.0
        self.run_hour_meter_neg: float = 0.0
        self.An: float = 0

    def parse(self, registers):
        # Pick from registers
        # An is computed separately

        self.line_neutral_voltage = _fast_int32_from_regs_le(registers, 0x024) / 10
        self.line_line_voltage = _fast_int32_from_regs_le(registers, 0x026) / 10
        self.power = _fast_int32_from_regs_le(registers, 0x028) / 10
        self.apparent_power = _fast_int32_from_regs_le(registers, 0x02A) / 10
        self.reactive_power = _fast_int32_from_regs_le(registers, 0x02C) / 10
        self.power_factor = _fast_int16_from_reg(registers, 0x031) / 1000
        self.frequency = _fast_int16_from_reg(registers, 0x033) / 10

        # print all above values
        # print(self)

    def __str__(self):
        return (
            f"line_neutral_voltage: {self.line_neutral_voltage}, "
            f"line_line_voltage: {self.line_line_voltage}, "
            f"An: {self.An}"
            f"power: {self.power}, "
            f"apparent_power: {self.apparent_power}, "
            f"reactive_power: {self.reactive_power}, "
            f"pf: {self.power_factor}, "
            f"frequency: {self.frequency}, "
            f"run_hour_meter: {self.run_hour_meter}, "
            f"run_hour_meter_neg: {self.run_hour_meter_neg}"
        )


class OtherEnergies:
    def __init__(self):
        self.kwh_plus_total: float = 0.0
        self.kvarh_plus_total: float = 0.0
        self.kwh_plus_partial: float = 0.0
        self.kvarh_plus_partial: float = 0.0
        self.kwh_plus_l1: float = 0.0
        self.kwh_plus_l2: float = 0.0
        self.kwh_plus_l3: float = 0.0
        self.kwh_neg_total: float = 0.0
        self.kwh_neg_partial: float = 0.0
        self.kvarh_neg_total: float = 0.0
        self.kvarh_neg_partial: float = 0.0
        self.kvah_total: float = 0.0
        self.kvah_partial: float = 0.0
        self.run_hour_meter: float = 0.0
        self.run_hour_meter_neg_kwh: float = 0.0
        self.run_hour_meter_partial: float = 0.0
        self.run_hour_meter_neg_kwh_partial: float = 0.0
        self.frequency: float = 0.0
        self.run_hour_life_counter: float = 0.0

    def parse(self, registers):
        self.kwh_plus_total = _fast_int64_from_regs_le(registers, 0x00) / 1000.0

        self.kvarh_plus_total = _fast_int64_from_regs_le(registers, 0x04) / 1000.0

        self.kwh_plus_l1 = _fast_int64_from_regs_le(registers, 0x10) / 1000.0
        self.kwh_plus_l2 = _fast_int64_from_regs_le(registers, 0x14) / 1000.0
        self.kwh_plus_l3 = _fast_int64_from_regs_le(registers, 0x18) / 1000.0

        self.kwh_neg_total = _fast_int64_from_regs_le(registers, 0x1C) / 1000.0

        self.kvarh_neg_total = _fast_int64_from_regs_le(registers, 0x24) / 1000.0

        self.kvah_total = _fast_int64_from_regs_le(registers, 0x2C) / 1000.0

        self.run_hour_meter = _fast_int32_from_regs_le(registers, 0x34) / 100.0

        self.run_hour_meter_neg_kwh = _fast_int32_from_regs_le(registers, 0x36) / 100.0

        self.frequency = _fast_int32_from_regs_le(registers, 0x3C) / 1000.0

        self.kwh_plus_partial = _fast_int64_from_regs_le(registers, 0x08) / 1000.0

        self.kvarh_plus_partial = _fast_int64_from_regs_le(registers, 0x0C) / 1000.0

        self.kwh_neg_partial = _fast_int64_from_regs_le(registers, 0x20) / 1000.0

        self.kvarh_neg_partial = _fast_int64_from_regs_le(registers, 0x28) / 1000.0

        self.kvah_partial = _fast_int64_from_regs_le(registers, 0x30) / 1000.0

        self.run_hour_meter_partial = _fast_int32_from_regs_le(registers, 0x38) / 100.0

        self.run_hour_meter_neg_kwh_partial = _fast_int32_from_regs_le(registers, 0x3A) / 100.0

        self.run_hour_life_counter = _fast_int32_from_regs_le(registers, 0x3E) / 100.0

        # print(self)

    def __str__(self):
        return (
            f"kwh_plus_total: {self.kwh_plus_total}, "
            f"kwh_plus_l1: {self.kwh_plus_l1}, "
            f"kwh_plus_l1: {self.kwh_plus_l2}, "
            f"kwh_plus_l1: {self.kwh_plus_l3}, "
            f"kwh_neg_total: {self.kwh_neg_total}"
        )


class PhaseData:
    def __init__(self):
        self.line_line_voltage: float = 0.0
        self.line_neutral_voltage: float = 0.0
        self.current: float = 0.0
        self.power: float = 0.0
        self.apparent_power: float = 0.0
        self.reactive_power: float = 0.0
        self.power_factor: float = 0.0

    def parse(self, phase_idx, registers):  # Each block of 7 registers corresponds to a phase:
        i = phase_idx * 2 + 0x0000
        self.line_neutral_voltage = _fast_int32_from_regs_le(registers, i) / 10.0

        i = phase_idx * 2 + 0x0006
        self.line_line_voltage = _fast_int32_from_regs_le(registers, i) / 10.0

        i = phase_idx * 2 + 0x000C
        self.current = _fast_int32_from_regs_le(registers, i) / 1000.0

        i = phase_idx * 2 + 0x0012
        self.power = _fast_int32_from_regs_le(registers, i) / 10.0

        i = phase_idx * 2 + 0x0018
        self.apparent_power = _fast_int32_from_regs_le(registers, i) / 10.0

        i = phase_idx * 2 + 0x001E
        self.reactive_power = _fast_int32_from_regs_le(registers, i) / 10.0

        i = phase_idx + 0x002E
        self.power_factor = _fast_int16_from_reg(registers, i) / 1000.0

        # print(self)

    def __str__(self):
        return (
            f"Line-to-line voltage={self.line_line_voltage}, "
            f"line-to-neutral voltage={self.line_neutral_voltage}, "
            f"Current {self.current}, "
            f"power={self.power}, "
            f"apparent_power={self.apparent_power}, "
            f"reactive_power={self.reactive_power}, "
            f"power_factor={self.power_factor}"
        )


class MeterData:
    def __init__(self):
        self.frame: Em540Frame = Em540Frame()
        self.phases = [PhaseData(), PhaseData(), PhaseData()]
        self.system = SystemData()
        self.other_energies = OtherEnergies()
        self._timestamp: float = 0

    @property
    def timestamp(self) -> float:
        return self._timestamp

    @property
    def serial_number(self) -> str:
        """Extract serial number from static registers 0x5000 (8 registers).

        Decodes ASCII characters from register values, filtering out non-printable
        characters (following Victron's decoding strategy).
        """
        serial_reg = self.frame.static_reg_map.get(0x5000)
        if serial_reg is None:
            return ""

        # Try both byte orders (high-byte-first and low-byte-first)
        def decode_ascii(regs, low_byte_first):
            raw_bytes = bytearray()
            for reg in regs:
                high = (reg >> 8) & 0xFF
                low = reg & 0xFF
                if low_byte_first:
                    raw_bytes.extend((low, high))
                else:
                    raw_bytes.extend((high, low))

            # Extract only printable ASCII characters
            printable = []
            for b in raw_bytes:
                if b == 0:  # Stop at null terminator
                    break
                if 32 <= b <= 126:  # Printable ASCII range
                    printable.append(chr(b))
                else:
                    break  # Stop at first non-printable character
            return "".join(printable)

        serial_be = decode_ascii(serial_reg.values, low_byte_first=False)
        serial_le = decode_ascii(serial_reg.values, low_byte_first=True)

        # Prefer encoding that produces valid alphanumeric/hyphen strings
        def score(candidate):
            if not candidate:
                return -1
            valid_chars = sum(c.isalnum() or c in "-_" for c in candidate)
            return valid_chars * 100 + len(candidate)

        return serial_be if score(serial_be) >= score(serial_le) else serial_le

    @property
    def model_number(self) -> str:
        """Extract model number from static registers 0x000B (Device Type).

        Maps device type IDs to model names:
        - 1744-1763: EM540
        - 2096, 1956: EM530
        """
        device_type_reg = self.frame.static_reg_map.get(0x000B)
        if device_type_reg is None or not device_type_reg.values:
            return ""

        device_type_id = device_type_reg.values[0]
        if 1744 <= device_type_id <= 1763:
            return "EM540"
        elif device_type_id in [2096, 1956]:
            return "EM530"
        else:
            return ""

    @property
    def static_data_valid(self) -> bool:
        """Check if static data has been read (serial number is not all zeros)."""
        serial_number = self.serial_number
        return len(serial_number) > 0 and serial_number != "\x00" * 16

    def update_from_frame(self):
        # Kee last update timestamp
        self._timestamp = time.time()

        # Remap registers as needed
        self.frame.remap_registers()

        # Parse Phase data
        values = self.frame.dynamic_reg_map[0x0000].values

        # Calculate total current
        An = 0
        for i in range(3):
            self.phases[i].parse(i, values)

            # Compute composite values
            An += self.phases[i].current

        # Parse system data
        self.system.An = An
        self.system.parse(values)

        # Parse other energies
        values = self.frame.dynamic_reg_map[0x0500].values
        self.other_energies.parse(values)
