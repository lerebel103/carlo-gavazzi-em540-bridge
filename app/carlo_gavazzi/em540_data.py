"""
Reference document used for the Modbus mapping:
https://www.gavazziautomation.com/fileadmin/images/PIM/OTHERSTUFF/COMPRO/EM500_CPP_Mod_V1.3_13022024.pdf

The reason for this register remap is that the EM540 offers two different groupings of registers for the same values,
in different ranges:
1. A contiguous block of registers from 0x0000 to 0x0DA (see section 4.1,
"Instantaneous variables and meters (grouped by variable type)"), which we partially read up to 0x005D (94 registers).

2. A second block of registers called "Instantaneous variables and meters (grouped by phase)" (see section 4.2),
which is a remapping of the same values, but grouped by phases.

We only read the first block (1) above, and then remap the values to the second block (2) below, so that clients can
read the values in the more convenient grouping by phase. This optimizes read performance, since we can't read all
registers fast enough to keep up with a 10Hz read rate. Even then, not all registers are read, only the most relevant
ones.
"""

import logging
from typing import List

from pymodbus.client import ModbusTcpClient

logger = logging.getLogger()

ZERO_FILL = -1

_LITTLE_ENDIAN = "little"
_DYNAMIC_PRIMARY_BLOCK_ADDR = 0x0000
_ENERGY_BLOCK_ADDR = 0x0500

_STATIC_REGISTER_SPECS = (
    (0x0302, "Firmware Version and revision code", 1, 0),
    (0x000B, "Device Type", 1, 0),
    (0x1002, "Measuring System", 1, 0),
    (0x1010, "DMD Integration Time", 2, 0),
    (0x1012, "Output Alarm and Pulse Output Config", 16, 0),
    (0x1101, "Tariff Enabling", 1, 0),
    (0x1103, "Measurement Mode", 1, 0),
    (0x1104, "Wrong connection", 2, 0),
    (0x110B, "Hour Counter Configuration", 1, 0),
    (0x1150, "Terminal Block Configuration", 9, 0),
    (0x1200, "Digital Input and Active Tariff Selection", 2, 0),
    (0x1600, "Pages filter, Screen Saver and Home Page", 0x2A + 1, 0),
    (0x4100, "Offset1", 0x4 + 1, 0),
    (0x4200, "Offset2", 0x10 + 4, 0),
    (0x5000, "Serial Number", 8, 0),
    (0x5008, "Name", 8, 0),
    (0x5012, "Device State", 0x30 - 0x12 + 2, 0),
)

_EM530_STATIC_REGISTER_SPECS = ((0x1003, "CT ratio", 2, 0),)

_DYNAMIC_REGISTER_SPECS = (
    (_DYNAMIC_PRIMARY_BLOCK_ADDR, "Meter Data1", 0x34, 0),
    (_ENERGY_BLOCK_ADDR, "Meter Data3", 0x053E - 0x0500 + 2, 4),
)

_ADDITIONAL_REMAPPED_REGISTER_SPECS = (
    (0x0033, "Frequency", 1),
    (0x0110, "Frequency", 2),
    (0x0034, "kWh (+) TOT", 2),
    (0x0112, "kWh (+) TOT", 2),
    (0x0036, "Kvarh (+) TOT", 2),
    (0x0114, "Kvarh (+) TOT", 2),
    (0x003C, "kWh (+) PARTIAL", 2),
    (0x0148, "Kwh (+) PARTIAL", 2),
    (0x003E, "Kvarh (+) PARTIAL", 2),
    (0x014A, "Kvarh (+) PARTIAL", 2),
    (0x0040, "kWh (+) L1", 2),
    (0x014C, "Kwh (+) L1", 2),
    (0x0042, "kWh (+) L2", 2),
    (0x014E, "Kwh (+) L2", 2),
    (0x0044, "kWh (+) L3", 2),
    (0x0046, "kWh (+) t1. ", 2),
    (0x0048, "kWh (+) t2. ", 2),
    (0x0150, "Kwh (+) L3", 2),
    (0x004E, "KWh (-) TOT", 2),
    (0x0116, "Kwh (-) TOT", 2),
    (0x0052, "KWh (-) PARTIAL", 2),
    (0x015A, "Kwh (-) PARTIAL", 2),
    (0x0050, "kvarh (-) TOT", 2),
    (0x0118, "Kvarh (-) TOT", 2),
    (0x0054, "kvarh (-) PARTIAL", 2),
    (0x015C, "Kvarh (-) PARTIAL", 2),
    (0x0056, "KVAh TOT", 2),
    (0x015E, "KVAh TOT", 2),
    (0x0058, "KVAh PARTIAL", 2),
    (0x0160, "KVAh PARTIAL", 2),
    (0x005A, "Run hour meter", 2),
    (0x00FE, "Run hour meter", 2),
    (0x005C, "Run hour meter kWh (-)", 2),
    (0x00F6, "Run hour meter kWh (-)", 2),
    (0x006E, "Run hour meter PARTIAL", 2),
    (0x0070, "Run hour meter kWh (-) PARTIAL", 2),
)

_ENERGY_INT64_REMAPS = (
    (0, 0x0034, 0x0112, 100),
    (1, 0x0036, 0x0114, 100),
    (2, 0x003C, 0x0148, 100),
    (3, 0x003E, 0x014A, 100),
    (4, 0x0040, 0x014C, 100),
    (5, 0x0042, 0x014E, 100),
    (6, 0x0044, 0x0150, 100),
    (7, 0x004E, 0x0116, 100),
    (8, 0x0052, 0x015A, 100),
    (9, 0x0050, 0x0118, 100),
    (10, 0x0054, 0x015C, 100),
    (11, 0x0056, 0x015E, 100),
    (12, 0x0058, 0x0160, 100),
)

_ENERGY_DIRECT_COPY_REMAPS = (
    (0x34, 0x005A, 0x00FE),
    (0x36, 0x005C, 0x00F6),
    (0x38, 0x006E, None),
    (0x3A, 0x0070, None),
)


def _build_register_map(specs) -> dict[int, "RegisterDefinition"]:
    return {
        addr: RegisterDefinition(description, [0] * size, skip_n_read=skip_n_read)
        for addr, description, size, skip_n_read in specs
    }


def _build_remapped_register_map() -> dict[int, "RegisterDefinition"]:
    reg_map = {
        target_addr: RegisterDefinition(f"Reserved {hex(target_addr)}", [0]) for _, target_addr in register_remap
    }

    for addr, description, size in _ADDITIONAL_REMAPPED_REGISTER_SPECS:
        reg_map[addr] = RegisterDefinition(description, [0] * size)

    return reg_map


# Remaps registers from block 0x0000-xxxxx to block 0x0F6h-xxxx, per comments above
register_remap = [
    # V L1 - N - Value weight: Volt*10
    (0x0, 0x0120),
    (0x1, 0x0121),
    # V L2 - N - Value weight: Volt*10
    (0x2, 0x012E),
    (0x3, 0x012F),
    # V L3 - N - Value weight: Volt*10
    (0x4, 0x013C),
    (0x5, 0x013D),
    # V L1 - L2 - Value weight: Volt*10
    (0x6, 0x011E),
    (0x7, 0x011F),
    # V L2 - L3 - Value weight: Volt*10
    (0x8, 0x012C),
    (0x9, 0x012D),
    # V L3 - L1 - Value weight: Volt*10
    (0x0A, 0x013A),
    (0x0B, 0x013B),
    # A L1 - Value weight: Ampere*1000
    (0x0C, 0x0122),
    (0x0D, 0x0123),
    # A L2 - Value weight: Ampere*1000
    (0x0E, 0x0130),
    (0x0F, 0x0131),
    # A L3 - Value weight: Ampere*1000
    (0x10, 0x013E),
    (0x11, 0x013F),
    # W L1 - Value weight: Watt*10
    (0x12, 0x0124),
    (0x13, 0x0125),
    # W L2 - Value weight: Watt*10
    (0x14, 0x0132),
    (0x15, 0x0133),
    # W L3 - Value weight: Watt*10
    (0x16, 0x0140),
    (0x17, 0x0141),
    # VA L1 - Value weight: VA*10
    (0x18, 0x0126),
    (0x19, 0x0127),
    # VA L2 - Value weight: VA*10
    (0x1A, 0x0134),
    (0x1B, 0x0135),
    # VA L3 - Value weight: VA*10
    (0x1C, 0x0142),
    (0x1D, 0x0143),
    # var L1 - Value weight: var*10
    (0x1E, 0x0128),
    (0x1F, 0x0129),
    # var L2 - Value weight: var*10
    (0x20, 0x0136),
    (0x21, 0x0137),
    # var L3 - Value weight: var*10
    (0x22, 0x0144),
    (0x23, 0x0145),
    # V L-N sys - Value weight: Volt*10
    (0x24, 0x0102),
    (0x25, 0x0103),
    # V L-L sys - Value weight: Volt*10
    (0x26, 0x0104),
    (0x27, 0x0105),
    # W sys - Value weight: Watt*10
    (0x28, 0x0106),
    (0x29, 0x0107),
    # VA sys - Value weight: VA*10
    (0x2A, 0x0108),
    (0x2B, 0x0109),
    # var sys - Value weight: var*10
    (0x2C, 0x010A),
    (0x2D, 0x010B),
    # PF L1 - Value weight: PF*1000
    (ZERO_FILL, 0x012A),
    (0x2E, 0x012B),
    # PF L2 - Value weight: PF*1000
    (ZERO_FILL, 0x0138),
    (0x2F, 0x0139),
    # PF L3 - Value weight: PF*1000
    (ZERO_FILL, 0x0146),
    (0x30, 0x0147),
    # PF sys - Value weight: PF*1000
    (ZERO_FILL, 0x010C),
    (0x31, 0x010D),
    # Phase sequence
    (ZERO_FILL, 0x010E),
    (0x32, 0x010F),
]


class RegisterDefinition:
    """Class representing a Modbus register definition.
    Attributes:
        description (str): Description of the register.
        values (List[int]): List of integer values representing the register data.
        skip_n_read (int): Number of reads to skip before updating values (default is 0).
        This is used to optimize read performance for non-critical values.
    """

    def __init__(self, description: str, values: List[int], skip_n_read: int = 0) -> None:
        self.description: str = description
        self._values: List[int] = values

        # Where set, only reads every n-th cycle to give better latency and read rates overall for non critical values
        self.skip_n_read: int = skip_n_read  # Number of reads to skip before updating values

    @property
    def values(self) -> List[int]:
        return self._values

    @values.setter
    def values(self, new_values: List[int]) -> None:
        """Set new values for the register, ensuring the length matches."""
        if len(new_values) == len(self._values):
            self._values = new_values
        else:
            raise ValueError(f"Expected length of {len(self._values)} values, got {len(new_values)}")


class Em540Frame:
    """Class representing a data frame from the EM540 device, or raw registers.

    Attributes:
        is_em530 (bool): Indicates if the device is an EM530 model.
        static_reg_map (dict): A dictionary mapping static register addresses to their definitions.
        dynamic_reg_map (dict): A dictionary mapping dynamic register addresses to their definitions.
        remapped_reg_map (dict): A dictionary mapping remapped register addresses to their definitions.
    """

    def __init__(self, is_em530=False):
        self.is_em530 = is_em530

        # Static registers are read once on startup. If a device-side write changes them later,
        # a service restart is needed to refresh the cached values.
        self.static_reg_map = _build_register_map(_STATIC_REGISTER_SPECS)

        if self.is_em530:
            self.static_reg_map.update(_build_register_map(_EM530_STATIC_REGISTER_SPECS))

        self.dynamic_reg_map = _build_register_map(_DYNAMIC_REGISTER_SPECS)
        self.remapped_reg_map = _build_remapped_register_map()

    def remap_registers(self):
        """Remap registers from dynamic_reg_map to remapped_reg_map based on register_remap.
        This function copies values from the dynamic registers to the remapped registers, after new data is read
        from the device."""
        primary_values = self.dynamic_reg_map[_DYNAMIC_PRIMARY_BLOCK_ADDR].values
        energy_values = self.dynamic_reg_map[_ENERGY_BLOCK_ADDR].values
        remapped = self.remapped_reg_map

        energy_converted = ModbusTcpClient.convert_from_registers(
            energy_values[: len(_ENERGY_INT64_REMAPS) * 4],
            ModbusTcpClient.DATATYPE.INT64,
            _LITTLE_ENDIAN,
        )

        for source_index, target_addr, alias_addr, divisor in _ENERGY_INT64_REMAPS:
            converted_value = ModbusTcpClient.convert_to_registers(
                int(energy_converted[source_index] / divisor),
                ModbusTcpClient.DATATYPE.INT32,
                _LITTLE_ENDIAN,
            )
            remapped[target_addr].values = converted_value
            remapped[alias_addr].values = converted_value

        for source_offset, target_addr, alias_addr in _ENERGY_DIRECT_COPY_REMAPS:
            copied_value = energy_values[source_offset : source_offset + 2]
            remapped[target_addr].values = copied_value
            if alias_addr is not None:
                remapped[alias_addr].values = copied_value

        frequency_value = ModbusTcpClient.convert_to_registers(
            int(
                ModbusTcpClient.convert_from_registers(
                    energy_values[0x3C:0x3E],
                    ModbusTcpClient.DATATYPE.INT32,
                    _LITTLE_ENDIAN,
                )
                / 100
            ),
            ModbusTcpClient.DATATYPE.INT16,
            _LITTLE_ENDIAN,
        )
        remapped[0x0033].values = frequency_value
        remapped[0x0110].values = [frequency_value[0], 0]

        for source_addr, target_addr in register_remap:
            if target_addr not in remapped:
                raise IndexError(f"Target address {hex(target_addr)} is not in remapped_reg_map")

            if source_addr == ZERO_FILL:
                remapped[target_addr].values[0] = 0
                continue

            logger.debug(
                "Mapping %s to %s offset %s",
                hex(source_addr),
                hex(target_addr),
                source_addr,
            )
            remapped[target_addr].values[0] = primary_values[source_addr]
