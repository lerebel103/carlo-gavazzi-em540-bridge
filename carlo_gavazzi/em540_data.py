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

    def __init__(
        self, description: str, values: List[int], skip_n_read: int = 0
    ) -> None:
        self.description: str = description
        self._values: List[int] = values

        # Where set, only reads every n-th cycle to give better latency and read rates overall for non critical values
        self.skip_n_read: int = (
            skip_n_read  # Number of reads to skip before updating values
        )

    @property
    def values(self) -> List[int]:
        return self._values

    @values.setter
    def values(self, new_values: List[int]) -> None:
        """Set new values for the register, ensuring the length matches."""
        if len(new_values) == len(self._values):
            self._values = new_values
        else:
            raise ValueError(
                f"Expected length of {len(self._values)} values, got {len(new_values)}"
            )

def _conv_helper_64_32(values, weight):
    """Helper function to convert a list of registers to a single integer value."""
    print("Converted to: ", ModbusTcpClient.convert_from_registers(values, ModbusTcpClient.DATATYPE.INT64, "little"))
    return ModbusTcpClient.convert_to_registers(
        ModbusTcpClient.convert_from_registers(values, ModbusTcpClient.DATATYPE.INT64, "little")[0] / weight,
        ModbusTcpClient.DATATYPE.INT32, "little")

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

        # Define the registers that are static and only read once on startup.
        # Some of these may however be updated later via a modbus write command, but this bridge would be unaware of
        # that.
        # A service restart would be needed to re-read them.
        self.static_reg_map = {
            0x0302: RegisterDefinition("Firmware Version and revision code", [0] * 1),
            0x000B: RegisterDefinition("Device Type", [0] * 1),
            0x1002: RegisterDefinition("Measuring System", [0] * 1),
            0x1010: RegisterDefinition("DMD Integration Time", [0] * 2),
            0x1012: RegisterDefinition(
                "Output Alarm and Pulse Output Config", [0] * 16
            ),
            0x1101: RegisterDefinition("Tariff Enabling", [0] * 1),
            0x1103: RegisterDefinition("Measurement Mode", [0] * 1),
            0x1104: RegisterDefinition("Wrong connection", [0] * 2),
            0x110B: RegisterDefinition("Hour Counter Configuration", [0] * 1),
            0x1150: RegisterDefinition("Terminal Block Configuration", [0] * 9),
            0x1200: RegisterDefinition(
                "Digital Input and Active Tariff Selection", [0] * 2
            ),
            0x1600: RegisterDefinition(
                "Pages filter, Screen Saver and Home Page", [0] * (0x2A + 1)
            ),
            # Serial port config and reset command omitted
            0x4100: RegisterDefinition("Offset1", [0] * (0x4 + 1)),
            0x4200: RegisterDefinition("Offset2", [0] * (0x10 + 4)),
            0x5000: RegisterDefinition("Serial Number", [0] * 8),
            0x5008: RegisterDefinition("Name", [0] * 8),
            0x5012: RegisterDefinition("Device State", [0] * (0x30 - 0x12 + 2)),
        }

        if self.is_em530:
            self.static_reg_map.update(
                {
                    0x1003: RegisterDefinition("CT ratio", [0] * 2),  # only for EM530
                }
            )

        # Define our dynamic registers that are read often
        self.dynamic_reg_map = {
            # Reads the registers from 0x0000 to 0x0032 - up to "Phase sequence"
            0x0000: RegisterDefinition("Meter Data1", [0] * 0x34),
            # Reads Other Instantaneous variables and meters (read only), section 4.2
            0x0500: RegisterDefinition("Meter Data3", [0] * (0x053E - 0x0500 + 2)),
        }

        # Define registers that are re-mapped in different ranges, there are duplicated registers in the EM540
        # See comments at the top of this file for more details
        self.remapped_reg_map = {}
        for item in register_remap:
            target_addr = item[1]
            self.remapped_reg_map[target_addr] = RegisterDefinition(
                f"Reserved {hex(target_addr)}", [0]
            )

        # Other mappings requiring us to transform data from dynamic registers
        self.remapped_reg_map[0x0033] = RegisterDefinition("Frequency", [0] * 1)
        self.remapped_reg_map[0x0110] = RegisterDefinition("Frequency", [0]* 2)
        self.remapped_reg_map[0x0034] = RegisterDefinition("kWh (+) TOT", [0] * 2)
        self.remapped_reg_map[0x0112] = RegisterDefinition("kWh (+) TOT", [0]*2)
        self.remapped_reg_map[0x0036] = RegisterDefinition("Kvarh (+) TOT", [0] * 2)
        self.remapped_reg_map[0x0114] = RegisterDefinition("Kvarh (+) TOT", [0]*2)
        self.remapped_reg_map[0x003C] = RegisterDefinition("kWh (+) PARTIAL", [0] * 2)
        self.remapped_reg_map[0x0148] = RegisterDefinition("Kwh (+) PARTIAL", [0]*2)
        self.remapped_reg_map[0x003E] = RegisterDefinition("Kvarh (+) PARTIAL", [0] * 2)
        self.remapped_reg_map[0x014A] = RegisterDefinition("Kvarh (+) PARTIAL", [0]*2)
        self.remapped_reg_map[0x0040] = RegisterDefinition("kWh (+) L1", [0] * 2)
        self.remapped_reg_map[0x014C] = RegisterDefinition("Kwh (+) L1", [0]*2)
        self.remapped_reg_map[0x0042] = RegisterDefinition("kWh (+) L2", [0] * 2)
        self.remapped_reg_map[0x014E] = RegisterDefinition("Kwh (+) L2", [0]*2)
        self.remapped_reg_map[0x0044] = RegisterDefinition("kWh (+) L3", [0] * 2)
        self.remapped_reg_map[0x0150] = RegisterDefinition("Kwh (+) L3", [0]*2)
        self.remapped_reg_map[0x004E] = RegisterDefinition("KWh (-) TOT", [0] * 2)
        self.remapped_reg_map[0x0116] = RegisterDefinition("Kwh (-) TOT", [0]*2)
        self.remapped_reg_map[0x0052] = RegisterDefinition("KWh (-) PARTIAL", [0] * 2)
        self.remapped_reg_map[0x015A] = RegisterDefinition("Kwh (-) PARTIAL", [0]*2)
        self.remapped_reg_map[0x0050] = RegisterDefinition("kvarh (-) TOT", [0] * 2)
        self.remapped_reg_map[0x0118] = RegisterDefinition("Kvarh (-) TOT", [0]*2)
        self.remapped_reg_map[0x0054] = RegisterDefinition("kvarh (-) PARTIAL", [0] * 2)
        self.remapped_reg_map[0x015C] = RegisterDefinition("Kvarh (-) PARTIAL", [0]*2)
        self.remapped_reg_map[0x0056] = RegisterDefinition("KVAh TOT", [0] * 2)
        self.remapped_reg_map[0x015E] = RegisterDefinition("KVAh TOT", [0]*2)
        self.remapped_reg_map[0x0058] = RegisterDefinition("KVAh PARTIAL", [0] * 2)
        self.remapped_reg_map[0x0160] = RegisterDefinition("KVAh PARTIAL", [0]*2)
        self.remapped_reg_map[0x005A] = RegisterDefinition("Run hour meter", [0] * 2)
        self.remapped_reg_map[0x00FE] = RegisterDefinition("Run hour meter", [0]*2)
        self.remapped_reg_map[0x005C] = RegisterDefinition("Run hour meter kWh (-)", [0] * 2)
        self.remapped_reg_map[0x00F6] = RegisterDefinition("Run hour meter kWh (-)", [0]*2)
        self.remapped_reg_map[0x006E] = RegisterDefinition("Run hour meter PARTIAL", [0] * 2)
        self.remapped_reg_map[0x0070] = RegisterDefinition("Run hour meter kWh (-) PARTIAL", [0] * 2)



    def remap_registers(self):
        """Remap registers from dynamic_reg_map to remapped_reg_map based on register_remap.
        This function copies values from the dynamic registers to the remapped registers, after new data is read
        from the device."""

        # First transform the data from the 0x500 range to the 0x0000 block, for fields that are missing, these
        # are read from the Other Energies block, which are more accurate (64-bit values).
        src_values = self.dynamic_reg_map[0x0500].values
        offset = 0x500

        # Convert the first 13 values, consequtively, from INT64 to INT32, applying the appropriate weight
        start_ix = 0x500 - offset
        end_ix = 0x500 + 13 * 4 - offset
        converted = ModbusTcpClient.convert_from_registers(
            src_values[start_ix:end_ix], ModbusTcpClient.DATATYPE.INT64, "little")

        idx = 0
        # 0500h	4	0034h	2	kWh (+) TOT	INT64	Value weight: Wh
        self.remapped_reg_map[0x0034].values = ModbusTcpClient.convert_to_registers(int(converted[idx] / 100), ModbusTcpClient.DATATYPE.INT32, "little")
        self.remapped_reg_map[0x112].values = self.remapped_reg_map[0x0034].values
        idx += 1
        # 0504h	4	0036h	2	Kvarh (+) TOT	INT64	Value weight: VARh
        self.remapped_reg_map[0x0036].values = ModbusTcpClient.convert_to_registers(int(converted[idx] / 100), ModbusTcpClient.DATATYPE.INT32, "little")
        self.remapped_reg_map[0x114].values = self.remapped_reg_map[0x0036].values
        idx += 1
        # 0508h	4	003Ch	2	kWh (+) PARTIAL	INT64	Value weight: Wh
        self.remapped_reg_map[0x003C].values = ModbusTcpClient.convert_to_registers(int(converted[idx] / 100), ModbusTcpClient.DATATYPE.INT32, "little")
        self.remapped_reg_map[0x148].values = self.remapped_reg_map[0x003C].values
        idx += 1
        # 050Ch	4	003Eh	2	Kvarh (+) PARTIAL	INT64	Value weight: VARh
        self.remapped_reg_map[0x003E].values = ModbusTcpClient.convert_to_registers(int(converted[idx] / 100), ModbusTcpClient.DATATYPE.INT32, "little")
        self.remapped_reg_map[0x14A].values = self.remapped_reg_map[0x003E].values
        idx += 1
        # 0510h	4	0040h	2	kWh (+) L1	INT64
        self.remapped_reg_map[0x0040].values = ModbusTcpClient.convert_to_registers(int(converted[idx] / 100), ModbusTcpClient.DATATYPE.INT32, "little")
        self.remapped_reg_map[0x14C].values = self.remapped_reg_map[0x0040].values
        idx += 1
        # 0514h	4	0042h	2	kWh (+) L2	INT64	Value weight: Wh
        self.remapped_reg_map[0x0042].values = ModbusTcpClient.convert_to_registers(int(converted[idx] / 100), ModbusTcpClient.DATATYPE.INT32, "little")
        self.remapped_reg_map[0x14E].values = self.remapped_reg_map[0x0042].values
        idx += 1
        # 0518h	4	0044h	2	kWh (+) L3	INT64
        self.remapped_reg_map[0x0044].values = ModbusTcpClient.convert_to_registers(int(converted[idx] / 100), ModbusTcpClient.DATATYPE.INT32, "little")
        self.remapped_reg_map[0x150].values = self.remapped_reg_map[0x0044].values
        idx += 1
        # 051Ch	4	004Eh	2	KWh (-) TOT	INT64	Value weight: Wh
        self.remapped_reg_map[0x004E].values = ModbusTcpClient.convert_to_registers(int(converted[idx] / 100), ModbusTcpClient.DATATYPE.INT32, "little")
        self.remapped_reg_map[0x116].values = self.remapped_reg_map[0x004E].values
        idx += 1
        # 0520h	4	0052h	2	KWh (-) PARTIAL	INT64	Value weight: Wh
        self.remapped_reg_map[0x0052].values = ModbusTcpClient.convert_to_registers(int(converted[idx] / 100), ModbusTcpClient.DATATYPE.INT32, "little")
        self.remapped_reg_map[0x15A].values = self.remapped_reg_map[0x0052].values
        idx += 1
        # 0524h	4	0050h	2	kvarh (-) TOT	INT64	Value weight: varh
        self.remapped_reg_map[0x0050].values = ModbusTcpClient.convert_to_registers(int(converted[idx] / 100), ModbusTcpClient.DATATYPE.INT32, "little")
        self.remapped_reg_map[0x118].values = self.remapped_reg_map[0x0050].values

        idx += 1
        # 0528h	4	0054h	2	kvarh (-) Partial	INT64	Value weight: varh
        self.remapped_reg_map[0x0054].values = ModbusTcpClient.convert_to_registers(int(converted[idx] / 100), ModbusTcpClient.DATATYPE.INT32, "little")
        self.remapped_reg_map[0x15C].values = self.remapped_reg_map[0x0054].values
        idx += 1
        # 052Ch	4	0056h	2	KVAh TOT	INT64	Value weight: VAh
        self.remapped_reg_map[0x0056].values = ModbusTcpClient.convert_to_registers(int(converted[idx] / 100), ModbusTcpClient.DATATYPE.INT32, "little")
        self.remapped_reg_map[0x15E].values = self.remapped_reg_map[0x0056].values
        idx += 1
        # 0530h	4	0058h	2	KVAh PARTIAL	INT64	Value weight: VAh
        self.remapped_reg_map[0x0058].values = ModbusTcpClient.convert_to_registers(int(converted[idx] / 100), ModbusTcpClient.DATATYPE.INT32, "little")
        self.remapped_reg_map[0x160].values = self.remapped_reg_map[0x0058].values

        # These are straight copies
        # 0534h	2	005Ah	2	Run hour meter	INT32	Value weight: hours*100
        self.remapped_reg_map[0x005A].values = src_values[0x534-offset:0x536-offset]
        self.remapped_reg_map[0x00FE].values = self.remapped_reg_map[0x005A].values
        # 0536h	2	005Ch	2	Run hour meter kWh (-)	INT32	Value weight: hours*100
        self.remapped_reg_map[0x005C].values = src_values[0x536-offset:0x538-offset]
        self.remapped_reg_map[0x00F6].values = self.remapped_reg_map[0x005C].values

        # 0538h	2	006Eh	2	Run hour meter PARTIAL	INT32	Value weight: hours*100
        self.remapped_reg_map[0x006E].values = src_values[0x538-offset:0x53A-offset]
        # 053Ah	2	0070h	2	"Run hour meter kWh (-) PARTIAL"	INT32	Value weight: hours*100
        self.remapped_reg_map[0x0070].values = src_values[0x53A-offset:0x53C-offset]

        # Frequency - stored as INT16, value weight Hz*100
        # 053Ch	2	0033h	1	Hz	INT32	Value weight: Hz*1000
        self.remapped_reg_map[0x0033].values = ModbusTcpClient.convert_to_registers(
            int(ModbusTcpClient.convert_from_registers(src_values[0x53C-offset:0x53E-offset], ModbusTcpClient.DATATYPE.INT32, "little") / 100)
            , ModbusTcpClient.DATATYPE.INT16, "little")
        self.remapped_reg_map[0x0110].values = [0, self.remapped_reg_map[0x0033].values[0]]

        # Now perform the remap as per register_remap
        for item in register_remap:
            source_addr = item[0]
            target_addr = item[1]

            # Find the reg definition for the target address
            if target_addr in self.remapped_reg_map:
                target_reg_def = self.remapped_reg_map[target_addr]

                # Find source reg definition
                if source_addr == ZERO_FILL or target_addr == ZERO_FILL:
                    # Fill with zeroes
                    target_reg_def.values[0] = 0
                else:
                    source_key = 0x0000
                    source_reg_def = self.dynamic_reg_map[source_key]

                    offset = source_addr - source_key
                    logger.debug(
                        "Mapping "
                        + hex(source_addr)
                        + " to "
                        + hex(target_addr)
                        + " offset "
                        + str(offset)
                    )
                    target_reg_def.values[0] = source_reg_def.values[offset]

            else:
                raise IndexError(
                    f"Target address {hex(target_addr)} is not in remapped_reg_map"
                )
