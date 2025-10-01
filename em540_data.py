import logging

logger = logging.getLogger()

ZERO_FILL = -1

register_remap = [
    # V L1 - N - Value weight: Volt*10
    (0x0, 0x0120), (0x1, 0x0121),
    # V L2 - N - Value weight: Volt*10
    (0x2, 0x012E), (0x3, 0x012F),
    # V L3 - N - Value weight: Volt*10
    (0x4, 0x013C), (0x5, 0x013D),

    # V L1 - L2 - Value weight: Volt*10
    (0x6, 0x011E), (0x7, 0x011F),
    # V L2 - L3 - Value weight: Volt*10
    (0x8, 0x012C), (0x9, 0x012D),
    # V L3 - L1 - Value weight: Volt*10
    (0x0A, 0x013A), (0x0B, 0x013B),

    # A L1 - Value weight: Ampere*1000
    (0x0C, 0x0122), (0x0D, 0x0123),
    # A L2 - Value weight: Ampere*1000
    (0x0E, 0x0130), (0x0F, 0x0131),
    # A L3 - Value weight: Ampere*1000
    (0x10, 0x013E), (0x11, 0x013F),

    # W L1 - Value weight: Watt*10
    (0x12, 0x0124), (0x13, 0x0125),
    # W L2 - Value weight: Watt*10
    (0x14, 0x0132), (0x15, 0x0133),
    # W L3 - Value weight: Watt*10
    (0x16, 0x0140), (0x17, 0x0141),

    # VA L1 - Value weight: VA*10
    (0x18, 0x0126), (0x19, 0x0127),
    # VA L2 - Value weight: VA*10
    (0x1A, 0x0134), (0x1B, 0x0135),
    # VA L3 - Value weight: VA*10
    (0x1C, 0x0142), (0x1D, 0x0143),

    # var L1 - Value weight: var*10
    (0x1E, 0x0128), (0x1F, 0x0129),
    # var L2 - Value weight: var*10
    (0x20, 0x0136), (0x21, 0x0137),
    # var L3 - Value weight: var*10
    (0x22, 0x0144), (0x23, 0x0145),

    # V L-N sys - Value weight: Volt*10
    (0x24, 0x0102), (0x25, 0x0103),
    # V L-L sys - Value weight: Volt*10
    (0x26, 0x0104), (0x27, 0x0105),
    # W sys - Value weight: Watt*10
    (0x28, 0x0106), (0x29, 0x0107),
    # VA sys - Value weight: VA*10
    (0x2A, 0x0108), (0x2B, 0x0109),
    # var sys - Value weight: var*10
    (0x2C, 0x010A), (0x2D, 0x010B),
    # PF L1 - Value weight: PF*1000
    (ZERO_FILL, 0x012A), (0x2E, 0x012B), # TODO must check that only lower part works in conversion
    # PF L2 - Value weight: PF*1000
    (ZERO_FILL, 0x0138), (0x2F, 0x0139), # TODO must check that only lower part works in conversion
    # PF L3 - Value weight: PF*1000
    (ZERO_FILL, 0x0146), (0x30, 0x0147), # TODO must check that only lower part works in conversion
    # PF sys - Value weight: PF*1000
    (ZERO_FILL, 0x010C), (0x31, 0x010D), # TODO must check that only lower part works in conversion
    # Phase sequence
    (ZERO_FILL, 0x010E), (0x32, 0x010F), # TODO must check that only lower part works in conversion
    # Frequency - Value weight: Hz*100
    (ZERO_FILL, 0x0110),(0x33, 0x0111), # TODO must check that only lower part works in conversion

    # Kw+ Total - Value weight: kWh*10
    (0x34, 0x0112), (0x35, 0x00113),
    # Kvar+ Total - Value weight: kvarh*10
    (0x36, 0x0114), (0x37, 0x0115),
    # W sys DMD - Value weight: Watt*10
    (0x38, 0x011A), (0x39, 0x011B),
    # W sys DMD Max - Value weight: Watt*10
    (0x3A, 0x011C), (0x3B, 0x011D),

    # Kwh (+) PARTIAL - Value weight: kWh*10
    (0x3C, 0x0148), (0x3D, 0x0149),
    # Kvarh (+) PARTIAL - Value weight: kvarh*10
    (0x3E, 0x014A), (0x3F, 0x014B),
    # Kwh (+) L1 - Value weight: kWh*10
    (0x40, 0x014C), (0x41, 0x014D),
    # Kwh (+) L2 - Value weight: kWh*10
    (0x42, 0x014E), (0x43, 0x014F),
    # Kwh (+) L3 - Value weight: kWh*10
    (0x44, 0x0150), (0x45, 0x0151),
    # Kwh (+) t1 - Value weight: kWh*10
    (0x46, 0x0152), (0x47, 0x0153),
    # Kwh (+) t2 - Value weight: kWh*10
    (0x48, 0x0154), (0x49, 0x0155),
    # n.a.
    (0x4A, ZERO_FILL), (0x4B, ZERO_FILL),
    # n.a.
    (0x4C, ZERO_FILL), (0x4D, ZERO_FILL),

    # Kwh (-) Total - Value weight: kWh*10
    (0x4E, 0x0116), (0x4F, 0x0117),
    # Kvarh (-) Total - Value weight: kvarh*10
    (0x50, 0x0118), (0x51, 0x0119),
    # Kwh (-) PARTIAL - Value weight: kWh*10
    (0x52, 0x015A), (0x53, 0x015B),
    # Kvarh (-) PARTIAL - Value weight: kvarh*10
    (0x54, 0x015C), (0x55, 0x015D),
    # KVah Total - Value weight: kVAh*10
    (0x56, 0x015E), (0x57, 0x015F),
    # KVAh partial - Value weight: kVAh*10
    (0x58, 0x0160), (0x59, 0x0161),

    # Run hour meter, Value weight: hours*100
    # (0x5C, 0x00FE), (0x5D, 0x00FF),
    # Run hour meter KWh (-), Value weight: hours*100
    # (0x5A, 0x00F6), (0x5B, 0x00F7),

    # n.a.
    # (0x5E, ZERO_FILL), (0x5F, ZERO_FILL),
    # n.a.
    # (0x60, ZERO_FILL), (0x61, ZERO_FILL),
    # n.a.
    # (0x62, ZERO_FILL), (0x63, ZERO_FILL),
    # n.a.
    # (0x64, ZERO_FILL), (0x65, ZERO_FILL),
    # n.a.
    # (0x66, ZERO_FILL), (0x67, ZERO_FILL),
    # n.a.
    # (0x68, ZERO_FILL), (0x69, ZERO_FILL),
    # n.a.
    # (0x6A, ZERO_FILL), (0x6B, ZERO_FILL),
    # n.a.
    # (0x6C, ZERO_FILL), (0x6D, ZERO_FILL),

    # Run hour meter partial
    # (0x6E, 0x00F8), (0x6F, 0x00F9),
]

class RegisterDefinition:
    def __init__(self, description, values, skip_n_read=0):
        self.description: str = description
        self._values: list[int] = values

        # Where set, only reads every n-th cycle to give better latency and read rates overall for non critical values
        self.skip_n_read: int = skip_n_read  # Number of reads to skip before updating values

    @property
    def values(self) -> list[int]:
        return self._values

    @values.setter
    def values(self, new_values: list[int]) -> None:
        if len(new_values) == len(self._values):
            # Copy each value to avoid reference issues
            for i in range(len(self._values)):
                self._values[i] = new_values[i]
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

        # Define the registers that are static and only read once on startup.
        # Some of these may however be updated later via a modbus write command.
        self.static_reg_map = {
            0x0302: RegisterDefinition("Firmware Version and revision code", [0] * 1),
            0x000B: RegisterDefinition("Device Type", [0] * 1),
            0x1002: RegisterDefinition("Measuring System", [0] * 1),
            0x1010: RegisterDefinition("DMD Integration Time", [0] * 2),
            0x1012: RegisterDefinition("Output Alarm and Pulse Output Config", [0] * 16),
            0x1101: RegisterDefinition("Tariff Enabling", [0] * 1),
            0x1103: RegisterDefinition("Measurement Mode", [0] * 1),
            0x1104: RegisterDefinition("Wrong connection", [0] * 2),
            0x110B: RegisterDefinition("Hour Counter Configuration", [0] * 1),
            0x1150: RegisterDefinition("Terminal Block Configuration", [0] * 9),
            0x1200: RegisterDefinition("Digital Input and Active Tariff Selection", [0] * 2),
            0x1600: RegisterDefinition("Pages filter, Screen Saver and Home Page", [0] * (0x2A + 1)),
            # Serial port config and reset command omitted
            0x4100: RegisterDefinition("Offset1", [0] * (0x4 + 1)),
            0x4200: RegisterDefinition("Offset2", [0] * (0x10 + 4)),
            0x5000: RegisterDefinition("Serial Number", [0] * 8),
            0x5008: RegisterDefinition("Name", [0] * 8),
            0x5012: RegisterDefinition("Device State", [0] * (0x30 - 0x12 + 2)),

        }

        if self.is_em530:
            self.static_reg_map.update({
                0x1003: RegisterDefinition("CT ratio", [0] * 2),  # only for EM530
            })

        # Define our dynamic registers that are read often
        self.dynamic_reg_map = {
            0x0000: RegisterDefinition("Meter Data1", [0] * 0x5A),
            0x0500: RegisterDefinition("Meter Data3", [0] * (0x053E - 0x0500 + 2), skip_n_read=4),
        }

        # Define registers that are re-mapped in different ranges, these will be populated manually
        # by copying the values from the dynamic registers above
        self.remapped_reg_map = {}
        for item in register_remap:
            target_addr = item[1]
            self.remapped_reg_map[target_addr] = RegisterDefinition(f"Reserved {hex(target_addr)}", [0])

    def remap_registers(self):
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
                    if source_addr < 0x006E:
                        source_key = 0x0000
                    elif source_addr >= 0x006E:
                        source_key = 0x006E
                    else:
                        raise IndexError(f"Source address {hex(source_addr)} is not in dynamic_reg_map")

                    source_reg_def = self.dynamic_reg_map[source_key]
                    offset = source_addr - source_key
                    logger.debug("Mapping " + hex(source_addr) + " to " + hex(target_addr) + " offset " + str(offset))
                    target_reg_def.values[0] = source_reg_def.values[offset]

            else:
                raise IndexError(f"Target address {hex(target_addr)} is not in remapped_reg_map")

