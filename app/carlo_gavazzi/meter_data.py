from datetime import datetime

from pymodbus.client import ModbusTcpClient

from app.carlo_gavazzi.em540_data import Em540Frame


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

        self.line_neutral_voltage = (
            ModbusTcpClient.convert_from_registers(
                registers[0x024 : 0x024 + 2], ModbusTcpClient.DATATYPE.INT32, "little"
            )
            / 10
        )
        self.line_line_voltage = (
            ModbusTcpClient.convert_from_registers(
                registers[0x026 : 0x026 + 2], ModbusTcpClient.DATATYPE.INT32, "little"
            )
            / 10
        )
        self.power = (
            ModbusTcpClient.convert_from_registers(
                registers[0x028 : 0x028 + 2], ModbusTcpClient.DATATYPE.INT32, "little"
            )
            / 10
        )
        self.apparent_power = (
            ModbusTcpClient.convert_from_registers(
                registers[0x02A : 0x02A + 2], ModbusTcpClient.DATATYPE.INT32, "little"
            )
            / 10
        )
        self.reactive_power = (
            ModbusTcpClient.convert_from_registers(
                registers[0x02C : 0x02C + 2], ModbusTcpClient.DATATYPE.INT32, "little"
            )
            / 10
        )
        self.power_factor = (
            ModbusTcpClient.convert_from_registers(
                registers[0x031 : 0x031 + 1], ModbusTcpClient.DATATYPE.INT16, "little"
            )
            / 1000
        )
        self.frequency = (
            ModbusTcpClient.convert_from_registers(
                registers[0x033 : 0x033 + 1], ModbusTcpClient.DATATYPE.INT16, "little"
            )
            / 10
        )

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
        self.kwh_plus_total = (
            ModbusTcpClient.convert_from_registers(registers[0x00 : 0x00 + 4], ModbusTcpClient.DATATYPE.INT64, "little")
            / 1000.0
        )

        self.kvarh_plus_total = (
            ModbusTcpClient.convert_from_registers(registers[0x04 : 0x04 + 4], ModbusTcpClient.DATATYPE.INT64, "little")
            / 1000.0
        )

        self.kwh_plus_l1 = (
            ModbusTcpClient.convert_from_registers(registers[0x10 : 0x10 + 4], ModbusTcpClient.DATATYPE.INT64, "little")
            / 1000.0
        )
        self.kwh_plus_l2 = (
            ModbusTcpClient.convert_from_registers(registers[0x14 : 0x14 + 4], ModbusTcpClient.DATATYPE.INT64, "little")
            / 1000.0
        )
        self.kwh_plus_l3 = (
            ModbusTcpClient.convert_from_registers(registers[0x18 : 0x18 + 4], ModbusTcpClient.DATATYPE.INT64, "little")
            / 1000.0
        )

        self.kwh_neg_total = (
            ModbusTcpClient.convert_from_registers(registers[0x1C : 0x1C + 4], ModbusTcpClient.DATATYPE.INT64, "little")
            / 1000.0
        )

        self.kvarh_neg_total = (
            ModbusTcpClient.convert_from_registers(registers[0x24 : 0x24 + 4], ModbusTcpClient.DATATYPE.INT64, "little")
            / 1000.0
        )

        self.kvah_total = (
            ModbusTcpClient.convert_from_registers(registers[0x2C : 0x2C + 4], ModbusTcpClient.DATATYPE.INT64, "little")
            / 1000.0
        )

        self.run_hour_meter = (
            ModbusTcpClient.convert_from_registers(registers[0x34 : 0x34 + 2], ModbusTcpClient.DATATYPE.INT32, "little")
            / 100.0
        )

        self.run_hour_meter_neg_kwh = (
            ModbusTcpClient.convert_from_registers(registers[0x36 : 0x36 + 2], ModbusTcpClient.DATATYPE.INT32, "little")
            / 100.0
        )

        self.frequency = (
            ModbusTcpClient.convert_from_registers(registers[0x3C : 0x3C + 2], ModbusTcpClient.DATATYPE.INT32, "little")
            / 1000.0
        )

        self.kwh_plus_partial = (
            ModbusTcpClient.convert_from_registers(registers[0x08 : 0x08 + 4], ModbusTcpClient.DATATYPE.INT64, "little")
            / 1000.0
        )

        self.kvarh_plus_partial = (
            ModbusTcpClient.convert_from_registers(registers[0x0C : 0x0C + 4], ModbusTcpClient.DATATYPE.INT64, "little")
            / 1000.0
        )

        self.kwh_neg_partial = (
            ModbusTcpClient.convert_from_registers(registers[0x20 : 0x20 + 4], ModbusTcpClient.DATATYPE.INT64, "little")
            / 1000.0
        )

        self.kvarh_neg_partial = (
            ModbusTcpClient.convert_from_registers(registers[0x28 : 0x28 + 4], ModbusTcpClient.DATATYPE.INT64, "little")
            / 1000.0
        )

        self.kvah_partial = (
            ModbusTcpClient.convert_from_registers(registers[0x30 : 0x30 + 4], ModbusTcpClient.DATATYPE.INT64, "little")
            / 1000.0
        )

        self.run_hour_meter_partial = (
            ModbusTcpClient.convert_from_registers(registers[0x38 : 0x38 + 2], ModbusTcpClient.DATATYPE.INT32, "little")
            / 100.0
        )

        self.run_hour_meter_neg_kwh_partial = (
            ModbusTcpClient.convert_from_registers(registers[0x3A : 0x3A + 2], ModbusTcpClient.DATATYPE.INT32, "little")
            / 100.0
        )

        self.run_hour_life_counter = (
            ModbusTcpClient.convert_from_registers(registers[0x3E : 0x3E + 2], ModbusTcpClient.DATATYPE.INT32, "little")
            / 100.0
        )

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
        self.line_neutral_voltage = (
            ModbusTcpClient.convert_from_registers(registers[i : i + 2], ModbusTcpClient.DATATYPE.INT32, "little")
            / 10.0
        )

        i = phase_idx * 2 + 0x0006
        self.line_line_voltage = (
            ModbusTcpClient.convert_from_registers(registers[i : i + 2], ModbusTcpClient.DATATYPE.INT32, "little")
            / 10.0
        )

        i = phase_idx * 2 + 0x000C
        self.current = (
            ModbusTcpClient.convert_from_registers(registers[i : i + 2], ModbusTcpClient.DATATYPE.INT32, "little")
            / 1000.0
        )

        i = phase_idx * 2 + 0x0012
        self.power = (
            ModbusTcpClient.convert_from_registers(registers[i : i + 2], ModbusTcpClient.DATATYPE.INT32, "little")
            / 10.0
        )

        i = phase_idx * 2 + 0x0018
        self.apparent_power = (
            ModbusTcpClient.convert_from_registers(registers[i : i + 2], ModbusTcpClient.DATATYPE.INT32, "little")
            / 10.0
        )

        i = phase_idx * 2 + 0x001E
        self.reactive_power = (
            ModbusTcpClient.convert_from_registers(registers[i : i + 2], ModbusTcpClient.DATATYPE.INT32, "little")
            / 10.0
        )

        i = phase_idx + 0x002E
        self.power_factor = (
            ModbusTcpClient.convert_from_registers(registers[i : i + 1], ModbusTcpClient.DATATYPE.INT16, "little")
            / 1000.0
        )

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

    def update_from_frame(self):
        # Kee last update timestamp
        self._timestamp = datetime.now().timestamp()

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
