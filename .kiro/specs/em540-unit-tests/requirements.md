# Requirements Document

## Introduction

This document defines the requirements for unit tests covering the Carlo Gavazzi EM540 energy meter bridge project. The tests ensure that register reads match the Gavazzi EM540 Modbus specification (EM500_CPP_Mod_V1.3), that register remapping between the two register layouts is correct, and that the main controller tick loop maintains priority over secondary operations to sustain a 10Hz read rate.

## Glossary

- **EM540**: Carlo Gavazzi EM540 three-phase energy meter
- **Register_Spec**: The Gavazzi EM540 Modbus specification document (EM500_CPP_Mod_V1.3_13022024.pdf) defining register addresses, data types, and value weights
- **PhaseData_Parser**: The `PhaseData.parse()` method that extracts per-phase electrical measurements from raw Modbus registers
- **SystemData_Parser**: The `SystemData.parse()` method that extracts system-level aggregate measurements from raw Modbus registers
- **OtherEnergies_Parser**: The `OtherEnergies.parse()` method that extracts energy counters and run-hour meters from the 0x0500 register block
- **Register_Remapper**: The `Em540Frame.remap_registers()` method that copies values from the "grouped by variable type" layout (0x0000 block) to the "grouped by phase" layout (0x0F6h+ block)
- **Em540_Master**: The `Em540Master` class that reads registers from the EM540 device over Modbus and notifies listeners
- **Main_Loop**: The `process_loop()` async function in `main.py` that drives the periodic data acquisition cycle
- **Slave_Bridge**: The `Em540Slave` class that serves acquired data to downstream Modbus clients via a datastore
- **Slave_Stats**: The `EM540SlaveStats` class that tracks RTU and TCP client connection counts
- **RegisterDefinition**: The `RegisterDefinition` class that holds register metadata and enforces value length constraints
- **Skip_N_Read**: An optimization where non-critical registers are read only every N-th cycle to maintain high read rates
- **ZERO_FILL**: A sentinel value (-1) in the register remap table indicating the target register should be filled with zero
- **Value_Weight**: A scaling factor applied to raw register values to convert to engineering units (e.g., Volt*10, Ampere*1000)
- **Notify_Thread**: The daemon thread in Em540Master that processes listener callbacks via `threading.Condition`

## Requirements

### Requirement 1: Phase Data Register Parsing

**User Story:** As a developer, I want to verify that PhaseData correctly parses raw Modbus registers, so that per-phase electrical measurements match the EM540 register specification.

#### Acceptance Criteria

1. WHEN the PhaseData_Parser parses registers for a given phase index, THE PhaseData_Parser SHALL read line-to-neutral voltage from register offset `phase_idx * 2 + 0x0000` as INT32 little-endian and divide by 10
2. WHEN the PhaseData_Parser parses registers for a given phase index, THE PhaseData_Parser SHALL read line-to-line voltage from register offset `phase_idx * 2 + 0x0006` as INT32 little-endian and divide by 10
3. WHEN the PhaseData_Parser parses registers for a given phase index, THE PhaseData_Parser SHALL read current from register offset `phase_idx * 2 + 0x000C` as INT32 little-endian and divide by 1000
4. WHEN the PhaseData_Parser parses registers for a given phase index, THE PhaseData_Parser SHALL read power from register offset `phase_idx * 2 + 0x0012` as INT32 little-endian and divide by 10
5. WHEN the PhaseData_Parser parses registers for a given phase index, THE PhaseData_Parser SHALL read apparent power from register offset `phase_idx * 2 + 0x0018` as INT32 little-endian and divide by 10
6. WHEN the PhaseData_Parser parses registers for a given phase index, THE PhaseData_Parser SHALL read reactive power from register offset `phase_idx * 2 + 0x001E` as INT32 little-endian and divide by 10
7. WHEN the PhaseData_Parser parses registers for a given phase index, THE PhaseData_Parser SHALL read power factor from register offset `phase_idx + 0x002E` as INT16 little-endian and divide by 1000
8. WHEN the PhaseData_Parser parses registers containing negative power values, THE PhaseData_Parser SHALL produce negative floating-point power output

### Requirement 2: System Data Register Parsing

**User Story:** As a developer, I want to verify that SystemData correctly parses raw Modbus registers, so that system-level aggregate measurements match the EM540 register specification.

#### Acceptance Criteria

1. WHEN the SystemData_Parser parses registers, THE SystemData_Parser SHALL read system line-to-neutral voltage from register offset 0x024 as INT32 little-endian and divide by 10
2. WHEN the SystemData_Parser parses registers, THE SystemData_Parser SHALL read system line-to-line voltage from register offset 0x026 as INT32 little-endian and divide by 10
3. WHEN the SystemData_Parser parses registers, THE SystemData_Parser SHALL read system power from register offset 0x028 as INT32 little-endian and divide by 10
4. WHEN the SystemData_Parser parses registers, THE SystemData_Parser SHALL read system apparent power from register offset 0x02A as INT32 little-endian and divide by 10
5. WHEN the SystemData_Parser parses registers, THE SystemData_Parser SHALL read system reactive power from register offset 0x02C as INT32 little-endian and divide by 10
6. WHEN the SystemData_Parser parses registers, THE SystemData_Parser SHALL read system power factor from register offset 0x031 as INT16 little-endian and divide by 1000
7. WHEN the SystemData_Parser parses registers, THE SystemData_Parser SHALL read frequency from register offset 0x033 as INT16 little-endian and divide by 10

### Requirement 3: Other Energies Register Parsing

**User Story:** As a developer, I want to verify that OtherEnergies correctly parses the 0x0500 register block, so that energy counters and run-hour meters match the EM540 register specification.

#### Acceptance Criteria

1. WHEN the OtherEnergies_Parser parses registers, THE OtherEnergies_Parser SHALL read kWh (+) total from offset 0x00 as INT64 little-endian and divide by 1000
2. WHEN the OtherEnergies_Parser parses registers, THE OtherEnergies_Parser SHALL read kvarh (+) total from offset 0x04 as INT64 little-endian and divide by 1000
3. WHEN the OtherEnergies_Parser parses registers, THE OtherEnergies_Parser SHALL read per-phase kWh (+) from offsets 0x10, 0x14, 0x18 as INT64 little-endian and divide by 1000
4. WHEN the OtherEnergies_Parser parses registers, THE OtherEnergies_Parser SHALL read kWh (-) total from offset 0x1C as INT64 little-endian and divide by 1000
5. WHEN the OtherEnergies_Parser parses registers, THE OtherEnergies_Parser SHALL read kvarh (-) total from offset 0x24 as INT64 little-endian and divide by 1000
6. WHEN the OtherEnergies_Parser parses registers, THE OtherEnergies_Parser SHALL read kVAh total from offset 0x2C as INT64 little-endian and divide by 1000
7. WHEN the OtherEnergies_Parser parses registers, THE OtherEnergies_Parser SHALL read run hour meter from offset 0x34 as INT32 little-endian and divide by 100
8. WHEN the OtherEnergies_Parser parses registers, THE OtherEnergies_Parser SHALL read frequency from offset 0x3C as INT32 little-endian and divide by 1000

### Requirement 4: MeterData Update Orchestration

**User Story:** As a developer, I want to verify that MeterData.update_from_frame() correctly orchestrates parsing of all data components, so that a single call produces a complete and consistent data snapshot.

#### Acceptance Criteria

1. WHEN update_from_frame is called, THE MeterData SHALL parse all three phases from the dynamic register map
2. WHEN update_from_frame is called, THE MeterData SHALL compute total current (An) as the sum of all three phase currents
3. WHEN update_from_frame is called, THE MeterData SHALL set a timestamp reflecting the current time
4. WHEN update_from_frame is called, THE MeterData SHALL invoke the Register_Remapper before parsing

### Requirement 5: Register Remap Correctness

**User Story:** As a developer, I want to verify that register remapping from the "grouped by variable type" layout to the "grouped by phase" layout is correct, so that downstream clients reading the phase-grouped registers get accurate data.

#### Acceptance Criteria

1. WHEN the Register_Remapper executes, THE Register_Remapper SHALL copy each source register value from dynamic_reg_map[0x0000] to the corresponding target address in remapped_reg_map as defined by the register_remap table
2. WHEN a register_remap entry has source address equal to ZERO_FILL, THE Register_Remapper SHALL set the target register value to zero
3. WHEN the Register_Remapper processes energy counters from the 0x0500 block, THE Register_Remapper SHALL convert INT64 values in Wh to INT32 values by dividing by 100
4. WHEN the Register_Remapper processes the frequency register at 0x053C, THE Register_Remapper SHALL convert from INT32 (Hz*1000) to INT16 (Hz*10) by dividing by 100
5. WHEN a register has two target addresses in the remap table (dual-mapped), THE Register_Remapper SHALL produce identical values at both target addresses
6. THE Register_Remapper SHALL populate all target addresses defined in the register_remap table after a single invocation

### Requirement 6: RegisterDefinition Behavior

**User Story:** As a developer, I want to verify that RegisterDefinition enforces value constraints, so that register data integrity is maintained.

#### Acceptance Criteria

1. WHEN new values are set on a RegisterDefinition with matching length, THE RegisterDefinition SHALL accept and store the new values
2. WHEN new values are set on a RegisterDefinition with mismatched length, THE RegisterDefinition SHALL raise a ValueError
3. THE RegisterDefinition SHALL default skip_n_read to 0 when not explicitly provided

### Requirement 7: Em540Frame Initialization

**User Story:** As a developer, I want to verify that Em540Frame initializes all register maps correctly, so that the frame is ready for data acquisition and remapping.

#### Acceptance Criteria

1. THE Em540Frame SHALL initialize static_reg_map with all expected register addresses and correct value lengths
2. THE Em540Frame SHALL initialize dynamic_reg_map with register 0x0000 (length 0x34) and register 0x0500 (length 0x40)
3. THE Em540Frame SHALL initialize remapped_reg_map with all target addresses from the register_remap table

### Requirement 8: Skip-N-Read Optimization

**User Story:** As a developer, I want to verify that the skip_n_read optimization correctly skips non-critical register reads, so that the 10Hz read rate target is achievable.

#### Acceptance Criteria

1. WHEN the read counter equals 1 (first cycle), THE Em540_Master SHALL read all dynamic registers regardless of skip_n_read settings
2. WHEN the read counter is greater than 1 and a register has skip_n_read value S greater than 0, THE Em540_Master SHALL read that register only when `read_counter % (S + 1) == 0`
3. WHEN a register has skip_n_read equal to 0, THE Em540_Master SHALL read that register on every cycle

### Requirement 9: Em540Master Error Handling

**User Story:** As a developer, I want to verify that Em540Master handles Modbus errors correctly, so that failures are reported to listeners and the system can recover.

#### Acceptance Criteria

1. WHEN acquire_data is called while the client is disconnected, THE Em540_Master SHALL return False and call read_failed on all listeners
2. WHEN a Modbus read returns an error response, THE Em540_Master SHALL return False and call read_failed on all listeners
3. WHEN a Modbus read returns fewer registers than requested, THE Em540_Master SHALL terminate the process with exit code 1
4. WHEN a ModbusIOException occurs during reading, THE Em540_Master SHALL return False
5. WHEN a ModbusException occurs during reading, THE Em540_Master SHALL close the client connection and return False

### Requirement 10: Em540Master Listener Notification

**User Story:** As a developer, I want to verify that Em540Master notifies listeners correctly after successful data acquisition, so that downstream components receive timely updates.

#### Acceptance Criteria

1. WHEN acquire_data succeeds, THE Em540_Master SHALL notify the Notify_Thread via threading.Condition
2. WHEN the Notify_Thread receives a notification, THE Notify_Thread SHALL call update_from_frame and then invoke new_data on all registered listeners
3. IF a listener raises an exception during notification, THEN THE Notify_Thread SHALL increment an error counter and continue operation
4. IF the error counter exceeds 10 consecutive errors, THEN THE Notify_Thread SHALL terminate the process with exit code 2

### Requirement 11: Main Loop Priority

**User Story:** As a developer, I want to verify that the main tick loop maintains timing priority over listener processing, so that the 10Hz data acquisition rate is not compromised by slow downstream consumers.

#### Acceptance Criteria

1. THE Main_Loop SHALL call acquire_data at intervals equal to the configured update_interval
2. WHILE the Notify_Thread is processing listener callbacks, THE Main_Loop SHALL continue sleeping until the next scheduled acquisition without blocking
3. WHEN the client is disconnected at the start of a loop iteration, THE Main_Loop SHALL attempt reconnection before calling acquire_data

### Requirement 12: Slave Bridge Datastore Updates

**User Story:** As a developer, I want to verify that the slave bridge correctly updates its Modbus datastore from master data, so that downstream Modbus clients read accurate register values.

#### Acceptance Criteria

1. WHEN new_data is called on the Slave_Bridge, THE Slave_Bridge SHALL update all dynamic register values in the datastore
2. WHEN new_data is called on the Slave_Bridge, THE Slave_Bridge SHALL update all static register values in the datastore
3. WHEN new_data is called on the Slave_Bridge, THE Slave_Bridge SHALL update all remapped register values in the datastore
4. THE Slave_Bridge SHALL apply a +1 offset to all register addresses when writing to the Modbus datastore (1-based addressing)

### Requirement 13: Slave Stats Tracking

**User Story:** As a developer, I want to verify that EM540SlaveStats correctly tracks client connections, so that monitoring and diagnostics have accurate connection data.

#### Acceptance Criteria

1. THE Slave_Stats SHALL initialize all connection counts to zero
2. WHEN a listener is added and changed is called, THE Slave_Stats SHALL invoke the listener with the current stats instance
3. WHEN multiple listeners are registered, THE Slave_Stats SHALL notify all listeners when changed is called
