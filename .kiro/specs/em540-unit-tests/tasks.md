# Implementation Plan: EM540 Unit Tests

## Overview

Add comprehensive unit tests for the EM540 energy meter bridge project. Tests verify register parsing against the Gavazzi EM540 Modbus spec, register remapping correctness, skip-n-read optimization, main loop timing priority, slave bridge datastore updates, and slave stats tracking. All tests use Python `unittest` with `unittest.mock`, no new dependencies.

## Tasks

- [x] 1. Create test helpers and Em540Data tests
  - [x] 1.1 Create `carlo_gavazzi/em540_data_test.py` with test helpers and TestRegisterDefinition
    - Create `encode_int32_le`, `encode_int16_le`, `encode_int64_le` helper functions using `ModbusTcpClient.convert_to_registers()`
    - Create `build_dynamic_registers()` helper that builds a 0x34-length register array matching the 0x0000 dynamic block layout
    - Write `TestRegisterDefinition` tests: values length enforcement, setter rejects wrong length, skip_n_read default
    - _Requirements: 6.1, 6.2, 6.3_

  - [x] 1.2 Write TestEm540Frame initialization and remap tests
    - Test static_reg_map contains all expected addresses
    - Test dynamic_reg_map has 0x0000 (length 0x34) and 0x0500 (length 0x40)
    - Test remapped_reg_map contains all target addresses from register_remap
    - Test remap_registers copies source values to correct targets for phase voltages, currents, powers
    - Test ZERO_FILL entries produce zero values in remapped registers
    - Test energy counter INT64→INT32 conversion with /100 weight
    - Test frequency conversion from INT32 (Hz*1000) to INT16 (Hz*10)
    - Test dual-mapped registers (e.g., 0x0034 and 0x0112) contain identical values
    - Test run hour meter straight copies
    - _Requirements: 7.1, 7.2, 7.3, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6_

  - [ ]* 1.3 Write property test for register remap correctness
    - **Property 4: Register Remap Correctness**
    - **Validates: Requirements 5.1, 5.2, 5.6**

  - [ ]* 1.4 Write property test for dual-map consistency
    - **Property 5: Remap Dual-Map Consistency**
    - **Validates: Requirement 5.5**

  - [ ]* 1.5 Write property test for energy counter conversion
    - **Property 6: Energy Counter Conversion**
    - **Validates: Requirement 5.3**

  - [ ]* 1.6 Write property test for frequency conversion
    - **Property 7: Frequency Conversion**
    - **Validates: Requirement 5.4**

- [x] 2. Create MeterData parsing tests
  - [x] 2.1 Create `carlo_gavazzi/meter_data_test.py` with TestPhaseData
    - Use `build_dynamic_registers()` helper to create known register values
    - Test parsing of all 7 fields for each phase index (0, 1, 2): voltage L-N, voltage L-L, current, power, apparent power, reactive power, power factor
    - Test negative power values produce negative floating-point output
    - Verify register offsets and value weights match EM540 spec (Volt*10, Ampere*1000, Watt*10, VA*10, var*10, PF*1000)
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8_

  - [ ]* 2.2 Write property test for phase data parsing round-trip
    - **Property 1: Phase Data Parsing Round-Trip**
    - **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7**

  - [x] 2.3 Write TestSystemData
    - Test parsing of system voltage L-N (0x024), voltage L-L (0x026), power (0x028), apparent power (0x02A), reactive power (0x02C), power factor (0x031), frequency (0x033)
    - Verify INT32 and INT16 data types and value weights
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7_

  - [ ]* 2.4 Write property test for system data parsing round-trip
    - **Property 2: System Data Parsing Round-Trip**
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7**

  - [x] 2.5 Write TestOtherEnergies
    - Test parsing of kWh (+) total, kvarh (+) total, per-phase kWh, kWh (-) total, kvarh (-) total, kVAh total
    - Test run hour meters (0x34, 0x36) and frequency (0x3C) and life counter (0x3E)
    - Verify INT64 and INT32 data types and value weights (/1000 for energy, /100 for hours, /1000 for frequency)
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8_

  - [ ]* 2.6 Write property test for other energies parsing round-trip
    - **Property 3: Other Energies Parsing Round-Trip**
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8**

  - [x] 2.7 Write TestMeterDataUpdateFromFrame
    - Test update_from_frame parses all three phases
    - Test total current (An) equals sum of three phase currents
    - Test timestamp is set after update
    - Test remap_registers is called during update
    - _Requirements: 4.1, 4.2, 4.3, 4.4_

  - [ ]* 2.8 Write property test for total current invariant
    - **Property 8: Total Current Invariant**
    - **Validates: Requirement 4.2**

- [x] 3. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Create Em540Master tests
  - [x] 4.1 Create `carlo_gavazzi/em540_master_test.py` with TestEm540Master
    - Mock `AsyncModbusSerialClient` / `AsyncModbusTcpClient` and config
    - Test acquire_data reads dynamic registers and notifies listeners via Condition on success
    - Test acquire_data returns False and calls read_failed on all listeners when disconnected
    - Test Modbus read error returns False and calls read_failed
    - Test register count mismatch calls `os._exit(1)`
    - Test ModbusIOException returns False
    - Test ModbusException closes client and returns False
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5, 10.1, 10.2_

  - [x] 4.2 Write skip-n-read optimization tests
    - Test first cycle (counter=1) reads all registers regardless of skip_n_read
    - Test subsequent cycles skip registers based on `counter % (skip_n_read + 1) != 0`
    - Test registers with skip_n_read=0 are read every cycle
    - _Requirements: 8.1, 8.2, 8.3_

  - [ ]* 4.3 Write property test for skip-n-read scheduling
    - **Property 9: Skip-N-Read Scheduling**
    - **Validates: Requirements 8.1, 8.2, 8.3**

  - [x] 4.4 Write listener notification and error counting tests
    - Test notify thread calls update_from_frame then new_data on listeners
    - Test listener exception increments error counter
    - Test 10+ consecutive errors triggers os._exit(2)
    - _Requirements: 10.2, 10.3, 10.4_

- [x] 5. Create main loop and timing tests
  - [x] 5.1 Create `main_test.py` with TestMainLoopPriority
    - Mock Em540Master, Em540Slave, Ts65aSlaveBridge, HABridge, configparser
    - Test acquire_data is called at configured update_interval
    - Test reconnect is attempted when client is disconnected
    - Test loop sleeps between intervals using mocked time.perf_counter and time.sleep
    - Test slow listener processing does not block next acquire_data call
    - _Requirements: 11.1, 11.2, 11.3_

  - [ ]* 5.2 Write property test for main loop timing priority
    - **Property 10: Main Loop Timing Priority**
    - **Validates: Requirements 11.1, 11.2**

  - [ ]* 5.3 Write property test for listener isolation
    - **Property 11: Listener Isolation**
    - **Validates: Requirement 11.2**

- [x] 6. Create slave bridge and slave stats tests
  - [x] 6.1 Create `carlo_gavazzi/em540_slave_bridge_test.py` with TestEm540Slave
    - Mock ModbusSparseDataBlock and Em540Frame
    - Test new_data updates dynamic, static, and remapped register values in datastore
    - Test datablock addresses use +1 offset (1-based addressing)
    - _Requirements: 12.1, 12.2, 12.3, 12.4_

  - [ ]* 6.2 Write property test for slave bridge register offset
    - **Property 12: Slave Bridge Register Offset**
    - **Validates: Requirement 12.4**

  - [x] 6.3 Create `carlo_gavazzi/em540_slave_stats_test.py` with TestEM540SlaveStats
    - Test initial counts are all zero
    - Test listener notification on changed()
    - Test multiple listeners all notified
    - _Requirements: 13.1, 13.2, 13.3_

- [x] 7. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Property tests validate universal correctness properties from the design document
- All tests use Python `unittest` + `unittest.mock`, no new dependencies
- Test files follow existing convention: `{module}_test.py` alongside source files
