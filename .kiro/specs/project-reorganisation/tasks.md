# Implementation Plan: Project Reorganisation

## Overview

Migrate the em540-bridge project to use a read/write ConfigManager with AppState dataclasses, expose config to Home Assistant via MQTT discovery entities, modernise CI/CD with GitHub Actions and ruff, and clean up dependencies. Tasks are ordered so foundational work (dependencies, config) lands before dependent features (HA entities, CI wiring).

## Tasks

- [x] 1. Dependency cleanup and dev tooling
  - [x] 1.1 Update requirements.txt: remove PyConfigParser, python-config-parser, schema; keep paho-mqtt, pymodbus, pyserial, PyYAML, uptime
    - _Requirements: 11.1_
  - [x] 1.2 Update requirements-dev.txt: replace black, flake8, isort with ruff; add pytest and hypothesis
    - _Requirements: 11.2, 11.3_
  - [x] 1.3 Remove .flake8 config file (no longer needed with ruff)
    - _Requirements: 11.2_
  - [x] 1.4 Add pyproject.toml with ruff configuration (line-length, target-version, select rules)
    - _Requirements: 4.1, 4.2_

- [x] 2. AppState dataclasses and ConfigManager
  - [x] 2.1 Create config.py with AppState and nested config dataclasses (Em540MasterConfig, Em540SlaveConfig, Ts65aSlaveConfig, MqttConfig), PERSISTED_FIELDS set, REQUIRED_SECTIONS, and ConfigError exception
    - Replace the existing config.py (pyconfigparser schema) with the new dataclass-based module
    - _Requirements: 5.1, 5.3, 12.1, 12.2_
  - [x] 2.2 Implement ConfigManager.load() — read YAML, validate required sections, populate AppState with defaults for missing fields
    - _Requirements: 5.1, 5.2, 5.3, 5.4_
  - [x] 2.3 Implement ConfigManager validation — mode, ports, slave_id, log_level, grid_feed_in_hard_limit, smoothing_num_points
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6_
  - [x] 2.4 Implement ConfigManager.schedule_persist(), _flush_loop(), _write(), start_flush_loop(), stop()
    - Debounced write-back of PERSISTED_FIELDS to YAML, preserving non-persisted fields
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6_
  - [x] 2.5 Write property test: Config persistence round-trip (Property 1)
    - **Property 1: Config persistence round-trip**
    - **Validates: Requirements 8.1, 5.1**
  - [x] 2.6 Write property test: Defaults applied for missing optional fields (Property 2)
    - **Property 2: Defaults applied for missing optional fields**
    - **Validates: Requirements 5.3, 12.2**
  - [x] 2.7 Write property test: Missing required section raises error (Property 3)
    - **Property 3: Missing required section raises error**
    - **Validates: Requirement 5.2**
  - [x] 2.8 Write property test: Config validation rejects out-of-range values (Property 4)
    - **Property 4: Config validation rejects out-of-range values**
    - **Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5, 6.6**
  - [x] 2.9 Write property test: Debounce guarantee (Property 5)
    - **Property 5: Debounce guarantee**
    - **Validates: Requirement 7.2**
  - [x] 2.10 Write property test: Non-persisted fields preserved on write (Property 6)
    - **Property 6: Non-persisted fields preserved on write**
    - **Validates: Requirements 7.3, 7.4**

- [x] 3. Checkpoint — ConfigManager complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 4. Wire ConfigManager into main.py
  - [x] 4.1 Update main.py to use ConfigManager instead of pyconfigparser — load config, pass AppState to components, start flush loop
    - Remove all pyconfigparser imports and usage
    - _Requirements: 5.1, 12.1_
  - [x] 4.2 Update HABridge.__init__ to accept AppState.mqtt (or compatible interface) instead of pyconfigparser config object
    - _Requirements: 5.1_

- [x] 5. HA Config Entities
  - [x] 5.1 Create home_assistant/ha_config_entities.py — HAConfigEntities class with entity definitions, advertise(), subscribe(), _on_command()
    - Define ConfigEntity dataclass with name, field_path, config_section, field_name, min/max/step, parse_value
    - Generate MQTT discovery payloads for each PERSISTED_FIELDS entry
    - _Requirements: 9.1, 9.2, 9.3, 9.4, 9.5_
  - [x] 5.2 Implement command handling — parse incoming MQTT values, update AppState, call schedule_persist(), publish state with retain
    - Reject invalid values with warning log
    - _Requirements: 10.1, 10.2, 10.3, 10.4_
  - [x] 5.3 Integrate HAConfigEntities into HABridge — instantiate in __init__, extend advertise(), subscribe on connect
    - _Requirements: 9.1, 9.5_
  - [x] 5.4 Write property test: MQTT discovery payload validity (Property 7)
    - **Property 7: MQTT discovery payload validity**
    - **Validates: Requirements 9.1, 9.2, 9.3, 9.4**
  - [x] 5.5 Write property test: Valid command updates state and triggers persist (Property 8)
    - **Property 8: Valid command updates state and triggers persist**
    - **Validates: Requirements 10.1, 10.2, 10.3**
  - [x] 5.6 Write property test: Invalid command leaves state unchanged (Property 9)
    - **Property 9: Invalid command leaves state unchanged**
    - **Validates: Requirement 10.4**

- [x] 6. Checkpoint — HA config entities complete
  - Ensure all tests pass, ask the user if questions arise.

- [x] 7. Makefile migration
  - [x] 7.1 Update Makefile — lint target uses ruff check + ruff format --check, add format target, update test target to use pytest, keep build and run targets
    - _Requirements: 4.1, 4.2, 4.3, 4.4_

- [x] 8. GitHub Actions CI/CD workflows
  - [x] 8.1 Create .github/workflows/ci.yml — lint with ruff, test with pytest on every push
    - _Requirements: 1.1, 1.2, 1.3, 1.4_
  - [x] 8.2 Create .github/workflows/build.yml — multi-arch Docker build (amd64, arm64), push on main/tag, build-only on other branches, version from git describe
    - _Requirements: 2.1, 2.2, 2.3, 2.4_
  - [x] 8.3 Create .github/workflows/release.yml — create GitHub Release on v* tag push with changelog
    - _Requirements: 3.1, 3.2_

- [x] 9. Dockerfile cleanup
  - [x] 9.1 Update Dockerfile if needed to ensure it works without removed dependencies (PyConfigParser, schema)
    - _Requirements: 11.1_

- [x] 10. Final checkpoint — Full integration
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties from the design document
- The project uses Python throughout — all code examples and implementations use Python
