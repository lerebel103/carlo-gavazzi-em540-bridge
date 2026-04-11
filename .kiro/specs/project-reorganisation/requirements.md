# Requirements Document

## Introduction

This document captures the requirements for the em540-bridge project reorganisation. The work spans three areas: (1) CI/CD automation with GitHub Actions and ruff-based tooling, (2) a read/write ConfigManager replacing pyconfigparser with dataclass-backed AppState and debounced YAML persistence, and (3) Home Assistant config entities exposed via MQTT discovery for runtime configuration changes.

## Glossary

- **ConfigManager**: The module responsible for loading, validating, and persisting configuration from/to a YAML file.
- **AppState**: A tree of Python dataclasses representing the full application configuration at runtime.
- **PERSISTED_FIELDS**: The subset of AppState fields that are written back to the YAML file when changed at runtime.
- **HAConfigEntities**: The module that exposes PERSISTED_FIELDS as editable Home Assistant entities via MQTT discovery.
- **MQTT_Discovery_Payload**: A JSON message published to a well-known topic that tells Home Assistant about a new entity.
- **Debounce_Period**: The minimum elapsed time (5 seconds) between the last config change and the write to disk.
- **CI_Pipeline**: The set of GitHub Actions workflows (ci.yml, build.yml, release.yml) that automate linting, testing, building, and releasing.
- **Ruff**: The Python linter and formatter that replaces black, flake8, and isort.
- **Flush_Loop**: A background daemon thread in ConfigManager that periodically checks for dirty state and writes to disk.

## Requirements

### Requirement 1: CI Linting and Testing

**User Story:** As a developer, I want automated linting and testing on every push, so that code quality regressions are caught before merge.

#### Acceptance Criteria

1. WHEN code is pushed to any branch, THE CI_Pipeline SHALL run ruff lint checks on all Python source and test files.
2. WHEN code is pushed to any branch, THE CI_Pipeline SHALL run pytest to execute all test files matching the `*_test.py` pattern.
3. IF ruff reports lint violations, THEN THE CI_Pipeline SHALL fail the workflow and report the violations.
4. IF any pytest test fails, THEN THE CI_Pipeline SHALL fail the workflow and report the failures.

### Requirement 2: Docker Build and Push

**User Story:** As a developer, I want Docker images built and pushed automatically, so that deployments use verified, multi-architecture images.

#### Acceptance Criteria

1. WHEN code is pushed to the main branch or a version tag, THE CI_Pipeline SHALL build Docker images for linux/amd64 and linux/arm64 architectures.
2. WHEN a Docker build is triggered by a push to main or a version tag, THE CI_Pipeline SHALL push the built images to DockerHub.
3. WHEN a Docker build is triggered, THE CI_Pipeline SHALL derive the image version from `git describe --tags`.
4. WHILE code is pushed to a non-main, non-tag branch, THE CI_Pipeline SHALL build images without pushing them.

### Requirement 3: Automated Release

**User Story:** As a developer, I want GitHub Releases created automatically when I push a version tag, so that releases are consistent and include a changelog.

#### Acceptance Criteria

1. WHEN a tag matching `v*` is pushed, THE CI_Pipeline SHALL create a GitHub Release for that tag.
2. WHEN creating a GitHub Release, THE CI_Pipeline SHALL generate a changelog from the git log since the previous tag.

### Requirement 4: Makefile Migration to Ruff

**User Story:** As a developer, I want local development commands to use ruff instead of black/flake8/isort, so that local and CI tooling are consistent.

#### Acceptance Criteria

1. THE Makefile SHALL provide a `lint` target that runs `ruff check` and `ruff format --check` on all Python files.
2. THE Makefile SHALL provide a `format` target that runs `ruff format` to auto-fix formatting.
3. THE Makefile SHALL provide a `test` target that runs pytest to discover and execute `*_test.py` files.
4. THE Makefile SHALL retain the existing `build` and `run` targets for Docker operations.

### Requirement 5: Config Loading

**User Story:** As a developer, I want configuration loaded from a YAML file into typed dataclasses, so that config access is type-safe and IDE-friendly.

#### Acceptance Criteria

1. WHEN the application starts, THE ConfigManager SHALL read the YAML config file and return a populated AppState dataclass.
2. WHEN a required top-level section is missing from the YAML file, THE ConfigManager SHALL raise a ConfigError naming the missing section.
3. WHEN optional fields are absent from the YAML file, THE ConfigManager SHALL apply default values defined in the AppState dataclasses.
4. IF the YAML file does not exist or contains invalid YAML syntax, THEN THE ConfigManager SHALL raise a ConfigError with a descriptive message.

### Requirement 6: Config Validation

**User Story:** As a developer, I want configuration values validated on load, so that invalid settings are rejected before the application runs.

#### Acceptance Criteria

1. WHEN loading configuration, THE ConfigManager SHALL validate that `mode` is one of "tcp" or "serial".
2. WHEN loading configuration, THE ConfigManager SHALL validate that all port values satisfy 0 < port < 65535.
3. WHEN loading configuration, THE ConfigManager SHALL validate that all `slave_id` values satisfy 0 < slave_id < 256.
4. WHEN loading configuration, THE ConfigManager SHALL validate that all `log_level` values are one of DEBUG, INFO, WARNING, ERROR, or CRITICAL.
5. WHEN loading configuration, THE ConfigManager SHALL validate that `grid_feed_in_hard_limit` is less than or equal to 0.
6. WHEN loading configuration, THE ConfigManager SHALL validate that `smoothing_num_points` satisfies 1 <= value <= 600.

### Requirement 7: Debounced Config Persistence

**User Story:** As a developer, I want runtime config changes written back to YAML with debouncing, so that rapid changes do not cause excessive disk writes.

#### Acceptance Criteria

1. WHEN `schedule_persist()` is called, THE ConfigManager SHALL mark the configuration as dirty and record the current timestamp.
2. WHILE the configuration is dirty, THE Flush_Loop SHALL wait at least the Debounce_Period (5 seconds) after the last `schedule_persist()` call before writing to disk.
3. WHEN the Flush_Loop writes to disk, THE ConfigManager SHALL write only PERSISTED_FIELDS values to the YAML file.
4. WHEN the Flush_Loop writes to disk, THE ConfigManager SHALL preserve all non-persisted fields and their values in the YAML file.
5. WHEN the Flush_Loop completes a write, THE ConfigManager SHALL clear the dirty flag.
6. IF a write to disk fails, THEN THE ConfigManager SHALL log the error and keep the dirty flag set for retry on the next flush cycle.

### Requirement 8: Config Persistence Round-Trip

**User Story:** As a developer, I want config changes to survive application restarts, so that runtime adjustments are not lost.

#### Acceptance Criteria

1. FOR ALL PERSISTED_FIELDS, WHEN a field value is modified and persisted, THE ConfigManager SHALL produce a YAML file that, when loaded again, yields the same modified value.

### Requirement 9: HA Config Entity Discovery

**User Story:** As a Home Assistant user, I want bridge configuration exposed as HA entities, so that I can adjust settings from the HA UI without editing files.

#### Acceptance Criteria

1. WHEN the MQTT client connects, THE HAConfigEntities SHALL publish an MQTT_Discovery_Payload for each PERSISTED_FIELDS entry.
2. THE HAConfigEntities SHALL include name, unique_id, command_topic, state_topic, and device fields in every MQTT_Discovery_Payload.
3. THE HAConfigEntities SHALL set entity_category to "config" in every MQTT_Discovery_Payload.
4. THE HAConfigEntities SHALL include min, max, and step constraints in discovery payloads for number entities.
5. WHEN the MQTT client connects, THE HAConfigEntities SHALL publish the current value of each config entity to its state topic with the retain flag.

### Requirement 10: HA Config Entity Command Handling

**User Story:** As a Home Assistant user, I want to change bridge settings from the HA UI and have them take effect immediately, so that I can tune the system without restarting.

#### Acceptance Criteria

1. WHEN a valid value is received on a config entity command topic, THE HAConfigEntities SHALL update the corresponding AppState field with the parsed value.
2. WHEN a valid value is received on a config entity command topic, THE HAConfigEntities SHALL call `schedule_persist()` on the ConfigManager.
3. WHEN a valid value is received on a config entity command topic, THE HAConfigEntities SHALL publish the updated value to the entity state topic with the retain flag.
4. IF an invalid value is received on a config entity command topic, THEN THE HAConfigEntities SHALL log a warning and leave the AppState unchanged.

### Requirement 11: Dependency Cleanup

**User Story:** As a developer, I want unused dependencies removed and new ones declared, so that the project has a clean, minimal dependency set.

#### Acceptance Criteria

1. THE requirements.txt SHALL remove PyConfigParser, python-config-parser, and schema from production dependencies.
2. THE requirements-dev.txt SHALL replace black, flake8, and isort with ruff.
3. THE requirements-dev.txt SHALL include pytest and hypothesis as development dependencies.

### Requirement 12: Backward Compatibility

**User Story:** As an existing user, I want my current config.yaml to work without modification after the upgrade, so that the migration is seamless.

#### Acceptance Criteria

1. WHEN an existing config.yaml (written for pyconfigparser) is loaded, THE ConfigManager SHALL parse it successfully and populate AppState with the values present.
2. WHEN an existing config.yaml lacks fields introduced by the new AppState defaults, THE ConfigManager SHALL apply default values for those fields.
