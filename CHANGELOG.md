# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- (Features in development)

### Changed
- (Changes in development)

### Fixed
- (Fixes in development)

## [0.1.1] - 2026-04-11

### Fixed
- EM540 static device metadata propagation in slave bridge: preserved overlapped static registers (including device type register `0x000B`) after dynamic/remapped datastore updates so downstream Victron-compatible clients no longer read `Unknown` device type.
- Added regression coverage to ensure overlapped static registers remain visible after dynamic refresh cycles.

## [0.1.0] - 2026-04-10

### Added
- **Modbus bridge functionality**: Read Carlo Gavazzi EM540/EM530 energy meters via RS485 and re-serve over Modbus/TCP
- **Transparent Modbus/TCP proxy**: Direct Victron GX-compatible register access
- **Fronius TS-65-A emulation layer**: EM540 data mapped to Fronius TS-65-A register layout with optional rolling averages
- **Home Assistant MQTT integration**: Full sensor discovery with 40+ measurements and diagnostics
- **Runtime configuration persistence**: YAML-backed ConfigManager with debounced write-back (5s debounce)
- **Remote configuration changes**: Home Assistant UI editable toggle for sensor publishing (`enable_ha_publish`)
- **High-performance data acquisition**: 10 Hz (100 ms) non-blocking read loop with circuit breaker for stale data
- **Production-grade Docker setup**: 
  - Multi-architecture images (amd64 + arm64)
  - HEALTHCHECK for auto-restart on process failure
  - Resource limits and logging configuration
  - PYTHONUNBUFFERED for real-time logs
- **Comprehensive test suite**: 247 unit tests with parallel execution via pytest-xdist (~6s runtime)
- **CI/CD automation**: GitHub Actions workflows for linting, testing, multi-arch Docker builds, and automated releases
- **Version management**: Git-derived version resolution (environment, git describe, or fallback)
- **Code quality**: Ruff-based linting and formatting with pre-commit hooks

### Changed
- Replaced static config with read/write ConfigManager supporting runtime persistence
- Refactored Modbus register mapping to table-driven spec from procedural setup
- Minimum Python version: 3.13 (up from 3.11)

### Fixed
- Frequency register word-order bug (0x0110 alias now uses correct INT32 LSW→MSW byte order)
- Incomplete `OtherEnergies` parsing (7 missing partial-energy fields now populated)
- TS65A VAh label (corrected from kWh to kVAh)
- HA unit labels to match sensor data classes

### Security
- Non-root container user (lerebel103) with proper permission management
- No privileged escalation required

---

[Unreleased]: https://github.com/lerebel103/carlo-gavazzi-em540-bridge/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/lerebel103/carlo-gavazzi-em540-bridge/releases/tag/v0.1.1
[0.1.0]: https://github.com/lerebel103/carlo-gavazzi-em540-bridge/releases/tag/v0.1.0
