# Carlo Gavazzi EM540 Energy Meter Modbus Bridge

[![GitHub](https://img.shields.io/badge/GitHub-lerebel103%2Fcarlo--gavazzi--em540--bridge-blue?logo=github)](https://github.com/lerebel103/carlo-gavazzi-em540-bridge)
[![Docker](https://img.shields.io/badge/Docker-lerebel103%2Fcarlo--gavazzi--em540--bridge-blue?logo=docker)](https://hub.docker.com/r/lerebel103/carlo-gavazzi-em540-bridge)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/license/mit)

Bridges a single Carlo Gavazzi EM540/EM530 energy meter to multiple downstream consumers simultaneously.

The bridge acts as a Modbus master to the physical meter over RS485 (via an RS485-to-IP converter or serial device),
reading dynamic registers at a default target rate of 10 Hz. It then re-serves this data in several formats:

- **Transparent Modbus/TCP proxy:** Direct access to EM540-compatible registers for clients such as Victron GX devices.
- **Fronius TS-65-A emulation:** EM540 data mapped to the Fronius TS-65-A register layout for Fronius inverter compatibility.
- **MQTT sensors for Home Assistant:** Key measurements published as MQTT sensors for monitoring and automation.

Fronius TS-65-A emulation maps EM540 data to the corresponding registers transparently. Optionally, meter values can be
smoothed over a configurable time window so the emulated values represent a rolling average rather than instantaneous
readings (see `config-default.yaml`).

> **Note:** The bridge is read-only by default. Static identification/config registers are read during connection
> setup, and acquisition cycles read the primary dynamic block plus the full energy block each tick. The only writes
> the bridge can ever make to the meter are the two **optional, disabled-by-default** meter-configuration enforcement
> settings described under [Meter Configuration Enforcement](#meter-configuration-enforcement-victron-setups). With
> both left at their defaults, the bridge never writes to the meter.

## Features

- **Modbus/RTU to Modbus/TCP proxy:** Reads metering registers from the EM540 via Modbus/RTU and serves them over Modbus/TCP.
- **Optional downstream Modbus/RTU serial adapters for clients:** EM540 and TS65A slave models can each be exposed over dedicated serial adapters.
- **Fronius TS-65-A emulation:** Maps EM540 data to the Fronius TS-65-A register format for compatibility with Fronius inverters.
- **Home Assistant integration:** Publishes measurements and diagnostics as MQTT sensors.
- **High-rate acquisition:** Targets a 10 Hz (100 ms) polling interval for near real-time updates.
- **Concurrent client support:** Serves multiple downstream clients without increasing load on the EM540 meter.
- **Stale-data protection:** A circuit breaker blocks downstream Modbus responses when upstream data is stale or unavailable, preventing silent delivery of bad values.

## Configuration

The master remains mutually exclusive: set [em540_master.mode](app/config.py#L36) to either `tcp` or `serial`.
If your serial path echoes transmitted bytes, enable [em540_master.handle_local_echo](app/config.py#L42).

This controls the **upstream meter link** only. Downstream client-facing serial bridges are configured separately under each slave section.

Each slave can expose the same internal model over multiple transports at once:

- EM540 slave: TCP, RTU-over-TCP, optional serial RTU
- TS65A slave: TCP, optional serial RTU

See [config-default.yaml](config-default.yaml) for the full sample layout.

### Downstream Serial Adapter Support (Client-Facing)

- Enable [em540_slave.serial.enabled](config-default.yaml) to expose the EM540-compatible slave over a serial RTU adapter for downstream clients.
- Enable [ts65a_slave.serial.enabled](config-default.yaml) to expose the TS65A-compatible slave over a serial RTU adapter for downstream clients.
- These can run concurrently with each slave's TCP endpoint.

When running with Docker, map each enabled serial adapter device into the container (for example `/dev/ttyUSB1` for EM540 slave serial and `/dev/ttyUSB2` for TS65A slave serial).

### Meter Configuration Enforcement (Victron setups)

On each (re)connect the bridge reads the meter's key configuration registers and logs them in a
human-readable form alongside their raw value, for example:

```
EM540 measurement mode: C (Bidirectional) [2]
EM540 measuring system: 3Pn (3-phase + neutral) [0]
EM540 wiring check: Correct [0]
```

This mirrors what a Victron GX device does when it talks to a Carlo Gavazzi meter: it expects the
meter to be in a specific state (bidirectional measurement so grid export reads as negative power,
and a three-phase measuring system) and will otherwise not account for energy correctly. When the
bridge sits between the meter and a Victron (or Fronius) system, these settings let it keep the
meter in that desired state automatically, so a factory reset, a manual front-panel change, or a
meter swap can't silently leave the meter mis-configured for your system.

Two **opt-in** flags under `em540_master` control whether the bridge corrects the meter (both
default to `false`, i.e. read-and-log only, no writes):

| Setting                          | Register | Applies                         | Default | Notes |
|----------------------------------|----------|---------------------------------|---------|-------|
| `ensure_bidirectional_mode`      | `0x1103` | Measurement mode → C / Bidirectional (`2`) | `false` | Safe, non-destructive. Matches Victron's expected mode so export reads as negative power. |
| `ensure_3phase_measuring_system` | `0x1002` | Measuring system → 3Pn / 3-phase + neutral (`0`) | `false` | **May reset the meter's kWh counters** when changed, so it is off by default. Only enable if your meter is genuinely wired 3-phase + neutral. |

Behaviour and safety:

- Registers are **always read and logged** on connect regardless of the flags; the flags only
  decide whether a corrective write is attempted.
- Writes are **read-first / write-only-on-mismatch**: the bridge writes only when the current value
  differs from the desired one, then reads the register back and caches/serves the meter's *actual*
  post-write value.
- Writes are **best-effort and never fatal**. On MID (PFx) meter models these registers are
  read-only and fixed by the part number; a rejected write is logged and the connection continues
  normally.
- These flags are configuration-file only and are intentionally **not** exposed as Home Assistant
  entities.
- The meter's own **wiring-check status** (register `0x1105`) is published as the enabled-by-default
  Home Assistant diagnostic sensor **EM540 Wiring Check Error** (`0` = correct, `1` = connection
  error), so a mis-wired meter is visible at a glance.

> On reconnect the config registers are re-read (a few extra Modbus reads, on reconnect only) so a
> change made on the device while the bridge was disconnected is observed and, if enabled, re-applied.

## Requirements

- **Hardware:** RS485 to Modbus/RTU physical converter to connect the EM540 meter to your network.
- **Optional hardware:** One or more dedicated serial adapters for downstream slave RTU access (EM540 and/or TS65A).
- **Meter configuration:** EM540 must be set to a baud rate of 57600 or higher to support a 100 ms read cycle.
- **Software (Docker path):** Docker Engine with Docker Compose plugin.
- **Software (manual path):** Python 3.14 and [uv](https://docs.astral.sh/uv/) for dependency management.

## Install and Run with Docker Compose (Recommended)

1. Clone this repository.
2. Copy the example config and edit it for your environment:
	`cp config-default.yaml config.yaml`
3. Update `config.yaml` with your RS485 converter details and MQTT broker settings.
4. Start the bridge with Compose:
	`docker compose up -d`
5. Check logs:
	`docker compose logs -f carlo-gavazzi-em540-bridge`
6. Stop the service:
	`docker compose down`

Notes:
- The provided `docker-compose.yaml` mounts `./config.yaml` into the container at `/etc/carlo-gavazzi-em540-bridge/config.yaml`.
- The Compose file uses image `lerebel103/carlo-gavazzi-em540-bridge:latest` by default.
- Exposed ports are `5001` (Modbus TCP), `5002` (Modbus RTU-over-TCP), and `5003` (TS-65-A emulation).

## Build and Run Manually (Without Docker)

1. Install [uv](https://docs.astral.sh/uv/getting-started/installation/).
2. Install dependencies:
	`uv sync --no-install-project`
3. Copy and edit configuration:
	`cp config-default.yaml config.yaml`
4. Run the bridge:
	`uv run python -m app --config ./config.yaml`

Optional helper commands:
- Start stack with project Makefile: `make up`
- Stop stack: `make down`
- View logs: `make logs`

## Development

- Install all dependencies (including dev tools): `uv sync --no-install-project`
- Run unit tests (parallel): `make test`
- Run unit tests (serial): `make test-serial`
- Run end-to-end integration tests (always Dockerized): `make test-integration`
- Lint: `make lint`
- Format: `make format`

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history and detailed release notes.

## Home Assistant Integration

The following measurement sensors are published to Home Assistant and refreshed at the interval configured in the config file:

| Sensor Name            | Unit  | Device Class   | State Class      | Precision |
|------------------------|-------|----------------|------------------|-----------|
| Frequency              | Hz    | frequency      | measurement      | 2         |
| Mean Voltage L-N       | V     | voltage        | measurement      | 1         |
| Voltage L1-N           | V     | voltage        | measurement      | 1         |
| Voltage L2-N           | V     | voltage        | measurement      | 1         |
| Voltage L3-N           | V     | voltage        | measurement      | 1         |
| Mean Voltage L-L       | V     | voltage        | measurement      | 1         |
| Voltage L1-L2          | V     | voltage        | measurement      | 1         |
| Voltage L2-L3          | V     | voltage        | measurement      | 1         |
| Voltage L3-L1          | V     | voltage        | measurement      | 1         |
| Current                | A     | current        | measurement      | 1         |
| Current L1             | A     | current        | measurement      | 1         |
| Current L2             | A     | current        | measurement      | 1         |
| Current L3             | A     | current        | measurement      | 1         |
| Power                  | W     | power          | measurement      | 0         |
| Power L1               | W     | power          | measurement      | 0         |
| Power L2               | W     | power          | measurement      | 0         |
| Power L3               | W     | power          | measurement      | 0         |
| Reactive Power         | var   | reactive_power | measurement      | 0         |
| Reactive Power L1      | var   | reactive_power | measurement      | 0         |
| Reactive Power L2      | var   | reactive_power | measurement      | 0         |
| Reactive Power L3      | var   | reactive_power | measurement      | 0         |
| Apparent Power         | VA    | apparent_power | measurement      | 0         |
| Apparent Power L1      | VA    | apparent_power | measurement      | 0         |
| Apparent Power L2      | VA    | apparent_power | measurement      | 0         |
| Apparent Power L3      | VA    | apparent_power | measurement      | 0         |
| Mean Power Factor      |       | power_factor   | measurement      | 2         |
| Power Factor L1        |       | power_factor   | measurement      | 2         |
| Power Factor L2        |       | power_factor   | measurement      | 2         |
| Power Factor L3        |       | power_factor   | measurement      | 2         |
| Energy Import          | kWh   | energy         | total_increasing | 2         |
| Energy Export          | kWh   | energy         | total_increasing | 2         |
| Reactive Energy Import | kvarh | reactive_energy| total_increasing | 2         |
| Reactive Energy Export | kvarh | reactive_energy| total_increasing | 2         |
| Apparent Energy kvah   | kVAh  | energy         | total_increasing | 2         |
| Run Hours              | h     | duration       | total_increasing | 1         |

The following diagnostic sensors are also published (entity category: `diagnostic`). Many are disabled
by default in Home Assistant to reduce clutter; enable the ones you need from the device page. All
state classes are `measurement`.

| Sensor Name                       | Unit | Device Class | Precision | Enabled by default |
|-----------------------------------|------|--------------|-----------|--------------------|
| Sys Uptime                        | s    | duration     | 0         | yes                |
| Bridge Uptime                     | s    | duration     | 0         | yes                |
| Acq Rate                          | Hz   | frequency    | 2         | yes                |
| MQTT Data Update Rate             | Hz   | frequency    | 2         | no                 |
| RS485 Master Read Failures        |      |              | 0         | no                 |
| RS485 Consumer Missed Updates     |      |              | 0         | no                 |
| RS485 Consumer Max Seq Gap        |      |              | 0         | no                 |
| Acq Dur Min                       | ms   | duration     | 2         | no                 |
| Acq Dur Max                       | ms   | duration     | 2         | no                 |
| Acq Dur Mean                      | ms   | duration     | 2         | no                 |
| Acq Headroom Min                  | ms   | duration     | 2         | no                 |
| Acq Headroom Max                  | ms   | duration     | 2         | no                 |
| Acq Headroom Mean                 | ms   | duration     | 2         | no                 |
| Tick Overruns                     |      |              | 0         | yes                |
| EM540 TCP Clients                 |      |              | 0         | yes                |
| EM540 TCP Disconnects             |      |              | 0         | yes                |
| EM540 TCP (RTU) Clients           |      |              | 0         | yes                |
| EM540 TCP (RTU) Disconnects       |      |              | 0         | yes                |
| EM540 Serial Active               |      |              | 0         | yes                |
| EM540 Serial Connects             |      |              | 0         | yes                |
| EM540 Serial Disconnects          |      |              | 0         | yes                |
| EM540 Circuit Breaker Open        |      |              | 0         | no                 |
| EM540 Circuit Breaker Open Count  |      |              | 0         | no                 |
| EM540 Stale Data Age              | ms   | duration     | 1         | yes                |
| EM540 Dropped Stale Requests      |      |              | 0         | yes                |
| EM540 Wiring Check Error          |      |              | 0         | yes                |
| TS65A TCP Clients                 |      |              | 0         | yes                |
| TS65A TCP Disconnects             |      |              | 0         | yes                |
| TS65A Serial Active               |      |              | 0         | yes                |
| TS65A Serial Connects             |      |              | 0         | yes                |
| TS65A Serial Disconnects          |      |              | 0         | yes                |
| Overfeed Limit Count              |      |              | 0         | yes                |
| Overfeed Limit Max Duration       | ms   | duration     | 2         | yes                |
| TS65A Circuit Breaker Open        |      |              | 0         | no                 |
| TS65A Circuit Breaker Open Count  |      |              | 0         | no                 |
| TS65A Stale Data Age              | ms   | duration     | 1         | yes                |
| TS65A Dropped Stale Requests      |      |              | 0         | no                 |

Transport diagnostics distinguish the downstream channels each bridge can serve:

- **TCP** — standard Modbus/TCP.
- **TCP (RTU)** — RTU framing over a TCP socket (EM540 bridge only).
- **Serial** — Modbus/RTU over a physical serial adapter. A serial line has no transport
  connect/disconnect event, so `Serial Active` (1/0), `Serial Connects`, and `Serial Disconnects`
  are inferred from request activity: a client is considered connected while requests arrive within
  `serial_idle_timeout` seconds, and the connect/disconnect counters increment on the activity edges.

In addition, per-quantity **daily min/max extrema** are published for Power, Current, Voltage L-N,
and Voltage L-L, at both system and per-phase (L1/L2/L3) scope (for example `Daily Power Min`,
`Daily Voltage L-N L2 Max`). These are evaluated on every upstream frame at the master and reset at
local midnight. All daily extrema sensors are disabled by default.

## References

- [GitHub Repository](https://github.com/lerebel103/carlo-gavazzi-em540-bridge)
- [Docker Hub Image](https://hub.docker.com/r/lerebel103/carlo-gavazzi-em540-bridge)
- [Changelog](CHANGELOG.md)
- [Carlo Gavazzi EM540/EM530 Modbus Register Map](https://www.gavazziautomation.com/fileadmin/images/PIM/OTHERSTUFF/COMPRO/EM500_CPP_Mod_V1.3_13022024.pdf)
