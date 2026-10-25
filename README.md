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
- **Serial frame latching:** Downstream Modbus-over-serial clients are latched to the upstream meter refresh rate, so they never receive duplicate frames and a multi-block polling sweep always reads one coherent snapshot (serial clients only — see below).
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

### Serial Frame Latching (Serial clients only)

Downstream Modbus-over-**serial** clients get their effective read rate latched to the upstream
EM540/EM530 refresh rate. Without it, a serial client that polls faster than the meter refreshes
simply reads the same registers again and receives **duplicate frames**; with it, every served
frame is a distinct upstream frame, and a multi-block polling sweep is **coherent** (all blocks in
one sweep come from the same upstream frame).

**Why it exists.** The EM540 is the one Carlo Gavazzi model that Victron's `dbus-cgwacs` treats as
a *fastloop* meter (its `AcSensor::supportFastloop()` returns true only for the EM540 protocol), so
an EM540-aware client reads as fast as the serial link allows and would otherwise see a stream of
duplicate frames. The latch paces that client to the real data rate instead.

**How it works (anchoring).** Real clients split one polling cycle into several separate reads
(power block, then voltage/current, then energy). The latch keys off the **power anchor registers**
a client reads on every cycle. Each bridge recognises a full power-anchor set, but exactly one of
them is the **primary cycle-start anchor** — the system-power register a real client reads *first*
every cycle:

- EM540: primary cycle-start anchor system power (`0x0028`); secondary power anchor the per-phase
  power block (`0x0012`).
- TS65A: primary cycle-start anchor the SunSpec AC power value (`40097`); secondary power anchors the
  three per-phase AC power registers (`40099`/`40101`/`40103`).

Only a read that overlaps the **single primary cycle-start anchor** starts a new polling cycle: it
waits (bounded) for a fresh upstream frame, pins it, and serves from it. That advances the pin **at
most once per client cycle**. Every other read of the sweep — a secondary power anchor or a
non-anchor block — is a follow-on read that serves immediately and coherently from that one pinned
frame, so the whole sweep comes from a single upstream generation and is never torn across frames,
and the sweep rate tracks the upstream refresh rate rather than being divided by the number of
blocks in the cycle.

**Consistency and correctness guarantees (serial).** With the latch enabled a serial client gets:

- **one fresh frame per cycle** — each cycle-start read advances to a strictly newer upstream
  generation, so the client never re-reads a frame it has already consumed;
- **whole-cycle coherence** — all blocks of a multi-block sweep (power, voltage/current, energy)
  are sliced from the one generation pinned at the cycle start, so no sweep is torn across frames;
- **no duplicate or stale frames** at the client's raw poll rate, while the served rate never
  exceeds the upstream rate.

A client that *never* reads the primary cycle-start anchor (a non-anchor-only poller) is not frozen
on whatever frame its first read happened to pin: its follow-on reads refresh the pin to the latest
**already-published** upstream generation. This pin-age bound is driven purely by upstream frame
**availability** — it only jumps to a frame the upstream has already produced, never to a configured
wall-clock cadence.

**What the latch does *not* do: it cannot make a client read faster than its own cycle clocks over
the wire.** A real multi-block client (e.g. a Victron GX reading the full EM540 register set, or a
Fronius inverter reading the SunSpec blocks) issues several reads per cycle, and the whole cycle's
on-wire time is bounded by the serial **baud rate**. At the **9600-baud default** the full EM540
read set clocks roughly 120–160 ms over the line, which **exceeds** the 80 ms (12.5 Hz) upstream
period, so a flat-out multi-block client is **wire-bound to about 6–9 Hz** — *below* the upstream
rate — purely by serial physics. This is **expected and correct at 9600 baud, not a bug and not a
duplicate-frame problem**: the latch still delivers smooth, duplicate-free, whole-cycle-coherent
frames capped at the upstream rate; it simply cannot clock a client's reads out faster than the line
allows. The pacing layer itself is unaffected by baud — baud only sets the wire ceiling. The
integration suite guards this expectation with a multi-block throughput test for both bridges
(`test_em540_serial_multi_block_client_is_wire_bound_below_upstream` and its TS65A twin), which
asserts the wire-bound band for the configured baud rather than a physically impossible 12.5 Hz.

The 6–9 Hz wire-bound figure is the ceiling *only at 9600 baud* — it is **not** a hard limit of the
bridge or of the Victron GX. See [Achieving full 12.5 Hz end-to-end](#achieving-full-125-hz-end-to-end-em540em530)
below for how to unlock the meter's natural rate all the way through to the consumer.

The multi-block throughput integration test above asserts the achievable wire-bound rate for the
*test harness's* configured baud; it is independent of the baud you run in the field.

### Achieving full 12.5 Hz end-to-end (EM540/EM530)

The EM540/EM530 can be read at its natural **~12.5 Hz** internal update rate, and that full rate can
be carried all the way through to a downstream Victron (or Fronius) consumer with minimal latency.
This is **not** the default — the defaults favour a steady, conservative 10 Hz-style cadence and a
9600-baud serial link, which caps a multi-block serial client at ~6–9 Hz (see above). To get the
full rate you must change **two independent things**; changing only one will not help:

**1. Run the bridge at the meter's natural rate — set the upstream tick to zero.**

Set the master read interval to `0`, which puts the bridge in **unpaced mode**: it reads the meter
as fast as the hardware produces frames (the EM540/EM530's true internal rate, ~12.5 Hz) instead of
on a fixed 100 ms tick.

```yaml
em540_master:
  update_interval: 0   # 0 = unpaced: read as fast as the meter produces (~12.5 Hz on EM540/EM530)
```

With `update_interval: 0` the upstream generation counter advances at the meter's real rate, so
every downstream path (serial latch, TCP, MQTT) is fed at ~12.5 Hz instead of 10 Hz. The serial
frame latch then paces serial clients to this ~12.5 Hz natural rate.

**2. Raise the serial baud so the client's multi-block cycle fits inside the 12.5 Hz window.**

Even at the natural ~12.5 Hz upstream, a multi-block serial client (e.g. a Victron GX) is still
**wire-bound**: at 9600 baud its full read cycle takes ~120–160 ms, longer than the 80 ms (12.5 Hz)
period, so it tops out at ~6–9 Hz. Raising the serial link to **115200 baud** clocks the whole cycle
well under 80 ms, so the client observes the full ~12.5 Hz with minimal latency. **Both ends must
agree on the baud rate** or you get framing errors (not merely a slow link). This has been **verified
on real hardware**.

- **On the Victron GX device**, the Carlo Gavazzi meter driver defaults to 9600 but accepts a baud
  argument. Edit the driver start script `/opt/victronenergy/dbus-cgwacs/start-cgwacs.sh` and add
  `--baud 115200` to the driver args line. The default line is `args="/dev/$tty"`; change it to:

  ```sh
  args="/dev/$tty --baud 115200"
  ```

  This is a manual, user-applied change to a Victron system file — the bridge cannot make it for you
  — and it is the path that has been verified to deliver a steady ~12.5 Hz to the Victron system.

- **On the bridge**, set the matching downstream serial baud so both ends agree:

  ```yaml
  em540_slave:
    serial:
      baudrate: 115200   # must match the GX --baud setting
  ts65a_slave:
    serial:
      baudrate: 115200   # if the TS65A serial output is used
  ```

**Summary:** `em540_master.update_interval: 0` unlocks the meter's natural ~12.5 Hz *into* the bridge;
raising the serial link to 115200 (GX start-script `--baud 115200` **and** the matching bridge
`serial.baudrate`) carries that ~12.5 Hz *out* to the consumer with minimal latency. Do both for
full-rate, low-latency throughput; the serial latch continues to guarantee smooth, duplicate-free,
whole-cycle-coherent frames the whole time.

**Configuration** (per bridge, under `em540_slave.serial` and `ts65a_slave.serial`):

| Setting | Default | Notes |
|---------|---------|-------|
| `pacing_enabled` | `true` | Set `false` to disable latching entirely (pure pass-through; duplicate frames possible). Runtime-settable and persisted — changeable live via Home Assistant (see below) with no restart. |
| `pacing_max_wait_s` | `0.12` | Upper bound on how long a read waits for a fresh frame. Must be strictly less than `serial.timeout` (validated). `<= 0` also disables latching. |
| `client_read_timeout_ms` | `200` | The client's own read deadline. The latch wait is clamped per request so the response still fits inside it. |

**Dynamic Home Assistant pacing toggle.** Each serial bridge exposes a Home Assistant **switch** —
**EM540 Serial Pacing** and **TS65A Serial Pacing** — that enables or disables that bridge's serial
pacing at runtime. The switch is bound to the live `pacing_enabled` config value the bridge's pacing
layer reads on every request, so flipping it takes effect **immediately with no service restart**,
and the change is persisted to `config.yaml`. Turn a switch **off** to let a serial client poll
unpaced and observe its **natural, unpaced read rate** (e.g. to compare against the paced rate or to
measure the raw wire-bound ceiling described above); turn it back **on** to restore duplicate-free,
coherent pacing.

Two Home Assistant diagnostic sensors expose the observed downstream serial poll rate so you can
see the latch in action: **EM540 Serial Read Rate** and **TS65A Serial Read Rate** (both in Hz,
enabled by default). The rate is **pinned to the power-register (primary cycle-start anchor) reads**
— it counts one tick each time a cycle-start anchor read advances the pin to a fresh frame, i.e.
once per client polling cycle. It is deliberately **not** counted per inbound request, because a
real client issues several sparse reads per cycle (and discovery scans add more), which would
otherwise inflate the number. So the sensor reports the client's true per-cycle refresh rate.

> **Serial only — not TCP.** This guarantee applies to Modbus-over-**serial** clients only. It is
> deliberately **not** implemented for TCP or RTU-over-TCP clients: those transports can carry many
> concurrent connections, and correct per-connection frame latching there is significantly more
> complex. TCP/RTU-over-TCP clients therefore get no duplicate-frame or single-sweep-coherence
> guarantee today. Extending the latch to TCP is possible future work, not a current promise.

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

### Upstream frame validation

Each parsed upstream frame is checked against the meter's documented limits before it is published
or folded into the daily min/max extrema. Two independent, spec-grounded criteria are applied under
`em540_master.frame_validation`:

- **Overflow sentinel** — the meter signals an over-range reading by setting the most-significant
  word of the relevant INT32 variable to `0x7FFF` (EM500_CPP_Mod_V1.3 §3.3 "Maximum electrical
  values"). This catches the exact INT32 saturation that previously surfaced as absurd per-phase
  current peaks.
- **Physical bounds** — datasheet-derived magnitude limits (EM540 datasheet):

  | Key | Default | Basis |
  |-----|---------|-------|
  | `max_phase_current_a` | `130.0` | 2 × Imax (65 A) **(EM540 only)** |
  | `max_system_current_a` | `390.0` | 3 × per-phase bound (defensive) **(EM540 only)** |
  | `max_voltage_ln_v` | `360.0` | 1.5 × Un (240 V) |
  | `max_voltage_ll_v` | `625.0` | ~1.5 × L-L (415 V) |
  | `min_frequency_hz` | `40.0` | 45 Hz − 5 Hz guard |
  | `max_frequency_hz` | `70.0` | 65 Hz + 5 Hz guard |

  The two current bounds apply to EM540 / direct-input meters only. An EM530 is
  CT-connected, so its per-phase and system (An) current bound is derived from the
  meter CT ratio (register `0x1003`) at connect time and the two config values
  above are ignored for that model. The voltage and frequency bounds apply to
  both models.

If **any** validated quantity fails, the **whole frame is rejected** (atomic): the last-good values
are retained downstream and in Home Assistant, nothing is folded into the daily extrema, and the
freshness timestamp is not advanced. A **single/transient** bad frame among good ones does **not**
force the downstream circuit breaker open — it is absorbed, keeping last-good. This differs from
ordinary bad reads (transport/length/parse failures), which signal `read_failed()` and open the
downstream breaker on the first failure.

Only **persistent** rejections fail closed: once no accepted frame has arrived for
`max_consecutive_invalid_s` seconds (default `5.0`), each subsequent rejected frame signals
`read_failed()` so the downstream breaker opens and Home Assistant goes unavailable, until an
accepted frame recovers (which re-arms the single-frame absorb). This bounded gate is independent of
and complementary to the freshness watchdog (`health_max_stale_s`); it still fails closed even when
the watchdog is disabled. Rejected frames are counted by the **RS485 Invalid Frames** diagnostic
sensor, which is distinct from **RS485 Master Read Failures** (transport/length/parse failures); a
rejected frame increments both, so Invalid Frames isolates the parsed-but-rejected subset.

Set `enabled: false` to disable validation entirely, or `check_overflow_sentinel: false` to disable
only the sentinel check. Override the numeric bounds only with equally justified values.

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
| Apparent Energy kvah   | kVAh  | _(none)_       | total_increasing | 2         |
| Run Hours              | h     | duration       | total_increasing | 1         |

The following diagnostic sensors are also published (entity category: `diagnostic`). Many are disabled
by default in Home Assistant to reduce clutter; enable the ones you need from the device page. Most
state classes are `measurement`; monotonic counters (e.g. RS485 Invalid Frames) use
`total_increasing`.

| Sensor Name                       | Unit | Device Class | Precision | Enabled by default |
|-----------------------------------|------|--------------|-----------|--------------------|
| Sys Uptime                        | s    | duration     | 0         | yes                |
| Bridge Uptime                     | s    | duration     | 0         | yes                |
| Acq Rate                          | Hz   | frequency    | 2         | yes                |
| MQTT Data Update Rate             | Hz   | frequency    | 2         | no                 |
| RS485 Master Read Failures        |      |              | 0         | no                 |
| RS485 Invalid Frames              |      |              | 0         | no                 |
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
| EM540 Serial Read Rate            | Hz   | frequency    | 2         | yes                |
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
| TS65A Serial Read Rate            | Hz   | frequency    | 2         | yes                |
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
  `Serial Read Rate` (Hz) reports the downstream serial client's observed poll rate, computed as a
  windowed rate the same way the upstream `Acq Rate` sensor is. It is **pinned to the power-register
  (primary cycle-start anchor) reads** — one tick per cycle-start pin advance — so it reflects the
  client's true per-cycle refresh rate rather than the raw inbound-request count (which the sparse
  multi-read sweeps and discovery scans of a real client would otherwise inflate). Handy for
  confirming the serial frame latch is pacing a fast client
  (see [Serial Frame Latching](#serial-frame-latching-serial-clients-only)).

In addition, per-quantity **daily min/max extrema** are published for Power, Current, Voltage L-N,
and Voltage L-L, at both system and per-phase (L1/L2/L3) scope (for example `Daily Power Min`,
`Daily Voltage L-N L2 Max`). These are evaluated on every upstream frame at the master and reset at
local midnight. All daily extrema sensors are disabled by default.

## References

- [GitHub Repository](https://github.com/lerebel103/carlo-gavazzi-em540-bridge)
- [Docker Hub Image](https://hub.docker.com/r/lerebel103/carlo-gavazzi-em540-bridge)
- [Changelog](CHANGELOG.md)
- [Carlo Gavazzi EM540/EM530 Modbus Register Map](https://www.gavazziautomation.com/fileadmin/images/PIM/OTHERSTUFF/COMPRO/EM500_CPP_Mod_V1.3_13022024.pdf)
