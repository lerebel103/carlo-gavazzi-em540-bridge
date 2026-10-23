# AGENTS.md

## Purpose

This project bridges a single Carlo Gavazzi EM540/EM530 meter to multiple downstream consumers.

It reads upstream Modbus data at a tight 10Hz target cadence and re-serves it as:

- EM540-compatible Modbus/TCP, RTU-over-TCP, and optional serial RTU
- Fronius TS65A-compatible Modbus/TCP and optional serial RTU
- MQTT telemetry and diagnostics for Home Assistant

## Key Commands

- Test (unit, parallel): `make test`
- Test (unit, serial): `make test-serial`
- Test (end-to-end integration, Dockerized): `make test-integration`
- Lint: `make lint`
- Format: `make format`
- Start stack: `make up`
- Stop stack: `make down`
- Logs: `make logs`

Notes:

- `make test` runs `uv run pytest tests/ -v -n auto -m "not integration"` — parallel and excluding
  the integration suite.
- `make test-serial` runs the same unit suite non-parallel (`-m "not integration"`), useful for
  debugging flaky ordering or shared-state issues.
- `make test-integration` builds `Dockerfile.integration` and runs the end-to-end suite (marked
  `integration`) inside Docker; it is not part of `make test` and requires a working Docker engine.
- `make lint` uses Ruff for both check and format-check.

## Agent Validation Workflow

- For major code changes, run validation in this order:
  1. `make format`
  2. `make lint`
  3. Run impacted tests (at minimum) and then `make test` when practical.
- If `make lint` fails after `make format`, treat remaining issues as non-format lint violations and fix them explicitly.
- If a required command cannot run in the local environment (for example, missing dependencies), report the blocker and the exact failing command.

## Critical Runtime Rules

- The master tick loop is performance-sensitive and targets 10Hz.
- The main asyncio event loop is reserved exclusively for the upstream Modbus read path.
  All other async I/O (downstream Modbus servers, MQTT, etc.) must run on separate event
  loops in their own threads to avoid starving the upstream read of scheduling time.
- Do not add blocking work to the tick loop.
- Do not add blocking work to Modbus listener notification paths.
- Do not add async servers or long-running coroutines to the main event loop. Downstream
  Modbus servers run on dedicated daemon threads with their own event loops.
- MQTT connect, reconnect, and publish failures must never interfere with the tick loop.
- Downstream consumers must not receive stale data silently.

## Current Design Constraints

- The master uses a latest-snapshot double-buffer model.
- Listener workers consume snapshots asynchronously from the active front buffer.
- Slow consumers are allowed to miss intermediate updates; this is tracked in diagnostics.
- Heavy dynamic Modbus register groups are intentionally polled less often via `skip_n_read`.
- MQTT is best-effort and outside the critical startup and tick paths.
- Daily per-quantity/per-phase min/max extrema are tracked at the master on every frame
  (`DailyExtrema`). This is the only point that observes every frame; downstream consumers are
  subsampled and would miss peaks. The tracker is lock-free single-writer (master loop) so it never
  blocks the tick loop; the diagnostics reader copies values without a lock. Extrema reset at local
  midnight (DST-aware) and re-seed from the first post-boundary sample.
- Physical serial (RTU) lines have no transport connect/disconnect events, so downstream serial-client
  presence is inferred from request activity (`SerialActivityTracker`): a client is "active" while
  requests arrive within `serial_idle_timeout`, and connect/disconnect counters increment on activity
  edges evaluated at diagnostics cadence. TCP and RTU-over-TCP channels still use real transport
  connect/disconnect events.
- Downstream SERIAL reads are frame-latched to the upstream refresh rate so serial clients never
  receive duplicate frames (a serial client can poll faster than the meter refreshes). Each bridge
  keeps a monotonic `self._frame_generation` counter, bumped in `new_data` ONLY on the successful-write
  branch (next to `data_received`); after the bump it wakes the serial read via
  `self._server_loop.call_soon_threadsafe(gate.publish, generation)` (non-blocking listener-thread
  work only). A shared async context proxy (`app/utils/serial_pacing.py` — `FrameGate` +
  `SerialPacingContext`, IDENTICAL for both bridges) is installed ONLY on `serial_server.context`;
  TCP and RTU-over-TCP keep the plain shared `SimCore` and are byte-for-byte unaffected (serial-only
  scoping is structural, not a runtime guard). The proxy's `async_getValues` is stale-breaker-first
  (returns `ExcCodes.DEVICE_BUSY` immediately when the tier-1 breaker is open — never blocks a doomed
  request), then bounded-waits on the gate for a generation newer than the one this serial line was
  last served (`asyncio.wait_for(..., pacing_max_wait_s)`), then on timeout serves the current frame
  (one possible duplicate on jitter) unless the breaker opened during the wait (fail-closed). Writes
  (`async_setValues`) are a straight pass-through (never gated). The gate's asyncio primitive is
  created on the dedicated server loop (in `_build_serial_server`) so it binds to the right loop, and
  the serial await yields that loop so co-resident TCP/RTU servers are never starved. A single
  per-serial-server last-served cursor suffices (serial is single-request, half-duplex). Configurable
  per bridge via `*_slave.serial.pacing_max_wait_s` (default 0.12s; `<= 0` or `pacing_enabled: false`
  disables latching) which MUST be strictly less than `serial.timeout` (validated). This preserves the
  staleness ladder (the latch sits before the datastore read and defers to tier 1) and does not touch
  the 0x000B overlay or the production parse/overlay path.

## Register Layout Invariants (validated against production)

The authoritative register specification for the EM540/EM530 is the Carlo Gavazzi Modbus
communication protocol document:
<https://www.gavazziautomation.com/fileadmin/images/PIM/OTHERSTUFF/COMPRO/EM500_CPP_Mod_V1.3_13022024.pdf>.
Consult it (not third-party sources) when reasoning about register addresses, scaling, or the
meaning of configuration values such as measurement mode and measuring system.

These are deliberate, verified behaviours. Do NOT "fix" them based on a static read of the
code — the parsing path is validated against a real EM540/EM530 device and works correctly.
Changing the production parsing/overlay to satisfy a theoretical concern here will break a
working system.

- **Address `0x000B` is intentionally shared.** In the EM540 map, `0x000B` is the Device Type
  register AND the high word of the L3-L1 line-to-line voltage INT32 (which spans `0x000A`-`0x000B`).
  The downstream slave overlays `0x000B` with the static Device Type value on purpose; this is the
  correct real-meter behaviour.
- **No corruption occurs at real-world values.** Line-to-line voltages are ~400 V, i.e. raw
  `Volt*10 <= ~6000`, which fits entirely in the low word (`0x000A`). A real meter leaves the high
  word (`0x000B`) at 0, so overlaying the Device Type there does not clobber any live voltage data.
- **L3-L1 voltage is served from its remapped location.** Clients read L3-L1 from the remapped
  registers `0x013A`/`0x013B`, which are populated from `0x000A`/`0x000B` before the Device Type
  overlay is applied. The overlay therefore never costs the L3-L1 reading.
- **Test data must use physically realistic values.** Integration/unit fixtures must scale primary
  block fields to real ranges (e.g. L-L voltage ~400 V) so synthetic data does not push a non-zero
  high word into `0x000B` and manufacture a collision that cannot happen on real hardware. When
  verifying the `0x000B` overlap end-to-end, assert the L3-L1 value at its remapped location and the
  Device Type via a standalone single-register `0x000B` read — never rely on a wide primary-block
  comparison where `primary[0x000B] == Device Type` is self-fulfilling.

## Failure Model

- Stale or failed upstream data opens a circuit breaker in downstream Modbus slave bridges.
- While the circuit is open, downstream Modbus requests return a Modbus exception instead of stale values.
- Short Modbus responses with unexpected register counts intentionally trigger `os._exit(1)`.
  This is a deliberate hard-fail path because it is treated as a systemic client/protocol corruption condition.
- Ordinary Modbus connect/read transport failures should recover in-process.
- Downstream controllers (e.g. Fronius inverters) scan a range of Modbus unit IDs to discover devices.
  Exception responses to IDs the bridge does not serve are normal scan noise and are logged at DEBUG;
  exceptions for a served slave ID indicate a genuine failure and are logged at ERROR (`pdu_helper.py`).

### Staleness protection ladder

Staleness is caught by five independent tiers, each fail-closed. Listed by role, not by threshold
magnitude: the consumer-facing wall-clock breakers (tiers 1-2) first, then the upstream detectors
that cause them to fire (tiers 3-4), then the last-resort self-exit (tier 5). The thresholds are
therefore not monotonic by design (0.5s -> 3.0s -> 1.0s -> 5.0s -> 30.0s); each is tuned for its
own consumer or detector. Preserve this fail-closed layering when changing any threshold.

1. **Downstream Modbus bridge wall-clock breaker** — `em540_slave.update_timeout` /
   `ts65a_slave.update_timeout` = 0.5s. Per-request breaker; returns a Modbus exception instead of
   stale registers. Bounds Modbus consumers. Blind to a repeating-but-valid frame (it refreshes the
   frame timestamp and keeps the breaker satisfied).
2. **HA/MQTT wall-clock freshness watchdog** — `mqtt.ha_max_stale_s` = 3.0s (`<= 0` disables).
   Marks HA unavailable on wall-clock frame age and stops publishing. Bounds MQTT/HA consumers.
   Same blind spot as tier 1.
3. **Upstream stale/duplicate-frame detector** — `em540_master.stale_frame_detection.max_stale_s`
   = 1.0s. Detects a repeating frame (identical fast-changing registers) and fails closed via the
   `read_failed()` fan-out — this is the REASON data stops flowing for a stuck-but-plausible meter,
   which then trips tiers 1/2. Blind to a frame that keeps changing plausibly.
4. **Persistent-invalid-frame gate** — `em540_master.frame_validation.max_consecutive_invalid_s`
   = 5.0s. Fails closed when frames persistently violate the spec/physical bounds. Blind to an
   in-range but wrong value.
5. **Last-resort process self-exit** — `em540_master.health_max_stale_s` = 30.0s (runtime floor
   25s, 40s startup grace; `<= 0` disables). Self-exits so Docker restarts a wedged instance.
   Coarsest tier; only fires when nothing fresher recovered.

Modbus consumers are bound by tier 1; MQTT/HA by tier 2. Tiers 3/4 are the reason data stops
flowing for a stuck-but-plausible meter (a repeating-but-valid frame refreshes `data.timestamp`
and would otherwise keep every wall-clock breaker satisfied), not the only thing stopping it.

## Architecture Map

- `app/main.py`: startup and tick loop scheduling; wires master stats and the daily-extrema source into the MQTT bridge
- `app/carlo_gavazzi/em540_master.py`: upstream Modbus master, double buffering, listener dispatch; also hosts `Em540MasterStats` and `DailyExtrema` (per-frame daily min/max tracker)
- `app/carlo_gavazzi/em540_slave_bridge.py`: EM540 downstream slave bridge (TCP, RTU-over-TCP, optional serial RTU)
- `app/fronius/ts65a_slave_bridge.py`: Fronius TS65A-compatible downstream bridge (TCP, optional serial RTU)
- `app/carlo_gavazzi/em540_slave_stats.py`, `app/fronius/ts65a_slave_stats.py`: per-bridge downstream stats, including serial-activity tracking
- `app/home_assistant/ha_bridge.py`: MQTT bridge for Home Assistant
- `app/home_assistant/ha_diagnostics.py`: diagnostic sensor definitions and publication
- `app/utils/pdu_helper.py`: stale-data circuit breaker plus served-ID-aware exception logging for downstream Modbus requests
- `app/utils/serial_activity.py`: `SerialActivityTracker` — infers downstream serial-client presence from request activity

## Testing Guidance

- Prefer targeted unit tests when changing hot-path behavior.
- Run at least the impacted suites in `tests/carlo_gavazzi`, `tests/home_assistant`, and `tests/main_test.py` when changing loop, recovery, or bridge behavior.
- Keep tests aligned with the actual scheduler semantics. The loop may skip missed ticks rather than execute catch-up bursts.
- Integration tests live under `tests/integration/`, are marked `integration`, and are excluded from
  `make test`. Run them via `make test-integration` (Dockerized) when changing end-to-end behavior
  such as downstream serial/TCP bridging, recovery, or protocol framing.

## Change Guidance

- **Do not edit code without explicit user permission.** Always propose changes and wait
  for approval before modifying any source files. Reading and analysing code is fine.
- **Always require explicit user permission before pushing to GitHub (`git push`).** Local
  commits are fine, but never push to any remote without the user explicitly asking.
- Optimize root causes before micro-optimizing parsing code.
- Prefer reducing upstream Modbus I/O, allocations, and contention in hot paths.
- Be careful when changing reconnect logic; recovery must not reintroduce blocking behavior.
- Preserve the stale-data protection model unless the system requirements change.