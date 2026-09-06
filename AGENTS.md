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

## Register Layout Invariants (validated against production)

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