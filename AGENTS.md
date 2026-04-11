# AGENTS.md

## Purpose

This project bridges a single Carlo Gavazzi EM540/EM530 meter to multiple downstream consumers.

It reads upstream Modbus data at a tight 10Hz target cadence and re-serves it as:

- EM540-compatible Modbus/TCP and RTU-over-TCP
- Fronius TS65A-compatible Modbus/TCP
- MQTT telemetry for Home Assistant

## Key Commands

- Test: `make test`
- Lint: `make lint`
- Format: `make format`
- Start stack: `make up`
- Stop stack: `make down`
- Logs: `make logs`

Notes:

- `make test` uses `$(PYTHON) -m pytest tests/ -v` (defaults to `python3`)
- `make lint` uses Ruff for both check and format-check

## Agent Validation Workflow

- For major code changes, run validation in this order:
  1. `make format`
  2. `make lint`
  3. Run impacted tests (at minimum) and then `make test` when practical.
- If `make lint` fails after `make format`, treat remaining issues as non-format lint violations and fix them explicitly.
- If a required command cannot run in the local environment (for example, missing dependencies), report the blocker and the exact failing command.

## Critical Runtime Rules

- The master tick loop is performance-sensitive and targets 10Hz.
- Do not add blocking work to the tick loop.
- Do not add blocking work to Modbus listener notification paths.
- MQTT connect, reconnect, and publish failures must never interfere with the tick loop.
- Downstream consumers must not receive stale data silently.

## Current Design Constraints

- The master uses a latest-snapshot double-buffer model.
- Listener workers consume snapshots asynchronously from the active front buffer.
- Slow consumers are allowed to miss intermediate updates; this is tracked in diagnostics.
- Heavy dynamic Modbus register groups are intentionally polled less often via `skip_n_read`.
- MQTT is best-effort and outside the critical startup and tick paths.

## Failure Model

- Stale or failed upstream data opens a circuit breaker in downstream Modbus slave bridges.
- While the circuit is open, downstream Modbus requests return a Modbus exception instead of stale values.
- Short Modbus responses with unexpected register counts intentionally trigger `os._exit(1)`.
  This is a deliberate hard-fail path because it is treated as a systemic client/protocol corruption condition.
- Ordinary Modbus connect/read transport failures should recover in-process.

## Architecture Map

- `app/main.py`: startup and tick loop scheduling
- `app/carlo_gavazzi/em540_master.py`: upstream Modbus master, double buffering, listener dispatch
- `app/carlo_gavazzi/em540_slave_bridge.py`: EM540 downstream slave bridge
- `app/fronius/ts65a_slave_bridge.py`: Fronius-compatible downstream bridge
- `app/home_assistant/ha_bridge.py`: MQTT bridge for Home Assistant
- `app/home_assistant/ha_diagnostics.py`: diagnostic sensor publication
- `app/utils/pdu_helper.py`: stale-data circuit breaker for downstream Modbus requests

## Testing Guidance

- Prefer targeted unit tests when changing hot-path behavior.
- Run at least the impacted suites in `tests/carlo_gavazzi`, `tests/home_assistant`, and `tests/main_test.py` when changing loop, recovery, or bridge behavior.
- Keep tests aligned with the actual scheduler semantics. The loop may skip missed ticks rather than execute catch-up bursts.

## Change Guidance

- Optimize root causes before micro-optimizing parsing code.
- Prefer reducing upstream Modbus I/O, allocations, and contention in hot paths.
- Be careful when changing reconnect logic; recovery must not reintroduce blocking behavior.
- Preserve the stale-data protection model unless the system requirements change.