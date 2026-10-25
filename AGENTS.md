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
  `self._server_loop.call_soon_threadsafe(gate.publish, generation, snapshot)` (non-blocking
  listener-thread work only). A shared async context proxy (`app/utils/serial_pacing.py` — `FrameGate` +
  `SerialPacingContext`, IDENTICAL for both bridges) is installed ONLY on `serial_server.context`;
  TCP and RTU-over-TCP keep the plain shared `SimCore` and are byte-for-byte unaffected (serial-only
  scoping is structural, not a runtime guard).
- Pacing is **ANCHOR-BASED**, because real serial clients split one polling cycle into MULTIPLE
  separate reads (power block, then voltage/current, then energy). Each bridge supplies the FULL
  power-anchor set via the `anchors` constructor arg AND a SINGLE `primary_anchor` (the system-power
  register a real client reads FIRST every cycle) — the POWER registers a real client polls EVERY
  cycle:
  - EM540 (`em540_slave_bridge._SERIAL_PACING_ANCHORS`): `{0x0028 (system power), 0x0012 (per-phase
    power block start)}`, primary `0x0028`; confirmed from Victron dbus-cgwacs `Em540Commands`
    (system + per-phase power read every cycle at interval 0; voltage/current, energy and frequency
    staggered).
  - TS65A (`ts65a_slave_bridge._SERIAL_PACING_ANCHORS`): `{40097, 40099, 40101, 40103}` (Ac power
    value + per-phase AC power starts), primary `40097`; the Fronius equivalent power registers.
  These are a protocol property of the served map, not user tunables. A read overlaps an anchor when
  the anchor address falls in `range(address, address + count)` (overlap on the start address is
  sufficient); a read is a **cycle start** only when it overlaps the SINGLE primary cycle-start
  anchor.
- On each gated read: ONLY a read overlapping the SINGLE primary cycle-start anchor starts a new
  polling cycle — it bounded-waits for a generation STRICTLY NEWER than the currently PINNED
  generation, then PINS that `(generation, snapshot)` pair and serves from it, advancing the pin **at
  most once per client cycle** (this is what paces the sweep to the upstream rate, one pin step per
  sweep). EVERY other read — a SECONDARY power anchor (EM540 `0x0012`; TS65A `40099/40101/40103`) or a
  non-anchor block — is a follow-on in the same cycle: it serves IMMEDIATELY and coherently from the
  pinned pair with NO wait and NO pin advance, so every block of a sweep comes from the SAME frame
  (no cross-frame tear, and the sweep rate is NOT divided by the number of blocks as a per-read
  cursor would do). The first-ever read before any pin exists behaves like today's first read (wait
  once for a fresh frame and pin it), so startup is covered.
- **Generation-availability pin-age bound.** A client that NEVER reads the primary cycle-start anchor
  (a non-anchor-only poller) is not frozen on whatever frame its first read pinned: a follow-on read
  from such a client refreshes the pin to the LATEST ALREADY-PUBLISHED generation when a strictly
  newer one exists. This refresh is bounded purely by upstream generation **availability** (it only
  jumps to a frame the gate has already announced, with NO wait and NO configured wall-clock cadence
  clamp). A client that DOES drive the pin via the primary anchor sets a `_cycle_start_seen` flag, so
  its follow-on reads never take this refresh and every block of its cycle stays coherent with the
  one pinned frame.
- **Unserved device-id delegation.** Downstream controllers (e.g. Fronius inverters) scan a RANGE of
  Modbus unit ids to discover devices. A gated read for a device id this bridge does not serve is
  delegated straight to the real core (which returns the correct scan exception) and NEVER advances
  the pacing pin or waits for a frame, so discovery scan noise cannot re-pace the served client. This
  is checked AFTER the stale-breaker (a doomed request still fails closed first) but before any wait
  or snapshot serve.
- The proxy's `async_getValues` remains stale-breaker-first (returns `ExcCodes.DEVICE_BUSY`
  immediately when the tier-1 breaker is open — never blocks a doomed request); the anchor bounded
  wait uses `asyncio.wait_for(..., effective_wait_s)` where `effective_wait_s = min(pacing_max_wait_s,
  per-request SerialWireBudget)`; on timeout it serves the current frame (one possible duplicate on
  jitter) unless the breaker opened during the wait (fail-closed). The `(generation, snapshot)` pair
  is published together and sampled as one unit, so the pinned generation always matches the bytes
  returned. Writes (`async_setValues`) are a straight pass-through (never gated). The gate's asyncio
  primitive is created on the dedicated server loop (in `_build_serial_server`) so it binds to the
  right loop, and the serial await yields that loop so co-resident TCP/RTU servers are never starved.
  A single per-serial-server pin suffices (serial is single-request, half-duplex). Configurable per
  bridge via `*_slave.serial.pacing_max_wait_s` (default 0.12s; `<= 0` or `pacing_enabled: false`
  disables latching, pure pass-through) which MUST be strictly less than `serial.timeout` (validated).
  This preserves the staleness ladder (the latch sits before the datastore read and defers to tier 1)
  and does not touch the 0x000B overlay or the production parse/overlay path.
- The snapshot fast-path is scoped to **register reads only** (FC3 read-holding, FC4 read-input).
  The proxy replaces the serial server's WHOLE context, so bit reads (FC1 read-coils, FC2
  read-discrete-inputs) and any other func code reach it too; those fall through to the real core so
  they keep correct datastore semantics/exception. A register read whose window touches an address
  the datastore does NOT define also falls through to the core so it raises the correct
  `ILLEGAL_ADDRESS` (the flat snapshot tuple carries no validity flags and would otherwise serve
  zeros for a map gap — e.g. a TS65A read at 49999). The defined-address set is computed once from
  the SimRuntime block validity flags (`serial_pacing.valid_register_addresses`). Valid register
  reads are still served atomically from the pinned `(generation, snapshot)` pair — the fallthrough
  is scoped to the invalid/non-register cases so it never breaks the atomic frame guarantee.
- Each downstream serial client's observed poll rate is exposed as a Home Assistant diagnostic
  sensor per bridge ("EM540 Serial Read Rate" / "TS65A Serial Read Rate", Hz, enabled by default),
  computed in `ha_diagnostics` with the SAME windowed delta-count/delta-time pattern as the upstream
  "Acq Rate" sensor. The cumulative count is `SerialActivityTracker.cycle_read_count`, pinned to the
  POWER cycle-start anchor: it ticks ONCE per primary-anchor pin advance to a new generation (one
  client polling cycle) via `record_cycle_read`, driven from `SerialPacingContext.on_cycle_advance`
  on the serial server loop (no tick-loop/listener work added). It is deliberately NOT derived from
  `total_requests` — a real Victron/Fronius client issues several sparse reads per cycle (plus
  discovery scans), which would overstate the refresh rate; `total_requests` is retained for
  serial-client presence/connect inference only.
- A tight-loop serial client under a steady live upstream observes a smooth client-perceived cadence:
  the EM540 and TS65A cadence integration gates both measure ~12.5Hz median with ~4ms inter-arrival
  jitter and p95 under ~90ms against a 12.5Hz upstream (see `tests/integration/e2e_serial_bridge_test.py`,
  the two `*_observes_smooth_12_5hz_cadence_*` tests — CI-gating cadence regression tests run by the
  `integration` job). The synchronous, lost-wakeup-free `FrameGate` wake is what delivers this: a
  parked serial read is released in the same loop turn as the generation publish, with no deferred
  notify task. This pacing is implemented for SERIAL clients ONLY — TCP and RTU-over-TCP clients get
  no frame-latching/coherence guarantee (per-connection latching for concurrent TCP connections is
  significantly more complex; future work).
- The cadence gates poll ONLY the power anchor (one tiny read), so their per-cycle wire time is far
  under the 80ms upstream period and they correctly read ~12.5Hz — but that is a single-block poller,
  NOT a real client. A real MULTI-BLOCK client (Victron GX full EM540 read set; Fronius SunSpec
  blocks) is WIRE-BOUND BELOW upstream at the 9600-baud DEFAULT: the full cycle clocks ~120-160ms
  over the line (byte time = 10/9600 ~= 1.042ms, RTU response = 5 + 2*count bytes), exceeding the
  80ms (12.5Hz) period, so a flat-out client tops out ~6-9Hz. This is physical, EXPECTED at 9600, and
  NOT a bug — pacing still delivers smooth, duplicate-free, coherent frames capped at upstream, but
  cannot clock a client's reads out faster than the line. Do NOT "fix" this by asserting 12.5Hz for a
  multi-block client or by changing `async_getValues` to chase the rate; the only lever is baud. The
  9600 ceiling is NOT a hard GX limit: the Victron GX driver baud is a CLI arg and has been verified
  on real hardware to run at 115200 (edit `/opt/victronenergy/dbus-cgwacs/start-cgwacs.sh` to add
  `--baud 115200`, and set the matching `*_slave.serial.baudrate: 115200` on the bridge — both ends
  must agree), after which the client reaches the full ~12.5Hz. See README for the operator steps.
  Guarded by the multi-block throughput gates
  `test_{em540,ts65a}_serial_multi_block_client_is_wire_bound_below_upstream` (assert the ~6-9Hz band
  for the TEST harness's configured baud, no duplicate anchor frames, bounded jitter), which
  SUPPLEMENT (do not replace) the anchor-only cadence gates. The PTY harness does not throttle to
  baud, so the tests charge each read its real 9600 on-wire time (`_sim_wire_time_s`) to reproduce
  the physics. Background: `.agents/tasks/serial-throughput-cap.md`.
- `pacing_enabled` is runtime-settable/persisted and exposed as a Home Assistant SWITCH per bridge
  ("EM540 Serial Pacing" / "TS65A Serial Pacing", `ha_config_entities.py`, `entity_type="switch"`).
  The switch is bound to the live `SlaveSerialConfig.pacing_enabled` the `SerialPacingContext` reads
  on every request, so flipping it enables/disables pacing IMMEDIATELY with NO proxy reconstruction
  and NO service restart (and is persisted via `ConfigManager`). Turning it off lets an operator
  observe the natural, unpaced read rate (e.g. the raw wire-bound ceiling above).

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
- `app/utils/serial_activity.py`: `SerialActivityTracker` — infers downstream serial-client presence from request activity (`total_requests`); also exposes a monotonic `cycle_read_count` (one per primary cycle-start pin advance) that is the source for the serial read-rate (Hz) diagnostic
- `app/utils/serial_pacing.py`: serial-only frame-latching proxy (`FrameGate` + `SerialPacingContext`) and `valid_register_addresses` helper; anchor-based pacing, register-read-only snapshot fast-path

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