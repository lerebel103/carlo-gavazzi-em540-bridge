---
name: code-review
description: Repository-specific review guidance for the Carlo Gavazzi EM540/EM530 to EM540 + Fronius TS65A Modbus bridge. Use when reviewing pull requests or code changes in this repository, to focus on this project's real-time, concurrency, and Modbus-protocol invariants.
---

# Code review guidance

This service bridges a single upstream Carlo Gavazzi EM540/EM530 meter to
multiple downstream consumers: an EM540-compatible Modbus slave (TCP,
RTU-over-TCP, optional serial RTU), a Fronius TS65A-compatible slave (TCP,
optional serial RTU), and MQTT telemetry for Home Assistant. It reads upstream
data at a ~10Hz target cadence and re-serves it.

Review for correctness and for the invariants below. Prefer flagging a real
behavioural regression over stylistic nits. When a change is an explicitly
scoped, temporary diagnostic (the PR says so), review the correctness of what it
sets out to do and do not litigate its long-term design or its accepted,
stated performance trade-offs.

## Real-time and concurrency invariants (highest priority)

- The master tick loop targets ~10Hz and is performance-sensitive. Flag any new
  blocking work (synchronous I/O, lock contention, `time.sleep`, unbounded
  allocation, logging handler I/O) added to the tick path.
- The main asyncio event loop is reserved exclusively for the upstream Modbus
  read path. Downstream Modbus servers and MQTT must run on separate event loops
  in their own threads. Flag any async server, long-running coroutine, or
  `run_coroutine_threadsafe` dependency added to the main loop.
- Do not add blocking work to Modbus listener notification paths or to the
  downstream request-serving path. Downstream reads must stay non-blocking and
  low-latency: they should serve from the in-memory register array without
  reaching back to the master, taking a shared lock the writer also holds, or
  doing I/O.
- MQTT connect/reconnect/publish failures must never interfere with the tick
  loop. MQTT is best-effort and outside the critical startup and tick paths.
- Off-loop logging pattern: expensive logging/formatting should be handed to a
  bounded queue (newest-wins) drained by a daemon worker, not performed inline on
  a hot path. Check new diagnostics follow this pattern.
- Watch for lock ordering and cross-thread hand-offs. The master publishes via an
  atomic front/back buffer pointer swap under a `threading.Condition`; listeners
  grab a whole-frame reference under that lock and copy out promptly. Flag reads
  that assume the front buffer stays stable indefinitely, or writers that mutate
  a buffer a reader may still hold.

## Failure model

- Stale or failed upstream data opens a circuit breaker in downstream Modbus
  bridges; while open, requests must return a Modbus exception, never stale
  values. Preserve this fail-closed behaviour.
- A short Modbus response with an unexpected register count intentionally
  triggers `os._exit(1)`. This is a deliberate systemic-corruption hard-fail, not
  a bug — do not "fix" it into a soft recovery.
- Ordinary connect/read transport failures should recover in-process without
  blocking. Be careful with reconnect logic: recovery must not reintroduce
  blocking behaviour.
- Downstream controllers scan a range of Modbus unit IDs. Exception responses to
  unserved IDs are normal scan noise (log at DEBUG); exceptions for a served ID
  are genuine failures (log at ERROR). Preserve this distinction.

## Modbus register-layout invariants (validated against real hardware)

These are deliberate and verified against a real EM540/EM530. Do NOT flag them as
bugs from a static read of the code.

- Address `0x000B` is intentionally shared: it is the Device Type register AND
  the high word of the L3-L1 line-to-line voltage INT32 (`0x000A`-`0x000B`). The
  downstream slave overlays `0x000B` with the static Device Type on purpose.
- At real values (~400 V L-L) the high word `0x000B` is 0, so the overlay clobbers
  no live data. L3-L1 is served from remapped registers `0x013A`/`0x013B`,
  populated before the overlay.
- Test/fixture data must use physically realistic values (e.g. L-L ~400 V) so
  synthetic data does not push a non-zero high word into `0x000B` and manufacture
  a collision that cannot happen on real hardware. Flag fixtures that use
  unrealistic values which fabricate such a collision.

## Modbus-protocol correctness

- Frame sizing: the max Modbus RTU ADU is 256 bytes. When capturing, buffering,
  or copying frames/registers, ensure caps retain a complete legal frame and all
  register values rather than silently truncating real traffic.
- Function codes: read paths are FC 3 (holding) / FC 4 (input); writes are FC
  6/16. When code keys behaviour off an address, check whether it should also key
  off the function code (e.g. a read-only trigger must not fire on a write).
- Check register decode/encode endianness and INT32/FLOAT32 word ordering against
  the documented layout.

## Testing expectations

- New features and bug fixes should have targeted unit tests, preferably hitting
  the impacted hot-path behaviour. Tests use the `unittest` style in this repo.
- Keep tests aligned with real scheduler semantics: the loop may skip missed
  ticks rather than run catch-up bursts.
- Integration tests are marked `integration`, live under `tests/integration/`,
  and are excluded from `make test` (run via `make test-integration`).
