# Verification: Spec-grounded upstream frame validation

Implemented per `.agents/tasks/frame-validation-design.md` and `.agents/tasks/plan.md`.
This was the FIRST implementation iteration (no `review.json` present — only
`design-review.json`, which the design already addresses).

## What was built

- **`app/carlo_gavazzi/frame_validation.py`** (new): pure `validate_primary_frame(...)` plus
  named, datasheet-cited constants (`_OVERFLOW_SENTINEL_MSW = 0x7FFF`, `_PRIMARY_INT32_MSW`
  tuple of 23 INT32 MSW indices, `MAX_PHASE_CURRENT_A=130`, `MAX_SYSTEM_CURRENT_A=390`,
  `MAX_VOLTAGE_LN_V=360`, `MAX_VOLTAGE_LL_V=625`, `MIN/MAX_FREQUENCY_HZ=40/70`). Deterministic
  order: full sentinel sweep (`sentinel:<slug>`) then range sweep (`range:<slug>`). No decode
  logic touched; reads parse output + raw primary-block registers only.
- **`app/carlo_gavazzi/em540_master.py`**: `Em540MasterStats.invalid_frame_total` +
  `last_invalid_frame_reason`; cached validation config in `__init__`; `_validate_frame`,
  `_count_invalid_frame`, `_maybe_log_invalid_frame` (rate-limited WARNING,
  `_INVALID_FRAME_LOG_INTERVAL=5.0`); gate wired into `acquire_data` AFTER `update_from_frame`
  and BEFORE the extrema fold / `_last_frame_monotonic` advance / buffer swap. Rejection sets
  `is_ok=False` and counts the failure (`_count_invalid_frame` + `_count_read_failure`) but does
  NOT call `_notify_listeners_read_failed()`: notifying listeners would force downstream circuit
  breakers open and mark HA unavailable immediately. Instead the buffer swap and timestamp advance
  are withheld, so consumers keep last-good and their own staleness timeouts / the master freshness
  watchdog open the breaker naturally only if bad frames persist. No breaker-trip code touched.
- **`app/config.py`**: `FrameValidationConfig` nested under `Em540MasterConfig` via
  `field(default_factory=...)`; `_validate_frame_validation` enforces bool toggles, finite `>0`
  numeric bounds (bool rejected), and `min_frequency_hz < max_frequency_hz`. Not added to
  `PERSISTED_FIELDS`.
- **`config-default.yaml`**: `frame_validation` block with cited defaults.
- **`app/home_assistant/ha_diagnostics.py`**: `invalid_frame_count` Sensor ("RS485 Invalid Frames",
  no unit, no device_class, `state_class=total_increasing`, diagnostic, disabled by default); added
  to `_all_sensors()` adjacent to the read-failed sensor; value set from `invalid_frame_total` in
  `mqtt_data()`.
- **`README.md`**: new "Upstream frame validation" config subsection + bounds table; diagnostics
  table row "RS485 Invalid Frames" adjacent to "RS485 Master Read Failures"; state-class preamble
  amended to note monotonic counters use `total_increasing`.

## Tests added

- `tests/carlo_gavazzi/frame_validation_test.py` (new, 17 tests): design matrix cases 1–10
  (normal pass; current/voltage/power/VA/var/system sentinels; `0x8000` not rejected; per-phase
  current bound + boundary; An defensive-guard semantics incl. per-phase-fires-first and direct
  override + boundary; L-N/L-L voltage bounds + boundaries; frequency band; sentinel toggle isolated
  on VA L1 + current fall-through contrast). Physically realistic fixtures (L-L ~400 V, L-N ~230 V,
  I ~10 A, 50 Hz) so `0x000B` MSW stays 0.
- `tests/carlo_gavazzi/em540_master_test.py`: `TestFrameValidationIntegration` (cases 11–15) —
  sentinel/out-of-range frames dropped end-to-end (invalid_frame_total++, read_failed_total++,
  listeners NOT signalled — `read_failed.assert_not_awaited()`, last-good `_front_data` retained,
  `_last_frame_monotonic` not advanced, extrema not folded); normal frame publishes/advances/folds;
  persistent bad frames freeze the
  timestamp while a single bad frame among good ones does not; config toggle fully bypasses the gate.
  `_make_config` helper gained a nested `frame_validation` SimpleNamespace (validation DISABLED by
  default there so pre-existing all-zero-frame plumbing tests — frequency 0 Hz legitimately fails
  the ≥40 Hz bound — stay focused; new tests opt in with `frame_validation_enabled=True`).
- `tests/home_assistant/ha_diagnostics_test.py`: new sensor value path, discovery validity (no
  device_class/unit, `total_increasing`, diagnostic, disabled, distinct `unique_id`), and bad-read
  sensor identity regression.
- `tests/config_test.py`: defaults, override load, non-bool toggle rejection, invalid-bound
  rejection (0/negative/non-numeric/bool/inf/nan), and frequency min<max enforcement.

## Deviation note

Plan step 10 suggested `_make_config` default `frame_validation.enabled=True`. That would break
pre-existing plumbing tests which feed all-zero mock frames (decoded frequency 0 Hz, which correctly
fails the ≥40 Hz physical bound — the design's "all-zero passes" edge note overlooked frequency).
To avoid altering those tests' unrelated fixtures, the helper defaults the toggle to `False` and the
dedicated validation tests enable it explicitly. Behaviour of the shipped product is unchanged
(`config-default.yaml` ships `enabled: true`).

## Commands run (in the worktree)

1. `make format` → clean (65 files unchanged / reformatted idempotently).
2. `make lint` → `ruff check` all checks passed; `ruff format --check` 65 files already formatted.
3. Impacted suites:
   - `uv run pytest tests/carlo_gavazzi/frame_validation_test.py -v` → 17 passed.
   - `uv run pytest tests/carlo_gavazzi/em540_master_test.py -q` → 54 passed.
   - `uv run pytest tests/config_test.py -q` → 241 passed, 1 skipped.
   - `uv run pytest tests/home_assistant/ha_diagnostics_test.py -q` → 22 passed.
4. `make test` (full parallel unit suite, `-m "not integration"`) → **565 passed, 1 skipped**.

`make test-integration` was not run: this change does not alter end-to-end serial/TCP framing,
and the plan explicitly scopes it out.
