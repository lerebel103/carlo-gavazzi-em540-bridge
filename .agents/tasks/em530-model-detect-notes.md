# EM530 live model detection — design & implementation note

Branch: `feat/em530-model-detect` (worktree `.worktrees/em530-model-detect`), built on
`feat/frame-validation` / PR #42. Commit `26d3194` added the EM530 CT-ratio-aware current bound but
left it **dead**: `Em540Frame.is_em530` is never set `True` in the live path, so every meter is
treated as an EM540 and `_recompute_effective_current_bounds()` always takes the EM540 branch. This
change wires up live model detection so the EM530 path is actually reached.

Scope guard (AGENTS.md): detection runs ONLY on the startup/connect static-read path, never on the
10Hz tick. The register DECODE path and the `0x000B` Device Type overlay are untouched. No remote
push.

## What already exists (verified in the worktree)

- `MeterData.model_number` (`app/carlo_gavazzi/meter_data.py`) reads static register `0x000B` and
  maps device-type ids: `1744..1763 -> "EM540"`, `2096` or `1956 -> "EM530"`, else `""`. This is a
  pure read; it does not set any flag.
- `Em540Frame.__init__(is_em530=False)` (`app/carlo_gavazzi/em540_data.py`) sets `self.is_em530` and,
  only when `is_em530` is `True`, adds the EM530-only static register `0x1003` ("CT ratio", 2 regs)
  to `static_reg_map` via `_build_register_map(_EM530_STATIC_REGISTER_SPECS)`.
- `Em540Master._static_read_plan` is a tuple FROZEN at construction (`__init__`) from the default
  EM540 `static_reg_map.keys()` — it does NOT include `0x1003`.
- `Em540Master.connect()` static-read block (~line 549): on first connect (`not _static_data_valid`)
  it reads `frame.static_reg_map` using `_static_read_plan`, then sets `_static_data_valid = True`,
  copies front->back (`_copy_meter_data`), and calls `_recompute_effective_current_bounds()` — the
  SINGLE refresh point for the CT bound. That method reads `frame.is_em530` and decodes `0x1003`;
  today it always sees `is_em530 = False`.
- `compute_em530_current_bounds(raw_ct_ratio)` (`app/carlo_gavazzi/frame_validation.py`) is the
  fallback authority: `None` (register absent/empty/all-zero) -> `(inf, inf)` which SKIPS the current
  magnitude bound (sentinel still applies); a present-but-implausible raw value is clamped into
  `[CT_RATIO_MIN=10, CT_RATIO_MAX=20000]`.
- HA already surfaces the model: `ha_bridge.py` calls `sensors.set_device_model_number(data.model_number)`
  once static data is set, and `model_number` reads `0x000B`. So once detection runs, the model also
  appears in HA's device "model" field with no new diagnostics code. No extra surfacing is in scope.

## The ordering problem

`is_em530` is only knowable AFTER `0x000B` is read, but the EM530-only CT register `0x1003` is only
in `static_reg_map` (and thus only read) when `is_em530` was `True` at frame construction, and
`_static_read_plan` is frozen before any read. Flipping a flag post-read is not enough: `0x1003`
was never in the plan, so it was never read, so `_recompute_effective_current_bounds()` would see
`is_em530=True` but an absent `0x1003` and fall into the `(inf, inf)` skip — correct-by-fallback but
NOT the real CT bound on FIRST connect.

## Chosen solution (and why)

**Detect from the just-read `0x000B`, and when EM530 is detected, augment both frames in place and
issue ONE targeted extra read of `0x1003`, THEN recompute bounds — all inside the existing
`fresh_static_read` block, before `_recompute_effective_current_bounds()`.**

Sequence inside the `else:` success branch of the first static read (replacing the single
`_recompute_effective_current_bounds()` call):

1. `_static_data_valid = True`, `fresh_static_read = True` (unchanged).
2. `self._copy_meter_data(self._front_data, self._back_data)` (unchanged) — both frames now hold the
   just-read static block including `0x000B`.
3. **Detect + log + set flag + (EM530 only) read `0x1003`** via a new helper
   `self._detect_and_apply_model()` (see below).
4. `self._recompute_effective_current_bounds()` (unchanged call site — still the SINGLE refresh
   point; its guarding comment is updated to note the model is now set just above it).

`_detect_and_apply_model()` does:

- Read the device-type id from `self._front_data.frame.static_reg_map[0x000B].values[0]` and resolve
  the model name by reusing the SAME mapping as `MeterData.model_number`. To avoid duplicating the
  magic numbers (1744..1763 / 2096 / 1956), factor a small module-level helper in `meter_data.py`,
  e.g. `model_name_from_device_type(device_type_id: int) -> str`, and have both `MeterData.model_number`
  and the master call it. `model_number` keeps working byte-for-byte.
- Log the detected model at INFO including the raw id (see "Log messages"). Log a WARNING for an
  unrecognized/empty id and state the fallback.
- Determine `is_em530 = (model == "EM530")`.
- If `is_em530` and the current frames are not already EM530-shaped (i.e. `0x1003` not in
  `static_reg_map`), **augment BOTH frames**: for each of `self._front_data.frame` and
  `self._back_data.frame`, set `frame.is_em530 = True` and
  `frame.static_reg_map.update(_build_register_map(_EM530_STATIC_REGISTER_SPECS))`. (Export/import
  the existing `_build_register_map` and `_EM530_STATIC_REGISTER_SPECS` from `em540_data.py`.)
- If `is_em530`, issue a **one-time targeted read of `0x1003`** into the front frame:
  `await self._read_registers(self._front_data.frame.static_reg_map, reg_addrs=(0x1003,))`, then copy
  that register into the back frame (reuse `_set_config_register(0x1003, values)`, which writes a
  fresh list into BOTH buffers and is the established both-buffers-consistent mutation path). On read
  failure: log a WARNING, leave `0x1003` empty/all-zero so `compute_em530_current_bounds(None)` takes
  the documented `(inf, inf)` skip fallback (do NOT fail the connection — serving valid data with the
  sentinel-only guard beats stalling the bridge).
- For EM540 / unrecognized: leave `is_em530 = False`; do nothing to the maps. EM540 behaviour is then
  byte-for-byte identical to today (bounds 130/390, no `0x1003`, no extra read).

### Why this approach over the alternatives

- **vs. rebuilding both frames as `Em540Frame(is_em530=True)` and re-reading the full static block:**
  that discards the static block just read, costs a second full static read (~17 register groups) on
  every EM530 boot, and needs the frames re-wired into both buffers (the frame objects are referenced
  by `_front_data`/`_back_data`). The targeted approach reads exactly one extra 2-register group only
  when EM530 is detected, keeps all other static data already in hand, and reuses the existing
  `_build_register_map` augmentation that `Em540Frame.__init__` itself uses — so the resulting map is
  identical to a freshly-constructed EM530 frame's.
- **vs. adding `0x1003` to `_static_read_plan` unconditionally:** that would read an EM530-only
  register from EM540 meters on every boot; on a real EM540 `0x1003` is not available and the read
  would error (and `_read_registers` returns False -> connect treated as failed). Reading it only
  after confirming EM530 avoids touching EM540 behaviour at all.

This keeps: (a) no hot-path cost (all at connect); (b) both buffers consistent (augment both maps +
`_set_config_register` for the value + the existing front->back copy already ran); (c) `0x000B`
overlay untouched; (d) the single-refresh-point contract — `_recompute_effective_current_bounds()`
is still called exactly once, now after the model/CT register are in place.

## Reconnect resilience

An ordinary reconnect keeps `_static_data_valid == True`, so the whole first-connect block
(including `_detect_and_apply_model()` and the bounds recompute) is skipped. The `is_em530` flag,
the augmented `static_reg_map` (with `0x1003`), and the cached `_effective_max_*` floats all persist
on the long-lived master/frame objects from the first successful connect. So the model is detected
**once per process lifetime** and survives reconnects unchanged — no re-detection, no recompute, no
risk of losing the EM530 state on a buffer swap (both buffers were made consistent on first connect
and `_copy_meter_data` keeps them aligned every tick). This matches the existing CT-bound comment's
contract; that comment is updated to mention model detection now happens in the same block.

## Buffer-consistency approach (front + back)

Model state touches three things, all made consistent across BOTH `_front_data.frame` and
`_back_data.frame` on first connect, before any tick swap:

1. `is_em530` — set on both frames in `_detect_and_apply_model()`.
2. `static_reg_map` membership of `0x1003` — `.update(_build_register_map(...))` applied to both frames'
   maps so a later `_copy_meter_data` (which iterates `source_frame.static_reg_map.items()`) finds the
   key present in the target too (no `KeyError`).
3. `0x1003` values — written to both buffers via `_set_config_register(0x1003, values)`.

The front->back `_copy_meter_data` already runs immediately before detection; because we augment the
maps on BOTH frames and write the CT value to BOTH, a subsequent per-tick swap can never surface a
frame that is EM530 in flag but missing `0x1003`, or vice versa.

## Exact log messages

- Detected (INFO), recognized model — raw id always shown:
  - `logger.info("Detected meter model: %s (device type %d)", model, device_type_id)`
    e.g. `Detected meter model: EM530 (device type 2096)` / `Detected meter model: EM540 (device type 1744)`.
- Unrecognized / empty device type (WARNING) — raw id shown + explicit fallback:
  - `logger.warning("Unrecognized meter device type %r; treating as EM540 (direct input, current magnitude bound 130/390 A).", device_type_id)`
  - (If the `0x000B` register is missing/empty, log the same WARNING with `device_type_id` shown as
    the raw value or `None`.)
- EM530 CT register read failed (WARNING):
  - `logger.warning("EM530 detected but failed to read CT ratio register 0x1003; current magnitude bound disabled (overflow sentinel still applies).")`
- (Existing DEBUG line in `_recompute_effective_current_bounds` for the computed bound is retained.)

## Fallback behaviour for unrecognized / empty device type

Match `26d3194` exactly: an unrecognized/empty device type -> `is_em530` stays `False` -> treated as
**EM540 / direct input** -> `_recompute_effective_current_bounds()` takes the EM540 branch and uses
the configured datasheet bounds **130 A phase / 390 A system**. (This is distinct from the EM530
*missing-CT* fallback, which is `(inf, inf)` skip — that path is only reached when the model WAS
detected as EM530 but `0x1003` is absent/empty.) The WARNING above states this explicitly so an
operator sees why a never-before-seen meter is being treated as an EM540.

## Spec / code citations

- Device-type mapping: `MeterData.model_number`, `app/carlo_gavazzi/meter_data.py`.
- `0x1003` "CT ratio" (EM530-only, UINT32, 2 words, raw = CT*10, min 10 / max 20000 / default 10):
  `_EM530_STATIC_REGISTER_SPECS` in `em540_data.py` and the spec note in
  `compute_em530_current_bounds` (frame_validation.py), citing Carlo Gavazzi EM500_CPP_Mod_V1.3
  Modicom 304100 / physical 1003h. Authoritative doc:
  <https://www.gavazziautomation.com/fileadmin/images/PIM/OTHERSTUFF/COMPRO/EM500_CPP_Mod_V1.3_13022024.pdf>
- `0x000B` shared Device Type / L3-L1 high word overlay — AGENTS.md "Register Layout Invariants"
  (overlay is intentional; must NOT be altered).
- Single-refresh-point contract and reconnect semantics: comment above
  `_recompute_effective_current_bounds()` call in `connect()`.
- Both-buffers mutation pattern: `_set_config_register`, `_copy_meter_data` (em540_master.py).

## Tests to add / extend (`tests/carlo_gavazzi/`)

Fixtures MUST use physically realistic values (AGENTS.md): L-L voltage ~400 V so the `0x000B` high
word stays 0 and the overlay invariant holds. Device type is a single value in register `0x000B`.
Use the project's logging-capture convention (`assertLogs`) on the master's logger
(`logging.getLogger("Em540Master")`) to assert the model is logged.

New/extended tests in `tests/carlo_gavazzi/em540_master_test.py` (new class, e.g.
`TestModelDetection`, using a connect-driven mock client that returns a device-type value for `0x000B`
and a chosen `0x1003` for EM530):

1. **EM530 id 2096 end-to-end connect:** static read returns device type 2096; after `connect()`:
   `frame.is_em530 is True` on BOTH front and back frames; `0x1003` present in both maps; a WARNING is
   NOT logged; INFO log contains `Detected meter model: EM530 (device type 2096)` (assertLogs);
   `_effective_max_phase_current_a` / `_system` equal the CT-scaled bound from
   `compute_em530_current_bounds(raw)` (reuse the raw/encoding helper `_encode_uint32_le` and the
   expectations from the `26d3194` bound tests).
2. **EM530 id 1956:** same as (1) with device type 1956 (asserts both EM530 ids map correctly).
3. **EM540 id (e.g. 1744):** after connect, `is_em530 is False` on both frames; `0x1003` NOT in either
   map; no extra `0x1003` read issued (assert the read plan / call args never request `0x1003`);
   bounds remain 130/390; INFO log contains `Detected meter model: EM540 (device type 1744)`.
4. **Unrecognized / empty device type (e.g. 9999 and 0/empty):** WARNING logged naming the raw id and
   stating the EM540 fallback; `is_em530 is False`; bounds 130/390 (matches `26d3194` fallback).
5. **EM530 detected but `0x1003` read fails:** CT read returns an error; connection still succeeds
   (not failed); WARNING logged; `_recompute_effective_current_bounds` takes the `(inf, inf)` skip
   (phase/system bounds == inf); sentinel still applies.
6. **Both buffers consistent after connect:** explicitly assert front and back frames agree on
   `is_em530`, `0x1003` membership, and `0x1003` values after an EM530 connect.
7. **Reconnect persistence:** after a successful EM530 first connect, force a second `connect()` with
   `_static_data_valid` already True; assert the static read / detection block is skipped (no new
   `0x000B`/`0x1003` reads for detection), `is_em530` stays True, and the cached effective bounds are
   unchanged (not recomputed to a wrong value).

Extend `tests/carlo_gavazzi/meter_data_test.py` if a `model_name_from_device_type` helper is factored:
add a direct unit test of the helper for an EM540 id, both EM530 ids, and an unrecognized id, and keep
an existing-behaviour assertion that `MeterData.model_number` still returns the same strings.

Note: the existing `TestFrameValidationIntegration._make_em530` helper and the `26d3194` bound tests
remain valid (they construct an EM530 frame directly and recompute); they are the oracle for the
CT-scaled expected values in tests 1-2.

## Existing suites that MUST stay green

- `tests/carlo_gavazzi/em540_master_test.py`
- `tests/carlo_gavazzi/meter_data_test.py`
- `tests/carlo_gavazzi/frame_validation_test.py`
- `tests/carlo_gavazzi/em540_data_test.py` (frame construction / register maps)
- Plus the full `make test` suite (parallel, excludes integration) once impacted suites pass.

Validation order per AGENTS.md: `make format` -> `make lint` -> impacted tests -> `make test`.

## Verification evidence (recorded run)

Ran inside the worktree, AGENTS.md Agent Validation Workflow order:

1. `make format` — ruff format: 65 files unchanged (clean).
2. `make lint` — ruff check (+ --fix) and format-check: all checks passed, 65 files already
   formatted. (First pass auto-fixed one import-ordering issue in `em540_master.py`; re-run clean.)
3. Impacted suites —
   `uv run pytest tests/carlo_gavazzi/em540_master_test.py tests/carlo_gavazzi/meter_data_test.py
   tests/carlo_gavazzi/frame_validation_test.py -q`: **127 passed** in 0.91s.
4. `make test` (parallel, excludes integration): **598 passed, 1 skipped** in 5.38s.

`make test-integration` NOT run: no framing/transport change (detection is confined to the
first-connect static-read path; the hot path and protocol framing are untouched).

### What was implemented

- `app/carlo_gavazzi/meter_data.py`: factored `model_name_from_device_type(device_type_id)` (shared
  1744..1763 -> EM540, 2096/1956 -> EM530, else ""); `MeterData.model_number` now delegates to it
  (behaviour unchanged).
- `app/carlo_gavazzi/em540_master.py`: new `async _detect_and_apply_model()` run once in `connect()`'s
  first-static-read branch, between `_copy_meter_data` and `_recompute_effective_current_bounds()`.
  It decodes Device Type (0x000B), logs the model (INFO with raw id; WARNING + EM540 fallback for
  unrecognized/empty), and for EM530 sets `is_em530` + adds `0x1003` on BOTH frames, reads `0x1003`
  once, and mirrors it into both buffers via `_set_config_register`. CT-read failure is non-fatal
  (WARNING; recompute takes the (inf, inf) skip). The single-refresh-point comment was updated to note
  detection now runs immediately above the recompute and must be re-run with it on any future static
  re-read. EM540 path unchanged (no 0x1003 read, bounds 130/390).
- Tests: `meter_data_test.py` adds `TestModelNameFromDeviceType` (helper + model_number parity);
  `em540_master_test.py` adds `TestModelDetection` with 8 connect-driven cases (EM530 2096/1956,
  EM540 1744, unrecognized 9999, empty 0, EM530 CT-read failure, front/back buffer consistency,
  reconnect persistence), asserting logs via `assertLogs("Em540Master")`.
