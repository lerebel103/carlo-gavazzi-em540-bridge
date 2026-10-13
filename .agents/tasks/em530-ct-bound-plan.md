# Implementation Plan — EM530 CT-ratio-aware current bound

PR #42 fix for the third Copilot HIGH finding: the per-phase / system current
bound (`MAX_PHASE_CURRENT_A = 130.0`, `MAX_SYSTEM_CURRENT_A = 390.0`) is derived
from the EM540's DIRECT 65 A input. The EM530 is CT-connected (5 A secondary),
so its decoded PRIMARY current legitimately scales with the CT ratio and can far
exceed 130 A. With validation on by default, every EM530 frame above 130 A would
be wrongly rejected and the bridge would go stale. This change makes the current
magnitude bound CT-ratio-aware **for EM530 only**, leaving EM540 unchanged.

Scope guardrails (AGENTS.md + task): this adds a model-aware BOUND only. It must
NOT change how any value is DECODED, must NOT touch the `0x000B` Device Type
overlay, must NOT touch the frequency rule, and must NOT touch voltage bounds
(CTs scale current, not voltage — voltage/frequency bounds apply to BOTH models).
The accept path must stay allocation-free / lock-free; the CT register is static
(frame-to-frame constant) so the EM530 bounds are computed ONCE and cached, not
recomputed every 10 Hz tick.

## Authoritative spec facts (cite; do NOT re-fetch)

Source: `.agents/tmp/em500_protocol.txt` (extracted from Carlo Gavazzi
EM500_CPP_Mod_V1.3, 13 Feb 2024). Content rephrased for licensing compliance.

- CT ratio register (file lines ~829-837): Modicom `304100` / physical `1003h`,
  length 2 words, format `UINT32`. Stored value = CT ratio x 10: min = 10
  (CT 1.0), max = 20000 (CT 2000.0), default = 10 (CT 1.0). EM530-only; on
  EM540/AV2 the register is not available (consistent with
  `_EM530_STATIC_REGISTER_SPECS = ((0x1003, "CT ratio", 2, 0),)` in
  `app/carlo_gavazzi/em540_data.py`, added to the static map only when
  `is_em530` is True).
- §3.1 (file: "the word order is LSW->MSW"): INT32/UINT32 word order is
  LSW->MSW. Decode `0x1003` with the existing little-endian UINT32 helper; do
  NOT reinvent.
- §4.1 current registers (file ~"300013 000Ch 2 A L1 INT32 Value weight:
  Ampere*1000"): A L1 `0x0C`, A L2 `0x0E`, A L3 `0x10` are INT32, weight
  Ampere*1000. On an EM530 these carry the already-CT-scaled PRIMARY current.
- EM530 secondary base current = 5 A (datasheet; stated in the task).

## Derivation to implement

Keep the SAME 2x headroom factor the EM540 bound uses.

- `ct_ratio = raw_0x1003 / 10.0` (compute legibly; the numeric coincidence that
  `max_phase_current_a == raw_0x1003` is documented in a comment, NOT relied on).
- `max_phase_current_a (EM530) = 2 * 5.0 * ct_ratio` = `10 * ct_ratio` A.
- `max_system_current_a (EM530) = 3 * max_phase_current_a` (mirrors the existing
  `390 = 3 x 130` relationship / the "An = sum of three phase currents"
  defensive guard).
- EM540 (`not is_em530`): UNCHANGED — use the configured `MAX_PHASE_CURRENT_A`
  (130) and `MAX_SYSTEM_CURRENT_A` (390).
- Clamp raw `0x1003` to its documented valid range `[10, 20000]` BEFORE use.

## Chosen design decisions (recorded)

1. **Where the bounds are computed: in the master, cached; validator stays
   pure.** The pure validator keeps its explicit `max_phase_current_a` /
   `max_system_current_a` kwargs and does NOT learn about models or CT registers
   (keeps it a pure function with stable reason slugs and avoids per-tick model
   branching). The master computes the EFFECTIVE per-phase / An current bounds
   and passes them in via the existing `_validate_frame` call. Rationale: the
   validator signature already takes the bounds as parameters, so this is the
   smallest, cleanest change and keeps all model/CT logic in one place.

2. **Caching vs recompute: cache once, invalidate on static re-read.** The CT
   register is static and only (re)read inside `connect()` when
   `not self._static_data_valid` (see `em540_master.py` `connect()`); it is NOT
   re-read on an ordinary reconnect (reconnect keeps `_static_data_valid` True
   and only re-reads the few config registers via `_apply_meter_config`). So the
   EM530 bounds are computed once, right after the static block is read and
   `self._static_data_valid = True` is set (the `fresh_static_read = True`
   branch). Store them on cached float attributes
   (`self._effective_max_phase_current_a`, `self._effective_max_system_current_a`)
   initialised in `__init__` to the configured EM540 defaults
   (`self._max_phase_current_a` / `self._max_system_current_a`). The hot path
   then reads two cached floats — no per-tick CT decode, no allocation. Because
   the only path that refreshes `0x1003` also runs that recompute, a future
   change that forces a static re-read on reconnect must call the same recompute
   helper; add a short comment at the static-read site noting this invariant.
   (Verify during implementation that no other code path mutates
   `static_reg_map[0x1003]` after startup; current reading shows none.)

3. **Fallback for missing / empty / out-of-range / not-yet-read `0x1003`
   (EM530 only): SKIP the current magnitude bound; sentinel still applies.**
   Justification (task-mandated "never falsely reject a legitimate EM530
   frame"): a real EM530 can legitimately produce currents far above 130 A, so
   falling back to the 130 A EM540 bound would false-reject valid frames and
   stale the bridge — the exact bug being fixed. Skipping the magnitude bound is
   implemented by passing an effectively-infinite bound
   (`float("inf")`) for both the per-phase and An current bounds, so the
   validator's `abs(x) > bound` comparisons never trip for current while every
   other check (voltage, frequency, and crucially the overflow SENTINEL, which
   is model-independent and ALWAYS applies) runs unchanged. Clamp handling: a raw
   value `< 10` or `> 20000` is clamped into `[10, 20000]` and USED (it is a
   present-but-implausible register, and clamping to the nearest valid CT still
   yields a usable bound); only genuinely ABSENT/empty data triggers the skip.
   This keeps the two failure modes distinct and testable:
   - raw present but out-of-range -> clamp then compute (bound is finite);
   - raw missing / empty / register not in map -> skip (infinite current bound).

4. **No new config surface.** EM530 scaling is derived at runtime from the
   meter's own CT register. `FrameValidationConfig` defaults (130/390) remain the
   EM540 values and the config schema is unchanged. (If a reviewer insists on
   exposing the EM530 secondary base current or headroom factor as config, that
   is a separate decision; default is NOT to add it.)

5. **Reason strings unchanged.** The validator still returns `range:a_l1` /
   `range:a_l2` / `range:a_l3` / `range:an`; only the numeric bound passed in
   differs. Existing tests that assert these slugs stay valid.

## Known scoping caveat (flag, do not silently fix)

`is_em530` defaults to `False` in `Em540Frame.__init__` and is NOT set `True`
anywhere in the live code path (grep: no `is_em530=True`, no runtime model
detection wiring; `MeterData()` always constructs `Em540Frame()` with the
default). So on current live data the EM530 branch is latent until model
detection is wired elsewhere. This task is scoped to make the bound CT-aware
*given* `is_em530` + `static_reg_map[0x1003]`, exactly as specified, and tests
drive `is_em530=True` explicitly. Do NOT add EM530 auto-detection here; note it
in the PR description as a known follow-up so the reviewer is aware the branch is
test-exercised but not yet live-reachable. Confirm this during implementation
(re-grep) before relying on it.

## Plan items

- [ ] 1. Add a small pure helper to compute the EM530 current bounds from the raw
      CT register, co-located with the validator so the spec citation lives next
      to the constant. Implement `compute_em530_current_bounds(raw_ct_ratio: int
      | None) -> tuple[float, float]` in
      `app/carlo_gavazzi/frame_validation.py`: returns
      `(max_phase_current_a, max_system_current_a)`; `None`/absent -> `(inf, inf)`
      (skip, per decision 3); otherwise clamp `raw_ct_ratio` to `[10, 20000]`,
      `ct_ratio = clamped / 10.0`, `phase = 2 * 5.0 * ct_ratio`,
      `system = 3 * phase`. Add module constants `CT_RATIO_SCALE = 10`,
      `CT_RATIO_MIN = 10`, `CT_RATIO_MAX = 20000`, `EM530_SECONDARY_BASE_A = 5.0`
      and a docstring citing `em500_protocol.txt` `1003h` line and the 2x
      headroom equivalence (note the `phase == clamped raw` coincidence but keep
      the legible computation). Pure, allocation-light, no I/O.
      Files: `app/carlo_gavazzi/frame_validation.py`
      Verify: `uv run pytest tests/carlo_gavazzi/frame_validation_test.py -q`
      (existing suite still green after the additive change; new helper tests
      added in item 4).

- [ ] 2. Decode the CT register and cache the effective current bounds in the
      master. In `app/carlo_gavazzi/em540_master.py`:
      (a) in `__init__`, initialise `self._effective_max_phase_current_a =
      self._max_phase_current_a` and `self._effective_max_system_current_a =
      self._max_system_current_a` (EM540 defaults) right after the existing
      `self._max_system_current_a` assignment.
      (b) add a method `_recompute_effective_current_bounds(self) -> None` that:
      if `not self._front_data.frame.is_em530`, resets the two effective
      attributes to the configured EM540 bounds and returns; else reads
      `self._front_data.frame.static_reg_map.get(0x1003)`, decodes the raw UINT32
      via the existing `_convert_from_registers_little(reg.values,
      ModbusTcpClient.DATATYPE.UINT32)` helper from `meter_data.py` (treat a
      missing register, empty `.values`, or an all-zero/absent read as `None`),
      calls `compute_em530_current_bounds(raw)` from item 1, and stores the
      result on the two effective attributes.
      (c) call `_recompute_effective_current_bounds()` once in `connect()` inside
      the `fresh_static_read = True` branch, immediately after
      `self._static_data_valid = True` / the buffer copy, with a comment stating
      this is the single refresh point and any future reconnect-time static
      re-read must call it too (decision 2).
      (d) change `_validate_frame` to pass
      `max_phase_current_a=self._effective_max_phase_current_a` and
      `max_system_current_a=self._effective_max_system_current_a` (voltage/freq/
      sentinel args unchanged).
      Import `ModbusTcpClient` and `_convert_from_registers_little` as needed
      (reuse `meter_data`'s helper; do not duplicate decode logic). Keep the hot
      path to two attribute reads — no CT decode in `_validate_frame`/`acquire_data`.
      Files: `app/carlo_gavazzi/em540_master.py`
      Verify: `uv run pytest tests/carlo_gavazzi/em540_master_test.py -q` (existing
      `TestFrameValidationIntegration` + plumbing tests still pass; EM540 path
      unchanged because `is_em530` defaults False).

- [ ] 3. Extend the master test config/frame helpers to support an EM530 fixture.
      In `tests/carlo_gavazzi/em540_master_test.py` add a helper on
      `TestFrameValidationIntegration._build` (or a sibling builder) that lets a
      test construct a master whose `_front_data.frame` is an EM530 frame with a
      chosen raw `0x1003` value and then (re)compute the effective bounds. Prefer:
      after building the master, set
      `master._front_data.frame = Em540Frame(is_em530=True)` and populate
      `master._front_data.frame.static_reg_map[0x1003].values` with the
      little-endian UINT32 encoding of the raw CT value (reuse
      `tests/integration/test_data.py:encode_int32_le`-style encoding or encode
      inline: LSW first), then call
      `master._recompute_effective_current_bounds()`. Keep EM540 fixtures using
      the existing default (`is_em530=False`). Also expose an EM530 variant of
      `_realistic()` only if current magnitude needs to exceed the primary-block
      INT32 range — current `_realistic_registers(current=...)` already encodes
      arbitrary Ampere*1000 INT32 values, so pass e.g. `current=1_500_000`
      (1500 A) directly. Keep L-L ~400 V so `0x000B` MSW stays 0 (AGENTS.md
      register-layout invariant).
      Files: `tests/carlo_gavazzi/em540_master_test.py`
      Verify: `uv run pytest tests/carlo_gavazzi/em540_master_test.py -q`
      (helpers import/encode cleanly; existing tests unaffected).

- [ ] 4. Add pure-validator / helper unit tests for the CT bound in
      `tests/carlo_gavazzi/frame_validation_test.py`. Cover:
      (a) `compute_em530_current_bounds(2000)` -> `(2000.0, 6000.0)` (CT 200.0:
      `2*5*200 = 2000`, An = `3*2000 = 6000`); and the documented equivalence
      `phase == clamped raw`.
      (b) `None` -> `(inf, inf)`.
      (c) clamp: raw `5` -> treated as `10` -> `(100.0, 300.0)`; raw `30000` ->
      treated as `20000` -> `(20000.0, 60000.0)`.
      (d) validator-level, EM530 bounds: a frame with 1500 A per phase PASSES
      when `max_phase_current_a=2000.0`/`max_system_current_a=6000.0` (would fail
      the old 130 A bound); a frame > 2000 A is REJECTED with `range:a_l1`.
      (e) EM530 An bound: construct a frame whose per-phase currents each pass
      2000 A but override `data.system.An` to e.g. 6500 (> 6000) and assert
      `range:an` (mirrors the existing `test_an_defensive_guard_via_direct_override`
      pattern); confirm 6000 A An passes.
      (f) EM540 unchanged (regression, already covered but assert explicitly):
      150 A rejected `range:a_l1`, 130 A boundary passes with the default
      130/390 bounds.
      (g) skip-fallback: with `max_phase_current_a=inf`/`max_system_current_a=inf`
      a 1500 A frame passes BUT a current-MSW `0x7FFF` sentinel on A L1 is still
      rejected `sentinel:a_l1` (sentinel always applies).
      Reuse `_realistic_registers`, `_set_int32`, `_parse`, `_validate` helpers;
      use physically realistic L-L ~400 V so `0x000B` MSW stays 0.
      Files: `tests/carlo_gavazzi/frame_validation_test.py`
      Verify: `uv run pytest tests/carlo_gavazzi/frame_validation_test.py -q`
      (all new + existing pass).

- [ ] 5. Add master-loop integration tests for the EM530 CT path in
      `tests/carlo_gavazzi/em540_master_test.py` `TestFrameValidationIntegration`,
      using the item-3 EM530 builder. Cover end-to-end via `acquire_data()`:
      (i) EM530 with raw `0x1003 = 2000` (bound 2000 A): a 1500 A frame PUBLISHES
      (result True, `invalid_frame_total == 0`, extrema fold ~1500 A); a > 2000 A
      frame is DROPPED (`result False`, `last_invalid_frame_reason == "range:a_l1"`,
      front buffer unchanged, timestamp not advanced).
      (ii) EM530 with MISSING/empty `0x1003` (register present but values all
      zero, or removed): a 1500 A frame PUBLISHES (skip fallback), but a current
      `0x7FFF` MSW sentinel frame is still DROPPED (`sentinel:a_l1`).
      (iii) EM530 clamp: raw `0x1003 = 5` clamps to CT 1.0 -> bound 100 A; a
      150 A frame is DROPPED `range:a_l1` (sanity that clamp path yields a finite
      bound).
      (iv) EM540 regression: existing default-config tests still drop 150 A
      (`range:a_l1`) and publish 130 A — leave existing tests intact, add an
      explicit assertion only if not already covered.
      Files: `tests/carlo_gavazzi/em540_master_test.py`
      Verify: `uv run pytest tests/carlo_gavazzi/em540_master_test.py -q`.

- [ ] 6. Config regression (no schema change expected). Confirm
      `tests/config_test.py` still passes unchanged — this change adds NO config
      fields (decision 4). If, and only if, implementation chose to expose any
      EM530 config (it should not), update `config_test.py` and
      `config-default.yaml`; otherwise leave both untouched.
      Files: `tests/config_test.py` (expected: no change);
      `app/config.py`, `config-default.yaml` (expected: no change)
      Verify: `uv run pytest tests/config_test.py -q`.

- [ ] 7. Full validation gate (AGENTS.md "Agent Validation Workflow"), run in
      order. Fix any format/lint findings before proceeding; treat residual lint
      after format as explicit violations to fix.
      Files: none (verification only)
      Verify, in order:
      1. `make format`
      2. `make lint`
      3. Impacted suites:
         `uv run pytest tests/carlo_gavazzi/frame_validation_test.py tests/carlo_gavazzi/em540_master_test.py tests/config_test.py -q`
      4. `make test` (full parallel unit suite, excluding integration) — all green.
      (`make test-integration` is Dockerized and out of scope here; only run if
      the implementer changes end-to-end bridging behaviour, which this change
      does not.)

## Files touched (summary)

- `app/carlo_gavazzi/frame_validation.py` — new `compute_em530_current_bounds`
  helper + CT constants (item 1).
- `app/carlo_gavazzi/em540_master.py` — effective-bound cache, recompute on
  static read, `_validate_frame` passes effective bounds (item 2).
- `tests/carlo_gavazzi/frame_validation_test.py` — helper + validator CT tests
  (item 4).
- `tests/carlo_gavazzi/em540_master_test.py` — EM530 fixture builder + loop
  integration tests (items 3, 5).
- `tests/config_test.py`, `app/config.py`, `config-default.yaml` — expected
  UNCHANGED (item 6 confirms).

## Requirements verification checklist (do not skip during implementation)

- EM540 path byte-for-byte unchanged (bounds 130/390; `is_em530` False default). VERIFY via existing tests.
- `0x000B` Device Type overlay and all DECODE paths untouched. VERIFY by diffing only bound logic.
- Frequency and voltage bounds untouched, apply to both models. VERIFY no edits to those branches.
- Hot path: no CT decode / allocation per tick (cached floats only). VERIFY `_validate_frame`/`acquire_data` do not call the CT decode.
- Static re-read on reconnect: current code does NOT re-read `0x1003` on reconnect; recompute is tied to the one static-read site. VERIFY no other mutation of `0x1003` and that the comment documents the invariant.
- Fallback (missing/invalid `0x1003`) never false-rejects a legit EM530 frame; sentinel still applies. VERIFY via item 4(g) and item 5(ii).
- Reason slugs `range:a_l1/a_l2/a_l3/an` stable. VERIFY existing + new tests.
- Scoping caveat: `is_em530` not yet live-wired — flag in PR, do not auto-wire. VERIFY via re-grep.
