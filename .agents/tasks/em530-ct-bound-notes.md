# Design note — EM530 CT-ratio-aware current bound

PR #42, third Copilot HIGH finding. The spec-grounded frame validator rejects a
whole parsed primary-block frame on (a) an overflow sentinel (INT32 MSW ==
`0x7FFF`) or (b) datasheet-derived physical bounds. The per-phase / system
current bounds (`MAX_PHASE_CURRENT_A = 130`, `MAX_SYSTEM_CURRENT_A = 390`) are
derived from the EM540's DIRECT 65 A input. The codebase also supports the
EM530, which is CT-connected (5 A secondary): its decoded A L1/L2/L3 registers
carry the already-CT-scaled PRIMARY current, which legitimately exceeds 130 A.
With validation enabled by default, every EM530 frame above 130 A would be
falsely rejected and the bridge would go stale.

This change makes the current magnitude bound CT-ratio-aware **for EM530 only**,
leaving EM540 unchanged at 130 A / 390 A. It is a BOUND-only change: no decode
path, no `0x000B` Device Type overlay, no voltage/frequency bound, and no
sentinel behaviour was touched.

## Spec citations

Source: Carlo Gavazzi `EM500_CPP_Mod_V1.3` (13 Feb 2024),
`.agents/tmp/em500_protocol.txt`. Content rephrased for licensing compliance.

- Register `0x1003` (Modicom `304100`) "Current transformer ratio", `UINT32`,
  2 words. Stored value = CT ratio x 10: minimum 10 (CT 1.0), maximum 20000
  (CT 2000.0), default 10. EM530-only; not available on AV2/EM540 models — which
  matches `_EM530_STATIC_REGISTER_SPECS = ((0x1003, "CT ratio", 2, 0),)` in
  `em540_data.py`, added to the static map only when `is_em530` is True.
- §3.1: INT32/UINT32 word order is LSW->MSW. `0x1003` is decoded with the shared
  little-endian helper (`_convert_from_registers_little(..., UINT32)`); decoding
  is NOT reinvented.
- §4.1: current registers A L1 `0x0C` / A L2 `0x0E` / A L3 `0x10` are INT32,
  weight Ampere*1000. On EM530 these carry CT-scaled PRIMARY current.
- EM530 secondary base (rated) current = 5 A (datasheet).

## Derivation

Keeps the SAME 2x headroom factor the EM540 bound uses.

- `ct_ratio      = clamped_raw / 10`  (e.g. raw 2000 -> CT 200.0)
- `max_phase_a   = 2 * 5.0 * ct_ratio`  (headroom x secondary base x CT ratio)
- `max_system_a  = 3 * max_phase_a`  (mirrors the EM540 `390 = 3 x 130`)

With secondary base 5 A and scale 10, `max_phase_a` equals the clamped raw value
numerically (`2 * 5 * raw/10 == raw`). The code computes it legibly and only
documents the coincidence in a comment; it does not rely on it.

Examples: raw 2000 -> (2000 A, 6000 A); raw 10 (CT 1.0) -> (10 A, 30 A); raw
20000 (CT 2000.0) -> (20000 A, 60000 A).

## Fallback decision (missing / empty / not-yet-read `0x1003`)

For EM530, if `0x1003` is absent, empty, or all-zero (static block not yet read),
`compute_em530_current_bounds(None)` returns `(inf, inf)`, which SKIPS the current
magnitude bound. Rationale: a real EM530 legitimately produces currents far above
130 A, so falling back to the EM540 130 A bound would false-reject valid frames
and stale the bridge — the exact bug being fixed. The model-independent overflow
sentinel still guards against genuine overflow, so skipping only the magnitude
comparison is safe. Every other bound (voltage, frequency) runs unchanged.

Clamp vs skip are kept distinct and separately tested:

- raw PRESENT but out of `[10, 20000]` -> clamp into range, then compute (finite
  bound). A present-but-implausible register is still a usable CT hint.
- raw MISSING / empty / register not in map -> skip (infinite current bound).

## Caching / recompute approach

The validator stays a PURE function with explicit `max_phase_current_a` /
`max_system_current_a` kwargs and stable `range:a_l1/a_l2/a_l3/an` reason slugs;
it never learns about models or CT registers. The master owns all model/CT logic:

- `__init__` initialises `self._effective_max_phase_current_a` /
  `self._effective_max_system_current_a` to the configured EM540 defaults
  (130 / 390).
- `_recompute_effective_current_bounds()` sets them: EM540 -> configured
  defaults; EM530 -> decode `0x1003` (UINT32, LSW->MSW) via the shared helper and
  call `compute_em530_current_bounds`.
- It is called ONCE in `connect()` inside the `fresh_static_read = True` branch,
  right after `self._static_data_valid = True` and the buffer copy. The CT
  register is static and only read there; an ordinary reconnect keeps
  `_static_data_valid` True and does not re-read it. A comment at the call site
  records the invariant: any future change that forces a static re-read on
  reconnect must call `_recompute_effective_current_bounds()` again.
- `_validate_frame` passes the two cached floats. The 10 Hz accept path performs
  only two attribute reads — no CT decode, no allocation per tick.

## No new config surface

EM530 scaling is derived at runtime from the meter's own CT register.
`FrameValidationConfig` defaults (130 / 390) remain the EM540 values and the
config schema is unchanged (`config_test.py` and `config-default.yaml` untouched).

## Known scoping caveat (follow-up, not fixed here)

`is_em530` defaults to `False` in `Em540Frame.__init__` and is not set `True`
anywhere in the live path (no runtime model detection wiring; `MeterData()`
always constructs `Em540Frame()` with the default). The EM530 branch is therefore
test-exercised (tests drive `is_em530=True` explicitly) but not yet live-reachable
until model detection is wired. Auto-detection is deliberately NOT added here;
it is flagged for the PR as a follow-up.
