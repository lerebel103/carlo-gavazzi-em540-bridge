# Implementation Plan — EM530 live model detection

Companion design note: `.agents/tasks/em530-model-detect-notes.md` (read it first — it holds the
ordering-problem rationale, exact log strings, buffer-consistency approach, and fallback semantics).

All paths are inside the worktree `/Users/willc/Projects/em540_ts65a_converter/.worktrees/em530-model-detect`.
Use `git -C <worktree>` for git. Do NOT push. Follow the AGENTS.md validation order on each code item:
`make format` -> `make lint` -> impacted tests. Decode path and the `0x000B` overlay stay untouched.

- [ ] 1. Factor the device-type -> model mapping into a reusable helper, keeping `model_number` intact.
      Add a module-level `model_name_from_device_type(device_type_id: int) -> str` in
      `app/carlo_gavazzi/meter_data.py` that encodes the SAME mapping currently inline in
      `MeterData.model_number` (`1744..1763 -> "EM540"`, `2096`/`1956 -> "EM530"`, else `""`). Rewrite
      `MeterData.model_number` to call it (no behaviour change; `""` still returned for missing/empty
      `0x000B`). Do not duplicate the magic numbers.
      Files: app/carlo_gavazzi/meter_data.py
      Verify: `uv run pytest tests/carlo_gavazzi/meter_data_test.py -v` passes (model_number behaviour unchanged).

- [ ] 2. Add a direct unit test for the new helper and reaffirm `model_number`.
      In `tests/carlo_gavazzi/meter_data_test.py` add tests for `model_name_from_device_type` covering an
      EM540 id (e.g. 1744 and a boundary 1763), both EM530 ids (2096, 1956), and an unrecognized id
      (e.g. 9999 -> ""). Keep/affirm an assertion that `MeterData.model_number` returns the same strings
      for a frame whose `0x000B` is seeded with those ids (use realistic L-L ~400 V elsewhere in the
      fixture per AGENTS.md).
      Files: tests/carlo_gavazzi/meter_data_test.py
      Verify: `uv run pytest tests/carlo_gavazzi/meter_data_test.py -v` — new tests pass.

- [ ] 3. Add the model-detection helper on the master.
      In `app/carlo_gavazzi/em540_master.py` add `def _detect_and_apply_model(self) -> None` (sync, called
      from the first-connect static block; it may `await` a read only if made a coroutine — see below).
      Because it must issue a Modbus read for `0x1003`, make it `async` (`async def
      _detect_and_apply_model(self)`). It:
      (a) reads `device_type_id = self._front_data.frame.static_reg_map[0x000B].values[0]` (guard
      missing/empty -> treat as unrecognized);
      (b) resolves the model via `model_name_from_device_type` (import it from `meter_data`);
      (c) logs INFO `"Detected meter model: %s (device type %d)"` for a recognized model, or WARNING
      `"Unrecognized meter device type %r; treating as EM540 (direct input, current magnitude bound
      130/390 A)."` for empty/unknown (exact strings in the note);
      (d) if model == "EM530": set `frame.is_em530 = True` on BOTH `self._front_data.frame` and
      `self._back_data.frame`, and `frame.static_reg_map.update(_build_register_map(_EM530_STATIC_REGISTER_SPECS))`
      on both (import both names from `em540_data`); then
      (e) `await self._read_registers(self._front_data.frame.static_reg_map, reg_addrs=(0x1003,))`; on
      success copy the value into both buffers via `self._set_config_register(0x1003,
      self._front_data.frame.static_reg_map[0x1003].values)`; on failure log the EM530-CT-read WARNING
      and leave `0x1003` empty (so recompute hits the `(inf, inf)` skip). Do NOT fail the connection on
      a `0x1003` read error. For EM540/unrecognized, do nothing to the maps (leave `is_em530` False).
      Files: app/carlo_gavazzi/em540_master.py
      Verify: `uv run pytest tests/carlo_gavazzi/em540_master_test.py -v` still passes (helper not yet wired; no behaviour change).

- [ ] 4. Wire detection into `connect()` and update the single-refresh-point comment.
      In `connect()`'s first-static-read success branch (the `else:` after `_read_registers`, ~line 564),
      between `self._copy_meter_data(self._front_data, self._back_data)` and
      `self._recompute_effective_current_bounds()`, insert `await self._detect_and_apply_model()`. Update
      the comment block above the recompute call to note that model detection (and the EM530 `0x1003`
      read) now happens immediately above, so `is_em530`/`0x1003` are set before the single recompute;
      keep the existing warning that a future static re-read on reconnect must re-run detection + recompute.
      Files: app/carlo_gavazzi/em540_master.py
      Verify: `uv run pytest tests/carlo_gavazzi/em540_master_test.py tests/carlo_gavazzi/frame_validation_test.py -v` passes
      (EM540 regression tests `test_em540_bounds_unchanged` and connect tests stay green — EM540 path unchanged).

- [ ] 5. Add the model-detection integration tests (EM530 ids, EM540, unrecognized, CT-read-fail,
      buffer consistency, reconnect persistence).
      In `tests/carlo_gavazzi/em540_master_test.py` add a `TestModelDetection` class with a connect-driven
      mock client whose `read_holding_registers` returns the seeded device-type value for `0x000B` and a
      chosen `0x1003` for EM530 (realistic L-L ~400 V in the static block). Cover the 7 cases in the note:
      (1) id 2096 EM530 end-to-end — is_em530 True on both frames, 0x1003 present+read, INFO log asserted
      via `assertLogs("Em540Master")`, effective bounds equal `compute_em530_current_bounds(raw)`;
      (2) id 1956 same; (3) EM540 id 1744 — is_em530 False, no 0x1003 in map, no 0x1003 read requested,
      bounds 130/390, INFO "EM540" logged; (4) unrecognized 9999 and empty/0 — WARNING logged, EM540
      fallback 130/390; (5) EM530 + 0x1003 read error — connect succeeds, WARNING logged, bounds == inf;
      (6) both buffers agree on is_em530 / 0x1003 membership / values; (7) reconnect with
      `_static_data_valid` already True skips detection and leaves is_em530 and cached bounds unchanged.
      Reuse `_encode_uint32_le` and the CT-bound oracle values from the existing `26d3194` tests.
      Files: tests/carlo_gavazzi/em540_master_test.py
      Verify: `uv run pytest tests/carlo_gavazzi/em540_master_test.py -v` — all new tests pass, existing pass.

- [ ] 6. Full validation sweep.
      Run the AGENTS.md validation order end to end.
      Files: (none — validation only)
      Verify: `make format` then `make lint` clean; `uv run pytest tests/carlo_gavazzi -v` green; then
      `make test` (parallel, excludes integration) green. Report any command that cannot run locally with
      the exact failing invocation.
