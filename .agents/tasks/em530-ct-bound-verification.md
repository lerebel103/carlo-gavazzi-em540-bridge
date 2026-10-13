# Verification — EM530 CT-ratio-aware current bound

All commands run from the worktree
`/Users/willc/Projects/em540_ts65a_converter/.worktrees/em530-ct-bound`
(branch `feat/em530-ct-bound`), in the AGENTS.md "Agent Validation Workflow"
order.

## 1. `make format`

`uv run ruff format app/ tests/` -> `65 files left unchanged`.
`uv run ruff check --fix app/ tests/` -> `All checks passed!`.

## 2. `make lint`

`uv run ruff check app/ tests/` -> `All checks passed!`.
`uv run ruff format --check app/ tests/` -> `65 files already formatted`.

## 3. Impacted suites

```
uv run pytest tests/carlo_gavazzi/frame_validation_test.py \
              tests/carlo_gavazzi/em540_master_test.py \
              tests/config_test.py -q
```

Result: `332 passed, 1 skipped`.

Covers:
- `TestEm530CurrentBounds` (new): pure `compute_em530_current_bounds` (raw 2000 ->
  (2000, 6000); None -> (inf, inf); clamp low raw 5 -> (10, 30); clamp high raw
  30000 -> (20000, 60000)); validator with EM530 bounds (1500 A passes / >2000 A
  `range:a_l1`); An 3x bound (6000 passes / 6500 `range:an`); EM540 unchanged
  (150 A `range:a_l1`, 130 A passes); skip-fallback with inf bounds still rejects
  a current `0x7FFF` sentinel (`sentinel:a_l1`).
- `TestFrameValidationIntegration` (new EM530 loop tests via `acquire_data()`):
  raw 2000 1500 A publishes (extrema fold ~1500 A); >2000 A dropped
  (`range:a_l1`, front buffer unchanged, timestamp not advanced); missing
  `0x1003` skip-fallback publishes 1500 A but still drops a current sentinel
  (`sentinel:a_l1`); clamp raw 5 -> 10 A bound drops 150 A (`range:a_l1`); EM540
  effective bounds remain 130 / 390.
- Existing `frame_validation_test`, `em540_master_test`
  (`TestFrameValidationIntegration` plumbing), and `config_test` all still pass.

## 4. `make test` (full parallel unit suite, excluding integration)

`uv run pytest tests/ -v -n auto -m "not integration"` -> `585 passed, 1 skipped`.

## Not run

`make test-integration` (Dockerized end-to-end) is not required: this change adds
a model-aware BOUND only and does not alter framing / decode / downstream
bridging behaviour.
