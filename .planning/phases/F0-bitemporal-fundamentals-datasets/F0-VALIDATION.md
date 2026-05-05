---
phase: F0
slug: bitemporal-fundamentals-datasets
status: passed
nyquist_compliant: true
wave_0_complete: true
created: 2026-05-05
updated: 2026-05-05
---

# Phase F0 - Validation Strategy

Reconstructed during `$gsd-validate-phase F0` because this completed phase had
PLAN, SUMMARY, RESULTS, and VERIFICATION artifacts but no prior VALIDATION.md.

## Test Infrastructure

| Property | Value |
|----------|-------|
| Framework | pytest 8.x |
| Config file | `pyproject.toml` |
| Quick run command | `uv run --extra dev pytest tests/unit/test_bitemporal_columns.py tests/integration/test_bitemporal_run_id.py -q --tb=short` |
| Full suite command | `uv run --extra dev pytest -q --tb=short` |
| Estimated runtime | Quick ~1s; full suite ~67s |

## Sampling Rate

- After every task commit: run the quick bitemporal validation command.
- After every plan wave: run the full non-live pytest suite.
- Before `$gsd-verify-work`: full suite must be green.
- Max feedback latency: about 67 seconds for the full local suite.

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| F0-01 | F0 | 1 | F0-BITEMP-01 | unit | `uv run --extra dev pytest tests/unit/test_bitemporal_columns.py -q --tb=short` | yes | green |
| F0-02 | F0 | 1 | F0-BITEMP-02 | unit | `uv run --extra dev pytest tests/unit/test_bitemporal_columns.py -q --tb=short` | yes | green |
| F0-03 | F0 | 1 | F0-BITEMP-03 | unit/integration | `uv run --extra dev pytest tests/unit/test_bitemporal_columns.py tests/integration/test_bitemporal_run_id.py -q --tb=short` | yes | green |
| F0-04 | F0 | 1 | F0-BITEMP-04 | unit | `uv run --extra dev pytest tests/unit/test_bitemporal_columns.py -q --tb=short` | yes | green |
| F0-05 | F0 | 1 | F0-ISSUE-01 | unit | `uv run --extra dev pytest tests/unit/test_bitemporal_columns.py -q --tb=short` | yes | green |
| F0-06 | F0 | 1 | F0-RUN-01 | integration | `uv run --extra dev pytest tests/integration/test_bitemporal_run_id.py -q --tb=short` | yes | green |
| F0-07 | F0 | 1 | F0-RUN-02 | unit | `uv run --extra dev pytest tests/unit/test_bitemporal_columns.py -q --tb=short` | yes | green |
| F0-08 | F0 | 1 | F0-REINGEST-01 | unit/integration | `uv run --extra dev pytest tests/unit/test_bitemporal_columns.py tests/integration/test_bitemporal_run_id.py -q --tb=short` | yes | green |
| F0-09 | F0 | 1 | F0-REINGEST-02 | documentation + tested code path | `uv run --extra dev pytest tests/unit/test_bitemporal_columns.py tests/integration/test_bitemporal_run_id.py -q --tb=short` | yes | green |
| F0-10 | F0 | 1 | F0-VERIFY-01 | unit/integration | `uv run --extra dev pytest tests/unit/test_bitemporal_columns.py tests/integration/test_bitemporal_run_id.py -q --tb=short` | yes | green |
| F0-11 | F0 | 1 | F0-VERIFY-02 | integration | `uv run --extra dev pytest tests/integration/test_bitemporal_run_id.py -q --tb=short` | yes | green |
| F0-12 | F0 | 1 | F0-VERIFY-03 | documentation | `uv run --extra dev pytest -q --tb=short` | yes | green |

## Wave 0 Requirements

Existing infrastructure covers all phase requirements:

- `pyproject.toml` supplies pytest configuration.
- `tests/unit/test_bitemporal_columns.py` covers base bitemporal invariants,
  direct transformer ergonomics, issue time, reingest sidecars, and static
  event-time fallback.
- `tests/integration/test_bitemporal_run_id.py` covers CLI run-id propagation,
  CLI reingest sidecar propagation, script-runner run-id propagation, script
  reingest propagation, and DuckDB bitemporal queryability.

## Manual-Only Verifications

All F0 phase requirements have automated verification.

Operational follow-up remains documented but is not a Nyquist gap: broad
historical re-transform row counts require local `data/bronze/` partitions that
are absent in this workspace. `F0-RESULTS.md` records the exact reingest
commands to run when those partitions are available.

## Validation Audit 2026-05-05

| Metric | Count |
|--------|-------|
| Gaps found | 2 |
| Resolved | 2 |
| Escalated | 0 |

Resolved gaps:

- Added CLI `--reingest` integration coverage that proves bronze sidecar
  `fetched_at` becomes silver `available_at`.
- Added direct `scripts/run_pipeline.py` silver-step coverage that proves the
  script runner threads `PipelineRunTracker.run_id` and `reingest=True` into
  `BaseSilverTransformer.run()`.

## External Audit Follow-Up 2026-05-05

| Finding | Requirement | Resolution | Automated Coverage |
|---------|-------------|------------|--------------------|
| DuckDB views failed on mixed pre/post-F0 Parquet schemas | F0-VERIFY-02 | `read_parquet(..., union_by_name=true)` now backs silver and gold views. | `tests/unit/test_duckdb_views.py` |
| Reingest sidecar discovery could include unrelated sibling datasets | F0-REINGEST-01 | Base discovery now uses explicit `BRONZE_SIBLING_DATASETS`; Open-Meteo historical opts into location siblings. | `tests/unit/test_bitemporal_columns.py` |
| Forecast transforms collapsed multiple `issue_time` vintages per delivery period | F0-ISSUE-01 | NDF and WINDFOR include `issue_time` in dedup keys when present. | `tests/unit/test_silver_transforms.py` |
| Malformed higher-priority sidecar timestamp could block valid fallback keys | F0-REINGEST-01 | Sidecar parsing now continues to later timestamp keys when one value is malformed. | `tests/unit/test_bitemporal_columns.py` |

Follow-up verification:

- `uv run --extra dev pytest tests/unit/test_duckdb_views.py tests/unit/test_bitemporal_columns.py tests/unit/test_silver_transforms.py tests/unit/test_openmeteo.py tests/integration/test_bitemporal_run_id.py -q --tb=short` - 117 passed, 1 warning.
- `uv run --extra dev ruff check src/gridflow/storage/duckdb.py src/gridflow/silver/base.py src/gridflow/silver/openmeteo/historical.py src/gridflow/silver/openmeteo/forecast.py src/gridflow/silver/elexon/wind_forecast.py src/gridflow/silver/elexon/demand_forecast.py tests/unit/test_duckdb_views.py tests/unit/test_bitemporal_columns.py tests/unit/test_silver_transforms.py tests/unit/test_openmeteo.py tests/integration/test_bitemporal_run_id.py` - passed.
- `uv run --extra dev pytest -q --tb=short` - 1010 passed, 253 skipped, 1 warning.

## Validation Sign-Off

- [x] All tasks have automated verification.
- [x] Sampling continuity has no three consecutive tasks without automated verify.
- [x] Wave 0 dependencies are already present.
- [x] No watch-mode flags are used.
- [x] Feedback latency is under 70 seconds for the full local suite.
- [x] `nyquist_compliant: true` is set in frontmatter.

Approval: approved 2026-05-05
