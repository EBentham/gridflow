---
phase: F0
plan: F0
subsystem: silver
tags:
  - bitemporal
  - modelling
  - lineage
key-files:
  created:
    - tests/unit/test_bitemporal_columns.py
    - tests/integration/test_bitemporal_run_id.py
    - .planning/phases/F0-bitemporal-fundamentals-datasets/F0-RESULTS.md
    - .planning/phases/F0-bitemporal-fundamentals-datasets/F0-SUMMARY.md
    - .planning/phases/F0-bitemporal-fundamentals-datasets/F0-VERIFICATION.md
  modified:
    - src/gridflow/silver/base.py
    - src/gridflow/cli.py
    - scripts/run_pipeline.py
    - src/gridflow/silver/elexon/indo.py
    - src/gridflow/silver/elexon/fuelhh.py
    - src/gridflow/silver/elexon/demand_forecast.py
    - src/gridflow/silver/elexon/wind_forecast.py
    - src/gridflow/silver/openmeteo/historical.py
requirements-completed:
  - F0-BITEMP-01
  - F0-BITEMP-02
  - F0-BITEMP-03
  - F0-BITEMP-04
  - F0-ISSUE-01
  - F0-RUN-01
  - F0-RUN-02
  - F0-REINGEST-01
  - F0-REINGEST-02
  - F0-VERIFY-01
  - F0-VERIFY-02
  - F0-VERIFY-03
completed: 2026-05-05
---

# Phase F0: Bitemporal Upgrade for Fundamentals Datasets Summary

F0 added point-in-time silver lineage for future `gridflow_models` training and
backtesting.

## Commits

| Commit | Description |
|--------|-------------|
| `302db69` | Added failing bitemporal silver lineage tests. |
| `4a1f4bb` | Implemented base silver lineage stamping, run-id propagation, reingest timestamps, and issue-time preservation. |
| `3aad3f7` | Added DuckDB query verification for bitemporal silver columns. |
| `339ca48` | Fixed static/reference dataset event-time fallback. |
| `e2f4782` | Added regression coverage for the static dataset fallback. |

## What Changed

- `BaseSilverTransformer.run()` accepts `run_id` and `reingest`.
- Silver writes stamp `event_time`, `available_at`, `source_run_id`, and
  `dataset_version` before Parquet and CSV are written.
- Reingest mode uses bronze sidecar timestamps where available.
- CLI and script transform paths pass `PipelineRunTracker.run_id` into silver
  transformers.
- `elexon/ndf` and `elexon/windfor` preserve `issue_time` when publish metadata
  exists.
- Static/reference datasets fall back to target-date event times.

## Verification

- `python -m pytest -q --tb=short`
- Result: `1000 passed, 253 skipped, 1 warning`
- Changed-file Ruff check passed.

## Deviations From Plan

**Static/reference event_time fallback** - Found during full-suite verification.
Several existing silver datasets do not have `timestamp_utc` or
`settlement_date`/`settlement_period`. Failing hard would break established
reference-data pipelines, so F0 now stamps target-date midnight UTC for those
datasets and covers it with a regression test.

**Historical re-transform not run** - This workspace has no `data/bronze/`
partitions for the five F0 datasets. The reingest path is implemented and tested,
and `F0-RESULTS.md` records the exact commands to run when bronze is available.

## Self-Check: PASSED

All F0 requirements are implemented or documented with the missing-bronze caveat.
The full non-live test suite passes.
