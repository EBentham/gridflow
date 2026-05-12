---
phase: F0
slug: bitemporal-fundamentals-datasets
status: complete
milestone: v0.8
---

# F0 Plan - Bitemporal Upgrade for Fundamentals Datasets

## Phase Requirements

| ID | Description | Acceptance |
|----|-------------|------------|
| F0-BITEMP-01 | Every successful silver write adds `event_time`, `available_at`, `source_run_id`, and `dataset_version`. | Tests assert these columns exist on representative silver outputs and persisted Parquet/CSV outputs include them. |
| F0-BITEMP-02 | `event_time` is derived from each dataset's semantic delivery timestamp. | Settlement-period datasets use `settlement_period_to_utc`; timestamp datasets reuse `timestamp_utc`; static/reference datasets fall back to target-date midnight UTC. |
| F0-BITEMP-03 | Normal transforms stamp one UTC `available_at` per write. | Integration test shows row `available_at` is UTC-aware and close to the matching run's `completed_at`. |
| F0-BITEMP-04 | `dataset_version` comes from transformer class metadata. | Unit tests cover the base default and the five in-scope transformer classes. |
| F0-ISSUE-01 | Forecast-style in-scope datasets expose `issue_time` where source publish metadata exists. | Tests for `elexon/ndf` and `elexon/windfor` either assert populated `issue_time` or record a documented source limitation. |
| F0-RUN-01 | CLI/script transform paths pass the active run id into `transformer.run()`. | Integration test joins silver `source_run_id` to `pipeline_runs.run_id`. |
| F0-RUN-02 | Direct transformer usage stays ergonomic. | Unit test calls `run(target_date)` without a run id and sees a non-empty synthetic `source_run_id`. |
| F0-REINGEST-01 | Re-ingest mode reconstructs historical `available_at` from bronze sidecars. | Unit/integration test uses a fixture sidecar timestamp and asserts silver `available_at` equals it. |
| F0-REINGEST-02 | Five demand-forecast foundation datasets are ready to re-transform from existing bronze. | `F0-RESULTS.md` records the exact commands and the local missing-bronze caveat for `elexon/indo`, `elexon/fuelhh`, `elexon/windfor`, `elexon/ndf`, and `open_meteo/historical`. |
| F0-VERIFY-01 | Tests prove bitemporal invariants and run traceability. | Focused unit/integration tests pass locally. |
| F0-VERIFY-02 | DuckDB views can query the new columns. | `SELECT event_time, available_at, source_run_id, dataset_version ... LIMIT 5` succeeds for at least `silver_indo`. |
| F0-VERIFY-03 | Handoff results are documented. | `F0-RESULTS.md` includes commands, test output, DuckDB checks, row counts, and caveats. |

## Architectural Responsibility Map

| Capability | Primary Tier | Files |
|------------|--------------|-------|
| Bitemporal column injection | Silver base class | `src/gridflow/silver/base.py` |
| Event-time derivation | Silver base class plus transformer conventions | `src/gridflow/silver/base.py`, in-scope transformers |
| Run-id propagation | CLI/script run tracker into transformer | `src/gridflow/cli.py`, `scripts/run_pipeline.py`, `src/gridflow/observability.py` |
| Historical `available_at` reconstruction | Silver re-ingest helper | `src/gridflow/silver/base.py`, `src/gridflow/cli.py`, `scripts/run_pipeline.py` |
| Dataset versioning | Transformer class metadata | `src/gridflow/silver/elexon/*.py`, `src/gridflow/silver/openmeteo/historical.py` |
| DuckDB compatibility | Existing view registration | `src/gridflow/storage/duckdb.py` |

## Standard Stack

No production dependencies. Keep tests on existing pytest tooling unless we
explicitly decide to add Hypothesis to the dev dependency group.

## Tasks

### Task 1 - RED: bitemporal invariant tests

Read first:

- `src/gridflow/silver/base.py`
- `src/gridflow/silver/elexon/indo.py`
- `src/gridflow/silver/elexon/fuelhh.py`
- `src/gridflow/silver/elexon/wind_forecast.py`
- `src/gridflow/silver/elexon/demand_forecast.py`
- `src/gridflow/silver/openmeteo/historical.py`
- `tests/unit/test_silver_transforms.py`
- `tests/unit/test_openmeteo.py`
- `tests/conftest.py`

Files:

- `tests/property/test_bitemporal_columns.py` or, if the project prefers not to
  add a `property` directory, `tests/unit/test_bitemporal_columns.py`.

Actions:

- Add parametrized tests for the five in-scope transformers.
- Assert the four base bitemporal columns are present, typed, and non-null for
  non-empty outputs.
- Assert `available_at` is UTC-aware.
- Assert observed-data `event_time <= available_at` for INDO, FUELHH, and
  Open-Meteo historical.
- Add forecast-specific assertions for `issue_time` on NDF and WINDFOR, or mark
  the exact source-field limitation to resolve before implementation.

Expected result: tests fail before Task 2 because base injection does not exist.

### Task 2 - GREEN: inject bitemporal columns in `BaseSilverTransformer`

Files:

- `src/gridflow/silver/base.py`

Actions:

- Add `DATASET_VERSION: ClassVar[str] = "1.0.0"`.
- Change `run(self, target_date: date)` to accept:
  - `run_id: str | None = None`
  - `reingest: bool = False`
- Generate `adhoc-...` run ids when no run id is supplied.
- Derive `event_time` through a base helper:
  - use `timestamp_utc` when present;
  - derive from `settlement_date` and `settlement_period` otherwise;
  - fall back to target-date midnight UTC for static/reference datasets.
- Capture `available_at` once before writes, or use sidecar-derived time in
  re-ingest mode.
- Add `source_run_id` and `dataset_version`.
- Preserve existing write behavior: Parquet and CSV are both written after
  bitemporal injection.

Expected result: Task 1 tests pass for base columns.

### Task 3 - Thread run id from CLI and script runners

Files:

- `src/gridflow/cli.py`
- `scripts/run_pipeline.py`
- `src/gridflow/observability.py` only if a clearer `current_run_id` property is useful.

Actions:

- Pass `tracker.run_id` into each `transformer.run(...)` call in:
  - `gridflow transform`
  - `gridflow pipeline` through its call to `transform`
  - `gridflow backfill` through its call to `transform`
  - `scripts/run_pipeline.py --step silver`
- Add a transform CLI/script option for re-ingest mode and thread it through the
  call chain.
- Keep existing command behavior unchanged when the flag is absent.

Tests:

- Add `tests/integration/test_run_id_propagation.py`.
- Use isolated temp `GRIDFLOW_*` paths or fixture bronze.
- Assert silver rows' `source_run_id` matches a row in `pipeline_runs`.

### Task 4 - Re-ingest helper from bronze sidecars

Files:

- `src/gridflow/silver/base.py`
- `src/gridflow/cli.py`
- `scripts/run_pipeline.py`

Actions:

- Add a helper such as `_available_at_from_bronze(target_date: date)`.
- Search the appropriate bronze date directory for `raw_*.meta.json` sidecars.
- Parse the most appropriate timestamp field available in the sidecar.
- Use the latest timestamp for that target date when multiple raw files exist.
- Fall back to write-time UTC when sidecars are absent, and log the fallback.
- Add `--reingest` to the CLI transform path and script runner.

Tests:

- Fixture a bronze response plus sidecar timestamp.
- Run transform in re-ingest mode.
- Assert silver `available_at` equals the sidecar timestamp.

### Task 5 - Version and issue-time handling for in-scope transformers

Files:

- `src/gridflow/silver/elexon/indo.py`
- `src/gridflow/silver/elexon/fuelhh.py`
- `src/gridflow/silver/elexon/wind_forecast.py`
- `src/gridflow/silver/elexon/demand_forecast.py`
- `src/gridflow/silver/openmeteo/historical.py`

Actions:

- Add explicit `DATASET_VERSION: ClassVar[str] = "1.0.0"` to the five in-scope
  transformer classes.
- For `DemandForecastTransformer` and `WindForecastTransformer`, preserve
  publish metadata as `issue_time` when the source payload includes
  `publishDateTime` or `publishTime`.
- Keep `issue_time` out of observed datasets unless a source-specific reason
  emerges.

### Task 6 - DuckDB view sanity check

Files:

- `src/gridflow/storage/duckdb.py` only if a problem is found.

Actions:

- Re-transform one representative dataset after Tasks 2-5.
- Refresh/init the catalogue.
- Run:

```sql
SELECT event_time, available_at, source_run_id, dataset_version
FROM silver_fuelhh
LIMIT 5;
```

- Confirm old/new Parquet schema mixing is acceptable. If DuckDB does not union
  schemas as expected, document the failure and implement the smallest safe view
  registration change.

### Task 7 - Re-transform the five in-scope datasets

Commands:

```powershell
gridflow transform elexon indo --reingest --start 2022-01-01 --end 2026-05-04
gridflow transform elexon fuelhh --reingest --start 2022-01-01 --end 2026-05-04
gridflow transform elexon windfor --reingest --start 2022-01-01 --end 2026-05-04
gridflow transform elexon ndf --reingest --start 2022-01-01 --end 2026-05-04
gridflow transform open_meteo historical --reingest --start 2022-01-01 --end 2026-05-04
```

Actions:

- Record row counts before and after.
- Record any missing bronze gaps rather than silently treating them as failures.
- Write `.planning/phases/F0-bitemporal-fundamentals-datasets/F0-RESULTS.md`.

## Verification Steps

1. Focused bitemporal tests pass.
2. Run-id propagation integration test passes.
3. Re-ingest sidecar timestamp test passes.
4. DuckDB query returns the four base bitemporal columns.
5. Re-transform row counts are documented in `F0-RESULTS.md`.
6. The plan records whether `issue_time` was implemented or intentionally
   deferred for `elexon/ndf` and `elexon/windfor`.

## Out of Scope For F0

- Append-only revision storage.
- Re-ingesting stack-model datasets.
- Building `gridflow_models`.
- Adding a commodity price connector.
- Changing Gold-layer modelling datasets.
- Scheduled live monitoring.

## Definition Of Done

F0 is done when all v0.8 F0 requirements are implemented, tests pass, the five
foundation datasets are re-transformed or their missing-bronze gaps are
documented, DuckDB can query the new fields, and `F0-RESULTS.md` gives the
future `gridflow_models` work a clear handoff.
