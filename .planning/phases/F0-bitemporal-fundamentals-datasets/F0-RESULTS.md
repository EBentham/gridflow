---
phase: F0
status: complete
created: 2026-05-05
updated: 2026-05-05
---

# F0 Results - Bitemporal Upgrade for Fundamentals Datasets

## Implementation Summary

F0 added bitemporal-lite lineage to silver outputs at the base transformer layer.
Every successful `BaseSilverTransformer.run()` now stamps:

- `event_time`
- `available_at`
- `source_run_id`
- `dataset_version`

Forecast-style F0 datasets also preserve `issue_time` when Elexon payloads
include `publishDateTime` or `publishTime`.

## Files Changed

- `src/gridflow/silver/base.py`
- `src/gridflow/cli.py`
- `scripts/run_pipeline.py`
- `src/gridflow/silver/elexon/indo.py`
- `src/gridflow/silver/elexon/fuelhh.py`
- `src/gridflow/silver/elexon/demand_forecast.py`
- `src/gridflow/silver/elexon/wind_forecast.py`
- `src/gridflow/silver/openmeteo/historical.py`
- `tests/unit/test_bitemporal_columns.py`
- `tests/integration/test_bitemporal_run_id.py`

## Dataset Naming

The F0 source documents used `openmeteo/historical`. The repo's actual registered
source/dataset pair is:

- `open_meteo/historical`

The implementation and tests use the repo spelling.

## Reingest Status

The reingest path is implemented and tested against bronze sidecars. Local broad
historical re-transform was not run because this workspace currently has no
`data/bronze/` directory for the five F0 datasets. Existing local data contains
`data/silver/` and `data/gridflow.duckdb`, but no local bronze partitions to
re-transform.

When historical bronze is available, the intended commands remain:

```powershell
gridflow transform elexon indo --reingest --start 2022-01-01 --end 2026-05-04
gridflow transform elexon fuelhh --reingest --start 2022-01-01 --end 2026-05-04
gridflow transform elexon windfor --reingest --start 2022-01-01 --end 2026-05-04
gridflow transform elexon ndf --reingest --start 2022-01-01 --end 2026-05-04
gridflow transform open_meteo historical --reingest --start 2022-01-01 --end 2026-05-04
```

## Verification

Focused bitemporal and CLI lineage tests:

```powershell
python -m pytest tests/unit/test_bitemporal_columns.py tests/integration/test_bitemporal_run_id.py -q --tb=short
```

Result:

```text
9 passed, 1 warning
```

Focused existing transformer regression suite:

```powershell
python -m pytest tests/unit/test_bitemporal_columns.py tests/integration/test_bitemporal_run_id.py tests/unit/test_silver_transforms.py tests/unit/test_openmeteo.py -q --tb=short
```

Result:

```text
107 passed, 1 warning
```

Cross-source mocked E2E regression after static-dataset fallback:

```powershell
python -m pytest tests/unit/test_bitemporal_columns.py tests/integration/test_bitemporal_run_id.py tests/integration/test_elexon_mocked_e2e.py tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsog_mocked_e2e.py tests/integration/test_gie_agsi_mocked_e2e.py tests/integration/test_neso_mocked_e2e.py -q --tb=short
```

Result:

```text
299 passed, 1 warning
```

Full non-live suite:

```powershell
python -m pytest -q --tb=short
```

Result:

```text
1000 passed, 253 skipped, 1 warning
```

Ruff on the changed implementation and new tests:

```powershell
python -m ruff check src/gridflow/silver/base.py src/gridflow/silver/elexon/indo.py src/gridflow/silver/elexon/fuelhh.py src/gridflow/silver/elexon/demand_forecast.py src/gridflow/silver/elexon/wind_forecast.py src/gridflow/silver/openmeteo/historical.py tests/unit/test_bitemporal_columns.py tests/integration/test_bitemporal_run_id.py
```

Result:

```text
All checks passed!
```

Note: running Ruff over all of `src/gridflow/cli.py` and `scripts/run_pipeline.py`
still reports pre-existing style issues unrelated to F0.

## DuckDB Check

`tests/integration/test_bitemporal_run_id.py` refreshes the DuckDB catalogue after
a CLI transform and runs:

```sql
SELECT event_time, available_at, source_run_id, dataset_version
FROM silver_fuelhh
LIMIT 5;
```

The test asserts all four fields are queryable and populated.

## Deviation

The original plan expected event time to come from `timestamp_utc` or
`settlement_date`/`settlement_period`. The full suite exposed existing static
and reference datasets without either shape (`bmunits_reference`, reference
metadata, GIE summary/listing/news families, tariffs, and factors).

Resolution: `BaseSilverTransformer` now falls back to target-date midnight UTC
for static/reference outputs. This preserves the "every silver write has
event_time" contract without breaking existing non-time-series datasets.

## Handoff To gridflow_models

`gridflow_models` can treat these columns as the initial silver data contract:

- Features should filter `available_at <= as_of`.
- Targets should expose their own `available_at` as `target_available_at`.
- Forecast-vintage-aware datasets can use `issue_time` where available.
- Reference/static datasets may have target-date event times until a model needs
  finer domain-specific semantics.
