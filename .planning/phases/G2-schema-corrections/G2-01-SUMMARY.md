---
plan: G2-01
phase: G2-schema-corrections
status: complete
completed: 2026-04-30
commits:
  - 8b20bc5
  - 52b4e07
  - f9c8f70
key-files:
  created: []
  modified:
    - src/gridflow/schemas/entsoe.py
    - src/gridflow/connectors/entsoe/endpoints.py
    - src/gridflow/silver/entsoe/load_forecast.py
    - src/gridflow/silver/entsoe/load_forecast_weekly.py
    - src/gridflow/silver/entsoe/wind_solar_forecast.py
    - src/gridflow/silver/entsoe/installed_capacity.py
    - tests/unit/test_entsoe.py
---

## What Was Built

Applied 5 targeted schema corrections to align Phase 1/2 ENTSO-E silver layer with the spec.

### Changes Made

**GAP-01 — EntsoeLoadForecast:** Added `forecast_horizon: str = "day_ahead"` field to schema. LoadForecastTransformer now emits this literal column between `resolution` and `data_provider`.

**GAP-02 — EntsoeLoadForecastWeekly:** Added `forecast_horizon: str = "week_ahead"` field to schema. LoadForecastWeeklyTransformer emits this literal column.

**GAP-03a — EntsoeWindSolarForecast:** Renamed `forecast_mw` → `generation_forecast_mw` throughout schema and transformer (rename in `raw_df.rename()`, cast, and `output_cols`).

**GAP-05 — EntsoeInstalledCapacity:** Renamed `installed_capacity_mw` → `capacity_mw` throughout schema and transformer.

**GAP-08 — imbalance_volume DOC_TYPES:** Changed `process_type="A16"` → `None` in `endpoints.py`.

### Tests

Updated `tests/unit/test_entsoe.py`:
- All assertions updated for renamed fields
- Added `test_forecast_horizon_day_ahead` and `test_forecast_horizon_week_ahead`
- Added `test_generation_forecast_mw_column_name` and `test_capacity_mw_column_name`
- Schema instantiation tests updated for new field names
- `test_imbalance_volume_doc_type` now asserts `process_type is None`

**Result:** 184 tests pass, 0 failures.

## Self-Check: PASSED

- [x] `uv run pytest tests/unit/test_entsoe.py -x -q` — 184 passed
- [x] `generation_forecast_mw` present in `wind_solar_forecast.py`, `forecast_mw` absent
- [x] `capacity_mw` present in `installed_capacity.py`, `installed_capacity_mw` absent
- [x] `forecast_horizon` in both load forecast transformers
- [x] `DOC_TYPES['imbalance_volume'].process_type is None`

## Deviations

None. All 5 corrections applied exactly as specified in the plan.
