---
phase: H5
plan: 01
status: complete
completed: 2026-05-03
---

# H5-01 Summary - Generation Unit and Reference Data Sources

## What Changed

- Added H5 ENTSO-E endpoint metadata and config for:
  - `installed_capacity_units`
  - `actual_generation_units`
  - `water_reservoirs`
  - `generation_units_master_data`
- Extended `EntsoeDocType` with `date_param` so reference-data endpoints can use
  `Implementation_DateAndOrTime=YYYY-MM-DD` instead of period-window params.
- Updated A95 master-data request construction to use `BusinessType=B11` and the
  documented implementation-date parameter shape verified against the live API.
- Added a namespace-agnostic generation-unit master-data parser.
- Added silver schemas and transformers for all four H5 datasets.
- Added XML fixtures for all H5 datasets and fixture-backed bronze-to-silver
  coverage.
- Added regression coverage for the H4 bronze partition issue:
  - connector responses set `RawResponse.data_date` from `periodStart`
  - bronze writes partition by `data_date`, not `fetched_at`
- Updated `docs/entsoe_endpoint_catalog.yaml` so H5 catalog rows are implemented
  and aligned with active `DOC_TYPES`.

## Verification

- `uv run --extra dev ruff check src/gridflow/connectors/entsoe src/gridflow/schemas/entsoe.py src/gridflow/silver/entsoe tests/unit/test_entsoe.py tests/unit/test_entsoe_endpoint_catalog.py tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_live.py` - passed.
- `uv run --extra dev pytest tests/unit/test_entsoe.py tests/unit/test_entsoe_endpoint_catalog.py tests/integration/test_entsoe_connector.py tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_live.py -m "not live" -x -q` - 288 passed, 58 deselected, 1 warning.
- `uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py::TestEntsoeLiveAllDatasets::test_live_request_shape_uses_supported_domain_params -q -rs` - 8 passed.
- Targeted live A95 parser sanity check - first GB response parsed 230 generation-unit master-data records.

## Notes

- The first live A95 probe failed with the initial period-window request shape.
  Targeted probing showed the live API accepts `Implementation_DateAndOrTime` as
  `YYYY-MM-DD`; the implementation now uses that metadata-driven request style.
- The real A95 response uses `TimeSeries` records with `registeredResource.*`
  fields, so the parser handles both the fixture's compact unit structure and
  the live document shape.
- Full live fetch and bronze-to-silver verification remains opt-in and dependent
  on ENTSO-E data availability for each endpoint/date.
