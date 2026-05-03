---
phase: H4
plan: 02
status: complete
completed: 2026-05-03
---

# H4-02 Summary - Endpoint Catalog Workflow and First Missing Source Batch

## What Changed

- Added `docs/entsoe_endpoint_catalog.yaml` as the auditable ENTSO-E Postman
  endpoint catalog and gap matrix.
- Classified official collection entries as implemented, planned, deferred, or
  excluded, with owner batches for remaining implementation work.
- Added catalog validation tests that keep implemented catalog rows aligned with
  active `DOC_TYPES` metadata.
- Promoted the first missing source batch:
  - `load_forecast_monthly` (`A65` / `A32`)
  - `load_forecast_yearly` (`A65` / `A33`)
  - `forecast_margin` (`A70` / `A33`)
- Added metadata in `DOC_TYPES` and `config/sources.yaml`, fixture XML, silver
  transformers, schema validation, exports, mocked E2E coverage, and unit tests
  for the promoted datasets.
- Generalized ENTSO-E request metadata with explicit `domain_params` support for
  future endpoint families that need custom area parameter names.

## Verification

- `uv run --extra dev pytest tests/unit/test_entsoe.py tests/unit/test_entsoe_endpoint_catalog.py tests/integration/test_entsoe_connector.py tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_live.py -m "not live" -x -q` - 260 passed, 48 deselected, 1 warning.
- `uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py::TestEntsoeLiveAllDatasets::test_live_request_shape_uses_supported_domain_params -q -rs` - 6 passed.
- `uv run --extra dev ruff check src/gridflow/connectors/entsoe src/gridflow/schemas/entsoe.py src/gridflow/silver/entsoe tests/unit/test_entsoe.py tests/unit/test_entsoe_endpoint_catalog.py tests/integration/test_entsoe_connector.py tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_live.py` - passed.
- Full-project `uv run --extra dev ruff check src tests` remains blocked by
  pre-existing lint backlog outside H4.

## Follow-Up

- Implement the cataloged H5-H8 endpoint batches: generation units and reservoirs,
  transmission market/capacity variants, outage extensions, and balancing
  extensions.
- Run full live all-dataset fetch and bronze-to-silver verification when
  `ENTSOE_API_KEY` is available.
