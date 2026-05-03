---
phase: H4
plan: 01
status: complete
completed: 2026-05-03
---

# H4-01 Summary - Existing ENTSO-E URL Construction Repaired

## What Changed

- Added endpoint-specific request metadata to `EntsoeDocType`.
- Updated the connector request builder to emit the documented domain parameter family
  for each existing dataset.
- Corrected current dataset metadata including:
  - `cross_border_flows`: `A88` -> `A11`
  - `net_transfer_capacity`: `processType=A01` -> `contract_MarketAgreement.Type=A01`
  - `actual_load`, `load_forecast`, `load_forecast_weekly`: `outBiddingZone_Domain`
  - generation datasets: `in_Domain`
  - `outages_generation`: `BiddingZone_Domain`
  - selected balancing fixed params and `contracted_reserves` process type.
- Updated mocked, unit, and live request-shape tests.

## Verification

- `uv run --extra dev ruff check src/gridflow/connectors/entsoe/client.py src/gridflow/connectors/entsoe/endpoints.py tests/integration/test_entsoe_connector.py tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_live.py tests/unit/test_entsoe.py` - passed.
- `uv run --extra dev pytest tests/integration/test_entsoe_connector.py tests/integration/test_entsoe_mocked_e2e.py tests/unit/test_entsoe.py -x -q` - 227 passed.
- `uv run --extra dev pytest tests/integration/test_entsoe_live.py -m "not live" -x -q` - 6 passed, 42 deselected.
- `uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py::TestEntsoeLiveAllDatasets::test_live_request_shape_uses_supported_domain_params -q -rs` - 6 passed.

## Follow-Up

Run H4-02 to convert the Postman endpoint collection into a full implementation backlog and
add missing data source batches.

