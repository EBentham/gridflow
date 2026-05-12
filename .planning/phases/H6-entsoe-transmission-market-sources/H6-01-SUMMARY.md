---
phase: H6
plan: 01
status: complete
completed: 2026-05-03
---

# H6-01 Summary - Transmission and Market Data Sources

## What Changed

- Added H6 ENTSO-E endpoint metadata and config for 16 transmission and market
  source datasets:
  - `dc_link_intraday_transfer_limits`
  - `commercial_schedules`
  - `commercial_schedules_net_positions`
  - `redispatching_cross_border`
  - `redispatching_internal`
  - `countertrading`
  - `congestion_management_costs`
  - `offered_transfer_capacity_continuous`
  - `offered_transfer_capacity_implicit`
  - `offered_transfer_capacity_explicit`
  - `auction_revenue`
  - `transfer_capacity_use`
  - `total_nominated_capacity`
  - `total_capacity_allocated`
  - `congestion_income`
  - `net_positions`
- Extended `EntsoeDocType` with `optional_params` so H6 auction, contract,
  update-date, and mixed-case request filters are forwarded from metadata
  without connector branches.
- Added metadata defaults for live market endpoints whose Postman filters are
  listed as optional but are mandatory in practice, including contract market
  agreement and auction filters.
- Set `congestion_management_costs` to the same-zone in/out request style after
  live backfill showed ENTSO-E rejects cross-border EIC pairs for A92.
- Set `net_positions` to the same-zone in/out request style after live backfill
  showed ENTSO-E rejects cross-border EIC pairs for A25/B09.
- Extended domain parsing to handle mixed-case `In_Domain.mRID` and
  `Out_Domain.mRID` payload tags.
- Added shared H6 silver transformer families for zone-pair quantity time
  series and monetary time series, keeping `quantity_mw` separate from
  `amount_eur`.
- Added H6 schemas, XML fixtures, URL-shape coverage, fixture-backed
  bronze-to-silver coverage, catalog validation, and live request-shape probes.
- Updated `docs/entsoe_endpoint_catalog.yaml` so implemented H6 rows align with
  active `DOC_TYPES`.

## Verification

- `uv run --extra dev ruff check src/gridflow/connectors/entsoe src/gridflow/schemas/entsoe.py src/gridflow/silver/entsoe tests/unit/test_entsoe.py tests/unit/test_entsoe_endpoint_catalog.py tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_live.py` - passed.
- `uv run --extra dev pytest tests/unit/test_entsoe.py tests/unit/test_entsoe_endpoint_catalog.py tests/integration/test_entsoe_connector.py tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_live.py -m "not live" -x -q` - 332 passed, 91 deselected, 1 warning.
- `uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py::TestEntsoeLiveAllDatasets::test_live_request_shape_uses_supported_domain_params -q -rs` - 11 passed.

## Decisions Made

- Deferred `flow_based_allocations` instead of forcing it through the generic
  time-series path because the B09 allocation-document shape needs a dedicated
  parser/schema review.
- Kept H6 request construction metadata-driven by adding optional-filter support
  and per-dataset defaults rather than special-case connector methods.
- Used shared H6 quantity and monetary transformer families because the payload
  shapes align, while preserving separate output value semantics.

## Deviations from Plan

- `flow_based_allocations` was explicitly reclassified to deferred during Task 3
  after fixture-shape review found no safe generic schema path.
- Live request-shape probing showed some catalog "optional" filters are
  mandatory for market endpoints, so metadata defaults were added before
  verification.
- End-to-end backfill UAT showed A92 congestion-management costs require
  matching `in_Domain` and `out_Domain` values, so the dataset now uses the
  existing same-zone request style.
- The same UAT showed A25/B09 net positions require matching `in_Domain` and
  `out_Domain` values, so `net_positions` uses the same-zone request style too.

## Commit

- `b1a1b4b` - `feat(H6-01): add ENTSO-E transmission market sources`

## Issues Encountered

- `gsd-sdk` was not available on PATH, so GSD state updates were applied
  manually from the planning artifacts.

## User Setup Required

None - no external service configuration required beyond the existing optional
`ENTSOE_API_KEY` for live tests.

## Next Phase Readiness

H6 is ready for H7. Outage extension planning can rely on active ENTSO-E
metadata/config/test alignment and the same catalog validation gate.
