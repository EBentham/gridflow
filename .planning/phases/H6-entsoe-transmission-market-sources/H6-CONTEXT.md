---
phase: H6
slug: entsoe-transmission-market-sources
status: ready_for_planning
created: 2026-05-03
---

# Phase H6 - Context

## Phase Boundary

H6 promotes the `H6-transmission-market` rows from
`docs/entsoe_endpoint_catalog.yaml`. It covers transmission, commercial
schedule, allocation, congestion-management, capacity, and market-position
datasets.

This is the largest remaining ENTSO-E source batch. Keep the implementation
reviewable by grouping work by parser family and by introducing request-builder
primitives before adding dataset rows that need them.

## Decisions

- Implement only rows currently marked `planned` for `H6-transmission-market`.
  Deferred rows remain deferred unless a fixture or live probe proves they are
  cheap and needed.
- Keep zone-pair request construction table-driven. Optional auction, contract,
  update-date, and offset filters should live in endpoint metadata or a small
  documented request-filter primitive.
- Split price-like datasets from quantity-like datasets at schema/transformer
  boundaries.

## Canonical References

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/phases/H4-entsoe-endpoint-catalog-request-builder/H4-02-SUMMARY.md`
- `docs/entsoe_endpoint_catalog.yaml`
- `src/gridflow/connectors/entsoe/endpoints.py`
- `src/gridflow/connectors/entsoe/client.py`
- `src/gridflow/connectors/entsoe/parsers.py`
- `src/gridflow/schemas/entsoe.py`
- `src/gridflow/silver/entsoe/`
- `config/sources.yaml`
- `tests/unit/test_entsoe_endpoint_catalog.py`
- `tests/integration/test_entsoe_mocked_e2e.py`
- `tests/integration/test_entsoe_live.py`

## Planned Catalog Rows

- Transmission: `dc_link_intraday_transfer_limits`,
  `commercial_schedules`, `commercial_schedules_net_positions`,
  `redispatching_cross_border`, `redispatching_internal`, `countertrading`,
  `congestion_management_costs`.
- Market/capacity: `offered_transfer_capacity_continuous`,
  `offered_transfer_capacity_implicit`, `offered_transfer_capacity_explicit`,
  `flow_based_allocations`, `auction_revenue`, `transfer_capacity_use`,
  `total_nominated_capacity`, `total_capacity_allocated`,
  `congestion_income`, `net_positions`.

## Deferred Ideas

- `third_country_transfer_capacity`, archive variants, and transmission project
  lifecycle documents unless project scope changes.
