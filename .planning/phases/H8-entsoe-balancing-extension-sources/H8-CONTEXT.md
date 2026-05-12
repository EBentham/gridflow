---
phase: H8
slug: entsoe-balancing-extension-sources
status: ready_for_planning
created: 2026-05-03
---

# Phase H8 - Context

## Phase Boundary

H8 promotes the near-term `H8-balancing-extensions` rows from the endpoint
catalog. It adds GL EB balancing state, bid, capacity, cross-zonal capacity, and
financial balancing datasets while keeping broader SO GL and
implementation-framework rows deferred.

## Decisions

- Keep existing balancing datasets stable: `contracted_reserves`,
  `activated_balancing_prices`, `imbalance_prices`, `imbalance_volume`, and
  `activated_balancing_qty`.
- Do not force bid/capacity payloads into generic quantity rows if the XML
  carries bid identity, product, direction, or market-agreement semantics that
  need dedicated columns.
- Archive variants and SO GL extensions remain deferred unless a current
  modelling requirement promotes them.

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
- `tests/unit/test_entsoe.py`
- `tests/unit/test_entsoe_endpoint_catalog.py`
- `tests/integration/test_entsoe_mocked_e2e.py`
- `tests/integration/test_entsoe_live.py`

## Planned Catalog Rows

- `current_balancing_state`
- `balancing_energy_bids`
- `aggregated_balancing_energy_bids`
- `procured_balancing_capacity`
- `cross_zonal_balancing_capacity`
- `balancing_financial_expenses_income`

## Deferred Ideas

- Balancing bid archive variants.
- SO GL capacity and sharing endpoints.
- Implementation-framework balancing extensions.
