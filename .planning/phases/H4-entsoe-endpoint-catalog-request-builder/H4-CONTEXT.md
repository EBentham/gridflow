---
phase: H4
slug: entsoe-endpoint-catalog-request-builder
status: ready_for_planning
created: 2026-05-03
---

# Phase H4 - Context

## Phase Boundary

H4 exists because live ENTSO-E request validation showed that "all 16 datasets are covered"
was not enough. The connector must construct the URL shape required by each ENTSO-E
endpoint, and the project needs a documented workflow for adding the many endpoint families
that are still absent from `DOC_TYPES`.

## Decisions

- Treat the official Postman collection at
  `https://documenter.getpostman.com/view/7009892/2s93JtP3F6` as the endpoint inventory
  source for H4.
- Treat ENTSO-E request parameter names as endpoint-specific. Do not collapse all area
  parameters into `in_Domain` / `out_Domain`.
- Keep response XML parsing separate from request query construction. Response tags may
  still contain `.mRID`; request query parameters should not unless the documented endpoint
  explicitly requires that key.
- Keep live tests opt-in, but use them for representative request-shape probes when the
  developer has `ENTSOE_API_KEY`.

## Canonical References

- `.planning/PROJECT.md` - project scope and constraints.
- `.planning/ROADMAP.md` - current milestone plan.
- `.planning/REQUIREMENTS.md` - v0.3 validation requirements and new H4 requirements.
- `.planning/debug/entsoe-connector-does-not-work.md` - prior invalid parameter diagnosis.
- `src/gridflow/connectors/entsoe/endpoints.py` - ENTSO-E dataset metadata registry.
- `src/gridflow/connectors/entsoe/client.py` - request construction and HTTP execution.
- `config/sources.yaml` - operator-facing source/dataset metadata.
- `tests/integration/test_entsoe_mocked_e2e.py` - mocked URL-shape gate.
- `tests/integration/test_entsoe_live.py` - opt-in live request-shape gate.

## Specific Findings From Docs Review

Existing connector mismatches found:

| Dataset | Previous shape | Documented shape |
|---------|----------------|------------------|
| `actual_load` | `in_Domain` + `out_Domain` | `outBiddingZone_Domain` |
| `load_forecast` | `in_Domain` + `out_Domain` | `outBiddingZone_Domain` |
| `load_forecast_weekly` | `in_Domain` + `out_Domain` | `outBiddingZone_Domain` |
| `actual_generation` | `in_Domain` + `out_Domain` | `in_Domain` |
| `wind_solar_forecast` | `in_Domain` + `out_Domain` | `in_Domain` |
| `installed_capacity` | `in_Domain` + `out_Domain` | `in_Domain` |
| `generation_forecast` | `in_Domain` + `out_Domain` | `in_Domain` |
| `cross_border_flows` | `documentType=A88` | `documentType=A11` |
| `net_transfer_capacity` | `processType=A01` | `contract_MarketAgreement.Type=A01` |
| `outages_generation` | `in_Domain` + `out_Domain` | `BiddingZone_Domain` + outage business type |
| `contracted_reserves` | no `processType` | `processType=A52` plus fixed business type |

Missing endpoint families to plan next:

- Load: month-ahead load forecast, year-ahead load forecast, forecast margin.
- Generation: installed capacity per unit, actual generation per unit, water reservoirs.
- Transmission and market: commercial schedules, offered transfer capacity variants,
  nominated/allocated capacity, DC links, redispatching, countertrading, congestion costs,
  flow-based allocations, non-EU allocations.
- Outages: load, production, transmission, offshore infrastructure, fallbacks.
- Balancing: bids, procured capacity, cross-zonal balancing capacity, financial expenses,
  SO GL and implementation-framework endpoints.
- Master/metadata: production and generation units, other market information.

## Deferred Ideas

- Full silver schema design for every missing endpoint is deferred to H4-02 execution.
- Gold-layer modelling datasets remain out of scope until source coverage is reliable.

