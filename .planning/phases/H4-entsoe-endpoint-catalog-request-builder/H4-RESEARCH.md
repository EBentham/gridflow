---
phase: H4
slug: entsoe-endpoint-catalog-request-builder
status: complete
created: 2026-05-03
---

# Phase H4 - Research

## Sources Reviewed

- ENTSO-E Transparency Platform Restful API Postman collection:
  `https://documenter.getpostman.com/view/7009892/2s93JtP3F6`
- Postman collection JSON endpoint exposed by the documenter page:
  `https://documenter.gw.postman.com/api/collections/7009892/2s93JtP3F6`
- ENTSO-E Transparency Platform Web API guide:
  `https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide_prod_backup_06_11_2024.html`
- ENTSO-E Transparency Platform DocumentType reference:
  `https://transparencyplatform.zendesk.com/hc/en-us/articles/15857043092756-DocumentType`

## Research Goal

Determine why the existing ENTSO-E connector can pass mocked tests while failing against
live endpoints, and define a durable workflow for reaching full endpoint coverage.

## Key Finding

The connector needs endpoint-specific request metadata. ENTSO-E uses multiple area
parameter families:

- `in_Domain` / `out_Domain`
- `outBiddingZone_Domain`
- `BiddingZone_Domain`
- `controlArea_Domain`
- `area_Domain`
- `Connecting_Domain` / `Acquiring_Domain`
- `registeredResource`

The old request builder only distinguished zone, zone-pair, and control-area calls. That
was insufficient for load, generation, outage, capacity, and several balancing endpoints.

## Current Coverage After H4-01

The 16 existing datasets now have a request-construction contract that can express:

- same-zone market prices;
- out-bidding-zone load datasets;
- in-domain generation datasets;
- cross-border zone-pair datasets;
- bidding-zone outage datasets;
- control-area balancing datasets;
- fixed endpoint query parameters such as `businessType` and
  `contract_MarketAgreement.Type`.

## Remaining Coverage Gaps

The Postman collection includes many endpoint families not present in `DOC_TYPES`.

| Domain | Missing examples |
|--------|------------------|
| Load | Month/year load forecasts, forecast margin |
| Generation | Generation unit data, installed capacity per unit, water reservoirs |
| Transmission | Commercial schedules, DC link capacity, redispatching, countertrading, congestion costs |
| Market | Offered transfer capacity, auction revenue, allocated/nominated capacity, flow-based allocations |
| Outages | Transmission, offshore, production-unit, consumption-unit, fallback data |
| Balancing | Energy bids, capacity procurement, cross-zonal balancing capacity, financial expenses, SO GL/IF datasets |
| Master data | Production/generation units, other market information |

## Implementation Implications

- Add a canonical endpoint catalog in code or config rather than scattering query rules in
  conditional branches.
- Store request parameter keys exactly as documented. Normalize only internal names.
- Add mocked request-shape tests that assert the exact domain parameter set per dataset.
- Add live request-shape probes by parameter style, not merely by document type.
- Introduce missing endpoints in domain batches so each batch can include parser, schema,
  silver transformer, mocked fixture, and live request-shape coverage.

## Validation Architecture

1. Static catalog test: configured ENTSO-E datasets and `DOC_TYPES` match on dataset name,
   document type, and process type.
2. Mocked URL-shape test: every dataset asserts exact area parameter family, fixed params,
   dates, and `securityToken`.
3. Representative live shape test: at least one dataset from each request style must reach
   the live API without `Input parameter does not exist`.
4. Domain-batch E2E tests: each new endpoint batch gets fixture-backed bronze-to-silver
   coverage before live validation.
5. Full live gate: opt-in all-dataset fetch and bronze-to-silver tests with redacted
   diagnostics.

## RESEARCH COMPLETE

