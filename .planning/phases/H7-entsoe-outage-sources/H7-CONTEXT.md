---
phase: H7
slug: entsoe-outage-sources
status: ready_for_planning
created: 2026-05-03
---

# Phase H7 - Context

## Phase Boundary

H7 promotes the planned outage rows from the endpoint catalog. It extends the
existing `outages_generation` work to consumption, transmission, offshore-grid,
and production-unit outage datasets.

H7 should reuse the outage-document parsing model where possible, but it must not
flatten asset-level outage metadata into generic quantity rows if that would lose
document status, resource, or affected-domain meaning.

## Decisions

- Preserve the existing `outages_generation` unit-level schema and tests.
- Implement primary outage datasets first; keep dependent variants such as net
  position impact, available capacity, and fallbacks deferred until the primary
  schemas are proven.
- Treat `DocStatus`, `mRID`, update windows, registered resource, and asset
  resource filters as metadata/request fields, not hard-coded connector logic.

## Canonical References

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/phases/G4-outages-unit-schema/G4-VALIDATION.md`
- `.planning/phases/H4-entsoe-endpoint-catalog-request-builder/H4-02-SUMMARY.md`
- `docs/entsoe_endpoint_catalog.yaml`
- `src/gridflow/connectors/entsoe/endpoints.py`
- `src/gridflow/connectors/entsoe/client.py`
- `src/gridflow/connectors/entsoe/parsers.py`
- `src/gridflow/schemas/entsoe.py`
- `src/gridflow/silver/entsoe/outages_generation.py`
- `tests/integration/test_entsoe_mocked_e2e.py`
- `tests/integration/test_entsoe_live.py`

## Planned Catalog Rows

- `outages_consumption` - A76/A53, `BiddingZone_Domain`, quantity time-series.
- `outages_transmission` - A78/A53, `In_Domain`/`Out_Domain`, outage document.
- `outages_offshore_grid` - A79, `BiddingZone_Domain`, outage document.
- `outages_production` - A77/A53, `BiddingZone_Domain`, outage document.

## Deferred Ideas

- Transmission net-position impact and available-capacity variants.
- Outage fallbacks.
