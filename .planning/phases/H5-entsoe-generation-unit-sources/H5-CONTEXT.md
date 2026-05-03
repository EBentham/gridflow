---
phase: H5
slug: entsoe-generation-unit-sources
status: ready_for_planning
created: 2026-05-03
---

# Phase H5 - Context

## Phase Boundary

H5 continues from H4 by promoting the catalog rows owned by
`H5-generation-units`. It adds the remaining generation-unit and
generation-reference ENTSO-E sources that need unit identity, reservoir, or
reference-data handling.

H5 consumes `docs/entsoe_endpoint_catalog.yaml` as the source of truth. It should
change only rows in the H5 batch unless implementation proves a row should be
deferred with a clear reason.

## Decisions

- Start by re-verifying the H4 bronze `data_date`/backfill regression from
  `.planning/phases/H4-entsoe-endpoint-catalog-request-builder/H4-UAT.md`.
- Keep aggregate generation datasets stable. Unit-level sources get their own
  schemas and transformers instead of overloading existing aggregate tables.
- Preserve ENTSO-E unit identifiers (`RegisteredResource.mRID`) and human names
  where present.
- Treat `generation_units_master_data` as a reference-data source. If its XML
  shape is not safely representable in the existing silver transformer pattern,
  implement metadata/request/fixture coverage and explicitly defer final silver
  modelling with a catalog reason.

## Canonical References

- `.planning/PROJECT.md`
- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/phases/H4-entsoe-endpoint-catalog-request-builder/H4-CONTEXT.md`
- `.planning/phases/H4-entsoe-endpoint-catalog-request-builder/H4-02-SUMMARY.md`
- `.planning/phases/H4-entsoe-endpoint-catalog-request-builder/H4-UAT.md`
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

- `installed_capacity_units` - A71/A33, `in_Domain`, optional `PsrType`, unit time-series parser.
- `actual_generation_units` - A73/A16, `in_Domain`, optional `PsrType` and `RegisteredResource`, unit time-series parser.
- `water_reservoirs` - A72/A16, `in_Domain`, quantity time-series parser.
- `generation_units_master_data` - A95/B11, `BiddingZone_Domain`, optional implementation date and `psrType`, master-data parser.

## Deferred Ideas

- Gold-layer modelling of generation units.
- Full slowly changing dimension design if master-data payloads require a larger reference-data phase.
