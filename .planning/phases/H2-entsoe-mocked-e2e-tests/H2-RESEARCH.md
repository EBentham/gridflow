---
phase: H2
slug: entsoe-mocked-e2e-tests
status: complete
researched: 2026-05-02
confidence: high
---

# Phase H2: ENTSO-E Mocked E2E Tests - Research

## Summary

H2 should add mocked ENTSO-E integration coverage in two layers:

1. URL construction coverage for all 16 ENTSO-E datasets registered in
   `gridflow.connectors.entsoe.endpoints.DOC_TYPES`.
2. Bronze-to-silver pipeline coverage for a representative subset of realistic XML
   fixtures, using `BronzeWriter` and real silver transformers.

The existing ENTSO-E connector is already shaped for this:

- `src/gridflow/connectors/entsoe/client.py` routes each dataset by `DOC_TYPES`.
- Zone datasets use `in_Domain.mRID` and `out_Domain.mRID`.
- Cross-border datasets use zone pairs.
- Balancing datasets use `controlArea_Domain.mRID`.
- `tests/integration/test_entsoe_connector.py` already uses `respx` and can be extended
  without live network calls.
- `tests/fixtures/entsoe/` contains one XML fixture per configured ENTSO-E dataset.

## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| MOCK-01 | Validate URL construction for each ENTSO-E dataset without live API | Add a parametrized integration test over `DOC_TYPES` and inspect `respx` call params. |
| MOCK-02 | Run full bronze-to-silver pipeline for representative datasets | Write XML fixtures through `BronzeWriter`, run real transformers, read generated Parquet. |
| MOCK-03 | URL-shape coverage spans all 16 datasets | Assert `set(DOC_TYPES) == set(load_settings().get_source_config("entsoe").datasets)` and test every dataset. |

## Key Findings

### URL Construction

`EntsoeConnector.fetch()` branches by dataset style:

- Zone style: `in_Domain.mRID`, `out_Domain.mRID`
- Zone pair style: `cross_border_flows`, `net_transfer_capacity`
- Control area style: `controlArea_Domain.mRID`

Every request should include:

- `documentType`
- `periodStart`
- `periodEnd`
- `securityToken` when `SourceConfig.api_key` is set
- `processType` only when `DOC_TYPES[dataset].process_type` is not `None`

### Bronze-To-Silver

Use `RawResponse(data_date=date(2024, 1, 15))` when writing fixtures through
`BronzeWriter`; otherwise bronze partitions by `fetched_at.date()` and transformers
looking at `target_date=date(2024, 1, 15)` will not find the file.

Representative datasets should cover the distinct path shapes:

- `day_ahead_prices`: price XML, zone-style request, price transformer
- `actual_load`: quantity XML, zone-style request, load transformer
- `cross_border_flows`: quantity XML, zone-pair request, flow transformer
- `imbalance_prices`: price XML, control-area request, balancing transformer

This subset exercises price parsing, quantity parsing, zone domains, zone-pair domains,
control-area domains, bronze writing, transformer reads, silver Parquet writes, and schema
validation for a Phase 3 balancing dataset.

### Windows Test Prerequisite

Current targeted ENTSO-E tests can fail on Windows with:

`ZoneInfoNotFoundError('No time zone found with key UTC')`

This appears when Polars converts timezone-aware UTC columns back to Python objects. Add
`tzdata>=2024.1` to project dependencies before adding H2 tests so Windows runners have
the IANA timezone database.

### Existing Full-Suite Blocker

The full test suite still has an unrelated collection blocker from H1:

`src/gridflow/silver/elexon/__init__.py` imports missing modules such as
`gridflow.silver.elexon.agpt`.

H2 should not fix Elexon package hygiene. H2 verification should run a targeted ENTSO-E
suite and attempt the full suite; if full-suite collection still fails only for that
known Elexon issue, record it as an existing blocker rather than an H2 failure.

## Validation Architecture

| Property | Value |
|----------|-------|
| Framework | pytest |
| HTTP mocking | respx |
| Quick command | `uv run --extra dev pytest tests/integration/test_entsoe_mocked_e2e.py -x -q` |
| Phase command | `uv run --extra dev pytest tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_connector.py tests/unit/test_entsoe.py -x -q` |
| Full command | `uv run --extra dev pytest -x -q` (expected to expose known Elexon import blocker until fixed elsewhere) |

## Risks

| Risk | Mitigation |
|------|------------|
| Fixture partition date mismatch makes transformers return 0 rows | Set `RawResponse.data_date` to the transform target date. |
| URL tests accidentally hit live ENTSO-E | Use `respx` for `https://web-api.tp.entsoe.eu/api` and a test token. |
| Test only validates one dataset | Parametrize over all `DOC_TYPES` and assert the configured source also has 16 datasets. |
| Windows timezone database missing | Add `tzdata` dependency before running transformer tests. |

## Open Questions

None. H2 scope is well-defined from the roadmap and current ENTSO-E code.
