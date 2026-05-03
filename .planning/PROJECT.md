# gridflow

## What This Is

gridflow is a UK/EU energy market data pipeline that ingests raw data from APIs
(Elexon, ENTSO-E, ENTSO-G, GIE, NESO, Open-Meteo) into a Bronze layer, transforms
it into validated Silver Parquet files via a medallion architecture, and exposes
Gold-layer modelling-ready datasets. It targets energy analysts and data engineers
who need reliable, normalised time-series data from disparate European market sources.

## Core Value

Every connector reliably fetches real data and every silver transformer produces
schema-valid output — verified end-to-end, not just in unit tests.

## Requirements

### Validated

- ✓ Bronze ingestion layer via connector registry — v0.1
- ✓ Silver transformation layer via transformer registry — v0.1
- ✓ DuckDB catalogue with view registration — v0.1
- ✓ CLI commands: ingest, transform, pipeline, backfill, export-csv, reset, status — v0.1
- ✓ ENTSO-E connector: 16 datasets across 3 phases — v0.1
- ✓ Phase 3 bronze read path fixed across all 5 transformers — v0.2-entsoe-gaps G1
- ✓ ENTSO-E silver schema corrections (5 fields) — v0.2-entsoe-gaps G2
- ✓ ENTSO-E balancing A-codes mapped to semantic strings — v0.2-entsoe-gaps G3
- ✓ outages_generation redesigned as unit-level schema — v0.2-entsoe-gaps G4

- ✓ CLI treats positional `all` argument as `--all` flag — v0.3-entsoe-validation H1
- ✓ Mocked ENTSO-E URL and bronze-to-silver E2E tests — v0.3-entsoe-validation H2

- [x] ENTSO-E request builder uses documented endpoint-specific area parameter styles for the existing 16 datasets - v0.3-entsoe-validation H4
- [x] ENTSO-E endpoint catalog/gap matrix classifies every official Postman collection endpoint - v0.3-entsoe-validation H4
- [x] ENTSO-E load month/year forecasts and forecast margin added through the medallion path - v0.3-entsoe-validation H4
- [x] ENTSO-E generation unit, water reservoir, and generation-unit master data sources added through the medallion path - v0.3-entsoe-validation H5
- [x] ENTSO-E live cleanup verifies implemented H1-H5 sources against real API availability and payload formats - v0.3-entsoe-validation H5.5
- [x] ENTSO-E transmission, commercial schedule, allocation, congestion, and market-position sources added or explicitly reclassified - v0.3-entsoe-validation H6
- [x] ENTSO-E consumption, transmission, offshore-grid, and production outage sources added or explicitly reclassified - v0.3-entsoe-validation H7
- [x] ENTSO-E balancing state, bid, capacity, cross-zonal capacity, and financial balancing sources added or explicitly reclassified - v0.3-entsoe-validation H8
- [x] Live test suite hits real ENTSO-E API and validates active ENTSO-E bronze-to-silver chains, with explicit no-data skips - v0.3-entsoe-validation H5.5/H8

### Active

- [ ] Extend live and mocked E2E coverage to Elexon, ENTSO-G, and GIE connectors
- [ ] Decide whether to promote deferred ENTSO-E catalog rows, including B09 flow-based allocations and SO GL / implementation-framework balancing extensions

### Out of Scope

- Live tests running in CI — no ENTSO-E API key in CI environment
- E2E tests for non-ENTSO-E connectors — focus is ENTSO-E for this milestone
- Gold layer validation — no gold consumers of ENTSO-E data yet
- GAP-03b psrType semantic mapping — backlog, no gold consumers of wind_solar_forecast

## Context

- Medallion architecture: Bronze (raw Parquet) → Silver (normalised Parquet) → Gold (modelling-ready)
- DuckDB serves as the catalogue layer with views over silver Parquet files
- ENTSO-E connector uses XML parsing for 48 active datasets across documented ENTSO-E document families
- All transformers call `SchemaClass(**sample)` on first row as runtime contract check
- Polars `replace_strict` is used for A-code mapping — unknown codes raise at transform time
- Windows 11 / OneDrive path — use `os.replace()` for atomic file writes
- Previous E2E test gaps: integration tests use `respx` mocking with simplified fixtures;
  URL correctness and real API compatibility have never been validated end-to-end
- Mocked ENTSO-E E2E coverage now validates all implemented URL shapes and representative fixture-backed bronze-to-silver flows

- ENTSO-E URL construction is endpoint-metadata-driven; load, generation, outage, balancing, and zone-pair datasets use distinct documented query parameter families.
- `docs/entsoe_endpoint_catalog.yaml` is the auditable source for official Postman endpoint classification and follow-up implementation batches.
- H5 added `date_param` request metadata for A95 reference-data endpoints that use `Implementation_DateAndOrTime=YYYY-MM-DD` instead of period windows.
- H5.5 inserted a cleanup gate before H6 because the full credentialed ENTSO-E live suite exposed unsupported active A83 metadata for the default GB control area, zipped outage payloads, live parser tag variants, and genuine fixed-date no-data acknowledgements.
- H6 added `optional_params` request metadata plus shared zone-pair quantity/amount transformer families for transmission and market time-series datasets; B09 `flow_based_allocations` remains deferred for dedicated parser/schema review.
- H7 preserves outage document mRID/status and asset or unit identity metadata in the new outage silver datasets while keeping the existing `outages_generation` unit-level output stable.
- H8 preserves balancing area, bid identity, market product, direction, agreement, and cross-zonal domain metadata in the new balancing silver datasets.
- v0.3 shipped with ENTSO-E expanded from 16 original datasets to 48 active datasets, backed by endpoint catalog validation, mocked medallion-path E2E tests, and opt-in live request-shape probes.
- Four close-out artifacts were acknowledged as deferred at v0.3 completion: one debug session, two UAT records, and one H3 verification record.

## Constraints

- **Tech stack**: Polars only (no pandas), uv package manager, Python 3.11+
- **Test runner**: pytest -x -q with `respx` for HTTP mocking
- **Platform**: Windows 11 — `os.replace()` for atomic writes, forward-slash paths in tests
- **ENTSO-E API**: requires `ENTSOE_API_KEY` env var; live tests must be opt-in (`--live`)
- **Compatibility**: existing ENTSO-E transformers and active H1-H6 datasets must remain passing after source-batch changes

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| `replace_strict` for A-code mapping | Unknown codes surface immediately at transform time | ✓ Good |
| `output_cols / available_cols` pattern | New parser fields don't break non-target transformers | ✓ Good |
| Atomic writes via `os.replace()` | Windows doesn't allow rename-to-overwrite | ✓ Good |
| `SchemaClass(**sample)` runtime check on all transformers | Catches schema drift before it reaches DuckDB | ✓ Good |
| Mocked ENTSO-E E2E before live tests | Proves request construction and transformer paths without API credentials | ✓ Good |
| Endpoint catalog as coverage source | Prevents silent ENTSO-E omissions and gives each gap an owner batch | ✓ Good |
| H5-H8 split by source family | Keeps remaining ENTSO-E source coverage reviewable and lets parser/schema risk stay isolated by domain | ✓ Good |
| H5.5 live cleanup before H6 | New source batches should build on a live-tested H1-H5 baseline | Adopted |
| B09 flow-based allocations deferred in H6 | Allocation documents need dedicated parser/schema review rather than generic TimeSeries widening | Adopted |
| H7 dependent outage variants deferred | Transmission net-position impact, available capacity, and fallback documents need separate interpretation/schema passes beyond primary outage rows | Adopted |
| H8 high-volume bid/capacity endpoints default to `offset=0` | ENTSO-E rejects unpaged live calls when more than 100 instances are returned; offset keeps request-shape probes live-compatible while allowing caller override | Adopted |
| Live tests opt-in with `--live` marker | API key not available in CI; live tests are for developer validation | ✓ Good |
| Close v0.3 with acknowledged H3/live artifacts deferred | H5.5 and H8 live request-shape gates passed, but one H3 credentialed full-live verification record remains human-owned | Deferred |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd-transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd-complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-05-03 after v0.3-entsoe-validation milestone*
