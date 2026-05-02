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

### Active

- [ ] Integration tests validate ENTSO-E URL construction without hitting live API (MOCK-01, MOCK-02, MOCK-03)
- [ ] Live test suite hits real ENTSO-E API and validates full bronze→silver chain (LIVE-01, LIVE-02, LIVE-03)

### Out of Scope

- Live tests running in CI — no ENTSO-E API key in CI environment
- E2E tests for non-ENTSO-E connectors — focus is ENTSO-E for this milestone
- Gold layer validation — no gold consumers of ENTSO-E data yet
- GAP-03b psrType semantic mapping — backlog, no gold consumers of wind_solar_forecast

## Context

- Medallion architecture: Bronze (raw Parquet) → Silver (normalised Parquet) → Gold (modelling-ready)
- DuckDB serves as the catalogue layer with views over silver Parquet files
- ENTSO-E connector uses XML parsing for all 16 datasets across 3 document types
- All transformers call `SchemaClass(**sample)` on first row as runtime contract check
- Polars `replace_strict` is used for A-code mapping — unknown codes raise at transform time
- Windows 11 / OneDrive path — use `os.replace()` for atomic file writes
- Previous E2E test gaps: integration tests use `respx` mocking with simplified fixtures;
  URL correctness and real API compatibility have never been validated end-to-end

## Constraints

- **Tech stack**: Polars only (no pandas), uv package manager, Python 3.11+
- **Test runner**: pytest -x -q with `respx` for HTTP mocking
- **Platform**: Windows 11 — `os.replace()` for atomic writes, forward-slash paths in tests
- **ENTSO-E API**: requires `ENTSOE_API_KEY` env var; live tests must be opt-in (`--live`)
- **Compatibility**: all 16 existing ENTSO-E transformers must remain passing after CLI changes

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| `replace_strict` for A-code mapping | Unknown codes surface immediately at transform time | ✓ Good |
| `output_cols / available_cols` pattern | New parser fields don't break non-target transformers | ✓ Good |
| Atomic writes via `os.replace()` | Windows doesn't allow rename-to-overwrite | ✓ Good |
| `SchemaClass(**sample)` runtime check on all transformers | Catches schema drift before it reaches DuckDB | ✓ Good |
| Live tests opt-in with `--live` marker | API key not available in CI; live tests are for developer validation | — Pending |

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
*Last updated: 2026-05-02 after Phase H1 completion*
