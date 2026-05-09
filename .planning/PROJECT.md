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

## Current State

**Shipped:** v0.11-open-meteo-renewable-extension F7.5 on 2026-05-09.

gridflow now carries 6 Open-Meteo dataset families (demand, wind, solar × archive/forecast) with role-specific location lists (25 sites), hub-height wind (10m + 100m, archive-verified), full irradiance components (GHI/DNI/DHI/GTI), cloud-cover decomposition, snow, and derived air density. All active endpoints across 6 vendors are live-validated; V1 production bugs fixed by V2. Silver is bitemporal-ready for `gridflow_models` consumption.

**Current focus:** Wind/solar silver schema is production-ready. Next decisions:
1. Run wind/solar bronze backfill 2018-2025 (commands in F7.5-RESULTS.md) — requires live API confirmation.
2. Begin `gridflow_models` F1 planning — can now use the new 25-site silver for wind/solar features.
3. Vault sync for open-meteo when `obsidian-vault` MCP is available (closes F7.5-VAULT-01).

## Last Milestone: v0.11 Open-Meteo Renewable Extension

**Goal:** Extend the Open-Meteo connector and silver layers for production-grade UK wind and solar forecasting.

**Status:** Completed 2026-05-09.

**Delivered features:**
- Role-split connector: 6 dataset-specific `WeatherDatasetSpec` entries replace monolithic location/variable pattern.
- Three Pydantic schemas: `DemandWeather`, `WindWeather`, `SolarWeather` (replaces `WeatherObservation`).
- Hub-height wind: ERA5 archive limited to 10m + 100m (live-verified 2026-05-09); forecast carries wider set.
- Derived air density: `ρ = P / (287.05 × T_K)` for demand + wind; property-tested over 18 (T, P) combinations.
- GTI solar via `extra_params=(("tilt","35"),("azimuth","180"))` on spec.
- `DATASET_VERSION` 1.0.0 → 2.0.0 across all 6 transformers.
- 74 new tests; 1116 total passing (net +74 over pre-F7.5 baseline).
- ADR-020 for location approximation decision.

## Previous Milestone: v0.8 Fundamentals Model Silver Foundations

**Goal:** Add bitemporal-lite lineage to gridflow silver outputs.

**Status:** Completed 2026-05-05.

**Target features:**
- Research and document official GIE AGSI endpoint families and query scopes.
- Build AGSI endpoint/source metadata for aggregate, country, company, and
  facility query scopes.
- Fix bronze exact gas-day/range requests, pagination by `last_page`, provenance,
  and rate-limit-aware expected-count planning.
- Preserve storage, listing, news, and unavailability payloads through silver
  where active.
- Add inventory, mocked request-shape, count-completeness, fixture-backed
  bronze-to-silver, opt-in live API-to-silver, and CLI smoke tests.

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
- [x] Elexon active dataset inventory matches source config, endpoint definitions, and silver transformer registrations - v0.4-elexon-validation I1
- [x] Elexon intentionally excluded endpoints are documented separately from active datasets - v0.4-elexon-validation I1
- [x] Elexon endpoint parameter styles are covered by registry-driven tests - v0.4-elexon-validation I1
- [x] Elexon mocked and fixture-backed tests cover representative transformer families and bronze-to-silver flows - v0.4-elexon-validation I2
- [x] Elexon live E2E tests ping the public Insights API and prove real responses flow into silver parquet - v0.4-elexon-validation I3
- [x] Elexon CLI and backfill live smoke tests run through isolated temp paths and verify bronze/silver outputs - v0.4-elexon-validation I4
- [x] ENTSOG endpoint catalog and inventory contract cover configured datasets, endpoint definitions, and silver transformer registrations - v0.5-entsog-pipeline-validation J1
- [x] ENTSOG bronze connector uses endpoint metadata for operational, CMP, interruption, aggregated, tariff, UMM, and reference endpoints - v0.5-entsog-pipeline-validation J2
- [x] ENTSOG physical-flow and generic silver transformers write deterministic silver parquet for active endpoint families - v0.5-entsog-pipeline-validation J3
- [x] ENTSOG mocked request-shape, fixture-backed bronze-to-silver, opt-in live API-to-silver, and isolated CLI smoke tests are in place - v0.5-entsog-pipeline-validation J4
- [x] ENTSOG generic transformer handles live duplicate snake_case column collisions such as `isCAMRelevant`/`isCamRelevant` - v0.5-entsog-pipeline-validation J4 close-out
- [x] NESO endpoint catalog covers all documented Carbon Intensity API routes - v0.6-neso-carbon-intensity-platform K1
- [x] NESO connector fetches every registered dataset via path-template metadata - v0.6-neso-carbon-intensity-platform K2
- [x] NESO silver transformers preserve all national, stats, factors, generation, and regional payload data - v0.6-neso-carbon-intensity-platform K3
- [x] NESO mocked and live E2E tests prove API responses flow through bronze into silver - v0.6-neso-carbon-intensity-platform K4
- [x] GIE AGSI endpoint catalog/source metadata/query planning cover storage, EIC listing, news, unavailability, and listing-derived expected counts - v0.7-gie-agsi-gas-storage-validation L1
- [x] GIE AGSI bronze ingestion uses exact-date/range query plans, `last_page` pagination, request provenance, and expected-count completeness checks - v0.7-gie-agsi-gas-storage-validation L2
- [x] GIE AGSI silver transformers preserve storage, listing, news, and unavailability payload data where active - v0.7-gie-agsi-gas-storage-validation L3
- [x] GIE AGSI mocked E2E tests prove fixture API responses flow through bronze into silver - v0.7-gie-agsi-gas-storage-validation L3
- [x] GIE AGSI live E2E tests prove representative real API responses flow through bronze into silver - v0.7-gie-agsi-gas-storage-validation L4
- [x] GIE AGSI CLI smoke tests prove pipeline, ingest/transform, and backfill run under isolated output paths - v0.7-gie-agsi-gas-storage-validation L4
- [x] Every successful silver transformer run writes `event_time`, `available_at`, `source_run_id`, and `dataset_version` - v0.8-fundamentals-model-silver-foundations F0
- [x] CLI and script transform paths pass `PipelineRunTracker.run_id` into silver transformer runs - v0.8-fundamentals-model-silver-foundations F0
- [x] Re-ingest mode reconstructs historical `available_at` from bronze sidecar metadata when available - v0.8-fundamentals-model-silver-foundations F0
- [x] `elexon/ndf` and `elexon/windfor` preserve `issue_time` from publish metadata where present - v0.8-fundamentals-model-silver-foundations F0
- [x] DuckDB views, focused tests, and `F0-RESULTS.md` prove the handoff contract for `gridflow_models` - v0.8-fundamentals-model-silver-foundations F0
- [x] Open-Meteo role-split: 6 datasets (demand/wind/solar × archive/forecast) with role-specific location lists (25 sites), hub-height wind (10m+100m archive-verified), irradiance components, cloud-cover decomposition, snow, derived air density - v0.11-open-meteo-renewable-extension F7.5
- [x] Hard rename `historical`→`historical_demand`, `forecast`→`forecast_demand`; `DATASET_VERSION` 1.0.0→2.0.0; ADR-020 for location approximation — v0.11 F7.5
- [x] All active V1 production bugs fixed (Elexon freq, NESO regional, REMIT/SOSO cap, system_prices run_type, ENTSOE A09 dedup, ENTSOG 404 short-circuit) — v0.10 V2
- [x] All 156 active gridflow endpoints live-validated; vault documentation populated — v0.9 V1

### Active

- [ ] Run Open-Meteo wind/solar bronze backfill 2018-2025 — ~52K location-days; commands in F7.5-RESULTS.md. Requires explicit live API confirmation.
- [ ] Run F0 `--reingest` commands when local historical bronze exists.
- [ ] Decide: start `gridflow_models` F1 next or prioritise backfill first.
- [ ] Vault sync for open-meteo when `obsidian-vault` MCP is available (closes F7.5-VAULT-01).

### Out of Scope

- Live tests running in CI — no ENTSO-E API key in CI environment
- GIE ALSI LNG validation — backlog, AGSI gas storage validated in v0.7
- Gold layer validation — no gold consumers of ENTSO-E data yet
- GAP-03b psrType semantic mapping — backlog, no gold consumers of wind_solar_forecast
- Creating the `gridflow_models` repository — v0.8/v0.11 prepare silver data first
- Append-only revision storage — deferred until stack/imbalance datasets need revision history
- Open-Meteo `minutely_15` (Workstream C) — gated on AROME 2026 boundary verification
- Open-Meteo `/v1/ensemble` — deferred until F7.5 silver produces residuals downstream
- Scheduled live smoke monitoring — cross-source follow-up, not yet scoped

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
- Elexon Insights APIs are public and currently require no API key, but live tests still need to remain opt-in because they hit production network services.
- Elexon connector parameter styles include path-date endpoints, publish/from-to datetime endpoints, settlementDate+settlementPeriod endpoints, and no-param reference endpoints.
- `docs/entsoe_endpoint_catalog.yaml` is the auditable source for official Postman endpoint classification and follow-up implementation batches.
- H5 added `date_param` request metadata for A95 reference-data endpoints that use `Implementation_DateAndOrTime=YYYY-MM-DD` instead of period windows.
- H5.5 inserted a cleanup gate before H6 because the full credentialed ENTSO-E live suite exposed unsupported active A83 metadata for the default GB control area, zipped outage payloads, live parser tag variants, and genuine fixed-date no-data acknowledgements.
- H6 added `optional_params` request metadata plus shared zone-pair quantity/amount transformer families for transmission and market time-series datasets; B09 `flow_based_allocations` remains deferred for dedicated parser/schema review.
- H7 preserves outage document mRID/status and asset or unit identity metadata in the new outage silver datasets while keeping the existing `outages_generation` unit-level output stable.
- H8 preserves balancing area, bid identity, market product, direction, agreement, and cross-zonal domain metadata in the new balancing silver datasets.
- v0.3 shipped with ENTSO-E expanded from 16 original datasets to 48 active datasets, backed by endpoint catalog validation, mocked medallion-path E2E tests, and opt-in live request-shape probes.
- Four close-out artifacts were acknowledged as deferred at v0.3 completion: one debug session, two UAT records, and one H3 verification record.
- v0.4 shipped Elexon validation from inventory through mocked request-shape tests, fixture-backed bronze-to-silver checks, opt-in public API-to-silver tests, and isolated live CLI/backfill smoke tests.
- v0.5 implements ENTSOG endpoint metadata across 33 active datasets, including operational indicators, CMP/event, aggregated, tariff, UMM, and reference endpoint families.
- ENTSOG live validation treats `404 No result found` and empty arrays as explicit no-data skips for narrow smoke windows, while successful responses must write bronze and silver parquet under temporary roots.
- ENTSOG `cmp_auction_premiums` live payloads can include both `isCAMRelevant` and `isCamRelevant`; the generic transformer coalesces same-normalized-name columns into one snake_case output column.
- v0.6 implements 33 NESO Carbon Intensity route variants with endpoint catalog, path-template connector metadata, family-aware silver transforms, mocked all-dataset E2E tests, opt-in live API-to-silver checks, and CLI smoke coverage.
- GIE AGSI API requires an `x-key` header and publishes daily gas storage data at `https://agsi.gie.eu`.
- GIE AGSI `/api/about?show=listing` returns the company/facility EIC inventory needed to derive expected company and facility request counts.
- GIE AGSI storage responses use `last_page` for pagination; `total` is the number of rows on the current page, not the global total.
- GIE AGSI exact-day and range requests must be count-checked so a query for 2026-05-01 writes all expected rows/pages for that day and no out-of-window gas days.
- GIE API documentation v007 supersedes the user-supplied v006 for planning purposes; v007 applies API v2 to ALSI and adds filtering detail, but v0.7 implementation scope remains AGSI gas storage.
- GIE AGSI storage bronze fetching is now query-plan driven for aggregate, country, company, and facility scopes; company/facility params include listing-derived EIC context.
- GIE AGSI bronze provenance records `page`, `total_pages`, request params, and `data_date`, with pagination driven by `last_page` rather than `total`.
- GIE AGSI live tests now prove representative aggregate, country, company, and facility storage reports can flow from the live API through bronze into silver parquet.
- GIE AGSI CLI smoke tests now prove `pipeline`, separate `ingest`/`transform`, and `backfill` run under isolated `GRIDFLOW_*` output paths.
- The `gridflow_models` architecture v3.1 requires bitemporal silver data so `TrainingSet` can reject feature rows where `available_at > as_of` and target rows where `target_available_at > as_of`.
- The first modelling foundation scope uses existing gridflow datasets only: `elexon/indo`, `elexon/fuelhh`, `elexon/windfor`, `elexon/ndf`, and `open_meteo/historical`.
- The supplied F0 spec used `openmeteo/historical`; the repo's actual source key is `open_meteo`.
- `elexon/ndf` and `elexon/windfor` are forecast-style datasets, so `issue_time` needs an explicit F0 decision even though the supplied F0 plan mostly describes four base bitemporal columns.

## Constraints

- **Tech stack**: Polars only (no pandas), uv package manager, Python 3.11+
- **Test runner**: pytest -x -q with `respx` for HTTP mocking
- **Platform**: Windows 11 — `os.replace()` for atomic writes, forward-slash paths in tests
- **ENTSO-E API**: requires `ENTSOE_API_KEY` env var; live tests must be opt-in (`--live`)
- **Elexon API**: public Insights API; no key required, but live tests must be opt-in and use narrow request windows/rate limits
- **ENTSO-G API**: public JSON API; no key required, but live tests must be opt-in and use narrow windows, `pointDirection`, exact-case indicators, and small `limit` overrides
- **GIE AGSI API**: requires `GIE_API_KEY` via `x-key` header; live tests must be opt-in, use exact gas-day windows, and respect the documented 60 calls/minute limit
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
| Elexon v0.4 mirrors ENTSO-E validation shape | The project needs connector-agnostic confidence that live API data reaches silver, but Elexon has JSON/public/no-key semantics and distinct parameter styles | Good |
| Elexon inventory tests compare real registries | Avoid duplicating the full active dataset list in tests while still catching config, endpoint, and silver registration drift | Adopted |
| Elexon excluded endpoints live in `EXCLUDED_ENDPOINTS` | Keeps removed, duplicate, or unstable endpoints visible without treating them as active datasets | Adopted |
| `GRIDFLOW_*` environment overrides beat YAML paths | Live CLI smoke tests and manual checks must be able to isolate data, DuckDB, and logs from normal project directories | Adopted |
| Elexon live tests use representative no-key public responses | Keeps live coverage fast while proving RawResponse, BronzeWriter, transformer, and parquet paths | Good |
| Elexon CLI smoke tests use curated datasets | Covers path-date, publish/from-to, and reference-data command paths without broad live API blast radius | Good |
| ENTSOG v0.5 mirrors ENTSO-E/Elexon validation shape | The next connector-confidence gap is ENTSOG, but gas TP has public JSON, mandatory point-direction filters, exact-case indicators, and noisy no-data responses | Adopted |
| ENTSOG endpoint registry is the source of truth | Operational indicators and non-operational endpoint paths differ enough that source config, connector requests, and tests must derive from shared metadata | Adopted |
| ENTSOG generic silver transformer tolerates placeholder dates | Live ENTSOG payloads include empty strings, `N/A`, and human-formatted timestamps; placeholders should become nulls instead of blocking endpoint coverage | Adopted |
| ENTSOG generic silver transformer coalesces column-name collisions | Live payloads can vary field casing within one endpoint; same-normalized-name columns should become one canonical snake_case column | Adopted |
| GIE AGSI v0.7 focuses on gas storage only | The user requested AGSI gas storage; ALSI LNG has the same documentation family but is a separate datasource confidence problem | Adopted |
| GIE AGSI pagination must use `last_page` | Live API behavior and documentation make `total` a per-page row count, so using it as global total silently truncates pages | Adopted |
| AGSI entity coverage derives from `/api/about?show=listing` | The listing endpoint is the only reliable way to know company/facility EIC query inventory and expected request counts | Adopted |
| Full AGSI live inventory tests are explicit and slow | GIE documents 60 calls/minute; representative live tests should stay fast, while full inventory gates can run deliberately when requested | Adopted |
| Keep AGSI `storage` as a compatibility alias in L2 | Existing silver registration still targets `gie_agsi/storage`; L3 can decide whether to migrate silver to `storage_reports` | Adopted |
| Register AGSI `storage_reports` while preserving `storage` | L3 needs catalog-aligned silver output without breaking the legacy transformer alias | Adopted |
| Transform active AGSI listing/news/unavailability families in L3 | Compact fixture coverage is enough to preserve documented payload fields without catalog-deferring active families | Adopted |
| Use exclusive end boundary for AGSI backfill smoke | The current backfill loop runs while `current < end`, so a one-day smoke uses 2026-05-01 to 2026-05-02 | Adopted |
| Capture `available_at` in the silver writer | `pipeline_runs.completed_at` is recorded after `transformer.run()` returns, so the row timestamp must be generated before atomic writes | Adopted |
| Pass `PipelineRunTracker.run_id` into transformer runs | `gridflow_models` needs row-level lineage back to a specific transform run | Adopted |
| Use `open_meteo/historical` in F0 | This is the actual registered source/dataset pair in `config/sources.yaml` and the Open-Meteo transformer | Adopted |
| Keep F0 dependency-neutral unless agreed otherwise | The supplied F0 spec asks for Hypothesis while also saying no new dependencies; pytest-based property-style checks fit the current stack | Adopted |
| Preserve forecast `issue_time` for F0 forecast datasets | Architecture v3.1 requires forecast vintages, and F0 includes `elexon/ndf` and `elexon/windfor` | Adopted |
| Hard rename openmeteo datasets (no aliases) | Clean break per CLAUDE.md no-compat-shim rule; all callers in one local repo | ✓ Good |
| WeatherDatasetSpec frozen dataclass as single source of truth | Eliminates drift between connector, transformer, config, and tests for 6 dataset variants | ✓ Good |
| Archive wind heights limited to 10m + 100m | ERA5 archive confirmed via live probe: 80/120/180m return all-null; live validation before building prevents silent null-fill silver | ✓ Good |
| Wave isolation abandoned; atomic commit for refactor + tests | Tests imported symbols being deleted; wave 1 alone couldn't stay green — pre-exec advisor review caught this | ✓ Good |
| Air density band `[0.95, 1.55]` not `[0.95, 1.40]` | Formula verification at joint extreme (T=-30°C, P=1050 hPa) → 1.504 kg/m³ before writing property tests | ✓ Good |
| DERIVE_* class variables for optional silver derivations | More declarative than string comparisons in base class; each subclass self-documents what it derives | Adopted |
| Double-underscore separator in bronze compound keys | Avoids ambiguous parsing when dataset prefix is multi-word; contract test enforces no `__` in location names | Adopted |
| Location centroids over per-turbine NRO coordinates (ADR-020) | Per-turbine NRO coordinates not freely available; centroid precision is acceptable for boosted feature inputs | Adopted |

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
*Last updated: 2026-05-09 after completing v0.11 Open-Meteo Renewable Extension*
