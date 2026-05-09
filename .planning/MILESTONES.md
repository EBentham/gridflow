# gridflow - Milestone History

---

## v0.11-open-meteo-renewable-extension - Open-Meteo Renewable Extension

**Completed:** 2026-05-09
**Phases:** F7.5 (1 phase, 1 plan, 4 commits)
**Test suite:** 1116 passed, 251 deselected (net +74 over pre-F7.5 baseline)

### Delivered

Extended the Open-Meteo connector and silver layer into three role-specific dataset families (demand, wind, solar) with role-specific location lists (7 + 12 + 6 = 25 sites) and variable sets. Silver now carries hub-height wind (10m + 100m, ERA5 archive-verified), full irradiance components (GHI/DNI/DHI/GTI), cloud-cover decomposition, snow variables, and derived air density, all at `DATASET_VERSION = "2.0.0"`.

### Key Accomplishments

1. **Role-split connector:** `WeatherDatasetSpec` dataclass + `DATASET_SPECS` lookup table replaces the single `LOCATIONS`/`HOURLY_VARIABLES` pattern with 6 dataset-specific specs.
2. **Hub-height verification:** Live ERA5 archive probe confirmed only 10m + 100m are reliably served; archive variable list restricted accordingly. Wind shear (100m/10m ratio) is preserved as an implicit signal.
3. **Three schema families:** `DemandWeather`, `WindWeather`, `SolarWeather` replace monolithic `WeatherObservation`, each with appropriate field sets.
4. **Air density derivation:** `ρ = P / (287.05 × T_K)` implemented with property tests over 18 (T, P) combinations; band `[0.95, 1.55]` widened from original `[0.95, 1.40]` after plan-time formula verification.
5. **Migration sweep:** Hard rename `historical` → `historical_demand` / `forecast` → `forecast_demand` + code review caught two stale string-literal references (`serving/client.py` and `scripts/run_all_sources.py`).
6. **74 new tests:** location-list, variable-list, schema, dataset-spec contract, air-density property, and irradiance-component invariant tests.

### Known Deferred Items at Close

- **F7.5-VAULT-01**: Obsidian vault sync (`quant-vault/30-vendors/open-meteo/`) — requires `obsidian-vault` MCP server session. In-repo docs are the version-controlled substitute.
- Open-Meteo `minutely_15` forecast resolution (Workstream C) — gated on AROME 2026 boundary verification.
- Wind/solar bronze backfill 2018-2025 (~52K location-days) — commands documented in F7.5-RESULTS.md.

### Archive

- [v0.11-open-meteo-renewable-extension-ROADMAP.md](milestones/v0.11-open-meteo-renewable-extension-ROADMAP.md)
- [v0.11-open-meteo-renewable-extension-REQUIREMENTS.md](milestones/v0.11-open-meteo-renewable-extension-REQUIREMENTS.md)

---

## v0.6-neso-carbon-intensity-platform - NESO Carbon Intensity Platform

**Completed:** 2026-05-04
**Phases:** K1-K4 (4 phases, inline implementation)
**Test suite:** NESO mocked and live E2E suites added; exact-day and settlement-period iteration semantics verified

### Delivered

Expanded NESO Carbon Intensity from a single national route to all documented
national intensity, statistics, factors, generation, and regional route
families with endpoint catalog, path-template request construction,
family-aware silver transforms, mocked all-dataset E2E tests, opt-in live
API-to-silver tests, and CLI smoke coverage.

### Key Accomplishments

1. **K1 - Research and catalog:** All documented Carbon Intensity routes are represented in the endpoint catalog and source config.
2. **K2 - Connector path templates:** NESO connector requests are built from endpoint metadata and preserve path values in bronze provenance.
3. **K3 - Silver transforms:** National, stats, factors, generation, and regional payload families write deterministic silver parquet.
4. **K4 - Live and CLI confidence:** Live API responses and user-facing CLI paths create isolated bronze and silver outputs.
5. **Close-out fix:** Same-day range windows and `intensity_period` settlement-period iteration now fetch complete target dates instead of empty or partial windows.

### Known Deferred Items

- Scheduled live endpoint monitoring remains a future cross-source decision.
- GIE AGSI gas storage validation is the next connector-confidence milestone.

### Archive

- [v0.6-neso-carbon-intensity-platform-ROADMAP.md](milestones/v0.6-neso-carbon-intensity-platform-ROADMAP.md)
- [v0.6-neso-carbon-intensity-platform-REQUIREMENTS.md](milestones/v0.6-neso-carbon-intensity-platform-REQUIREMENTS.md)

---

## v0.5-entsog-pipeline-validation - ENTSOG Pipeline Validation

**Shipped:** 2026-05-04
**Phases:** J1-J4 (4 phases, inline implementation)
**Commits:** 1 archive close-out commit planned
**Test suite:** 857 non-live tests passed; 26 ENTSOG live API-to-silver tests passed with 7 expected no-data skips; 1 live ENTSOG CLI smoke test passed

### Delivered

Built ENTSOG validation from endpoint research through live CLI confidence:
33 active ENTSOG datasets, metadata-driven bronze requests, specialised and
generic silver transforms, mocked request-shape tests, fixture-backed
bronze-to-silver checks, opt-in live API-to-silver validation, and a real
backfill regression fix for CMP auction premiums.

### Key Accomplishments

1. **J1 - Research and inventory:** ENTSOG TP API endpoint research, endpoint catalog, source config, endpoint registry, and silver transformer registration now form one auditable contract.
2. **J2 - Bronze request builder:** ENTSOG fetches now use endpoint metadata with exact-case `/operationalData`, `timeZone=UCT`, exact-case indicators, and mandatory `pointDirection` defaults.
3. **J3 - Silver transforms:** Physical flows retain GWh/day normalisation while generic ENTSOG transformers cover operational, CMP/event, tariff/UMM, aggregated, and reference datasets.
4. **J4 - Live and CLI confidence:** Opt-in live tests prove public ENTSOG responses can flow through bronze into silver, while live no-data outcomes are explicitly classified.
5. **Close-out regression:** The generic transformer now coalesces duplicate snake_case column collisions such as `isCAMRelevant`/`isCamRelevant`, and the real `cmp_auction_premiums` backfill succeeds.

### Known Deferred Items

- Domain-specific ENTSOG silver schemas remain deferred until downstream gas gold consumers need typed models beyond generic normalised records.
- Scheduled live endpoint monitoring remains a future cross-source decision.
- GIE AGSI/ALSI validation remains the next connector-confidence candidate.
- No v0.5 milestone audit file exists because `gsd-sdk` is unavailable in this runtime; close-out proceeded from passing regression, live, CLI, and targeted backfill verification.

### Archive

- [v0.5-entsog-pipeline-validation-ROADMAP.md](milestones/v0.5-entsog-pipeline-validation-ROADMAP.md)
- [v0.5-entsog-pipeline-validation-REQUIREMENTS.md](milestones/v0.5-entsog-pipeline-validation-REQUIREMENTS.md)

---

## v0.4-elexon-validation - Elexon Pipeline Validation

**Shipped:** 2026-05-04
**Phases:** I1-I4 (4 phases, 4 plans)
**Commits:** 10 | **Files:** 39 changed | **Lines:** +3767 / -161
**Test suite:** 81 non-live regression tests passed; 5 live CLI smoke tests passed; 5 live API-to-silver tests passed in I3

### Delivered

Built Elexon validation from inventory through live CLI smoke coverage: active
dataset alignment, mocked request-shape tests, fixture-backed bronze-to-silver
tests, opt-in live API-to-silver tests, and isolated user-facing CLI/backfill
smoke tests.

### Key Accomplishments

1. **I1 - Inventory and scaffolding:** Active Elexon config, endpoint registry, and silver transformer registration are tested with explicit excluded endpoint documentation.
2. **I2 - Mocked and fixture E2E:** Every active configured Elexon dataset has mocked request-shape coverage, while representative fixtures prove bronze-to-silver transformer paths.
3. **I3 - Live API to silver:** Public no-key Elexon responses for `system_prices`, `boal`, `freq`, `pn`, and `bmunits_reference` flow through temp bronze into silver parquet.
4. **I4 - CLI and backfill smoke:** `pipeline`, separate `ingest`/`transform`, and `backfill` run live against temp-root `GRIDFLOW_*` paths for curated Elexon datasets.
5. **Config isolation fix:** Runtime `GRIDFLOW_DATA_DIR`, `GRIDFLOW_DUCKDB_PATH`, and `GRIDFLOW_LOG_DIR` overrides now beat YAML defaults.

### Known Deferred Items

No new v0.4 deferred items. Existing v0.3 deferred close-out artifacts remain recorded in `.planning/STATE.md`.

### Archive

- [v0.4-elexon-validation-ROADMAP.md](milestones/v0.4-elexon-validation-ROADMAP.md)
- [v0.4-elexon-validation-REQUIREMENTS.md](milestones/v0.4-elexon-validation-REQUIREMENTS.md)

---

## v0.3-entsoe-validation — ENTSO-E Pipeline Validation

**Shipped:** 2026-05-03
**Phases:** H1-H8 (9 phases, 11 plans)
**Commits:** 19 | **Files:** 137 changed | **Lines:** +17341 / -257
**Test suite:** 378 passed, 122 deselected (H8 non-live gate); 97 passed, 44 skipped (final amended focused tests)

### Delivered

Expanded and validated the ENTSO-E pipeline from the original 16 datasets to 48 active datasets, with endpoint-specific request construction, an auditable official endpoint catalog, medallion-path mocked coverage, opt-in live request-shape gates, and new generation, transmission, outage, and balancing source families.

### Key Accomplishments

1. **H1-H3 — CLI and E2E validation foundation:** Positional `all` now behaves as `--all`; mocked bronze-to-silver coverage and credential-gated live scaffolding cover the ENTSO-E dataset fleet.
2. **H4 — Endpoint catalog and request builder:** ENTSO-E requests now use documented area/date/zone-pair parameter families, and `docs/entsoe_endpoint_catalog.yaml` tracks implementation, deferral, and scope decisions.
3. **H5-H5.5 — Generation/reference data and live cleanup:** Added unit-level generation, reservoir, and master-data sources, then repaired live payload handling for zip XML, tag variants, no-data acknowledgements, and active-source request shapes.
4. **H6-H8 — Source family expansion:** Added transmission/market, outage, and balancing extension datasets with parser, schema, transformer, fixture, mocked E2E, catalog, and live request-shape coverage.
5. **Catalog-backed deferrals:** Flow-based allocations, dependent outage variants, balancing archive rows, SO GL, and implementation-framework extensions remain explicit follow-up items rather than silent gaps.

### Known Deferred Items

Four close-out artifacts were acknowledged and deferred at milestone close; see `.planning/STATE.md` Deferred Items.

### Archive

- [v0.3-entsoe-validation-ROADMAP.md](milestones/v0.3-entsoe-validation-ROADMAP.md)
- [v0.3-entsoe-validation-REQUIREMENTS.md](milestones/v0.3-entsoe-validation-REQUIREMENTS.md)

---

## v0.2-entsoe-gaps — ENTSO-E Extension Gap Closure

**Shipped:** 2026-05-02
**Phases:** G1–G4 (4 phases, 5 plans)
**Commits:** 21 | **Files:** 43 changed | **Lines:** +5213 / −67
**Test suite:** 551 passed, 44 deselected (live)

### Delivered

Closed all 10 gap-IDs from the ENTSO-E connector extension audit, transforming a
partially-correct ENTSO-E silver layer into a fully spec-compliant one.

### Key Accomplishments

1. **G1 — Bronze path + connector tests:** Fixed doubled `read_bronze()` path in all 5 Phase 3 transformers; added 4-test integration suite for `_fetch_control_area` parameter correctness
2. **G2 — Schema corrections:** Applied 5 targeted fixes — `forecast_horizon` literal fields for load forecasts, `generation_forecast_mw` and `capacity_mw` renames, `process_type=None` for imbalance_volume
3. **G3 — Balancing code mapping:** Eliminated all raw ENTSO-E A-codes from Phase 3 silver; direction and reserve_type now emit semantic strings ("long"/"short", "fcr"/"afrr"/"mfrr"/"rr", "up"/"down"); `price_eur_mwh` correctly named; `ingested_at` added to all 5 datasets
4. **G4 — Unit-level outages:** Parser extended to extract `RegisteredResource.mRID`/`name` from A80 XML (backward-compat); `outages_generation` silver redesigned to unit-level schema with `outage_type` A-code mapping

### Deferred (Backlog)

- GAP-03b: wind_solar_forecast psrType semantic mapping (B16→solar, B18→wind_onshore, B19→wind_offshore)

### Archive

- [v0.2-entsoe-gaps-ROADMAP.md](milestones/v0.2-entsoe-gaps-ROADMAP.md)
- [v0.2-entsoe-gaps-MILESTONE-AUDIT.md](milestones/v0.2-entsoe-gaps-MILESTONE-AUDIT.md)

---
