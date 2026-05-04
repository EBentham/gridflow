# gridflow - Milestone History

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
