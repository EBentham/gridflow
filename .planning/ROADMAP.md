# gridflow — Roadmap

---

## Milestones

- ✅ **v0.2-entsoe-gaps** — Gap Closure G1–G4 (shipped 2026-05-02)
- ✅ **v0.3-entsoe-validation** — ENTSO-E Pipeline Validation H1–H8 (shipped 2026-05-03)
- 🔄 **v0.4-elexon-validation** — Elexon Pipeline Validation I1–I4 (planning)

---

## Phases

<details>
<summary>✅ v0.2-entsoe-gaps — ENTSO-E Extension Gap Closure (G1–G4) — SHIPPED 2026-05-02</summary>

- [x] Phase G1: Fix Phase 3 bronze read path + connector test — ✅ Done
- [x] Phase G2: Minor schema corrections (GAP-01/02/03a/05/08) — ✅ Done 2026-04-30
- [x] Phase G3: Map balancing codes to semantic values (GAP-06/07) — ✅ Done 2026-05-02
- [x] Phase G4: outages_generation unit-level schema (GAP-04) — ✅ Done 2026-05-02

See full details: [milestones/v0.2-entsoe-gaps-ROADMAP.md](milestones/v0.2-entsoe-gaps-ROADMAP.md)

</details>

---

<details>
<summary>✅ v0.3-entsoe-validation — ENTSO-E Pipeline Validation (H1-H8) — SHIPPED 2026-05-03</summary>

- [x] Phase H1: Fix CLI `all` positional alias — completed 2026-05-02
- [x] Phase H2: ENTSO-E mocked E2E tests — completed 2026-05-02
- [x] Phase H3: Live ENTSO-E test suite scaffolding and credential-gated all-dataset coverage — implementation complete; credentialed close-out deferred
- [x] Phase H4: ENTSO-E endpoint catalog + request builder correction — completed 2026-05-03
- [x] Phase H5: ENTSO-E generation unit and reference data sources — completed 2026-05-03
- [x] Phase H5.5: ENTSO-E live cleanup before H6 — completed 2026-05-03
- [x] Phase H6: ENTSO-E transmission and market data sources — completed 2026-05-03
- [x] Phase H7: ENTSO-E outage extension data sources — completed 2026-05-03
- [x] Phase H8: ENTSO-E balancing extension data sources — completed 2026-05-03

See full details: [milestones/v0.3-entsoe-validation-ROADMAP.md](milestones/v0.3-entsoe-validation-ROADMAP.md)

Known deferred close-out items: 4; see [STATE.md](STATE.md).

</details>

---

### 🔄 v0.4-elexon-validation — Elexon Pipeline Validation

- [ ] **Phase I1**: Elexon inventory, test scaffolding, and request-style baseline
  - Requirements: ELEXON-INV-01, ELEXON-INV-02, ELEXON-INV-03
  - **Plans:** 1 plan
  - Plans:
    - [ ] `.planning/phases/I1-elexon-inventory-test-scaffolding/I1-01-PLAN.md` - Elexon inventory contract and live-test scaffolding
  - Success: Configured Elexon datasets, `ENDPOINTS`, and silver registry alignment is tested.
  - Success: Decommissioned/duplicate/empty endpoints are documented separately from active datasets.
  - Success: Elexon live-test scaffolding uses opt-in `@pytest.mark.live`, temp config/data helpers, and source/dataset/stage diagnostics.

- [ ] **Phase I2**: Elexon mocked request-shape and fixture-backed bronze-to-silver tests
  - Requirements: ELEXON-MOCK-01, ELEXON-MOCK-02, ELEXON-MOCK-03
  - **Plans:** 1 plan
  - Success: Mocked tests validate request URL/parameter shape for every active configured Elexon dataset without network access.
  - Success: Realistic Elexon JSON fixtures write to bronze and run representative silver transformers across the main data families.
  - Success: Tests assert bronze metadata, `data_date` partitioning, pagination/chunk behavior, and expected silver columns.

- [ ] **Phase I3**: Elexon live API to silver test suite
  - Requirements: ELEXON-LIVE-01, ELEXON-LIVE-02, ELEXON-LIVE-03, ELEXON-LIVE-04, ELEXON-LIVE-05
  - **Plans:** 1 plan
  - Success: Opt-in live tests call the public Elexon Insights API for active configured datasets with narrow deterministic windows.
  - Success: Representative live responses are written through `BronzeWriter`, transformed to silver parquet, and checked for rows, columns, data provider, and schema validity.
  - Success: Empty/no-data responses and known removed endpoints are classified explicitly as skip/deferred/documented outcomes.
  - Success: Live tests require no API key and remain excluded from normal test runs.

- [ ] **Phase I4**: Elexon CLI/backfill live smoke tests and milestone close-out docs
  - Requirements: ELEXON-CLI-01, ELEXON-CLI-02, ELEXON-CLI-03, ELEXON-DOC-01, ELEXON-DOC-02
  - **Plans:** 1 plan
  - Success: Live `pipeline`, `ingest`, `transform`, and `backfill` smoke tests run against isolated temp config/data paths.
  - Success: CLI tests verify bronze and silver outputs for a safe curated dataset subset and fail non-zero on real dataset errors.
  - Success: Phase artifacts document live commands, chosen dataset windows, expected skips, and troubleshooting notes.
  - Success: Requirements traceability remains 100% mapped.

---

## Backlog

| Item | Source | Notes |
|------|--------|-------|
| GAP-03b: wind_solar_forecast psrType mapping (B16→solar, B18→wind_onshore, B19→wind_offshore) | v0.2 gap closure audit | Deferred — no gold consumers yet |
| Extend E2E coverage to Elexon, ENTSO-G, GIE connectors | v0.3 scope decision | ENTSO-E first; other connectors deferred |
