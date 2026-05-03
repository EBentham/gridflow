# gridflow — Roadmap

---

## Milestones

- ✅ **v0.2-entsoe-gaps** — Gap Closure G1–G4 (shipped 2026-05-02)
- 🔄 **v0.3-entsoe-validation** — ENTSO-E Pipeline Validation H1–H3 (in progress)

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

### 🔄 v0.3-entsoe-validation — ENTSO-E Pipeline Validation

- [x] **Phase H1**: Fix CLI `all` positional alias — treat `all` dataset arg as `--all` flag — Done 2026-05-02
  - Requirements: CLI-01, CLI-02
  - **Plans:** 1 plan
  - Plans:
    - [x] H1-01-PLAN.md — Create test file and apply single-condition fix to `_resolve_datasets`
  - Success: `gridflow pipeline entsoe all --last 24h` runs all 16 datasets without error
  - Success: Same alias works for `ingest` and `transform` subcommands
  - Success: Existing `--all` flag behaviour unchanged; H1 focused tests pass
  - Note: Full-suite gate is blocked by pre-existing missing Elexon silver imports, documented in H1-VERIFICATION.md

- [x] **Phase H2**: ENTSO-E mocked E2E tests — URL construction + bronze→silver pipeline — Done 2026-05-02
  - Requirements: MOCK-01, MOCK-02, MOCK-03
  - **Plans:** 1 plan
  - Plans:
    - [x] H2-01-PLAN.md - Add mocked ENTSO-E URL-shape and bronze-to-silver integration tests
  - Success: Test validates correct URL shape (base URL + params) for all 16 ENTSO-E datasets
  - Success: Pipeline test runs bronze→silver for representative datasets using realistic XML fixtures
  - Success: All 16 datasets have at least URL-construction coverage
  - Note: Full-suite gate is blocked by pre-existing missing Elexon silver imports, documented in H2-VERIFICATION.md

- [ ] **Phase H3**: Live ENTSO-E test suite (`@pytest.mark.live`)
  - Requirements: LIVE-01, LIVE-02, LIVE-03
  - **Plans:** 2 plans
  - Plans:
    - [x] H3-01-PLAN.md - Add live-test scaffolding and CLI failure propagation
    - [x] H3-02-PLAN.md - Add all-dataset live ENTSO-E E2E and command coverage
  - Success: `pytest -m live` fetches real data from ENTSO-E API for all 16 ENTSO-E datasets
  - Success: Fetched responses parse and transform to silver without error
  - Success: Tests auto-skip when `ENTSOE_API_KEY` is absent; skipped by default without `-m live`
  - Cross-cutting constraints: H3 remains ENTSO-E-only; live tests cover all 16 datasets; command failures hard-fail with diagnostics.
  - Status: Implementation complete; credentialed live verification pending because `ENTSOE_API_KEY` is absent in the execution environment.

- [x] **Phase H4**: ENTSO-E endpoint catalog + request builder correction - Done 2026-05-03
    - Requirements: URL-01, DOC-01, COVER-01, COVER-02, LIVE-04
    - **Plans:** 2 plans
    - Plans:
    - [x] H4-01-PLAN.md - Repair existing ENTSO-E request construction against documented parameter styles
    - [x] H4-02-PLAN.md - Build endpoint catalog workflow and promote the first missing source batch
  - Success: Existing ENTSO-E datasets use documented request parameter families, not one broad zone-style URL.
  - Success: Official endpoint inventory is represented in `docs/entsoe_endpoint_catalog.yaml` with every row classified.
  - Success: Load month/year forecasts and forecast margin added through metadata, parser, schema, transformer, fixtures, mocked E2E, and live request-shape gates.
  - Success: New ENTSO-E endpoints can be added in repeatable batches: metadata, parser, schema, transformer, fixture, mocked E2E, live shape gate.
  - Status: H4 complete; remaining planned endpoint families are cataloged for H5-H8 batches.

- [x] **Phase H5**: ENTSO-E generation unit and reference data sources - Done 2026-05-03
  - Requirements: SRC-GEN-01, SRC-GEN-02, SRC-GEN-03, SRC-GEN-04, COVER-03, LIVE-05
  - **Plans:** 1 plan
  - Plans:
    - [x] H5-01-PLAN.md - Add generation unit, reservoir, and generation-unit master-data sources
  - Success: `installed_capacity_units`, `actual_generation_units`, `water_reservoirs`, and `generation_units_master_data` are implemented through metadata, parser, schema, transformer, fixtures, mocked E2E, catalog validation, and live request-shape gates.
  - Success: Unit-level identifiers and names are preserved without weakening existing aggregate generation schemas.
  - Cross-cutting constraints: first re-verify the H4 bronze `data_date`/backfill partition regression; keep catalog status synchronized with DOC_TYPES.
  - Status: H5 complete; H4 bronze partition regression covered and H5 live request-shape probe passed.

- [x] **Phase H5.5**: ENTSO-E live cleanup before H6 - Done 2026-05-03
  - Requirements: LIVE-01, LIVE-02, LIVE-05, LIVE-CLEAN-01, LIVE-CLEAN-02, LIVE-CLEAN-03
  - **Plans:** 1 plan
  - Plans:
    - [x] H5.5-01-PLAN.md - Debug and repair live all-dataset ENTSO-E failures through H5
  - Success: `uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py -v -rs` passes or reports only expected ENTSO-E no-data skips.
  - Success: Incorrect endpoint metadata, zip XML payload handling, and live parser variants are fixed before H6 adds more sources.
  - Cross-cutting constraints: keep live tests opt-in, preserve token redaction, and distinguish genuine no-data acknowledgements from malformed request shapes.
  - Status: H5.5 complete; full live suite passes for active ENTSO-E datasets with expected no-data skips.

- [x] **Phase H6**: ENTSO-E transmission and market data sources - Done 2026-05-03
  - Requirements: SRC-TX-01, SRC-TX-02, SRC-TX-03, SRC-TX-04, COVER-03, LIVE-05
  - **Plans:** 1 plan
  - Plans:
    - [x] H6-01-PLAN.md - Add transmission, commercial schedule, capacity allocation, congestion, and market-position sources
  - Success: H6 catalog rows are implemented or explicitly reclassified with reasons.
  - Success: Zone-pair request styles support documented optional market filters without ad hoc connector branches.
  - Cross-cutting constraints: batch by parser family, keep large schema additions reviewable, and preserve existing 19 ENTSO-E datasets.
  - Status: H6 complete; 16 transmission/market datasets added, `flow_based_allocations` explicitly deferred for dedicated allocation-document schema review, and H6 live request-shape probes passed.

- [x] **Phase H7**: ENTSO-E outage extension data sources - Done 2026-05-03
  - Requirements: SRC-OUT-01, SRC-OUT-02, SRC-OUT-03, COVER-03, LIVE-05
  - **Plans:** 1 plan
  - Plans:
    - [x] H7-01-PLAN.md - Add consumption, transmission, offshore-grid, and production outage sources
  - Success: New outage datasets share the established outage-document parser path where possible and expose asset/status fields needed by silver schemas.
  - Success: Deferred outage variants remain documented with dependency reasons.
  - Cross-cutting constraints: do not regress the existing `outages_generation` unit-level schema.
  - Status: H7 complete; four primary outage datasets implemented, dependent outage variants remain deferred with updated reasons, and H7 live request-shape probes passed.

- [x] **Phase H8**: ENTSO-E balancing extension data sources - Done 2026-05-03
  - Requirements: SRC-BAL-01, SRC-BAL-02, SRC-BAL-03, SRC-BAL-04, COVER-03, LIVE-05
  - **Plans:** 1 plan
  - Plans:
    - [x] H8-01-PLAN.md - Add GL EB balancing state, bid, capacity, cross-zonal capacity, and financial balancing sources
  - Success: Near-term planned balancing-extension catalog rows are implemented through the medallion path.
  - Success: SO GL and implementation-framework extension rows remain deferred with explicit H9/backlog reasons.
  - Cross-cutting constraints: dedicated bid/capacity parser families are introduced only when the generic time-series parser cannot represent the payload safely.
  - Status: H8 complete; six balancing-extension datasets implemented, high-volume bid/capacity endpoints use default offset paging for live compatibility, and H8 live request-shape probes passed.

---

## Backlog

| Item | Source | Notes |
|------|--------|-------|
| GAP-03b: wind_solar_forecast psrType mapping (B16→solar, B18→wind_onshore, B19→wind_offshore) | v0.2 gap closure audit | Deferred — no gold consumers yet |
| Extend E2E coverage to Elexon, ENTSO-G, GIE connectors | v0.3 scope decision | ENTSO-E first; other connectors deferred |
