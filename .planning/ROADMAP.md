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

- [ ] **Phase H2**: ENTSO-E mocked E2E tests — URL construction + bronze→silver pipeline
  - Requirements: MOCK-01, MOCK-02, MOCK-03
  - Success: Test validates correct URL shape (base URL + params) for all 16 ENTSO-E datasets
  - Success: Pipeline test runs bronze→silver for representative datasets using realistic XML fixtures
  - Success: All 16 datasets have at least URL-construction coverage

- [ ] **Phase H3**: Live ENTSO-E test suite (`@pytest.mark.live`)
  - Requirements: LIVE-01, LIVE-02, LIVE-03
  - Success: `pytest -m live` fetches real data from ENTSO-E API for a representative subset
  - Success: Fetched responses parse and transform to silver without error
  - Success: Tests auto-skip when `ENTSOE_API_KEY` is absent; skipped by default without `-m live`

---

## Backlog

| Item | Source | Notes |
|------|--------|-------|
| GAP-03b: wind_solar_forecast psrType mapping (B16→solar, B18→wind_onshore, B19→wind_offshore) | v0.2 gap closure audit | Deferred — no gold consumers yet |
| Extend E2E coverage to Elexon, ENTSO-G, GIE connectors | v0.3 scope decision | ENTSO-E first; other connectors deferred |
