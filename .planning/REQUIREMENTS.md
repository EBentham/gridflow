# Requirements: gridflow v0.3

**Defined:** 2026-05-02
**Core Value:** Every connector reliably fetches real data and every silver transformer produces schema-valid output — verified end-to-end, not just in unit tests.

## v0.3 Requirements

### CLI

- [x] **CLI-01**: User can run `gridflow pipeline entsoe all --last 24h` and it processes all ENTSO-E datasets (positional `all` treated as `--all`) — Phase H1
- [x] **CLI-02**: Same `all` positional alias works for `gridflow ingest` and `gridflow transform` subcommands — Phase H1

### Testing — Mocked

- [ ] **MOCK-01**: Integration test validates URL construction for each ENTSO-E dataset (correct base URL, required query parameters present) without hitting the live API
- [ ] **MOCK-02**: Integration test runs the full bronze→silver pipeline for a representative set of ENTSO-E datasets using realistic XML fixture responses
- [ ] **MOCK-03**: URL-shape test coverage spans all 16 ENTSO-E registered datasets

### Testing — Live

- [ ] **LIVE-01**: Live test suite (`@pytest.mark.live`) fetches real data from the ENTSO-E API for a representative subset of datasets when `ENTSOE_API_KEY` is set
- [ ] **LIVE-02**: Live tests verify that fetched XML responses parse and transform to silver without errors
- [ ] **LIVE-03**: Live tests are skipped by default and can be opted in with `pytest -m live`; a conftest fixture gate skips automatically when no API key is present

## Future Requirements

### Testing — Broader Coverage

- **BROAD-01**: Live and mocked E2E coverage extended to Elexon, ENTSO-G, and GIE connectors
- **BROAD-02**: GAP-03b: wind_solar_forecast psrType semantic mapping (B16→solar, B18→wind_onshore, B19→wind_offshore)

## Out of Scope

| Feature | Reason |
|---------|--------|
| Live tests in CI | No ENTSO-E API key available in CI; live tests are for developer validation only |
| E2E tests for non-ENTSO-E connectors | Scope limited to ENTSO-E for this milestone |
| Gold layer validation | No gold consumers of ENTSO-E data yet |
| Performance/load testing | Not a current concern |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| CLI-01 | Phase H1 | Complete |
| CLI-02 | Phase H1 | Complete |
| MOCK-01 | Phase H2 | Pending |
| MOCK-02 | Phase H2 | Pending |
| MOCK-03 | Phase H2 | Pending |
| LIVE-01 | Phase H3 | Pending |
| LIVE-02 | Phase H3 | Pending |
| LIVE-03 | Phase H3 | Pending |

**Coverage:**
- v0.3 requirements: 8 total
- Mapped to phases: 8
- Unmapped: 0 ✓

---
*Requirements defined: 2026-05-02*
*Last updated: 2026-05-02 after initial definition*
