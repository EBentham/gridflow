# Requirements: gridflow v0.3

**Defined:** 2026-05-02
**Core Value:** Every connector reliably fetches real data and every silver transformer produces schema-valid output — verified end-to-end, not just in unit tests.

## v0.3 Requirements

### CLI

- [x] **CLI-01**: User can run `gridflow pipeline entsoe all --last 24h` and it processes all ENTSO-E datasets (positional `all` treated as `--all`) — Phase H1
- [x] **CLI-02**: Same `all` positional alias works for `gridflow ingest` and `gridflow transform` subcommands — Phase H1

### Testing — Mocked

- [x] **MOCK-01**: Integration test validates URL construction for each ENTSO-E dataset (correct base URL, required query parameters present) without hitting the live API — Phase H2
- [x] **MOCK-02**: Integration test runs the full bronze→silver pipeline for a representative set of ENTSO-E datasets using realistic XML fixture responses — Phase H2
- [x] **MOCK-03**: URL-shape test coverage spans all ENTSO-E registered datasets — Phase H2/H4

### Testing — Live

- [x] **LIVE-01**: Live test suite (`@pytest.mark.live`) fetches real data from the ENTSO-E API for active ENTSO-E datasets when `ENTSOE_API_KEY` is set - Phase H5.5
- [x] **LIVE-02**: Live tests verify that fetched XML responses parse and transform to silver without errors, with explicit skips for genuine ENTSO-E no-data acknowledgements - Phase H5.5
- [x] **LIVE-03**: Live tests are skipped by default and can be opted in with `pytest -m live`; a conftest fixture gate skips automatically when no API key is present - Phase H3
- [x] **LIVE-04**: Opt-in live request-shape probe covers representative ENTSO-E request parameter families and rejects unsupported parameter-name errors - Phase H4
- [x] **LIVE-CLEAN-01**: Live ENTSO-E cleanup distinguishes genuine no-data acknowledgements from invalid requests and parser regressions - Phase H5.5
- [x] **LIVE-CLEAN-02**: ENTSO-E live `application/zip` XML responses are ingested as XML bronze inputs that downstream transformers can consume - Phase H5.5
- [x] **LIVE-CLEAN-03**: Unit-level live XML tag variants are parsed into the same silver fields as fixture-backed unit payloads - Phase H5.5

### ENTSO-E Endpoint Coverage

- [x] **URL-01**: Existing ENTSO-E datasets construct documented request URLs, including load, generation, outage, balancing, and zone-pair parameter styles - Phase H4
- [x] **DOC-01**: Official ENTSO-E endpoint collection is represented as an auditable catalog or gap matrix in the repo - Phase H4
- [x] **COVER-01**: Every endpoint in the official collection is classified as implemented, planned, intentionally deferred, or out of scope - Phase H4
- [x] **COVER-02**: First high-priority missing ENTSO-E source batch is implemented through metadata, parser, schema, transformer, mocked E2E, and live request-shape gates - Phase H4
- [ ] **COVER-03**: Remaining planned ENTSO-E catalog rows are implemented or intentionally reclassified with a reason and owner batch - Phases H5-H8
- [ ] **LIVE-05**: Opt-in live request-shape probes cover every newly implemented H5-H8 request family without leaking `ENTSOE_API_KEY`
- [x] **SRC-GEN-01**: Installed capacity per production unit is available as `installed_capacity_units` with unit identity fields - Phase H5
- [x] **SRC-GEN-02**: Actual generation per generation unit is available as `actual_generation_units` with unit identity fields - Phase H5
- [x] **SRC-GEN-03**: Water reservoirs and hydro storage plants are available as `water_reservoirs` - Phase H5
- [x] **SRC-GEN-04**: Production and generation unit master data is available as `generation_units_master_data` or explicitly deferred if the reference payload needs a separate modelling phase - Phase H5
- [ ] **SRC-TX-01**: Transmission transfer/capacity variants are implemented for H6 planned catalog rows - Phase H6
- [ ] **SRC-TX-02**: Commercial schedule and net-position variants are implemented for H6 planned catalog rows - Phase H6
- [ ] **SRC-TX-03**: Market allocation, auction revenue, transfer-capacity use, and capacity allocated/nominated datasets are implemented for H6 planned catalog rows - Phase H6
- [ ] **SRC-TX-04**: Congestion management datasets are implemented for H6 planned catalog rows - Phase H6
- [ ] **SRC-OUT-01**: Aggregated consumption outage data is available as `outages_consumption` - Phase H7
- [ ] **SRC-OUT-02**: Transmission and offshore-grid outage data are available as `outages_transmission` and `outages_offshore_grid` - Phase H7
- [ ] **SRC-OUT-03**: Production-unit outage data is available as `outages_production` - Phase H7
- [ ] **SRC-BAL-01**: Current balancing state and balancing financial expenses/income are available as silver datasets - Phase H8
- [ ] **SRC-BAL-02**: Balancing energy bid datasets are implemented or split with explicit parser-backed deferral reasons - Phase H8
- [ ] **SRC-BAL-03**: Procured balancing capacity and cross-zonal balancing capacity are available as silver datasets - Phase H8
- [ ] **SRC-BAL-04**: SO GL and implementation-framework balancing extensions remain intentionally deferred unless promoted by updated project scope - Phase H8

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
| MOCK-01 | Phase H2 | Complete |
| MOCK-02 | Phase H2 | Complete |
| MOCK-03 | Phase H2 | Complete |
| LIVE-01 | Phase H5.5 | Complete - active ENTSO-E datasets fetch real live responses |
| LIVE-02 | Phase H5.5 | Complete - active live XML transforms or skips explicit no-data acknowledgements |
| LIVE-03 | Phase H3 | Satisfied - live gate opt-in and absent-key skip verified |
| LIVE-04 | Phase H4 | Satisfied - representative live request-shape gate passes |
| LIVE-CLEAN-01 | Phase H5.5 | Complete |
| LIVE-CLEAN-02 | Phase H5.5 | Complete |
| LIVE-CLEAN-03 | Phase H5.5 | Complete |
| URL-01 | Phase H4 | Complete |
| DOC-01 | Phase H4 | Complete |
| COVER-01 | Phase H4 | Complete |
| COVER-02 | Phase H4 | Complete for first load-domain batch; remaining batches tracked in endpoint catalog |
| COVER-03 | Phases H5-H8 | Partial - H5 rows implemented; H6-H8 remain planned |
| LIVE-05 | Phases H5-H8 | Partial - H5 request-shape gate passed; H6-H8 remain planned |
| SRC-GEN-01 | Phase H5 | Complete |
| SRC-GEN-02 | Phase H5 | Complete |
| SRC-GEN-03 | Phase H5 | Complete |
| SRC-GEN-04 | Phase H5 | Complete |
| SRC-TX-01 | Phase H6 | Planned |
| SRC-TX-02 | Phase H6 | Planned |
| SRC-TX-03 | Phase H6 | Planned |
| SRC-TX-04 | Phase H6 | Planned |
| SRC-OUT-01 | Phase H7 | Planned |
| SRC-OUT-02 | Phase H7 | Planned |
| SRC-OUT-03 | Phase H7 | Planned |
| SRC-BAL-01 | Phase H8 | Planned |
| SRC-BAL-02 | Phase H8 | Planned |
| SRC-BAL-03 | Phase H8 | Planned |
| SRC-BAL-04 | Phase H8 | Planned |

**Coverage:**
- v0.3 requirements: 33 total
- Mapped to phases: 33
- Unmapped: 0 ✓

---
*Requirements defined: 2026-05-02*
*Last updated: 2026-05-03 after Phase H5.5 live cleanup*
