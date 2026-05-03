# Requirements: gridflow v0.4

**Defined:** 2026-05-03
**Core Value:** Every connector reliably fetches real data and every silver transformer produces schema-valid output — verified end-to-end, not just in unit tests.

## v0.4 Requirements

### Elexon Inventory

- [x] **ELEXON-INV-01**: Developer can compare configured Elexon datasets, `ENDPOINTS`, and silver transformer registrations and see that every active configured dataset has a matching request definition and transformer.
- [x] **ELEXON-INV-02**: Developer can see decommissioned, duplicate, empty, or intentionally excluded Elexon endpoints documented separately from active datasets so live tests do not produce false failures.
- [x] **ELEXON-INV-03**: Developer can verify every active Elexon endpoint has an explicit parameter style: path-date, publish/from-to datetime, settlementDate+settlementPeriod, or no-param reference data.

### Mocked and Fixture E2E

- [x] **ELEXON-MOCK-01**: Mocked Elexon tests validate request URL and query parameter shape for every active configured Elexon dataset without hitting the live API.
- [x] **ELEXON-MOCK-02**: Fixture-backed tests write realistic Elexon JSON responses through `BronzeWriter` and run representative silver transformers across generation, demand, balancing, notices, reference, and REMIT families.
- [x] **ELEXON-MOCK-03**: Mocked/fixture tests assert bronze metadata, `data_date` partitioning, pagination/chunk handling, and expected silver columns for representative datasets.

### Live API to Silver

- [ ] **ELEXON-LIVE-01**: Opt-in live Elexon tests call the real public Insights API for active configured datasets with narrow, deterministic windows and assert HTTP success, JSON shape, content size, and pagination metadata.
- [ ] **ELEXON-LIVE-02**: Opt-in live Elexon tests write real API responses to bronze and transform them to silver parquet for a representative dataset set spanning all parameter styles and major transformer families.
- [ ] **ELEXON-LIVE-03**: Live Elexon tests verify silver output row counts, required columns, `data_provider` values where present, and schema validation via existing transformer contracts.
- [ ] **ELEXON-LIVE-04**: Live Elexon tests explicitly classify empty/no-data responses and known removed endpoints as skip/deferred/documented outcomes, not silent passes.
- [ ] **ELEXON-LIVE-05**: Live Elexon tests are marked `@pytest.mark.live`, are excluded from normal test runs, require no API key, and provide clear diagnostics including source, dataset, stage, URL, status, and body preview.

### CLI and Backfill

- [ ] **ELEXON-CLI-01**: Developer can run a live `gridflow pipeline elexon ...` smoke test against a safe curated Elexon dataset subset and see bronze and silver output directories created under a temporary data root.
- [ ] **ELEXON-CLI-02**: Developer can run live `gridflow ingest elexon ...` followed by `gridflow transform elexon ...` against isolated temp config/data paths and see failures reported per dataset with non-zero exit on real errors.
- [ ] **ELEXON-CLI-03**: Developer can run a live Elexon backfill smoke test for at least one path-date, one publish-datetime/from-to, and one no-param/reference dataset without polluting the normal project data directory.

### Documentation and Close-Out

- [ ] **ELEXON-DOC-01**: Elexon live test commands, selected live dataset windows, expected skips, and troubleshooting notes are documented in phase artifacts.
- [ ] **ELEXON-DOC-02**: Requirements and roadmap traceability show which phase owns every Elexon validation requirement.

## Future Requirements

### Broader Connector Coverage

- **BROAD-01**: Extend the same live/mock/fixture E2E pattern to ENTSO-G and GIE connectors.
- **BROAD-02**: Add scheduled live smoke monitoring outside the normal unit-test suite.

### Elexon Source Expansion

- **ELEXON-FUT-01**: Promote additional official Elexon datasets not currently configured after endpoint availability, payload shape, and silver modelling are reviewed.
- **ELEXON-FUT-02**: Build a full Elexon endpoint catalog equivalent to `docs/entsoe_endpoint_catalog.yaml`.

## Out of Scope

| Feature | Reason |
|---------|--------|
| Running Elexon live tests in CI by default | Live tests hit production network services and should remain opt-in developer validation |
| Implementing new Elexon datasets | This milestone validates the existing active Elexon pipeline rather than expanding source coverage |
| Gold-layer validation | User priority is live API through silver; gold consumers can be tested after connector confidence improves |
| ENTSO-G and GIE E2E | Tracked as future requirements so v0.4 stays focused on Elexon |
| Full Elexon endpoint catalog | Useful follow-up, but not required to prove current configured datasets flow to silver |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| ELEXON-INV-01 | Phase I1 | Complete |
| ELEXON-INV-02 | Phase I1 | Complete |
| ELEXON-INV-03 | Phase I1 | Complete |
| ELEXON-MOCK-01 | Phase I2 | Complete |
| ELEXON-MOCK-02 | Phase I2 | Complete |
| ELEXON-MOCK-03 | Phase I2 | Complete |
| ELEXON-LIVE-01 | Phase I3 | Pending |
| ELEXON-LIVE-02 | Phase I3 | Pending |
| ELEXON-LIVE-03 | Phase I3 | Pending |
| ELEXON-LIVE-04 | Phase I3 | Pending |
| ELEXON-LIVE-05 | Phase I3 | Pending |
| ELEXON-CLI-01 | Phase I4 | Pending |
| ELEXON-CLI-02 | Phase I4 | Pending |
| ELEXON-CLI-03 | Phase I4 | Pending |
| ELEXON-DOC-01 | Phase I4 | Pending |
| ELEXON-DOC-02 | Phase I4 | Pending |

**Coverage:**
- v0.4 requirements: 16 total
- Mapped to phases: 16
- Unmapped: 0

---
*Requirements defined: 2026-05-03*
*Last updated: 2026-05-04 after Phase I2 completion*
