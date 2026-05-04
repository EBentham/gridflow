# Requirements: gridflow v0.5 ENTSOG Pipeline Validation

**Defined:** 2026-05-04
**Core Value:** Every connector reliably fetches real data and every silver transformer produces schema-valid output - verified end-to-end, not just in unit tests.

## v0.5 Requirements

### ENTSOG Inventory

- [x] **ENTSOG-INV-01**: Developer can compare configured ENTSOG datasets, endpoint definitions, and silver transformer registrations and see that every active configured dataset has a matching request definition and transformer.
- [x] **ENTSOG-INV-02**: Developer can inspect an ENTSOG endpoint catalog that classifies each documented TP API endpoint, operational indicator dataset, request style, parser family, implementation status, and known live-data caveat.
- [x] **ENTSOG-INV-03**: Developer can see exact request parameter styles for ENTSOG operational, CMP, interruption, aggregated, tariff, UMM, and reference endpoints.

### Bronze Connector

- [x] **ENTSOG-BRONZE-01**: Developer can fetch every active ENTSOG dataset through the connector using endpoint metadata rather than one-off `physical_flows` logic.
- [x] **ENTSOG-BRONZE-02**: Operational datasets use exact-case indicators and include default GB-relevant `pointDirection` filters so live requests do not fail with documented no-result responses caused by missing filters.
- [x] **ENTSOG-BRONZE-03**: Date-window ENTSOG datasets include `from`, `to`, `periodType`, `timeZone`, and bounded `limit` handling with caller overrides for live smoke tests.
- [x] **ENTSOG-BRONZE-04**: Referential ENTSOG datasets fetch with endpoint-specific filters such as `hasData=1` without forcing irrelevant date windows.

### Silver Transform

- [x] **ENTSOG-SILVER-01**: Physical flow silver output preserves the existing GWh/day normalisation and remains compatible with the current `physical_flows` dataset.
- [x] **ENTSOG-SILVER-02**: Generic ENTSOG silver transformers convert endpoint JSON records into deterministic snake_case parquet outputs with provider metadata, timestamps where available, and stable deduplication.
- [x] **ENTSOG-SILVER-03**: Reference datasets can be transformed into non-empty silver outputs from realistic bronze JSON without requiring date-specific semantics.

### Mocked and Fixture E2E

- [x] **ENTSOG-MOCK-01**: Mocked ENTSOG tests validate request URL and query parameter shape for every active configured ENTSOG dataset without touching the network.
- [x] **ENTSOG-MOCK-02**: Fixture-backed tests write ENTSOG JSON responses through `BronzeWriter` and run representative silver transformers across operational, CMP/event, tariff/UMM, and reference families.
- [x] **ENTSOG-MOCK-03**: Tests assert bronze metadata, `data_date` partitioning for date-window datasets, reference output placement, and expected silver columns.

### Live and CLI Smoke

- [x] **ENTSOG-LIVE-01**: Opt-in live ENTSOG tests call the real public API for all active endpoint families with narrow deterministic windows, small limits, and clear source/dataset/stage diagnostics.
- [x] **ENTSOG-LIVE-02**: Live ENTSOG tests explicitly classify `No result found` or empty payloads as skip/deferred outcomes rather than silent passes.
- [x] **ENTSOG-LIVE-03**: Representative live ENTSOG responses can be written to bronze and transformed to silver parquet under temporary data roots.
- [x] **ENTSOG-CLI-01**: Developer can run isolated live `gridflow pipeline entsog ...` smoke tests for a safe curated ENTSOG dataset subset and see bronze/silver outputs without polluting normal project data.

## Future Requirements

- **ENTSOG-FUT-01**: Add domain-specific silver schemas for every ENTSOG endpoint family once downstream gold consumers require typed models beyond generic normalised records.
- **ENTSOG-FUT-02**: Add scheduled live monitoring for ENTSOG endpoint availability outside the normal unit-test suite.
- **GIE-FUT-01**: Apply the same connector-confidence pattern to GIE AGSI/ALSI after ENTSOG validation lands.

## Out of Scope

| Feature | Reason |
| --- | --- |
| Running ENTSOG live tests in CI by default | Public API calls can be slow/noisy and should remain opt-in developer validation. |
| Gold-layer ENTSOG modelling views | The requested scope is API endpoint coverage plus bronze and silver stages. |
| Dynamic discovery of all European point directions before every operational fetch | Useful later, but default static GB-relevant point directions keep initial requests narrow and predictable. |
| Per-endpoint bespoke Pydantic models for every ENTSOG record shape | Generic silver records are sufficient for endpoint coverage; typed schemas can follow downstream modelling needs. |

## Traceability

| Requirement | Phase | Status |
| --- | --- | --- |
| ENTSOG-INV-01 | Phase J1 | Complete |
| ENTSOG-INV-02 | Phase J1 | Complete |
| ENTSOG-INV-03 | Phase J1 | Complete |
| ENTSOG-BRONZE-01 | Phase J2 | Complete |
| ENTSOG-BRONZE-02 | Phase J2 | Complete |
| ENTSOG-BRONZE-03 | Phase J2 | Complete |
| ENTSOG-BRONZE-04 | Phase J2 | Complete |
| ENTSOG-SILVER-01 | Phase J3 | Complete |
| ENTSOG-SILVER-02 | Phase J3 | Complete |
| ENTSOG-SILVER-03 | Phase J3 | Complete |
| ENTSOG-MOCK-01 | Phase J3 | Complete |
| ENTSOG-MOCK-02 | Phase J3 | Complete |
| ENTSOG-MOCK-03 | Phase J3 | Complete |
| ENTSOG-LIVE-01 | Phase J4 | Complete |
| ENTSOG-LIVE-02 | Phase J4 | Complete |
| ENTSOG-LIVE-03 | Phase J4 | Complete |
| ENTSOG-CLI-01 | Phase J4 | Complete |

**Coverage:**
- v0.5 requirements: 17 total
- Mapped to phases: 17
- Unmapped: 0

---
*Requirements defined: 2026-05-04*
*Last updated: 2026-05-04 after v0.5 implementation and verification*
