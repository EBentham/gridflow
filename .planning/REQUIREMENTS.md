# gridflow - Current Milestone Requirements

## Milestone v0.7 GIE AGSI Gas Storage Validation

### Endpoint Inventory

- [x] **AGSI-01**: Developer can see every official GIE AGSI API endpoint family, query parameter, response family, and active/deferred decision in an auditable endpoint catalog.
- [x] **AGSI-02**: Gridflow source config and connector endpoint metadata expose the same active AGSI dataset inventory for storage, EIC listing, news, and unavailability families.
- [x] **AGSI-03**: AGSI company and facility query planning is driven by `/api/about?show=listing` so expected bronze entries can be derived from live or fixture inventory.

### Bronze Completeness

- [x] **AGSI-04**: User can query AGSI storage by aggregate type, country, company, facility, exact gas day, and gas-day range with documented `x-key`, date, filter, pagination, and rate-limit semantics.
- [x] **AGSI-05**: AGSI connector paginates with `last_page`, writes one bronze response per expected page/request, records query scope in provenance, and partitions bronze by the gas day being requested.
- [x] **AGSI-06**: For exact-day queries such as 2026-05-01, bronze completeness tests prove every expected response/page for the selected AGSI query plan was fetched and no out-of-window gas days leaked into bronze.

### Silver Pipeline

- [x] **AGSI-07**: AGSI storage silver output preserves all storage inventory, flow, capacity, fullness, status, update, service-announcement, and entity metadata needed from live API payloads.
- [x] **AGSI-08**: AGSI reference/news/unavailability silver outputs either preserve documented payload fields through deterministic silver parquet or are explicitly classified as deferred with a catalog reason.
- [x] **AGSI-09**: Fixture-backed bronze-to-silver tests prove AGSI bronze payloads for aggregate, country, company, facility, listing, news, and unavailability families successfully transform into schema-valid silver outputs.

### Verification

- [x] **AGSI-10**: Non-live tests prove endpoint inventory alignment, mocked request shapes, pagination by `last_page`, expected-count accounting, and representative bronze-to-silver flows for every active AGSI dataset family.
- [x] **AGSI-11**: Opt-in live API tests use `GIE_API_KEY` to prove live AGSI responses flow through bronze into silver, including representative aggregate/country/company/facility scopes and explicit no-data/error classification.
- [x] **AGSI-12**: Opt-in live CLI smoke tests run AGSI pipeline/backfill commands under isolated `GRIDFLOW_*` paths and verify bronze and silver outputs for curated AGSI datasets.

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| AGSI-01 | L1 | Completed |
| AGSI-02 | L2 | Completed |
| AGSI-03 | L1 | Completed |
| AGSI-04 | L2 | Completed |
| AGSI-05 | L2 | Completed |
| AGSI-06 | L2 | Completed |
| AGSI-07 | L3 | Completed |
| AGSI-08 | L3 | Completed |
| AGSI-09 | L3 | Completed |
| AGSI-10 | L3 | Completed |
| AGSI-11 | L4 | Completed |
| AGSI-12 | L4 | Completed |

