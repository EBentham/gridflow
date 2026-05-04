# gridflow - Current Milestone Requirements

## Milestone v0.6 NESO Carbon Intensity Platform

### Endpoint Inventory

- [x] **NESO-01**: Developer can see every documented NESO Carbon Intensity API route in an auditable endpoint catalog.
- [x] **NESO-02**: Gridflow source config and connector endpoint metadata include the same active NESO dataset inventory.
- [x] **NESO-03**: NESO endpoint path construction records path variables in bronze provenance metadata.

### Bronze And Silver Pipeline

- [x] **NESO-04**: User can ingest every NESO dataset through the connector registry without authentication.
- [x] **NESO-05**: User can transform national intensity, statistics, factors, generation, and regional responses into deterministic silver parquet.
- [x] **NESO-06**: Regional silver output preserves region metadata, intensity values, and all nested generation-mix entries for an API query.
- [x] **NESO-07**: Generation silver output preserves every nested fuel percentage entry for an API query.

### Verification

- [x] **NESO-08**: Non-live tests prove endpoint inventory, path construction, mocked fetches, bronze writes, and silver transforms for every active NESO dataset.
- [x] **NESO-09**: Opt-in live tests prove every active NESO API route can flow from real API response through bronze into silver.
- [x] **NESO-10**: Opt-in live CLI smoke test proves the user-facing `pipeline` command path creates NESO bronze and silver outputs in isolated directories.

## Traceability

| Requirement | Phase |
|-------------|-------|
| NESO-01 | K1 |
| NESO-02 | K1 |
| NESO-03 | K2 |
| NESO-04 | K2 |
| NESO-05 | K3 |
| NESO-06 | K3 |
| NESO-07 | K3 |
| NESO-08 | K3 |
| NESO-09 | K4 |
| NESO-10 | K4 |
