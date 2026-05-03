---
phase: H5
status: passed
verified: 2026-05-03
---

# H5 Verification - ENTSO-E Generation Unit Sources

## Result

H5 passed automated verification.

## Requirements

| Requirement | Status | Evidence |
|-------------|--------|----------|
| SRC-GEN-01 | passed | `installed_capacity_units` metadata, config, fixture, schema, transformer, mocked E2E, and live request-shape coverage added. |
| SRC-GEN-02 | passed | `actual_generation_units` metadata, config, fixture, schema, transformer, mocked E2E, and live request-shape coverage added. |
| SRC-GEN-03 | passed | `water_reservoirs` metadata, config, fixture, schema, transformer, and mocked E2E coverage added. |
| SRC-GEN-04 | passed | `generation_units_master_data` implemented with master-data parser, schema, transformer, fixture, mocked E2E, and live A95 request-shape coverage. |
| COVER-03 | partial | H5 catalog rows are implemented and aligned with `DOC_TYPES`; H6-H8 rows remain planned. |
| LIVE-05 | partial | H5 representative live request-shape coverage passed; H6-H8 request families remain future work. |

## Automated Checks

- Ruff targeted H5 gate: passed.
- Non-live pytest gate: 288 passed, 58 deselected, 1 warning.
- Credentialed live request-shape gate: 8 passed.
- Targeted live A95 parser sanity check: first GB response parsed 230 records.

## Regression Coverage

- H4 bronze partition issue is covered by tests proving ENTSO-E responses carry
  `data_date` from `periodStart` and `BronzeWriter` partitions by `data_date`
  rather than ingestion date.

## Residual Risk

- Full live fetch and live bronze-to-silver transformation for every ENTSO-E
  dataset remains opt-in and is still sensitive to API credentials and endpoint
  data availability for the chosen date range.
