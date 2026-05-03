---
phase: H6
status: passed
verified: 2026-05-03
---

# H6 Verification - ENTSO-E Transmission and Market Sources

## Result

H6 passed automated verification.

## Requirements

| Requirement | Status | Evidence |
|-------------|--------|----------|
| SRC-TX-01 | passed | Transmission transfer/capacity rows are implemented through metadata, config, schemas, shared transformers, fixtures, mocked E2E, and live request-shape coverage. |
| SRC-TX-02 | passed | Commercial schedules and net-position variants are active datasets with fixture-backed bronze-to-silver coverage. |
| SRC-TX-03 | passed | Market allocation, auction revenue, transfer-capacity use, nominated capacity, allocated capacity, congestion income, and net positions are implemented. |
| SRC-TX-04 | passed | Redispatching, countertrading, and congestion-management cost datasets are implemented with business-type metadata and silver coverage. |
| COVER-03 | partial | H6 catalog rows are implemented or explicitly reclassified; H7-H8 planned rows remain. |
| LIVE-05 | partial | H6 representative live request-shape coverage passed; H7-H8 request families remain future work. |

## Automated Checks

- Ruff targeted H6 gate: passed.
- Non-live pytest gate: 332 passed, 91 deselected, 1 warning.
- Credentialed live request-shape gate: 11 passed.

## Reclassified Rows

- `flow_based_allocations` is deferred with a parser/schema reason because B09
  allocation documents need dedicated shape review.
- `third_country_transfer_capacity` remains deferred as lower priority than
  core bidding-zone and GB interconnector datasets.

## Residual Risk

- Full live fetch and live bronze-to-silver transformation for every ENTSO-E
  dataset remains opt-in and sensitive to API credentials, endpoint permissions,
  and data availability for the selected date range.
- H6 uses shared transformer families for aligned time-series payloads; future
  data variants with richer allocation fields should get dedicated parser and
  schema work rather than widening these generic families.
