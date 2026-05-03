---
status: complete
phase: H6-entsoe-transmission-market-sources
source:
  - H6-01-SUMMARY.md
started: 2026-05-03T11:43:02+01:00
updated: 2026-05-03T11:43:02+01:00
---

## Current Test

[testing complete]

## Tests

### 1. Backfill all active ENTSO-E datasets for the H6 validation date
expected: `uv run --extra dev gridflow backfill entsoe --all --start 2026-04-15 --end 2026-04-16` completes without leaking `ENTSOE_API_KEY`, without request-shape failures, and prints `Backfill complete`.
result: pass
evidence: The exact command completed successfully after same-area request metadata fixes for `congestion_management_costs` and `net_positions`.

### 2. H6 datasets ingest and transform through the real CLI path
expected: Newly added H6 datasets are included by `--all`, fetch live ENTSO-E responses, and transform successfully; valid no-data acknowledgements may produce 0 rows without failing the command.
result: pass
evidence: H6 datasets reached ingest and transform. Datasets with live data produced rows including `dc_link_intraday_transfer_limits`, `commercial_schedules`, `commercial_schedules_net_positions`, `redispatching_internal`, `countertrading`, `auction_revenue`, `total_nominated_capacity`, `total_capacity_allocated`, and `net_positions`; no-data H6 endpoints completed with 0 rows.

### 3. H6 regression gates still pass after UAT fixes
expected: Targeted lint, non-live ENTSO-E tests, and live request-shape probes pass after the end-to-end fixes.
result: pass
evidence: Ruff passed; targeted non-live pytest reported 332 passed, 91 deselected, 1 warning; live request-shape pytest reported 11 passed.

## Summary

total: 3
passed: 3
issues: 0
pending: 0
skipped: 0
blocked: 0

## Gaps

[none]
