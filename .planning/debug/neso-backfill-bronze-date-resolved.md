---
status: resolved
trigger: "NESO backfill wrote intensity_current bronze under the fetch date instead of requested backfill dates, so transform wrote no silver."
created: 2026-05-04
updated: 2026-05-04
---

## Current Focus

hypothesis: NESO connector left `RawResponse.data_date` unset for non-window, non-reference endpoints.
test: Run isolated live `backfill neso intensity_current --start 2026-05-01 --end 2026-05-03 --chunk-days 1`.
expecting: Bronze partitions should be `2026/05/01` and `2026/05/02`; silver parquet should be written for both dates.
next_action: none

## Evidence

- timestamp: 2026-05-04
  observation: `CarbonIntensityConnector.fetch()` set `data_date` only when `endpoint.requires_window` was true.
- timestamp: 2026-05-04
  observation: `intensity_current` has no window path variables, so bronze writer fell back to `fetched_at.date()`.
- timestamp: 2026-05-04
  observation: After setting `data_date=window_start.date()` for all non-reference NESO responses, isolated live backfill wrote bronze under `2026/05/01` and `2026/05/02`, then wrote matching silver parquet.

## Eliminated

- hypothesis: Silver transformer could not parse current intensity payloads.
  reason: Once bronze was under target partitions, transform wrote one row per chunk.

## Resolution

root_cause: Non-reference NESO current-style endpoints did not set `RawResponse.data_date`, so bronze partitioning used fetch time rather than requested backfill date.
fix: Set `data_date` for every non-reference NESO response and keep static reference endpoints latest-file based.
verification: Focused NESO tests pass; isolated live backfill for `intensity_current` writes bronze and silver for each requested chunk date.
files_changed:
- src/gridflow/connectors/neso/carbon_intensity.py
- tests/integration/test_neso_mocked_e2e.py
