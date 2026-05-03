---
phase: I3
plan: 01
subsystem: testing
tags: [elexon, live-api, bronze, silver, polars, pytest]

requires:
  - phase: I2
    provides: Elexon mocked request-shape and fixture-backed bronze-to-silver patterns
provides:
  - Opt-in live Elexon API-to-silver integration tests for representative datasets
  - Live public API response checks through RawResponse, BronzeWriter, silver transformers, and parquet assertions
  - Explicit documented handling for known excluded Elexon endpoints
affects: [I4, elexon-live-tests, elexon-cli-smoke-tests]

tech-stack:
  added: []
  patterns:
    - Live-marked pytest integration test with non-live documentation sentinel
    - Public Elexon API responses written only under pytest temp data roots
    - Representative request-style matrix for DATE_PATH, PUBLISH_DATETIME, SETTLEMENT_DATE_PERIOD, and NO_PARAMS

key-files:
  created:
    - tests/integration/test_elexon_live_e2e.py
  modified:
    - .planning/phases/I3-elexon-live-api-to-silver-test-suite/I3-01-SUMMARY.md
    - .planning/phases/I3-elexon-live-api-to-silver-test-suite/I3-VERIFICATION.md
    - .planning/ROADMAP.md
    - .planning/REQUIREMENTS.md
    - .planning/STATE.md
    - .planning/PROJECT.md

key-decisions:
  - "Mark only the real network E2E test as live so `-m \"not live\"` has a non-live sentinel test and exits 0."
  - "Use one successful non-empty live response per representative dataset to keep the suite fast while still proving the medallion path."
  - "Preserve `bmunits_reference` as a non-date-partitioned silver parquet path."

patterns-established:
  - "Live Elexon tests assert request metadata, bronze sidecars, silver rows, expected columns, and data_provider where present."
  - "Known removed or intentionally excluded endpoints are checked as documentation coverage rather than called in the bronze-to-silver path."

requirements-completed:
  - ELEXON-LIVE-01
  - ELEXON-LIVE-02
  - ELEXON-LIVE-03
  - ELEXON-LIVE-04
  - ELEXON-LIVE-05

duration: 18min
completed: 2026-05-04
---

# Phase I3 Plan 01: Elexon Live API to Silver Test Suite Summary

**Opt-in live Elexon tests now prove representative public Insights API responses flow through bronze storage into silver parquet.**

## Performance

- **Duration:** 18 min
- **Started:** 2026-05-04T00:13:00+01:00
- **Completed:** 2026-05-04T00:31:00+01:00
- **Tasks:** 4
- **Files modified:** 6

## Accomplishments

- Added `tests/integration/test_elexon_live_e2e.py` with live coverage for `system_prices`, `boal`, `freq`, `pn`, and `bmunits_reference`.
- Covered representative `DATE_PATH`, `PUBLISH_DATETIME`/`from`-`to`, `SETTLEMENT_DATE_PERIOD`, and `NO_PARAMS` Elexon request styles.
- Wrote live API responses through `BronzeWriter`, asserted `.meta.json` sidecars, ran registered silver transformers, and read generated parquet with Polars.
- Verified silver row counts, required columns, and `data_provider == "elexon"` where present.
- Added a non-live documentation sentinel for `EXCLUDED_ENDPOINTS` so normal `-m "not live"` runs prove exclusion behavior and return exit code 0.

## Task Commits

1. **I3-01 live Elexon API-to-silver suite** - `460c4da` (`test(I3-01): add elexon live api silver suite`)

## Files Created/Modified

- `tests/integration/test_elexon_live_e2e.py` - Adds opt-in live Elexon API-to-bronze-to-silver integration coverage and excluded endpoint documentation checks.
- `.planning/phases/I3-elexon-live-api-to-silver-test-suite/I3-01-SUMMARY.md` - Captures phase execution results.
- `.planning/phases/I3-elexon-live-api-to-silver-test-suite/I3-VERIFICATION.md` - Captures goal-backward verification.
- `.planning/ROADMAP.md` - Marks I3 and I3-01 complete.
- `.planning/REQUIREMENTS.md` - Marks ELEXON-LIVE requirements complete.
- `.planning/STATE.md` - Advances current position to I4.
- `.planning/PROJECT.md` - Moves Elexon mocked/live validation outcomes into validated project state.

## Decisions Made

- The live network test is function-level `@pytest.mark.live`, not module-level. Module-level live marking caused `pytest -m "not live"` to deselect the whole file and return pytest exit code 5, so a non-live sentinel test now keeps the opt-in proof green.
- The live test writes one selected non-empty response per representative dataset to keep the suite bounded while still proving the connector, bronze writer, transformer, and parquet path.
- HTTP 4xx from representative active datasets fails the live test because that indicates request-shape or API drift; empty `data` responses are classified with explicit skip diagnostics.

## Deviations from Plan

**1. [Rule 3 - Blocking] Module-level live mark made the non-live verification command fail**
- **Found during:** Task 4
- **Issue:** `pytest tests/integration/test_elexon_live_e2e.py -m "not live" -q` deselected every test and exited non-zero.
- **Fix:** Marked only the real network E2E test with `@pytest.mark.live` and left the excluded-endpoint documentation assertion non-live.
- **Files modified:** `tests/integration/test_elexon_live_e2e.py`
- **Verification:** Non-live command passed with `1 passed, 5 deselected`.
- **Committed in:** `460c4da`

**Total deviations:** 1 auto-fixed blocking verification issue.
**Impact:** The opt-in live behavior is stronger: normal non-live runs now have an actual assertion and still avoid public API calls.

## Issues Encountered

- Initial lint flagged type-only imports; fixed with `TYPE_CHECKING`.
- The public Elexon representative endpoints all returned usable live data during execution. No live skips were needed.

## Verification

- `uv run --extra dev ruff check tests/integration/test_elexon_live_e2e.py` - passed.
- `uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m "not live" -q` - passed, 1 passed and 5 deselected.
- `uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m live -q -rs` - passed, 5 passed and 1 deselected.
- `uv run --extra dev ruff check tests/integration/test_elexon_live_e2e.py tests/endpoints/test_endpoint_live.py tests/integration/test_elexon_mocked_e2e.py` - passed.
- `uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py -m "not live" -q` - passed, 71 tests.

## User Setup Required

None. The live Elexon suite uses the public no-key Insights API and remains opt-in with `-m live`.

## Next Phase Readiness

I4 can reuse `tests/integration/test_elexon_live_e2e.py` as the API-to-silver confidence baseline when adding CLI, ingest, transform, pipeline, and backfill live smoke tests against isolated temp paths.

## Self-Check: PASSED

- Plan tasks completed.
- Requirements `ELEXON-LIVE-01` through `ELEXON-LIVE-05` satisfied.
- Live, non-live, lint, and regression verification passed.
- No human verification required.

---
*Phase: I3*
*Completed: 2026-05-04*
