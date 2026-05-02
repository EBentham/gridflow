---
phase: H3-live-entsoe-test-suite
plan: 02
subsystem: entsoe-testing
tags: [pytest, live, entsoe, bronze, silver, cli]

requires: [H3-01-live-test-scaffolding-and-cli-failure-propagation]
provides:
  - All-16 live ENTSO-E fetch tests
  - All-16 live ENTSO-E bronze-to-silver tests
  - Live command coverage for pipeline, ingest, and transform
  - Honest credential-gated verification record
affects: [entsoe-live-tests, entsoe-connector, entsoe-silver-transformers, entsoe-cli]

tech-stack:
  added: []
  patterns:
    - EntsoeConnector live fetch parametrized over DOC_TYPES
    - BronzeWriter plus real transformer live E2E coverage
    - CliRunner live commands under temp config/data roots

key-files:
  created:
    - tests/integration/test_entsoe_live.py
  modified: []

key-decisions:
  - "Cover every registered ENTSO-E DOC_TYPES dataset in live connector and bronze-to-silver tests."
  - "Treat empty live responses, zero-row transforms, and non-zero command output as hard failures after opt-in."
  - "Record missing ENTSOE_API_KEY as human-needed verification rather than marking H3 passed."

patterns-established:
  - "Live E2E tests should assert response HTTP status, content type, body size, bronze artifacts, silver parquet, and row counts with dataset/stage diagnostics."
  - "Common CLI live commands should run under copied config and temp pipeline paths."

requirements-completed: []

duration: 25min
completed: 2026-05-02
status: human_needed
---

# Phase H3 Plan 02: Live ENTSO-E E2E Summary

**All-dataset live ENTSO-E connector, bronze-to-silver, and command tests are implemented but await credentials for real API execution.**

## Performance

- **Started:** 2026-05-02T18:18:00+01:00
- **Completed:** 2026-05-02T18:43:00+01:00
- **Tasks:** 4
- **Files modified:** 1

## Accomplishments

- Added live connector tests parametrized over all 16 `DOC_TYPES` datasets.
- Added live bronze-to-silver tests that fetch real XML, write through `BronzeWriter`, run real ENTSO-E transformers, and assert silver parquet output.
- Added live command tests for `gridflow pipeline entsoe all --last 24h`, `gridflow ingest entsoe all --last 24h`, and `gridflow transform entsoe all --last 24h`.
- Added redacted dataset/stage diagnostics for fetch, bronze write, transform, and command failures.

## Task Commits

1. **Tasks 1-3: All-dataset live tests and command coverage** - `0ec7573` (test)

## Files Created/Modified

- `tests/integration/test_entsoe_live.py` - Contains all live connector, bronze-to-silver, and CLI command coverage.

## Decisions Made

- Used `LIVE_TARGET_DATE = 2024-01-15` and a one-day UTC fetch window as the first shared live window. If ENTSO-E returns no data for one or more datasets, that should fail and the date window should be adjusted with evidence.
- Used production `EntsoeConnector`, `BronzeWriter`, `get_transformer`, and Typer command surfaces rather than test doubles for live coverage.
- Left Elexon live coverage out of H3, per phase scope.

## Deviations from Plan

None - implementation stayed within the H3 ENTSO-E live-test scope.

## Issues Encountered

- `ENTSOE_API_KEY` is absent in this environment, so the live gate was invoked but all live tests skipped. H3 cannot be marked passed until the key is provided and the live gate runs.
- The full-suite command is still blocked during collection by the known non-H3 Elexon import issue: `ModuleNotFoundError: No module named 'gridflow.silver.elexon.agpt'`.

## Verification

- `uv run --extra dev pytest tests/integration/test_entsoe_live.py -m "not live" -x -q` - 6 passed, 36 deselected.
- `uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py -x -q` - 36 skipped, 6 deselected because `ENTSOE_API_KEY` is absent.
- `uv run --extra dev pytest tests/integration/test_entsoe_live.py tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_connector.py tests/unit/test_entsoe.py -x -q` - 232 passed, 36 skipped.
- `uv run --extra dev pytest -x -q` - attempted; stopped at the known Elexon `gridflow.silver.elexon.agpt` import blocker.

## User Setup Required

Set `ENTSOE_API_KEY` in the shell and run:

```powershell
uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py -x -q
```

If that passes, rerun the focused ENTSO-E regression command from the verification section and H3 can be marked complete.

## Next Phase Readiness

H3 is implementation-complete but verification is pending live credentials. The Elexon-focused follow-up project remains separate.

## Self-Check: HUMAN_NEEDED

Implementation and non-live verification passed. Real ENTSO-E live execution is blocked until `ENTSOE_API_KEY` is available.

---
*Phase: H3-live-entsoe-test-suite*
*Completed: 2026-05-02*
