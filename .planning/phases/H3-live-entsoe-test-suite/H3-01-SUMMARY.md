---
phase: H3-live-entsoe-test-suite
plan: 01
subsystem: entsoe-testing
tags: [pytest, live, entsoe, cli, diagnostics]

requires: [H2-entsoe-mocked-e2e-tests]
provides:
  - Opt-in live ENTSO-E pytest scaffolding
  - Temporary config/data isolation helpers for live command tests
  - CLI ingest/transform failure propagation with redacted diagnostics
  - Pytest collection gate so live tests do not run unless selected with -m live
affects: [H3-live-entsoe-test-suite, entsoe-cli, entsoe-live-tests]

tech-stack:
  added: []
  patterns:
    - live pytest marker plus credential skip gate
    - temporary config directory for command-level integration tests
    - dataset-level CLI failure aggregation before non-zero exit

key-files:
  created:
    - tests/integration/test_entsoe_live.py
  modified:
    - src/gridflow/cli.py
    - tests/conftest.py

key-decisions:
  - "Keep live tests opt-in at pytest collection time, even if ENTSOE_API_KEY is present locally."
  - "Continue processing remaining datasets after one CLI dataset fails, then exit non-zero with a per-dataset summary."
  - "Redact ENTSO-E securityToken query values from user-facing CLI failure text."

patterns-established:
  - "Live API tests should combine @pytest.mark.live with an explicit -m live opt-in and an ENTSOE_API_KEY skip gate."
  - "Command-level tests can copy config/sources.yaml into a temp cwd and rewrite settings.yaml paths to avoid normal data/log/DuckDB output."

requirements-completed: [LIVE-03]

duration: 35min
completed: 2026-05-02
---

# Phase H3 Plan 01: Live ENTSO-E Scaffolding Summary

**Opt-in live ENTSO-E scaffolding with isolated command paths and observable CLI dataset failures.**

## Performance

- **Started:** 2026-05-02T18:08:00+01:00
- **Completed:** 2026-05-02T18:43:00+01:00
- **Tasks:** 3
- **Files modified:** 3

## Accomplishments

- Added `tests/integration/test_entsoe_live.py` with non-live tests for marker registration, all-16 dataset alignment, credential redaction, temporary config isolation, and CLI failure propagation.
- Updated `ingest()` and `transform()` so dataset failures are collected, reported to stderr, and returned as `typer.Exit(1)` after all datasets are attempted.
- Added a pytest collection gate in `tests/conftest.py` so live-marked tests remain opt-in with `-m live`.

## Task Commits

1. **Tasks 1-3: Live scaffolding and CLI failure propagation** - `0ec7573` (test)

## Files Created/Modified

- `tests/integration/test_entsoe_live.py` - New live-test module with non-live scaffolding checks and live helpers.
- `src/gridflow/cli.py` - Adds per-dataset failure aggregation and redacted command failure summaries for ingest/transform.
- `tests/conftest.py` - Adds pytest live-test opt-in collection gate and small import cleanup required by lint.

## Decisions Made

- Used `CliRunner` and direct command-helper calls for command coverage so tests can run under a temporary cwd and temporary data root.
- Kept H3 ENTSO-E-only and avoided Elexon edits, matching the phase boundary.
- Preserved unrelated dirty logging/config changes by staging only H3-owned CLI hunks.

## Deviations from Plan

### Auto-fixed Issues

**1. LIVE-03 opt-in enforcement added in conftest**
- **Found during:** Task 1 verification against REQUIREMENTS.md and ROADMAP.md.
- **Issue:** Marker-only live tests would still run during default pytest if a developer had `ENTSOE_API_KEY` set.
- **Fix:** Added a pytest collection hook that skips `@pytest.mark.live` tests unless the marker expression opts into live tests.
- **Files modified:** `tests/conftest.py`
- **Verification:** `uv run --extra dev pytest tests/integration/test_entsoe_live.py -x -q` ran only non-live tests and skipped live-marked tests.
- **Committed in:** `0ec7573`

**Total deviations:** 1 auto-fixed requirement-alignment issue.

## Issues Encountered

- `uv run --extra dev ruff check src/gridflow/cli.py tests/conftest.py tests/integration/test_entsoe_live.py` is still blocked by pre-existing broad `src/gridflow/cli.py` lint debt outside H3 scope (`Optional[...]`, old `timezone.utc`, import ordering, long lines, and reset helper style). H3-owned test files pass ruff.

## Verification

- `uv run --extra dev pytest tests/integration/test_entsoe_live.py -m "not live" -x -q` - 6 passed, 36 deselected.
- `uv run --extra dev pytest tests/unit/test_cli_resolve_datasets.py -x -q` - 9 passed.
- `uv run --extra dev ruff check tests/conftest.py tests/integration/test_entsoe_live.py` - passed.

## User Setup Required

None for Plan 01. Plan 02 live execution requires `ENTSOE_API_KEY`.

## Next Phase Readiness

Plan 02 can run the all-dataset live connector, bronze-to-silver, and command tests once `ENTSOE_API_KEY` is available.

## Self-Check: PASSED

Plan 01's H3-owned non-live tests, opt-in gate, and CLI failure propagation are implemented and verified. The remaining full-file CLI lint debt is documented as pre-existing.

---
*Phase: H3-live-entsoe-test-suite*
*Completed: 2026-05-02*
