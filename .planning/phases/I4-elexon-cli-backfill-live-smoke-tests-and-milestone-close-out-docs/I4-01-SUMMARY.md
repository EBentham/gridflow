---
phase: I4
plan: 01
subsystem: testing
tags: [elexon, cli, live-api, backfill, pytest]

requires:
  - phase: I3
    provides: Opt-in live Elexon API-to-bronze-to-silver confidence baseline
provides:
  - Opt-in live CLI smoke tests for pipeline, ingest, transform, and backfill
  - Environment-isolated CLI execution through GRIDFLOW_DATA_DIR, GRIDFLOW_DUCKDB_PATH, and GRIDFLOW_LOG_DIR
  - I4 live command and troubleshooting documentation
affects: [elexon-live-tests, cli, config-settings, v0.4-closeout]

tech-stack:
  added: []
  patterns:
    - Typer CliRunner live smoke tests with pytest temp roots
    - YAML plus environment config precedence regression coverage
    - Non-live documentation sentinel for live command artifacts

key-files:
  created:
    - tests/integration/test_elexon_cli_live_smoke.py
    - .planning/phases/I4-elexon-cli-backfill-live-smoke-tests-and-milestone-close-out-docs/I4-LIVE-COMMANDS.md
    - .planning/phases/I4-elexon-cli-backfill-live-smoke-tests-and-milestone-close-out-docs/I4-01-SUMMARY.md
    - .planning/phases/I4-elexon-cli-backfill-live-smoke-tests-and-milestone-close-out-docs/I4-VERIFICATION.md
  modified:
    - src/gridflow/config/settings.py
    - tests/unit/test_cli_resolve_datasets.py
    - .planning/ROADMAP.md
    - .planning/REQUIREMENTS.md
    - .planning/STATE.md
    - .planning/PROJECT.md

key-decisions:
  - "GRIDFLOW_* environment variables must override YAML pipeline paths so CLI smoke tests and manual live checks can isolate data, DuckDB, and logs."
  - "Use Typer CliRunner with real command argument vectors for CLI smoke coverage, keeping live network tests opt-in through pytest markers."
  - "Use `system_prices`, `freq`, and `bmunits_reference` as the curated I4 smoke subset covering path-date, publish/from-to, and no-param/reference command paths."

requirements-completed:
  - ELEXON-CLI-01
  - ELEXON-CLI-02
  - ELEXON-CLI-03
  - ELEXON-DOC-01
  - ELEXON-DOC-02

duration: 1h
completed: 2026-05-04
---

# Phase I4 Plan 01: Elexon CLI/Backfill Live Smoke Tests and Close-Out Docs Summary

**Opt-in live CLI smoke tests now prove Elexon `pipeline`, `ingest`, `transform`, and `backfill` run through isolated temp bronze and silver paths.**

## Performance

- **Duration:** 1h
- **Started:** 2026-05-04T00:40:00+01:00
- **Completed:** 2026-05-04T01:40:00+01:00
- **Tasks:** 5
- **Files modified:** 10

## Accomplishments

- Added `tests/integration/test_elexon_cli_live_smoke.py` with live CLI coverage for `pipeline`, separate `ingest`/`transform`, and `backfill`.
- Added a non-live documentation sentinel so `-m "not live"` proves I4 close-out docs exist without calling the public API.
- Fixed settings precedence so `GRIDFLOW_DATA_DIR`, `GRIDFLOW_DUCKDB_PATH`, and `GRIDFLOW_LOG_DIR` override YAML defaults during CLI runs.
- Added unit regression coverage proving those `GRIDFLOW_*` overrides win over `config/settings.yaml`.
- Added `I4-LIVE-COMMANDS.md` with exact commands, selected windows, expected skips, troubleshooting, and requirement mapping.
- Updated requirements, roadmap, state, and project context to mark I4 and v0.4 Elexon validation complete.

## Task Commits

1. **I4-01 CLI live smoke coverage** - `629596c` (`test(I4-01): add elexon cli live smoke coverage`)

## Deviations from Plan

**1. [Rule 3 - Blocking] YAML pipeline paths overrode CLI test isolation**
- **Found during:** Task 2/3 live verification.
- **Issue:** The initial live CLI smoke tests passed at the CLI level but wrote to `data/` and `data/gridflow.duckdb` because `load_settings()` passed YAML values as `PipelineSettings` init kwargs, which took priority over `GRIDFLOW_*` environment variables.
- **Fix:** Updated `load_settings()` to overlay `GRIDFLOW_*` environment values onto YAML before constructing `PipelineSettings`, and added a unit regression test.
- **Files modified:** `src/gridflow/config/settings.py`, `tests/unit/test_cli_resolve_datasets.py`.
- **Verification:** Targeted ruff, non-live tests, live CLI smoke tests, and I1-I4 regression gates passed.
- **Committed in:** `629596c`.

**Total deviations:** 1 auto-fixed blocking issue.
**Impact:** The phase is stronger than planned: CLI isolation is now enforced by production config behavior and tested directly.

## Issues Encountered

- The first live CLI smoke run wrote ignored local artifacts under the normal `data/` tree before the config precedence bug was fixed. Subsequent live runs verified temp-root isolation.
- Live Elexon public API calls for the selected datasets all succeeded. No live skips were needed.

## Verification

- `uv run --extra dev ruff check tests/integration/test_elexon_cli_live_smoke.py` - passed.
- `uv run --extra dev pytest tests/integration/test_elexon_cli_live_smoke.py -m "not live" -q` - passed, 1 passed and 5 deselected.
- `uv run --extra dev pytest tests/integration/test_elexon_cli_live_smoke.py -m live -q -rs` - passed, 5 passed and 1 deselected.
- `uv run --extra dev ruff check src/gridflow/config/settings.py tests/integration/test_elexon_cli_live_smoke.py tests/integration/test_elexon_live_e2e.py tests/integration/test_elexon_mocked_e2e.py tests/unit/test_cli_resolve_datasets.py` - passed.
- `uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py tests/unit/test_cli_resolve_datasets.py -m "not live" -q` - passed, 81 tests.

## User Setup Required

None. The live Elexon CLI smoke suite uses the public no-key Insights API and remains opt-in with `-m live`.

## Next Phase Readiness

All v0.4 Elexon validation phases are complete. The milestone is ready for `$gsd-complete-milestone`.

## Self-Check: PASSED

- Plan tasks completed.
- Requirements `ELEXON-CLI-01`, `ELEXON-CLI-02`, `ELEXON-CLI-03`, `ELEXON-DOC-01`, and `ELEXON-DOC-02` satisfied.
- Live, non-live, lint, and regression verification passed.
- No human verification required.

---
*Phase: I4*
*Completed: 2026-05-04*
