---
phase: L4-agsi-opt-in-live-api-to-silver-tests-cli-smoke-tests-close-out-verification
plan: L4-01
subsystem: live-validation
tags:
  - gie
  - agsi
  - live
  - cli
  - bronze
  - silver
requires:
  - phase: L3
    provides: AGSI active-family silver transformers and fixture-backed bronze-to-silver coverage
provides:
  - Credentialed AGSI live API-to-silver tests for storage scopes and unavailability classification
  - Isolated AGSI live CLI smoke tests for pipeline, ingest/transform, and backfill
  - L4 live command documentation and milestone close-out traceability
affects:
  - v0.7 GIE AGSI Gas Storage Validation close-out
tech-stack:
  added: []
  patterns:
    - Credential-gated pytest live tests
    - Temp-root CLI smoke tests with GRIDFLOW_* overrides
    - Live RawResponse -> BronzeWriter -> registered transformer -> parquet assertions
key-files:
  created:
    - tests/integration/test_gie_agsi_live_e2e.py
    - tests/integration/test_gie_agsi_cli_live_smoke.py
    - .planning/phases/L4-agsi-opt-in-live-api-to-silver-tests-cli-smoke-tests-close-out-verification/L4-LIVE-COMMANDS.md
    - .planning/phases/L4-agsi-opt-in-live-api-to-silver-tests-cli-smoke-tests-close-out-verification/L4-USER-SETUP.md
  modified:
    - .planning/PROJECT.md
    - .planning/REQUIREMENTS.md
    - .planning/ROADMAP.md
    - .planning/STATE.md
key-decisions:
  - "Keep representative AGSI live tests narrow; full inventory expected-count validation is explicit via GRIDFLOW_AGSI_FULL_INVENTORY_LIVE=1."
  - "Use 2026-05-02 as the backfill end boundary for a one-day 2026-05-01 smoke because the CLI backfill loop treats end as exclusive."
  - "Classify AGSI unavailability live no-data/API ambiguity explicitly instead of silently passing or failing without context."
requirements-completed:
  - AGSI-11
  - AGSI-12
duration: 25 min
completed: 2026-05-04
---

# Phase L4 Plan L4-01: AGSI Opt-In Live API-To-Silver Tests, CLI Smoke Tests, And Close-Out Verification Summary

**Credentialed GIE AGSI live API responses now flow through bronze and silver, and the user-facing CLI smoke paths run under isolated temp roots.**

## Performance

- **Duration:** 25 min
- **Started:** 2026-05-04T17:16:00+01:00
- **Completed:** 2026-05-04T17:41:00+01:00
- **Tasks:** 3
- **Files modified:** 7

## Accomplishments

- Added `tests/integration/test_gie_agsi_live_e2e.py` with credential-gated live coverage for aggregate, country, company, and facility `storage_reports` scopes.
- Added live unavailability classification and a deliberately explicit full-inventory expected-count gate that stays representative unless `GRIDFLOW_AGSI_FULL_INVENTORY_LIVE=1` is set.
- Added `tests/integration/test_gie_agsi_cli_live_smoke.py` covering `pipeline`, separate `ingest`/`transform`, and `backfill` for `gie_agsi/storage_reports` under temp `GRIDFLOW_*` paths.
- Added `L4-LIVE-COMMANDS.md` with API key setup, isolated environment variables, command examples, pytest gates, rate-limit guidance, unavailability ambiguity, and ALSI follow-up.
- Updated requirements, roadmap, state, and project context to mark L4 and v0.7 AGSI validation complete.

## Task Commits

Per-task commits were not created because `gsd-sdk` is unavailable in this runtime and the worktree already contained overlapping uncommitted L2/L3 planning and implementation changes. The files are present in the working tree and verified by the commands below.

## Files Created/Modified

- `tests/integration/test_gie_agsi_live_e2e.py` - Live API-to-bronze-to-silver coverage for AGSI storage scopes and unavailability classification.
- `tests/integration/test_gie_agsi_cli_live_smoke.py` - Live CLI smoke tests with isolated temp data, DuckDB, and log paths.
- `.planning/phases/L4-agsi-opt-in-live-api-to-silver-tests-cli-smoke-tests-close-out-verification/L4-LIVE-COMMANDS.md` - Manual and automated live command reference.
- `.planning/phases/L4-agsi-opt-in-live-api-to-silver-tests-cli-smoke-tests-close-out-verification/L4-USER-SETUP.md` - GIE live-gate environment setup reference.
- `.planning/phases/L4-agsi-opt-in-live-api-to-silver-tests-cli-smoke-tests-close-out-verification/L4-VERIFICATION.md` - Goal-backward phase verification.
- `.planning/REQUIREMENTS.md` - Marks `AGSI-11` and `AGSI-12` complete.
- `.planning/ROADMAP.md` - Marks Phase L4 complete.
- `.planning/STATE.md` and `.planning/PROJECT.md` - Record v0.7 completion status and validated requirements.

## Decisions Made

- The full AGSI inventory gate is explicit and opt-in because the provider documents a 60 calls/minute limit.
- CLI backfill uses `2026-05-01` to `2026-05-02` for a one-day smoke because the current backfill loop uses an exclusive end boundary.
- `unavailability` remains a live-classified active endpoint with explicit skip diagnostics for documented ambiguity or empty windows.

## Deviations from Plan

**[Rule 1 - Bug] Backfill same-day smoke produced no chunks** - Found during: Task 2 live verification.

- **Issue:** The first backfill smoke used `--start 2026-05-01 --end 2026-05-01`; the CLI backfill loop uses `while current < end_dt`, so the command exited successfully with zero chunks and no bronze/silver output.
- **Fix:** Updated the backfill live smoke and command docs to use `--end 2026-05-02` for a one-day exclusive boundary.
- **Files modified:** `tests/integration/test_gie_agsi_cli_live_smoke.py`, `L4-LIVE-COMMANDS.md`
- **Verification:** L4 live gate passed after the adjustment.

**Total deviations:** 1 auto-fixed. **Impact:** Positive; the smoke now catches real output creation rather than accepting an empty backfill run.

## Issues Encountered

None remaining.

## Verification

```powershell
uv run --extra dev ruff check tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py
```

Result: passed.

```powershell
uv run --extra dev pytest tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py -m "not live" -q
```

Result: 1 passed, 9 deselected.

```powershell
uv run --extra dev pytest tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py -m live -q -rs
```

Result: 8 passed, 1 skipped, 1 deselected, 1 warning. The skip was the intentional representative-only full-inventory gate.

```powershell
uv run --extra dev ruff check tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py
```

Result: passed.

```powershell
uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q
```

Result: 63 passed.

```powershell
uv run --extra dev pytest -m "not live" -q
```

Result: 984 passed, 253 deselected, 1 dependency deprecation warning.

## User Setup Required

Set `GIE_API_KEY` before running live AGSI gates. `L4-LIVE-COMMANDS.md` documents the commands and isolated `GRIDFLOW_*` environment setup.

## Next Phase Readiness

v0.7 GIE AGSI Gas Storage Validation is complete and ready for milestone close-out. ALSI LNG remains a follow-up connector-confidence candidate.

## Self-Check: PASSED

- `AGSI-11` is covered by credentialed live API-to-silver tests for representative AGSI storage scopes.
- `AGSI-12` is covered by isolated CLI smoke tests for pipeline, ingest/transform, and backfill.
- Live tests are opt-in, credential-gated, and avoid normal project data paths.
- L1-L3 AGSI non-live regression coverage remains green.

---
*Phase: L4-agsi-opt-in-live-api-to-silver-tests-cli-smoke-tests-close-out-verification*
*Completed: 2026-05-04*
