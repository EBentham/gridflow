---
phase: H1-cli-all-positional-alias
plan: 01
subsystem: cli
tags: [typer, pytest, dataset-resolution, entsoe]

requires: []
provides:
  - Positional `all` dataset alias for shared CLI dataset resolution
  - Unit coverage for `all`, `ALL`, `All`, ENTSO-E 16-dataset expansion, `--all`, named dataset, and error paths
affects: [H2-entsoe-mocked-e2e-tests, H3-live-entsoe-test-suite, cli]

tech-stack:
  added: []
  patterns:
    - Shared helper normalization in `_resolve_datasets`
    - Direct helper unit tests using pytest fixtures and real config loading

key-files:
  created:
    - tests/unit/test_cli_resolve_datasets.py
  modified:
    - src/gridflow/cli.py

key-decisions:
  - "Treat positional `all` case-insensitively inside `_resolve_datasets` so all callers inherit the behavior."
  - "Use a real `load_settings()` ENTSO-E assertion to verify the roadmap's 16-dataset claim."

patterns-established:
  - "CLI dataset aliases are normalized in `_resolve_datasets`, not in each Typer command body."
  - "CLI helper tests may call `_resolve_datasets` directly and assert `typer.BadParameter` for direct exception paths."

requirements-completed: [CLI-01, CLI-02]

duration: 22min
completed: 2026-05-02
---

# Phase H1: Fix CLI `all` Positional Alias Summary

**Shared CLI dataset resolution now treats positional `all` as the existing `--all` flag.**

## Performance

- **Duration:** 22 min
- **Started:** 2026-05-02T17:42:00+01:00
- **Completed:** 2026-05-02T18:04:00+01:00
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments

- Added 9 focused unit tests for `_resolve_datasets`, including lowercase, uppercase, mixed-case, and ENTSO-E 16-dataset expansion.
- Changed `_resolve_datasets` in `src/gridflow/cli.py` so `dataset="all"` behaves exactly like `all_flag=True`.
- Preserved existing `--all`, named dataset, missing dataset, and invalid settings behaviors.

## Task Commits

1. **Task 1: Create RED tests for positional all alias** - `424ba6d` (test)
2. **Task 2: Fix _resolve_datasets condition** - `c4b2751` (fix)

**Plan metadata:** `935ad70` (docs(H1): complete phase plan)

## Files Created/Modified

- `tests/unit/test_cli_resolve_datasets.py` - Unit tests for positional `all`, existing `--all`, named dataset, and error paths.
- `src/gridflow/cli.py` - Single-condition update in `_resolve_datasets`.

## Decisions Made

- Normalize the alias in `_resolve_datasets` only, because `ingest`, `transform`, `backfill`, `export_csv`, and `pipeline` all route through that helper.
- Keep the comparison case-insensitive via `dataset.lower() == "all"` and guard against `None`.

## Deviations from Plan

None - implementation followed the plan. Verification found one pre-existing full-suite blocker outside H1 scope.

## Issues Encountered

- `uv run pytest -x -q` is blocked during collection by `ModuleNotFoundError: No module named 'gridflow.silver.elexon.agpt'`. This is unrelated to H1; `.planning/codebase/ARCHITECTURE.md` already records that `src/gridflow/silver/elexon/__init__.py` imports missing Elexon silver modules.
- The local `.venv` initially pointed at a Windows Store Python shim that was not executable. Rebuilt the environment with local Python 3.12 through `uv`.

## Verification

- RED check: `uv run --extra dev pytest tests/unit/test_cli_resolve_datasets.py -q` produced 4 failed alias tests and 5 passed existing-behavior tests.
- Focused GREEN check: `uv run --extra dev pytest tests/unit/test_cli_resolve_datasets.py -x -q` passed with 9 tests.
- Full suite: attempted with `uv run --extra dev pytest -x -q`; blocked by the pre-existing Elexon package import issue above.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

H1's CLI behavior is ready for H2/H3. The unrelated Elexon silver package import mismatch should be handled before relying on the global pytest suite as a milestone-level gate.

---
*Phase: H1-cli-all-positional-alias*
*Completed: 2026-05-02*
