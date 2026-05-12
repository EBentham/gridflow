---
phase: I4
plan: 01
type: execute
wave: 1
depends_on: []
requirements:
  - ELEXON-CLI-01
  - ELEXON-CLI-02
  - ELEXON-CLI-03
  - ELEXON-DOC-01
  - ELEXON-DOC-02
files_modified:
  - src/gridflow/config/settings.py
  - tests/integration/test_elexon_cli_live_smoke.py
  - tests/unit/test_cli_resolve_datasets.py
  - .planning/phases/I4-elexon-cli-backfill-live-smoke-tests-and-milestone-close-out-docs/I4-LIVE-COMMANDS.md
  - .planning/phases/I4-elexon-cli-backfill-live-smoke-tests-and-milestone-close-out-docs/I4-01-SUMMARY.md
  - .planning/phases/I4-elexon-cli-backfill-live-smoke-tests-and-milestone-close-out-docs/I4-VERIFICATION.md
  - .planning/ROADMAP.md
  - .planning/REQUIREMENTS.md
  - .planning/STATE.md
autonomous: true
---

# I4-01 Plan - Elexon CLI/Backfill Live Smoke Tests and Milestone Close-Out Docs

## Objective

<objective>

Add opt-in live CLI smoke tests proving `gridflow pipeline`, `gridflow ingest`,
`gridflow transform`, and `gridflow backfill` can run public Elexon datasets
through isolated temp bronze and silver paths, then close out the v0.4 Elexon
validation milestone with command documentation, troubleshooting notes, and
requirements traceability.

</objective>

## Execution Context

Read these before editing:

- `.planning/phases/I4-elexon-cli-backfill-live-smoke-tests-and-milestone-close-out-docs/I4-RESEARCH.md`
- `.planning/phases/I4-elexon-cli-backfill-live-smoke-tests-and-milestone-close-out-docs/I4-VALIDATION.md`
- `.planning/phases/I3-elexon-live-api-to-silver-test-suite/I3-01-SUMMARY.md`
- `.planning/phases/I3-elexon-live-api-to-silver-test-suite/I3-01-PLAN.md`
- `.planning/phases/I2-elexon-mocked-request-shape-and-fixture-backed-bronze-to-silver-tests/I2-01-SUMMARY.md`
- `.planning/phases/I1-elexon-inventory-test-scaffolding/I1-01-SUMMARY.md`
- `.planning/REQUIREMENTS.md`
- `.planning/ROADMAP.md`
- `.planning/STATE.md`
- `CLAUDE.md`
- `config/sources.yaml`
- `src/gridflow/cli.py`
- `src/gridflow/config/settings.py`
- `tests/conftest.py`
- `tests/integration/test_elexon_live_e2e.py`
- `tests/integration/test_elexon_mocked_e2e.py`
- `tests/unit/test_cli_resolve_datasets.py`

## Must Haves

- The plan must satisfy `ELEXON-CLI-01`, `ELEXON-CLI-02`, `ELEXON-CLI-03`,
  `ELEXON-DOC-01`, and `ELEXON-DOC-02`.
- Live CLI smoke tests must be opt-in with `@pytest.mark.live` and excluded from
  normal `-m "not live"` runs.
- Tests must use isolated temp data, DuckDB, and log paths via environment
  overrides, never the project `data/` or `logs/` directories.
- Tests must invoke the CLI app with real user-facing command arguments, not only
  direct connector or transformer calls.
- `pipeline`, separate `ingest` plus `transform`, and `backfill` must each have
  live smoke coverage.
- The backfill smoke must cover at least one path-date dataset, one publish/from-to
  dataset, and one no-param/reference dataset, or document and fix any current CLI
  limitation that blocks that requirement.
- Command docs must capture exact live commands, chosen dataset windows, expected
  skips, troubleshooting notes, and requirement traceability.
- Real dataset failures must surface as non-zero CLI exits with per-dataset output;
  tests must not turn such failures into silent passes.

## Tasks

<tasks>

### 1. Add Isolated CLI Live Smoke Test Harness

<read_first>

- `src/gridflow/cli.py`
- `src/gridflow/config/settings.py`
- `tests/conftest.py`
- `tests/integration/test_elexon_live_e2e.py`
- `tests/unit/test_cli_resolve_datasets.py`

</read_first>

<action>

Create `tests/integration/test_elexon_cli_live_smoke.py` with:

- `from __future__ import annotations`
- imports for `dataclass`, `Path`, `pytest`, and `CliRunner` from
  `typer.testing`
- import `app` from `gridflow.cli`
- a module-level `runner = CliRunner()`
- a small `CliSmokePaths` dataclass with `data_dir`, `duckdb_path`, and `log_dir`
- a helper `_isolated_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> CliSmokePaths`
  that sets `GRIDFLOW_DATA_DIR`, `GRIDFLOW_DUCKDB_PATH`, and `GRIDFLOW_LOG_DIR`
  to subpaths under `tmp_path`
- helpers `_assert_under_tmp(path: Path, tmp_path: Path)`, `_assert_bronze_created(paths, dataset)`,
  `_assert_silver_created(paths, dataset)`, and `_invoke_cli(args: list[str])`
  that include command output in assertion failures
- constants for a narrow deterministic live window, starting with
  `START = "2026-02-01"` and `END = "2026-02-02"`

Do not call the live Elexon API from helper setup. Do not read `ELEXON_API_KEY`.

</action>

<verify>

<automated>uv run --extra dev ruff check tests/integration/test_elexon_cli_live_smoke.py</automated>

</verify>

<done>

- `tests/integration/test_elexon_cli_live_smoke.py` exists.
- The file imports `CliRunner` and `gridflow.cli.app`.
- The file sets `GRIDFLOW_DATA_DIR`, `GRIDFLOW_DUCKDB_PATH`, and `GRIDFLOW_LOG_DIR`.
- The file contains no `ELEXON_API_KEY` reference.
- Ruff exits 0 for the new file.

</done>

### 2. Add Live `pipeline`, `ingest`, and `transform` Smoke Tests

<read_first>

- `tests/integration/test_elexon_cli_live_smoke.py`
- `src/gridflow/cli.py`
- `tests/integration/test_elexon_live_e2e.py`

</read_first>

<action>

In `tests/integration/test_elexon_cli_live_smoke.py`, add:

1. `test_live_pipeline_elexon_system_prices_creates_bronze_and_silver`
   marked `@pytest.mark.live`.
   - use `_isolated_env(tmp_path, monkeypatch)`;
   - invoke `["pipeline", "elexon", "system_prices", "--start", START, "--end", END]`;
   - assert exit code 0;
   - assert output includes pipeline, ingest, transform, and completion signals;
   - assert bronze and silver outputs exist under the temp data root.

2. `test_live_ingest_then_transform_elexon_freq_creates_outputs`
   marked `@pytest.mark.live`.
   - use `_isolated_env(tmp_path, monkeypatch)`;
   - invoke `["ingest", "elexon", "freq", "--start", START, "--end", END]`;
   - assert exit code 0 and bronze output exists;
   - invoke `["transform", "elexon", "freq", "--start", START, "--end", END]`;
   - assert exit code 0 and silver output exists;
   - assert the outputs stay under the temp data root.

If `freq` is temporarily empty for the selected window, use `boal` as the publish
datetime fallback and document the selected dataset in `I4-LIVE-COMMANDS.md`.

</action>

<verify>

<automated>uv run --extra dev pytest tests/integration/test_elexon_cli_live_smoke.py -m live -q -rs</automated>

</verify>

<done>

- Pipeline live smoke covers `system_prices`.
- Separate ingest and transform live smoke covers a publish/from-to dataset.
- Both tests assert bronze and silver outputs under pytest temp roots.
- CLI failures include command output in assertion failures.
- `ELEXON-CLI-01` and `ELEXON-CLI-02` are covered by automated live assertions.

</done>

### 3. Add Live Backfill Smoke Coverage Across Request Styles

<read_first>

- `src/gridflow/cli.py`
- `src/gridflow/connectors/elexon/endpoints.py`
- `tests/integration/test_elexon_cli_live_smoke.py`
- `tests/integration/test_elexon_live_e2e.py`

</read_first>

<action>

Add a parametrized live test named
`test_live_backfill_elexon_curated_dataset_creates_outputs` with cases:

| dataset | purpose |
| --- | --- |
| `system_prices` | path-date backfill |
| `freq` or `boal` | publish/from-to datetime backfill |
| `bmunits_reference` | no-param/reference backfill |

For each case:

- call `_isolated_env(tmp_path, monkeypatch)`;
- invoke `["backfill", "elexon", dataset, "--start", START, "--end", END, "--chunk-days", "1"]`;
- assert exit code 0;
- assert output includes `Backfilling elexon/{dataset}` and `Backfill complete`;
- assert bronze output exists under the temp data root;
- assert silver output exists under the temp data root.

If the current CLI cannot transform `bmunits_reference` through `backfill` because
reference data is not date-partitioned, prefer a narrow production fix in the CLI
or transformer path over weakening the test. If a production fix is out of scope,
stop and record the blocker rather than marking `ELEXON-CLI-03` complete.

</action>

<verify>

<automated>uv run --extra dev pytest tests/integration/test_elexon_cli_live_smoke.py -m live -q -rs</automated>

</verify>

<done>

- Backfill live smoke covers a path-date dataset.
- Backfill live smoke covers a publish/from-to dataset.
- Backfill live smoke covers a no-param/reference dataset.
- Each case verifies bronze and silver output under the temp data root.
- `ELEXON-CLI-03` is covered by automated live assertions.

</done>

### 4. Add Close-Out Command Documentation and Non-Live Sentinel

<read_first>

- `.planning/REQUIREMENTS.md`
- `.planning/ROADMAP.md`
- `.planning/phases/I4-elexon-cli-backfill-live-smoke-tests-and-milestone-close-out-docs/I4-RESEARCH.md`
- `docs/CLI_CHEAT_SHEET.md`

</read_first>

<action>

Create `.planning/phases/I4-elexon-cli-backfill-live-smoke-tests-and-milestone-close-out-docs/I4-LIVE-COMMANDS.md`
with:

- exact live commands for `pipeline`, `ingest`, `transform`, and `backfill`;
- selected dataset windows and why each dataset was chosen;
- expected skip/deferred outcomes, if any;
- troubleshooting notes for public Elexon service errors, empty windows, temp path
  isolation, and live marker selection;
- a requirements traceability table mapping each I4 requirement to the test/doc
  evidence.

Add a non-live test in `tests/integration/test_elexon_cli_live_smoke.py` named
`test_i4_live_command_documentation_covers_cli_closeout_requirements` that reads
`I4-LIVE-COMMANDS.md` and asserts it mentions:

- `pipeline`
- `ingest`
- `transform`
- `backfill`
- `system_prices`
- `bmunits_reference`
- all five I4 requirement IDs

</action>

<verify>

<automated>uv run --extra dev pytest tests/integration/test_elexon_cli_live_smoke.py -m "not live" -q</automated>

</verify>

<done>

- `I4-LIVE-COMMANDS.md` exists and covers commands, dataset windows, skips, and troubleshooting.
- The non-live sentinel passes under `-m "not live"`.
- `ELEXON-DOC-01` and `ELEXON-DOC-02` are covered by phase artifacts and tests.

</done>

### 5. Run Final Gates and Close Out v0.4 Traceability

<read_first>

- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/PROJECT.md`
- `.planning/MILESTONES.md`
- `.planning/phases/I1-elexon-inventory-test-scaffolding/I1-01-SUMMARY.md`
- `.planning/phases/I2-elexon-mocked-request-shape-and-fixture-backed-bronze-to-silver-tests/I2-01-SUMMARY.md`
- `.planning/phases/I3-elexon-live-api-to-silver-test-suite/I3-01-SUMMARY.md`

</read_first>

<action>

Run:

```powershell
uv run --extra dev ruff check tests/integration/test_elexon_cli_live_smoke.py tests/integration/test_elexon_live_e2e.py tests/integration/test_elexon_mocked_e2e.py tests/unit/test_cli_resolve_datasets.py
uv run --extra dev pytest tests/integration/test_elexon_cli_live_smoke.py -m "not live" -q
uv run --extra dev pytest tests/integration/test_elexon_cli_live_smoke.py -m live -q -rs
uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py tests/unit/test_cli_resolve_datasets.py -m "not live" -q
```

After gates pass:

- create `I4-01-SUMMARY.md` with command outcomes, created artifacts, live skips,
  and requirement mapping;
- create `I4-VERIFICATION.md` with goal-backward verification for all five I4
  requirements;
- update `.planning/REQUIREMENTS.md` to mark the I4 requirements complete;
- update `.planning/ROADMAP.md` to mark Phase I4 and its plan complete;
- update `.planning/STATE.md` to mark v0.4 complete or ready for milestone audit,
  depending on the local GSD convention visible in prior milestones;
- update `.planning/PROJECT.md` only if it has an active status section tracking
  v0.4 Elexon validation outcomes.

</action>

<verify>

<automated>uv run --extra dev ruff check tests/integration/test_elexon_cli_live_smoke.py tests/integration/test_elexon_live_e2e.py tests/integration/test_elexon_mocked_e2e.py tests/unit/test_cli_resolve_datasets.py</automated>
<automated>uv run --extra dev pytest tests/integration/test_elexon_cli_live_smoke.py -m "not live" -q</automated>
<automated>uv run --extra dev pytest tests/integration/test_elexon_cli_live_smoke.py -m live -q -rs</automated>
<automated>uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py tests/unit/test_cli_resolve_datasets.py -m "not live" -q</automated>

</verify>

<done>

- Ruff exits 0.
- Non-live CLI smoke sentinel passes.
- Live CLI smoke tests pass or report explicit legitimate skips without false passes.
- Regression tests pass.
- I4 summary and verification artifacts exist.
- Roadmap, requirements, and state traceability show I4 ownership and completion.

</done>

</tasks>

## Threat Model

<threat_model>

| ID | Threat | Severity | Mitigation |
| --- | --- | --- | --- |
| T-I4-01 | Live CLI smoke tests pollute normal project `data/`, `logs/`, or DuckDB catalog. | High | Set `GRIDFLOW_DATA_DIR`, `GRIDFLOW_LOG_DIR`, and `GRIDFLOW_DUCKDB_PATH` to pytest temp paths and assert all artifacts are under `tmp_path`. |
| T-I4-02 | Live tests accidentally run in default CI/local test runs. | High | Mark real network tests with `@pytest.mark.live` and verify `-m "not live"` passes with only the docs sentinel. |
| T-I4-03 | CLI smoke tests bypass the CLI and miss argument/exit-code behavior. | High | Use Typer `CliRunner` with real command argument vectors and assert exit codes/output. |
| T-I4-04 | Empty or unavailable public API windows become false green results. | High | Treat HTTP active-dataset failures as test failures; allow skips only with explicit dataset/window/stage diagnostics. |
| T-I4-05 | Backfill hides command failures because nested ingest/transform exits are swallowed. | Medium | Assert exit codes, output, and actual bronze/silver artifacts for each curated backfill dataset. |
| T-I4-06 | Close-out docs drift from test reality. | Medium | Add a non-live sentinel that checks required command/docs/requirement tokens in `I4-LIVE-COMMANDS.md`; final summary records exact outcomes. |

</threat_model>

## Verification

<verification>

Run:

```powershell
uv run --extra dev ruff check tests/integration/test_elexon_cli_live_smoke.py tests/integration/test_elexon_live_e2e.py tests/integration/test_elexon_mocked_e2e.py tests/unit/test_cli_resolve_datasets.py
uv run --extra dev pytest tests/integration/test_elexon_cli_live_smoke.py -m "not live" -q
uv run --extra dev pytest tests/integration/test_elexon_cli_live_smoke.py -m live -q -rs
uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py tests/unit/test_cli_resolve_datasets.py -m "not live" -q
```

</verification>

## Success Criteria

<success_criteria>

- `ELEXON-CLI-01`: A live `pipeline elexon system_prices` smoke test runs against
  isolated temp paths and verifies bronze and silver outputs.
- `ELEXON-CLI-02`: Live `ingest elexon <publish-datetime dataset>` followed by
  `transform elexon <same dataset>` runs against isolated temp paths, verifies
  outputs, and surfaces real failures as non-zero exits.
- `ELEXON-CLI-03`: Live `backfill elexon <dataset>` smoke tests cover path-date,
  publish/from-to, and no-param/reference Elexon datasets without writing to the
  normal project data directory.
- `ELEXON-DOC-01`: `I4-LIVE-COMMANDS.md` documents live commands, selected windows,
  expected skips, and troubleshooting notes.
- `ELEXON-DOC-02`: I4 summary, verification, requirements, roadmap, and state
  artifacts preserve 100% traceability for the v0.4 Elexon validation milestone.
- Existing I1-I3 Elexon mocked/live regression gates remain green.

</success_criteria>
