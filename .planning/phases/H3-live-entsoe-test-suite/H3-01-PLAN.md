---
phase: H3
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - src/gridflow/cli.py
  - tests/integration/test_entsoe_live.py
autonomous: true
requirements:
  - LIVE-01
  - LIVE-03

must_haves:
  truths:
    - "`tests/integration/test_entsoe_live.py` defines live-test helpers without making live API calls during non-live test runs."
    - "Live tests skip only when `ENTSOE_API_KEY` is absent; helper diagnostics never print the secret value."
    - "CLI ingest/transform failures are no longer silently hidden behind zero-exit commands when any dataset fails."
    - "Command-level tests can run under a temporary config/data root so live output does not touch normal `./data`, `./logs`, or DuckDB files."
    - "Failure summaries include dataset names and enough detail to distinguish fetch, bronze, transform, and command failures."
  artifacts:
    - path: "tests/integration/test_entsoe_live.py"
      provides: "Opt-in live-test scaffolding, skip behavior tests, diagnostics helpers, and command isolation helpers"
      exports: ["TestEntsoeLivePrerequisites", "TestEntsoeCliFailurePropagation"]
    - path: "src/gridflow/cli.py"
      provides: "Hard-fail CLI behavior when per-dataset ingest/transform work fails"
      exports: ["ingest", "transform", "pipeline"]
  key_links:
    - from: "tests/integration/test_entsoe_live.py"
      to: "src/gridflow/cli.py"
      via: "Typer command invocation or command helper call"
      pattern: "CliRunner or direct command function"
    - from: "tests/integration/test_entsoe_live.py"
      to: "src/gridflow/config/settings.py"
      via: "temporary config/data root isolation"
      pattern: "load_settings"
---

<objective>
Add the H3 live-test foundation and make command failures observable.

Purpose: before hitting the real ENTSO-E API, ensure live tests are safely gated, have
diagnostic helpers, run under isolated data paths, and can catch command failures through
non-zero CLI exits.

Output:
- `tests/integration/test_entsoe_live.py` exists with non-live tests for skip behavior,
  diagnostic redaction, and temporary command isolation helpers.
- `src/gridflow/cli.py` propagates ingest/transform dataset failures as non-zero command exits.
</objective>

<execution_context>
@.planning/PROJECT.md
@.planning/ROADMAP.md
@.planning/REQUIREMENTS.md
@.planning/STATE.md
@.planning/phases/H3-live-entsoe-test-suite/H3-CONTEXT.md
@.planning/phases/H3-live-entsoe-test-suite/H3-RESEARCH.md
@.planning/phases/H3-live-entsoe-test-suite/H3-PATTERNS.md
@tests/integration/test_entsoe_mocked_e2e.py
@src/gridflow/cli.py
@src/gridflow/config/settings.py
</execution_context>

<context>
H3 is ENTSO-E-only. Do not add Elexon live coverage in this phase.

Current CLI behavior catches dataset failures and continues. H3 must make failures visible to
command-level tests: once a user opts into a command, failed datasets should produce a non-zero
exit and useful per-dataset diagnostics.

Preserve unrelated dirty worktree changes. `src/gridflow/cli.py` may already contain user edits;
read it immediately before editing and make the smallest compatible change.
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Create RED tests for live gating and CLI failure observability</name>
  <read_first>
    - tests/integration/test_entsoe_mocked_e2e.py
    - tests/conftest.py
    - src/gridflow/cli.py
    - src/gridflow/config/settings.py
  </read_first>
  <files>tests/integration/test_entsoe_live.py</files>
  <action>
Create `tests/integration/test_entsoe_live.py`.

Include:
- `from __future__ import annotations`
- standard-library imports for `os`, `shutil`, `datetime.UTC`, `datetime.date`,
  `datetime.datetime`, `pathlib.Path`, and any typing needed.
- pytest imports.
- first-party imports for `DOC_TYPES`, `load_settings`, and CLI command surfaces as needed.

Define helpers:
- `_has_entsoe_api_key() -> bool` using environment presence only.
- `requires_entsoe_api_key = pytest.mark.skipif(not _has_entsoe_api_key(), reason="ENTSOE_API_KEY is required for live ENTSO-E tests")`.
- `_redact(value: str) -> str` that never returns the raw key.
- `_copy_config_with_temp_paths(tmp_path: Path, tmp_data_dir: Path) -> Path` that creates
  a temporary `config/` directory with copied `sources.yaml` and rewritten `settings.yaml`
  pointing data/log/DuckDB paths at temp locations.

Add non-live tests:
- `TestEntsoeLivePrerequisites.test_live_marker_registered` asserts `live` is present in pytest config or documents marker presence via `pyproject.toml`.
- `TestEntsoeLivePrerequisites.test_entsoe_config_and_doc_types_cover_same_16_datasets` mirrors H2 and asserts config/DOC_TYPES alignment.
- `TestEntsoeLivePrerequisites.test_api_key_redaction_never_returns_secret` sets a fake key and asserts helper diagnostics do not contain it.
- `TestEntsoeLivePrerequisites.test_temp_config_points_pipeline_paths_at_tmp_dir` verifies the temporary config helper isolates data/log/DuckDB paths.

Add a RED command failure-propagation test using monkeypatches or small fake command dependencies:
- Force one ENTSO-E dataset operation to fail.
- Assert the command exits non-zero or raises `typer.Exit(1)`.
- Assert output or exception context identifies the failed dataset.

Do not make any live HTTP calls in these non-live tests.
  </action>
  <verify>
    <automated>uv run --extra dev pytest tests/integration/test_entsoe_live.py -m "not live" -x -q</automated>
  </verify>
  <acceptance_criteria>
    - Non-live tests run without `ENTSOE_API_KEY`
    - Live marker and all-16 dataset alignment are checked
    - Secret redaction is tested
    - Temporary config/data isolation is tested
    - CLI failure propagation test initially fails against current hidden-failure behavior
  </acceptance_criteria>
  <done>RED tests capture H3 live gating and command failure observability.</done>
</task>

<task type="auto">
  <name>Task 2: Propagate per-dataset CLI failures as command failures</name>
  <read_first>
    - src/gridflow/cli.py
    - tests/integration/test_entsoe_live.py
  </read_first>
  <files>src/gridflow/cli.py</files>
  <action>
Update `ingest()` and `transform()` so they collect failed dataset names and error messages.

Required behavior:
- Continue processing remaining datasets so all attempted failures are reported.
- After the loop, if any dataset failed, print a concise failure summary to stderr and raise
  `typer.Exit(1)`.
- Preserve successful output for datasets that pass.
- Ensure `pipeline()` propagates the non-zero exit from `ingest()` or `transform()` instead
  of printing a false-success pipeline completion.
- Do not print API keys or full request URLs containing `securityToken`.

Keep the change minimal and compatible with existing command signatures.
  </action>
  <verify>
    <automated>uv run --extra dev pytest tests/integration/test_entsoe_live.py -m "not live" -x -q</automated>
  </verify>
  <acceptance_criteria>
    - Failure-propagation test passes
    - Existing `_resolve_datasets` behavior remains unchanged
    - Successful multi-dataset runs still process all datasets
    - Failed multi-dataset runs exit non-zero after reporting failed datasets
  </acceptance_criteria>
  <done>CLI failures are observable by command-level tests.</done>
</task>

<task type="auto">
  <name>Task 3: Add diagnostic helpers for live fetch and transform assertions</name>
  <read_first>
    - tests/integration/test_entsoe_mocked_e2e.py
    - src/gridflow/connectors/entsoe/client.py
    - src/gridflow/silver/registry.py
  </read_first>
  <files>tests/integration/test_entsoe_live.py</files>
  <action>
Extend the live test module with helpers that will be used by Plan 02:

- `_assert_live_responses(dataset: str, responses: list[RawResponse]) -> None`
  - fail if response list is empty;
  - fail if any body is empty;
  - include dataset name, HTTP status, content type, and redacted request context.

- `_write_live_responses_to_bronze(tmp_data_dir: Path, dataset: str, responses: list[RawResponse], target_date: date) -> list[Path]`
  - write every response through `BronzeWriter`;
  - ensure `data_date` is set or normalized for partitioning;
  - assert XML and `.meta.json` files exist.

- `_assert_silver_output(tmp_data_dir: Path, dataset: str, target_date: date, rows: int) -> None`
  - assert rows > 0;
  - assert silver parquet exists;
  - read parquet and assert row count and `data_provider == "entsoe"` when present.

- `_diagnostic_context(dataset: str, stage: str, exc: Exception | None = None) -> str`
  - centralize readable failure messages.

Do not call the live API in this task.
  </action>
  <verify>
    <automated>uv run --extra dev pytest tests/integration/test_entsoe_live.py -m "not live" -x -q</automated>
  </verify>
  <acceptance_criteria>
    - Helpers are unit-tested or exercised by non-live tests where possible
    - Diagnostics include dataset and stage
    - Diagnostics redact credentials
    - Helpers reuse `BronzeWriter` and real parquet reads
  </acceptance_criteria>
  <done>Live diagnostic and assertion helpers are ready for all-dataset tests.</done>
</task>

</tasks>

<verification>

Before Plan 01 is marked complete:

1. `uv run --extra dev pytest tests/integration/test_entsoe_live.py -m "not live" -x -q` passes without `ENTSOE_API_KEY`.
2. `uv run --extra dev pytest tests/unit/test_cli_resolve_datasets.py -x -q` still passes.
3. `uv run --extra dev ruff check src/gridflow/cli.py tests/integration/test_entsoe_live.py` passes.
4. `git diff --name-only` for this plan is limited to `src/gridflow/cli.py`, `tests/integration/test_entsoe_live.py`, and H3 summary docs.

</verification>

<success_criteria>

- LIVE-03 groundwork is present: no API key means live tests skip, not fail.
- Once command failures happen, CLI exits non-zero and surfaces dataset-level diagnostics.
- Live test helpers can safely isolate data output and redact credentials.
- Plan 02 can add real API calls without needing more scaffolding.

</success_criteria>
