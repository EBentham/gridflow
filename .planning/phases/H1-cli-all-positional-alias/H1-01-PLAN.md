---
phase: H1
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - src/gridflow/cli.py
  - tests/unit/test_cli_resolve_datasets.py
autonomous: true
requirements:
  - CLI-01
  - CLI-02

must_haves:
  truths:
    - "Running `gridflow pipeline entsoe all --last 24h` processes all ENTSO-E datasets without raising BadParameter"
    - "Running `gridflow ingest entsoe all` and `gridflow transform entsoe all` resolve to all datasets (via _resolve_datasets)"
    - "Existing --all flag behaviour is unchanged: passing all_flag=True still returns all datasets"
    - "Named dataset (e.g. `system_prices`) still returns a single-element list"
    - "No dataset and no --all flag still raises typer.BadParameter"
    - "All existing tests continue to pass"
  artifacts:
    - path: "src/gridflow/cli.py"
      provides: "Fixed _resolve_datasets helper — line 621 condition includes positional 'all' alias"
      contains: "all_flag or (dataset is not None and dataset.lower() == \"all\")"
    - path: "tests/unit/test_cli_resolve_datasets.py"
      provides: "Unit tests covering CLI-01 and CLI-02 via direct helper invocation, including ENTSO-E 16-dataset expansion"
      exports: ["TestAllPositionalAlias", "TestEntsoeDatasetExpansion", "TestAllFlagBehaviourUnchanged", "TestSpecificDataset", "TestErrorPaths"]
  key_links:
    - from: "tests/unit/test_cli_resolve_datasets.py"
      to: "src/gridflow/cli.py"
      via: "from gridflow.cli import _resolve_datasets"
      pattern: "_resolve_datasets"
    - from: "ingest(), transform(), backfill(), export_csv()"
      to: "_resolve_datasets()"
      via: "direct function call"
      pattern: "_resolve_datasets\\("
---

<objective>
Fix the CLI so that passing `all` as a positional dataset argument is treated identically
to passing the `--all` flag. This resolves the UX confusion documented in STATE.md where
users naturally type `gridflow pipeline entsoe all --last 24h` but receive a BadParameter
error because typer routes `all` as the positional `dataset` argument, not the `--all` flag.

Purpose: Satisfy CLI-01 and CLI-02 by making all four commands (ingest, transform,
pipeline, backfill) handle `all` as a positional alias through the single shared helper
`_resolve_datasets`, with no per-command changes required.

Output:
- `tests/unit/test_cli_resolve_datasets.py` — 9 unit tests covering the alias, ENTSO-E
  16-dataset expansion, flag, named dataset, and error paths
- `src/gridflow/cli.py` — single-line condition change in `_resolve_datasets` (line 621)
</objective>

<execution_context>
@/home/.claude/get-shit-done/workflows/execute-plan.md
@/home/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.planning/ROADMAP.md
@.planning/STATE.md

<interfaces>
<!-- Key interface the executor must reproduce exactly -->

From src/gridflow/cli.py, lines 609-626 (current state, verified 2026-05-02):
```python
def _resolve_datasets(
    source: str,
    dataset: str | None,
    all_flag: bool,
    settings: object,
) -> list[str]:
    """Resolve which datasets to process."""
    from gridflow.config.settings import GridflowConfig

    if not isinstance(settings, GridflowConfig):
        raise TypeError("Expected GridflowConfig")

    if all_flag:
        source_config = settings.get_source_config(source)
        return list(source_config.datasets.keys())
    if dataset:
        return [dataset]
    raise typer.BadParameter("Specify a dataset name or use --all")
```

From tests/conftest.py, lines 29-57 (sample_config fixture — available project-wide):
```python
@pytest.fixture
def sample_config(tmp_data_dir: Path) -> GridflowConfig:
    return GridflowConfig(
        pipeline=PipelineSettings(data_dir=tmp_data_dir, ...),
        quality=QualityConfig(),
        sources={
            "elexon": SourceConfig(
                base_url="https://data.elexon.co.uk/bmrs/api/v1",
                api_key="test-key",
                datasets={
                    "system_prices": DatasetConfig(
                        endpoint="/balancing/settlement/system-prices",
                        schedule="hourly",
                        max_query_days=1,
                    ),
                },
            ),
        },
    )
```
Note: `sample_config` has one Elexon dataset (`system_prices`). Tests asserting
"all datasets" will get `["system_prices"]` — this is correct and sufficient.
Plan completeness note: `sample_config` proves the helper behavior using the existing
pytest fixture, and the generated test file also includes one `load_settings()`-backed
ENTSO-E assertion so the roadmap's "all 16 datasets" claim is verified directly
against `config/sources.yaml`.
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Create test file for _resolve_datasets</name>
  <read_first>
    - tests/conftest.py (lines 29-57 — sample_config fixture construction; injected via pytest, no import needed)
    - tests/unit/test_time_utils.py (analog: class-per-behaviour pattern, exception path pattern)
    - src/gridflow/cli.py (lines 609-626 — current _resolve_datasets implementation to test against)
  </read_first>
  <files>tests/unit/test_cli_resolve_datasets.py</files>
  <action>
Create `tests/unit/test_cli_resolve_datasets.py` with the following exact content:

```python
"""Unit tests for _resolve_datasets CLI helper."""

from __future__ import annotations

import pytest
import typer

from gridflow.cli import _resolve_datasets
from gridflow.config.settings import load_settings


class TestAllPositionalAlias:
    def test_lowercase_all_treated_as_flag(self, sample_config):
        """Positional 'all' expands to all datasets for the source."""
        result = _resolve_datasets("elexon", "all", False, sample_config)
        assert result == list(sample_config.get_source_config("elexon").datasets.keys())

    def test_uppercase_all_treated_as_flag(self, sample_config):
        """Positional 'ALL' (case-insensitive) also expands to all datasets."""
        result = _resolve_datasets("elexon", "ALL", False, sample_config)
        assert result == list(sample_config.get_source_config("elexon").datasets.keys())

    def test_mixed_case_all_treated_as_flag(self, sample_config):
        """Positional 'All' (mixed case) also expands to all datasets."""
        result = _resolve_datasets("elexon", "All", False, sample_config)
        assert result == list(sample_config.get_source_config("elexon").datasets.keys())


class TestEntsoeDatasetExpansion:
    def test_entsoe_all_expands_to_all_16_configured_datasets(self):
        """ENTSO-E positional 'all' expands to all configured datasets."""
        settings = load_settings()
        result = _resolve_datasets("entsoe", "all", False, settings)
        expected = list(settings.get_source_config("entsoe").datasets.keys())

        assert len(expected) == 16
        assert result == expected


class TestAllFlagBehaviourUnchanged:
    def test_all_flag_true_expands_datasets(self, sample_config):
        """Existing --all flag behaviour is preserved."""
        result = _resolve_datasets("elexon", None, True, sample_config)
        assert result == list(sample_config.get_source_config("elexon").datasets.keys())

    def test_all_flag_true_with_dataset_string_expands(self, sample_config):
        """When all_flag=True, dataset string is irrelevant — all datasets returned."""
        result = _resolve_datasets("elexon", "system_prices", True, sample_config)
        assert result == list(sample_config.get_source_config("elexon").datasets.keys())


class TestSpecificDataset:
    def test_specific_dataset_returned_as_single_item_list(self, sample_config):
        """A named dataset returns a one-element list."""
        result = _resolve_datasets("elexon", "system_prices", False, sample_config)
        assert result == ["system_prices"]


class TestErrorPaths:
    def test_no_dataset_no_flag_raises_bad_parameter(self, sample_config):
        """No dataset and no --all flag raises typer.BadParameter."""
        with pytest.raises(typer.BadParameter):
            _resolve_datasets("elexon", None, False, sample_config)

    def test_invalid_settings_type_raises_type_error(self):
        """Non-GridflowConfig settings object raises TypeError."""
        with pytest.raises(TypeError):
            _resolve_datasets("elexon", "system_prices", False, object())
```

Key implementation notes:
- `sample_config` is injected by pytest from `tests/conftest.py` — no import needed.
- Direct calls to `_resolve_datasets` (no Click context) propagate `typer.BadParameter`
  as a normal Python exception — use `pytest.raises(typer.BadParameter)`, NOT
  `pytest.raises(SystemExit)`.
- The 4 positional alias tests MUST FAIL before Task 2 applies the fix (RED phase).
  Run: `uv run pytest tests/unit/test_cli_resolve_datasets.py -x -q` — expect 4 failures.
  If all 9 pass before the fix, something is wrong — stop and investigate.
  </action>
  <verify>
    <automated>grep -c "def test_" tests/unit/test_cli_resolve_datasets.py</automated>
  </verify>
  <acceptance_criteria>
    - `grep -c "def test_" tests/unit/test_cli_resolve_datasets.py` outputs `9`
    - `grep "from gridflow.cli import _resolve_datasets" tests/unit/test_cli_resolve_datasets.py` exits 0
    - `grep "from gridflow.config.settings import load_settings" tests/unit/test_cli_resolve_datasets.py` exits 0
    - `grep "from __future__ import annotations" tests/unit/test_cli_resolve_datasets.py` exits 0
    - `grep "class TestAllPositionalAlias" tests/unit/test_cli_resolve_datasets.py` exits 0
    - `grep "class TestEntsoeDatasetExpansion" tests/unit/test_cli_resolve_datasets.py` exits 0
    - `grep "class TestAllFlagBehaviourUnchanged" tests/unit/test_cli_resolve_datasets.py` exits 0
    - `grep "class TestSpecificDataset" tests/unit/test_cli_resolve_datasets.py` exits 0
    - `grep "class TestErrorPaths" tests/unit/test_cli_resolve_datasets.py` exits 0
    - Running `uv run pytest tests/unit/test_cli_resolve_datasets.py -x -q` shows 4 FAILED
      (the alias tests) and 5 passed — this is the expected RED state
  </acceptance_criteria>
  <done>Test file exists with 9 tests; 4 alias tests fail (RED) and 5 others pass.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Fix _resolve_datasets condition in cli.py (GREEN)</name>
  <read_first>
    - src/gridflow/cli.py (lines 609-626 — the function being modified; verify current state matches the interface block above before editing)
    - tests/unit/test_cli_resolve_datasets.py (the test file just created — understand what must pass)
  </read_first>
  <files>src/gridflow/cli.py</files>
  <behavior>
    - test_lowercase_all_treated_as_flag: _resolve_datasets("elexon", "all", False, cfg) == ["system_prices"]
    - test_uppercase_all_treated_as_flag: _resolve_datasets("elexon", "ALL", False, cfg) == ["system_prices"]
    - test_mixed_case_all_treated_as_flag: _resolve_datasets("elexon", "All", False, cfg) == ["system_prices"]
    - test_entsoe_all_expands_to_all_16_configured_datasets: _resolve_datasets("entsoe", "all", False, load_settings()) returns all 16 configured ENTSO-E datasets
    - test_all_flag_true_expands_datasets: _resolve_datasets("elexon", None, True, cfg) == ["system_prices"]
    - test_all_flag_true_with_dataset_string_expands: _resolve_datasets("elexon", "system_prices", True, cfg) == ["system_prices"]
    - test_specific_dataset_returned_as_single_item_list: _resolve_datasets("elexon", "system_prices", False, cfg) == ["system_prices"]
    - test_no_dataset_no_flag_raises_bad_parameter: raises typer.BadParameter
    - test_invalid_settings_type_raises_type_error: raises TypeError
  </behavior>
  <action>
In `src/gridflow/cli.py`, change line 621 only. No other lines in this function change.

BEFORE (line 621):
```python
    if all_flag:
```

AFTER (line 621):
```python
    if all_flag or (dataset is not None and dataset.lower() == "all"):
```

The full function after the change (lines 609-626) must read exactly:
```python
def _resolve_datasets(
    source: str,
    dataset: str | None,
    all_flag: bool,
    settings: object,
) -> list[str]:
    """Resolve which datasets to process."""
    from gridflow.config.settings import GridflowConfig

    if not isinstance(settings, GridflowConfig):
        raise TypeError("Expected GridflowConfig")

    if all_flag or (dataset is not None and dataset.lower() == "all"):
        source_config = settings.get_source_config(source)
        return list(source_config.datasets.keys())
    if dataset:
        return [dataset]
    raise typer.BadParameter("Specify a dataset name or use --all")
```

Do NOT change any other function, import, or line. This is a single-condition change.

Why `dataset is not None and dataset.lower() == "all"` (not just `dataset == "all"`):
- Guards against calling `.lower()` on None
- Handles ALL, All, aLl etc. case-insensitively
- Satisfies CLI-01 (pipeline) and CLI-02 (ingest, transform) through this single helper
  because all four commands (ingest, transform, backfill, pipeline-delegated calls)
  funnel through _resolve_datasets.
  </action>
  <verify>
    <automated>uv run pytest tests/unit/test_cli_resolve_datasets.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - `grep 'all_flag or (dataset is not None and dataset.lower() == "all")' src/gridflow/cli.py` exits 0
    - `uv run pytest tests/unit/test_cli_resolve_datasets.py -x -q` exits 0 with 9 passed, 0 failed
    - `uv run pytest -x -q` exits 0 (full suite green — no regressions)
  </acceptance_criteria>
  <done>All 9 unit tests pass; full test suite green; the condition change is the only diff in cli.py.</done>
</task>

</tasks>

<verification>
Phase-level checks before marking H1 complete:

1. `grep 'all_flag or (dataset is not None and dataset.lower() == "all")' src/gridflow/cli.py` — must exit 0
2. `grep -c "def test_" tests/unit/test_cli_resolve_datasets.py` — must output `9`
3. `uv run pytest tests/unit/test_cli_resolve_datasets.py -x -q` — must exit 0, 9 passed
4. `uv run pytest -x -q` — full suite must be green (no regressions)
5. `git diff --name-only` — only `src/gridflow/cli.py` and `tests/unit/test_cli_resolve_datasets.py` changed
</verification>

<success_criteria>
- `gridflow pipeline entsoe all --last 24h` no longer raises BadParameter (positional `all` resolves to all datasets)
- `gridflow ingest entsoe all` and `gridflow transform entsoe all` also resolve correctly (same helper)
- Existing `--all` flag continues to work unchanged
- Named datasets continue to return a single-element list
- 9 unit tests pass; full test suite green
- Single-line change in cli.py; no per-command changes
</success_criteria>

<output>
After completion, create `.planning/phases/H1-cli-all-positional-alias/H1-01-SUMMARY.md`
using the summary template at @$HOME/.claude/get-shit-done/templates/summary.md.
</output>
