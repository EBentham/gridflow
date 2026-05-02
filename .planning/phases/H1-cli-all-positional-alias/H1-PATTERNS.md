# Phase H1: Fix CLI `all` Positional Alias - Pattern Map

**Mapped:** 2026-05-02
**Files analyzed:** 2
**Analogs found:** 2 / 2

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `src/gridflow/cli.py` | utility (helper function) | request-response | `src/gridflow/cli.py` — `build()` inline alias block (lines 159-164) | exact |
| `tests/unit/test_cli_resolve_datasets.py` | test | request-response | `tests/unit/test_time_utils.py` | exact |

## Pattern Assignments

### `src/gridflow/cli.py` — modify `_resolve_datasets()` (lines 609-626)

**Analog:** Inline alias block in `build()` command (same file, lines 159-164), which
demonstrates the project pattern for handling `dataset == "all"` via an `or` condition
on the `all_flag` guard.

**Current function** (`src/gridflow/cli.py`, lines 609-626):
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

**Target change — single line modification** (line 621):

Change:
```python
    if all_flag:
```

To:
```python
    if all_flag or (dataset is not None and dataset.lower() == "all"):
```

Full function after change:
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

**Why `dataset is not None and dataset.lower() == "all"` rather than just `dataset == "all"`:**
Guards against `dataset=None` before calling `.lower()`, and handles `"ALL"` / `"All"` case-insensitively.

**Callers (all covered automatically by fixing the helper):**
- `ingest()` — line 36
- `transform()` — line 98
- `backfill()` — line 208
- `export_csv()` — line 267
- `pipeline()` delegates to `ingest`/`transform` which each call `_resolve_datasets` independently

---

### `tests/unit/test_cli_resolve_datasets.py` (new file, test, request-response)

**Analog:** `tests/unit/test_time_utils.py`

**Module docstring pattern** (line 1):
```python
"""Unit tests for settlement period / UTC conversion utilities."""
```

**Imports pattern** (lines 1-12 of `test_time_utils.py`):
```python
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from gridflow.utils.time import (
    date_range,
    parse_lookback,
    ...
)
```

**Class-per-behaviour pattern** (lines 15-105 of `test_time_utils.py`):
```python
class TestSettlementPeriodToUTC:
    def test_sp1_winter(self):
        """SP1 on a winter day starts at 00:00 UTC."""
        result = settlement_period_to_utc(date(2024, 1, 15), 1)
        assert result == datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc)
    ...

class TestParseLookback:
    def test_invalid_unit(self):
        with pytest.raises(ValueError):
            parse_lookback("10x")
```

**`sample_config` fixture injection** (from `tests/conftest.py`, lines 29-57):
The fixture is available project-wide. Tests receive it via standard pytest fixture
injection — no import needed.

```python
@pytest.fixture
def sample_config(tmp_data_dir: Path) -> GridflowConfig:
    return GridflowConfig(
        ...
        sources={
            "elexon": SourceConfig(
                ...
                datasets={
                    "system_prices": DatasetConfig(...),
                },
            ),
        },
    )
```

Note: `sample_config` contains only one Elexon dataset (`system_prices`). The `all`
alias tests will assert against a single-element list. This is sufficient to prove the
logic; a multi-dataset fixture is not required by this phase.

**Error path pattern** (lines 84-86 of `test_time_utils.py`):
```python
    def test_invalid_unit(self):
        with pytest.raises(ValueError):
            parse_lookback("10x")
```
For `_resolve_datasets`, use `typer.BadParameter` (not `SystemExit`) when calling
the function directly without a Click context.

**Full new test file to create** (`tests/unit/test_cli_resolve_datasets.py`):
```python
"""Unit tests for _resolve_datasets CLI helper."""

from __future__ import annotations

import pytest
import typer

from gridflow.cli import _resolve_datasets


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

---

## Shared Patterns

### `from __future__ import annotations`
**Source:** Every module in `src/gridflow/` (project-wide convention from `CLAUDE.md`)
**Apply to:** `tests/unit/test_cli_resolve_datasets.py`
```python
from __future__ import annotations
```

### Google-style docstrings on public functions
**Source:** `src/gridflow/cli.py` line 615 (`"""Resolve which datasets to process."""`)
**Apply to:** No public function changes; `_resolve_datasets` docstring unchanged.

### pytest class grouping
**Source:** `tests/unit/test_time_utils.py` lines 15-105
**Apply to:** `tests/unit/test_cli_resolve_datasets.py` — group by behaviour class
(`TestAllPositionalAlias`, `TestAllFlagBehaviourUnchanged`, `TestSpecificDataset`,
`TestErrorPaths`).

### Exception scope: `typer.BadParameter` vs `SystemExit`
**Source:** RESEARCH.md, Pitfall 3
**Apply to:** `test_no_dataset_no_flag_raises_bad_parameter`
When `_resolve_datasets` is called directly (no Click context), `typer.BadParameter`
propagates as-is. Use `pytest.raises(typer.BadParameter)`, not `pytest.raises(SystemExit)`.

---

## No Analog Found

None. Both files have strong analogs in the codebase.

---

## Metadata

**Analog search scope:** `src/gridflow/cli.py`, `tests/unit/`, `tests/conftest.py`
**Files scanned:** 4 (`cli.py`, `conftest.py`, `test_time_utils.py`, `test_schemas.py`)
**Pattern extraction date:** 2026-05-02
