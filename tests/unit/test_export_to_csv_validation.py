"""CH1-03 (CH-SEC-04): export_to_csv input validation.

`scripts/export_to_csv.py` interpolates `--start` / `--end` / `--limit` and the
output path into SQL / COPY strings. These tests pin the pure validation
helpers that reject:

  - a malformed `--start` / `--end` (not an ISO date);
  - a non-positive `--limit` (0 or negative);
  - an output path that escapes the requested directory (e.g. `../`).

RED before CH1-03: the helpers do not exist yet (AttributeError on import).
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest


def _load_export_script():
    """Load scripts/export_to_csv.py as a module for direct helper testing."""
    script_path = Path(__file__).parents[2] / "scripts" / "export_to_csv.py"
    spec = importlib.util.spec_from_file_location("gridflow_export_to_csv_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_validate_dates_accepts_iso() -> None:
    mod = _load_export_script()
    # Valid ISO dates round-trip without raising.
    mod._validate_dates("2024-01-15", "2024-01-16")
    mod._validate_dates(None, None)
    mod._validate_dates("2024-01-15", None)


def test_validate_dates_rejects_malformed() -> None:
    mod = _load_export_script()
    with pytest.raises(ValueError):
        mod._validate_dates("2024-13-99", None)
    with pytest.raises(ValueError):
        mod._validate_dates(None, "not-a-date")
    # Classic SQL-injection-shaped value must be rejected as a date.
    with pytest.raises(ValueError):
        mod._validate_dates("2024-01-15' OR '1'='1", None)


def test_validate_limit_rejects_non_positive() -> None:
    mod = _load_export_script()
    assert mod._validate_limit(None) is None
    assert mod._validate_limit(10) == 10
    with pytest.raises(ValueError):
        mod._validate_limit(0)
    with pytest.raises(ValueError):
        mod._validate_limit(-5)


def test_safe_output_path_rejects_escape(tmp_path: Path) -> None:
    mod = _load_export_script()
    out_dir = tmp_path / "exports"
    # A normal view name resolves inside the output dir.
    safe = mod._safe_output_path(out_dir, "silver_system_prices")
    assert safe.parent.resolve() == out_dir.resolve()
    # A `../` escape must be refused.
    with pytest.raises(ValueError):
        mod._safe_output_path(out_dir, "../../etc/evil")


def test_safe_output_path_rejects_single_quote_in_dir(tmp_path: Path) -> None:
    """E-1: an --output-dir containing a single quote is refused.

    The COPY target is a SQL string literal; a `'` in the path would break out
    of the literal and inject SQL. `_safe_output_path` rejects the path as
    defense-in-depth before the COPY site is ever reached.

    RED before E-1: `_safe_output_path` only checks `../` containment, so a
    `'`-containing dir that stays inside `out_dir` passes.
    """
    evil_dir = tmp_path / "ex'); CREATE TABLE pwned(x INT); --"
    with pytest.raises(ValueError):
        mod = _load_export_script()
        mod._safe_output_path(evil_dir, "silver_system_prices")
