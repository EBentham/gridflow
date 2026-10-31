"""scripts/extract_all.py, which builds the front-end's chart data from silver."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

if TYPE_CHECKING:
    from types import ModuleType

    import pytest


def _load_script() -> ModuleType:
    script_path = Path(__file__).parents[2] / "scripts" / "extract_all.py"
    spec = importlib.util.spec_from_file_location("gridflow_extract_all_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_rows_differing_only_by_settlement_period_are_not_collapsed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Two periods of one date with equal values are two rows of settlement
    data; with settlement_period left out of the comparison, one was dropped."""
    mod = _load_script()
    db_path = tmp_path / "catalogue.duckdb"
    con = duckdb.connect(str(db_path))
    con.execute(
        "CREATE TABLE silver_elexon_fake AS SELECT * FROM (VALUES "
        "(DATE '2026-08-01', 1, 42.0), (DATE '2026-08-01', 2, 42.0)) "
        "AS t(settlement_date, settlement_period, price)"
    )
    con.close()
    monkeypatch.setattr(mod, "DB_PATH", db_path)
    monkeypatch.setattr(mod, "OUT_ROOT", tmp_path / "out")
    monkeypatch.setattr(mod, "STATE", tmp_path / "out" / "_run_state.jsonl")
    monkeypatch.setattr(mod, "load_catalogue", lambda scope=None: [("elexon", "fake", {})])

    (record,) = mod.export_all(full_history=True)

    assert record["status"] == "OK"
    assert record["rows"] == 2
    assert record["duplicate_rows_removed"] == 0
