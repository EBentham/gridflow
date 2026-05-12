"""Integration tests for F15-C: DuckDB view refresh after CLI writes.

PBI-04: after transform/build, silver/gold DuckDB views must be queryable
without a separate `gridflow init`.

RED before F15-C (transform and build do not call refresh_views):
  - Test 1: silver_fuelhh view absent after transform exits.
  - Test 2: gold_system_marginal_price view absent after build exits.

GREEN after F15-C (transform and build both call refresh_views at the end).

Test 3 is always green: refresh_views idempotency does not require F15-C.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
import polars as pl
import pytest
from typer.testing import CliRunner

from gridflow.cli import app

FIXTURES = Path(__file__).parent.parent / "fixtures" / "elexon"
runner = CliRunner()


def _isolated_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    data_dir = tmp_path / "data"
    db_path = tmp_path / "gridflow.duckdb"
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(tmp_path / "logs"))
    # F15-D: gold SQL views reference silver tables absent from test tmpdirs.
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    return data_dir, db_path


def _write_fuelhh_bronze(data_dir: Path) -> None:
    target = date(2024, 1, 15)
    bronze_dir = (
        data_dir / "bronze" / "elexon" / "fuelhh"
        / str(target.year) / f"{target.month:02d}" / f"{target.day:02d}"
    )
    bronze_dir.mkdir(parents=True, exist_ok=True)
    payload = json.loads((FIXTURES / "fuelhh_response.json").read_text())
    (bronze_dir / "raw_test.json").write_text(json.dumps(payload))


@pytest.mark.integration
def test_transform_makes_silver_view_queryable_immediately(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F15-C / PBI-04: silver_fuelhh view returns rows the moment transform exits.

    Pre-F15-C: init_catalogue at transform-start sees an empty data_dir; the
    newly-written silver parquet is invisible to a subsequent DuckDB connection
    because no refresh_views is called.  FAILS RED (CatalogException or 0 rows).

    Post-F15-C: transform calls refresh_views after writing; the view is
    registered and a fresh connection can query it immediately.  PASSES GREEN.
    """
    data_dir, db_path = _isolated_env(tmp_path, monkeypatch)
    _write_fuelhh_bronze(data_dir)

    result = runner.invoke(
        app,
        [
            "transform", "elexon", "fuelhh",
            "--start", "2024-01-15",
            "--end", "2024-01-15",
        ],
    )
    assert result.exit_code == 0, result.output

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute("SELECT COUNT(*) FROM silver_fuelhh").fetchone()
    finally:
        con.close()
    assert rows[0] > 0, "silver_fuelhh view should be queryable immediately post-transform"


@pytest.mark.integration
def test_build_refreshes_gold_views(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """F15-C / PBI-04: gold parquet written by build is queryable immediately.

    The fake builder writes one gold parquet row; post-F15-C, build calls
    refresh_views so the gold_system_marginal_price view is registered.

    Pre-F15-C: DuckDB raises CatalogException on the query.  FAILS RED.
    Post-F15-C: view exists and returns rows.  PASSES GREEN.
    """
    data_dir, db_path = _isolated_env(tmp_path, monkeypatch)
    gold_dir = data_dir / "gold" / "system_marginal_price"

    class _FakeBuilder:
        def __init__(self, data_dir: Path) -> None:
            pass

        def run(self, start: date, end: date) -> int:
            part = gold_dir / "year=2026" / "month=05"
            part.mkdir(parents=True)
            pl.DataFrame({"price_gbp": [100.0]}).write_parquet(part / "part.parquet")
            return 1

    monkeypatch.setattr(
        "gridflow.gold.system_marginal_price.SystemMarginalPriceBuilder",
        _FakeBuilder,
    )

    result = runner.invoke(
        app,
        [
            "build", "system_marginal_price",
            "--start", "2026-05-01",
            "--end", "2026-05-01",
        ],
    )
    assert result.exit_code == 0, result.output

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = con.execute("SELECT COUNT(*) FROM gold_system_marginal_price").fetchone()
    finally:
        con.close()
    assert rows[0] > 0, "gold_system_marginal_price view should be queryable post-build"


def test_refresh_views_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """refresh_views can be called twice without error (CREATE OR REPLACE VIEW)."""
    _, db_path = _isolated_env(tmp_path, monkeypatch)
    data = tmp_path / "data"
    data.mkdir(exist_ok=True)

    from gridflow.storage.duckdb import init_catalogue, refresh_views

    init_catalogue(db_path, data)
    refresh_views(db_path, data)
    refresh_views(db_path, data)
