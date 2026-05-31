"""Integration tests for F15-D: gold-view loud-fail under strict mode.

PBI-05: _try_create_view and _register_gold_views must raise under pytest/dev
instead of silently swallowing view-creation errors.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest


@pytest.mark.integration
def test_register_gold_views_raises_on_broken_sql_under_strict_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F15-D / PBI-05: _register_gold_views raises when GRIDFLOW_ENV=dev.

    The real gold SQL files reference silver views (silver_storage, etc.) that
    do not exist in a fresh in-memory connection.  DuckDB fails at CREATE VIEW
    time (not lazily).  Pre-F15-D: exception is swallowed; pytest.raises sees
    no raise → FAILS RED.  Post-F15-D: exception propagates → PASSES GREEN.
    """
    from gridflow.storage.duckdb import _register_gold_views

    monkeypatch.setenv("GRIDFLOW_ENV", "dev")

    con = duckdb.connect(":memory:")
    try:
        with pytest.raises(Exception):
            _register_gold_views(con)
    finally:
        con.close()


@pytest.mark.integration
def test_try_create_view_raises_under_pytest(tmp_path: Path) -> None:
    """F15-D / PBI-05: _try_create_view raises when PYTEST_CURRENT_TEST is set.

    pytest auto-sets PYTEST_CURRENT_TEST for every test, so strict mode is on
    during all test runs.  Pre-F15-D: exception swallowed → FAILS RED.
    Post-F15-D: exception propagates → PASSES GREEN.
    """
    from gridflow.storage.duckdb import _try_create_view

    con = duckdb.connect(":memory:")
    try:
        with pytest.raises(Exception):
            _try_create_view(
                con,
                "silver_x",
                str(tmp_path / "does" / "not" / "exist" / "*.parquet"),
            )
    finally:
        con.close()


@pytest.mark.integration
def test_production_mode_swallows(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """F15-D / PBI-05: legacy swallow behaviour preserved in production mode.

    When neither PYTEST_CURRENT_TEST nor GRIDFLOW_ENV=dev/test is set,
    _try_create_view must NOT raise — logs debug and continues.  Passes both
    before and after F15-D because the production swallow path is unchanged.
    """
    from gridflow.storage.duckdb import _try_create_view

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("GRIDFLOW_ENV", raising=False)

    con = duckdb.connect(":memory:")
    try:
        _try_create_view(
            con,
            "silver_x",
            str(tmp_path / "missing" / "*.parquet"),
        )
    finally:
        con.close()
