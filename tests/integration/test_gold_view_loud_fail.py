"""Integration tests for F15-D: gold-view loud-fail under strict mode.

PBI-05: _try_create_view and _register_gold_views must raise under pytest/dev
instead of silently swallowing view-creation errors.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import duckdb
import pytest

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.integration
def test_register_gold_views_raises_on_broken_sql_under_strict_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """F15-D / PBI-05: _register_gold_views raises when GRIDFLOW_ENV=dev.

    The real gold SQL files reference silver views (silver_gie_agsi_storage,
    etc.) that do not exist in a fresh in-memory connection.  DuckDB fails at CREATE VIEW
    time (not lazily).  Pre-F15-D: exception is swallowed; pytest.raises sees
    no raise → FAILS RED.  Post-F15-D: exception propagates → PASSES GREEN.
    """
    from gridflow.storage.duckdb import _register_gold_views

    monkeypatch.setenv("GRIDFLOW_ENV", "dev")

    con = duckdb.connect(":memory:")
    try:
        with pytest.raises(duckdb.Error):
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
        with pytest.raises(duckdb.Error):
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


@pytest.mark.integration
def test_register_gold_views_debug_logs_benign_absent_in_production_mode(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """B1.1 / F-14 / R3-c: benign-absent gold view registration logs DEBUG.

    With neither PYTEST_CURRENT_TEST nor GRIDFLOW_ENV=dev/test set, and no
    silver views registered, every gold SQL file fails to bind because its
    referenced silver relation does not exist yet -- the benign "not yet
    transformed" case (storage/duckdb.py:369-390). _register_gold_views must
    swallow (not raise) and log at DEBUG, mirroring _try_create_view's
    benign-absent convention (storage/duckdb.py:354-382), not WARNING.
    """
    from gridflow.storage.duckdb import _register_gold_views

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("GRIDFLOW_ENV", raising=False)

    con = duckdb.connect(":memory:")
    try:
        with caplog.at_level(logging.DEBUG, logger="gridflow.storage.duckdb"):
            _register_gold_views(con)
    finally:
        con.close()

    debug_messages = [
        record.getMessage() for record in caplog.records if record.levelno == logging.DEBUG
    ]
    warning_messages = [
        record.getMessage() for record in caplog.records if record.levelno == logging.WARNING
    ]
    assert any("uk_imbalance_context.sql" in msg for msg in debug_messages), (
        f"expected a DEBUG log mentioning uk_imbalance_context.sql, got: {debug_messages}"
    )
    assert not warning_messages, (
        f"expected no WARNING logs for the benign-absent case, got: {warning_messages}"
    )


@pytest.mark.integration
def test_register_gold_views_warns_on_real_error_in_production_mode(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """B1.1 / F-14 / R3-c: a genuine DDL/binder error still WARNs.

    A gold SQL file referencing an EXISTING silver table but a bogus column
    is a deterministic DDL/binder bug, not a "not yet transformed" absence
    (the CatalogException "does not exist" case _is_benign_absent_gold_view
    matches). _register_gold_views must swallow it (strict-mode raise path
    untouched) but log at WARNING so the failure stays visible in
    production, distinguishing it from the benign-absent DEBUG case.
    """
    from gridflow.storage import duckdb as duckdb_module

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("GRIDFLOW_ENV", raising=False)

    fake_module_dir = tmp_path / "storage"
    fake_module_dir.mkdir()
    monkeypatch.setattr(duckdb_module, "__file__", str(fake_module_dir / "duckdb.py"))

    views_dir = tmp_path / "gold" / "views"
    views_dir.mkdir(parents=True)
    (views_dir / "broken_real_error.sql").write_text(
        "CREATE OR REPLACE VIEW gold_broken AS SELECT bogus_col FROM silver_present"
    )

    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE silver_present (a INTEGER)")
    try:
        with caplog.at_level(logging.DEBUG, logger="gridflow.storage.duckdb"):
            duckdb_module._register_gold_views(con)
    finally:
        con.close()

    warning_messages = [
        record.getMessage() for record in caplog.records if record.levelno == logging.WARNING
    ]
    debug_messages = [
        record.getMessage() for record in caplog.records if record.levelno == logging.DEBUG
    ]
    assert any("broken_real_error.sql" in msg for msg in warning_messages), (
        f"expected a WARNING mentioning broken_real_error.sql, got: {warning_messages}"
    )
    assert not any("broken_real_error.sql" in msg for msg in debug_messages), (
        f"expected no DEBUG log for the real-error fixture, got: {debug_messages}"
    )
