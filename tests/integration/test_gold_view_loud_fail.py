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


def _gold_view_logs(
    con: duckdb.DuckDBPyConnection,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
    data_dir: Path | None = None,
) -> dict[int, list[str]]:
    """Register the gold views in production mode, after the silver views under
    ``data_dir`` when given; return the gold-view log lines by level."""
    from gridflow.storage.duckdb import _register_gold_views, _register_views

    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)
    monkeypatch.delenv("GRIDFLOW_ENV", raising=False)
    monkeypatch.setattr("gridflow.storage.duckdb._failed_over_parquet", set())
    with caplog.at_level(logging.DEBUG, logger="gridflow.storage.duckdb"):
        if data_dir is not None:
            _register_views(con, data_dir / "silver", data_dir / "gold")
        _register_gold_views(con)
    by_level: dict[int, list[str]] = {}
    for record in caplog.records:
        if record.getMessage().startswith("Gold view"):
            by_level.setdefault(record.levelno, []).append(record.getMessage())
    return by_level


@pytest.mark.integration
def test_production_mode_logs_one_info_line_for_a_gold_view_without_silver(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A fresh catalogue has no silver, so every gold view fails to bind. That is
    the expected state before a backfill: one INFO line per view naming the
    dataset it waits for, and no WARNING.
    """
    con = duckdb.connect(":memory:")
    try:
        logs = _gold_view_logs(con, caplog, monkeypatch)
    finally:
        con.close()

    assert logs.get(logging.WARNING, []) == []
    assert sorted(logs[logging.INFO]) == [
        "Gold view eu_gas_storage.sql registers after gie_agsi/storage is backfilled",
        "Gold view gb_day_ahead_benchmark.sql registers after elexon/mid is backfilled",
        "Gold view uk_imbalance_context.sql registers after elexon/system_prices is backfilled",
    ]


@pytest.mark.integration
def test_production_mode_warns_when_a_gold_view_fails_for_another_reason(
    caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """System prices silver exists but its ``_latest`` view could not be built, so
    uk_imbalance_context is not waiting for a backfill: it keeps the WARNING with
    DuckDB's message.
    """
    con = duckdb.connect(":memory:")
    try:
        con.execute("CREATE VIEW silver_elexon_system_prices AS SELECT 1 AS settlement_period")
        logs = _gold_view_logs(con, caplog, monkeypatch)
    finally:
        con.close()

    (warning,) = logs[logging.WARNING]
    assert warning.startswith("Gold view registration failed for uk_imbalance_context.sql")
    assert "silver_elexon_system_prices_latest does not exist" in warning


@pytest.mark.integration
def test_production_mode_warns_when_a_gold_views_silver_will_not_read(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Elexon MID silver exists but is not valid Parquet, so its view fails to
    register. gb_day_ahead_benchmark then lacks silver_elexon_mid, but a backfill
    would not fix that: it keeps the WARNING.
    """
    mid = tmp_path / "silver" / "elexon" / "mid" / "year=2024" / "month=01"
    mid.mkdir(parents=True)
    (mid / "mid_20240115.parquet").write_bytes(b"not parquet")

    con = duckdb.connect(":memory:")
    try:
        logs = _gold_view_logs(con, caplog, monkeypatch, data_dir=tmp_path)
    finally:
        con.close()

    (warning,) = logs[logging.WARNING]
    assert warning.startswith("Gold view registration failed for gb_day_ahead_benchmark.sql")
    assert "silver_elexon_mid does not exist" in warning


@pytest.mark.integration
@pytest.mark.parametrize(
    "query",
    [
        "SELECT * FROM silver_elxon_mid",  # misspelt source
        "SELECT * FROM silver_elexon_mid_latest",  # mid has no _latest view
        "SELECT no_such_function(1)",
    ],
)
def test_a_relation_no_backfill_registers_is_not_awaited(query: str) -> None:
    from gridflow.storage.duckdb import _awaited_silver

    con = duckdb.connect(":memory:")
    try:
        with pytest.raises(duckdb.CatalogException) as caught:
            con.execute(query)
        assert _awaited_silver(con, caught.value) is None
    finally:
        con.close()
