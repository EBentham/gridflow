"""CH2-02 / CH-COR-02: the `quality` command must honour `settings.quality`.

These are CLI-level tests on purpose. ``check_null_rate`` and
``check_time_series_gaps`` already accept ``max_rate`` / ``expected_freq_minutes``
arguments (the existing unit tests pass them explicitly), so a unit test against
``checks.py`` would prove nothing — it would be green before the fix. The actual
bug lives in ``cli.py``: the ``quality`` command called those functions without
threading the configured thresholds through. So the fail-first assertion drives
the real command and reads the persisted ``quality_reports.passed`` outcome.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

import duckdb
import polars as pl
from typer.testing import CliRunner

from gridflow.cli import app
from gridflow.config import settings as settings_module
from gridflow.config.settings import (
    GridflowConfig,
    PipelineSettings,
    QualityConfig,
)
from gridflow.storage.parquet import write_parquet

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    import pytest

runner = CliRunner()


def _make_settings(tmp_path: Path, quality: QualityConfig) -> GridflowConfig:
    """Build a GridflowConfig pointed at an isolated tmp data dir."""
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    return GridflowConfig(
        pipeline=PipelineSettings(
            data_dir=data_dir,
            log_dir=tmp_path / "logs",
            duckdb_path=tmp_path / "gridflow.duckdb",
        ),
        quality=quality,
        sources={},
    )


def _passed_for_check(duckdb_path: Path, check_name: str) -> list[bool]:
    """Return the persisted ``passed`` flags for a given check from DuckDB."""
    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        rows = con.execute(
            "SELECT passed FROM quality_reports WHERE check_name = ?",
            [check_name],
        ).fetchall()
    finally:
        con.close()
    return [r[0] for r in rows]


# --- (A1) null_rate_threshold is consulted ----------------------------------


def _write_silver_with_null_rate(data_dir: Path) -> None:
    """A silver frame whose single numeric column is 20% null.

    20% null fails the default 0.05 threshold but passes a configured 0.5.
    """
    values = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, None, None]
    df = pl.DataFrame({"value": values}, schema={"value": pl.Float64})
    write_parquet(df, data_dir / "silver" / "testsrc" / "testds" / "part-0.parquet")


def test_null_rate_threshold_default_flags_20pct_nulls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With the default 0.05 threshold, a 20%-null column must FAIL.

    This is the baseline for the flip test below; it also pins current behaviour.
    """
    cfg = _make_settings(tmp_path, QualityConfig(null_rate_threshold=0.05))
    _write_silver_with_null_rate(cfg.pipeline.data_dir)
    monkeypatch.setattr(settings_module, "load_settings", lambda: cfg)

    result = runner.invoke(app, ["quality", "--all"])
    assert result.exit_code == 0, result.stdout

    passed = _passed_for_check(cfg.pipeline.duckdb_path, "null_rate")
    assert passed and all(p is False for p in passed), "20% nulls must fail at 0.05"


def test_null_rate_threshold_config_flips_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A configured null_rate_threshold of 0.5 must PASS the same 20%-null column.

    RED before the fix: ``cli.py`` ignored ``settings.quality`` and always used
    the hardcoded 0.05 default, so the outcome stayed FAIL regardless of config.
    """
    cfg = _make_settings(tmp_path, QualityConfig(null_rate_threshold=0.5))
    _write_silver_with_null_rate(cfg.pipeline.data_dir)
    monkeypatch.setattr(settings_module, "load_settings", lambda: cfg)

    result = runner.invoke(app, ["quality", "--all"])
    assert result.exit_code == 0, result.stdout

    passed = _passed_for_check(cfg.pipeline.duckdb_path, "null_rate")
    assert passed and all(p is True for p in passed), (
        "20% nulls must pass once the threshold is raised to 0.5 — "
        "proves settings.quality.null_rate_threshold is consulted"
    )


# --- (A2) expected_freq_minutes is consulted --------------------------------


def _write_silver_hourly(data_dir: Path) -> None:
    """A settlement-grain silver frame on a strict 60-minute cadence.

    The gap check only measures datasets on the settlement-period grain, so the
    frame carries ``settlement_date``/``settlement_period`` (every other period).
    """
    start = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    ts = [start + timedelta(minutes=60 * i) for i in range(6)]
    df = pl.DataFrame(
        {
            "timestamp_utc": ts,
            "settlement_date": [t.date() for t in ts],
            "settlement_period": [2 * i + 1 for i in range(6)],
        }
    )
    write_parquet(df, data_dir / "silver" / "hourlysrc" / "hourlyds" / "part-0.parquet")


def test_gap_check_default_freq_flags_hourly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A 60-min cadence is flagged under the default 30-min assumption (baseline)."""
    cfg = _make_settings(tmp_path, QualityConfig(expected_freq_minutes=30))
    _write_silver_hourly(cfg.pipeline.data_dir)
    monkeypatch.setattr(settings_module, "load_settings", lambda: cfg)

    result = runner.invoke(app, ["quality", "--all"])
    assert result.exit_code == 0, result.stdout

    passed = _passed_for_check(cfg.pipeline.duckdb_path, "time_series_gaps")
    assert passed and all(p is False for p in passed), "hourly data fails a 30-min expectation"


def test_gap_check_config_freq_flips_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An hourly dataset must PASS once expected_freq_minutes is set to 60.

    RED before the fix: ``cli.py`` hardcoded the 30-min assumption, so the
    hourly frame stayed flagged regardless of config.
    """
    cfg = _make_settings(tmp_path, QualityConfig(expected_freq_minutes=60))
    _write_silver_hourly(cfg.pipeline.data_dir)
    monkeypatch.setattr(settings_module, "load_settings", lambda: cfg)

    result = runner.invoke(app, ["quality", "--all"])
    assert result.exit_code == 0, result.stdout

    passed = _passed_for_check(cfg.pipeline.duckdb_path, "time_series_gaps")
    assert passed and all(p is True for p in passed), (
        "hourly data must pass once expected_freq_minutes is 60 — "
        "proves the gap cadence is configurable"
    )


# --- (A3) the gap check measures each entity series, on the settlement grain --


def _write_fuelhh(data_dir: Path, *, missing: tuple[str, int] | None = None) -> None:
    """Two fuel types on the half-hourly grid, settlement periods 1-4 of one day."""
    start = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    rows = [
        {
            "settlement_date": date(2024, 1, 1),
            "settlement_period": period,
            "timestamp_utc": start + timedelta(minutes=30 * (period - 1)),
            "fuel_type": fuel,
            "generation_mw": 100.0,
        }
        for fuel in ("CCGT", "WIND")
        for period in range(1, 5)
        if (fuel, period) != missing
    ]
    write_parquet(pl.DataFrame(rows), data_dir / "silver" / "elexon" / "fuelhh" / "part-0.parquet")


def _gap_results(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, write: Callable[[Path], None]
) -> list[bool]:
    cfg = _make_settings(tmp_path, QualityConfig())
    write(cfg.pipeline.data_dir)
    monkeypatch.setattr(settings_module, "load_settings", lambda: cfg)

    result = runner.invoke(app, ["quality", "--all"])
    assert result.exit_code == 0, result.stdout
    return _passed_for_check(cfg.pipeline.duckdb_path, "time_series_gaps")


def test_gap_check_measures_each_entity_series(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both fuel types share every timestamp; measured across them, a clean
    multi-entity series was full of zero intervals and always failed."""
    assert _gap_results(tmp_path, monkeypatch, _write_fuelhh) == [True]


def test_gap_check_still_finds_a_gap_inside_one_series(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def write(data_dir: Path) -> None:
        _write_fuelhh(data_dir, missing=("WIND", 2))

    assert _gap_results(tmp_path, monkeypatch, write) == [False]


def test_gap_check_leaves_a_frame_missing_a_key_column_to_the_duplicate_check(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """fuelhh's key names fuel_type; a frame without it must not crash the run."""

    def write(data_dir: Path) -> None:
        _write_fuelhh(data_dir)
        path = data_dir / "silver" / "elexon" / "fuelhh" / "part-0.parquet"
        write_parquet(pl.read_parquet(path).drop("fuel_type").unique(), path)

    assert _gap_results(tmp_path, monkeypatch, write) == []


def test_gap_check_skips_a_constant_placeholder_period(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """NDFD is daily and stamps every row settlement period 1: a placeholder,
    not the half-hourly grain, so its daily steps are not gaps."""

    def write(data_dir: Path) -> None:
        start = datetime(2024, 1, 3, 0, 0, tzinfo=UTC)
        days = [start + timedelta(days=i) for i in range(5)]
        df = pl.DataFrame(
            {
                "settlement_date": [day.date() for day in days],
                "settlement_period": [1] * 5,
                "timestamp_utc": days,
                "forecast_type": ["2_14_day"] * 5,
                "published_at": [datetime(2024, 1, 1, 9, 0, tzinfo=UTC)] * 5,
                "national_demand_mw": [30_000.0] * 5,
            }
        )
        write_parquet(df, data_dir / "silver" / "elexon" / "ndfd" / "part-0.parquet")

    assert _gap_results(tmp_path, monkeypatch, write) == []


def test_gap_check_skips_a_dataset_without_a_settlement_cadence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An hourly price series has no settlement period to declare a cadence, so
    it is not measured against the half-hourly one it always failed."""

    def write(data_dir: Path) -> None:
        start = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
        df = pl.DataFrame(
            {
                "timestamp_utc": [start + timedelta(hours=i) for i in range(6)],
                "area_code": ["GB"] * 6,
                "price_eur_mwh": [50.0] * 6,
            }
        )
        write_parquet(df, data_dir / "silver" / "entsoe" / "day_ahead_prices" / "part-0.parquet")

    assert _gap_results(tmp_path, monkeypatch, write) == []
