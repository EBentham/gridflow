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

from datetime import UTC, datetime, timedelta
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
    """A silver frame on a strict 60-minute cadence."""
    start = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    ts = [start + timedelta(minutes=60 * i) for i in range(6)]
    df = pl.DataFrame({"timestamp_utc": ts})
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
