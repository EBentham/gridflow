"""Behavioural tests for the quality-check layer (issue 20).

These tests assert *behaviour and values*, not shapes:
- ``check_time_series_gaps`` must not raise on a normal frame, and must bite on
  duplicate timestamps (zero interval) and too-frequent cadence.
- ``check_null_rate`` must count float ``NaN`` as missing, not just ``null``.
- ``check_range`` must not silently pass genuine ``null``s.
- ``quality_reports`` ids must be unique across separate report runs.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import polars as pl
import pytest

from gridflow.quality.checks import (
    check_null_rate,
    check_range,
    check_time_series_gaps,
)
from gridflow.quality.reporter import QualityReporter

if TYPE_CHECKING:
    from pathlib import Path


def _half_hourly(n: int, start: datetime | None = None) -> pl.DataFrame:
    start = start or datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    return pl.DataFrame({"timestamp_utc": [start + timedelta(minutes=30 * i) for i in range(n)]})


# --- check_time_series_gaps --------------------------------------------------


def test_gap_check_does_not_raise_on_normal_frame() -> None:
    """A clean half-hourly frame (>= 2 rows) must return a passing result.

    Pre-fix: raises TypeError (Expr vs Series.filter) under Polars 1.40.1.
    """
    df = _half_hourly(4)
    result = check_time_series_gaps(df, expected_freq_minutes=30)
    assert result.passed is True
    assert result.metric == 0


def test_gap_check_flags_true_gap() -> None:
    """A 90-minute hole on a 30-minute cadence must be flagged."""
    df = pl.DataFrame(
        {
            "timestamp_utc": [
                datetime(2024, 1, 1, 0, 0, tzinfo=UTC),
                datetime(2024, 1, 1, 0, 30, tzinfo=UTC),
                datetime(2024, 1, 1, 2, 0, tzinfo=UTC),  # 90-min gap
            ]
        }
    )
    result = check_time_series_gaps(df, expected_freq_minutes=30)
    assert result.passed is False
    assert result.metric == 1


def test_gap_check_flags_duplicate_timestamps() -> None:
    """Two identical (zero-interval) timestamps must not pass."""
    ts = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    df = pl.DataFrame({"timestamp_utc": [ts, ts, ts + timedelta(minutes=30)]})
    result = check_time_series_gaps(df, expected_freq_minutes=30)
    assert result.passed is False


def test_gap_check_flags_too_frequent_cadence() -> None:
    """A 15-minute cadence against an expected 30-minute cadence must not pass."""
    start = datetime(2024, 1, 1, 0, 0, tzinfo=UTC)
    df = pl.DataFrame({"timestamp_utc": [start + timedelta(minutes=15 * i) for i in range(4)]})
    result = check_time_series_gaps(df, expected_freq_minutes=30)
    assert result.passed is False


def test_gap_check_short_circuits_on_insufficient_data() -> None:
    """< 2 rows or missing column still short-circuits to a benign pass."""
    assert check_time_series_gaps(_half_hourly(1)).passed is True
    assert check_time_series_gaps(pl.DataFrame({"other": [1, 2]})).passed is True


# --- check_null_rate ---------------------------------------------------------


def test_null_rate_counts_nan() -> None:
    """An all-NaN float column must report ~100% missing and fail.

    Pre-fix: null_count() ignores NaN -> reports 0.0 / passed=True.
    """
    df = pl.DataFrame({"x": [float("nan")] * 3}, schema={"x": pl.Float64})
    result = check_null_rate(df, "x")
    assert result.metric == pytest.approx(1.0)
    assert result.passed is False


def test_null_rate_counts_mixed_nan_and_value() -> None:
    """NaN counts toward the rate alongside genuine values."""
    df = pl.DataFrame({"x": [1.0, float("nan"), 3.0, 4.0]}, schema={"x": pl.Float64})
    result = check_null_rate(df, "x", max_rate=0.05)
    assert result.metric == pytest.approx(0.25)
    assert result.passed is False


def test_null_rate_still_catches_genuine_nulls() -> None:
    """Genuine nulls are still counted (no regression)."""
    df = pl.DataFrame({"x": [1.0, None, None, 4.0]}, schema={"x": pl.Float64})
    result = check_null_rate(df, "x", max_rate=0.05)
    assert result.metric == pytest.approx(0.5)
    assert result.passed is False


def test_null_rate_passes_clean_float_column() -> None:
    """A fully populated float column passes with 0% missing."""
    df = pl.DataFrame({"x": [1.0, 2.0, 3.0]}, schema={"x": pl.Float64})
    result = check_null_rate(df, "x")
    assert result.metric == pytest.approx(0.0)
    assert result.passed is True


def test_null_rate_handles_non_float_column() -> None:
    """is_nan is float-only; an int column must not raise."""
    df = pl.DataFrame({"x": [1, 2, None]}, schema={"x": pl.Int64})
    result = check_null_rate(df, "x", max_rate=0.5)
    assert result.metric == pytest.approx(1 / 3)


def test_null_rate_short_circuits_empty_and_missing() -> None:
    """Empty frame and missing column short-circuits are preserved."""
    assert check_null_rate(pl.DataFrame({"x": []}), "x").passed is True
    assert check_null_rate(pl.DataFrame({"x": [1.0]}), "missing").passed is False


# --- check_range -------------------------------------------------------------


def test_range_does_not_silently_pass_nulls() -> None:
    """Genuine nulls must not be scored as in-range.

    Pre-fix: null comparisons -> null -> dropped by filter -> passed=True.
    """
    df = pl.DataFrame({"x": [1.0, None, 3.0]}, schema={"x": pl.Float64})
    result = check_range(df, "x", min_val=0.0, max_val=10.0)
    assert result.passed is False


def test_range_passes_clean_in_range_column() -> None:
    """All in-range, no nulls -> passes."""
    df = pl.DataFrame({"x": [1.0, 2.0, 3.0]}, schema={"x": pl.Float64})
    result = check_range(df, "x", min_val=0.0, max_val=10.0)
    assert result.passed is True


def test_range_flags_out_of_range() -> None:
    """A value above max is flagged."""
    df = pl.DataFrame({"x": [1.0, 2.0, 99.0]}, schema={"x": pl.Float64})
    result = check_range(df, "x", min_val=0.0, max_val=10.0)
    assert result.passed is False


# --- quality_reports id uniqueness ------------------------------------------


def test_quality_report_ids_unique_across_runs(tmp_path: Path) -> None:
    """Two separate report runs must not produce colliding identity keys.

    Pre-fix: id is a per-batch enumerate index -> both runs start at id=0.
    """
    from gridflow.quality.checks import QualityResult

    duckdb_path = tmp_path / "q.duckdb"
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    r1 = QualityReporter(data_dir, duckdb_path)
    r1.add_result(QualityResult("c", "ds", "src", True, 0.0, "ok"))
    r1.add_result(QualityResult("c2", "ds", "src", True, 0.0, "ok"))
    assert r1.write_report() == 2

    r2 = QualityReporter(data_dir, duckdb_path)
    r2.add_result(QualityResult("c", "ds", "src", True, 0.0, "ok"))
    r2.add_result(QualityResult("c2", "ds", "src", True, 0.0, "ok"))
    assert r2.write_report() == 2

    import duckdb

    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        cols = [c[0] for c in con.execute("DESCRIBE quality_reports").fetchall()]
        assert "run_id" in cols, "expected a run_id identity column"
        # (run_id, id) tuples must be unique across the two runs.
        total, distinct = con.execute(
            "SELECT count(*), count(DISTINCT (run_id, id)) FROM quality_reports"
        ).fetchone()
    finally:
        con.close()

    assert total == 4
    assert distinct == 4


def test_write_report_reconciles_legacy_quality_reports_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A pre-existing legacy quality_reports (no run_id) must not break writes.

    Before the gap-check TypeError fix, `gridflow quality` crashed before
    write_report() ran, so a legacy 8-column table was never exercised by the
    explicit-column INSERT. Now that the command completes, init_catalogue must
    reconcile the schema so the run_id INSERT succeeds against an old table.
    """
    import duckdb

    from gridflow.quality.checks import QualityResult
    from gridflow.storage import duckdb as duckdb_mod
    from gridflow.storage.duckdb import init_catalogue

    # Gold SQL views reference silver tables absent from this tmpdir; under
    # strict mode (pytest) their registration would raise. The legacy-schema
    # migration is independent of view registration.
    monkeypatch.setattr(duckdb_mod, "_register_gold_views", lambda con: None)

    duckdb_path = tmp_path / "legacy.duckdb"
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    # Simulate a legacy DB: 8-column quality_reports without run_id.
    con = duckdb.connect(str(duckdb_path))
    con.execute(
        """
        CREATE TABLE quality_reports (
            id INTEGER, run_date TIMESTAMP WITH TIME ZONE, check_name VARCHAR,
            dataset VARCHAR, source VARCHAR, passed BOOLEAN, metric DOUBLE,
            detail VARCHAR
        )
        """
    )
    con.close()

    # init_catalogue must migrate the legacy table (add run_id).
    init_catalogue(duckdb_path, data_dir)

    reporter = QualityReporter(data_dir, duckdb_path)
    reporter.add_result(QualityResult("c", "ds", "src", True, 0.0, "ok"))
    written = reporter.write_report()

    assert written == 1, "write_report must succeed against a migrated legacy table"

    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        cols = [c[0] for c in con.execute("DESCRIBE quality_reports").fetchall()]
        n = con.execute("SELECT count(*) FROM quality_reports").fetchone()[0]
    finally:
        con.close()
    assert "run_id" in cols
    assert n == 1
