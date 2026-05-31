"""Reusable data quality check functions."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

import polars as pl


@dataclass
class QualityResult:
    """Result of a single data quality check."""

    check_name: str
    dataset: str
    source: str
    passed: bool
    metric: float | None = None
    detail: str = ""


def check_null_rate(
    df: pl.DataFrame,
    column: str,
    source: str = "",
    dataset: str = "",
    max_rate: float = 0.05,
) -> QualityResult:
    """Flag if null rate exceeds threshold."""
    if column not in df.columns:
        return QualityResult(
            check_name="null_rate",
            dataset=dataset,
            source=source,
            passed=False,
            metric=1.0,
            detail=f"Column '{column}' not found in DataFrame",
        )

    if len(df) == 0:
        return QualityResult(
            check_name="null_rate",
            dataset=dataset,
            source=source,
            passed=True,
            metric=0.0,
            detail=f"{column}: empty DataFrame",
        )

    missing = df[column].null_count()
    # null_count() does NOT count float NaN. An upstream API emitting an
    # all-NaN numeric column would otherwise be reported as 0% null and pass.
    if df[column].dtype.is_float():
        missing += int(df[column].is_nan().sum())
    null_rate = missing / len(df)
    return QualityResult(
        check_name="null_rate",
        dataset=dataset,
        source=source,
        passed=null_rate <= max_rate,
        metric=null_rate,
        detail=f"{column}: {null_rate:.2%} null/NaN (threshold: {max_rate:.2%})",
    )


def check_time_series_gaps(
    df: pl.DataFrame,
    time_col: str = "timestamp_utc",
    expected_freq_minutes: int = 30,
    source: str = "",
    dataset: str = "",
) -> QualityResult:
    """Detect cadence anomalies in a time series against an expected frequency.

    Flags any interval that deviates from ``expected_freq_minutes`` by more than
    the tolerance in *either* direction:

    - intervals larger than expected (a true gap / missing period), and
    - intervals smaller than expected, including zero intervals from duplicate
      timestamps and a switch to a too-frequent cadence.

    The frame is sorted first, so negative intervals cannot occur and are not
    checked. (Duplicate-key detection beyond the time column is the job of
    :func:`check_duplicates`.)
    """
    if time_col not in df.columns or len(df) < 2:
        return QualityResult(
            check_name="time_series_gaps",
            dataset=dataset,
            source=source,
            passed=True,
            metric=0,
            detail="Insufficient data for gap detection",
        )

    sorted_df = df.sort(time_col)
    diffs = sorted_df[time_col].diff().drop_nulls()

    # Use stdlib timedelta values so the comparison stays in Series space.
    # pl.duration(...) returns an Expr, which Series.filter cannot consume
    # (raises TypeError under Polars 1.40.1).
    expected = timedelta(minutes=expected_freq_minutes)
    tolerance = timedelta(minutes=1)
    anomalies = diffs.filter((diffs > expected + tolerance) | (diffs < expected - tolerance))

    return QualityResult(
        check_name="time_series_gaps",
        dataset=dataset,
        source=source,
        passed=len(anomalies) == 0,
        metric=float(len(anomalies)),
        detail=(
            f"Found {len(anomalies)} intervals deviating from "
            f"{expected_freq_minutes}min (gaps or too-frequent/duplicate rows)"
        ),
    )


def check_range(
    df: pl.DataFrame,
    column: str,
    min_val: float,
    max_val: float,
    source: str = "",
    dataset: str = "",
) -> QualityResult:
    """Flag values outside expected range."""
    if column not in df.columns:
        return QualityResult(
            check_name="range_check",
            dataset=dataset,
            source=source,
            passed=False,
            detail=f"Column '{column}' not found",
        )

    # Genuine nulls compare as null (filtered out by < / >), so they must be
    # caught explicitly — otherwise a column of nulls passes the range check.
    # Float NaN is caught by the comparison (NaN > max_val is True in Polars).
    out_of_range = df.filter(
        (pl.col(column) < min_val) | (pl.col(column) > max_val) | pl.col(column).is_null()
    )
    return QualityResult(
        check_name="range_check",
        dataset=dataset,
        source=source,
        passed=len(out_of_range) == 0,
        metric=float(len(out_of_range)),
        detail=f"{len(out_of_range)} values outside [{min_val}, {max_val}] or null",
    )


def check_row_count(
    df: pl.DataFrame,
    min_rows: int = 1,
    source: str = "",
    dataset: str = "",
) -> QualityResult:
    """Check that DataFrame has at least min_rows."""
    return QualityResult(
        check_name="row_count",
        dataset=dataset,
        source=source,
        passed=len(df) >= min_rows,
        metric=float(len(df)),
        detail=f"{len(df)} rows (minimum: {min_rows})",
    )


def check_duplicates(
    df: pl.DataFrame,
    key_columns: list[str],
    source: str = "",
    dataset: str = "",
) -> QualityResult:
    """Check for duplicate rows based on key columns."""
    missing = [c for c in key_columns if c not in df.columns]
    if missing:
        return QualityResult(
            check_name="duplicates",
            dataset=dataset,
            source=source,
            passed=False,
            detail=f"Key columns not found: {missing}",
        )

    dupes = df.group_by(key_columns).len().filter(pl.col("len") > 1)
    return QualityResult(
        check_name="duplicates",
        dataset=dataset,
        source=source,
        passed=len(dupes) == 0,
        metric=float(len(dupes)),
        detail=f"{len(dupes)} duplicate key combinations found",
    )
