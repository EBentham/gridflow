"""Reusable data quality check functions.

The checks ``gridflow quality`` runs reduce a DuckDB relation to a few numbers
in SQL instead of collecting the dataset: NESO portal families hold over 100
million rows, and collecting them into Polars ran the command out of memory.
Under the connection's memory limit DuckDB spills a large aggregation to disk.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import polars as pl

from gridflow.silver.latest_views import _quote_identifier

if TYPE_CHECKING:
    from collections.abc import Sequence

    import duckdb

# The name each check's SQL reads its relation under.
_INPUT = "quality_check_input"
_FLOAT_TYPES = frozenset({"DOUBLE", "FLOAT"})


@dataclass
class QualityResult:
    """Result of a single data quality check."""

    check_name: str
    dataset: str
    source: str
    passed: bool
    metric: float | None = None
    detail: str = ""


def _one_row(rel: duckdb.DuckDBPyRelation, sql: str) -> tuple[Any, ...]:
    """Run ``sql``, which reads ``rel`` as ``_INPUT``, and return its single row."""
    row = rel.query(_INPUT, sql).fetchone()
    assert row is not None  # every query here is an ungrouped aggregate
    return row


def check_null_rates(
    rel: duckdb.DuckDBPyRelation,
    columns: Sequence[str],
    source: str = "",
    dataset: str = "",
    max_rate: float = 0.05,
) -> list[QualityResult]:
    """Flag each column whose null rate exceeds the threshold, in one pass over ``rel``."""
    types = dict(zip(rel.columns, (str(t) for t in rel.types), strict=True))
    present = [c for c in columns if c in types]
    missing_exprs = []
    for column in present:
        col = _quote_identifier(column)
        expr = f"count(*) - count({col})"
        # count() counts float NaN as a value. An upstream API emitting an
        # all-NaN numeric column would otherwise be reported as 0% null and pass.
        if types[column] in _FLOAT_TYPES:
            expr += f" + count(*) FILTER (WHERE isnan({col}))"
        missing_exprs.append(expr)
    select = ", ".join(["count(*)", *missing_exprs])
    rows, *missing = _one_row(rel, f"SELECT {select} FROM {_INPUT}")
    missing_by_column = dict(zip(present, missing, strict=True))

    results = []
    for column in columns:
        if column not in types:
            result = QualityResult(
                check_name="null_rate",
                dataset=dataset,
                source=source,
                passed=False,
                metric=1.0,
                detail=f"Column '{column}' not found in DataFrame",
            )
        elif rows == 0:
            result = QualityResult(
                check_name="null_rate",
                dataset=dataset,
                source=source,
                passed=True,
                metric=0.0,
                detail=f"{column}: empty DataFrame",
            )
        else:
            null_rate = missing_by_column[column] / rows
            result = QualityResult(
                check_name="null_rate",
                dataset=dataset,
                source=source,
                passed=null_rate <= max_rate,
                metric=null_rate,
                detail=f"{column}: {null_rate:.2%} null/NaN (threshold: {max_rate:.2%})",
            )
        results.append(result)
    return results


def check_time_series_gaps(
    rel: duckdb.DuckDBPyRelation,
    time_col: str = "timestamp_utc",
    expected_freq_minutes: int = 30,
    source: str = "",
    dataset: str = "",
    group_by: Sequence[str] = (),
) -> QualityResult:
    """Detect cadence anomalies in a time series against an expected frequency.

    Flags any interval that deviates from ``expected_freq_minutes`` by more than
    the tolerance in *either* direction:

    - intervals larger than expected (a true gap / missing period), and
    - intervals smaller than expected, including zero intervals from duplicate
      timestamps and a switch to a too-frequent cadence.

    Intervals are measured in time order, so negative intervals cannot occur
    and are not checked. Null timestamps sort first, as they did in the Polars
    check, and every interval touching one is skipped. (Duplicate-key detection
    beyond the time column is the job of :func:`check_duplicates`.)

    ``group_by`` names the entity columns of a relation holding several series
    (one per fuel type, BM unit, ...): intervals are measured within each
    series, since across them every shared timestamp is a zero interval.
    """
    insufficient = QualityResult(
        check_name="time_series_gaps",
        dataset=dataset,
        source=source,
        passed=True,
        metric=0,
        detail="Insufficient data for gap detection",
    )
    if time_col not in rel.columns:
        return insufficient

    time = _quote_identifier(time_col)
    partition = ""
    if group_by:
        partition = f"PARTITION BY {', '.join(_quote_identifier(c) for c in group_by)} "
    step = f"epoch_ns({time}) - lag(epoch_ns({time})) OVER ({partition}ORDER BY {time} NULLS FIRST)"
    minute_ns = 60 * 10**9
    upper = (expected_freq_minutes + 1) * minute_ns
    lower = (expected_freq_minutes - 1) * minute_ns
    rows, anomalies = _one_row(
        rel,
        f"SELECT count(*), count(*) FILTER (WHERE step > {upper} OR step < {lower}) "
        f"FROM (SELECT {step} AS step FROM {_INPUT})",
    )
    if rows < 2:
        return insufficient

    return QualityResult(
        check_name="time_series_gaps",
        dataset=dataset,
        source=source,
        passed=anomalies == 0,
        metric=float(anomalies),
        detail=(
            f"Found {anomalies} intervals deviating from "
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
    rel: duckdb.DuckDBPyRelation,
    min_rows: int = 1,
    source: str = "",
    dataset: str = "",
) -> QualityResult:
    """Check that the relation has at least min_rows."""
    (rows,) = _one_row(rel, f"SELECT count(*) FROM {_INPUT}")
    return QualityResult(
        check_name="row_count",
        dataset=dataset,
        source=source,
        passed=rows >= min_rows,
        metric=float(rows),
        detail=f"{rows} rows (minimum: {min_rows})",
    )


def check_duplicates(
    rel: duckdb.DuckDBPyRelation,
    key_columns: list[str],
    source: str = "",
    dataset: str = "",
) -> QualityResult:
    """Check for duplicate rows based on key columns; rows sharing a null key count."""
    missing = [c for c in key_columns if c not in rel.columns]
    if missing:
        return QualityResult(
            check_name="duplicates",
            dataset=dataset,
            source=source,
            passed=False,
            detail=f"Key columns not found: {missing}",
        )

    keys = ", ".join(_quote_identifier(c) for c in key_columns)
    (dupes,) = _one_row(
        rel,
        f"SELECT count(*) FROM (SELECT 1 FROM {_INPUT} GROUP BY {keys} HAVING count(*) > 1)",
    )
    return QualityResult(
        check_name="duplicates",
        dataset=dataset,
        source=source,
        passed=dupes == 0,
        metric=float(dupes),
        detail=f"{dupes} duplicate key combinations found",
    )
