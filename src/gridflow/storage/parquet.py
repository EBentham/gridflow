"""Parquet read/write utilities using pyarrow."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import date

logger = logging.getLogger(__name__)


def write_parquet(
    df: pl.DataFrame,
    path: Path,
    compression: str = "zstd",
) -> Path:
    """Write a Polars DataFrame to Parquet with atomic write (temp + rename).

    Returns the final path.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f".tmp_{path.name}"

    df.write_parquet(tmp_path, compression=compression)
    # os.replace is atomic on both Unix and Windows (overwrites existing)
    os.replace(tmp_path, path)

    logger.debug(f"Wrote {len(df)} rows to {path}")
    return path


def read_parquet(path: Path | str) -> pl.DataFrame:
    """Read a Parquet file or glob pattern into a Polars DataFrame."""
    path_str = str(path)

    if "*" in path_str:
        return pl.read_parquet(path_str, hive_partitioning=True, missing_columns="insert")

    path_obj = Path(path_str)
    if not path_obj.exists():
        logger.warning(f"Parquet file not found: {path}")
        return pl.DataFrame()

    return pl.read_parquet(path_obj, hive_partitioning=True, missing_columns="insert")


def scan_parquet_dir(directory: Path) -> pl.LazyFrame:
    """Lazily scan all Parquet files in a directory tree.

    Lazy equivalent of :func:`read_parquet_dir`: returns a ``LazyFrame`` so the
    caller controls when (and how much of) the tree is materialised. Tolerates
    mixed schemas across files via ``missing_columns="insert"``; schema errors
    surface on ``.collect()`` rather than at scan time.

    Args:
        directory: Root of a Hive-partitioned silver tree.

    Returns:
        A ``LazyFrame`` over the tree, or an empty ``LazyFrame`` when the
        directory is absent or contains no Parquet files.
    """
    if not directory.exists() or not any(directory.rglob("*.parquet")):
        logger.warning(f"No Parquet files found in {directory}")
        return pl.LazyFrame()
    pattern = str(directory / "**" / "*.parquet")
    return pl.scan_parquet(pattern, hive_partitioning=True, missing_columns="insert")


def scan_parquet_range(
    directory: Path,
    start: date | None = None,
    end: date | None = None,
    *,
    date_col: str = "settlement_date",
) -> pl.LazyFrame:
    """Lazily scan a silver tree, pruning to the date range's partitions.

    Silver is partitioned ``year=YYYY/month=MM`` by the same date its rows
    belong to, so restricting the scan to the ``(year, month)`` partitions that
    overlap ``[start, end]`` is equivalent to (and cheaper than) reading the
    whole tree and filtering. The glob narrowing is the load-bearing pruning:
    files in non-overlapping months are never opened. A residual ``date_col``
    predicate then trims boundary days exactly, so correctness does not depend
    on predicate pushdown.

    Args:
        directory: Root of a Hive-partitioned silver tree.
        start: Inclusive lower bound, or ``None`` for no lower bound.
        end: Inclusive upper bound, or ``None`` for no upper bound.
        date_col: Name of the ``pl.Date`` column to apply the residual
            boundary predicate against.

    Returns:
        A ``LazyFrame`` restricted to the overlapping partitions and date
        range, or an empty ``LazyFrame`` when no partition overlaps.
    """
    # Glob pruning needs both concrete bounds; otherwise fall back to the
    # whole-tree scan and still apply whichever residual predicate exists.
    if start is None or end is None:
        lf = scan_parquet_dir(directory)
        if start is not None:
            lf = lf.filter(pl.col(date_col) >= start)
        if end is not None:
            lf = lf.filter(pl.col(date_col) <= end)
        return lf

    # Require actual parquet files (not just an existing dir): an empty
    # partition dir left by an interrupted write would make pl.scan_parquet
    # raise on the zero-match glob, whereas the whole-tree **/*.parquet path
    # silently skips it. Matching that tolerance keeps the two paths equivalent.
    globs = [
        str(part / "*.parquet")
        for year, month in _overlapping_partitions(start, end)
        if any((part := directory / f"year={year}" / f"month={month:02d}").glob("*.parquet"))
    ]
    if not globs:
        logger.warning(f"No overlapping Parquet partitions in {directory} for {start}..{end}")
        return pl.LazyFrame()

    # Scan each overlapping partition separately and diagonal-concat: a single
    # multi-glob scan resolves the schema from the first file and rejects later
    # files carrying extra columns (real here -- silver gains pre/post-F0
    # bitemporal columns over time). Per-partition diagonal concat unions the
    # schema and null-fills, dropping nothing.
    lfs = [pl.scan_parquet(g, hive_partitioning=True, missing_columns="insert") for g in globs]
    lf = pl.concat(lfs, how="diagonal")
    lf = lf.filter(pl.col(date_col) >= start)
    lf = lf.filter(pl.col(date_col) <= end)
    return lf


def _overlapping_partitions(start: date, end: date) -> Iterator[tuple[int, int]]:
    """Yield ``(year, month)`` pairs inclusive between two dates, monthly.

    Granularity is the month, matching the silver ``year=/month=`` partition
    scheme. Iterates by integer year/month so no intermediate ``date`` objects
    are constructed.
    """
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        yield (year, month)
        if month == 12:
            year, month = year + 1, 1
        else:
            month += 1


def read_parquet_dir(directory: Path) -> pl.DataFrame:
    """Read all Parquet files in a directory tree.

    Thin eager wrapper over :func:`scan_parquet_dir`: preserves the exact
    empty-on-no-files shape and propagates schema errors on collect so
    regressions surface instead of returning silently empty data.
    """
    return scan_parquet_dir(directory).collect()
