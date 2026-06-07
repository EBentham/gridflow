"""Parquet read/write utilities using pyarrow."""

from __future__ import annotations

import glob
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import polars as pl

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import date

logger = logging.getLogger(__name__)

# Mirror of polars' private ParquetCompression literal; kept local to avoid
# importing from polars._typing (a private module that may move between releases).
_ParquetCompression = Literal["lz4", "uncompressed", "snappy", "gzip", "brotli", "zstd"]


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

    # polars validates the compression value at runtime; cast satisfies its Literal param.
    df.write_parquet(tmp_path, compression=cast("_ParquetCompression", compression))
    # os.replace is atomic on both Unix and Windows (overwrites existing)
    os.replace(tmp_path, path)

    logger.debug(f"Wrote {len(df)} rows to {path}")
    return path


def read_parquet(path: Path | str) -> pl.DataFrame:
    """Read a Parquet file or glob pattern into a Polars DataFrame.

    For a glob, each matched file is read separately and diagonal-concatenated
    so that within-glob schema drift (an extra column in a later file) unions
    and null-fills instead of raising. A single multi-file read resolves the
    schema from the first file and rejects any later file carrying an extra
    column -- a real failure once silver gains bitemporal columns over time and
    a partial re-transform leaves a narrow file beside a wide one. Genuine
    corruption / dtype conflicts still surface (per-file read raises). See
    :func:`scan_parquet_dir` for the directory-tree equivalent.
    """
    path_str = str(path)

    if "*" in path_str:
        files = sorted(glob.glob(path_str, recursive=True))
        if not files:
            # No match: defer to polars so a genuinely bad path raises the same
            # error as before rather than being masked as empty data.
            return pl.read_parquet(path_str, hive_partitioning=True, missing_columns="insert")
        frames = [
            pl.read_parquet(f, hive_partitioning=True, missing_columns="insert") for f in files
        ]
        return pl.concat(frames, how="diagonal")

    path_obj = Path(path_str)
    if not path_obj.exists():
        logger.warning(f"Parquet file not found: {path}")
        return pl.DataFrame()

    return pl.read_parquet(path_obj, hive_partitioning=True, missing_columns="insert")


def scan_parquet_dir(directory: Path) -> pl.LazyFrame:
    """Lazily scan all Parquet files in a directory tree.

    Lazy equivalent of :func:`read_parquet_dir`: returns a ``LazyFrame`` so the
    caller controls when (and how much of) the tree is materialised. Each file is
    scanned separately and diagonal-concatenated so within-tree schema drift (an
    extra column in a later file) unions and null-fills instead of raising; a
    single multi-file glob scan would resolve the schema from the first file and
    reject any later file carrying an extra column. This mirrors the per-file
    approach in :func:`scan_parquet_range`. Genuine corruption / dtype conflicts
    still surface on ``.collect()`` rather than at scan time.

    Args:
        directory: Root of a Hive-partitioned silver tree.

    Returns:
        A ``LazyFrame`` over the tree, or an empty ``LazyFrame`` when the
        directory is absent or contains no Parquet files.
    """
    if not directory.exists():
        logger.warning(f"No Parquet files found in {directory}")
        return pl.LazyFrame()
    # Skip transient ``.tmp_*.parquet`` files: write_parquet writes to a
    # ``.tmp_<name>`` sibling then os.replace()s it into place, so a ``.tmp_``
    # parquet is always an in-flight or torn write that a concurrent read must not
    # pick up. glob-based read_parquet already skips dotfiles; mirror that here so
    # both read paths agree.
    files = sorted(f for f in directory.rglob("*.parquet") if not f.name.startswith(".tmp_"))
    if not files:
        logger.warning(f"No Parquet files found in {directory}")
        return pl.LazyFrame()
    lfs = [pl.scan_parquet(f, hive_partitioning=True, missing_columns="insert") for f in files]
    return pl.concat(lfs, how="diagonal")


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

    Note:
        ``date_col`` MUST be a ``pl.Date`` column. The residual ``<= end``
        predicate compares against a Python ``date``; if ``date_col`` were a
        ``Datetime`` (especially tz-aware) column, Polars would coerce that
        bound to midnight and silently drop the end-day's non-midnight rows.
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

    # Enumerate the individual parquet files in each overlapping partition (the
    # ``year=/month=`` glob is the load-bearing pruning -- files in non-
    # overlapping months are never listed). Requiring actual files, not just an
    # existing dir, matches the whole-tree **/*.parquet path: an empty partition
    # dir left by an interrupted write is skipped silently rather than raising
    # on a zero-match glob.
    files = [
        f
        for year, month in _overlapping_partitions(start, end)
        for f in sorted((directory / f"year={year}" / f"month={month:02d}").glob("*.parquet"))
    ]
    if not files:
        logger.warning(f"No overlapping Parquet partitions in {directory} for {start}..{end}")
        return pl.LazyFrame()

    # Scan each file separately and diagonal-concat. A single glob/multi-file
    # scan resolves the schema from the first file and rejects any later file
    # carrying an extra column -- a real failure here, since silver gains
    # pre/post-F0 bitemporal columns over time and a partial re-transform can
    # leave a narrow file beside a wide one in the SAME partition. Per-FILE
    # diagonal concat unions the schema and null-fills, dropping nothing.
    lfs = [pl.scan_parquet(f, hive_partitioning=True, missing_columns="insert") for f in files]
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
