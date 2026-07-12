"""Parquet read/write utilities using pyarrow."""

from __future__ import annotations

import glob
import logging
import os
import re
import stat
import time
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast
from uuid import uuid4

import polars as pl

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import date

logger = logging.getLogger(__name__)

# Mirror of polars' private ParquetCompression literal; kept local to avoid
# importing from polars._typing (a private module that may move between releases).
_ParquetCompression = Literal["lz4", "uncompressed", "snappy", "gzip", "brotli", "zstd"]
_SUFFIX_TEMP_RE = re.compile(r"^.+\.tmp_[0-9a-f]{16}$")


def is_reserved_temp_path(path: Path | str) -> bool:
    """Return whether a path name belongs to a supported temporary grammar."""
    name = Path(path).name
    return name.startswith(".tmp_") or _SUFFIX_TEMP_RE.fullmatch(name) is not None


def write_parquet(
    df: pl.DataFrame,
    path: Path,
    compression: str = "zstd",
) -> Path:
    """Write a Polars DataFrame to Parquet with atomic write (temp + rename).

    Returns the final path.
    """
    if is_reserved_temp_path(path):
        raise ValueError(f"Refusing to write a reserved temporary path as final output: {path}")

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.parent / f"{path.name}.tmp_{uuid4().hex[:16]}"

    try:
        # polars validates the compression value at runtime; cast satisfies its Literal param.
        df.write_parquet(tmp_path, compression=cast("_ParquetCompression", compression))
        # os.replace is atomic on both Unix and Windows (overwrites existing)
        os.replace(tmp_path, path)
    except Exception:
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError as cleanup_error:
            logger.warning(
                "Could not clean failed Parquet temporary file %s: %s",
                tmp_path,
                cleanup_error,
            )
        raise

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
        matched = sorted(glob.glob(path_str, recursive=True))
        files = [f for f in matched if not is_reserved_temp_path(f)]
        if not files:
            if matched:
                return pl.DataFrame()
            # No match: defer to polars so a genuinely bad path raises the same
            # error as before rather than being masked as empty data.
            return pl.read_parquet(path_str, hive_partitioning=True, missing_columns="insert")
        frames = [
            pl.read_parquet(f, hive_partitioning=True, missing_columns="insert") for f in files
        ]
        return pl.concat(frames, how="diagonal")

    path_obj = Path(path_str)
    if is_reserved_temp_path(path_obj):
        raise ValueError(f"Refusing to read reserved temporary Parquet path: {path_obj}")
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
    files = sorted(f for f in directory.rglob("*.parquet") if not is_reserved_temp_path(f))
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
        if not is_reserved_temp_path(f)
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


def sweep_orphan_temp_files(
    silver_root: Path,
    gold_root: Path,
    *,
    max_age_seconds: float = 86_400,
    now_epoch: float | None = None,
) -> int:
    """Remove aged reserved temps contained under authoritative data roots.

    The walk never follows linked or junction directories and deletion uses a
    non-following stat immediately before unlinking. During transition, a
    pre-fix writer can still recreate a deterministic legacy ``.tmp_`` name in
    the narrow stat-to-unlink window; suffix temps avoid that name-sharing risk.

    Args:
        silver_root: Exact authoritative Silver root to inspect.
        gold_root: Exact authoritative Gold root to inspect.
        max_age_seconds: Minimum age for deletion. Exact-cutoff files qualify.
        now_epoch: Optional injected Unix timestamp for deterministic tests.

    Returns:
        Number of temporary files successfully removed.

    Raises:
        ValueError: If ``max_age_seconds`` is negative.
    """
    if max_age_seconds < 0:
        raise ValueError("max_age_seconds must be non-negative")

    cutoff = (time.time() if now_epoch is None else now_epoch) - max_age_seconds
    removed = 0
    for root in (Path(silver_root), Path(gold_root)):
        try:
            root_stat = os.stat(root, follow_symlinks=False)
        except FileNotFoundError:
            continue
        except OSError as exc:
            logger.warning("Could not inspect temporary-file sweep root %s: %s", root, exc)
            continue
        if _is_link_or_junction(root_stat):
            logger.warning("Skipping linked temporary-file sweep root: %s", root)
            continue
        try:
            resolved_root = root.resolve(strict=True)
        except OSError as exc:
            logger.warning("Could not resolve temporary-file sweep root %s: %s", root, exc)
            continue

        def _walk_error(exc: OSError, sweep_root: Path = root) -> None:
            logger.warning(
                "Could not inspect temporary-file sweep path under %s: %s",
                sweep_root,
                exc,
            )

        for current, dir_names, file_names in os.walk(
            root, topdown=True, onerror=_walk_error, followlinks=False
        ):
            current_path = Path(current)
            traversable: list[str] = []
            for directory_name in dir_names:
                directory = current_path / directory_name
                try:
                    directory_stat = os.stat(directory, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    logger.warning(
                        "Could not inspect directory during temp sweep %s: %s", directory, exc
                    )
                    continue
                if _is_link_or_junction(directory_stat):
                    logger.warning("Skipping linked directory during temp sweep: %s", directory)
                    continue
                traversable.append(directory_name)
            dir_names[:] = traversable

            for file_name in file_names:
                candidate = current_path / file_name
                if not is_reserved_temp_path(candidate):
                    continue
                try:
                    resolved_candidate = candidate.resolve(strict=True)
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    logger.warning(
                        "Could not resolve temporary-file candidate %s: %s", candidate, exc
                    )
                    continue
                if not resolved_candidate.is_relative_to(resolved_root):
                    logger.warning("Skipping temporary file outside root %s: %s", root, candidate)
                    continue
                try:
                    candidate_stat = os.stat(candidate, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    logger.warning("Could not stat temporary-file candidate %s: %s", candidate, exc)
                    continue
                if _is_link_or_junction(candidate_stat):
                    logger.warning("Skipping linked temporary-file candidate: %s", candidate)
                    continue
                if not stat.S_ISREG(candidate_stat.st_mode) or candidate_stat.st_mtime > cutoff:
                    continue
                try:
                    candidate.unlink()
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    logger.warning("Could not remove orphan temporary file %s: %s", candidate, exc)
                    continue
                removed += 1

    if removed:
        logger.info("Removed %d aged orphan temporary file(s)", removed)
    return removed


def _is_link_or_junction(path_stat: os.stat_result) -> bool:
    """Return whether a non-following stat describes a link or reparse point."""
    file_attribute_reparse_point = 0x400
    return stat.S_ISLNK(path_stat.st_mode) or bool(
        getattr(path_stat, "st_file_attributes", 0) & file_attribute_reparse_point
    )
