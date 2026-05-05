"""Parquet read/write utilities using pyarrow."""

from __future__ import annotations

import logging
import os
from pathlib import Path

import polars as pl

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
        return pl.read_parquet(
            path_str, hive_partitioning=True, missing_columns="insert"
        )

    path_obj = Path(path_str)
    if not path_obj.exists():
        logger.warning(f"Parquet file not found: {path}")
        return pl.DataFrame()

    return pl.read_parquet(
        path_obj, hive_partitioning=True, missing_columns="insert"
    )


def read_parquet_dir(directory: Path) -> pl.DataFrame:
    """Read all Parquet files in a directory tree.

    Tolerates mixed schemas across files (e.g. pre/post-F0 bitemporal columns)
    by inserting nulls for columns missing from individual files. Schema errors
    propagate so regressions surface instead of returning silently empty data.
    """
    if not directory.exists() or not any(directory.rglob("*.parquet")):
        logger.warning(f"No Parquet files found in {directory}")
        return pl.DataFrame()
    pattern = str(directory / "**" / "*.parquet")
    return pl.read_parquet(
        pattern, hive_partitioning=True, missing_columns="insert"
    )
