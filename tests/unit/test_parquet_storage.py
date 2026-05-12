"""Tests for Polars-backed Parquet read helpers."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl
import pytest
from polars.exceptions import PolarsError

from gridflow.storage.parquet import read_parquet, read_parquet_dir

if TYPE_CHECKING:
    from pathlib import Path


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def test_read_parquet_dir_tolerates_mixed_pre_and_post_f0_schemas(
    tmp_path: Path,
) -> None:
    dataset_dir = tmp_path / "silver" / "elexon" / "mixed" / "year=2024" / "month=01"
    _write_parquet(pl.DataFrame({"value": [1]}), dataset_dir / "old.parquet")
    _write_parquet(
        pl.DataFrame({"value": [2], "available_at": ["2024-01-16T00:00:00Z"]}),
        dataset_dir / "new.parquet",
    )

    df = read_parquet_dir(tmp_path / "silver" / "elexon" / "mixed").sort("value")

    assert df["value"].to_list() == [1, 2]
    assert df["available_at"].to_list() == [None, "2024-01-16T00:00:00Z"]


def test_read_parquet_glob_tolerates_mixed_schemas(tmp_path: Path) -> None:
    _write_parquet(pl.DataFrame({"value": [1]}), tmp_path / "old.parquet")
    _write_parquet(
        pl.DataFrame({"value": [2], "source_run_id": ["run-xyz"]}),
        tmp_path / "new.parquet",
    )

    df = read_parquet(str(tmp_path / "*.parquet")).sort("value")

    assert df["value"].to_list() == [1, 2]
    assert df["source_run_id"].to_list() == [None, "run-xyz"]


def test_read_parquet_dir_returns_empty_when_no_files(tmp_path: Path) -> None:
    df = read_parquet_dir(tmp_path / "does_not_exist")
    assert df.is_empty()

    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    df = read_parquet_dir(empty_dir)
    assert df.is_empty()


def test_read_parquet_dir_propagates_schema_errors(tmp_path: Path) -> None:
    """Genuine schema corruption must surface, not be swallowed as 'no files'."""
    dataset_dir = tmp_path / "broken"
    _write_parquet(pl.DataFrame({"value": [1]}), dataset_dir / "a.parquet")
    (dataset_dir / "b.parquet").write_bytes(b"not a parquet file")

    with pytest.raises(PolarsError):
        read_parquet_dir(dataset_dir)
