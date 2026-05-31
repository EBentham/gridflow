"""Tests for DuckDB view registration."""

from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from gridflow.storage.duckdb import get_connection, init_catalogue

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def test_silver_view_reads_mixed_pre_and_post_f0_schemas(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # F15-D: gold SQL views reference silver tables absent from test tmpdir.
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)

    data_dir = tmp_path / "data"
    db_path = tmp_path / "gridflow.duckdb"
    dataset_dir = data_dir / "silver" / "elexon" / "mixed" / "year=2024" / "month=01"

    _write_parquet(pl.DataFrame({"value": [1]}), dataset_dir / "old.parquet")
    _write_parquet(
        pl.DataFrame({"value": [2], "available_at": ["2024-01-16T00:00:00Z"]}),
        dataset_dir / "new.parquet",
    )

    init_catalogue(db_path, data_dir)
    con = get_connection(db_path, read_only=True)
    try:
        rows = con.execute(
            """
            SELECT value, available_at IS NULL AS missing_available_at
            FROM silver_mixed
            ORDER BY value
            """
        ).fetchall()
    finally:
        con.close()

    assert rows == [(1, True), (2, False)]


def test_gold_view_reads_mixed_schemas(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # F15-D: gold SQL views reference silver tables absent from test tmpdir.
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)

    data_dir = tmp_path / "data"
    db_path = tmp_path / "gridflow.duckdb"
    dataset_dir = data_dir / "gold" / "model_features" / "year=2024" / "month=01"

    _write_parquet(pl.DataFrame({"value": [1]}), dataset_dir / "old.parquet")
    _write_parquet(
        pl.DataFrame({"value": [2], "feature_version": ["post-f0"]}),
        dataset_dir / "new.parquet",
    )

    init_catalogue(db_path, data_dir)
    con = get_connection(db_path, read_only=True)
    try:
        rows = con.execute(
            """
            SELECT value, feature_version IS NULL AS missing_feature_version
            FROM gold_model_features
            ORDER BY value
            """
        ).fetchall()
    finally:
        con.close()

    assert rows == [(1, True), (2, False)]
