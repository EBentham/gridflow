"""Regression: elexon/indo silver schema stays uniform across a publishTime gap.

Guards the F24 fix. A `SELECT *` partition-glob read over `elexon/indo` spanning a
date whose bronze carried `publishTime` and one whose bronze did not must not raise a
DuckDB schema mismatch. Before the fix the transformer dropped `published_at` entirely
when bronze lacked `publishTime`, so the glob (which unifies schema across files before
applying the event-time filter) failed at the read stage — the exact break that routed
gridflow_models' demand-forecast notebook into an acceptable-data-availability fallback.

Reads with `union_by_name` left at its default (off) on purpose: this asserts the
on-disk store is genuinely uniform, not that a tolerant reader papers over drift.
"""

from __future__ import annotations

import json
from datetime import date
from typing import TYPE_CHECKING

import duckdb
import polars as pl

from gridflow.silver.elexon.indo import INDOTransformer

if TYPE_CHECKING:
    from pathlib import Path


def _write_indo_bronze(bronze_root: Path, day: date, *, with_publish_time: bool) -> None:
    """Write a minimal indo bronze partition for one settlement date.

    Mirrors the layout INDOTransformer.read_bronze expects:
    bronze/elexon/indo/<YYYY>/<MM>/<DD>/raw_*.json with a {"data": [...]} body.
    """
    records = []
    for period in (1, 2):
        rec: dict[str, object] = {
            "settlementDate": day.isoformat(),
            "settlementPeriod": period,
            "demand": 28000.0 + period,
        }
        if with_publish_time:
            rec["publishTime"] = f"{day.isoformat()}T00:15:00Z"
        records.append(rec)

    partition = bronze_root / str(day.year) / f"{day.month:02d}" / f"{day.day:02d}"
    partition.mkdir(parents=True, exist_ok=True)
    (partition / "raw_test.json").write_text(json.dumps({"data": records}))


def test_indo_glob_read_across_publish_time_gap(tmp_data_dir: Path) -> None:
    bronze_root = tmp_data_dir / "bronze" / "elexon" / "indo"
    present_day = date(2024, 1, 1)   # bronze carries publishTime
    absent_day = date(2026, 4, 14)   # bronze lacks publishTime (the drift trigger)
    _write_indo_bronze(bronze_root, present_day, with_publish_time=True)
    _write_indo_bronze(bronze_root, absent_day, with_publish_time=False)

    transformer = INDOTransformer(tmp_data_dir)
    assert transformer.run(present_day) == 2
    assert transformer.run(absent_day) == 2

    glob = (tmp_data_dir / "silver" / "elexon" / "indo" / "**" / "*.parquet").as_posix()
    con = duckdb.connect()
    try:
        # SELECT * WITHOUT union_by_name — the gridflow_models read shape that
        # previously raised InvalidInputException at the 2024<->2026 boundary.
        result = con.execute(
            "SELECT * FROM read_parquet(?, hive_partitioning=true) ORDER BY event_time",
            [glob],
        ).pl()
    finally:
        con.close()

    assert result.height == 4
    assert "published_at" in result.columns
    # 2024 rows keep real publish times; 2026 rows are typed-null — both schema-uniform.
    assert result["published_at"].null_count() == 2

    # The on-disk silver contract must be UTC-typed. (DuckDB relabels
    # TIMESTAMPTZ to the session timezone on read, so the read-back dtype is
    # not a reliable check — assert the stored Parquet schema directly.)
    for parquet_file in sorted(
        (tmp_data_dir / "silver" / "elexon" / "indo").glob("year=*/**/*.parquet")
    ):
        stored = pl.read_parquet(parquet_file)
        assert stored.schema["published_at"] == pl.Datetime("us", "UTC")
