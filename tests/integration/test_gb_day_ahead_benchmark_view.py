"""Offline producer, catalogue, manifest and SDK contracts for the GB benchmark."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

import duckdb
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from gridflow.serving.client import GridflowClient
from gridflow.silver.elexon.mid import MIDTransformer
from gridflow.silver.schema_manifest import get_silver_schema_manifest, silver_schema_manifest_frame
from gridflow.storage.duckdb import init_catalogue, refresh_views
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

_STAMP = datetime(2024, 10, 29, 12, tzinfo=UTC)
_COLUMNS = (
    "timestamp_utc",
    "settlement_date",
    "settlement_period",
    "benchmark_price_gbp_mwh",
    "benchmark_volume_mwh",
    "data_provider_id",
    "available_at",
    "vintage_policy",
)


@pytest.fixture(params=["absent", "present", "null"])
def benchmark_catalogue(
    tmp_path: Path, request: pytest.FixtureRequest, seed_silver: Callable[..., None]
) -> tuple[Path, str | None]:
    """Build a real catalogue over two providers, including an autumn DST day."""
    paths = PathBuilder(tmp_path)
    seed_silver(tmp_path, include_vintage_policy=request.param != "absent")
    raw = pl.DataFrame(
        {
            "settlementDate": ["2024-10-28"] * 2 + ["2024-10-27"] * 8,
            "settlementPeriod": [1, 1, 50, 50, 49, 49, 2, 2, 1, 1],
            "dataProvider": ["N2EXMIP", "APXMIDP"] * 5,
            "price": [999.0, 65.0, 999.0, 64.0, 999.0, -3.0, 999.0, 0.0, 999.0, 61.0],
            "volume": [9999.0, 150.0, 9999.0, 140.0, 9999.0, 130.0, 9999.0, 120.0, 9999.0, 110.0],
        }
    )
    transformer = MIDTransformer(tmp_path)
    # Repeated input must collapse per provider in silver, without losing APXMIDP.
    silver = transformer.transform(pl.concat([raw, raw])).with_columns(
        pl.lit(_STAMP).alias("available_at")
    )
    assert silver.height == 10
    assert silver.schema["timestamp_utc"] == pl.Datetime("us", "UTC")
    assert silver.schema["available_at"] == pl.Datetime("us", "UTC")
    policy = "ingest_time" if request.param == "present" else None
    if request.param != "absent":
        silver = silver.with_columns(pl.lit(policy, dtype=pl.String).alias("vintage_policy"))
    for target_date in (date(2024, 10, 27), date(2024, 10, 28)):
        path = paths.silver_file("elexon", "mid", target_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        silver.filter(pl.col("settlement_date") == target_date).write_parquet(path)
    init_catalogue(paths.duckdb_path(), tmp_path)
    return paths.duckdb_path(), policy


def test_benchmark_registers_filters_and_preserves_values(
    benchmark_catalogue: tuple[Path, str | None],
) -> None:
    """Real registration preserves APXMIDP values, provenance and settlement grain."""
    db_path, policy = benchmark_catalogue
    with duckdb.connect(str(db_path), read_only=True) as con:
        result = con.execute(
            "SELECT * FROM gold_gb_day_ahead_benchmark ORDER BY timestamp_utc"
        ).pl()
    assert tuple(result.columns) == _COLUMNS
    assert result.height == 5
    assert result.unique(subset=["settlement_date", "settlement_period"]).height == 5
    assert result["data_provider_id"].to_list() == ["APXMIDP"] * 5
    assert result["settlement_period"].to_list() == [1, 2, 49, 50, 1]
    assert result["benchmark_price_gbp_mwh"].to_list() == [61.0, 0.0, -3.0, 64.0, 65.0]
    assert result["benchmark_volume_mwh"].to_list() == [110.0, 120.0, 130.0, 140.0, 150.0]
    assert result["available_at"].to_list() == [_STAMP] * 5
    assert result["vintage_policy"].to_list() == [policy] * 5
    assert result.schema["vintage_policy"] == pl.String
    assert result["timestamp_utc"][0] == datetime(2024, 10, 26, 23, tzinfo=UTC)
    assert result["timestamp_utc"][3] == datetime(2024, 10, 27, 23, 30, tzinfo=UTC)


def test_benchmark_accessor_retains_provenance_and_filters_settlement_dates(
    benchmark_catalogue: tuple[Path, str | None],
) -> None:
    """Inclusive settlement dates retain a first period delivered on the prior UTC date."""
    db_path, policy = benchmark_catalogue
    with GridflowClient(db_path) as client:
        frame = client.get_gb_day_ahead_benchmark(date(2024, 10, 27), "2024-10-27")
        assert isinstance(frame, pl.DataFrame)
        assert tuple(frame.columns) == _COLUMNS
        assert frame["settlement_period"].to_list() == [1, 2, 49, 50]
        assert frame["available_at"].to_list() == [_STAMP] * 4
        assert frame["vintage_policy"].to_list() == [policy] * 4
        assert frame["timestamp_utc"].is_sorted()
        assert client.get_gb_day_ahead_benchmark("2024-10-27", "2024-10-28").height == 5
        empty = client.get_gb_day_ahead_benchmark("2024-10-29", "2024-10-30")
        assert empty.is_empty()
        assert empty.schema == frame.schema


def test_benchmark_manifest_matches_registered_schema(
    benchmark_catalogue: tuple[Path, str | None],
) -> None:
    """Discovery advertises the gold alias and its complete, stable public schema."""
    db_path, _policy = benchmark_catalogue
    entries = [
        entry for entry in get_silver_schema_manifest() if entry.dataset == "gb_day_ahead_benchmark"
    ]
    assert len(entries) == 1
    entry = entries[0]
    assert entry.source is None
    assert entry.relation_kind == "gold"
    assert entry.relation_name == "gold_gb_day_ahead_benchmark"
    assert entry.qualified_view is None
    assert entry.designated_date_col == "settlement_date"
    assert entry.date_col_sql_type == "DATE"
    assert entry.columns_source == "gold_sql"
    assert entry.columns == _COLUMNS
    with GridflowClient(db_path) as client:
        assert tuple(client.get_gb_day_ahead_benchmark("2024-10-27", "2024-10-28").columns) == (
            entry.columns
        )
    assert (
        silver_schema_manifest_frame().filter(pl.col("dataset") == "gb_day_ahead_benchmark").height
        == 1
    )
    assert not any(
        row.dataset == "gb_day_ahead_benchmark"
        for row in get_silver_schema_manifest(include_serving_aliases=False)
    )


def test_benchmark_refresh_preserves_results(
    benchmark_catalogue: tuple[Path, str | None],
) -> None:
    """Repeated registration stays idempotent for both old and V-a silver schemas."""
    db_path, _policy = benchmark_catalogue
    with GridflowClient(db_path) as client:
        before = client.get_gb_day_ahead_benchmark("2024-10-27", "2024-10-28")
    refresh_views(db_path, db_path.parent)
    with GridflowClient(db_path) as client:
        after = client.get_gb_day_ahead_benchmark("2024-10-27", "2024-10-28")
    assert_frame_equal(before, after)


def _mid_row(settlement_date: date, price: float, stamp: datetime) -> pl.DataFrame:
    """Produce one legacy-schema APXMIDP row with an explicit availability stamp."""
    return pl.DataFrame(
        {
            "timestamp_utc": [
                datetime(
                    settlement_date.year, settlement_date.month, settlement_date.day, tzinfo=UTC
                )
            ],
            "settlement_date": [settlement_date],
            "settlement_period": pl.Series([1], dtype=pl.Int32),
            "market_index_price": [price],
            "market_index_volume": [100.0],
            "data_provider_id": ["APXMIDP"],
            "available_at": [stamp],
        }
    )


@pytest.mark.parametrize("reverse_files", [False, True])
@pytest.mark.parametrize("tied_stamps", [False, True])
def test_benchmark_enforces_grain_across_overlapping_files(
    tmp_path: Path, reverse_files: bool, tied_stamps: bool, seed_silver: Callable[..., None]
) -> None:
    """Latest stamp wins across partitions; equal stamps have a stable value tiebreak."""
    paths = PathBuilder(tmp_path)
    seed_silver(tmp_path)
    settlement_date = date(2024, 3, 1)
    earlier_stamp = _STAMP if tied_stamps else datetime(2024, 10, 28, 12, tzinfo=UTC)
    rows = [
        _mid_row(settlement_date, 50.0 if tied_stamps else 60.0, earlier_stamp),
        _mid_row(settlement_date, 55.0, _STAMP),
    ]
    if reverse_files:
        rows.reverse()
    for partition_date, row in zip((settlement_date, date(2024, 3, 2)), rows, strict=True):
        path = paths.silver_file("elexon", "mid", partition_date)
        path.parent.mkdir(parents=True, exist_ok=True)
        row.write_parquet(path)
    init_catalogue(paths.duckdb_path(), tmp_path)
    with duckdb.connect(str(paths.duckdb_path()), read_only=True) as con:
        assert con.execute("SELECT count(*) FROM silver_elexon_mid").fetchone() == (2,)
        result = con.execute("SELECT * FROM gold_gb_day_ahead_benchmark").pl()
    assert result.height == 1
    assert result["benchmark_price_gbp_mwh"].to_list() == [55.0]
    assert result["available_at"].to_list() == [_STAMP]


def test_benchmark_refresh_preserves_nonempty_legacy_and_new_policy_partitions(
    tmp_path: Path, seed_silver: Callable[..., None]
) -> None:
    """An existing legacy catalogue gains policy labels without losing old rows or NULLs."""
    paths = PathBuilder(tmp_path)
    seed_silver(tmp_path)
    legacy_date = date(2024, 3, 1)
    new_date = date(2024, 3, 2)
    legacy = _mid_row(legacy_date, 50.0, _STAMP)
    legacy_path = paths.silver_file("elexon", "mid", legacy_date)
    legacy_path.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_parquet(legacy_path)
    init_catalogue(paths.duckdb_path(), tmp_path)
    with GridflowClient(paths.duckdb_path()) as client:
        before = client.get_gb_day_ahead_benchmark(legacy_date, new_date)
    assert before.height == 1
    assert before["vintage_policy"].to_list() == [None]

    new = _mid_row(new_date, 55.0, _STAMP).with_columns(
        pl.lit("ingest_time", dtype=pl.String).alias("vintage_policy")
    )
    new.write_parquet(paths.silver_file("elexon", "mid", new_date))
    refresh_views(paths.duckdb_path(), tmp_path)
    with GridflowClient(paths.duckdb_path()) as client:
        after = client.get_gb_day_ahead_benchmark(legacy_date, new_date)
    assert after.height == 2
    assert after["settlement_date"].to_list() == [legacy_date, new_date]
    assert after["benchmark_price_gbp_mwh"].to_list() == [50.0, 55.0]
    assert after["vintage_policy"].to_list() == [None, "ingest_time"]
    assert after.schema == before.schema
    assert_frame_equal(before, after.filter(pl.col("settlement_date") == legacy_date))
