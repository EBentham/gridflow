"""Tests for bitemporal silver lineage columns."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import polars as pl

from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.elexon.demand_forecast import DemandForecastTransformer
from gridflow.silver.elexon.fuelhh import FuelHHTransformer
from gridflow.silver.elexon.indo import INDOTransformer
from gridflow.silver.elexon.wind_forecast import WindForecastTransformer
from gridflow.silver.openmeteo.historical import HistoricalDemandWeather
from gridflow.storage.parquet import read_parquet

FIXTURES = Path(__file__).parent.parent / "fixtures"
TARGET_DATE = date(2024, 1, 15)
RUN_ID = "test-run-id"


class StaticTransformer(BaseSilverTransformer):
    """Minimal transformer for static/reference datasets without timestamps."""

    source = "test"
    dataset = "static"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        return pl.DataFrame([{"name": "reference-row"}])

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        return raw_df


class SiblingCollisionTransformer(BaseSilverTransformer):
    """Transformer whose dataset name has real-world sibling-prefix collisions."""

    source = "test"
    dataset = "storage"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        return pl.DataFrame([{"name": "storage-row"}])

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        return raw_df


def _date_dir(root: Path, source: str, dataset: str, target_date: date) -> Path:
    return (
        root
        / "bronze"
        / source
        / dataset
        / str(target_date.year)
        / f"{target_date.month:02d}"
        / f"{target_date.day:02d}"
    )


def _write_bronze_json(
    data_dir: Path,
    source: str,
    dataset: str,
    target_date: date,
    payload: dict,
    fetched_at: datetime | None = None,
) -> None:
    bronze_dir = _date_dir(data_dir, source, dataset, target_date)
    bronze_dir.mkdir(parents=True, exist_ok=True)
    (bronze_dir / "raw_test.json").write_text(json.dumps(payload))
    if fetched_at is not None:
        (bronze_dir / "raw_test.meta.json").write_text(
            json.dumps(
                {
                    "source": source,
                    "dataset": dataset,
                    "fetched_at": fetched_at.isoformat(),
                    "data_date": target_date.isoformat(),
                }
            )
        )


def _write_openmeteo_historical_bronze(
    data_dir: Path,
    target_date: date,
    fetched_at: datetime | None = None,
) -> None:
    payload = json.loads(
        (FIXTURES / "openmeteo" / "historical_london_response.json").read_text()
    )
    # F7.5: per-location bronze uses double-underscore separator.
    bronze_dir = _date_dir(
        data_dir, "open_meteo", "historical_demand__london", target_date
    )
    bronze_dir.mkdir(parents=True, exist_ok=True)
    (bronze_dir / "raw_test.json").write_text(json.dumps(payload))
    if fetched_at is not None:
        (bronze_dir / "raw_test.meta.json").write_text(
            json.dumps(
                {
                    "source": "open_meteo",
                    "dataset": "historical_demand__london",
                    "fetched_at": fetched_at.isoformat(),
                    "data_date": target_date.isoformat(),
                }
            )
        )


def _read_single_silver(data_dir: Path, source: str, dataset: str) -> pl.DataFrame:
    pattern = data_dir / "silver" / source / dataset / "**" / "*.parquet"
    return read_parquet(pattern)


def _assert_base_bitemporal_columns(
    df: pl.DataFrame, expected_version: str = "1.0.0"
) -> None:
    for column in ["event_time", "available_at", "source_run_id", "dataset_version"]:
        assert column in df.columns
        assert df[column].null_count() == 0
    assert df["event_time"].dtype == pl.Datetime("us", "UTC")
    assert df["available_at"].dtype == pl.Datetime("us", "UTC")
    assert df["source_run_id"].to_list() == [RUN_ID] * len(df)
    assert set(df["dataset_version"].to_list()) == {expected_version}


def test_indo_run_writes_bitemporal_columns(tmp_data_dir: Path) -> None:
    payload = json.loads((FIXTURES / "elexon" / "ndf_response.json").read_text())
    for row in payload["data"]:
        row["demand"] = row.pop("nationalDemand")

    _write_bronze_json(tmp_data_dir, "elexon", "indo", TARGET_DATE, payload)

    rows = INDOTransformer(tmp_data_dir).run(TARGET_DATE, run_id=RUN_ID)
    df = _read_single_silver(tmp_data_dir, "elexon", "indo")

    assert rows == len(df)
    _assert_base_bitemporal_columns(df)
    assert df["event_time"].to_list() == df["timestamp_utc"].to_list()
    assert (df["event_time"] <= df["available_at"]).all()
    csv_path = tmp_data_dir / "silver" / "elexon" / "indo" / "indo_20240115.csv"
    assert "available_at" in csv_path.read_text().splitlines()[0]


def test_fuelhh_run_writes_bitemporal_columns(tmp_data_dir: Path) -> None:
    payload = json.loads((FIXTURES / "elexon" / "fuelhh_response.json").read_text())
    _write_bronze_json(tmp_data_dir, "elexon", "fuelhh", TARGET_DATE, payload)

    rows = FuelHHTransformer(tmp_data_dir).run(TARGET_DATE, run_id=RUN_ID)
    df = _read_single_silver(tmp_data_dir, "elexon", "fuelhh")

    assert rows == len(df)
    _assert_base_bitemporal_columns(df)
    assert df["event_time"].to_list() == df["timestamp_utc"].to_list()
    assert (df["event_time"] <= df["available_at"]).all()


def test_openmeteo_historical_run_writes_bitemporal_columns(tmp_data_dir: Path) -> None:
    _write_openmeteo_historical_bronze(tmp_data_dir, TARGET_DATE)

    rows = HistoricalDemandWeather(tmp_data_dir).run(TARGET_DATE, run_id=RUN_ID)
    df = _read_single_silver(tmp_data_dir, "open_meteo", "historical_demand")

    assert rows == len(df)
    # F7.5 bumps DATASET_VERSION 1.0.0 -> 2.0.0 on all openmeteo transformers.
    _assert_base_bitemporal_columns(df, expected_version="2.0.0")
    assert df["event_time"].to_list() == df["timestamp_utc"].to_list()
    assert (df["event_time"] <= df["available_at"]).all()


def test_run_without_context_uses_synthetic_source_run_id(tmp_data_dir: Path) -> None:
    payload = json.loads((FIXTURES / "elexon" / "fuelhh_response.json").read_text())
    _write_bronze_json(tmp_data_dir, "elexon", "fuelhh", TARGET_DATE, payload)

    FuelHHTransformer(tmp_data_dir).run(TARGET_DATE)
    df = _read_single_silver(tmp_data_dir, "elexon", "fuelhh")

    assert df["source_run_id"].str.starts_with("adhoc-").all()


def test_static_dataset_falls_back_to_target_date_event_time(tmp_data_dir: Path) -> None:
    StaticTransformer(tmp_data_dir).run(TARGET_DATE, run_id=RUN_ID)
    df = _read_single_silver(tmp_data_dir, "test", "static")

    assert df["event_time"][0] == datetime(2024, 1, 15, tzinfo=UTC)


def test_reingest_available_at_uses_bronze_sidecar_timestamp(tmp_data_dir: Path) -> None:
    sidecar_time = datetime(2024, 1, 16, 9, 30, 0, tzinfo=UTC)
    payload = json.loads((FIXTURES / "elexon" / "fuelhh_response.json").read_text())
    _write_bronze_json(
        tmp_data_dir,
        "elexon",
        "fuelhh",
        TARGET_DATE,
        payload,
        fetched_at=sidecar_time,
    )

    FuelHHTransformer(tmp_data_dir).run(TARGET_DATE, run_id=RUN_ID, reingest=True)
    df = _read_single_silver(tmp_data_dir, "elexon", "fuelhh")

    assert set(df["available_at"].to_list()) == {sidecar_time}


def test_reingest_ignores_unconfigured_sibling_sidecars(tmp_data_dir: Path) -> None:
    own_time = datetime(2024, 1, 16, 9, 30, 0, tzinfo=UTC)
    sibling_time = datetime(2024, 1, 17, 9, 30, 0, tzinfo=UTC)
    _write_bronze_json(
        tmp_data_dir,
        "test",
        "storage",
        TARGET_DATE,
        {"data": []},
        fetched_at=own_time,
    )
    _write_bronze_json(
        tmp_data_dir,
        "test",
        "storage_reports",
        TARGET_DATE,
        {"data": []},
        fetched_at=sibling_time,
    )

    SiblingCollisionTransformer(tmp_data_dir).run(
        TARGET_DATE,
        run_id=RUN_ID,
        reingest=True,
    )
    df = _read_single_silver(tmp_data_dir, "test", "storage")

    assert set(df["available_at"].to_list()) == {own_time}


def test_sidecar_timestamp_parse_falls_through_to_next_key(tmp_data_dir: Path) -> None:
    valid_time = datetime(2024, 1, 16, 9, 30, 0, tzinfo=UTC)
    payload = json.loads((FIXTURES / "elexon" / "fuelhh_response.json").read_text())
    bronze_dir = _date_dir(tmp_data_dir, "elexon", "fuelhh", TARGET_DATE)
    bronze_dir.mkdir(parents=True, exist_ok=True)
    (bronze_dir / "raw_test.json").write_text(json.dumps(payload))
    (bronze_dir / "raw_test.meta.json").write_text(
        json.dumps(
            {
                "source": "elexon",
                "dataset": "fuelhh",
                "available_at": "not-a-timestamp",
                "fetched_at": valid_time.isoformat(),
                "data_date": TARGET_DATE.isoformat(),
            }
        )
    )

    FuelHHTransformer(tmp_data_dir).run(TARGET_DATE, run_id=RUN_ID, reingest=True)
    df = _read_single_silver(tmp_data_dir, "elexon", "fuelhh")

    assert set(df["available_at"].to_list()) == {valid_time}


def test_openmeteo_reingest_uses_location_sidecar_timestamp(tmp_data_dir: Path) -> None:
    sidecar_time = datetime(2024, 1, 16, 10, 0, 0, tzinfo=UTC)
    _write_openmeteo_historical_bronze(
        tmp_data_dir,
        TARGET_DATE,
        fetched_at=sidecar_time,
    )

    HistoricalDemandWeather(tmp_data_dir).run(
        TARGET_DATE,
        run_id=RUN_ID,
        reingest=True,
    )
    df = _read_single_silver(tmp_data_dir, "open_meteo", "historical_demand")

    assert set(df["available_at"].to_list()) == {sidecar_time}


def test_demand_forecast_preserves_publish_time_as_issue_time() -> None:
    raw = pl.DataFrame(
        [
            {
                "settlementDate": "2024-01-15",
                "settlementPeriod": 1,
                "nationalDemand": 28500.0,
                "publishDateTime": "2024-01-14T09:30:00Z",
            }
        ]
    )

    result = DemandForecastTransformer(Path("/tmp/test")).transform(raw)

    assert "issue_time" in result.columns
    assert result["issue_time"].dtype == pl.Datetime("us", "UTC")
    assert result["issue_time"][0] == datetime(2024, 1, 14, 9, 30, tzinfo=UTC)


def test_wind_forecast_preserves_publish_time_as_issue_time() -> None:
    raw = pl.DataFrame(
        [
            {
                "settlementDate": "2024-01-15",
                "settlementPeriod": 1,
                "initialForecast": 4500.0,
                "latestForecast": 4320.0,
                "publishDateTime": "2024-01-14T08:00:00Z",
            }
        ]
    )

    result = WindForecastTransformer(Path("/tmp/test")).transform(raw)

    assert "issue_time" in result.columns
    assert result["issue_time"].dtype == pl.Datetime("us", "UTC")
    assert result["issue_time"][0] == datetime(2024, 1, 14, 8, 0, tzinfo=UTC)
