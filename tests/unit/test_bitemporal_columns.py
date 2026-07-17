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
from gridflow.silver.elexon.remit import REMITTransformer
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


class MixedVintageTransformer(BaseSilverTransformer):
    """Emits ``published_at`` non-null on SOME rows only — the row-wise coalesce /
    column-swap-trap probe (ADR-025 §3 P1.1)."""

    source = "test"
    dataset = "mixed_vintage"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        return pl.DataFrame([{"name": "r1"}, {"name": "r2"}, {"name": "r3"}])

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        return raw_df.with_columns(
            pl.Series(
                "published_at",
                [
                    datetime(2024, 1, 10, 8, tzinfo=UTC),
                    None,
                    datetime(2024, 1, 10, 12, tzinfo=UTC),
                ],
                dtype=pl.Datetime("us", "UTC"),
            )
        )


class AllPublishedTransformer(BaseSilverTransformer):
    """Emits ``published_at`` non-null on EVERY row."""

    source = "test"
    dataset = "all_published"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        return pl.DataFrame([{"name": "r1"}, {"name": "r2"}, {"name": "r3"}])

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        return raw_df.with_columns(
            pl.Series(
                "published_at",
                [
                    datetime(2024, 1, 10, 8, tzinfo=UTC),
                    datetime(2024, 1, 10, 10, tzinfo=UTC),
                    datetime(2024, 1, 10, 12, tzinfo=UTC),
                ],
                dtype=pl.Datetime("us", "UTC"),
            )
        )


class AllNullPublishedTransformer(BaseSilverTransformer):
    """Emits a typed-null ``published_at`` column (the Elexon absent-publishTime shape)."""

    source = "test"
    dataset = "all_null_published"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        return pl.DataFrame([{"name": "r1"}, {"name": "r2"}])

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        return raw_df.with_columns(
            pl.lit(None).cast(pl.Datetime("us", "UTC")).alias("published_at")
        )


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
    payload = json.loads((FIXTURES / "openmeteo" / "historical_london_response.json").read_text())
    # F7.5: per-location bronze uses double-underscore separator.
    bronze_dir = _date_dir(data_dir, "open_meteo", "historical_demand__london", target_date)
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


def _assert_base_bitemporal_columns(df: pl.DataFrame, expected_version: str = "1.0.0") -> None:
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
    # CH3-02 (CH-PERF-02): silver CSV write is opt-in, default OFF. A plain run
    # writes only the Parquet partition; no per-date CSV sidecar.
    csv_path = tmp_data_dir / "silver" / "elexon" / "indo" / "indo_20240115.csv"
    assert not csv_path.exists()


def test_silver_csv_not_written_by_default(tmp_data_dir: Path) -> None:
    """CH3-02 (CH-PERF-02): default-OFF flag writes Parquet but no per-date CSV.

    This is the genuine fail-first assertion: today ``_write_csv`` runs on every
    ``run()``, so the CSV always exists (RED). Once the write is gated behind
    ``write_silver_csv`` (default ``False``), only the Parquet partition is
    written.
    """
    payload = json.loads((FIXTURES / "elexon" / "ndf_response.json").read_text())
    for row in payload["data"]:
        row["demand"] = row.pop("nationalDemand")
    _write_bronze_json(tmp_data_dir, "elexon", "indo", TARGET_DATE, payload)

    transformer = INDOTransformer(tmp_data_dir)
    assert transformer.write_silver_csv is False
    rows = transformer.run(TARGET_DATE, run_id=RUN_ID)

    df = _read_single_silver(tmp_data_dir, "elexon", "indo")
    assert rows == len(df)
    csv_path = tmp_data_dir / "silver" / "elexon" / "indo" / "indo_20240115.csv"
    assert not csv_path.exists()


def test_silver_csv_written_when_flag_enabled(tmp_data_dir: Path) -> None:
    """CH3-02 (CH-PERF-02): the opt-in flag restores the per-date CSV sidecar."""
    payload = json.loads((FIXTURES / "elexon" / "ndf_response.json").read_text())
    for row in payload["data"]:
        row["demand"] = row.pop("nationalDemand")
    _write_bronze_json(tmp_data_dir, "elexon", "indo", TARGET_DATE, payload)

    transformer = INDOTransformer(tmp_data_dir)
    transformer.write_silver_csv = True
    rows = transformer.run(TARGET_DATE, run_id=RUN_ID)

    csv_path = tmp_data_dir / "silver" / "elexon" / "indo" / "indo_20240115.csv"
    assert csv_path.exists()
    header = csv_path.read_text().splitlines()[0]
    assert "available_at" in header
    df = _read_single_silver(tmp_data_dir, "elexon", "indo")
    assert rows == len(df)


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


def test_reingest_prefers_written_at_over_fetched_at(tmp_data_dir: Path) -> None:
    """A sidecar carrying both fetched_at (earlier) and written_at (later)
    must anchor reingest availability to written_at.

    Pre-fix: the key-preference list put written_at LAST, so the earlier
    fetched_at won and availability was reconstructed from a pre-write proxy.
    """
    fetched_at = datetime(2024, 1, 16, 9, 30, 0, tzinfo=UTC)
    written_at = datetime(2024, 1, 16, 9, 45, 0, tzinfo=UTC)  # later
    payload = json.loads((FIXTURES / "elexon" / "fuelhh_response.json").read_text())
    bronze_dir = _date_dir(tmp_data_dir, "elexon", "fuelhh", TARGET_DATE)
    bronze_dir.mkdir(parents=True, exist_ok=True)
    (bronze_dir / "raw_test.json").write_text(json.dumps(payload))
    (bronze_dir / "raw_test.meta.json").write_text(
        json.dumps(
            {
                "source": "elexon",
                "dataset": "fuelhh",
                "fetched_at": fetched_at.isoformat(),
                "written_at": written_at.isoformat(),
                "data_date": TARGET_DATE.isoformat(),
            }
        )
    )

    FuelHHTransformer(tmp_data_dir).run(TARGET_DATE, run_id=RUN_ID, reingest=True)
    df = _read_single_silver(tmp_data_dir, "elexon", "fuelhh")

    assert set(df["available_at"].to_list()) == {written_at}


def test_causality_assertion_has_teeth(tmp_data_dir: Path) -> None:
    """Demonstrate the event_time <= available_at invariant CAN fail.

    The existing causality assertions run with available_at = now() against
    2024 fixtures, so the inequality is structurally always true and guards
    nothing. Here we force availability EARLIER than the event via a sidecar,
    and assert the invariant is violated — proving the assertion is not vacuous.
    """
    # publishTime (event_time) is 2024-01-15T...; pin availability BEFORE it.
    early_available = datetime(2024, 1, 14, 0, 0, 0, tzinfo=UTC)
    payload = json.loads((FIXTURES / "elexon" / "fuelhh_response.json").read_text())
    bronze_dir = _date_dir(tmp_data_dir, "elexon", "fuelhh", TARGET_DATE)
    bronze_dir.mkdir(parents=True, exist_ok=True)
    (bronze_dir / "raw_test.json").write_text(json.dumps(payload))
    (bronze_dir / "raw_test.meta.json").write_text(
        json.dumps(
            {
                "source": "elexon",
                "dataset": "fuelhh",
                "written_at": early_available.isoformat(),
                "data_date": TARGET_DATE.isoformat(),
            }
        )
    )

    FuelHHTransformer(tmp_data_dir).run(TARGET_DATE, run_id=RUN_ID, reingest=True)
    df = _read_single_silver(tmp_data_dir, "elexon", "fuelhh")

    # event_time is the half-hourly settlement time on 2024-01-15, which is
    # AFTER the forced 2024-01-14 availability -> the causality invariant is
    # violated. If this assertion's premise were vacuous, this would not hold.
    assert (df["event_time"] > df["available_at"]).all()


def _write_remit_revision(
    data_dir: Path,
    target_date: date,
    revision: int,
    publish_time: str,
    written_at: datetime,
) -> None:
    """Write a single REMIT revision to its own bronze partition + sidecar."""
    bronze_dir = _date_dir(data_dir, "elexon", "remit", target_date)
    bronze_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "data": [
            {
                "mrid": "MSG-A",
                "revisionNumber": revision,
                "publishTime": publish_time,
                "messageType": "Production unavailability",
                "messageHeading": f"Outage notice rev {revision}",
                "eventType": "Planned",
                "unavailabilityType": "Planned",
                "participantId": "PART-A",
                "registrationCode": "REG-A",
                "assetId": "ASSET-1",
                "assetType": "Generation",
                "affectedUnit": "UNIT-1",
                "affectedUnitEIC": "EIC-1",
                "biddingZone": "GB",
                "fuelType": "Gas",
                "normalCapacity": 500.0,
                "availableCapacity": 0.0,
                "unavailableCapacity": 500.0,
                "eventStatus": "Active",
                "eventStartTime": "2024-02-02T00:00:00Z",
                "eventEndTime": "2024-02-05T00:00:00Z",
                "cause": "Maintenance",
                "relatedInformation": "",
            }
        ]
    }
    (bronze_dir / f"raw_rev{revision}.json").write_text(json.dumps(payload))
    (bronze_dir / f"raw_rev{revision}.meta.json").write_text(
        json.dumps(
            {
                "source": "elexon",
                "dataset": "remit",
                "written_at": written_at.isoformat(),
                "data_date": target_date.isoformat(),
            }
        )
    )


def test_per_revision_availability_does_not_collapse(tmp_data_dir: Path) -> None:
    """Two revisions fetched at distinct write times must carry distinct
    availability, and an ``available_at <= as_of`` cut between them must admit
    exactly the earlier revision.

    Models the real bitemporal case: revision 1 published+written at 08:00,
    revision 2 at 12:00, ingested in separate reingest passes (distinct bronze
    partitions). A model asking "what was visible at 10:00?" must see only
    revision 1.
    """
    date_rev1 = date(2024, 2, 1)
    date_rev2 = date(2024, 2, 2)
    written_rev1 = datetime(2024, 2, 1, 8, 0, 0, tzinfo=UTC)
    written_rev2 = datetime(2024, 2, 1, 12, 0, 0, tzinfo=UTC)

    _write_remit_revision(tmp_data_dir, date_rev1, 1, "2024-02-01T08:00:00Z", written_rev1)
    _write_remit_revision(tmp_data_dir, date_rev2, 2, "2024-02-01T12:00:00Z", written_rev2)

    REMITTransformer(tmp_data_dir).run(date_rev1, run_id="run-1", reingest=True)
    REMITTransformer(tmp_data_dir).run(date_rev2, run_id="run-2", reingest=True)

    union = _read_single_silver(tmp_data_dir, "elexon", "remit")

    # Distinct availability per revision (not collapsed to a single value).
    avail_by_rev = {
        row["revision_number"]: row["available_at"]
        for row in union.select(["revision_number", "available_at"]).to_dicts()
    }
    assert avail_by_rev[1] == written_rev1
    assert avail_by_rev[2] == written_rev2
    assert avail_by_rev[1] != avail_by_rev[2]

    # An as-of cut at 10:00 admits exactly the earlier revision.
    as_of = datetime(2024, 2, 1, 10, 0, 0, tzinfo=UTC)
    visible = union.filter(pl.col("available_at") <= as_of)
    assert visible["revision_number"].to_list() == [1]


def test_demand_forecast_preserves_publish_time_as_published_at() -> None:
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

    assert "published_at" in result.columns
    assert result["published_at"].dtype == pl.Datetime("us", "UTC")
    assert result["published_at"][0] == datetime(2024, 1, 14, 9, 30, tzinfo=UTC)


def test_wind_forecast_preserves_publish_time_as_published_at() -> None:
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

    assert "published_at" in result.columns
    assert result["published_at"].dtype == pl.Datetime("us", "UTC")
    assert result["published_at"][0] == datetime(2024, 1, 14, 8, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# ADR-025 §3 (P1.1): available_at = coalesce(published_at, ingest_time), row-wise.
# These pin the base-write coalesce at silver/base.py::_add_bitemporal_columns.
# ---------------------------------------------------------------------------


def test_available_at_coalesces_published_at_rowwise(tmp_data_dir: Path) -> None:
    """CRITICAL row-wise regression (the column-swap trap). A frame with
    published_at non-null on SOME rows: the vintage-bearing rows get
    available_at == published_at; the null-published_at row falls back to the
    ingest scalar — NOT null, NOT a neighbour's vintage."""
    sidecar_time = datetime(2024, 1, 16, 9, 0, 0, tzinfo=UTC)
    _write_bronze_json(
        tmp_data_dir, "test", "mixed_vintage", TARGET_DATE, {"data": []}, fetched_at=sidecar_time
    )
    MixedVintageTransformer(tmp_data_dir).run(TARGET_DATE, run_id=RUN_ID, reingest=True)
    df = _read_single_silver(tmp_data_dir, "test", "mixed_vintage")

    assert df["available_at"].null_count() == 0  # the trap: available_at is never null
    for published, available in zip(
        df["published_at"].to_list(), df["available_at"].to_list(), strict=True
    ):
        assert available == (published if published is not None else sidecar_time)
    # both vintages and the ingest-scalar fallback are present
    assert datetime(2024, 1, 10, 8, tzinfo=UTC) in df["available_at"].to_list()
    assert datetime(2024, 1, 10, 12, tzinfo=UTC) in df["available_at"].to_list()
    assert sidecar_time in df["available_at"].to_list()


def test_available_at_equals_published_at_when_all_rows_published(tmp_data_dir: Path) -> None:
    """Every row carries a vintage → available_at is the vintage on every row;
    the ingest scalar appears nowhere."""
    sidecar_time = datetime(2024, 1, 16, 9, 0, 0, tzinfo=UTC)
    _write_bronze_json(
        tmp_data_dir, "test", "all_published", TARGET_DATE, {"data": []}, fetched_at=sidecar_time
    )
    AllPublishedTransformer(tmp_data_dir).run(TARGET_DATE, run_id=RUN_ID, reingest=True)
    df = _read_single_silver(tmp_data_dir, "test", "all_published")

    assert df["available_at"].to_list() == df["published_at"].to_list()
    assert sidecar_time not in df["available_at"].to_list()


def test_all_null_published_at_column_falls_back_to_scalar(tmp_data_dir: Path) -> None:
    """A typed-null published_at column (the Elexon absent-publishTime shape) →
    every available_at is the ingest scalar. This is WHY the fuelhh/indo/remit
    sidecar pins elsewhere in this file stay green after the coalesce."""
    sidecar_time = datetime(2024, 1, 16, 9, 0, 0, tzinfo=UTC)
    _write_bronze_json(
        tmp_data_dir,
        "test",
        "all_null_published",
        TARGET_DATE,
        {"data": []},
        fetched_at=sidecar_time,
    )
    AllNullPublishedTransformer(tmp_data_dir).run(TARGET_DATE, run_id=RUN_ID, reingest=True)
    df = _read_single_silver(tmp_data_dir, "test", "all_null_published")

    assert set(df["available_at"].to_list()) == {sidecar_time}


def test_available_at_unchanged_without_published_at_column(tmp_data_dir: Path) -> None:
    """Byte-preservation: a transformer emitting NO published_at column hits the
    else branch — available_at is the scalar, byte-identical to pre-P1.1."""
    sidecar_time = datetime(2024, 1, 16, 9, 0, 0, tzinfo=UTC)
    _write_bronze_json(
        tmp_data_dir, "test", "static", TARGET_DATE, {"data": []}, fetched_at=sidecar_time
    )
    StaticTransformer(tmp_data_dir).run(TARGET_DATE, run_id=RUN_ID, reingest=True)
    df = _read_single_silver(tmp_data_dir, "test", "static")

    assert set(df["available_at"].to_list()) == {sidecar_time}
