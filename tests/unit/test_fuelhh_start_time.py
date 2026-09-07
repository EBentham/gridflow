"""Regression tests for FUELHH vendor-start settlement identity."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest

from gridflow.silver.elexon.fuelhh import FuelHHTransformer
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from pathlib import Path


MISLABEL = {
    "settlementDate": "2021-09-22",
    "settlementPeriod": 48,
    "fuelType": "CCGT",
    "generation": 8099,
    "startTime": "2021-09-21T22:30:00Z",
    "publishTime": "2021-09-21T23:00:00Z",
}
GENUINE = {
    "settlementDate": "2021-09-22",
    "settlementPeriod": 48,
    "fuelType": "CCGT",
    "generation": 4489,
    "startTime": "2021-09-22T22:30:00Z",
    "publishTime": "2021-09-22T23:00:00Z",
}


def _start_logs(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == "gridflow.silver.elexon.fuelhh"
        and "start_time_fallback_count=" in record.getMessage()
    ]


def _assert_one_count_log(
    caplog: pytest.LogCaptureFixture,
    *,
    count: int,
    level: int,
    rows: int,
) -> None:
    records = _start_logs(caplog)
    assert len(records) == 1
    assert records[0].levelno == level
    assert records[0].getMessage().endswith(f"start_time_fallback_count={count} rows={rows}")


def _with_alias(row: dict[str, Any], alias: str) -> dict[str, Any]:
    result = dict(row)
    result[alias] = result.pop("startTime")
    return result


@pytest.mark.parametrize("alias", ["startTime", "startTimeOfHalfHrPeriod"])
@pytest.mark.parametrize(
    ("row", "expected_date", "expected_period", "expected_timestamp"),
    [
        (
            MISLABEL,
            date(2021, 9, 21),
            48,
            datetime(2021, 9, 21, 22, 30, tzinfo=UTC),
        ),
        (
            {
                "settlementDate": "2022-07-15",
                "settlementPeriod": 48,
                "fuelType": "CCGT",
                "generation": 15317,
                "startTime": "2022-07-14T22:30:00Z",
                "publishTime": "2022-07-14T23:00:00Z",
            },
            date(2022, 7, 14),
            48,
            datetime(2022, 7, 14, 22, 30, tzinfo=UTC),
        ),
        (
            {
                "settlementDate": "2022-03-28",
                "settlementPeriod": 46,
                "fuelType": "CCGT",
                "generation": 14937,
                "startTime": "2022-03-27T22:30:00Z",
                "publishTime": "2022-03-27T23:00:00Z",
            },
            date(2022, 3, 27),
            46,
            datetime(2022, 3, 27, 22, 30, tzinfo=UTC),
        ),
        (
            {
                "settlementDate": "2021-11-01",
                "settlementPeriod": 50,
                "fuelType": "CCGT",
                "generation": 3082,
                "startTime": "2021-10-31T23:30:00Z",
                "publishTime": "2021-11-01T00:00:00Z",
            },
            date(2021, 10, 31),
            50,
            datetime(2021, 10, 31, 23, 30, tzinfo=UTC),
        ),
    ],
)
def test_vendor_start_corrects_settlement_identity(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    alias: str,
    row: dict[str, Any],
    expected_date: date,
    expected_period: int,
    expected_timestamp: datetime,
) -> None:
    transformer = FuelHHTransformer(tmp_path)

    with caplog.at_level(logging.INFO, logger="gridflow.silver.elexon.fuelhh"):
        result = transformer.transform(pl.DataFrame([_with_alias(row, alias)]))

    actual = result.row(0, named=True)
    assert actual["settlement_date"] == expected_date
    assert actual["settlement_period"] == expected_period
    assert actual["timestamp_utc"] == expected_timestamp
    assert actual["fuel_type"] == "CCGT"
    assert actual["generation_mw"] == float(row["generation"])
    assert actual["published_at"] == datetime.fromisoformat(row["publishTime"])
    assert actual["data_provider"] == "elexon"
    assert actual["ingested_at"].tzinfo is not None
    assert result.schema["settlement_date"] == pl.Date
    assert result.schema["settlement_period"] == pl.Int32
    assert result.schema["timestamp_utc"] == pl.Datetime("us", "UTC")
    assert transformer.last_start_time_fallback_count == 0
    _assert_one_count_log(caplog, count=0, level=logging.INFO, rows=1)


@pytest.mark.parametrize(
    ("label_date", "start"),
    [
        ("2021-09-22", "2021-09-22T22:30:00Z"),
        ("2022-07-15", "2022-07-15T22:30:00Z"),
    ],
)
def test_consistent_vendor_start_pins_event_date(
    tmp_path: Path,
    label_date: str,
    start: str,
) -> None:
    row = dict(GENUINE)
    row.update(settlementDate=label_date, startTime=start)

    result = FuelHHTransformer(tmp_path).transform(pl.DataFrame([row]))

    assert result["settlement_date"].to_list() == [date.fromisoformat(label_date)]
    assert result["timestamp_utc"].to_list() == [datetime.fromisoformat(start)]


@pytest.mark.parametrize("rows", [[MISLABEL, GENUINE], [GENUINE, MISLABEL]])
def test_mixed_frame_preserves_previously_colliding_rows(
    tmp_path: Path,
    rows: list[dict[str, Any]],
) -> None:
    result = FuelHHTransformer(tmp_path).transform(pl.DataFrame(rows))

    assert result.select(
        "settlement_date",
        "settlement_period",
        "timestamp_utc",
        "fuel_type",
        "generation_mw",
    ).to_dicts() == [
        {
            "settlement_date": date(2021, 9, 21),
            "settlement_period": 48,
            "timestamp_utc": datetime(2021, 9, 21, 22, 30, tzinfo=UTC),
            "fuel_type": "CCGT",
            "generation_mw": 8099.0,
        },
        {
            "settlement_date": date(2021, 9, 22),
            "settlement_period": 48,
            "timestamp_utc": datetime(2021, 9, 22, 22, 30, tzinfo=UTC),
            "fuel_type": "CCGT",
            "generation_mw": 4489.0,
        },
    ]


@pytest.mark.parametrize("start", [pytest.param("missing", id="missing"), None, "not-a-time"])
def test_unusable_start_falls_back_to_label(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    start: str | None,
) -> None:
    row = dict(MISLABEL)
    if start == "missing":
        row.pop("startTime")
    else:
        row["startTime"] = start
    transformer = FuelHHTransformer(tmp_path)

    with caplog.at_level(logging.INFO, logger="gridflow.silver.elexon.fuelhh"):
        result = transformer.transform(pl.DataFrame([row]))

    assert result.select("settlement_date", "settlement_period", "timestamp_utc").row(
        0, named=True
    ) == {
        "settlement_date": date(2021, 9, 22),
        "settlement_period": 48,
        "timestamp_utc": datetime(2021, 9, 22, 22, 30, tzinfo=UTC),
    }
    assert transformer.last_start_time_fallback_count == 1
    _assert_one_count_log(caplog, count=1, level=logging.WARNING, rows=1)


def test_mixed_usable_and_fallback_rows_count_each_branch(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    fallback = dict(MISLABEL)
    fallback.update(fuelType="WIND", startTime=None)
    transformer = FuelHHTransformer(tmp_path)

    with caplog.at_level(logging.INFO, logger="gridflow.silver.elexon.fuelhh"):
        result = transformer.transform(pl.DataFrame([MISLABEL, fallback]))

    assert result.select("settlement_date", "fuel_type").to_dicts() == [
        {"settlement_date": date(2021, 9, 21), "fuel_type": "CCGT"},
        {"settlement_date": date(2021, 9, 22), "fuel_type": "WIND"},
    ]
    assert transformer.last_start_time_fallback_count == 1
    _assert_one_count_log(caplog, count=1, level=logging.WARNING, rows=2)


@pytest.mark.parametrize("primary", [None, "not-a-time"])
def test_legacy_alias_is_used_when_primary_cannot_parse(
    tmp_path: Path,
    primary: str | None,
) -> None:
    row = dict(MISLABEL)
    row.update(
        startTime=primary,
        startTimeOfHalfHrPeriod="2021-09-21T22:30:00Z",
    )
    transformer = FuelHHTransformer(tmp_path)

    result = transformer.transform(pl.DataFrame([row]))

    assert result["settlement_date"].to_list() == [date(2021, 9, 21)]
    assert transformer.last_start_time_fallback_count == 0


def test_primary_alias_wins_when_both_valid_aliases_conflict(tmp_path: Path) -> None:
    row = dict(MISLABEL)
    row.update(
        startTime="2021-09-22T22:30:00Z",
        startTimeOfHalfHrPeriod="2021-09-21T22:30:00Z",
    )

    result = FuelHHTransformer(tmp_path).transform(pl.DataFrame([row]))

    assert result["settlement_date"].to_list() == [date(2021, 9, 22)]
    assert result["timestamp_utc"].to_list() == [datetime(2021, 9, 22, 22, 30, tzinfo=UTC)]


def test_existing_entity_key_and_keep_last_semantics_are_preserved(tmp_path: Path) -> None:
    ccgt_first = dict(GENUINE)
    ccgt_first["generation"] = 1
    wind = dict(GENUINE)
    wind.update(fuelType="WIND", generation=3)
    ccgt_last = dict(GENUINE)
    ccgt_last["generation"] = 2

    result = FuelHHTransformer(tmp_path).transform(pl.DataFrame([ccgt_first, wind, ccgt_last]))

    assert result.select("fuel_type", "generation_mw").to_dicts() == [
        {"fuel_type": "CCGT", "generation_mw": 2.0},
        {"fuel_type": "WIND", "generation_mw": 3.0},
    ]


def test_fallback_counter_counts_input_rows_before_deduplication(tmp_path: Path) -> None:
    fallback = dict(MISLABEL)
    fallback.pop("startTime")
    transformer = FuelHHTransformer(tmp_path)

    result = transformer.transform(pl.DataFrame([fallback, fallback]))

    assert result.height == 1
    assert transformer.last_start_time_fallback_count == 2


def test_counter_resets_for_valid_empty_and_structurally_invalid_frames(tmp_path: Path) -> None:
    fallback = dict(MISLABEL)
    fallback.pop("startTime")
    transformer = FuelHHTransformer(tmp_path)

    transformer.transform(pl.DataFrame([fallback]))
    assert transformer.last_start_time_fallback_count == 1
    transformer.transform(pl.DataFrame([GENUINE]))
    assert transformer.last_start_time_fallback_count == 0
    assert transformer.transform(pl.DataFrame()).is_empty()
    assert transformer.last_start_time_fallback_count == 0
    assert transformer.transform(pl.DataFrame([{"settlementDate": "2021-09-22"}])).is_empty()
    assert transformer.last_start_time_fallback_count == 0


def test_no_bronze_run_resets_counter(tmp_path: Path) -> None:
    fallback = dict(MISLABEL)
    fallback.pop("startTime")
    transformer = FuelHHTransformer(tmp_path)
    transformer.transform(pl.DataFrame([fallback]))
    assert transformer.last_start_time_fallback_count == 1

    assert transformer.run(date(2021, 9, 22)) == 0
    assert transformer.last_start_time_fallback_count == 0


def test_run_persists_dataset_version_2(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transformer = FuelHHTransformer(tmp_path)
    monkeypatch.setattr(
        transformer,
        "read_bronze",
        lambda _target_date: pl.DataFrame([GENUINE]),
    )

    written = transformer.run(date(2021, 9, 22), run_id="fuelhh-version-test")
    persisted = pl.read_parquet(
        PathBuilder(tmp_path).silver_file("elexon", "fuelhh", date(2021, 9, 22))
    )

    assert written == 1
    assert persisted["dataset_version"].unique().to_list() == ["2.0.0"]
    assert transformer.last_validation_failure_count == 0
    assert transformer.last_start_time_fallback_count == 0
