"""Regression: ENTSO-G capacity and CMP silver keeps records in force on the gas day.

ENTSO-G serves a capacity or CMP record on every day its validity window overlaps
(a 2015-2048 technical capacity, a 2025/26 gas-year CMP product). Silver used to
keep a record only when its window STARTED on the fetched day, so it dropped
nearly all of them (BACKLOG 18a, 18b). These datasets now keep each record whose
window overlaps the partition's gas day, once, stamped on that gas day.
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest

import gridflow.silver.entsog  # noqa: F401 -- registers the generic entsog family
from gridflow.silver.entsog.datetime import (
    gas_day_bounds,
    partition_validity_records_to_gas_day,
)
from gridflow.silver.registry import get_transformer

if TYPE_CHECKING:
    from pathlib import Path

_DAY = date(2026, 8, 3)
_GAS_DAY_START = datetime(2026, 8, 3, 4, tzinfo=UTC)  # 06:00 CEST


def _write_bronze(data_dir: Path, dataset: str, day: date, key: str, records: list[Any]) -> None:
    partition = (
        data_dir / "bronze" / "entsog" / dataset / f"{day.year}" / f"{day.month:02d}/{day.day:02d}"
    )
    partition.mkdir(parents=True, exist_ok=True)
    (partition / "raw_test.json").write_text(json.dumps({key: records}))
    (partition / "raw_test.meta.json").write_text(
        json.dumps({"written_at": f"{day.isoformat()}T12:00:00+00:00"})
    )


def _silver(data_dir: Path, dataset: str, day: date) -> pl.DataFrame:
    return pl.read_parquet(
        data_dir
        / "silver"
        / "entsog"
        / dataset
        / f"year={day.year}"
        / f"month={day.month:02d}"
        / f"{dataset}_{day.strftime('%Y%m%d')}.parquet"
    )


def _capacity_record(record_id: str, period_from: str, period_to: str) -> dict[str, Any]:
    return {
        "id": record_id,
        "indicator": "Firm Technical",
        "operatorKey": "UK-TSO-0001",
        "pointKey": "ITP-00005",
        "directionKey": "exit",
        "periodFrom": period_from,
        "periodTo": period_to,
        "value": 1000,
        "unit": "kWh/d",
        "dataSet": 1,
    }


def test_gas_day_bounds_follow_cet_across_clock_changes() -> None:
    assert gas_day_bounds(date(2026, 8, 3)) == (
        datetime(2026, 8, 3, 4, tzinfo=UTC),
        datetime(2026, 8, 4, 4, tzinfo=UTC),
    )
    # Clocks go back at 03:00 on 25 Oct, inside the gas day that starts on 24 Oct.
    change_start, change_end = gas_day_bounds(date(2026, 10, 24))
    assert change_start == datetime(2026, 10, 24, 4, tzinfo=UTC)
    assert change_end == datetime(2026, 10, 25, 5, tzinfo=UTC)


def test_partition_keeps_overlapping_drops_disjoint_and_keeps_start_date_rows() -> None:
    long_validity = {
        "periodFrom": "2015-10-30T06:00:00+01:00",
        "periodTo": "2048-12-13T06:00:00+01:00",
    }
    ended = {"periodFrom": "2025-10-01T06:00:00+02:00", "periodTo": "2026-08-03T06:00:00+02:00"}
    statement = {"periodFrom": "2026-08-03T00:00:00+02:00", "periodTo": "2026-08-03T00:00:00+02:00"}
    cmp_product = {
        "periodFrom": None,
        "capacityFrom": "2025-10-01T06:00:00+02:00",
        "capacityTo": "2026-10-01T06:00:00+02:00",
    }
    open_ended = {"capacityFrom": "2026-05-01T06:00:00+02:00"}
    undated = {"id": "x"}

    kept, undated_count = partition_validity_records_to_gas_day(
        [long_validity, ended, statement, cmp_product, open_ended, undated],
        _DAY,
        ("periodFrom", "capacityFrom"),
        (("periodFrom", "periodTo"), ("capacityFrom", "capacityTo")),
    )

    # `ended` stops exactly at the gas-day start: half-open, so not in force.
    assert kept == [long_validity, statement, cmp_product, open_ended, undated]
    assert undated_count == 1


def test_capacity_keeps_long_validity_records_stamped_on_the_gas_day(
    tmp_data_dir: Path,
) -> None:
    records = [
        _capacity_record("daily", "2026-08-03T05:00:00+01:00", "2026-08-04T05:00:00+01:00"),
        _capacity_record("long", "2015-10-30T06:00:00+01:00", "2048-12-13T06:00:00+01:00"),
        _capacity_record("gone", "2025-10-01T06:00:00+02:00", "2026-08-03T06:00:00+02:00"),
    ]
    _write_bronze(tmp_data_dir, "firm_technical", _DAY, "operationalData", records)

    assert get_transformer("entsog", "firm_technical", tmp_data_dir).run(_DAY) == 2

    silver = _silver(tmp_data_dir, "firm_technical", _DAY)
    assert sorted(silver["id"].to_list()) == ["daily", "long"]
    assert silver["timestamp_utc"].to_list() == [_GAS_DAY_START, _GAS_DAY_START]
    long_row = silver.filter(pl.col("id") == "long")
    assert long_row["period_from"].item() == datetime(2015, 10, 30, 5, tzinfo=UTC)
    assert long_row["value"].item() == 1000.0
    assert "gf_gas_day_start" not in silver.columns


@pytest.mark.parametrize(
    ("dataset", "key", "value_field"),
    [
        ("cmp_auction_premiums", "cmpAuctions", "auctionPremium"),
        ("cmp_unsuccessful_requests", "cmpUnsuccessfulRequests", "requestedVolume"),
    ],
)
def test_cmp_keeps_capacity_window_records_and_stamps_statements(
    tmp_data_dir: Path, dataset: str, key: str, value_field: str
) -> None:
    product = {
        "id": "product",
        "pointKey": "ITP-00005",
        "periodFrom": None,
        "auctionFrom": "2018-07-02T09:00:00+02:00",
        "capacityFrom": "2025-10-01T06:00:00+02:00",
        "capacityTo": "2026-10-01T06:00:00+02:00",
        value_field: 5,
        "dataSet": 3,
    }
    statement = {
        "id": "statement",
        "pointKey": "ITP-00113",
        "periodFrom": None,
        "capacityFrom": "2026-08-03T00:00:00+02:00",
        "capacityTo": "2026-08-03T00:00:00+02:00",
        "generalRemarks": "Currently there are no requests.",
        "dataset": 3,
    }
    _write_bronze(tmp_data_dir, dataset, _DAY, key, [product, statement])

    assert get_transformer("entsog", dataset, tmp_data_dir).run(_DAY) == 2

    silver = _silver(tmp_data_dir, dataset, _DAY)
    assert silver["timestamp_utc"].null_count() == 0
    assert silver["timestamp_utc"].to_list() == [_GAS_DAY_START, _GAS_DAY_START]


def test_non_validity_datasets_keep_the_start_date_rule(tmp_data_dir: Path) -> None:
    records = [
        {
            "id": "today",
            "periodFrom": "2026-08-03T06:00:00+02:00",
            "periodTo": "2026-08-04T06:00:00+02:00",
            "value": 1,
        },
        {
            "id": "multi_day",
            "periodFrom": "2026-08-01T06:00:00+02:00",
            "periodTo": "2026-08-05T06:00:00+02:00",
            "value": 2,
        },
    ]
    _write_bronze(tmp_data_dir, "nominations", _DAY, "operationalData", records)

    assert get_transformer("entsog", "nominations", tmp_data_dir).run(_DAY) == 1
    silver = _silver(tmp_data_dir, "nominations", _DAY)
    assert silver["id"].to_list() == ["today"]
    assert silver["timestamp_utc"].item() == _GAS_DAY_START


def test_register_timestamp_falls_back_row_wise_past_null_columns(tmp_data_dir: Path) -> None:
    transformer = get_transformer("entsog", "operator_point_directions", tmp_data_dir)
    raw = pl.DataFrame(
        {
            "id": ["a", "b"],
            "validFrom": [None, None],
            "lastUpdateDateTime": ["2026-09-27T02:18:00+02:00", "2026-09-26T10:00:00+02:00"],
        }
    )

    silver = transformer.transform(raw)

    assert silver["timestamp_utc"].null_count() == 0
    assert silver["timestamp_utc"].to_list() == [
        datetime(2026, 9, 26, 8, tzinfo=UTC),
        datetime(2026, 9, 27, 0, 18, tzinfo=UTC),
    ]
