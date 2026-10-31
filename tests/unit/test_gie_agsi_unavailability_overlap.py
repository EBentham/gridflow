"""Regression tests for the AGSI unavailability per-day overlap filter.

Before the fix, ``_unavailability_record_overlaps`` looked only for the
fixture's ``eventStart``/``eventEnd`` keys. Live records carry ``start``/``end``,
so every live record classified as dateless and was kept: each daily silver
partition held the full bronze outage set (341 rows on each of 2026-08-01..05).
"""

from __future__ import annotations

import json
from datetime import date
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest

from gridflow.silver.gie.agsi import (
    UnavailabilityTransformer,
    _classify_unavailability_record,
)
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from pathlib import Path


def _live(start: str | None, end: str | None, facility: str = "21W0000000000702") -> dict:
    """A record in the live API shape (nested dicts, naive ``YYYY-MM-DD HH:MM:SS``)."""
    return {
        "published": "2026-08-02 05:50:36",
        "country": {"name": "France", "code": "FR"},
        "company": {"name": "Storengy", "eic": "21X000000001083B"},
        "facility": {"name": "VGS SEDIANE B", "eic": facility},
        "start": start,
        "end": end,
        "volume": "0.000",
        "injection": "0.0",
        "withdrawal": "167.5",
        "description": "N/A",
        "end_flag": "Confirmed",
        "type": "Planned",
    }


@pytest.mark.parametrize(
    ("record", "target", "expected"),
    [
        # Gas-day style 06:00 -> 06:00 outage touches both calendar days.
        (_live("2026-08-01 06:00:00", "2026-08-02 06:00:00"), date(2026, 7, 31), "outside"),
        (_live("2026-08-01 06:00:00", "2026-08-02 06:00:00"), date(2026, 8, 1), "overlap"),
        (_live("2026-08-01 06:00:00", "2026-08-02 06:00:00"), date(2026, 8, 2), "overlap"),
        (_live("2026-08-01 06:00:00", "2026-08-02 06:00:00"), date(2026, 8, 3), "outside"),
        # Same-day intraday outage.
        (_live("2026-08-03 12:00:00", "2026-08-03 23:00:00"), date(2026, 8, 2), "outside"),
        (_live("2026-08-03 12:00:00", "2026-08-03 23:00:00"), date(2026, 8, 3), "overlap"),
        (_live("2026-08-03 12:00:00", "2026-08-03 23:00:00"), date(2026, 8, 4), "outside"),
        # Multi-month outage.
        (_live("2026-08-03 05:00:00", "2026-12-23 15:00:00"), date(2026, 8, 2), "outside"),
        (_live("2026-08-03 05:00:00", "2026-12-23 15:00:00"), date(2026, 9, 15), "overlap"),
        (_live("2026-08-03 05:00:00", "2026-12-23 15:00:00"), date(2026, 12, 23), "overlap"),
        (_live("2026-08-03 05:00:00", "2026-12-23 15:00:00"), date(2026, 12, 24), "outside"),
        # A timestamp end is exclusive: ending exactly at midnight does not touch that day.
        (_live("2026-08-12 16:30:00", "2026-08-13 00:00:00"), date(2026, 8, 12), "overlap"),
        (_live("2026-08-12 16:30:00", "2026-08-13 00:00:00"), date(2026, 8, 13), "outside"),
        # Open-ended (null end): ongoing from its start day onward.
        (_live("2026-08-04 04:00:00", None), date(2026, 8, 3), "outside"),
        (_live("2026-08-04 04:00:00", None), date(2026, 8, 4), "overlap"),
        (_live("2026-08-04 04:00:00", None), date(2027, 1, 1), "overlap"),
        (_live("2026-08-04 04:00:00", ""), date(2026, 8, 5), "overlap"),
        # Null start: open on the past side, bounded by its end.
        (_live(None, "2026-08-04 04:00:00"), date(2026, 8, 4), "overlap"),
        (_live(None, "2026-08-04 04:00:00"), date(2026, 8, 5), "outside"),
        # Zero-length window anchors on its start.
        (_live("2026-08-05 10:00:00", "2026-08-05 10:00:00"), date(2026, 8, 4), "outside"),
        (_live("2026-08-05 10:00:00", "2026-08-05 10:00:00"), date(2026, 8, 5), "overlap"),
        (_live("2026-08-05 10:00:00", "2026-08-05 10:00:00"), date(2026, 8, 6), "outside"),
    ],
)
def test_live_shape_overlap(record: dict, target: date, expected: str) -> None:
    assert _classify_unavailability_record(record, target) == expected


@pytest.mark.parametrize(
    ("record", "target", "expected"),
    [
        # Fixture shape: camelCase keys, ISO 8601 with a UTC designator.
        (
            {"eventStart": "2026-05-01T06:00:00Z", "eventEnd": "2026-05-02T06:00:00Z"},
            date(2026, 4, 30),
            "outside",
        ),
        (
            {"eventStart": "2026-05-01T06:00:00Z", "eventEnd": "2026-05-02T06:00:00Z"},
            date(2026, 5, 2),
            "overlap",
        ),
        (
            {"eventStart": "2026-05-01T06:00:00Z", "eventEnd": "2026-05-02T06:00:00Z"},
            date(2026, 5, 3),
            "outside",
        ),
        # Offset-aware bounds are compared in UTC: 23:30+02:00 is 21:30Z on 05-01.
        (
            {"eventStart": "2026-05-01T23:30:00+02:00", "eventEnd": "2026-05-02T01:00:00+02:00"},
            date(2026, 5, 1),
            "overlap",
        ),
        (
            {"eventStart": "2026-05-01T23:30:00+02:00", "eventEnd": "2026-05-02T01:00:00+02:00"},
            date(2026, 5, 2),
            "outside",
        ),
        # Date-only bounds cover the whole named end day.
        ({"gasDayStart": "2026-05-01", "gasDayEnd": "2026-05-02"}, date(2026, 4, 30), "outside"),
        ({"gasDayStart": "2026-05-01", "gasDayEnd": "2026-05-02"}, date(2026, 5, 2), "overlap"),
        ({"gasDayStart": "2026-05-01", "gasDayEnd": "2026-05-02"}, date(2026, 5, 3), "outside"),
    ],
)
def test_other_record_shapes_overlap(record: dict, target: date, expected: str) -> None:
    assert _classify_unavailability_record(record, target) == expected


@pytest.mark.parametrize(
    "record",
    [
        {"facility": {"eic": "21W0000000000702"}},
        _live(None, None),
        _live("not-a-date", "2026-08-04 00:00:00"),
        _live("2026-08-01 06:00:00", "vendor-new-format"),
    ],
)
def test_dateless_or_unparseable_record_is_undated(record: dict) -> None:
    assert _classify_unavailability_record(record, date(2026, 8, 1)) == "undated"


def _seed_range_bronze(data_dir: Path, records: list[dict[str, Any]]) -> None:
    """Seed one bronze partition the way the connector writes a ranged fetch."""
    bronze_dir = PathBuilder(data_dir).bronze_date_dir(
        "gie_agsi", "unavailability", date(2026, 8, 1)
    )
    bronze_dir.mkdir(parents=True)
    (bronze_dir / "raw_page1.json").write_text(
        json.dumps({"last_page": 1, "total": len(records), "data": records}), encoding="utf-8"
    )
    (bronze_dir / "raw_page1.meta.json").write_text(
        json.dumps({"request_params": {"start": "2026-08-01", "end": "2026-08-06", "page": 1}}),
        encoding="utf-8",
    )


# Patterned on the 2026-08-01 live bronze: daily gas-day outages, an intraday
# outage, a long planned outage and one far in the future.
_RANGE_RECORDS = [
    _live("2026-08-01 06:00:00", "2026-08-02 06:00:00", facility="A"),
    _live("2026-08-02 06:00:00", "2026-08-03 06:00:00", facility="A"),
    _live("2026-08-03 12:00:00", "2026-08-03 23:00:00", facility="B"),
    _live("2026-08-03 05:00:00", "2026-12-23 15:00:00", facility="C"),
    _live("2026-10-01 06:00:00", "2026-10-02 06:00:00", facility="D"),
]
_EXPECTED_FACILITIES = {
    date(2026, 8, 1): ["A"],
    date(2026, 8, 2): ["A", "A"],
    date(2026, 8, 3): ["A", "B", "C"],
    date(2026, 8, 4): ["C"],
    date(2026, 8, 5): ["C"],
}


@pytest.mark.parametrize(("target", "facilities"), sorted(_EXPECTED_FACILITIES.items()))
def test_run_writes_only_overlapping_outages_per_day(
    tmp_data_dir: Path, target: date, facilities: list[str]
) -> None:
    """The full bronze outage set must not leak into every daily partition."""
    _seed_range_bronze(tmp_data_dir, _RANGE_RECORDS)
    transformer = UnavailabilityTransformer(tmp_data_dir)

    rows = transformer.run(target, run_id=f"unavail-{target:%Y%m%d}")

    assert rows == len(facilities) < len(_RANGE_RECORDS)
    parquet = next(transformer.silver_dir.rglob(f"unavailability_{target:%Y%m%d}.parquet"))
    persisted = pl.read_parquet(parquet)
    eics = sorted(persisted["facility_eic"].to_list())
    assert eics == sorted(facilities)
    assert persisted["dataset_version"].unique().to_list() == ["2.0.0"]


def test_read_bronze_keeps_undated_records_and_warns_once(
    tmp_data_dir: Path, caplog: pytest.LogCaptureFixture
) -> None:
    _seed_range_bronze(
        tmp_data_dir,
        [
            _live("2026-08-01 06:00:00", "2026-08-02 06:00:00", facility="A"),
            _live("2026-09-01 06:00:00", "2026-09-02 06:00:00", facility="late"),
            _live(None, None, facility="undated-1"),
            _live("vendor-new-format", None, facility="undated-2"),
        ],
    )
    transformer = UnavailabilityTransformer(tmp_data_dir)

    with caplog.at_level("WARNING", logger="gridflow.silver.gie.agsi"):
        df = transformer.read_bronze(date(2026, 8, 1))

    eics = sorted(df["facility_eic"].to_list())
    assert eics == ["A", "undated-1", "undated-2"]
    warnings = [r for r in caplog.records if "had no parseable start/end" in r.getMessage()]
    assert len(warnings) == 1
    assert "2 record(s)" in warnings[0].getMessage()


def test_dedup_drops_only_records_repeated_verbatim(tmp_data_dir: Path) -> None:
    """Detects a key narrower than the vendor record (a renamed facility merged into its
    namesake) or a dedup after parsing (a date-only end and a midnight end typing to one
    datetime, merged): only the verbatim repeat is dropped."""
    renamed = _live("2026-08-01 06:00:00", "2026-08-02 06:00:00", facility="A")
    renamed["facility"] = {**renamed["facility"], "name": "VGS SEDIANE B (renamed)"}
    records = [
        _live("2026-08-01 06:00:00", "2026-08-02 06:00:00", facility="A"),
        _live("2026-08-01 06:00:00", "2026-08-02 06:00:00", facility="A"),
        renamed,
        _live("2026-08-01 06:00:00", "2026-08-02", facility="B"),
        _live("2026-08-01 06:00:00", "2026-08-02 00:00:00", facility="B"),
    ]
    _seed_range_bronze(tmp_data_dir, records)
    transformer = UnavailabilityTransformer(tmp_data_dir)

    rows = transformer.run(date(2026, 8, 1), run_id="dedup")

    assert rows == 4
    persisted = pl.read_parquet(
        next(transformer.silver_dir.rglob("unavailability_20260801.parquet"))
    )
    assert sorted(persisted.filter(pl.col("facility_eic") == "A")["facility_name"]) == [
        "VGS SEDIANE B",
        "VGS SEDIANE B (renamed)",
    ]
    assert persisted.filter(pl.col("facility_eic") == "B").height == 2
