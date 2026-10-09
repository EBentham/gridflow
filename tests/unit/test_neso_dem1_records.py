"""The ten demand-forecast frozen records (v0.22-K-DEM-1).

Every test writes a recorded fixture capture (a slice of the 2026-10-08 swept
bronze, ``tests/fixtures/neso_data_portal/dem1/``) into a short data root and
runs the transformer the **real package registry** generates for the family,
so a record that does not match its vendor body fails here, not at activation.
On master none of these families has a record, so ``get_transformer`` raises
for every key below.

``git`` and ``core.autocrlf`` normalise a committed fixture's line endings, so
:func:`dem1_body` re-terminates every record with the bronze original's
convention and no test asserts on a fixture's raw bytes.

Measured unit evidence (FACTS C4) for the 1-day current file: every one of its
twelve rows equals the same-package historic archive's latest row on demand, CP
clocks and the other compared fields, and the archive's dictionary declares
``FORECASTDEMAND`` in MW; the current file's own dictionary carries no unit, so
its MW is a documented sibling-unit transfer, recorded here because a
``ColumnSpec`` has no comment field.
"""

from __future__ import annotations

import csv
import io
import os
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pytest
from _neso_generic_support import write_capture

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.registry import Held
from gridflow.silver.neso_data_portal import generic
from gridflow.silver.neso_data_portal.completion import (
    capture_id_for,
    read_completion,
)
from gridflow.silver.registry import get_transformer
from gridflow.utils.time import settlement_period_to_utc

if TYPE_CHECKING:
    from collections.abc import Iterator

SOURCE = "neso_data_portal"
DEM1_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "dem1"
DAY = date(2026, 10, 8)
WRITTEN = datetime(2026, 10, 8, 9, 0, tzinfo=UTC)

# package slug, package id, resource id, resource name, vendor filename, ckan_last_modified:
# the real sidecar identities of the ten captures of record (2026-10-08).
SIDECARS: dict[str, tuple[str, str, str, str, str, str]] = {
    "demand_forecast_1d_day_ahead_national": (
        "1-day-ahead-demand-forecast",
        "8fbc8a09-06af-4c90-886f-d3025d38a349",
        "aec5601a-7f3e-4c4c-bf56-d8e4184d3c5b",
        "Day Ahead National Demand Forecast",
        "ng_demand_1da_20261009.csv",
        "2026-10-08T08:00:00.985148",
    ),
    "demand_forecast_1d_historic_day_ahead": (
        "1-day-ahead-demand-forecast",
        "8fbc8a09-06af-4c90-886f-d3025d38a349",
        "9847e7bb-986e-49be-8138-717b25933fbb",
        "Historic Day Ahead Demand Forecasts",
        "archive_1dayahead.csv",
        "2026-10-08T08:00:22.840245",
    ),
    "demand_forecast_2d_day_ahead": (
        "2-day-ahead-demand-forecast",
        "836df27e-e92c-45bf-80c0-a197e7c0ece3",
        "cda26f27-4bb6-4632-9fb5-2d029ca605e1",
        "2 Day Ahead Demand Forecast",
        "ng_demand_2da_20261009.csv",
        "2026-10-07T15:15:00.349284",
    ),
    "demand_forecast_2d_historic_day_ahead": (
        "2-day-ahead-demand-forecast",
        "836df27e-e92c-45bf-80c0-a197e7c0ece3",
        "24abd271-5936-45c7-85f4-2a6b450ef6b7",
        "Historic 2 Day Ahead Demand Forecasts",
        "archive_2dayahead.csv",
        "2026-10-07T15:15:12.392517",
    ),
    "national_forecast_7d_day_ahead_demand": (
        "7-day-ahead-national-forecast",
        "2b90a483-f59d-455b-be6d-3cb4c13a85d0",
        "70d3d674-15a6-4e41-83b4-410440c0b0b9",
        "7 Day Ahead Demand Forecast",
        "ng_demand_7da_20261014.csv",
        "2026-10-07T15:15:00.672524",
    ),
    "national_forecast_7d_historic_day_ahead": (
        "7-day-ahead-national-forecast",
        "2b90a483-f59d-455b-be6d-3cb4c13a85d0",
        "6f7408b4-47fd-4ae7-b1e5-f095a3a5a2dc",
        "Historic 7 Day Ahead Demand Forecasts",
        "archive_7dayahead.csv",
        "2026-10-07T15:15:13.758197",
    ),
    "national_demand_fc_2_14d_days_ahead": (
        "2-14-days-ahead-national-demand-forecast",
        "633daec6-3e70-444a-88b0-c4cef9419d40",
        "9af18ed4-efc8-4779-bef5-c0927fee9b0a",
        "2-14 Days Ahead Cardinal Point Forecast",
        "ng_demand_14da_20261022.csv",
        "2026-10-08T08:45:03.632388",
    ),
    "national_demand_fc_2_14d_days_ahead_half": (
        "2-14-days-ahead-national-demand-forecast",
        "633daec6-3e70-444a-88b0-c4cef9419d40",
        "7c0411cd-2714-4bb5-a408-adb065edf34d",
        "2-14 Days Ahead Half Hourly Forecast",
        "ng-demand-14da-hh.csv",
        "2026-10-08T08:46:32.246773",
    ),
    "national_demand_fc_2_14d_historic_day": (
        "2-14-days-ahead-national-demand-forecast",
        "633daec6-3e70-444a-88b0-c4cef9419d40",
        "4dd712a2-ee2c-455d-a9c0-9d3564c80fa0",
        "Historic 2-14 Day Ahead Demand Forecasts",
        "archive_14dayahead.csv",
        "2026-10-08T08:46:27.592963",
    ),
    "daily_demand_update": (
        "daily-demand-update",
        "7a12172a-939c-404c-b581-a6128b74f588",
        "177f6fa4-ae49-4182-81ea-0c6b35f26ca6",
        "Demand Data Update",
        "demanddataupdate.csv",
        "2026-10-08T08:20:09.592329",
    ),
}
DEM1_KEYS = tuple(SIDECARS)

# Bronze originals that end every record in CRLF (the daily update is LF).
CRLF_FAMILIES = frozenset(DEM1_KEYS) - {"daily_demand_update"}

CURRENT_CP = (
    "demand_forecast_1d_day_ahead_national",
    "demand_forecast_2d_day_ahead",
    "national_forecast_7d_day_ahead_demand",
    "national_demand_fc_2_14d_days_ahead",
)
HISTORIC_CP = (
    "demand_forecast_1d_historic_day_ahead",
    "demand_forecast_2d_historic_day_ahead",
    "national_forecast_7d_historic_day_ahead",
    "national_demand_fc_2_14d_historic_day",
)

_ARCHIVE_Q = (
    "TODO: FORECAST_TIMESTAMP zone is undocumented (values carry Z but sit after the file's "
    "CKAN last_modified when read as UTC); whether it is the immutable issue instant"
)
HELD: dict[str, str] = {
    "demand_forecast_1d_historic_day_ahead": _ARCHIVE_Q
    + "; some timestamps are vendor-documented email-recovery times",
    "demand_forecast_2d_historic_day_ahead": _ARCHIVE_Q,
    "national_forecast_7d_historic_day_ahead": _ARCHIVE_Q + "; the field has no vendor definition",
    "national_demand_fc_2_14d_historic_day": _ARCHIVE_Q + "; the field has no vendor definition",
    "national_demand_fc_2_14d_days_ahead_half": (
        "TODO: whether GDATETIME (UTC) labels the half-hour end (611/624 rows align as end "
        "labels; 2400 closes the date) - vendor confirmation needed; DST-day behaviour "
        "unobserved"
    ),
    "daily_demand_update": (
        "TODO: zero ND/TSD on FORECAST_ACTUAL_INDICATOR = F rows: a forecast of zero or "
        "no forecast?"
    ),
}

TEMPORAL: dict[str, tuple[str, tuple[str, ...]]] = {
    **{key: ("date_sp1", ("targetdate",)) for key in CURRENT_CP + HISTORIC_CP},
    "national_demand_fc_2_14d_days_ahead_half": ("date_sp1", ("date",)),
    "daily_demand_update": ("sp_pair", ("settlement_date", "settlement_period")),
}

POLARS_DTYPES: dict[str, Any] = {
    "string": pl.Utf8,
    "int64": pl.Int64,
    "float64": pl.Float64,
    "date": pl.Date,
    "datetime": pl.Datetime("us", "UTC"),
}


def _short_base() -> str:
    """The drive root on Windows (a 40-character family key plus the engine's run-id names
    pass MAX_PATH under the long per-user temp directory); the system temp elsewhere."""
    drive = os.path.splitdrive(tempfile.gettempdir())[0]
    return drive + os.sep if drive else tempfile.gettempdir()


@pytest.fixture
def data() -> Iterator[Path]:
    """A data root with a short path (ADR-036: the engine's names pass MAX_PATH below tmp_path)."""
    with tempfile.TemporaryDirectory(
        prefix="dm", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        yield Path(root)


def _records(raw: bytes) -> int:
    return len(list(csv.reader(io.StringIO(raw.decode("utf-8-sig"), newline=""))))


def dem1_body(key: str) -> bytes:
    """The fixture ``<key>.csv`` terminated by the bronze original's line convention."""
    raw = (DEM1_DIR / f"{key}.csv").read_bytes().replace(b"\r\n", b"\n")
    if key in CRLF_FAMILIES and _records(raw) == raw.count(b"\n"):
        return raw.replace(b"\n", b"\r\n")
    return raw


def _rows(body: bytes) -> int:
    return _records(body) - 1


def _table(body: bytes) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(body.decode("utf-8"), newline="")))


def _capture(data: Path, key: str, body: bytes, *, written_at: datetime = WRITTEN) -> str:
    slug, package_id, resource_id, name, filename, modified = SIDECARS[key]
    path, _sidecar = write_capture(
        data,
        key,
        body=body,
        written_at=written_at,
        partition=DAY,
        package_slug=slug,
        package_id=package_id,
        resource_id=resource_id,
        resource_name=name,
        resource_filename=filename,
        ckan_last_modified=modified,
    )
    return capture_id_for(path, data)


def _silver(data: Path, key: str) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _record(key: str) -> Any:
    record = registry_module.load_registry().families[key][1].record
    assert record is not None, key
    return record


def _run_one(data: Path, key: str, body: bytes | None = None) -> pl.DataFrame:
    _capture(data, key, body if body is not None else dem1_body(key))
    get_transformer(SOURCE, key, data).run(DAY, run_id="r")
    return _silver(data, key)


def _naive_utc(stamp: str) -> datetime:
    return datetime.fromisoformat(stamp).replace(tzinfo=UTC)


@pytest.mark.parametrize("key", DEM1_KEYS)
def test_every_dem1_family_types_its_vendor_body(data: Path, key: str) -> None:
    """Detects a family without a generated transformer, a record whose header or
    dtypes the vendor body does not satisfy, an entity (guard) key that is not unique on
    the body, and a clock taken from anywhere but the sidecar (``published_at``) or the
    declared recipe (``timestamp_utc``).
    """
    body = dem1_body(key)
    capture_id = _capture(data, key, body)

    transformer = get_transformer(SOURCE, key, data)
    written_rows = transformer.run(DAY, run_id="r")

    assert written_rows == _rows(body)
    assert transformer.last_excluded_row_count == 0
    completion = read_completion(data, key, capture_id)
    assert completion is not None
    assert completion["outcome"] == "populated"
    assert completion["row_count"] == _rows(body)
    assert completion["rows_excluded"] == 0

    record = _record(key)
    frame = _silver(data, key)
    expected = [name for name, _type in generic.output_columns(record)]
    assert [c for c in frame.columns if c not in ("year", "month")] == [
        c for c in expected if c not in ("year", "month")
    ]
    assert not frame.select(list(record.entity_key)).is_duplicated().any()
    for column in record.epochs[0].columns:
        assert frame.schema[column.name] == POLARS_DTYPES[column.dtype], column.name

    published = _naive_utc(SIDECARS[key][5])
    assert frame["published_at"].unique().to_list() == [published]
    assert frame["available_at"].unique().to_list() == [published]
    assert frame["capture_written_at"].unique().to_list() == [WRITTEN]
    recipe = record.temporal
    if recipe.kind == "sp_pair":
        assert frame["timestamp_utc"].to_list() == [
            settlement_period_to_utc(d, p)
            for d, p in zip(
                frame[recipe.date_column].to_list(),
                frame[recipe.period_column].to_list(),
                strict=True,
            )
        ]
    else:
        assert recipe.kind == "date_sp1"
        assert frame["timestamp_utc"].to_list() == [
            settlement_period_to_utc(value, 1) for value in frame[recipe.date_column].to_list()
        ]


def test_dem1_record_shapes_match_the_unit_table() -> None:
    """Detects a record drifting from the unit spec: reader, one header epoch, no issue
    recipe, capture vintage, whole-capture selection, temporal recipe, the guard key (every
    column, never a value key), or eligibility (held questions are the FACTS TODOs).
    """
    for key in DEM1_KEYS:
        record = _record(key)
        kind, columns = TEMPORAL[key]
        assert record.reader == "csv", key
        assert len(record.epochs) == 1, key
        assert record.epochs[0].issue.kind == "none", key
        assert record.vintage == "ckan_last_modified", key
        assert record.latest == "whole_capture", key
        assert record.temporal.kind == kind, key
        assert record.temporal.inputs == columns, key
        assert record.entity_key == tuple(c.name for c in record.epochs[0].columns), key
        for column in record.epochs[0].columns:
            assert not column.null_tokens, (key, column.name)
            # only the temporal inputs exclude a row; every other blank is a null (no invented rule)
            assert column.nullable == (column.name not in record.temporal.inputs), (
                key,
                column.name,
            )
            if column.name == "settlement_period":  # the repo-wide 1..50 bound, never 1..48
                assert (column.min, column.max) == (1, 50), key
            else:  # no invented bounds on demand, flow or capacity values
                assert column.min is None and column.max is None, (key, column.name)
        if key in HELD:
            assert isinstance(record.eligibility, Held), key
            assert record.eligibility.unit == "E-SEM", key
            assert record.eligibility.question == HELD[key], key
        else:
            assert record.eligibility is None, key


def test_dem1_files_families_stay_documentation_only() -> None:
    """Detects a documentation-only ``_files`` family gaining a record. ``historic_demand``
    itself is DEM-1H's (per-resource selection, ADR-039; ``test_neso_dem1h_record.py``).
    """
    families = registry_module.load_registry().families
    for key in (
        "historic_demand_files",
        "daily_demand_update_files",
        "demand_forecast_1d_files",
        "demand_forecast_2d_files",
        "national_forecast_7d_files",
    ):
        assert families[key][1].record is None, key


def test_cp_clocks_cardinal_points_and_markers_stay_raw_text(data: Path) -> None:
    """Detects CP clocks cast to numbers or a UTC instant (the dictionary's UTC statement
    conflicts with the measured UK-local alignment), a zero-padded cardinal point such as
    ``03`` read as a number, and ``F_Point`` ``NA`` / ``Om`` / blank rewritten by a global
    null token.
    """
    for key in CURRENT_CP + HISTORIC_CP:
        raw = _table(dem1_body(key))
        frame = _run_one(data, key)
        for source, name in (
            ("CP_ST_TIME", "cp_st_time"),
            ("CP_END_TIME", "cp_end_time"),
            ("CARDINALPOINT", "cardinalpoint"),
            ("CP_TYPE", "cp_type"),
        ):
            assert frame.schema[name] == pl.Utf8, (key, name)
            assert frame[name].to_list() == [row[source] for row in raw], (key, name)
        assert frame.schema["daysahead"] == pl.Int64, key
        assert frame.schema["forecastdemand"] == pl.Float64, key
    historic = _run_one(data, "demand_forecast_1d_historic_day_ahead")
    assert "NA" in historic["f_point"].to_list()
    assert "03" in historic["cardinalpoint"].to_list()
    assert historic.schema["f_point"] == pl.Utf8
    seven = _run_one(data, "national_forecast_7d_historic_day_ahead")
    assert "Om" in seven["f_point"].to_list()
    # no global null token: a blank F_Point is the vendor's empty string, not an invented null
    assert "" in seven["f_point"].to_list()


def test_historic_forecast_timestamp_is_kept_raw_and_no_issue_time_is_derived(
    data: Path,
) -> None:
    """Detects the undocumented-zone ``FORECAST_TIMESTAMP`` being parsed as UTC into an
    issue instant (held until the zone and the recovery-time rows are settled): the column
    stays the vendor string, ``published_at`` stays the CKAN last_modified, and the
    1-day archive's several issues per target/CP survive as separate rows.
    """
    for key in HISTORIC_CP:
        frame = _run_one(data, key)
        assert frame.schema["forecast_timestamp"] == pl.Utf8, key
        assert frame["forecast_timestamp"].str.ends_with("Z").all(), key
        assert "issue_time" not in frame.columns, key
    one = _run_one(data, "demand_forecast_1d_historic_day_ahead")
    per_group = one.group_by(["daysahead", "targetdate", "cardinalpoint"]).agg(
        pl.col("forecast_timestamp").n_unique().alias("issues")
    )
    assert per_group["issues"].max() >= 2


def _forward_violations(key: str, body: bytes) -> list[date]:
    """Target dates of ``body`` that are not later than the sidecar's ckan_last_modified date.

    The property (RULINGS 529) that makes ``ckan_last_modified`` a valid issue proxy for a
    current cardinal-point file: it is published before the day it forecasts, so no row can
    be a hindsight value that the capture time pretends was available earlier.
    """
    modified = date.fromisoformat(SIDECARS[key][5][:10])
    targets = {datetime.strptime(row["TARGETDATE"], "%Y%m%d").date() for row in _table(body)}
    return sorted(t for t in targets if t <= modified)


@pytest.mark.parametrize("key", CURRENT_CP)
def test_current_cp_targets_are_later_than_the_capture_vintage(data: Path, key: str) -> None:
    """Detects a current CP fixture (or a record vintage) whose target date is not after
    the file's CKAN last_modified date, which would void the vintage-as-issue proxy; and
    that the pin itself fails on a past target.
    """
    body = dem1_body(key)
    assert _forward_violations(key, body) == []
    frame = _run_one(data, key, body)
    modified = date.fromisoformat(SIDECARS[key][5][:10])
    assert (frame["targetdate"] > modified).all()

    first = _table(body)[0]["TARGETDATE"]
    stale = body.replace(f",{first},".encode(), b",20261001,", 1)
    assert _forward_violations(key, stale) == [date(2026, 10, 1)]


def test_half_hourly_gdatetime_is_the_utc_label_and_timestamp_is_the_local_day_start(
    data: Path,
) -> None:
    """Detects ``GDATETIME`` read as local time, shifted by half an hour, or used as the
    temporal anchor: ``gdatetime`` must equal the raw UTC label, ``ctime`` stay text, and
    ``timestamp_utc`` be the UK-local date's start (``date_sp1(date)``).
    """
    key = "national_demand_fc_2_14d_days_ahead_half"
    body = dem1_body(key)
    raw = _table(body)
    frame = _run_one(data, key, body)
    assert frame["gdatetime"].to_list() == [_naive_utc(row["GDATETIME"]) for row in raw]
    assert frame.schema["ctime"] == pl.Utf8
    assert frame.schema["gdatetime"] == pl.Datetime("us", "UTC")
    assert frame["timestamp_utc"].unique().to_list() == [datetime(2026, 10, 9, 23, 0, tzinfo=UTC)]
    assert frame["date"].unique().to_list() == [date(2026, 10, 10)]
    # the label of the last row of the labelled date (CTIME 2400) is next-day local midnight
    closing = frame.filter(pl.col("ctime") == "2400")
    assert closing["gdatetime"].to_list() == [datetime(2026, 10, 10, 23, 0, tzinfo=UTC)]
    assert frame.schema["nationaldemand"] == pl.Float64


def _synthetic_daily(body: bytes, **overrides: str) -> bytes:
    """``body`` plus a copy of its first row with ``overrides`` (header names) applied.

    The 2026-10-08 daily update spans 2026-09-01 -> 2026-10-15, all BST, so a GMT date
    cannot be cut from it: the extra row is derived in the test and the committed fixture
    stays real.
    """
    lines = body.decode("utf-8").split("\n")
    header = lines[0].split(",")
    cells = lines[1].split(",")
    for name, value in overrides.items():
        cells[header.index(name)] = value
    lines.insert(len(lines) - 1, ",".join(cells))
    return "\n".join(lines).encode("utf-8")


def test_daily_update_timestamp_is_the_uk_period_start_in_bst_and_gmt(data: Path) -> None:
    """Detects ``sp_pair`` read as UTC wall-clock (the BST rows would be an hour late), the
    period read as an end label, and a settlement date treated as UTC: period 1 of a BST
    date starts at 23:00Z the day before, of a GMT date at 00:00Z; period 50 exists on the
    25-hour autumn day.
    """
    key = "daily_demand_update"
    body = _synthetic_daily(
        _synthetic_daily(
            _synthetic_daily(
                dem1_body(key),
                SETTLEMENT_DATE="2026-12-01",
                SETTLEMENT_PERIOD="3",
                ND="19000",
            ),
            SETTLEMENT_DATE="2026-10-25",
            SETTLEMENT_PERIOD="50",
            ND="18000",
        ),
        SETTLEMENT_DATE="2026-10-26",
        SETTLEMENT_PERIOD="1",
        ND="17000",
    )
    frame = _run_one(data, key, body)
    by_pair = {
        (d, p): t
        for d, p, t in zip(
            frame["settlement_date"],
            frame["settlement_period"],
            frame["timestamp_utc"],
            strict=True,
        )
    }
    assert by_pair[(date(2026, 9, 1), 1)] == datetime(2026, 8, 31, 23, 0, tzinfo=UTC)
    assert by_pair[(date(2026, 9, 1), 2)] == datetime(2026, 8, 31, 23, 30, tzinfo=UTC)
    assert by_pair[(date(2026, 12, 1), 3)] == datetime(2026, 12, 1, 1, 0, tzinfo=UTC)
    assert by_pair[(date(2026, 10, 25), 50)] == datetime(2026, 10, 25, 23, 30, tzinfo=UTC)
    assert by_pair[(date(2026, 10, 26), 1)] == datetime(2026, 10, 26, 0, 0, tzinfo=UTC)


def test_daily_update_zeros_on_forecast_rows_survive_as_zero_not_null(data: Path) -> None:
    """Detects the forecast-tail zero ND/TSD being nulled (an invented missing-value rule),
    dropped, or the A/F indicator being lost: the family is held on that question, so the
    values must reach silver exactly as the vendor wrote them, with a ``-1`` and a ``0``
    in a signed flow column surviving as numbers.
    """
    key = "daily_demand_update"
    body = _synthetic_daily(dem1_body(key), EAST_WEST_FLOW="-1", SETTLEMENT_PERIOD="9")
    frame = _run_one(data, key, body)
    forecast = frame.filter(pl.col("forecast_actual_indicator") == "F")
    assert forecast.height > 0
    zeros = forecast.filter(pl.col("nd") == 0.0)
    assert zeros.height > 0
    assert zeros["tsd"].to_list() == [0.0] * zeros.height
    assert frame.schema["nd"] == pl.Float64 and frame.schema["tsd"] == pl.Float64
    assert frame["nd"].null_count() == 0 and frame["tsd"].null_count() == 0
    assert frame.schema["forecast_actual_indicator"] == pl.Utf8
    assert set(frame["forecast_actual_indicator"].unique().to_list()) == {"A", "F"}
    assert -1.0 in frame["east_west_flow"].to_list()
    assert 0.0 in frame["east_west_flow"].to_list() or 0.0 in frame["ifa_flow"].to_list()
    assert frame.height == _rows(body)
