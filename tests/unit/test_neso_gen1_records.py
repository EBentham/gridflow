"""The wind / margin forecast frozen records (v0.22-K-GEN-1): fourteen families.

Every test writes recorded fixture captures (slices of the 2026-10-08 swept bronze,
``tests/fixtures/neso_data_portal/gen1/``) into a short data root and runs the transformer the
**real package registry** generates, so a record that does not fit its vendor body fails here,
not at activation. On master none of the fourteen families has a record, so ``get_transformer``
raises for every key below.

Units (K-GEN-1-FACTS g4; a ``ColumnSpec`` has no unit field, so they are recorded here): the
wind ``Capacity`` / ``Wind_Forecast`` / ``Incentive_forecast`` columns, the daily NRAPM margin
and every OPMR value column are MW; settlement periods, ``ENG_Week``/``ENG_Year`` and ``CP`` are
identifiers; the two weekly NRAPM value columns have no vendor unit or scale (the hold).

Fixture cuts (a scratch script, not committed): the two ordinary 14-day bodies keep their UTF-8
BOM; the two ordinary-14-day, day-ahead and metered windfarm BMU bodies hold their first two
settlement slots; each of the eight historic BMU resources holds its first two slots, one July
slot and one December/January slot (<= 40 generators each); the historic national body holds
its first 40 rows, a window around its first target carrying two ``Forecast_Timestamp`` labels
and its last 20 rows; the two OPMR bodies hold their header, their first 40 rows, the last 24,
the rows with blank cells (daily) and the vendor's trailing lone ``\\r`` line. ``git`` and
``core.autocrlf`` normalise a committed fixture's line endings, so :func:`body` rebuilds every
record with the bronze original's convention and no test asserts on a fixture's raw bytes.

``Maximum IC Export `` and ``National Surplus `` carry a trailing space in the real OPMR
headers, but the generic reader strips header names before the epoch match (``readers.py``
``read_csv_body``; ``csv_bronze._assert_header_contract``), so the records freeze the stripped
spelling the engine matches. The deviation from K-GEN-1-FACTS' "Executor correction" is pinned
by :func:`test_opmr_trailing_space_headers_type_through_the_stripped_epoch`.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import tempfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import duckdb
import polars as pl
import pytest
from _neso_generic_support import write_capture

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.eligibility import effective_eligibility
from gridflow.connectors.neso_data_portal.registry import Eligible, Held
from gridflow.silver.latest_views import LATEST_VIEW_SPECS, select_latest_vintage
from gridflow.silver.neso_data_portal import generic
from gridflow.silver.neso_data_portal.completion import (
    capture_id_for,
    read_completion,
    scan_completions,
)
from gridflow.silver.neso_data_portal.reconcile import reconcile
from gridflow.silver.registry import get_transformer
from gridflow.storage.duckdb import init_catalogue

if TYPE_CHECKING:
    from collections.abc import Iterator

    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord

SOURCE = "neso_data_portal"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "gen1"
DAY = date(2026, 10, 8)
WRITTEN = datetime(2026, 10, 8, 12, 0, tzinfo=UTC)
LONDON = ZoneInfo("Europe/London")
BOM = b"\xef\xbb\xbf"

METERED = "metered_wind_forecast_14d"
METERED_FARM = "metered_wind_forecast_14d_windfarm"
DAYS_AHEAD = "wind_forecast_14d_days_ahead"
DAYS_AHEAD_BMU = "wind_forecast_14d_day_ahead_bmu"
DA = "da_wind_forecast_day_ahead"
DA_BMU = "da_wind_forecast_day_ahead_bmu"
HIST = "da_wind_forecast_historic_day_ahead"
HIST_BMU = "da_wind_forecast_historic_day_ahead_bmu"
NRAPM_N = "nrapm_forecast_national_daily_days"
NRAPM_S = "nrapm_forecast_scotland_daily_days"
WEEKLY_N = "nrapm_forecast_weekly_week_national"
WEEKLY_S = "nrapm_forecast_weekly_week_scotland"
DAILY_OPMR = "daily_opmr"
WEEKLY_OPMR = "weekly_opmr"
KEYS = (
    METERED,
    METERED_FARM,
    DAYS_AHEAD,
    DAYS_AHEAD_BMU,
    DA,
    DA_BMU,
    HIST,
    HIST_BMU,
    NRAPM_N,
    NRAPM_S,
    WEEKLY_N,
    WEEKLY_S,
    DAILY_OPMR,
    WEEKLY_OPMR,
)

PACKAGES: dict[str, tuple[str, str]] = {
    METERED: (
        "14-days-ahead-operational-metered-wind-forecasts",
        "ca6fc361-9099-4ab2-ac02-c959431e84bc",
    ),
    METERED_FARM: (
        "14-days-ahead-operational-metered-wind-forecasts",
        "ca6fc361-9099-4ab2-ac02-c959431e84bc",
    ),
    DAYS_AHEAD: ("14-days-ahead-wind-forecasts", "2f134a4e-92e5-43b8-96c3-0dd7d92fcc52"),
    DAYS_AHEAD_BMU: ("14-days-ahead-wind-forecasts", "2f134a4e-92e5-43b8-96c3-0dd7d92fcc52"),
    DA: ("day-ahead-wind-forecast", "fbe3701d-1487-443e-abe9-47a6c01ecce2"),
    DA_BMU: ("day-ahead-wind-forecast", "fbe3701d-1487-443e-abe9-47a6c01ecce2"),
    HIST: ("day-ahead-wind-forecast", "fbe3701d-1487-443e-abe9-47a6c01ecce2"),
    HIST_BMU: ("day-ahead-wind-forecast", "fbe3701d-1487-443e-abe9-47a6c01ecce2"),
    NRAPM_N: (
        "negative-reserve-active-power-margin-nrapm-forecast",
        "64151a33-3075-437d-bf11-fc00f297520e",
    ),
    NRAPM_S: (
        "negative-reserve-active-power-margin-nrapm-forecast",
        "64151a33-3075-437d-bf11-fc00f297520e",
    ),
    WEEKLY_N: (
        "negative-reserve-active-power-margin-nrapm-forecast",
        "64151a33-3075-437d-bf11-fc00f297520e",
    ),
    WEEKLY_S: (
        "negative-reserve-active-power-margin-nrapm-forecast",
        "64151a33-3075-437d-bf11-fc00f297520e",
    ),
    DAILY_OPMR: ("daily-opmr", "8e0c417f-ee54-4c8b-8ed2-0d64b042c6ed"),
    WEEKLY_OPMR: ("weekly-opmr", "025b9417-83a0-445b-a818-414ea49cf7c3"),
}

# fixture file -> (family, resource id, resource name, vendor resource_filename,
# ckan_last_modified): the real sidecar identities of the 2026-10-08 captures. The eight
# historic BMU resources are datastore dumps: their vendor filename is the bare resource id
# and their sidecar carries no ckan_last_modified.
SIDECARS: dict[str, tuple[str, str, str, str, str | None]] = {
    "14da_wind_forecast_all.csv": (
        METERED,
        "b1ae17c2-cc5e-4254-9bbf-3eddcf30d033",
        "14 Day Ahead Operational Metered Wind Forecast",
        "14da_wind_forecast_all.csv",
        "2026-10-08T10:16:12.426355",
    ),
    "14da_windunit_forecast_20261008_1115.csv": (
        METERED_FARM,
        "31e6d895-af48-41bd-a4db-28a78c7e9c4b",
        "14 Day Ahead Operational Metered Windfarm-Level Wind Forecast",
        "14da_windunit_forecast_20261008_1115.csv",
        "2026-10-08T10:16:24.243581",
    ),
    "14da_wind_forecast.csv": (
        DAYS_AHEAD,
        "93c3048e-1dab-4057-a2a9-417540583929",
        "14 Days Ahead Wind Forecast",
        "14da_wind_forecast.csv",
        "2026-10-08T11:30:47.254827",
    ),
    "14da_windunit_forecast.csv": (
        DAYS_AHEAD_BMU,
        "342aae25-d3a6-436c-b168-db8b247ccb83",
        "14 Day Ahead Wind BMU Forecast",
        "14da_windunit_forecast.csv",
        "2026-10-08T11:30:44.268960",
    ),
    "b2f03146-f05d-4824-a663-3a4f36090c71-20261008084006.csv": (
        DA,
        "b2f03146-f05d-4824-a663-3a4f36090c71",
        "Day Ahead Wind Forecast",
        "b2f03146-f05d-4824-a663-3a4f36090c71-20261008084006.csv",
        "2026-10-08T08:40:07.850057",
    ),
    "da_windunit_forecast_20261008083510.csv": (
        DA_BMU,
        "90e581e9-2d7f-4eaa-8fe0-6e323ee0f5f7",
        "Day Ahead Wind BMU Forecast",
        "da_windunit_forecast_20261008083510.csv",
        "2026-10-08T08:35:11.947478",
    ),
    "7524ec65-f782-4258-aaf8-5b926c17b966-20261008084501.csv": (
        HIST,
        "7524ec65-f782-4258-aaf8-5b926c17b966",
        "Historic Day Ahead Wind Forecasts",
        "7524ec65-f782-4258-aaf8-5b926c17b966-20261008084501.csv",
        "2026-10-08T08:45:11.458782",
    ),
    "9c26224d-ba6f-4fff-a21f-6f0090d8ac7d.csv": (
        HIST_BMU,
        "9c26224d-ba6f-4fff-a21f-6f0090d8ac7d",
        "Historic Day Ahead Wind BMU Forecasts 2018",
        "9c26224d-ba6f-4fff-a21f-6f0090d8ac7d",
        None,
    ),
    "11bc4219-0524-468d-8378-bba06c6114c4.csv": (
        HIST_BMU,
        "11bc4219-0524-468d-8378-bba06c6114c4",
        "Historic Day Ahead Wind BMU Forecasts 2019",
        "11bc4219-0524-468d-8378-bba06c6114c4",
        None,
    ),
    "ae7cd368-7fcf-47ab-a835-27a6ee7a679e.csv": (
        HIST_BMU,
        "ae7cd368-7fcf-47ab-a835-27a6ee7a679e",
        "Historic Day Ahead Wind BMU Forecasts 2020",
        "ae7cd368-7fcf-47ab-a835-27a6ee7a679e",
        None,
    ),
    "53587966-5b79-4a8c-8029-efe1c3d13601.csv": (
        HIST_BMU,
        "53587966-5b79-4a8c-8029-efe1c3d13601",
        "Historic Day Ahead Wind BMU Forecasts 2021",
        "53587966-5b79-4a8c-8029-efe1c3d13601",
        None,
    ),
    "411cfef3-2297-49b0-8426-476d913ba310.csv": (
        HIST_BMU,
        "411cfef3-2297-49b0-8426-476d913ba310",
        "Historic Day Ahead Wind BMU Forecasts 2022",
        "411cfef3-2297-49b0-8426-476d913ba310",
        None,
    ),
    "b8a784ac-f6fe-4f93-a965-ce6a32df0963.csv": (
        HIST_BMU,
        "b8a784ac-f6fe-4f93-a965-ce6a32df0963",
        "Historic Day Ahead Wind BMU Forecasts 2023",
        "b8a784ac-f6fe-4f93-a965-ce6a32df0963",
        None,
    ),
    "22625e0b-f63b-4d0e-9bbd-54383fc3e931.csv": (
        HIST_BMU,
        "22625e0b-f63b-4d0e-9bbd-54383fc3e931",
        "Historic Day Ahead Wind BMU Forecasts 2024",
        "22625e0b-f63b-4d0e-9bbd-54383fc3e931",
        None,
    ),
    "81413643-508e-4358-8414-c87e7c4bcc43.csv": (
        HIST_BMU,
        "81413643-508e-4358-8414-c87e7c4bcc43",
        "Historic Day Ahead Wind BMU Forecasts 2025",
        "81413643-508e-4358-8414-c87e7c4bcc43",
        None,
    ),
    "dailynrapmresult.csv": (
        NRAPM_N,
        "4c391daa-14e2-4991-a476-5539033ebb3d",
        "National Daily 2-14 days NRAPM Forecast",
        "dailynrapmresult.csv",
        "2026-10-08T11:04:46.768036",
    ),
    "scotdailynrapmresult.csv": (
        NRAPM_S,
        "c2861f81-b400-4456-870e-d51a4b530331",
        "Scotland Daily 2-14 days NRAPM Forecast",
        "scotdailynrapmresult.csv",
        "2026-10-08T10:55:55.308683",
    ),
    "weeklynrapmresult.csv": (
        WEEKLY_N,
        "42377a3d-e202-475f-9069-dcd4d1fae952",
        "Weekly 2-52 week National NRAPM Forecast",
        "weeklynrapmresult.csv",
        "2026-10-08T10:46:06.065398",
    ),
    "scotweeklynrapmresult.csv": (
        WEEKLY_S,
        "88f3e766-fff2-418c-aef2-4b627d2cdabd",
        "Weekly 2-52 week Scotland NRAPM forecast",
        "scotweeklynrapmresult.csv",
        "2026-10-08T10:35:06.056281",
    ),
    "csv_opmr_daily.csv": (
        DAILY_OPMR,
        "0eede912-8820-4c66-a58a-f7436d36b95f",
        "Daily Operational Planning Margin Requirement",
        "csv_opmr_daily.csv",
        "2026-10-08T09:20:23.077566",
    ),
    "csv_opmr_weekly.csv": (
        WEEKLY_OPMR,
        "c08419f5-a28d-4e35-87a4-676d7eb05713",
        "Weekly Operational Planning Margin Requirement (OPMR)",
        "csv_opmr_weekly.csv",
        "2026-10-08T11:20:53.625059",
    ),
}
HIST_BMU_FILES = tuple(name for name, (fam, *_rest) in SIDECARS.items() if fam == HIST_BMU)
OPMR_FILES = ("csv_opmr_daily.csv", "csv_opmr_weekly.csv")

# The bronze originals' line convention: CRLF, except the day-ahead / historic / BMU-archive
# bodies and both OPMR bodies (LF; the OPMR originals end in a lone ``\r\n`` blank line).
CRLF_FAMILIES = frozenset({METERED, METERED_FARM, DAYS_AHEAD, DAYS_AHEAD_BMU}) | {
    NRAPM_N,
    NRAPM_S,
    WEEKLY_N,
    WEEKLY_S,
}

# family -> (temporal kind, temporal inputs, entity key, issue column or None, held question)
_NO_CHANGE = None
HELD: dict[str, str] = {
    DAYS_AHEAD: (
        "TODO: 3 target rows sit before both the stated production instant and the CKAN "
        "vintage; whether the body mixes retrospective values with forecasts, and whether "
        "ForecastDateTime dates the availability of every row"
    ),
    DAYS_AHEAD_BMU: (
        "TODO: 693 target rows sit at or before the CKAN vintage and the body has no issue "
        "column; there is no evidence of when each value was available"
    ),
    HIST: (
        "TODO: Forecast_Timestamp has no vendor zone (its values carry no offset); 192 targets "
        "carry two labels and ~14k labels fall at or after target start; the issue zone and "
        "when each value was available are undefined"
    ),
    HIST_BMU: (
        "TODO: the vendor dictionary gives Timestamp as a UK-time publication instant, but no "
        "source shows the archived values are unchanged since that instant"
    ),
    WEEKLY_N: (
        "TODO: the load-factor and probability columns have no vendor unit or scale, and the "
        "dictionary titles and descriptions contradict each other"
    ),
    WEEKLY_S: (
        "TODO: the load-factor and probability columns have no vendor unit or scale, and the "
        "dictionary titles and descriptions contradict each other"
    ),
    DAILY_OPMR: (
        "TODO: history-bearing body (26,709 targets at or before the CKAN vintage); Publish "
        "Date is a date with no issue instant or zone"
    ),
    WEEKLY_OPMR: (
        "TODO: NESO's engineering year/week to target date mapping is undocumented; the body "
        "gives a publication date only"
    ),
}
ELIGIBLE = tuple(key for key in KEYS if key not in HELD)
FORWARD_ONLY = (METERED, METERED_FARM, DA, DA_BMU, NRAPM_N, NRAPM_S)
"""The six eligible families: every fixture target sits after the capture's CKAN vintage (the
forward-target rule, RULINGS 529)."""

TEMPORAL: dict[str, tuple[str, tuple[str, ...]]] = {
    METERED: ("utc_instant", ("datetime",)),
    METERED_FARM: ("utc_instant", ("datetime",)),
    DAYS_AHEAD: ("utc_instant", ("datetime",)),
    DAYS_AHEAD_BMU: ("utc_instant", ("datetime",)),
    DA: ("utc_instant", ("datetime_gmt",)),
    DA_BMU: ("utc_instant", ("datetime",)),
    HIST: ("utc_instant", ("datetime_gmt",)),
    HIST_BMU: ("utc_instant", ("datetime",)),
    NRAPM_N: ("date_sp1", ("date",)),
    NRAPM_S: ("date_sp1", ("date",)),
    WEEKLY_N: ("date_sp1", ("date",)),
    WEEKLY_S: ("date_sp1", ("date",)),
    DAILY_OPMR: ("date_sp1", ("date",)),
    WEEKLY_OPMR: ("none", ()),
}
ENTITY_KEYS: dict[str, tuple[str, ...]] = {
    METERED: ("datetime", "issue_time"),
    METERED_FARM: ("datetime", "generator_name"),
    DAYS_AHEAD: ("datetime", "issue_time"),
    DAYS_AHEAD_BMU: ("datetime", "generator_name"),
    DA: ("datetime_gmt",),
    DA_BMU: ("datetime", "generator_name"),
    HIST: ("datetime_gmt", "forecast_timestamp"),
    HIST_BMU: ("resource_id", "datetime", "generator_name", "issue_time"),
    NRAPM_N: ("date", "cp"),
    NRAPM_S: ("date", "cp"),
    WEEKLY_N: ("date", "week_number"),
    WEEKLY_S: ("date", "week_number"),
    DAILY_OPMR: ("publish_date", "date"),
    WEEKLY_OPMR: ("eng_year", "eng_week", "publish_date"),
}
ISSUE_COLUMN: dict[str, str] = {
    METERED: "forecast_datetime",
    DAYS_AHEAD: "forecast_datetime",
    HIST_BMU: "timestamp",
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
def data(monkeypatch: pytest.MonkeyPatch) -> Iterator[Path]:
    """A short data root; gold views are out of scope for these catalogues."""
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    with tempfile.TemporaryDirectory(
        prefix="g1", dir=_short_base(), ignore_cleanup_errors=True
    ) as root:
        yield Path(root)


def body(filename: str) -> bytes:
    """The fixture ``filename`` terminated by the bronze original's line convention."""
    family = SIDECARS[filename][0]
    raw = (FIXTURES / filename).read_bytes().replace(b"\r\n", b"\n")
    if filename in OPMR_FILES:
        # the vendor's trailing lone ``\r`` line: LF data lines, then one ``\r\n`` blank line
        return raw.rstrip(b"\n") + b"\n\r\n"
    if family in CRLF_FAMILIES:
        return raw.replace(b"\n", b"\r\n")
    return raw


def _table(raw: bytes) -> list[dict[str, str]]:
    text = raw.removeprefix(BOM).decode("utf-8")
    return list(csv.DictReader(io.StringIO(text, newline="")))


def _populated(raw: bytes) -> list[dict[str, str]]:
    """The body's rows with at least one non-blank cell (the reader drops the others)."""
    return [row for row in _table(raw) if any(value.strip() for value in row.values())]


def _rows(filename: str) -> int:
    return len(_populated(body(filename)))


def _filenames(family: str) -> list[str]:
    return [name for name, (fam, *_rest) in SIDECARS.items() if fam == family]


def _capture(data: Path, filename: str, *, raw: bytes | None = None) -> str:
    family, resource_id, name, vendor_filename, modified = SIDECARS[filename]
    slug, package_id = PACKAGES[family]
    # distinct write instants so eight captures of one family never share a body name
    written = WRITTEN + timedelta(seconds=list(SIDECARS).index(filename))
    path, _sidecar = write_capture(
        data,
        family,
        body=raw if raw is not None else body(filename),
        written_at=written,
        partition=DAY,
        package_slug=slug,
        package_id=package_id,
        resource_id=resource_id,
        resource_name=name,
        resource_filename=vendor_filename,
        ckan_last_modified=modified,
        url_type="datastore" if modified is None else "upload",
    )
    return capture_id_for(path, data)


def _record(key: str) -> SchemaRecord:
    record = registry_module.load_registry().families[key][1].record
    assert record is not None, key
    return record


def _silver(data: Path, key: str) -> pl.DataFrame:
    files = sorted((data / "silver" / SOURCE / key).rglob("[!.]*.parquet"))
    return pl.concat([pl.read_parquet(f, hive_partitioning=False) for f in files])


def _run(data: Path, key: str) -> dict[str, str]:
    """Capture every fixture of ``key`` and transform them; filename -> capture id."""
    ids = {filename: _capture(data, filename) for filename in _filenames(key)}
    get_transformer(SOURCE, key, data).run(DAY, run_id="r")
    return ids


def _naive_utc(stamp: str) -> datetime:
    return datetime.fromisoformat(stamp).replace(tzinfo=UTC)


@pytest.mark.parametrize("key", KEYS)
def test_every_fixture_types_with_no_exclusion(data: Path, key: str) -> None:
    """Detects a family without a generated transformer, a header that matches no epoch (a
    BOM glued to the first name, a trailing-space OPMR header), a cast the vendor body does
    not satisfy, a clock taken from anywhere but the declared recipe, and any row excluded:
    every capture completes with all its populated rows."""
    transformer = get_transformer(SOURCE, key, data)
    ids = {filename: _capture(data, filename) for filename in _filenames(key)}
    written = transformer.run(DAY, run_id="r")
    assert written == sum(_rows(filename) for filename in ids)
    assert transformer.last_excluded_row_count == 0
    for filename, capture_id in ids.items():
        completion = read_completion(data, key, capture_id)
        assert completion is not None, filename
        assert completion["outcome"] == "populated", filename
        assert completion["row_count"] == _rows(filename), filename
        assert completion["rows_excluded"] == 0, filename

    record = _record(key)
    frame = _silver(data, key)
    expected = [name for name, _type in generic.output_columns(record)]
    assert [c for c in frame.columns if c not in ("year", "month")] == [
        c for c in expected if c not in ("year", "month")
    ]
    for column in record.epochs[0].columns:
        assert frame.schema[column.name] == POLARS_DTYPES[column.dtype], column.name
    assert frame["timestamp_utc"].null_count() == 0


@pytest.mark.parametrize("key", KEYS)
def test_entity_key_is_unique_in_the_output(data: Path, key: str) -> None:
    """Detects an entity key that does not identify the output grain: the key must be unique
    across everything the family wrote (the engine's duplicate guard would otherwise fail the
    real bodies), and the fixture must be non-trivial."""
    _run(data, key)
    silver = _silver(data, key)
    assert silver.height > 0
    assert silver.select(list(ENTITY_KEYS[key])).is_duplicated().sum() == 0


@pytest.mark.parametrize("key", KEYS)
def test_record_shapes_match_the_unit_table(key: str) -> None:
    """Detects a record drifting from the unit spec: csv reader, utf-8, one header epoch,
    whole-capture selection, the temporal recipe, the entity key, the issue recipe, the
    vintage, eligibility; and that only temporal inputs and issue columns exclude rows (every
    other column nullable, no invented null token, no bound but the settlement period's 1..50)."""
    record = _record(key)
    assert record.reader == "csv"
    assert record.encoding == "utf-8"
    assert len(record.epochs) == 1
    assert record.latest == "whole_capture"
    assert (record.temporal.kind, record.temporal.inputs) == TEMPORAL[key]
    assert record.entity_key == ENTITY_KEYS[key]
    issue = record.epochs[0].issue
    if key in ISSUE_COLUMN:
        assert (issue.kind, issue.column) == ("data_column", ISSUE_COLUMN[key])
    else:
        assert issue.kind == "none"
    if key == HIST_BMU:
        assert record.vintage == "capture_fallback"
        assert record.latest_partition == "resource_id"
    else:
        assert record.vintage == "ckan_last_modified"
        assert record.latest_partition is None
    required = set(TEMPORAL[key][1]) | ({ISSUE_COLUMN[key]} if key in ISSUE_COLUMN else set())
    for column in record.epochs[0].columns:
        assert column.nullable == (column.name not in required), (key, column.name)
        assert not column.null_tokens, (key, column.name)
        if column.name == "settlement_period":
            # the registry-wide 1..50 rule (CLAUDE.md; the pilot test); never an exclusion of null
            assert (column.dtype, column.min, column.max) == ("int64", 1, 50), key
        else:
            assert column.min is None and column.max is None, (key, column.name)
    package, family = registry_module.load_registry().families[key]
    effective = effective_eligibility(package, family)
    if key in HELD:
        assert isinstance(record.eligibility, Held)
        assert record.eligibility.unit == "E-SEM"
        assert record.eligibility.question == HELD[key]
        assert effective == record.eligibility
    else:
        # eligible records carry no per-output override and inherit the package's status
        assert record.eligibility is None
        assert isinstance(package.eligibility, Eligible)
        assert isinstance(effective, Eligible)


def test_eligible_set_is_the_six_forward_target_families() -> None:
    """Detects a hold or an eligibility flipped against RULINGS 542: six eligible families
    (metered 14-day x2, day-ahead wind x2, daily NRAPM x2) and eight held."""
    assert set(ELIGIBLE) == set(FORWARD_ONLY)
    assert len(HELD) == 8
    assert len(KEYS) == 14


@pytest.mark.parametrize("key", FORWARD_ONLY)
def test_eligible_fixture_targets_are_after_the_capture_vintage(data: Path, key: str) -> None:
    """Detects an eligible no-issue forecast whose captured targets are not all after the
    capture's CKAN ``last_modified`` (RULINGS 529: the vintage is then a safe issue proxy; the
    property DEM-1 pins for the cardinal-point forecasts)."""
    _run(data, key)
    frame = _silver(data, key)
    published = _naive_utc(next(v[4] for v in SIDECARS.values() if v[0] == key) or "")
    assert frame["published_at"].unique().to_list() == [published]
    assert frame["timestamp_utc"].min() > published
    assert frame.filter(pl.col("timestamp_utc") <= pl.col("published_at")).height == 0


@pytest.mark.parametrize("key", [DAYS_AHEAD, DAYS_AHEAD_BMU, DAILY_OPMR])
def test_held_fixtures_keep_targets_at_or_before_the_vintage(data: Path, key: str) -> None:
    """Detects a hold without its evidence: the three bodies held on past targets keep rows
    at or before the CKAN vintage in the fixture, so the forward-target property is shown to
    fail for them (and a cut that quietly dropped those rows would fail here)."""
    _run(data, key)
    frame = _silver(data, key)
    assert frame.filter(pl.col("timestamp_utc") <= pl.col("published_at")).height > 0


def test_bom_bodies_type_their_first_column(data: Path) -> None:
    """Detects a UTF-8 BOM glued to the first header name: both ordinary 14-day bodies start
    with ``EF BB BF``; the first column must still match the epoch (``Datetime``, never
    ``\\ufeffDatetime``) and type as a UTC instant with no null."""
    for filename in ("14da_wind_forecast.csv", "14da_windunit_forecast.csv"):
        assert body(filename).startswith(BOM), filename
    for key in (DAYS_AHEAD, DAYS_AHEAD_BMU):
        _run(data, key)
        frame = _silver(data, key)
        assert frame.schema["datetime"] == pl.Datetime("us", "UTC"), key
        assert frame["datetime"].null_count() == 0, key
        assert _record(key).epochs[0].columns[0].source == "Datetime"
    # the bodies without a BOM do not gain one
    assert not body("14da_wind_forecast_all.csv").startswith(BOM)


def test_national_issue_columns_declare_the_production_instant(data: Path) -> None:
    """Detects an issue column read in the wrong zone or not copied to ``issue_time``: both
    14-day national bodies stamp every row with one UTC production instant (``ForecastDatetime``
    / ``ForecastDateTime``, ``...Z``), and ``issue_time`` is that instant."""
    for key, stamp in ((METERED, "2026-10-08T09:52:34"), (DAYS_AHEAD, "2026-10-08T11:30:00")):
        _run(data, key)
        frame = _silver(data, key)
        assert frame["issue_time"].unique().to_list() == [_naive_utc(stamp)], key
        assert (frame["issue_time"] == frame["forecast_datetime"]).all(), key


def test_wind_values_keep_zeros_negatives_and_names_as_is(data: Path) -> None:
    """Detects a zero forecast turned null, a sentinel conversion, a clipped negative, and any
    normalisation of a generator name: names survive byte-identical and in order in all four
    current BMU-level bodies and all eight archive resources; zero ``Wind_Forecast`` /
    ``Capacity`` cells are kept as 0.0."""
    for key in (METERED_FARM, DAYS_AHEAD_BMU, DA_BMU, HIST_BMU):
        ids = _run(data, key)
        silver = _silver(data, key)
        for filename, capture_id in ids.items():
            rows = _populated(body(filename))
            part = silver.filter(pl.col("bronze_capture_id") == capture_id)
            assert part["generator_name"].to_list() == [r["Generator_Name"] for r in rows], filename
            assert part["generator_full_name"].to_list() == [
                r["Generator_Full_Name"] for r in rows
            ], filename
            assert part["wind_forecast"].to_list() == [float(r["Wind_Forecast"]) for r in rows]
            assert part["capacity"].to_list() == [float(r["Capacity"]) for r in rows]
    farm = _silver(data, METERED_FARM)
    assert (farm["wind_forecast"] == 0.0).sum() > 0
    assert (farm["capacity"] == 0.0).sum() > 0


def test_historic_bmu_resources_type_and_latest_returns_every_resource(data: Path) -> None:
    """Detects a per-family ``_latest`` (one resource surviving) in the catalogue or in
    Polars, a resource whose rows are lost, and ``resource_id`` missing from the output: all
    eight archive resources are captured, typed, and served by ``_latest`` with their own rows."""
    ids = _run(data, HIST_BMU)
    silver = _silver(data, HIST_BMU)
    resource_ids = {SIDECARS[f][1] for f in HIST_BMU_FILES}
    assert len(resource_ids) == 8
    assert set(silver["resource_id"].to_list()) == resource_ids
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    con = duckdb.connect(str(db), read_only=True)
    try:
        latest = con.execute(f'SELECT * FROM "silver_{SOURCE}_{HIST_BMU}_latest"').pl()
    finally:
        con.close()
    assert set(latest["bronze_capture_id"].to_list()) == set(ids.values())
    assert set(latest["resource_id"].to_list()) == resource_ids
    files = sorted((data / "silver" / SOURCE / HIST_BMU).rglob("[!.]*.parquet"))
    polars = select_latest_vintage(
        pl.scan_parquet(files, hive_partitioning=False),
        LATEST_VIEW_SPECS[(SOURCE, HIST_BMU)],
        completions=scan_completions(data),
    ).collect()
    assert sorted(polars["bronze_capture_id"].to_list()) == sorted(
        latest["bronze_capture_id"].to_list()
    )
    assert latest.height == sum(_rows(f) for f in HIST_BMU_FILES)
    report = reconcile(data, registry_module.load_registry(), [HIST_BMU], DAY)
    assert report.clean, report.lines()


def test_historic_bmu_timestamp_converts_from_london_to_utc(data: Path) -> None:
    """Detects the publication ``Timestamp`` read as UTC (or as a fixed offset): it is UK
    time (vendor BMU dictionary), so a BST label is one hour ahead of UTC and a GMT label is
    equal. The fixtures hold both seasons, and every row is checked against an independent
    ``zoneinfo`` conversion."""
    ids = _run(data, HIST_BMU)
    silver = _silver(data, HIST_BMU)
    offsets: set[timedelta] = set()
    for filename, capture_id in ids.items():
        rows = _populated(body(filename))
        part = silver.filter(pl.col("bronze_capture_id") == capture_id)
        expected = []
        for row in rows:
            local = datetime.fromisoformat(row["Timestamp"]).replace(tzinfo=LONDON)
            offsets.add(local.utcoffset() or timedelta(0))
            expected.append(local.astimezone(UTC))
        assert part["issue_time"].to_list() == expected, filename
        assert part["timestamp"].to_list() == expected, filename
    assert offsets == {timedelta(0), timedelta(hours=1)}
    # the target instant is a UTC instant: no zone shift on ``Datetime``
    first = silver.row(0, named=True)
    assert first["timestamp_utc"] == first["datetime"]


def test_historic_bmu_dump_vintage_is_the_capture_instant(data: Path) -> None:
    """Detects a datastore dump taking the metadata or the publication ``Timestamp`` as its
    vintage (ADR-035): a ``capture_fallback`` family stamps no CKAN ``published_at`` and its
    ``available_at`` is the capture's own ``written_at``."""
    ids = _run(data, HIST_BMU)
    silver = _silver(data, HIST_BMU)
    assert silver["published_at"].null_count() == silver.height
    for filename, capture_id in ids.items():
        part = silver.filter(pl.col("bronze_capture_id") == capture_id)
        written = WRITTEN + timedelta(seconds=list(SIDECARS).index(filename))
        assert part["available_at"].unique().to_list() == [written], filename


def test_historic_national_keeps_both_labels_of_a_shared_target(data: Path) -> None:
    """Detects a deduplication on ``Datetime_GMT`` alone: 192 real targets carry two
    ``Forecast_Timestamp`` labels, the fixture keeps such a pair, both rows survive in the
    output and in ``_latest``, and the label stays the vendor's raw text (no zone invented)."""
    filename = "7524ec65-f782-4258-aaf8-5b926c17b966-20261008084501.csv"
    rows = _populated(body(filename))
    labels: dict[str, set[str]] = {}
    for row in rows:
        labels.setdefault(row["Datetime_GMT"], set()).add(row["Forecast_Timestamp"])
    shared = [target for target, seen in labels.items() if len(seen) > 1]
    assert shared, "the fixture must keep a target with two Forecast_Timestamp labels"

    _run(data, HIST)
    silver = _silver(data, HIST)
    assert silver.height == len(rows)
    assert silver.schema["forecast_timestamp"] == pl.Utf8
    assert silver["forecast_timestamp"].to_list() == [r["Forecast_Timestamp"] for r in rows]
    for target in shared:
        part = silver.filter(pl.col("datetime_gmt") == _naive_utc(target))
        assert sorted(part["forecast_timestamp"].to_list()) == sorted(labels[target])
    db = data / "cat.duckdb"
    init_catalogue(db, data)
    con = duckdb.connect(str(db), read_only=True)
    try:
        latest = con.execute(f'SELECT * FROM "silver_{SOURCE}_{HIST}_latest"').pl()
    finally:
        con.close()
    assert latest.height == len(rows)
    assert "issue_time" not in silver.columns


def test_da_wind_forecast_day_ahead_clock_has_no_z_suffix(data: Path) -> None:
    """Detects ``Datetime_GMT`` parsed with a ``Z`` suffix or as local time: the day-ahead
    body carries ``%Y-%m-%dT%H:%M:%S`` UTC with no suffix and period 1 is 23:00 UTC the day
    before its settlement date in BST."""
    _run(data, DA)
    silver = _silver(data, DA)
    assert silver["datetime_gmt"][0] == datetime(2026, 10, 8, 23, 0, tzinfo=UTC)
    assert silver["date"][0] == date(2026, 10, 9)
    assert silver["settlement_period"][0] == 1
    assert silver["timestamp_utc"].to_list() == silver["datetime_gmt"].to_list()


def test_opmr_trailing_space_headers_type_through_the_stripped_epoch(data: Path) -> None:
    """Detects a record that cannot match the real OPMR header. The vendor bodies write
    ``Maximum IC Export `` and ``National Surplus `` with a trailing space, but the reader
    strips header names before the epoch match, so the record freezes the stripped spelling
    (an exact-trailing-space source never matches: ``HeaderEpochError``). Both bodies' raw
    headers keep the space, both records name the stripped source, and the columns type."""
    for filename in OPMR_FILES:
        header = body(filename).split(b"\n", 1)[0]
        assert b"Maximum IC Export ," in header, filename
        assert b"National Surplus ," in header, filename
    for key, filename in ((DAILY_OPMR, "csv_opmr_daily.csv"), (WEEKLY_OPMR, "csv_opmr_weekly.csv")):
        sources = [c.source for c in _record(key).epochs[0].columns]
        assert "Maximum IC Export" in sources and "National Surplus" in sources, key
        assert "Maximum IC Export " not in sources and "National Surplus " not in sources, key
        _run(data, key)
        silver = _silver(data, key)
        rows = _populated(body(filename))
        assert silver.schema["maximum_ic_export"] == pl.Float64
        assert silver.schema["national_surplus"] == pl.Float64
        assert silver["maximum_ic_export"].to_list() == [
            float(r["Maximum IC Export "]) for r in rows
        ]
        assert silver["national_surplus"].to_list() == [float(r["National Surplus "]) for r in rows]


@pytest.mark.parametrize("filename", OPMR_FILES)
def test_opmr_blank_row_is_dropped_by_the_logged_reader_path(
    data: Path, filename: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Detects the vendor's trailing lone ``\\r`` line reaching the typed output, or being
    dropped silently: the reader's blank-row path removes it and logs one INFO record naming
    it, so ``rows_excluded`` stays 0 (a blank line is not an excluded data row) and the rows
    written equal the populated rows. Blank numeric cells in a populated row stay null."""
    key = SIDECARS[filename][0]
    raw = body(filename)
    assert raw.endswith(b"\n\r\n"), "the fixture must keep the vendor's trailing blank line"
    populated = _populated(raw)
    with caplog.at_level(logging.INFO, logger="gridflow.silver.csv_bronze"):
        capture_id = _capture(data, filename)
        transformer = get_transformer(SOURCE, key, data)
        written = transformer.run(DAY, run_id="r")
    assert written == len(populated)
    assert transformer.last_excluded_row_count == 0
    completion = read_completion(data, key, capture_id)
    assert completion is not None
    assert completion["rows_excluded"] == 0
    messages = [r.getMessage() for r in caplog.records if "blank row" in r.getMessage()]
    assert len(messages) == 1, messages
    assert messages[0].startswith("dropped 1 blank row(s)")
    silver = _silver(data, key)
    assert silver["maximum_ic_import"].null_count() == sum(
        1 for r in populated if not r["Maximum IC Import"].strip()
    )
    if key == DAILY_OPMR:
        blanks = [r for r in populated if not r["Operating Reserve provided by ICs"].strip()]
        assert blanks, "the daily fixture must keep rows with blank numeric cells"
        assert silver["operating_reserve_provided_by_ics"].null_count() == len(blanks)


def test_opmr_publish_date_is_a_date_and_not_an_issue(data: Path) -> None:
    """Detects ``Publish Date`` promoted to an issue instant (it is a date with no zone) or
    parsed with a time: the daily/weekly records type it as a date column, declare no issue,
    and emit no ``issue_time`` column; the weekly body keeps its engineering year/week as
    integers and has no target anchor (``timestamp_utc`` is the capture's own instant)."""
    for key in (DAILY_OPMR, WEEKLY_OPMR):
        _run(data, key)
        silver = _silver(data, key)
        assert silver.schema["publish_date"] == pl.Date
        assert "issue_time" not in silver.columns
        assert _record(key).epochs[0].issue.kind == "none"
    weekly = _silver(data, WEEKLY_OPMR)
    assert weekly.schema["eng_year"] == pl.Int64
    assert weekly.schema["eng_week"] == pl.Int64
    assert (
        weekly["timestamp_utc"].unique().to_list()
        == weekly["capture_written_at"].unique().to_list()
    )
    daily = _silver(data, DAILY_OPMR)
    assert daily.schema["date"] == pl.Date


def test_national_and_scotland_nrapm_stay_separate_series(data: Path) -> None:
    """Detects national and Scotland NRAPM merged, subtracted or deduplicated (GEN1-3): the
    two daily and the two weekly families are separate outputs with their own geographic
    value column, the same target keys and different values."""
    for national, scotland, value_n, value_s in (
        (
            NRAPM_N,
            NRAPM_S,
            "mw_away_from_risk_of_system_level_nrapm_forecast_conditions",
            "mw_away_from_risk_of_scotland_level_nrapm_forecast_conditions",
        ),
        (
            WEEKLY_N,
            WEEKLY_S,
            "wind_load_factor_required_to_cause_a_system_level_nrapm",
            "wind_load_factor_required_to_cause_a_scotland_level_nrapm",
        ),
    ):
        _run(data, national)
        _run(data, scotland)
        n, s = _silver(data, national), _silver(data, scotland)
        assert value_n in n.columns and value_n not in s.columns
        assert value_s in s.columns and value_s not in n.columns
        keys = list(ENTITY_KEYS[national])
        assert n.height == s.height > 0
        assert sorted(n.select(keys).rows()) == sorted(s.select(keys).rows())
    daily_n = _silver(data, NRAPM_N).sort("date", "cp")
    daily_s = _silver(data, NRAPM_S).sort("date", "cp")
    assert daily_n["mw_away_from_risk_of_system_level_nrapm_forecast_conditions"].to_list() != (
        daily_s["mw_away_from_risk_of_scotland_level_nrapm_forecast_conditions"].to_list()
    )
    # the daily CP label is kept as text and never turned into an instant
    assert daily_n.schema["cp"] == pl.Utf8
    assert set(daily_n["cp"].to_list()) == {"1B", "3B"}
