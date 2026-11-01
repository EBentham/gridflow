"""The three legacy NESO families on frozen records (ADR-034 amendment: P-8 superseded).

Daily wind availability, historic generation mix and the embedded forecast run
through the generic engine on the COMMITTED registry records, like every other
recorded family. Each test writes captures under a short temp data root (the
generic output name passes Windows MAX_PATH only under a short root) and reads
the engine's own output and completion record; nothing touches
``C:/gridflow-data``.

The fixture bodies are the ones the retired bespoke tests used; their provenance
is disclosed beside each (``tests/fixtures/neso_data_portal/``).
"""

from __future__ import annotations

import tempfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
from _neso_generic_support import snapshot, write_capture

from gridflow.schemas.neso_data_portal import is_valid_settlement_period
from gridflow.silver.latest_views import LATEST_VIEW_SPECS
from gridflow.silver.neso_data_portal.completion import NesoCaptureFailedError, read_failure
from gridflow.silver.neso_data_portal.generic import GenericNesoTransformer
from gridflow.silver.registry import get_transformer_class
from gridflow.utils.time import settlement_period_to_utc

if TYPE_CHECKING:
    from collections.abc import Iterator

    import polars as pl

SOURCE = "neso_data_portal"
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal"
DWA = "daily_wind_availability"
HGM = "historic_generation_mix"
EWSF = "embedded_wind_solar_forecast"

# The committed registry's package and resource facts for each family.
FAMILIES: dict[str, dict[str, str]] = {
    DWA: {
        "package_slug": "daily-wind-availability",
        "package_id": "3758a0ed-6c96-4e36-88d0-107f5020ddf3",
        "resource_id": "7aa508eb-36f5-4298-820f-2fa6745ae2e7",
        "resource_name": "Daily Wind Availability",
        "resource_filename": "windavailability.csv",
    },
    HGM: {
        "package_slug": "historic-generation-mix",
        "package_id": "88313ae5-94e4-4ddc-a790-593554d8c6b9",
        "resource_id": "f93d1835-75bc-43e5-84ad-12472b180a98",
        "resource_name": "Historic GB Generation Mix",
        "resource_filename": "df_fuel_ckan.csv",
    },
    EWSF: {
        "package_slug": "embedded-wind-and-solar-forecasts",
        "package_id": "91c0c70e-0ef5-4116-b6fa-7ad084b5e0e8",
        "resource_id": "db6c038f-98af-4570-ab60-24d71ebd0ae5",
        "resource_name": "Embedded Solar and Wind Forecast",
        "resource_filename": "202608161825_embedded_forecast.csv",
    },
}

DAY = date(2026, 8, 16)
WRITTEN = datetime(2026, 8, 16, 18, 25, tzinfo=UTC)
LAST_MODIFIED = "2026-08-16T18:25:03.877001"
PUBLISHED = datetime(2026, 8, 16, 18, 25, 3, 877001, tzinfo=UTC)
ISSUE = datetime(2026, 8, 16, 18, 25, tzinfo=UTC)

DWA_HEADER = "BMU_ID,Date,MW\n"
EWSF_HEADER = (
    "DATE_GMT,TIME_GMT,SETTLEMENT_DATE,SETTLEMENT_PERIOD,EMBEDDED_WIND_FORECAST,"
    "EMBEDDED_WIND_CAPACITY,EMBEDDED_SOLAR_FORECAST,EMBEDDED_SOLAR_CAPACITY\n"
)


@pytest.fixture
def data() -> Iterator[Path]:
    """A short data root under the system temp directory."""
    with tempfile.TemporaryDirectory(prefix="nl", ignore_cleanup_errors=True) as root:
        yield Path(root)


def _fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes().replace(b"\r\n", b"\n")


def _capture(data: Path, key: str, body: bytes, **overrides: Any) -> Path:
    facts = {**FAMILIES[key], **overrides}
    body_path, _sidecar = write_capture(
        data,
        key,
        body=body,
        written_at=WRITTEN,
        ckan_last_modified=LAST_MODIFIED,
        **facts,
    )
    return body_path


def _run(data: Path, key: str) -> int:
    cls = get_transformer_class(SOURCE, key)
    assert cls is not None
    return cls(data).run(DAY, run_id="legacy-records")


def _only(data: Path, key: str) -> tuple[pl.DataFrame, dict[str, Any]]:
    state = snapshot(data, key)
    (frame,) = state["outputs"].values()
    (row,) = state["ledger"].values()
    return frame, row


def _failure_class(data: Path, key: str, body: Path) -> str:
    document = read_failure(data, key, body.relative_to(data).as_posix())
    assert document is not None
    return str(document["error_class"])


def _ewsf_row(settlement_date: date, period: int) -> str:
    stamp = f"{settlement_date.isoformat()}T00:00:00"
    return f"{stamp},18:30,{stamp},{period},468,6417,1323,23301\n"


@pytest.mark.parametrize(
    ("key", "date_column"),
    [(DWA, "availability_date"), (HGM, "timestamp_utc"), (EWSF, "settlement_date")],
)
def test_each_legacy_key_is_a_generated_transformer(key: str, date_column: str) -> None:
    """Detects a legacy key still routed to a bespoke class, or a lost date column or key."""
    cls = get_transformer_class(SOURCE, key)

    assert cls is not None and issubclass(cls, GenericNesoTransformer)
    assert date_column == cls.GENERATED_DATE_COLUMN
    assert cls.DATASET_VERSION == "1+e1"
    spec = LATEST_VIEW_SPECS[(SOURCE, key)]
    assert spec.tiebreak_columns == ("capture_written_at", "bronze_capture_id")


def test_daily_wind_parity(data: Path) -> None:
    """Detects a changed value, row count, vendor clock or D-25 day start on daily wind."""
    _capture(data, DWA, _fixture("daily_wind_availability.csv"))

    assert _run(data, DWA) == 6
    frame, row = _only(data, DWA)
    frame = frame.sort("availability_date", "bmu_id")
    assert frame["bmu_id"].to_list() == [
        "ABRTW-1",
        "AFTOW-1",
        "ABRTW-1",
        "ACHLW-1",
        "ACHRW-1",
        "ACHYW-1",
    ]
    assert frame["availability_mw"].to_list() == [110.75, 87.5, 120.5, 98.0, 45.25, 61.0]
    starts = dict(zip(frame["availability_date"], frame["timestamp_utc"], strict=False))
    assert starts[date(2026, 1, 15)] == datetime(2026, 1, 15, 0, 0, tzinfo=UTC)
    assert starts[date(2026, 8, 16)] == datetime(2026, 8, 15, 23, 0, tzinfo=UTC)
    assert frame["event_time"].equals(frame["timestamp_utc"], check_names=False)
    assert set(frame["published_at"]) == set(frame["available_at"]) == {PUBLISHED}
    assert "data_provider" not in frame.columns
    assert (row["outcome"], row["row_count"], row["rows_excluded"]) == ("populated", 6, 0)


def test_historic_generation_mix_parity(data: Path) -> None:
    """Detects a lost or renamed vendor column, a shifted UTC instant or a lost DST row."""
    _capture(data, HGM, _fixture("historic_generation_mix.csv"))

    assert _run(data, HGM) == 9
    frame, _row = _only(data, HGM)
    first = frame.sort("timestamp_utc").row(0, named=True)
    assert first["timestamp_utc"] == datetime(2009, 1, 1, 0, 0, tzinfo=UTC)
    assert (first["gas"], first["coal"], first["carbon_intensity"]) == (8367.0, 15037.0, 524.0)
    assert (first["gas_pct"], first["fossil_pct"]) == (24.9, 69.7)
    assert not [name for name in frame.columns if name.endswith("_perc")]
    assert sum(name.endswith("_pct") for name in frame.columns) == 16
    assert frame["datetime"].equals(frame["timestamp_utc"], check_names=False)
    assert frame["event_time"].equals(frame["timestamp_utc"], check_names=False)
    instants = set(frame["timestamp_utc"])
    for instant in (
        datetime(2026, 3, 29, 0, 30, tzinfo=UTC),
        datetime(2026, 3, 29, 1, 0, tzinfo=UTC),
        datetime(2026, 10, 25, 0, 30, tzinfo=UTC),
        datetime(2026, 10, 25, 1, 0, tzinfo=UTC),
    ):
        assert instant in instants
    assert len(instants) == 9
    assert set(frame["published_at"]) == {PUBLISHED}


def test_embedded_forecast_parity(data: Path) -> None:
    """Detects a lost filename issue time or an event time off the settlement pair."""
    _capture(data, EWSF, _fixture("embedded_forecast.csv"))

    assert _run(data, EWSF) == 20
    frame, _row = _only(data, EWSF)
    assert set(frame["issue_time"]) == {ISSUE}
    expected = [
        settlement_period_to_utc(row["settlement_date"], row["settlement_period"])
        for row in frame.iter_rows(named=True)
    ]
    assert frame["event_time"].to_list() == expected
    assert frame["timestamp_utc"].equals(frame["event_time"], check_names=False)
    assert set(frame["published_at"]) == {PUBLISHED}


@pytest.mark.parametrize(
    ("name", "periods"),
    [("embedded_forecast_dst_autumn.csv", 50), ("embedded_forecast_dst_spring.csv", 46)],
)
def test_embedded_forecast_dst_days_keep_distinct_instants(
    data: Path, name: str, periods: int
) -> None:
    """Detects a wall-clock conversion collapsing or repeating a DST-day period."""
    _capture(data, EWSF, _fixture(name))

    assert _run(data, EWSF) == periods
    frame, _row = _only(data, EWSF)
    assert sorted(frame["settlement_period"]) == list(range(1, periods + 1))
    instants = sorted(frame["event_time"])
    assert len(set(instants)) == periods
    assert instants[-1] - instants[0] == timedelta(minutes=30 * (periods - 1))


@pytest.mark.parametrize("day", [DAY, date(2026, 3, 29), date(2026, 10, 25)])
def test_embedded_forecast_out_of_calendar_periods_are_excluded_and_counted(
    data: Path, day: date
) -> None:
    """Detects a kept SP0 or SP49 (a neighbouring day's event time) or a hidden exclusion.

    Every period in -2..52 on a 48-, 46- and 50-period day: the survivors are
    exactly what D-27's two-sided predicate accepts, and the completion counts
    the rest (the bespoke completion always said 0).
    """
    periods = range(-2, 53)
    _capture(data, EWSF, (EWSF_HEADER + "".join(_ewsf_row(day, p) for p in periods)).encode())
    kept = [p for p in periods if is_valid_settlement_period(day, p)]

    assert _run(data, EWSF) == len(kept)
    frame, row = _only(data, EWSF)
    assert sorted(frame["settlement_period"]) == kept
    assert (row["row_count"], row["rows_excluded"]) == (len(kept), len(periods) - len(kept))


def test_daily_wind_duplicate_key_fails_the_capture(data: Path) -> None:
    """Detects a repeated (bmu_id, Date) silently collapsed to one row."""
    body = _capture(
        data, DWA, (DWA_HEADER + "ABRTW-1,2026-08-16,1\nABRTW-1,2026-08-16,2\n").encode()
    )

    with pytest.raises(NesoCaptureFailedError):
        _run(data, DWA)
    assert snapshot(data, DWA) == {"outputs": {}, "ledger": {}}
    assert _failure_class(data, DWA, body) == "DuplicateEntityKeyError"


def test_historic_generation_mix_offset_datetime_fails_the_capture(data: Path) -> None:
    """Detects an offset-bearing DATETIME read as naive UTC (shifting every row)."""
    header, first, *_rest = _fixture("historic_generation_mix.csv").decode().splitlines()
    row = first.replace("2009-01-01T00:00:00", "2009-01-01T00:00:00Z", 1)
    body = _capture(data, HGM, f"{header}\n{row}\n".encode())

    with pytest.raises(NesoCaptureFailedError):
        _run(data, HGM)
    assert snapshot(data, HGM) == {"outputs": {}, "ledger": {}}
    assert _failure_class(data, HGM, body) == "InvalidOperationError"


def test_embedded_forecast_without_an_issue_token_fails_the_capture(data: Path) -> None:
    """Detects a fetch-clock substitute for a missing filename issue time."""
    body = _capture(
        data, EWSF, _fixture("embedded_forecast.csv"), resource_filename="embedded_forecast.csv"
    )

    with pytest.raises(NesoCaptureFailedError):
        _run(data, EWSF)
    assert snapshot(data, EWSF) == {"outputs": {}, "ledger": {}}
    assert _failure_class(data, EWSF, body) == "IssueTimeError"


@pytest.mark.parametrize(
    ("rows", "kept", "excluded"),
    [
        ("ABRTW-1,2026-08-16,\nACHLW-1,2026-08-16,98\n", 2, 0),
        (",2026-08-16,120.5\nACHLW-1,2026-08-16,98\n", 1, 1),
    ],
    ids=["null-value-kept", "null-key-excluded"],
)
def test_daily_wind_null_handling(data: Path, rows: str, kept: int, excluded: int) -> None:
    """Pins decision 51: a null value is kept uncounted; a null-key row is excluded and counted."""
    _capture(data, DWA, (DWA_HEADER + rows).encode())

    assert _run(data, DWA) == kept
    frame, row = _only(data, DWA)
    assert frame.height == kept
    assert (row["row_count"], row["rows_excluded"]) == (kept, excluded)
