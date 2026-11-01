"""One window convention: ``--end D`` fetches and transforms through D.

Inside gridflow every window is half-open ``[start, end)`` in UTC, and the CLI
turns a bare ``--end D`` into ``D+1 00:00Z``. The contract test below feeds each
connector the window the CLI resolves for ``--start A --end D`` and reads, from
the requests it actually sends (respx, no network), which days it asked the
vendor for: exactly A..D, never D+1. The same windows then drive the real
``run_transform`` and ``run_build``.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, ClassVar

import httpx
import polars as pl
import pytest
import respx
from typer.testing import CliRunner

from gridflow.cli import app
from gridflow.config.settings import DatasetConfig, SourceConfig, load_settings
from gridflow.connectors.elexon.client import ElexonConnector
from gridflow.connectors.entsoe.client import EntsoeConnector
from gridflow.connectors.entsog.client import EntsogConnector
from gridflow.connectors.gie.client import AgsiConnector, AlsiConnector
from gridflow.connectors.neso.carbon_intensity import CarbonIntensityConnector, _request_specs
from gridflow.connectors.neso.endpoints import ENDPOINTS as NESO_ENDPOINTS
from gridflow.connectors.openmeteo.client import OpenMeteoConnector
from gridflow.pipeline import runner
from gridflow.pipeline.runner import resolve_dates
from gridflow.silver.base import BaseSilverTransformer
from gridflow.storage.paths import PathBuilder
from gridflow.utils.time import date_range, window_dates

if TYPE_CHECKING:
    from collections.abc import Callable

    from gridflow.connectors.base import BaseConnector

FIXTURES = Path(__file__).parent.parent / "fixtures"
_JSON = {"content-type": "application/json"}


@pytest.fixture(autouse=True)
def _instant_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _no_sleep(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr("asyncio.sleep", _no_sleep)


def _with_key(source: str) -> SourceConfig:
    return (
        load_settings()
        .get_source_config(source)
        .model_copy(update={"api_key": "test-key", "rate_limit_per_second": 1000, "timeout": 5})
    )


def _elexon_config() -> SourceConfig:
    return SourceConfig(
        base_url="https://data.elexon.co.uk/bmrs/api/v1",
        rate_limit_per_second=1000,
        timeout=5,
        datasets={"system_prices": DatasetConfig(), "pn": DatasetConfig()},
    )


def _openmeteo_config() -> SourceConfig:
    return SourceConfig(
        base_url="",
        rate_limit_per_second=1000,
        timeout=5,
        datasets={"historical_demand": DatasetConfig(endpoint="/archive")},
    )


def _entsoe_config() -> SourceConfig:
    return SourceConfig(
        base_url="https://web-api.tp.entsoe.eu",
        api_key="test-token",
        api_key_header="",
        rate_limit_per_second=1000,
        timeout=5,
        datasets={"actual_load": DatasetConfig()},
    )


# --- responders: a minimal valid body for each vendor ----------------------


def _elexon_body(request: httpx.Request) -> httpx.Response:
    period = int(request.url.params.get("settlementPeriod", "1"))
    data = [] if period > 1 else [{"settlementDate": "2024-01-15", "settlementPeriod": 1}]
    return httpx.Response(200, json={"data": data}, headers=_JSON)


def _agsi_body(request: httpx.Request) -> httpx.Response:
    gas_day = request.url.params["date"]
    return httpx.Response(
        200,
        json={"last_page": 1, "total": 1, "gas_day": gas_day, "data": [{"gasDayStart": gas_day}]},
        headers=_JSON,
    )


def _alsi_body(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"last_page": 1, "total": 0, "data": []}, headers=_JSON)


def _openmeteo_body(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"hourly": {"time": []}}, headers=_JSON)


def _neso_body(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"data": []}, headers=_JSON)


def _entsoe_body(_request: httpx.Request) -> httpx.Response:
    xml = (FIXTURES / "entsoe" / "actual_load_gb.xml").read_bytes()
    return httpx.Response(200, content=xml, headers={"content-type": "application/xml"})


def _entsog_body(_request: httpx.Request) -> httpx.Response:
    return httpx.Response(200, json={"operationalData": []}, headers=_JSON)


# --- extractors: the days a request asked the vendor for -------------------


def _last_path_segment(request: httpx.Request) -> set[date]:
    return {date.fromisoformat(request.url.path.rsplit("/", 1)[-1])}


def _param(name: str) -> Callable[[httpx.Request], set[date]]:
    return lambda request: {date.fromisoformat(request.url.params[name])}


def _inclusive_params(first: str, last: str) -> Callable[[httpx.Request], set[date]]:
    def days(request: httpx.Request) -> set[date]:
        params = request.url.params
        return set(date_range(date.fromisoformat(params[first]), date.fromisoformat(params[last])))

    return days


def _entsoe_period(request: httpx.Request) -> set[date]:
    """``periodEnd`` is exclusive: a request up to D+1 00:00 asks for D only."""

    def parse(value: str) -> datetime:
        return datetime.strptime(value, "%Y%m%d%H%M").replace(tzinfo=UTC)

    params = request.url.params
    return set(window_dates(parse(params["periodStart"]), parse(params["periodEnd"])))


CASES: list[tuple[str, Callable[[], BaseConnector], str, str, Any, Any]] = [
    (
        "elexon system_prices",
        lambda: ElexonConnector(_elexon_config()),
        "system_prices",
        r"https://data\.elexon\.co\.uk/.*",
        _elexon_body,
        _last_path_segment,
    ),
    (
        "elexon pn",
        lambda: ElexonConnector(_elexon_config()),
        "pn",
        r"https://data\.elexon\.co\.uk/.*",
        _elexon_body,
        _param("settlementDate"),
    ),
    (
        "gie_agsi storage",
        lambda: AgsiConnector(_with_key("gie_agsi")),
        "storage",
        r"https://agsi\.gie\.eu/.*",
        _agsi_body,
        _param("date"),
    ),
    (
        "gie_alsi lng",
        lambda: AlsiConnector(_with_key("gie_alsi")),
        "lng",
        r"https://alsi\.gie\.eu/.*",
        _alsi_body,
        _inclusive_params("from", "till"),
    ),
    (
        "open_meteo historical_demand",
        lambda: OpenMeteoConnector(_openmeteo_config()),
        "historical_demand",
        r"https://.*open-meteo\.com/.*",
        _openmeteo_body,
        _inclusive_params("start_date", "end_date"),
    ),
    (
        "neso intensity_date",
        lambda: CarbonIntensityConnector(_with_key("neso")),
        "intensity_date",
        r"https://api\.carbonintensity\.org\.uk/.*",
        _neso_body,
        _last_path_segment,
    ),
    (
        "entsoe actual_load",
        lambda: EntsoeConnector(_entsoe_config()),
        "actual_load",
        r"https://web-api\.tp\.entsoe\.eu/.*",
        _entsoe_body,
        _entsoe_period,
    ),
    (
        "entsog physical_flows",
        lambda: EntsogConnector(_with_key("entsog")),
        "physical_flows",
        r"https://transparency\.entsog\.eu/.*",
        _entsog_body,
        _inclusive_params("from", "to"),
    ),
]


D = datetime(2024, 1, 15, tzinfo=UTC)
WINDOWS: list[tuple[str, Callable[[], tuple[datetime, datetime]], list[date]]] = [
    ("[D, D+1)", lambda: (D, D + timedelta(days=1)), [date(2024, 1, 15)]),
    (
        "--start D --end D",
        lambda: resolve_dates("2024-01-15", "2024-01-15", None, 24),
        [date(2024, 1, 15)],
    ),
    (
        "--start D --end D+2",
        lambda: resolve_dates("2024-01-15", "2024-01-17", None, 24),
        [date(2024, 1, 15), date(2024, 1, 16), date(2024, 1, 17)],
    ),
]


@pytest.mark.parametrize(
    ("window", "expected"),
    [case[1:] for case in WINDOWS],
    ids=[case[0] for case in WINDOWS],
)
@pytest.mark.parametrize(
    ("connector", "dataset", "url", "respond", "days_of"),
    [case[1:] for case in CASES],
    ids=[case[0] for case in CASES],
)
@pytest.mark.asyncio
async def test_end_date_fetches_through_that_day(
    connector: Callable[[], BaseConnector],
    dataset: str,
    url: str,
    respond: Callable[[httpx.Request], httpx.Response],
    days_of: Callable[[httpx.Request], set[date]],
    window: Callable[[], tuple[datetime, datetime]],
    expected: list[date],
) -> None:
    start, end = window()

    with respx.mock(assert_all_called=True) as router:
        router.get(url__regex=url).mock(side_effect=respond)
        async with connector() as client:
            await client.fetch(dataset, start, end)
        requested = set().union(*(days_of(call.request) for call in router.calls))

    assert sorted(requested) == expected


class _RecordingTransformer(BaseSilverTransformer):
    """Records the dates the real ``run_transform`` asks it to transform."""

    source = "test"
    dataset = "window"
    seen: ClassVar[list[date]] = []

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        return pl.DataFrame()

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        return raw_df

    def run(self, target_date: date, run_id: str | None = None, reingest: bool = False) -> int:
        type(self).seen.append(target_date)
        return 1


@pytest.mark.parametrize(
    ("window", "expected"),
    [case[1:] for case in WINDOWS],
    ids=[case[0] for case in WINDOWS],
)
def test_end_date_transforms_and_builds_through_that_day(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    window: Callable[[], tuple[datetime, datetime]],
    expected: list[date],
) -> None:
    """The same window drives the real ``run_transform`` and ``run_build``."""
    built: list[tuple[date, date]] = []
    builder = SimpleNamespace(run=lambda first, last: built.append((first, last)) or 0)
    tracker = SimpleNamespace(
        run_id="probe",
        complete=lambda **_kwargs: None,
        complete_with_warnings=lambda **_kwargs: None,
        fail=lambda _message: None,
    )
    _RecordingTransformer.seen = []
    monkeypatch.setattr(
        "gridflow.silver.registry.get_transformer",
        lambda *_args: _RecordingTransformer(tmp_path),
    )
    monkeypatch.setattr("gridflow.gold.registry.get_builder", lambda *_args: builder)
    monkeypatch.setattr("gridflow.observability.PipelineRunTracker", lambda *_args: tracker)
    settings = SimpleNamespace(pipeline=SimpleNamespace(data_dir=tmp_path, write_silver_csv=False))
    ctx = runner.PipelineContext(con=None, settings=settings)  # type: ignore[arg-type]
    start, end = window()

    [transformed] = runner.run_transform(ctx, "test", ["window"], start, end)
    [gold] = runner.run_build(ctx, [runner.GOLD_DATASETS[0]], start, end)

    assert transformed.ok, transformed.error
    assert _RecordingTransformer.seen == expected
    assert gold.ok, gold.error
    assert built == [(expected[0], expected[-1])]


def test_neso_daily_endpoint_fetches_the_window_last_day() -> None:
    """An incremental window ending at 10:00 today used to stop at yesterday."""
    specs = _request_specs(
        NESO_ENDPOINTS["intensity_date"],
        datetime(2026, 3, 1, 10, tzinfo=UTC),
        datetime(2026, 3, 5, 10, tzinfo=UTC),
    )

    assert [window_start.date() for window_start, _end, _overrides in specs] == [
        date(2026, 3, day) for day in range(1, 6)
    ]


def test_one_day_cli_backfill_writes_and_transforms_that_day(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``backfill --start D --end D`` ran zero chunks; it is now one chunk that
    writes bronze to D alone and transforms D."""
    data_dir = tmp_path / "data"
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(tmp_path / "gridflow.duckdb"))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("ELEXON_API_KEY", "test-key")
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    body = (FIXTURES / "elexon" / "system_prices_response.json").read_text()

    with respx.mock(assert_all_called=True) as router:
        router.get(url__regex=r"https://data\.elexon\.co\.uk/.*").mock(
            return_value=httpx.Response(200, text=body, headers=_JSON)
        )
        result = CliRunner().invoke(
            app,
            ["backfill", "elexon", "system_prices", "--start", "2024-01-15", "--end", "2024-01-15"],
        )
        paths = [call.request.url.path for call in router.calls]

    assert result.exit_code == 0, result.output
    assert paths == ["/bmrs/api/v1/balancing/settlement/system-prices/2024-01-15"]
    assert "Chunk 1: 2024-01-15 to 2024-01-15" in result.output
    bronze = PathBuilder(data_dir).bronze_dir("elexon", "system_prices")
    partitions = {
        p.parent.relative_to(bronze).as_posix()
        for p in bronze.rglob("*.json")
        if not p.name.endswith(".meta.json")
    }
    assert partitions == {"2024/01/15"}
    silver = list((data_dir / "silver" / "elexon" / "system_prices").rglob("*.parquet"))
    assert len(silver) == 1
    assert silver[0].name.startswith("system_prices_20240115")
