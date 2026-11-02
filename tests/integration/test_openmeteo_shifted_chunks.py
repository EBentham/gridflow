"""Open-Meteo silver across two backfills whose multi-day chunks start on different days.

Each chunk is one archive request per site, filed in bronze under the chunk's
first day. Silver is one file per UTC date, rewritten from the rows of that
date only, so a key can never land twice. A date with no bronze partition of
its own looks back for the request that reached it; before that look-back
checked each request's window, a nearer, shorter request (a one-day chunk, or
a run that ended earlier) left the date out of silver while the backfill
reported success. HTTP is answered locally; nothing is fetched.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx
import polars as pl
import pytest
import respx
from typer.testing import CliRunner

from gridflow import cli as cli_module
from gridflow.config.settings import load_settings
from gridflow.connectors.openmeteo.endpoints import (
    ARCHIVE_BASE_URL,
    DATASET_SPECS,
    DEMAND_LOCATIONS,
)
from gridflow.pipeline import runner
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from pathlib import Path

    from conftest import IsolatedPaths

pytestmark = pytest.mark.integration
cli = CliRunner()


def _archive_reply(request: httpx.Request) -> httpx.Response:
    """Every hour of ``start_date..end_date`` (inclusive), as the archive API answers."""
    params = request.url.params
    first = date.fromisoformat(params["start_date"])
    last = date.fromisoformat(params["end_date"])
    hours = [
        datetime(first.year, first.month, first.day, tzinfo=UTC) + timedelta(hours=h)
        for h in range(((last - first).days + 1) * 24)
    ]
    variables = params["hourly"].split(",")
    return httpx.Response(
        200,
        json={
            "latitude": float(params["latitude"]),
            "longitude": float(params["longitude"]),
            "hourly": {
                "time": [t.strftime("%Y-%m-%dT%H:%M") for t in hours],
                **{v: [10.0] * len(hours) for v in variables},
            },
        },
    )


def _day(n: int) -> datetime:
    return datetime(2024, 1, 1, tzinfo=UTC) + timedelta(days=n)


def _assert_every_hour_once(data_dir: Path, union_start: datetime, union_end: datetime) -> None:
    """Every (hour, demand site) of ``[union_start, union_end)`` is in silver exactly once."""
    silver = PathBuilder(data_dir).silver_dir("open_meteo", "historical_demand")
    frame = pl.read_parquet(str(silver / "**" / "*.parquet"))
    per_key = frame.group_by("timestamp_utc", "location").len()
    assert per_key["len"].max() == 1

    days = [(union_start + timedelta(days=n)).date() for n in range((union_end - union_start).days)]
    landed = per_key.group_by(pl.col("timestamp_utc").dt.date().alias("day")).agg(
        pl.col("len").sum()
    )
    counts = dict(zip(landed["day"].to_list(), landed["len"].to_list(), strict=True))
    sites = len(DEMAND_LOCATIONS)
    assert {d: counts.get(d, 0) for d in days} == dict.fromkeys(days, 24 * sites)


@pytest.fixture
def fast_cli_settings(isolated_env: IsolatedPaths, monkeypatch: pytest.MonkeyPatch) -> None:
    """The CLI loads settings whose Open-Meteo rate limit does not pace the local replies."""
    settings = load_settings()
    settings.sources["open_meteo"].rate_limit_per_second = 1000
    monkeypatch.setattr(cli_module, "_load_and_log", lambda: settings)


@pytest.mark.parametrize(
    ("first", "second"),
    [
        pytest.param((_day(0), _day(20), 10), (_day(1), _day(21), 10), id="second-run-a-day-later"),
        pytest.param(
            (_day(9), _day(12), 5), (_day(4), _day(19), 15), id="second-run-before-a-shorter-first"
        ),
        pytest.param((_day(0), _day(7), 1), (_day(1), _day(8), 7), id="tour-rerun-after-one-day"),
    ],
)
def test_two_backfills_with_shifted_chunks(
    isolated_env: IsolatedPaths,
    first: tuple[datetime, datetime, int],
    second: tuple[datetime, datetime, int],
) -> None:
    """Detects a date dropping out of silver, or landing twice, when a second
    backfill re-fetches overlapping dates in chunks that start on other days:
    every (hour, site) of the two windows' union is there exactly once."""
    settings = load_settings()
    settings.sources["open_meteo"].rate_limit_per_second = 1000
    with respx.mock(assert_all_called=False) as router:
        router.get(url__startswith=ARCHIVE_BASE_URL).mock(side_effect=_archive_reply)
        for start, end, chunk_days in (first, second):
            report = runner.run_backfill(
                settings, "open_meteo", ["historical_demand"], start, end, chunk_days=chunk_days
            )
            assert report.ok, report.failed

    union_start, union_end = min(first[0], second[0]), max(first[1], second[1])
    _assert_every_hour_once(isolated_env.data_dir, union_start, union_end)


def test_preset_chunk_days_rerun_after_one_day(
    isolated_env: IsolatedPaths, fast_cli_settings: None, tmp_path: Path
) -> None:
    """Detects a preset's multi-day Open-Meteo chunk dropping a date from silver,
    or landing one twice: the tour-rerun case above, driven through
    ``gridflow backfill --preset`` with ``chunk_days: 7`` on the second run."""
    runs = [("2024-01-01", "2024-01-07", ""), ("2024-01-02", "2024-01-08", ", chunk_days: 7")]
    requests = []
    with respx.mock(assert_all_called=False) as router:
        route = router.get(url__startswith=ARCHIVE_BASE_URL).mock(side_effect=_archive_reply)
        for number, (start, end, chunk) in enumerate(runs):
            preset = tmp_path / f"run{number}.yaml"
            preset.write_text(
                f"datasets: [{{source: open_meteo, dataset: historical_demand{chunk}}}]\n"
                f"window: {{start: {start}, end: {end}}}\n",
                encoding="utf-8",
            )
            before = route.call_count
            result = cli.invoke(cli_module.app, ["backfill", "--preset", str(preset)])
            assert result.exit_code == 0, result.output
            requests.append(route.call_count - before)

    sites = len(DEMAND_LOCATIONS)
    assert requests == [7 * sites, sites]
    _assert_every_hour_once(isolated_env.data_dir, _day(0), _day(8))


def test_tour_fetches_open_meteo_once_per_site(
    fast_cli_settings: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Detects the packaged tour's Open-Meteo entries going back to one-day
    chunks: its week is one request per weather site (25), where one-day chunks
    made 175 of the tour's 203 requests. The other sources are not run."""
    real_backfill = runner.run_backfill

    def _open_meteo_only(
        settings: Any, source: str, datasets: list[str], *args: Any, **kwargs: Any
    ) -> runner.RunReport:
        if source != "open_meteo":
            return runner.RunReport()
        return real_backfill(settings, source, datasets, *args, **kwargs)

    monkeypatch.setattr(runner, "run_backfill", _open_meteo_only)
    requests = []
    for args in ([], ["--chunk-days", "1"]):
        with respx.mock(assert_all_called=False) as router:
            route = router.get(url__startswith=ARCHIVE_BASE_URL).mock(side_effect=_archive_reply)
            result = cli.invoke(cli_module.app, ["backfill", "--preset", "tour", *args])
        assert result.exit_code == 0, result.output
        requests.append(route.call_count)

    weather = ("historical_demand", "historical_wind", "historical_solar")
    sites = sum(len(DATASET_SPECS[ds].locations) for ds in weather)
    assert requests == [sites, 7 * sites] == [25, 175]
