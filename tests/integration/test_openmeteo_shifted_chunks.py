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
from typing import TYPE_CHECKING

import httpx
import polars as pl
import pytest
import respx

from gridflow.config.settings import load_settings
from gridflow.connectors.openmeteo.endpoints import ARCHIVE_BASE_URL, DEMAND_LOCATIONS
from gridflow.pipeline import runner
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from conftest import IsolatedPaths

pytestmark = pytest.mark.integration


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

    silver = PathBuilder(isolated_env.data_dir).silver_dir("open_meteo", "historical_demand")
    frame = pl.read_parquet(str(silver / "**" / "*.parquet"))
    per_key = frame.group_by("timestamp_utc", "location").len()
    assert per_key["len"].max() == 1

    union_start, union_end = min(first[0], second[0]), max(first[1], second[1])
    days = [(union_start + timedelta(days=n)).date() for n in range((union_end - union_start).days)]
    landed = per_key.group_by(pl.col("timestamp_utc").dt.date().alias("day")).agg(
        pl.col("len").sum()
    )
    counts = dict(zip(landed["day"].to_list(), landed["len"].to_list(), strict=True))
    sites = len(DEMAND_LOCATIONS)
    assert {d: counts.get(d, 0) for d in days} == dict.fromkeys(days, 24 * sites)
