"""GIE ALSI silver across two backfills whose multi-day chunks start on different days.

Each chunk is one ``from..till`` request per country, filed in bronze under the
chunk's first day. Silver is one file per gas day, rewritten from that day's
rows only. A gas day with no bronze partition of its own looks back for the
request that reached it; before that look-back checked each request's window,
a nearer, shorter request (a one-day chunk, or a run that ended earlier) left
the day out of silver while the backfill reported success. HTTP is answered
locally; nothing is fetched.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING

import httpx
import polars as pl
import pytest
import respx

from gridflow.config.settings import load_settings
from gridflow.connectors.gie.endpoints import ALSI_COUNTRIES
from gridflow.pipeline import runner
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from conftest import IsolatedPaths

pytestmark = pytest.mark.integration

ALSI_API = "https://alsi.gie.eu/api"


def _alsi_reply(request: httpx.Request) -> httpx.Response:
    """One row per gas day of ``from..till`` (inclusive), newest first, as ALSI answers."""
    params = request.url.params
    first = date.fromisoformat(params["from"])
    last = date.fromisoformat(params["till"])
    country = params["country"]
    days = [last - timedelta(days=n) for n in range((last - first).days + 1)]
    return httpx.Response(
        200,
        json={
            "last_page": 1,
            "total": len(days),
            "data": [
                {
                    "name": country,
                    "code": country,
                    "gasDayStart": day.isoformat(),
                    "gasDayEnd": (day + timedelta(days=1)).isoformat(),
                    "sendOut": "100.0",
                    "dtmi": {"lng": "390", "gwh": "2666.0"},
                    "dtrs": "200",
                }
                for day in days
            ],
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
    """Detects a gas day dropping out of silver, or landing twice, when a second
    backfill re-fetches overlapping days in chunks that start on other days:
    every (gas day, country) of the two windows' union is there exactly once."""
    settings = load_settings()
    settings.sources["gie_alsi"].api_key = "test-key"
    settings.sources["gie_alsi"].rate_limit_per_second = 1000
    with respx.mock(assert_all_called=False) as router:
        router.get(url__startswith=ALSI_API).mock(side_effect=_alsi_reply)
        for start, end, chunk_days in (first, second):
            report = runner.run_backfill(
                settings, "gie_alsi", ["lng"], start, end, chunk_days=chunk_days
            )
            assert report.ok, report.failed

    silver = PathBuilder(isolated_env.data_dir).silver_dir("gie_alsi", "lng")
    frame = pl.read_parquet(str(silver / "**" / "*.parquet"))
    per_key = frame.group_by("gas_day", "country_code").len()
    assert per_key["len"].max() == 1

    union_start, union_end = min(first[0], second[0]), max(first[1], second[1])
    days = [(union_start + timedelta(days=n)).date() for n in range((union_end - union_start).days)]
    landed = per_key.group_by("gas_day").agg(pl.col("len").sum())
    counts = dict(zip(landed["gas_day"].to_list(), landed["len"].to_list(), strict=True))
    assert {d: counts.get(d, 0) for d in days} == dict.fromkeys(days, len(ALSI_COUNTRIES))
