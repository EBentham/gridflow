"""GIE allows 60 calls a minute, so every GIE send is paced at the configured rate.

Proved with a fake clock and a fake sleep: no real waits and no live calls.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

import httpx
import pytest
import respx

from gridflow.config.settings import load_settings
from gridflow.connectors.gie.client import AlsiConnector
from gridflow.connectors.gie.endpoints import ALSI_COUNTRIES, GIE_MAX_CALLS_PER_MINUTE

START = datetime(2024, 1, 15, tzinfo=UTC)


@pytest.mark.parametrize("source", ["gie_agsi", "gie_alsi"])
def test_configured_rate_stays_within_the_gie_limit(source: str) -> None:
    """Both GIE sources share one API key and its documented cap."""
    rate = load_settings().get_source_config(source).rate_limit_per_second
    assert rate * 60 <= GIE_MAX_CALLS_PER_MINUTE


@respx.mock
async def test_sends_are_paced_across_connector_instances(monkeypatch: pytest.MonkeyPatch) -> None:
    """The semaphore alone let a fetch's countries go out back to back, and the
    runner builds a new connector for the next dataset or backfill chunk."""
    clock = [1000.0]

    async def fake_sleep(seconds: float) -> None:
        clock[0] += seconds

    monkeypatch.setattr("gridflow.connectors.base.monotonic", lambda: clock[0], raising=False)
    monkeypatch.setattr("asyncio.sleep", fake_sleep)
    sent_at: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent_at.append(clock[0])
        return httpx.Response(200, json={"last_page": 1, "data": []})

    respx.get(re.compile(r"^https://alsi\.gie\.eu/.*")).mock(side_effect=handler)
    config = (
        load_settings()
        .get_source_config("gie_alsi")
        .model_copy(update={"api_key": "test-key", "rate_limit_per_second": 1})
    )

    for _ in range(2):
        async with AlsiConnector(config) as connector:
            await connector.fetch("lng", START, START)

    assert len(sent_at) == 2 * len(ALSI_COUNTRIES)
    gaps = [later - earlier for earlier, later in zip(sent_at, sent_at[1:], strict=False)]
    assert all(gap >= 1.0 for gap in gaps), gaps
