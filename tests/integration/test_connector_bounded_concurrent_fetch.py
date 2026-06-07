"""CH3-03 / CH-PERF-03 (C2-7): bounded concurrent across-unit fetch.

The four API connectors fetch independent units — GIE countries, Open-Meteo
locations, ENTSO-E zones, Elexon date-chunks — concurrently via a bounded
``asyncio.gather(..., return_exceptions=True)`` while keeping paging *within* a
unit sequential. This module pins:

* **equivalence** — the concurrent fetch returns the SAME set of responses as
  the sequential version did (order-independent), the load-bearing correctness
  guarantee (characterisation guard — GREEN on the refactor);
* **partial-failure contract preserved** — for GIE (the only per-unit-tolerant
  connector) one failing country still tallies ``last_skipped_units == 1`` and
  returns the survivors; for the raise-on-any connectors a single failure still
  raises (characterisation guards);
* **all-fail** — every unit failing still raises (characterisation guard);
* **bounded concurrency** — across-unit fetches genuinely overlap (>= 2 in
  flight) yet never exceed ``rate_limit_per_second`` in flight at once. This is
  the one genuinely RED-before-the-refactor assertion: the sequential loop never
  has two requests in flight.

Mocking mirrors ``test_gie_alsi_legacy_pagination.py`` / ``test_openmeteo.py``
(respx). The overlap probe replaces ``connector._client.get`` with an async stub
that suspends inside the request so concurrent tasks actually interleave (a
synchronous respx handler returns without yielding and would make the probe
vacuous).
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import pytest
import respx

from gridflow.config.settings import DatasetConfig, SourceConfig, load_settings
from gridflow.connectors.elexon.client import ElexonConnector
from gridflow.connectors.entsoe.client import EntsoeConnector
from gridflow.connectors.entsoe.endpoints import DEFAULT_ZONES
from gridflow.connectors.gie.client import AlsiConnector
from gridflow.connectors.gie.endpoints import ALSI_COUNTRIES
from gridflow.connectors.openmeteo.client import OpenMeteoConnector
from gridflow.connectors.openmeteo.endpoints import ARCHIVE_BASE_URL, DATASET_SPECS

if TYPE_CHECKING:
    from collections.abc import Callable

ALSI_BASE_URL = "https://alsi.gie.eu"
ENTSOE_BASE = "https://web-api.tp.entsoe.eu"
ELEXON_BASE = "https://data.elexon.co.uk/bmrs/api/v1"

START = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
END = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _instant_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Neutralise tenacity's exponential backoff so a persistent 500 retries
    instantly (patching ``asyncio.sleep``, the decorator-agnostic route)."""

    async def _no_sleep(*_args: object, **_kwargs: object) -> None:
        return None

    monkeypatch.setattr("asyncio.sleep", _no_sleep)


# ---------------------------------------------------------------------------
# Configs
# ---------------------------------------------------------------------------


def _alsi_config(rate: int = 1000) -> SourceConfig:
    return (
        load_settings()
        .get_source_config("gie_alsi")
        .model_copy(update={"api_key": "test-key", "rate_limit_per_second": rate, "timeout": 5})
    )


def _openmeteo_config(rate: int = 1000) -> SourceConfig:
    return SourceConfig(
        base_url="",
        rate_limit_per_second=rate,
        timeout=5,
        datasets={"historical_demand": DatasetConfig(endpoint="/archive")},
    )


def _entsoe_config(rate: int = 1000) -> SourceConfig:
    return SourceConfig(
        base_url=ENTSOE_BASE,
        api_key="test-token",
        api_key_header="",
        rate_limit_per_second=rate,
        timeout=5,
        datasets={"actual_load": DatasetConfig()},
    )


def _elexon_config(rate: int = 1000) -> SourceConfig:
    return SourceConfig(
        base_url=ELEXON_BASE,
        rate_limit_per_second=rate,
        timeout=5,
        datasets={"system_prices": DatasetConfig(), "pn": DatasetConfig()},
    )


# ---------------------------------------------------------------------------
# Body builders
# ---------------------------------------------------------------------------


def _alsi_body(*, country: str) -> bytes:
    return json.dumps(
        {
            "last_page": 1,
            "total": 1,
            "gas_day": "2024-01-15",
            "dataset": "lng",
            "data": [{"name": country, "lngInventory": "1.0"}],
        }
    ).encode()


def _openmeteo_body() -> str:
    return json.dumps({"hourly": {"time": [], "temperature_2m": []}})


def _elexon_body() -> bytes:
    # No ``meta`` block → get_pagination_info defaults to (1, 1): single page.
    return json.dumps({"data": [{"settlementDate": "2024-01-15"}]}).encode()


def _entsoe_xml() -> bytes:
    fixture = Path(__file__).parent.parent / "fixtures" / "entsoe" / "actual_load_gb.xml"
    return fixture.read_bytes()


# ---------------------------------------------------------------------------
# Concurrency probe: replace the client's GET with a suspending async stub so
# concurrent tasks genuinely interleave inside the semaphore window.
# ---------------------------------------------------------------------------


# ``asyncio.sleep`` is monkeypatched to a no-op by the autouse fixture for
# retries; bind the genuine coroutine function at import time so the probe keeps
# a real suspension point regardless of the patch.
_real_sleep = asyncio.sleep


class _ConcurrencyProbe:
    """Tracks the peak number of in-flight GETs across a fetch."""

    def __init__(self, response_factory: Callable[[httpx.Request], httpx.Response]) -> None:
        self._response_factory = response_factory
        self.in_flight = 0
        self.max_in_flight = 0

    def install(self, connector: object) -> None:
        """Replace the connector's ``client.get`` with a suspending stub.

        Real respx handlers return synchronously and never yield, so concurrent
        tasks would complete one-at-a-time and the probe would see a vacuous
        ``max_in_flight == 1``. Suspending inside the GET (after the semaphore is
        acquired) opens a genuine overlap window.
        """
        client = connector._client  # type: ignore[attr-defined]
        real_build = client.build_request

        async def _get(url: str, *, params: dict[str, object] | None = None) -> httpx.Response:
            self.in_flight += 1
            self.max_in_flight = max(self.max_in_flight, self.in_flight)
            try:
                await _real_sleep(0.01)
                request = real_build("GET", url, params=params)
                response = self._response_factory(request)
                # Attach the request so the connector's raise_for_status works on
                # the hand-built response.
                response.request = request
                return response
            finally:
                self.in_flight -= 1

        client.get = _get  # type: ignore[assignment]


# ===========================================================================
# GIE (tally connector) — equivalence + partial-failure + all-fail
# ===========================================================================


@respx.mock
@pytest.mark.asyncio
async def test_gie_concurrent_fetch_returns_all_countries() -> None:
    """Concurrent GIE fetch returns one response per ALSI country (same set as
    the sequential loop), order-independent."""
    respx.get(re.compile(rf"^{re.escape(ALSI_BASE_URL)}/.*")).mock(
        side_effect=lambda request: httpx.Response(
            200,
            content=_alsi_body(country=request.url.params.get("country", "")),
            headers={"content-type": "application/json"},
        )
    )

    async with AlsiConnector(_alsi_config()) as connector:
        responses = await connector.fetch("lng", START, END)

    served = sorted(json.loads(r.body)["data"][0]["name"] for r in responses)
    assert served == sorted(ALSI_COUNTRIES)
    assert connector.last_skipped_units == 0


@respx.mock
@pytest.mark.asyncio
async def test_gie_partial_failure_tallies_and_returns_survivors() -> None:
    """One country 500s on every attempt; the rest succeed → ``last_skipped_units
    == 1`` and the survivors are returned (CH-COR-01 contract preserved under
    concurrency)."""
    failing = ALSI_COUNTRIES[0]

    def handler(request: httpx.Request) -> httpx.Response:
        country = request.url.params.get("country", "")
        if country == failing:
            return httpx.Response(500, text="persistent upstream error")
        return httpx.Response(
            200, content=_alsi_body(country=country), headers={"content-type": "application/json"}
        )

    respx.get(re.compile(rf"^{re.escape(ALSI_BASE_URL)}/.*")).mock(side_effect=handler)

    async with AlsiConnector(_alsi_config()) as connector:
        responses = await connector.fetch("lng", START, END)

    assert connector.last_skipped_units == 1
    assert len(responses) == len(ALSI_COUNTRIES) - 1
    assert failing not in {json.loads(r.body)["data"][0]["name"] for r in responses}


@respx.mock
@pytest.mark.asyncio
async def test_gie_all_countries_fail_raises() -> None:
    """Every country 500s → ``fetch`` re-raises (raise-on-all-fail)."""
    respx.get(re.compile(rf"^{re.escape(ALSI_BASE_URL)}/.*")).mock(
        return_value=httpx.Response(500, text="persistent upstream error")
    )

    async with AlsiConnector(_alsi_config()) as connector:
        with pytest.raises(httpx.HTTPStatusError):
            await connector.fetch("lng", START, END)


@pytest.mark.asyncio
async def test_gie_fetch_is_bounded_and_overlapping() -> None:
    """Across-country GIE fetch overlaps (>= 2 in flight) yet never exceeds
    ``rate_limit_per_second`` in flight.

    RED before CH3-03: the sequential loop awaited one country at a time, so
    ``max_in_flight`` was 1.
    """
    rate = 3
    probe = _ConcurrencyProbe(
        lambda request: httpx.Response(
            200,
            content=_alsi_body(country=request.url.params.get("country", "")),
            headers={"content-type": "application/json"},
        )
    )

    async with AlsiConnector(_alsi_config(rate=rate)) as connector:
        probe.install(connector)
        responses = await connector.fetch("lng", START, END)

    assert len(responses) == len(ALSI_COUNTRIES)
    assert probe.max_in_flight >= 2, "expected concurrent across-country fetch"
    assert probe.max_in_flight <= rate, f"semaphore breached: {probe.max_in_flight} > {rate}"


# ===========================================================================
# Open-Meteo (raise-on-any) — equivalence + all-fail + bounded
# ===========================================================================


@respx.mock
@pytest.mark.asyncio
async def test_openmeteo_concurrent_fetch_returns_all_locations() -> None:
    """Concurrent Open-Meteo fetch returns one response per location, preserving
    input order (gather is order-stable)."""
    respx.get(url__startswith=ARCHIVE_BASE_URL).mock(
        return_value=httpx.Response(200, text=_openmeteo_body())
    )

    spec = DATASET_SPECS["historical_demand"]
    async with OpenMeteoConnector(_openmeteo_config()) as connector:
        responses = await connector.fetch("historical_demand", START, END)

    assert [r.dataset for r in responses] == [
        f"historical_demand__{loc.name}" for loc in spec.locations
    ]
    assert connector.last_skipped_units == 0


@respx.mock
@pytest.mark.asyncio
async def test_openmeteo_one_location_failure_raises() -> None:
    """A single persistently-failing location raises (raise-on-any preserved)."""
    spec = DATASET_SPECS["historical_demand"]
    failing = spec.locations[0]

    def handler(request: httpx.Request) -> httpx.Response:
        lat = request.url.params.get("latitude")
        if lat is not None and abs(float(lat) - failing.latitude) < 1e-9:
            return httpx.Response(500, text="persistent upstream error")
        return httpx.Response(200, text=_openmeteo_body())

    respx.get(url__startswith=ARCHIVE_BASE_URL).mock(side_effect=handler)

    async with OpenMeteoConnector(_openmeteo_config()) as connector:
        with pytest.raises(httpx.HTTPStatusError):
            await connector.fetch("historical_demand", START, END)


@pytest.mark.asyncio
async def test_openmeteo_fetch_is_bounded_and_overlapping() -> None:
    """Across-location Open-Meteo fetch overlaps yet stays within the semaphore."""
    rate = 4
    probe = _ConcurrencyProbe(lambda _request: httpx.Response(200, text=_openmeteo_body()))

    async with OpenMeteoConnector(_openmeteo_config(rate=rate)) as connector:
        probe.install(connector)
        responses = await connector.fetch("historical_demand", START, END)

    assert len(responses) == len(DATASET_SPECS["historical_demand"].locations)
    assert probe.max_in_flight >= 2, "expected concurrent across-location fetch"
    assert probe.max_in_flight <= rate, f"semaphore breached: {probe.max_in_flight} > {rate}"


# ===========================================================================
# ENTSO-E (raise-on-any) — equivalence + bounded
# ===========================================================================


@respx.mock
@pytest.mark.asyncio
async def test_entsoe_concurrent_fetch_returns_all_zones() -> None:
    """Concurrent ENTSO-E fetch returns one response per default zone."""
    respx.get(f"{ENTSOE_BASE}/api").mock(
        return_value=httpx.Response(
            200, content=_entsoe_xml(), headers={"content-type": "text/xml"}
        )
    )

    async with EntsoeConnector(_entsoe_config()) as connector:
        responses = await connector.fetch(
            "actual_load",
            datetime(2024, 1, 15, tzinfo=UTC),
            datetime(2024, 1, 16, tzinfo=UTC),
        )

    # actual_load is out_bidding_zone style → one response per DEFAULT_ZONES entry.
    assert len(responses) == len(DEFAULT_ZONES)
    assert all(r.source == "entsoe" for r in responses)


@pytest.mark.asyncio
async def test_entsoe_fetch_is_bounded_and_overlapping() -> None:
    """Across-zone ENTSO-E fetch overlaps yet stays within the semaphore.

    ENTSO-E additionally paces requests via ``_throttle_request`` (kept intact);
    its sleep is the ``asyncio.sleep`` neutralised by the autouse fixture, so
    here the semaphore is the operative bound. In production the throttle makes
    ENTSO-E effectively serial, so this proves bounding/overlap, not a speedup.
    """
    rate = 3
    probe = _ConcurrencyProbe(
        lambda _request: httpx.Response(
            200, content=_entsoe_xml(), headers={"content-type": "text/xml"}
        )
    )

    async with EntsoeConnector(_entsoe_config(rate=rate)) as connector:
        probe.install(connector)
        responses = await connector.fetch(
            "actual_load",
            datetime(2024, 1, 15, tzinfo=UTC),
            datetime(2024, 1, 16, tzinfo=UTC),
        )

    assert len(responses) == len(DEFAULT_ZONES)
    assert probe.max_in_flight >= 2, "expected concurrent across-zone fetch"
    assert probe.max_in_flight <= rate, f"semaphore breached: {probe.max_in_flight} > {rate}"


# ===========================================================================
# Elexon (raise-on-any across date-chunks) — equivalence + bounded
# ===========================================================================


@respx.mock
@pytest.mark.asyncio
async def test_elexon_concurrent_fetch_returns_all_dates() -> None:
    """Concurrent Elexon fetch returns one response per date in the range (DATE_PATH
    style — system_prices), in date order."""
    respx.get(re.compile(rf"^{re.escape(ELEXON_BASE)}/.*")).mock(
        return_value=httpx.Response(
            200, content=_elexon_body(), headers={"content-type": "application/json"}
        )
    )

    start = datetime(2024, 1, 15, tzinfo=UTC)
    end = datetime(2024, 1, 18, tzinfo=UTC)
    async with ElexonConnector(_elexon_config()) as connector:
        responses = await connector.fetch("system_prices", start, end)

    # 4 inclusive dates, one single-page response each.
    assert len(responses) == 4
    assert [r.data_date for r in responses] == [
        datetime(2024, 1, d, tzinfo=UTC).date() for d in (15, 16, 17, 18)
    ]


@respx.mock
@pytest.mark.asyncio
async def test_elexon_settlement_period_concurrent_matches_sequential() -> None:
    """SETTLEMENT_DATE_PERIOD (pn) across two dates returns the same response set
    as the sequential loop, exercising the within-date period early-stop logic
    under concurrency.

    Each date serves periods 1-2 with data and period 3 with an empty ``data``
    array (the early-stop terminator), so each date contributes exactly two
    responses regardless of fetch order.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        period = int(request.url.params.get("settlementPeriod", "0"))
        date_str = str(request.url.params.get("settlementDate", ""))
        if period >= 3:
            body = json.dumps({"data": []}).encode()
        else:
            body = json.dumps(
                {"data": [{"settlementDate": date_str, "settlementPeriod": period}]}
            ).encode()
        return httpx.Response(200, content=body, headers={"content-type": "application/json"})

    respx.get(re.compile(rf"^{re.escape(ELEXON_BASE)}/.*")).mock(side_effect=handler)

    start = datetime(2024, 1, 15, tzinfo=UTC)
    end = datetime(2024, 1, 16, tzinfo=UTC)
    async with ElexonConnector(_elexon_config()) as connector:
        responses = await connector.fetch("pn", start, end)

    # 2 dates x 2 served periods = 4 responses; the result set (date, period) is
    # identical to the sequential loop regardless of across-date ordering.
    served = sorted(
        (str(r.data_date), int(json.loads(r.body)["data"][0]["settlementPeriod"]))
        for r in responses
    )
    assert served == [
        ("2024-01-15", 1),
        ("2024-01-15", 2),
        ("2024-01-16", 1),
        ("2024-01-16", 2),
    ]


@pytest.mark.asyncio
async def test_elexon_fetch_is_bounded_and_overlapping() -> None:
    """Across-date Elexon fetch overlaps yet stays within the semaphore."""
    rate = 3
    probe = _ConcurrencyProbe(
        lambda _request: httpx.Response(
            200, content=_elexon_body(), headers={"content-type": "application/json"}
        )
    )

    start = datetime(2024, 1, 15, tzinfo=UTC)
    end = datetime(2024, 1, 20, tzinfo=UTC)  # 6 dates > rate
    async with ElexonConnector(_elexon_config(rate=rate)) as connector:
        probe.install(connector)
        responses = await connector.fetch("system_prices", start, end)

    assert len(responses) == 6
    assert probe.max_in_flight >= 2, "expected concurrent across-date fetch"
    assert probe.max_in_flight <= rate, f"semaphore breached: {probe.max_in_flight} > {rate}"
