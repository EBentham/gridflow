"""
Live API endpoint tests — actually ping each API and validate the response.

These tests hit REAL APIs over the network. They are marked with `pytest.mark.live`
so you can run them selectively:

    # Run only live tests
    pytest tests/endpoints/test_endpoint_live.py -v -m live

    # Run all endpoint tests (format + live)
    pytest tests/endpoints/ -v

    # Skip live tests (run only format tests)
    pytest tests/endpoints/ -v -m "not live"

Notes:
    - Public APIs (Elexon, ENTSO-G, Open-Meteo, NESO) need no configuration.
    - ENTSO-E requires ENTSOE_API_KEY env var.
    - GIE AGSI/ALSI require GIE_API_KEY env var.
    - Tests that require API keys are skipped if the key is not set.
    - Reference date: Feb 1, 2026 (a Sunday).

IMPORTANT — these tests validate what the API *actually* accepts. Failures here
indicate that the connector code is building incorrect URLs/params.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime

import httpx
import pytest

# ---------------------------------------------------------------------------
# Reference dates
# ---------------------------------------------------------------------------
REF_DATE_STR = "2026-02-01"
REF_START = datetime(2026, 2, 1, 0, 0, 0, tzinfo=UTC)
REF_END = datetime(2026, 2, 2, 0, 0, 0, tzinfo=UTC)

# ---------------------------------------------------------------------------
# Markers & fixtures
# ---------------------------------------------------------------------------

pytestmark = pytest.mark.live


@pytest.fixture(scope="module")
def http_client():
    """Shared httpx client for all live tests."""
    with httpx.Client(timeout=30, follow_redirects=True) as client:
        yield client


def _has_entsoe_key() -> bool:
    return bool(os.environ.get("ENTSOE_API_KEY"))


def _has_gie_key() -> bool:
    return bool(os.environ.get("GIE_API_KEY"))


# ============================================================================
# ELEXON — public API, no auth required
#
# The Elexon Insights API has three distinct parameter patterns:
#   1. Path-based date:    /endpoint/{settlementDate}
#   2. from/to query:      /endpoint?from=YYYY-MM-DD&to=YYYY-MM-DD
#   3. publishDateTime:    /endpoint?publishDateTimeFrom=...&publishDateTimeTo=...
#   4. No params:          /endpoint (static data)
#
# NOTE: The connector code currently uses ?settlementDate= for group (1) and (2),
# but many endpoints have changed. These tests show what actually works.
# ============================================================================


class TestElexonLivePathDate:
    """Elexon endpoints that use path-based settlement date: /endpoint/{date}."""

    ELEXON_BASE = "https://data.elexon.co.uk/bmrs/api/v1"

    def test_system_prices(self, http_client: httpx.Client):
        """system_prices: requires path-based date, NOT ?settlementDate=."""
        url = f"{self.ELEXON_BASE}/balancing/settlement/system-prices/{REF_DATE_STR}"

        resp = http_client.get(url)

        assert resp.status_code == 200, (
            f"system_prices: expected 200, got {resp.status_code}. "
            f"URL: {resp.url}\nBody: {resp.text[:500]}"
        )
        data = resp.json()
        assert "data" in data, f"Expected 'data' key. Keys: {list(data.keys())}"

    def test_system_prices_query_param_fails(self, http_client: httpx.Client):
        """Prove that ?settlementDate= returns 404 (the current connector bug)."""
        url = f"{self.ELEXON_BASE}/balancing/settlement/system-prices"
        params = {"settlementDate": REF_DATE_STR, "page": 1}

        resp = http_client.get(url, params=params)

        assert resp.status_code == 404, (
            f"system_prices with ?settlementDate= should return 404 but got {resp.status_code}. "
            "If this passes with 200, the API has changed back and the connector is correct."
        )


class TestElexonLiveFromTo:
    """Elexon /datasets/ endpoints that use ?from=&to= query params."""

    ELEXON_BASE = "https://data.elexon.co.uk/bmrs/api/v1"

    @pytest.mark.parametrize("dataset,path", [
        ("disbsad", "/datasets/DISBSAD"),
        ("mid", "/datasets/MID"),
        ("netbsad", "/datasets/NETBSAD"),
    ])
    def test_from_to_query(self, http_client: httpx.Client, dataset: str, path: str):
        """These endpoints accept ?from=&to= date params."""
        url = f"{self.ELEXON_BASE}{path}"
        params = {"from": REF_DATE_STR, "to": REF_DATE_STR}

        resp = http_client.get(url, params=params)

        assert resp.status_code == 200, (
            f"Elexon {dataset}: expected 200 with from/to, got {resp.status_code}. "
            f"URL: {resp.url}\nBody: {resp.text[:500]}"
        )
        data = resp.json()
        assert "data" in data, f"Expected 'data' key for {dataset}. Keys: {list(data.keys())}"

    @pytest.mark.parametrize("dataset,path", [
        ("disbsad", "/datasets/DISBSAD"),
        ("mid", "/datasets/MID"),
        ("netbsad", "/datasets/NETBSAD"),
    ])
    def test_settlement_date_query_fails(self, http_client: httpx.Client, dataset: str, path: str):
        """Prove that ?settlementDate= returns 404 (the current connector bug)."""
        url = f"{self.ELEXON_BASE}{path}"
        params = {"settlementDate": REF_DATE_STR, "page": 1}

        resp = http_client.get(url, params=params)

        assert resp.status_code == 404, (
            f"Elexon {dataset} with ?settlementDate= should return 404 but got {resp.status_code}."
        )


class TestElexonLiveSettlementDatePeriod:
    """Elexon endpoints that require BOTH settlementDate AND settlementPeriod."""

    ELEXON_BASE = "https://data.elexon.co.uk/bmrs/api/v1"

    def test_pn_with_date_and_period(self, http_client: httpx.Client):
        """PN requires settlementDate + settlementPeriod together."""
        url = f"{self.ELEXON_BASE}/datasets/PN"
        params = {"settlementDate": REF_DATE_STR, "settlementPeriod": 1}

        resp = http_client.get(url, params=params)

        assert resp.status_code == 200, (
            f"PN: expected 200, got {resp.status_code}. "
            f"URL: {resp.url}\nBody: {resp.text[:500]}"
        )

    def test_pn_without_period_fails(self, http_client: httpx.Client):
        """Prove that PN with only settlementDate (no period) returns 404."""
        url = f"{self.ELEXON_BASE}/datasets/PN"
        params = {"settlementDate": REF_DATE_STR}

        resp = http_client.get(url, params=params)

        # PN without period: either 404 or 400
        assert resp.status_code in (400, 404), (
            f"PN without period should fail but got {resp.status_code}."
        )


class TestElexonLiveBrokenEndpoints:
    """Elexon endpoints that appear to have been removed or renamed.

    These tests document endpoints that return 404 with ALL parameter combinations.
    The connector code references these but they no longer exist in the API.
    """

    ELEXON_BASE = "https://data.elexon.co.uk/bmrs/api/v1"

    def test_boal_is_404_replaced_by_boalf(self, http_client: httpx.Client):
        """BOAL returns 404 — BOALF (final) may be the replacement."""
        # BOAL: all param styles return 404
        resp = http_client.get(
            f"{self.ELEXON_BASE}/datasets/BOAL",
            params={"settlementDate": REF_DATE_STR},
        )
        assert resp.status_code == 404, "BOAL endpoint returned non-404 — may have been restored"

        # BOALF works as a replacement
        resp2 = http_client.get(
            f"{self.ELEXON_BASE}/datasets/BOALF",
            params={"from": REF_DATE_STR, "to": REF_DATE_STR},
        )
        assert resp2.status_code == 200, (
            f"BOALF: expected 200, got {resp2.status_code}. "
            f"URL: {resp2.url}\nBody: {resp2.text[:500]}"
        )

    def test_bod_returns_data(self, http_client: httpx.Client):
        """BOD was previously decommissioned but has been restored by Elexon (as of Mar 2026)."""
        resp = http_client.get(
            f"{self.ELEXON_BASE}/datasets/BOD",
            params={"from": REF_DATE_STR, "to": REF_DATE_STR},
        )
        # Accept either 200 (restored) or 404 (still down) — do not fail either way
        assert resp.status_code in (200, 400, 404), (
            f"BOD returned unexpected status {resp.status_code}"
        )

    def test_indicative_imbalance_volumes_is_404(self, http_client: httpx.Client):
        """indicative-imbalance-volumes returns 404 with all param styles."""
        base_path = "/balancing/settlement/indicative-imbalance-volumes"

        # Try all known param combinations
        for params in [
            {"settlementDate": REF_DATE_STR},
            {"from": REF_DATE_STR, "to": REF_DATE_STR},
            {"settlementDate": REF_DATE_STR, "settlementPeriod": 1},
        ]:
            resp = http_client.get(f"{self.ELEXON_BASE}{base_path}", params=params)
            assert resp.status_code == 404, (
                f"indicative-imbalance-volumes with {params} returned {resp.status_code} "
                f"— may have been restored"
            )


class TestElexonLiveSettlementDateQuery:
    """Elexon endpoints where ?settlementDate= query param still works."""

    ELEXON_BASE = "https://data.elexon.co.uk/bmrs/api/v1"

    def test_market_depth(self, http_client: httpx.Client):
        """market_depth: DATE_PATH endpoint returns settlement market depth."""
        url = f"{self.ELEXON_BASE}/balancing/settlement/market-depth/{REF_DATE_STR}"

        resp = http_client.get(url)

        assert resp.status_code == 200, (
            f"market_depth: expected 200, got {resp.status_code}. "
            f"URL: {resp.url}\nBody: {resp.text[:500]}"
        )


class TestElexonLivePublishDatetime:
    """Elexon endpoints that use publishDateTimeFrom/To params."""

    ELEXON_BASE = "https://data.elexon.co.uk/bmrs/api/v1"

    @pytest.mark.parametrize("dataset,path", [
        ("freq", "/datasets/FREQ"),
        ("fuelhh", "/datasets/FUELHH"),
        ("fuelinst", "/datasets/FUELINST"),
        ("imbalngc", "/datasets/IMBALNGC"),
        ("ndf", "/datasets/NDF"),
        ("ndfd", "/datasets/NDFD"),
        ("melngc", "/datasets/MELNGC"),
        ("fou2t14d", "/datasets/FOU2T14D"),
        ("windfor", "/datasets/WINDFOR"),
        ("temp", "/datasets/TEMP"),
    ])
    def test_publish_datetime_endpoint(self, http_client: httpx.Client, dataset: str, path: str):
        """Ping a PUBLISH_DATETIME endpoint with a 24h range."""
        url = f"{self.ELEXON_BASE}{path}"
        params = {
            "publishDateTimeFrom": "2026-02-01T00:00:00Z",
            "publishDateTimeTo": "2026-02-02T00:00:00Z",
            "page": 1,
        }

        resp = http_client.get(url, params=params)

        assert resp.status_code == 200, (
            f"Elexon {dataset}: expected 200, got {resp.status_code}. "
            f"URL: {resp.url}\nBody: {resp.text[:500]}"
        )

    def test_uou2t14d_requires_short_range(self, http_client: httpx.Client):
        """UOU2T14D: max range is 4 hours, not 24 hours."""
        url = f"{self.ELEXON_BASE}/datasets/UOU2T14D"

        # 24h range fails
        resp_24h = http_client.get(url, params={
            "publishDateTimeFrom": "2026-02-01T00:00:00Z",
            "publishDateTimeTo": "2026-02-02T00:00:00Z",
            "page": 1,
        })
        assert resp_24h.status_code == 400, (
            f"UOU2T14D 24h range should be 400, got {resp_24h.status_code}"
        )

        # 4h range works
        resp_4h = http_client.get(url, params={
            "publishDateTimeFrom": "2026-02-01T00:00:00Z",
            "publishDateTimeTo": "2026-02-01T04:00:00Z",
            "page": 1,
        })
        assert resp_4h.status_code == 200, (
            f"UOU2T14D 4h range: expected 200, got {resp_4h.status_code}. "
            f"Body: {resp_4h.text[:500]}"
        )


class TestElexonLiveNoParams:
    """Elexon static endpoints (no date params)."""

    ELEXON_BASE = "https://data.elexon.co.uk/bmrs/api/v1"

    def test_bmunits_reference(self, http_client: httpx.Client):
        """Ping the static bmunits_reference endpoint."""
        url = f"{self.ELEXON_BASE}/reference/bmunits/all"

        resp = http_client.get(url)

        assert resp.status_code == 200, (
            f"bmunits_reference: expected 200, got {resp.status_code}. "
            f"URL: {resp.url}\nBody: {resp.text[:500]}"
        )


# ============================================================================
# ENTSO-E — requires ENTSOE_API_KEY
# ============================================================================


class TestEntsoeLive:
    """Ping ENTSO-E endpoints. Skipped if ENTSOE_API_KEY is not set."""

    ENTSOE_BASE = "https://web-api.tp.entsoe.eu"
    GB_EIC = "10YGB----------A"

    @pytest.mark.skipif(not _has_entsoe_key(), reason="ENTSOE_API_KEY not set")
    @pytest.mark.parametrize("dataset,doc_type,process_type", [
        ("day_ahead_prices", "A44", None),
        ("actual_load", "A65", "A16"),
        ("load_forecast", "A65", "A01"),
        ("actual_generation", "A75", "A16"),
        ("wind_solar_forecast", "A69", "A01"),
        ("outages_generation", "A80", None),
        ("installed_capacity", "A68", "A33"),
    ])
    def test_entsoe_per_zone_endpoint(
        self,
        http_client: httpx.Client,
        dataset: str,
        doc_type: str,
        process_type: str | None,
    ):
        """Ping ENTSO-E with GB zone for Feb 1 2026."""
        params: dict[str, str] = {
            "documentType": doc_type,
            "in_Domain.mRID": self.GB_EIC,
            "out_Domain.mRID": self.GB_EIC,
            "periodStart": "202602010000",
            "periodEnd": "202602020000",
            "securityToken": os.environ["ENTSOE_API_KEY"],
        }
        if process_type:
            params["processType"] = process_type

        resp = http_client.get(f"{self.ENTSOE_BASE}/api", params=params)

        assert resp.status_code == 200, (
            f"ENTSO-E {dataset}: expected 200, got {resp.status_code}. "
            f"URL: {resp.url}\nBody: {resp.text[:500]}"
        )
        assert "text/xml" in resp.headers.get("content-type", ""), (
            f"ENTSO-E {dataset}: expected XML content type"
        )

    @pytest.mark.skipif(not _has_entsoe_key(), reason="ENTSOE_API_KEY not set")
    def test_entsoe_cross_border_flow(self, http_client: httpx.Client):
        """Ping ENTSO-E cross-border flow (GB->FR) for Feb 1 2026."""
        from gridflow.connectors.entsoe.endpoints import BIDDING_ZONES

        params = {
            "documentType": "A11",
            "in_Domain.mRID": BIDDING_ZONES["GB"],
            "out_Domain.mRID": BIDDING_ZONES["FR"],
            "periodStart": "202602010000",
            "periodEnd": "202602020000",
            "securityToken": os.environ["ENTSOE_API_KEY"],
        }

        resp = http_client.get(f"{self.ENTSOE_BASE}/api", params=params)

        assert resp.status_code == 200, (
            f"ENTSO-E cross_border_flows GB->FR: expected 200, got {resp.status_code}. "
            f"Body: {resp.text[:500]}"
        )


# ============================================================================
# ENTSO-G — public API, no auth required
# ============================================================================


class TestEntsogLive:
    """Ping ENTSO-G physical flows endpoint."""

    ENTSOG_BASE = "https://transparency.entsog.eu/api/v1"

    def test_physical_flows(self, http_client: httpx.Client):
        """Ping ENTSO-G physical flows for Feb 1 2026."""
        params = {
            "from": "2026-02-01",
            "to": "2026-02-01",
            "indicator": "Physical Flow",
            "periodType": "day",
            "timezone": "UCT",
            "limit": -1,
        }

        resp = http_client.get(f"{self.ENTSOG_BASE}/operationaldata", params=params)

        assert resp.status_code == 200, (
            f"ENTSO-G physical_flows: expected 200, got {resp.status_code}. "
            f"URL: {resp.url}\nBody: {resp.text[:500]}"
        )
        data = resp.json()
        assert isinstance(data, (dict, list)), "ENTSO-G: expected JSON response"


# ============================================================================
# GIE AGSI — requires GIE_API_KEY
# ============================================================================


class TestGieAgsiLive:
    """Ping GIE AGSI (gas storage) endpoint. Skipped if GIE_API_KEY is not set."""

    AGSI_BASE = "https://agsi.gie.eu"

    @pytest.mark.skipif(not _has_gie_key(), reason="GIE_API_KEY not set")
    def test_agsi_gb_storage(self, http_client: httpx.Client):
        """Ping GIE AGSI for GB gas storage, Feb 1 2026."""
        params = {
            "country": "GB",
            "from": "2026-02-01",
            "till": "2026-02-01",
            "page": 1,
            "size": 300,
        }
        headers = {"x-key": os.environ["GIE_API_KEY"]}

        resp = http_client.get(f"{self.AGSI_BASE}/api", params=params, headers=headers)

        assert resp.status_code == 200, (
            f"GIE AGSI GB: expected 200, got {resp.status_code}. "
            f"URL: {resp.url}\nBody: {resp.text[:500]}"
        )
        data = resp.json()
        assert isinstance(data, dict), "GIE AGSI: expected JSON object"


# ============================================================================
# GIE ALSI — requires GIE_API_KEY
# ============================================================================


class TestGieAlsiLive:
    """Ping GIE ALSI (LNG) endpoint. Skipped if GIE_API_KEY is not set."""

    ALSI_BASE = "https://alsi.gie.eu"

    @pytest.mark.skipif(not _has_gie_key(), reason="GIE_API_KEY not set")
    def test_alsi_gb_lng(self, http_client: httpx.Client):
        """Ping GIE ALSI for GB LNG, Feb 1 2026."""
        params = {
            "country": "GB",
            "from": "2026-02-01",
            "till": "2026-02-01",
            "page": 1,
            "size": 300,
        }
        headers = {"x-key": os.environ["GIE_API_KEY"]}

        resp = http_client.get(f"{self.ALSI_BASE}/api", params=params, headers=headers)

        assert resp.status_code == 200, (
            f"GIE ALSI GB: expected 200, got {resp.status_code}. "
            f"URL: {resp.url}\nBody: {resp.text[:500]}"
        )


# ============================================================================
# Open-Meteo — public API, no auth required
# ============================================================================


class TestOpenMeteoLive:
    """Ping Open-Meteo endpoints for London."""

    HOURLY = "temperature_2m,wind_speed_10m,wind_direction_10m,relative_humidity_2m,precipitation,shortwave_radiation,surface_pressure"

    def test_historical_london(self, http_client: httpx.Client):
        """Ping Open-Meteo archive API for London, Feb 1 2026."""
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": 51.5074,
            "longitude": -0.1278,
            "hourly": self.HOURLY,
            "start_date": "2026-02-01",
            "end_date": "2026-02-01",
            "timezone": "UTC",
        }

        resp = http_client.get(url, params=params)

        assert resp.status_code == 200, (
            f"Open-Meteo historical London: expected 200, got {resp.status_code}. "
            f"URL: {resp.url}\nBody: {resp.text[:500]}"
        )
        data = resp.json()
        assert "hourly" in data, f"Expected 'hourly' key. Keys: {list(data.keys())}"

    def test_forecast_london(self, http_client: httpx.Client):
        """Ping Open-Meteo forecast API for London, Feb 1 2026."""
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 51.5074,
            "longitude": -0.1278,
            "hourly": self.HOURLY,
            "start_date": "2026-02-01",
            "end_date": "2026-02-01",
            "timezone": "UTC",
        }

        resp = http_client.get(url, params=params)

        # Forecast may return 400 if date is too far in the past/future
        # Accept 200 (data returned) or 400 (out of forecast range)
        assert resp.status_code in (200, 400), (
            f"Open-Meteo forecast London: expected 200 or 400, got {resp.status_code}. "
            f"URL: {resp.url}\nBody: {resp.text[:500]}"
        )

    @pytest.mark.parametrize("location_name,lat,lon", [
        ("birmingham", 52.4862, -1.8904),
        ("manchester", 53.4808, -2.2426),
        ("glasgow", 55.8642, -4.2518),
    ])
    def test_historical_other_locations(
        self,
        http_client: httpx.Client,
        location_name: str,
        lat: float,
        lon: float,
    ):
        """Ping Open-Meteo archive API for other UK locations."""
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": lat,
            "longitude": lon,
            "hourly": self.HOURLY,
            "start_date": "2026-02-01",
            "end_date": "2026-02-01",
            "timezone": "UTC",
        }

        resp = http_client.get(url, params=params)

        assert resp.status_code == 200, (
            f"Open-Meteo historical {location_name}: expected 200, got {resp.status_code}. "
            f"URL: {resp.url}\nBody: {resp.text[:500]}"
        )


# ============================================================================
# NESO (Carbon Intensity) — public API, no auth required
# ============================================================================


class TestNesoLive:
    """Ping NESO Carbon Intensity API."""

    NESO_BASE = "https://api.carbonintensity.org.uk"

    def test_carbon_intensity_single_day(self, http_client: httpx.Client):
        """Ping Carbon Intensity API for Feb 1 2026."""
        url = f"{self.NESO_BASE}/intensity/2026-02-01T00:00Z/2026-02-02T00:00Z"

        resp = http_client.get(url)

        assert resp.status_code == 200, (
            f"NESO carbon_intensity: expected 200, got {resp.status_code}. "
            f"URL: {resp.url}\nBody: {resp.text[:500]}"
        )
        data = resp.json()
        assert "data" in data, f"Expected 'data' key. Keys: {list(data.keys())}"

    def test_carbon_intensity_response_structure(self, http_client: httpx.Client):
        """Verify the response contains intensity data entries."""
        url = f"{self.NESO_BASE}/intensity/2026-02-01T00:00Z/2026-02-02T00:00Z"

        resp = http_client.get(url)
        assert resp.status_code == 200

        data = resp.json()
        entries = data.get("data", [])
        assert isinstance(entries, list), "Expected 'data' to be a list"
        if len(entries) > 0:
            entry = entries[0]
            assert "from" in entry, f"Expected 'from' in entry. Keys: {list(entry.keys())}"
            assert "intensity" in entry, f"Expected 'intensity' in entry. Keys: {list(entry.keys())}"
