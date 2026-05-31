"""
Tests for endpoint URL construction across all data sources.

Validates that each connector builds the correct URL strings and query parameters
for every dataset. Uses Feb 1 2026 as the reference date.

Run with:
    pytest tests/endpoints/test_endpoint_urls.py -v
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import httpx
import pytest
import respx

from gridflow.config.settings import SourceConfig, load_settings

# ---------------------------------------------------------------------------
# Reference dates used throughout
# ---------------------------------------------------------------------------
REF_DATE = date(2026, 2, 1)
REF_START = datetime(2026, 2, 1, 0, 0, 0, tzinfo=UTC)
REF_END = datetime(2026, 2, 2, 0, 0, 0, tzinfo=UTC)

ELEXON_BASE = "https://data.elexon.co.uk/bmrs/api/v1"
ENTSOE_BASE = "https://web-api.tp.entsoe.eu"
ENTSOG_BASE = "https://transparency.entsog.eu/api/v1"
AGSI_BASE = "https://agsi.gie.eu"
ALSI_BASE = "https://alsi.gie.eu"
OPENMETEO_ARCHIVE_BASE = "https://archive-api.open-meteo.com/v1"
OPENMETEO_FORECAST_BASE = "https://api.open-meteo.com/v1"
NESO_BASE = "https://api.carbonintensity.org.uk"


def _connector_config(source: str) -> SourceConfig:
    """Load a real source config with test-friendly auth/rate-limit/timeout.

    Behavioural endpoint tests drive the connector's *real* request builder
    via respx capture rather than re-deriving the query dict in the test
    body, so they need a live ``SourceConfig`` (correct base_url + datasets)
    with a stub API key and an effectively-unthrottled rate limit.
    """
    return load_settings().get_source_config(source).model_copy(
        update={"api_key": "test-key", "rate_limit_per_second": 1000, "timeout": 5}
    )


# ============================================================================
# ELEXON
# ============================================================================


class TestElexonEndpointDefinitions:
    """Verify every Elexon endpoint is registered with the correct path and param style."""

    def test_active_datasets_match_configured_inventory(self):
        from gridflow.config.settings import load_settings
        from gridflow.connectors.elexon.endpoints import ENDPOINTS

        configured = set(load_settings().get_source_config("elexon").datasets)

        assert set(ENDPOINTS) == configured

    def test_all_paths_start_with_slash(self):
        from gridflow.connectors.elexon.endpoints import ENDPOINTS

        for name, ep in ENDPOINTS.items():
            assert ep.path.startswith("/"), f"{name}: path '{ep.path}' must start with /"

    def test_intentionally_excluded_datasets_stay_out_of_active_inventory(self):
        from gridflow.config.settings import load_settings
        from gridflow.connectors.elexon.endpoints import ENDPOINTS, EXCLUDED_ENDPOINTS

        configured = set(load_settings().get_source_config("elexon").datasets)

        for dataset, reason in EXCLUDED_ENDPOINTS.items():
            assert reason, f"{dataset} must include an exclusion reason"
            assert dataset not in ENDPOINTS
            assert dataset not in configured


class TestElexonNewDatasetParams:
    """Verify new Tier-1 datasets are registered with correct paths and param styles."""

    @pytest.mark.parametrize("dataset,expected_path", [
        ("agpt", "/datasets/AGPT"),
        ("agws", "/datasets/AGWS"),
        ("atl", "/datasets/ATL"),
        ("indo", "/datasets/INDO"),
        ("itsdo", "/datasets/ITSDO"),
        ("indod", "/datasets/INDOD"),
        ("nonbm", "/datasets/NONBM"),
        ("inddem", "/datasets/INDDEM"),
        ("indgen", "/datasets/INDGEN"),
        ("tsdf", "/datasets/TSDF"),
        ("tsdfd", "/datasets/TSDFD"),
        ("lolpdrm", "/datasets/LOLPDRM"),
        ("remit", "/datasets/REMIT"),
        ("soso", "/datasets/SOSO"),
    ])
    def test_new_publish_datetime_datasets(self, dataset: str, expected_path: str):
        from gridflow.connectors.elexon.endpoints import ENDPOINTS, ParamStyle, build_params

        ep = ENDPOINTS[dataset]
        assert ep.param_style == ParamStyle.PUBLISH_DATETIME
        assert ep.path == expected_path

        params = build_params(ep, start=REF_START, end=REF_END, page=1)
        assert params["publishDateTimeFrom"] == "2026-02-01T00:00:00Z"
        assert params["publishDateTimeTo"] == "2026-02-02T00:00:00Z"

    def test_market_depth_is_date_path(self):
        from gridflow.connectors.elexon.endpoints import ENDPOINTS, ParamStyle

        ep = ENDPOINTS["market_depth"]
        assert ep.param_style == ParamStyle.DATE_PATH
        assert ep.path == "/balancing/settlement/market-depth"


class TestElexonDatePathParams:
    """Verify DATE_PATH endpoints (date embedded in URL, no query date param)."""

    def test_system_prices_uses_date_path(self):
        from gridflow.connectors.elexon.endpoints import ENDPOINTS, ParamStyle

        ep = ENDPOINTS["system_prices"]
        assert ep.param_style == ParamStyle.DATE_PATH
        assert ep.path == "/balancing/settlement/system-prices"

    def test_system_prices_path_construction(self):
        """Connector builds /balancing/settlement/system-prices/{date} at fetch time."""
        from gridflow.connectors.elexon.endpoints import ENDPOINTS, build_params

        ep = ENDPOINTS["system_prices"]
        path = f"{ep.path}/{REF_DATE.isoformat()}"
        assert path == "/balancing/settlement/system-prices/2026-02-01"

        # build_params produces no date param for DATE_PATH — only pagination
        params = build_params(ep, page=1)
        assert "settlementDate" not in params
        assert params.get("page") == 1


class TestElexonFromToParams:
    """Verify from/to style PUBLISH_DATETIME endpoints (boal, disbsad, mid, netbsad)."""

    @pytest.mark.parametrize("dataset,expected_path", [
        ("boal", "/datasets/BOALF"),
        ("disbsad", "/datasets/DISBSAD"),
        ("mid", "/datasets/MID"),
        ("netbsad", "/datasets/NETBSAD"),
    ])
    def test_from_to_param_style(self, dataset: str, expected_path: str):
        from gridflow.connectors.elexon.endpoints import ENDPOINTS, ParamStyle, build_params

        ep = ENDPOINTS[dataset]
        assert ep.param_style == ParamStyle.PUBLISH_DATETIME
        assert ep.path == expected_path
        assert ep.from_param == "from"
        assert ep.to_param == "to"

        params = build_params(ep, start=REF_START, end=REF_END, page=1)
        assert params["from"] == "2026-02-01T00:00:00Z"
        assert params["to"] == "2026-02-02T00:00:00Z"
        assert params["page"] == 1

    def test_pn_uses_settlement_date_period(self):
        from datetime import date

        from gridflow.connectors.elexon.endpoints import ENDPOINTS, ParamStyle, build_params

        ep = ENDPOINTS["pn"]
        assert ep.param_style == ParamStyle.SETTLEMENT_DATE_PERIOD
        assert ep.path == "/datasets/PN"

        params = build_params(
            ep, settlement_date=date(2026, 2, 1), settlement_period=1, page=1
        )
        assert params["settlementDate"] == "2026-02-01"
        assert params["settlementPeriod"] == 1
        assert params["page"] == 1


class TestElexonPublishDatetimeParams:
    """Verify PUBLISH_DATETIME endpoints produce correct query params.

    Most PUBLISH_DATETIME endpoints use the default
    publishDateTimeFrom/To names. `freq` is the documented exception:
    Swagger declares measurementDateTimeFrom/To for /datasets/FREQ.
    The connector overrides from_param/to_param accordingly (V2-FIX-01)."""

    @pytest.mark.parametrize("dataset,expected_path,from_param,to_param", [
        ("freq", "/datasets/FREQ", "measurementDateTimeFrom", "measurementDateTimeTo"),
        ("fuelhh", "/datasets/FUELHH", "publishDateTimeFrom", "publishDateTimeTo"),
        ("fuelinst", "/datasets/FUELINST", "publishDateTimeFrom", "publishDateTimeTo"),
        ("imbalngc", "/datasets/IMBALNGC", "publishDateTimeFrom", "publishDateTimeTo"),
        ("ndf", "/datasets/NDF", "publishDateTimeFrom", "publishDateTimeTo"),
        ("ndfd", "/datasets/NDFD", "publishDateTimeFrom", "publishDateTimeTo"),
        ("melngc", "/datasets/MELNGC", "publishDateTimeFrom", "publishDateTimeTo"),
        ("fou2t14d", "/datasets/FOU2T14D", "publishDateTimeFrom", "publishDateTimeTo"),
        ("uou2t14d", "/datasets/UOU2T14D", "publishDateTimeFrom", "publishDateTimeTo"),
        ("windfor", "/datasets/WINDFOR", "publishDateTimeFrom", "publishDateTimeTo"),
        ("temp", "/datasets/TEMP", "publishDateTimeFrom", "publishDateTimeTo"),
    ])
    def test_publish_datetime_url(
        self, dataset: str, expected_path: str, from_param: str, to_param: str
    ):
        from gridflow.connectors.elexon.endpoints import ENDPOINTS, ParamStyle, build_params

        ep = ENDPOINTS[dataset]
        assert ep.param_style == ParamStyle.PUBLISH_DATETIME
        assert ep.path == expected_path

        params = build_params(ep, start=REF_START, end=REF_END, page=1)
        assert params[from_param] == "2026-02-01T00:00:00Z"
        assert params[to_param] == "2026-02-02T00:00:00Z"
        assert params["page"] == 1

    @pytest.mark.parametrize("dataset,expected_path,from_param,to_param", [
        ("freq", "/datasets/FREQ", "measurementDateTimeFrom", "measurementDateTimeTo"),
        ("fuelhh", "/datasets/FUELHH", "publishDateTimeFrom", "publishDateTimeTo"),
        ("fuelinst", "/datasets/FUELINST", "publishDateTimeFrom", "publishDateTimeTo"),
        ("imbalngc", "/datasets/IMBALNGC", "publishDateTimeFrom", "publishDateTimeTo"),
        ("ndf", "/datasets/NDF", "publishDateTimeFrom", "publishDateTimeTo"),
        ("ndfd", "/datasets/NDFD", "publishDateTimeFrom", "publishDateTimeTo"),
        ("melngc", "/datasets/MELNGC", "publishDateTimeFrom", "publishDateTimeTo"),
        ("fou2t14d", "/datasets/FOU2T14D", "publishDateTimeFrom", "publishDateTimeTo"),
        ("uou2t14d", "/datasets/UOU2T14D", "publishDateTimeFrom", "publishDateTimeTo"),
        ("windfor", "/datasets/WINDFOR", "publishDateTimeFrom", "publishDateTimeTo"),
        ("temp", "/datasets/TEMP", "publishDateTimeFrom", "publishDateTimeTo"),
    ])
    def test_publish_datetime_full_url_string(
        self, dataset: str, expected_path: str, from_param: str, to_param: str
    ):
        from gridflow.connectors.elexon.endpoints import ENDPOINTS, build_params

        ep = ENDPOINTS[dataset]
        params = build_params(ep, start=REF_START, end=REF_END, page=1)

        expected_url = (
            f"{ELEXON_BASE}{expected_path}"
            f"?{from_param}=2026-02-01T00:00:00Z"
            f"&{to_param}=2026-02-02T00:00:00Z"
            f"&page=1"
        )
        actual_url = f"{ELEXON_BASE}{ep.path}?" + "&".join(f"{k}={v}" for k, v in params.items())
        assert actual_url == expected_url


class TestElexonNoParamsEndpoint:
    """Verify bmunits_reference produces no query params."""

    def test_bmunits_reference_url(self):
        from gridflow.connectors.elexon.endpoints import ENDPOINTS, ParamStyle, build_params

        ep = ENDPOINTS["bmunits_reference"]
        assert ep.param_style == ParamStyle.NO_PARAMS
        assert ep.path == "/reference/bmunits/all"
        assert ep.supports_pagination is False

        params = build_params(ep)
        assert params == {}

        full_url = f"{ELEXON_BASE}{ep.path}"
        assert full_url == f"{ELEXON_BASE}/reference/bmunits/all"


# ============================================================================
# ENTSO-E
# ============================================================================


class TestEntsoeEndpointDefinitions:
    """Verify ENTSO-E document types, zones, and URL construction."""

    def test_all_expected_datasets_registered(self):
        from gridflow.connectors.entsoe.endpoints import DOC_TYPES

        expected = [
            "day_ahead_prices", "actual_load", "load_forecast",
            "actual_generation", "wind_solar_forecast",
            "cross_border_flows", "outages_generation", "installed_capacity",
        ]
        for ds in expected:
            assert ds in DOC_TYPES, f"Missing ENTSO-E dataset: {ds}"

    def test_all_default_zones_have_eic_codes(self):
        from gridflow.connectors.entsoe.endpoints import BIDDING_ZONES, DEFAULT_ZONES

        for zone in DEFAULT_ZONES:
            assert zone in BIDDING_ZONES, f"Zone {zone} missing from BIDDING_ZONES"
            eic = BIDDING_ZONES[zone]
            assert len(eic) > 0, f"Empty EIC for zone {zone}"

    def test_default_zones_are_correct(self):
        from gridflow.connectors.entsoe.endpoints import DEFAULT_ZONES

        assert DEFAULT_ZONES == ["GB", "FR", "NL", "BE", "DE-LU", "IE-SEM"]

    def test_datetime_format(self):
        from gridflow.connectors.entsoe.endpoints import ENTSOE_DT_FORMAT

        formatted = REF_START.strftime(ENTSOE_DT_FORMAT)
        assert formatted == "202602010000"

    @pytest.mark.parametrize("dataset,doc_type,process_type", [
        ("day_ahead_prices", "A44", None),
        ("actual_load", "A65", "A16"),
        ("load_forecast", "A65", "A01"),
        ("actual_generation", "A75", "A16"),
        ("wind_solar_forecast", "A69", "A01"),
        ("cross_border_flows", "A11", None),
        ("outages_generation", "A80", None),
        ("installed_capacity", "A68", "A33"),
    ])
    def test_document_type_mapping(self, dataset: str, doc_type: str, process_type: str | None):
        from gridflow.connectors.entsoe.endpoints import DOC_TYPES

        dt = DOC_TYPES[dataset]
        assert dt.document_type == doc_type
        assert dt.process_type == process_type


class TestEntsoeUrlConstruction:
    """Verify the exact query params that would be sent for ENTSO-E requests."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_day_ahead_prices_gb_params(self):
        """Drive the REAL ENTSO-E request builder and assert on the captured
        outgoing request, not a dict re-derived in the test body.

        ``EntsoeConnector.fetch`` iterates ``DEFAULT_ZONES`` GB-first and
        sequentially, so ``requests[0]`` is the GB request (the
        ``in_Domain == '10YGB----------A'`` assertion self-confirms this).

        Regression guard: FAILS if the domain params are renamed/mis-cased
        (e.g. ``in_Domain`` -> ``In_Domain``), if ``documentType`` drifts
        from ``A44``, or if the ``securityToken`` auth param is dropped.
        Note the real query param is ``in_Domain`` (NOT ``in_Domain.mRID`` —
        that was the XML field name the deleted fiction asserted on).
        """
        from gridflow.connectors.entsoe.client import EntsoeConnector

        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(
                200, content=b"<root/>", headers={"content-type": "text/xml"}
            )

        respx.get(url__startswith=ENTSOE_BASE).mock(side_effect=handler)

        async with EntsoeConnector(_connector_config("entsoe")) as connector:
            await connector.fetch("day_ahead_prices", REF_START, REF_END)

        assert requests, "connector issued no request"
        params = dict(requests[0].url.params)

        assert params["documentType"] == "A44"
        assert params["in_Domain"] == "10YGB----------A"
        assert params["out_Domain"] == "10YGB----------A"
        assert params["periodStart"] == "202602010000"
        assert params["periodEnd"] == "202602020000"
        # ENTSO-E authenticates via a query-param token, not a header.
        assert params["securityToken"] == "test-key"
        # day_ahead_prices (A44) carries no processType.
        assert "processType" not in params

    def test_actual_generation_gb_params(self):
        from gridflow.connectors.entsoe.endpoints import BIDDING_ZONES, DOC_TYPES, ENTSOE_DT_FORMAT

        doc = DOC_TYPES["actual_generation"]
        zone_eic = BIDDING_ZONES["GB"]

        params = {
            "documentType": doc.document_type,
            "processType": doc.process_type,
            "in_Domain.mRID": zone_eic,
            "out_Domain.mRID": zone_eic,
            "periodStart": REF_START.strftime(ENTSOE_DT_FORMAT),
            "periodEnd": REF_END.strftime(ENTSOE_DT_FORMAT),
        }

        assert params["documentType"] == "A75"
        assert params["processType"] == "A16"

    def test_cross_border_flows_gb_fr_params(self):
        from gridflow.connectors.entsoe.endpoints import BIDDING_ZONES, DOC_TYPES, ENTSOE_DT_FORMAT

        doc = DOC_TYPES["cross_border_flows"]
        in_eic = BIDDING_ZONES["GB"]
        out_eic = BIDDING_ZONES["FR"]

        params = {
            "documentType": doc.document_type,
            "in_Domain.mRID": in_eic,
            "out_Domain.mRID": out_eic,
            "periodStart": REF_START.strftime(ENTSOE_DT_FORMAT),
            "periodEnd": REF_END.strftime(ENTSOE_DT_FORMAT),
        }

        assert params["documentType"] == "A11"
        assert params["in_Domain.mRID"] == "10YGB----------A"
        assert params["out_Domain.mRID"] == "10YFR-RTE------C"

    def test_bidding_zone_eic_codes(self):
        from gridflow.connectors.entsoe.endpoints import BIDDING_ZONES

        expected = {
            "GB": "10YGB----------A",
            "DE-LU": "10Y1001A1001A82H",
            "FR": "10YFR-RTE------C",
            "NL": "10YNL----------L",
            "BE": "10YBE----------2",
            "ES": "10YES-REE------0",
            "IT": "10YIT-GRTN-----B",
            "DK-1": "10YDK-1--------W",
            "DK-2": "10YDK-2--------M",
            "NO-1": "10YNO-1--------2",
            "SE-1": "10Y1001A1001A44P",
            "IE-SEM": "10Y1001A1001A59C",
        }
        for zone, eic in expected.items():
            assert BIDDING_ZONES.get(zone) == eic, f"Wrong EIC for {zone}"


class TestEntsoeFlowPairs:
    """Verify cross-border flow pairs are correct."""

    def test_flow_pairs_defined(self):
        from gridflow.connectors.entsoe.client import _FLOW_PAIRS

        expected = [
            ("GB", "FR"), ("GB", "NL"), ("GB", "BE"), ("GB", "IE-SEM"),
            ("FR", "BE"), ("FR", "DE-LU"), ("NL", "DE-LU"), ("NL", "BE"),
        ]
        assert expected == _FLOW_PAIRS

    def test_all_flow_pair_zones_have_eic(self):
        from gridflow.connectors.entsoe.client import _FLOW_PAIRS
        from gridflow.connectors.entsoe.endpoints import BIDDING_ZONES

        for in_zone, out_zone in _FLOW_PAIRS:
            assert in_zone in BIDDING_ZONES, f"Flow pair zone {in_zone} missing from BIDDING_ZONES"
            assert out_zone in BIDDING_ZONES, (
                f"Flow pair zone {out_zone} missing from BIDDING_ZONES"
            )


# ============================================================================
# ENTSO-G
# ============================================================================


class TestEntsogEndpointDefinitions:
    """Verify ENTSO-G endpoint constants and URL construction."""

    def test_api_path(self):
        from gridflow.connectors.entsog.endpoints import ENTSOG_API_PATH

        assert ENTSOG_API_PATH == "/operationalData"

    def test_constants(self):
        from gridflow.connectors.entsog.endpoints import (
            DEFAULT_PERIOD_TYPE,
            DEFAULT_POINT_DIRECTIONS,
            ENTSOG_ALL_RECORDS_LIMIT,
            ENTSOG_TIMEZONE,
            ENTSOG_TIMEZONE_PARAM,
            PHYSICAL_FLOW_INDICATOR,
        )

        assert PHYSICAL_FLOW_INDICATOR == "Physical Flow"
        assert DEFAULT_PERIOD_TYPE == "day"
        assert ENTSOG_TIMEZONE == "UCT"  # ENTSO-G convention
        assert ENTSOG_TIMEZONE_PARAM == "timeZone"
        assert ENTSOG_ALL_RECORDS_LIMIT == -1
        assert DEFAULT_POINT_DIRECTIONS

    def test_query_params_format(self):
        """Verify the exact query params the connector would build."""
        from gridflow.connectors.entsog.endpoints import (
            DEFAULT_POINT_DIRECTIONS,
            ENDPOINTS,
            build_params,
        )

        params = build_params(ENDPOINTS["physical_flows"], start=REF_START, end=REF_END)

        assert params["from"] == "2026-02-01"
        assert params["to"] == "2026-02-02"
        assert params["indicator"] == "Physical Flow"
        assert params["periodType"] == "day"
        assert params["timeZone"] == "UCT"
        assert params["limit"] == -1
        assert "pointDirection" not in params

        params = build_params(ENDPOINTS["nominations"], start=REF_START, end=REF_END)
        assert params["pointDirection"] == ",".join(DEFAULT_POINT_DIRECTIONS)

    def test_key_point_keys(self):
        from gridflow.connectors.entsog.endpoints import KEY_POINT_KEYS

        expected = ["IUK", "BBL", "FRAN", "IRL", "NIRL", "NORI"]
        assert expected == KEY_POINT_KEYS

    def test_active_entsog_datasets_match_configured_inventory(self):
        from gridflow.config.settings import load_settings
        from gridflow.connectors.entsog.endpoints import ENDPOINTS

        configured = set(load_settings().get_source_config("entsog").datasets)
        assert set(ENDPOINTS) == configured


# ============================================================================
# GIE (AGSI + ALSI)
# ============================================================================


class TestGieEndpointDefinitions:
    """Verify GIE AGSI/ALSI endpoint constants and URL construction."""

    def test_api_path(self):
        from gridflow.connectors.gie.endpoints import GIE_API_PATH

        assert GIE_API_PATH == "/api"

    def test_page_size(self):
        from gridflow.connectors.gie.endpoints import DEFAULT_PAGE_SIZE

        assert DEFAULT_PAGE_SIZE == 300

    def test_agsi_countries(self):
        from gridflow.connectors.gie.endpoints import AGSI_COUNTRIES

        expected = ["AT", "BE", "DE", "ES", "FR", "GB", "IT", "NL", "PL"]
        assert expected == AGSI_COUNTRIES

    def test_alsi_countries(self):
        from gridflow.connectors.gie.endpoints import ALSI_COUNTRIES

        expected = ["BE", "ES", "FR", "GB", "IT", "NL", "PL", "PT"]
        assert expected == ALSI_COUNTRIES

    @respx.mock
    @pytest.mark.asyncio
    async def test_query_params_format(self):
        """Drive the REAL GIE request builder and assert on the captured
        outgoing request, not a dict re-derived in the test body.

        Uses the ALSI (``gie_alsi``) connector — its ``lng`` fetch takes the
        legacy country-scoped path that builds the ``from``/``till`` date
        params directly. ALSI iterates countries BE-first, so ``requests[0]``
        is country ``BE`` (any single capture proves the param shape).

        Regression guard: FAILS if the date-window param reverts ``till`` ->
        ``to`` (the GIE-specific naming that distinguishes it from every
        other connector), or if ``from``/``country`` drift.
        """
        from gridflow.connectors.gie.client import AlsiConnector
        from gridflow.connectors.gie.endpoints import DEFAULT_PAGE_SIZE

        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(
                200,
                content=b'{"data": []}',
                headers={"content-type": "application/json"},
            )

        respx.get(url__startswith=ALSI_BASE).mock(side_effect=handler)

        async with AlsiConnector(_connector_config("gie_alsi")) as connector:
            await connector.fetch("lng", REF_START, REF_END)

        assert requests, "connector issued no request"
        params = dict(requests[0].url.params)

        assert params["country"] == "BE"
        assert params["from"] == "2026-02-01"
        # GIE uses 'till', not 'to' — the critical naming difference.
        assert params["till"] == "2026-02-02"
        assert "to" not in params
        assert params["size"] == str(DEFAULT_PAGE_SIZE)

    def test_uses_till_not_to(self):
        """GIE uses 'till' not 'to' — critical difference."""
        params = {
            "country": "GB",
            "from": "2026-02-01",
            "till": "2026-02-01",
            "page": 1,
            "size": 300,
        }
        assert "till" in params
        assert "to" not in params


# ============================================================================
# Open-Meteo
# ============================================================================


class TestOpenMeteoEndpointDefinitions:
    """Verify Open-Meteo endpoint constants and URL construction."""

    def test_base_urls(self):
        from gridflow.connectors.openmeteo.endpoints import ARCHIVE_BASE_URL, FORECAST_BASE_URL

        assert ARCHIVE_BASE_URL == "https://archive-api.open-meteo.com/v1"
        assert FORECAST_BASE_URL == "https://api.open-meteo.com/v1"

    def test_demand_locations(self):
        from gridflow.connectors.openmeteo.endpoints import DEMAND_LOCATIONS

        assert len(DEMAND_LOCATIONS) == 7
        names = [loc.name for loc in DEMAND_LOCATIONS]
        expected = ["london", "birmingham", "manchester", "leeds", "glasgow", "cardiff", "belfast"]
        assert names == expected

    def test_wind_locations(self):
        from gridflow.connectors.openmeteo.endpoints import WIND_LOCATIONS

        assert len(WIND_LOCATIONS) == 12
        names = {loc.name for loc in WIND_LOCATIONS}
        # Spot-check a representative offshore site and an onshore site.
        assert "hornsea" in names
        assert "whitelee" in names

    def test_solar_locations(self):
        from gridflow.connectors.openmeteo.endpoints import SOLAR_LOCATIONS

        assert len(SOLAR_LOCATIONS) == 6
        names = {loc.name for loc in SOLAR_LOCATIONS}
        assert "cornwall" in names
        assert "kent" in names

    def test_location_coordinates(self):
        from gridflow.connectors.openmeteo.endpoints import DEMAND_LOCATIONS

        london = DEMAND_LOCATIONS[0]
        assert london.name == "london"
        assert london.latitude == pytest.approx(51.5074)
        assert london.longitude == pytest.approx(-0.1278)

    def test_demand_hourly_vars(self):
        from gridflow.connectors.openmeteo.endpoints import DEMAND_HOURLY_VARS

        # F7.5-VARS-05: snow vars added to demand for winter peak.
        expected = (
            "temperature_2m", "wind_speed_10m", "wind_direction_10m",
            "relative_humidity_2m", "precipitation", "shortwave_radiation",
            "surface_pressure", "snowfall", "snow_depth",
        )
        assert expected == DEMAND_HOURLY_VARS

    def test_archive_wind_excludes_uninterpolated_heights(self):
        from gridflow.connectors.openmeteo.endpoints import WIND_ARCHIVE_VARS

        # Verified 2026-05-09 against ERA5 archive at Hornsea (53.88, 1.79)
        # and Whitelee (55.69, -4.27): wind_speed_{80,120,180}m return
        # `units: "undefined"` and all-null. Archive variable list omits
        # them so silver doesn't carry empty columns.
        assert "wind_speed_80m" not in WIND_ARCHIVE_VARS
        assert "wind_speed_120m" not in WIND_ARCHIVE_VARS
        assert "wind_speed_180m" not in WIND_ARCHIVE_VARS
        # 10m and 100m do work on archive — both must be requested.
        assert "wind_speed_10m" in WIND_ARCHIVE_VARS
        assert "wind_speed_100m" in WIND_ARCHIVE_VARS

    def test_forecast_wind_includes_full_height_set(self):
        from gridflow.connectors.openmeteo.endpoints import WIND_FORECAST_VARS

        for height in ("10m", "80m", "100m", "120m", "180m"):
            assert f"wind_speed_{height}" in WIND_FORECAST_VARS, height

    def test_solar_extra_params(self):
        from gridflow.connectors.openmeteo.endpoints import DATASET_SPECS

        for ds in ("historical_solar", "forecast_solar"):
            assert DATASET_SPECS[ds].extra_params == (
                ("tilt", "35"),
                ("azimuth", "180"),
            )
        for ds in ("historical_demand", "forecast_demand",
                   "historical_wind", "forecast_wind"):
            assert DATASET_SPECS[ds].extra_params == ()

    @respx.mock
    @pytest.mark.asyncio
    async def test_historical_demand_url_format(self):
        """Drive the REAL Open-Meteo request builder and assert on the host +
        path of each fetch's captured outgoing request, not a URL re-derived
        in the test body.

        Each fetch is captured in isolation and its *own* produced URL is
        asserted (keyed by which fetch made the call, NOT by which host the
        request happened to land on): a ``historical_*`` fetch must hit the
        archive endpoint, a ``forecast_*`` fetch the forecast endpoint. This
        is what makes an archive<->forecast base-URL swap detectable — bucket
        by receiving host instead and a swap stays invisible.

        Regression guard: FAILS if either base URL regresses (archive and
        forecast hosts swapped, or the ``/archive`` vs ``/forecast`` path
        suffix flipped). Also pins the query-param shape (lat/lon/dates/
        timezone/hourly) built by ``_fetch_location``.
        """
        from gridflow.connectors.openmeteo.client import OpenMeteoConnector

        def _capture() -> list[httpx.Request]:
            """Register host-matched capture routes; return the capture list.

            Both Open-Meteo hosts are mocked so that — whatever URL the
            connector builds — the request is captured rather than escaping
            to the network. The caller asserts the captured URL is the one
            the fetch *should* have produced.
            """
            captured: list[httpx.Request] = []

            def handler(request: httpx.Request) -> httpx.Response:
                captured.append(request)
                return httpx.Response(
                    200, content=b"{}", headers={"content-type": "application/json"}
                )

            respx.get(url__startswith=OPENMETEO_ARCHIVE_BASE).mock(side_effect=handler)
            respx.get(url__startswith=OPENMETEO_FORECAST_BASE).mock(side_effect=handler)
            return captured

        config = _connector_config("open_meteo")

        # Historical fetch — its produced URL must be the archive endpoint.
        historical_requests = _capture()
        async with OpenMeteoConnector(config) as connector:
            await connector.fetch("historical_demand", REF_START, REF_END)

        assert historical_requests, "historical fetch issued no request"
        historical_url = historical_requests[0].url
        assert (
            f"{historical_url.scheme}://{historical_url.host}{historical_url.path}"
            == "https://archive-api.open-meteo.com/v1/archive"
        )

        params = dict(historical_url.params)
        # DEMAND_LOCATIONS[0] is London (51.5074, -0.1278).
        assert params["latitude"] == "51.5074"
        assert params["longitude"] == "-0.1278"
        assert params["start_date"] == "2026-02-01"
        assert params["end_date"] == "2026-02-02"
        assert params["timezone"] == "UTC"
        assert "temperature_2m" in params["hourly"]
        assert "snowfall" in params["hourly"]

        # Forecast fetch — its produced URL must be the forecast endpoint.
        forecast_requests = _capture()
        async with OpenMeteoConnector(config) as connector:
            await connector.fetch("forecast_demand", REF_START, REF_END)

        assert forecast_requests, "forecast fetch issued no request"
        forecast_url = forecast_requests[0].url
        assert (
            f"{forecast_url.scheme}://{forecast_url.host}{forecast_url.path}"
            == "https://api.open-meteo.com/v1/forecast"
        )

    def test_forecast_url_format(self):
        from gridflow.connectors.openmeteo.endpoints import FORECAST_BASE_URL

        url = f"{FORECAST_BASE_URL}/forecast"
        assert url == "https://api.open-meteo.com/v1/forecast"


# ============================================================================
# NESO (Carbon Intensity)
# ============================================================================


class TestNesoEndpointDefinitions:
    """Verify NESO Carbon Intensity URL construction."""

    def test_path_format(self):
        """Verify the path-based URL construction."""
        from_str = REF_START.strftime("%Y-%m-%dT%H:%MZ")
        to_str = REF_END.strftime("%Y-%m-%dT%H:%MZ")
        path = f"/intensity/{from_str}/{to_str}"

        assert path == "/intensity/2026-02-01T00:00Z/2026-02-02T00:00Z"

    def test_full_url_format(self):
        from_str = REF_START.strftime("%Y-%m-%dT%H:%MZ")
        to_str = REF_END.strftime("%Y-%m-%dT%H:%MZ")
        full_url = f"{NESO_BASE}/intensity/{from_str}/{to_str}"

        assert full_url == "https://api.carbonintensity.org.uk/intensity/2026-02-01T00:00Z/2026-02-02T00:00Z"

    def test_14_day_chunking_logic(self):
        """Verify the chunking would produce correct paths for ranges > 14 days."""
        from datetime import timedelta

        max_days = 14
        start = REF_START
        end = REF_START + timedelta(days=30)

        chunks = []
        chunk_start = start
        while chunk_start < end:
            chunk_end = min(chunk_start + timedelta(days=max_days), end)
            path = (
                f"/intensity"
                f"/{chunk_start.strftime('%Y-%m-%dT%H:%MZ')}"
                f"/{chunk_end.strftime('%Y-%m-%dT%H:%MZ')}"
            )
            chunks.append(path)
            chunk_start = chunk_end

        assert len(chunks) == 3  # 14 + 14 + 2 days
        assert chunks[0] == "/intensity/2026-02-01T00:00Z/2026-02-15T00:00Z"
        assert chunks[1] == "/intensity/2026-02-15T00:00Z/2026-03-01T00:00Z"
        assert chunks[2] == "/intensity/2026-03-01T00:00Z/2026-03-03T00:00Z"


# ============================================================================
# Cross-source: base URL consistency with config
# ============================================================================


class TestBaseUrlsMatchConfig:
    """Verify that base URLs in endpoint modules match sources.yaml."""

    def test_openmeteo_archive_url(self):
        from gridflow.connectors.openmeteo.endpoints import ARCHIVE_BASE_URL

        assert ARCHIVE_BASE_URL == "https://archive-api.open-meteo.com/v1"

    def test_openmeteo_forecast_url(self):
        from gridflow.connectors.openmeteo.endpoints import FORECAST_BASE_URL

        assert FORECAST_BASE_URL == "https://api.open-meteo.com/v1"

    def test_source_names_match_registry(self):
        """Verify all connectors register with the correct source name."""
        from gridflow.connectors.elexon.client import ElexonConnector
        from gridflow.connectors.entsoe.client import EntsoeConnector
        from gridflow.connectors.entsog.client import EntsogConnector
        from gridflow.connectors.gie.client import AgsiConnector, AlsiConnector
        from gridflow.connectors.neso.carbon_intensity import CarbonIntensityConnector
        from gridflow.connectors.openmeteo.client import OpenMeteoConnector

        assert ElexonConnector.source_name == "elexon"
        assert EntsoeConnector.source_name == "entsoe"
        assert EntsogConnector.source_name == "entsog"
        assert AgsiConnector.source_name == "gie_agsi"
        assert AlsiConnector.source_name == "gie_alsi"
        assert OpenMeteoConnector.source_name == "open_meteo"
        assert CarbonIntensityConnector.source_name == "neso"
