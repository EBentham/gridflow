"""
Tests for endpoint URL construction across all data sources.

Validates that each connector builds the correct URL strings and query parameters
for every dataset. Uses Feb 1 2026 as the reference date.

Run with:
    pytest tests/endpoints/test_endpoint_urls.py -v
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

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
    """Verify PUBLISH_DATETIME endpoints produce correct query params."""

    @pytest.mark.parametrize("dataset,expected_path", [
        ("freq", "/datasets/FREQ"),
        ("fuelhh", "/datasets/FUELHH"),
        ("fuelinst", "/datasets/FUELINST"),
        ("imbalngc", "/datasets/IMBALNGC"),
        ("ndf", "/datasets/NDF"),
        ("ndfd", "/datasets/NDFD"),
        ("melngc", "/datasets/MELNGC"),
        ("fou2t14d", "/datasets/FOU2T14D"),
        ("uou2t14d", "/datasets/UOU2T14D"),
        ("windfor", "/datasets/WINDFOR"),
        ("temp", "/datasets/TEMP"),
    ])
    def test_publish_datetime_url(self, dataset: str, expected_path: str):
        from gridflow.connectors.elexon.endpoints import ENDPOINTS, ParamStyle, build_params

        ep = ENDPOINTS[dataset]
        assert ep.param_style == ParamStyle.PUBLISH_DATETIME
        assert ep.path == expected_path

        params = build_params(ep, start=REF_START, end=REF_END, page=1)
        assert params["publishDateTimeFrom"] == "2026-02-01T00:00:00Z"
        assert params["publishDateTimeTo"] == "2026-02-02T00:00:00Z"
        assert params["page"] == 1

    @pytest.mark.parametrize("dataset,expected_path", [
        ("freq", "/datasets/FREQ"),
        ("fuelhh", "/datasets/FUELHH"),
        ("fuelinst", "/datasets/FUELINST"),
        ("imbalngc", "/datasets/IMBALNGC"),
        ("ndf", "/datasets/NDF"),
        ("ndfd", "/datasets/NDFD"),
        ("melngc", "/datasets/MELNGC"),
        ("fou2t14d", "/datasets/FOU2T14D"),
        ("uou2t14d", "/datasets/UOU2T14D"),
        ("windfor", "/datasets/WINDFOR"),
        ("temp", "/datasets/TEMP"),
    ])
    def test_publish_datetime_full_url_string(self, dataset: str, expected_path: str):
        from gridflow.connectors.elexon.endpoints import ENDPOINTS, build_params

        ep = ENDPOINTS[dataset]
        params = build_params(ep, start=REF_START, end=REF_END, page=1)

        expected_url = (
            f"{ELEXON_BASE}{expected_path}"
            f"?publishDateTimeFrom=2026-02-01T00:00:00Z"
            f"&publishDateTimeTo=2026-02-02T00:00:00Z"
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

    def test_day_ahead_prices_gb_params(self):
        from gridflow.connectors.entsoe.endpoints import BIDDING_ZONES, DOC_TYPES, ENTSOE_DT_FORMAT

        doc = DOC_TYPES["day_ahead_prices"]
        zone_eic = BIDDING_ZONES["GB"]
        period_start = REF_START.strftime(ENTSOE_DT_FORMAT)
        period_end = REF_END.strftime(ENTSOE_DT_FORMAT)

        params = {
            "documentType": doc.document_type,
            "in_Domain.mRID": zone_eic,
            "out_Domain.mRID": zone_eic,
            "periodStart": period_start,
            "periodEnd": period_end,
        }
        if doc.process_type:
            params["processType"] = doc.process_type

        assert params["documentType"] == "A44"
        assert params["in_Domain.mRID"] == "10YGB----------A"
        assert params["out_Domain.mRID"] == "10YGB----------A"
        assert params["periodStart"] == "202602010000"
        assert params["periodEnd"] == "202602020000"
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

        assert ENTSOG_API_PATH == "/operationaldata"

    def test_constants(self):
        from gridflow.connectors.entsog.endpoints import (
            DEFAULT_PERIOD_TYPE,
            ENTSOG_ALL_RECORDS_LIMIT,
            ENTSOG_TIMEZONE,
            PHYSICAL_FLOW_INDICATOR,
        )

        assert PHYSICAL_FLOW_INDICATOR == "Physical Flow"
        assert DEFAULT_PERIOD_TYPE == "day"
        assert ENTSOG_TIMEZONE == "UCT"  # Not "UTC" — ENTSO-G convention
        assert ENTSOG_ALL_RECORDS_LIMIT == -1

    def test_query_params_format(self):
        """Verify the exact query params the connector would build."""
        from gridflow.connectors.entsog.endpoints import (
            DEFAULT_PERIOD_TYPE,
            ENTSOG_ALL_RECORDS_LIMIT,
            ENTSOG_TIMEZONE,
            PHYSICAL_FLOW_INDICATOR,
        )

        params = {
            "from": REF_START.strftime("%Y-%m-%d"),
            "to": REF_END.strftime("%Y-%m-%d"),
            "indicator": PHYSICAL_FLOW_INDICATOR,
            "periodType": DEFAULT_PERIOD_TYPE,
            "timezone": ENTSOG_TIMEZONE,
            "limit": ENTSOG_ALL_RECORDS_LIMIT,
        }

        assert params["from"] == "2026-02-01"
        assert params["to"] == "2026-02-02"
        assert params["indicator"] == "Physical Flow"
        assert params["periodType"] == "day"
        assert params["timezone"] == "UCT"
        assert params["limit"] == -1

    def test_key_point_keys(self):
        from gridflow.connectors.entsog.endpoints import KEY_POINT_KEYS

        expected = ["IUK", "BBL", "FRAN", "IRL", "NIRL", "NORI"]
        assert expected == KEY_POINT_KEYS


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

    def test_query_params_format(self):
        """Verify the exact query params the connector would build."""
        from gridflow.connectors.gie.endpoints import DEFAULT_PAGE_SIZE

        params = {
            "country": "GB",
            "from": REF_START.strftime("%Y-%m-%d"),
            "till": REF_END.strftime("%Y-%m-%d"),
            "page": 1,
            "size": DEFAULT_PAGE_SIZE,
        }

        assert params["country"] == "GB"
        assert params["from"] == "2026-02-01"
        assert params["till"] == "2026-02-02"
        assert params["page"] == 1
        assert params["size"] == 300

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

    def test_locations(self):
        from gridflow.connectors.openmeteo.endpoints import LOCATIONS

        assert len(LOCATIONS) == 7
        names = [loc.name for loc in LOCATIONS]
        expected = ["london", "birmingham", "manchester", "leeds", "glasgow", "cardiff", "belfast"]
        assert names == expected

    def test_location_coordinates(self):
        from gridflow.connectors.openmeteo.endpoints import LOCATIONS

        london = LOCATIONS[0]
        assert london.name == "london"
        assert london.latitude == pytest.approx(51.5074)
        assert london.longitude == pytest.approx(-0.1278)

    def test_hourly_variables(self):
        from gridflow.connectors.openmeteo.endpoints import HOURLY_VARIABLES

        expected = [
            "temperature_2m", "wind_speed_10m", "wind_direction_10m",
            "relative_humidity_2m", "precipitation", "shortwave_radiation",
            "surface_pressure",
        ]
        assert expected == HOURLY_VARIABLES

    def test_historical_url_format(self):
        from gridflow.connectors.openmeteo.endpoints import (
            ARCHIVE_BASE_URL,
            HOURLY_VARIABLES,
            LOCATIONS,
        )

        location = LOCATIONS[0]  # London
        url = f"{ARCHIVE_BASE_URL}/archive"
        params = {
            "latitude": location.latitude,
            "longitude": location.longitude,
            "hourly": ",".join(HOURLY_VARIABLES),
            "start_date": REF_START.strftime("%Y-%m-%d"),
            "end_date": REF_END.strftime("%Y-%m-%d"),
            "timezone": "UTC",
        }

        assert url == "https://archive-api.open-meteo.com/v1/archive"
        assert params["latitude"] == 51.5074
        assert params["longitude"] == -0.1278
        assert params["start_date"] == "2026-02-01"
        assert params["end_date"] == "2026-02-02"
        assert params["timezone"] == "UTC"
        assert "temperature_2m" in params["hourly"]

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
