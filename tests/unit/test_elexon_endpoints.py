"""Unit tests for Elexon endpoint definitions and build_params()."""

from __future__ import annotations

from datetime import UTC, date, datetime

import gridflow.silver.elexon  # noqa: F401 - import triggers transformer registration
from gridflow.config.settings import load_settings
from gridflow.connectors.elexon.endpoints import (
    ENDPOINTS,
    EXCLUDED_ENDPOINTS,
    ParamStyle,
    build_params,
)
from gridflow.silver.registry import list_transformers


class TestParamStyleEnum:
    def test_all_styles_defined(self):
        assert ParamStyle.SETTLEMENT_DATE.value == "settlement_date"
        assert ParamStyle.SETTLEMENT_DATE_PERIOD.value == "settlement_date_period"
        assert ParamStyle.PUBLISH_DATETIME.value == "publish_datetime"
        assert ParamStyle.DATE_PATH.value == "date_path"
        assert ParamStyle.NO_PARAMS.value == "no_params"


class TestEndpointRegistry:
    def test_expected_datasets_registered(self):
        expected = {
            "system_prices",
            "boal",
            "disbsad",
            "mid",
            "pn",
            "freq",
            "fuelhh",
            "windfor",
            "ndf",
            "ndfd",
            "bmunits_reference",
        }
        for ds in expected:
            assert ds in ENDPOINTS, f"Missing endpoint: {ds}"

    def test_system_prices_uses_date_path_style(self):
        ep = ENDPOINTS["system_prices"]
        assert ep.param_style == ParamStyle.DATE_PATH

    def test_freq_uses_publish_datetime_style(self):
        ep = ENDPOINTS["freq"]
        assert ep.param_style == ParamStyle.PUBLISH_DATETIME

    def test_fuelhh_uses_publish_datetime_style(self):
        ep = ENDPOINTS["fuelhh"]
        assert ep.param_style == ParamStyle.PUBLISH_DATETIME

    def test_bmunits_uses_no_params_style(self):
        ep = ENDPOINTS["bmunits_reference"]
        assert ep.param_style == ParamStyle.NO_PARAMS
        assert ep.supports_pagination is False

    def test_all_endpoints_have_path(self):
        for name, ep in ENDPOINTS.items():
            assert ep.path.startswith("/"), f"{name}: path should start with /"


class TestElexonInventoryContract:
    def test_configured_datasets_have_endpoint_definitions(self):
        datasets = load_settings().get_source_config("elexon").datasets

        missing = set(datasets) - set(ENDPOINTS)

        assert missing == set()

    def test_endpoint_registry_matches_configured_active_datasets(self):
        datasets = load_settings().get_source_config("elexon").datasets

        unexpected = set(ENDPOINTS) - set(datasets)

        assert unexpected == set()

    def test_configured_paths_match_endpoint_registry(self):
        datasets = load_settings().get_source_config("elexon").datasets

        mismatches = {
            name: (config.endpoint, ENDPOINTS[name].path)
            for name, config in datasets.items()
            if config.endpoint != ENDPOINTS[name].path
        }

        assert mismatches == {}

    def test_configured_datasets_have_silver_transformers(self):
        datasets = load_settings().get_source_config("elexon").datasets
        registered = {dataset for _, dataset in list_transformers("elexon")}

        missing = set(datasets) - registered

        assert missing == set()

    def test_endpoint_param_styles_are_known(self):
        invalid = {
            name: endpoint.param_style
            for name, endpoint in ENDPOINTS.items()
            if not isinstance(endpoint.param_style, ParamStyle)
        }

        assert invalid == {}

    def test_intentional_exclusions_are_documented(self):
        required = {"bod", "generation_by_fuel", "indicative_imbalance_volumes"}

        assert required <= set(EXCLUDED_ENDPOINTS)
        for name in required:
            assert EXCLUDED_ENDPOINTS[name], f"{name} must include an exclusion reason"
            assert name not in ENDPOINTS, f"{name} should not be active"


class TestBuildParamsSettlementDate:
    def test_basic_settlement_date(self):
        # pn uses SETTLEMENT_DATE_PERIOD style (still uses settlementDate param)
        ep = ENDPOINTS["pn"]
        params = build_params(ep, settlement_date=date(2024, 1, 15))
        assert params["settlementDate"] == "2024-01-15"
        assert params["page"] == 1

    def test_settlement_date_with_page(self):
        ep = ENDPOINTS["pn"]
        params = build_params(ep, settlement_date=date(2024, 1, 15), page=3)
        assert params["page"] == 3

    def test_settlement_date_with_period(self):
        ep = ENDPOINTS["pn"]
        params = build_params(ep, settlement_date=date(2024, 1, 15), settlement_period=10)
        assert params["settlementPeriod"] == 10

    def test_new_tier1_datasets_use_publish_datetime(self):
        """All new Tier-1 datasets use PUBLISH_DATETIME style."""
        new_datasets = [
            "agpt",
            "agws",
            "atl",
            "indo",
            "itsdo",
            "indod",
            "nonbm",
            "inddem",
            "indgen",
            "tsdf",
            "tsdfd",
            "lolpdrm",
            "remit",
            "soso",
        ]
        for ds in new_datasets:
            assert ds in ENDPOINTS, f"Missing endpoint: {ds}"
            ep = ENDPOINTS[ds]
            assert ep.param_style == ParamStyle.PUBLISH_DATETIME, (
                f"{ds} should use PUBLISH_DATETIME"
            )

    def test_market_depth_uses_date_path(self):
        ep = ENDPOINTS["market_depth"]
        assert ep.param_style == ParamStyle.DATE_PATH
        assert ep.path == "/balancing/settlement/market-depth"

    def test_boal_uses_from_to_publish_datetime(self):
        # boal was moved from SETTLEMENT_DATE to PUBLISH_DATETIME (from/to params)
        ep = ENDPOINTS["boal"]
        assert ep.param_style == ParamStyle.PUBLISH_DATETIME
        assert ep.from_param == "from"
        assert ep.to_param == "to"
        start = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
        end = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)
        params = build_params(ep, start=start, end=end)
        assert "from" in params
        assert "settlementDate" not in params


class TestBuildParamsPublishDatetime:
    def test_freq_uses_measurement_datetime_param_names(self):
        """V2-FIX-01 regression: Elexon Swagger declares /datasets/FREQ
        accepts measurementDateTimeFrom/To. The connector previously
        sent the default publishDateTimeFrom/To names; the API silently
        ignored them and returned latest 5761 samples regardless of
        window. Override must keep the corrected names."""
        ep = ENDPOINTS["freq"]
        start = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
        end = datetime(2024, 1, 15, 23, 59, tzinfo=UTC)
        params = build_params(ep, start=start, end=end)
        assert "measurementDateTimeFrom" in params, (
            "freq must send measurementDateTimeFrom (Swagger param name); "
            "publishDateTimeFrom is silently ignored by the API"
        )
        assert "measurementDateTimeTo" in params
        assert "publishDateTimeFrom" not in params
        assert "publishDateTimeTo" not in params
        assert params["measurementDateTimeFrom"] == "2024-01-15T00:00:00Z"
        assert params["measurementDateTimeTo"] == "2024-01-15T23:59:00Z"

    def test_freq_endpoint_overrides_param_names(self):
        """V2-FIX-01: ENDPOINTS["freq"] must explicitly set the
        Swagger-correct param names rather than relying on the
        PUBLISH_DATETIME defaults."""
        ep = ENDPOINTS["freq"]
        assert ep.from_param == "measurementDateTimeFrom"
        assert ep.to_param == "measurementDateTimeTo"

    def test_fuelhh_publish_datetime(self):
        ep = ENDPOINTS["fuelhh"]
        start = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
        end = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)
        params = build_params(ep, start=start, end=end)
        assert "publishDateTimeFrom" in params
        assert params["page"] == 1

    def test_no_settlement_date_in_publish_datetime_style(self):
        """PUBLISH_DATETIME endpoints should not include settlementDate."""
        ep = ENDPOINTS["freq"]
        start = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
        params = build_params(
            ep,
            settlement_date=date(2024, 1, 15),  # should be ignored
            start=start,
            end=start,
        )
        assert "settlementDate" not in params


class TestRemitSosoMaxChunkHours:
    """V2-FIX-03: Elexon enforces an undocumented max-1-day window on
    /datasets/REMIT and /datasets/SOSO. The default `max_chunk_hours=24`
    sits exactly at the boundary and crosses it on DST shifts. Use 23
    to leave a margin."""

    def test_remit_max_chunk_hours_23(self):
        ep = ENDPOINTS["remit"]
        assert ep.max_chunk_hours == 23, (
            "REMIT must declare max_chunk_hours=23; vendor enforces an "
            "undocumented max-1-day query window (HTTP 400 otherwise)."
        )

    def test_soso_max_chunk_hours_23(self):
        ep = ENDPOINTS["soso"]
        assert ep.max_chunk_hours == 23, (
            "SOSO must declare max_chunk_hours=23; same vendor 1-day cap as REMIT."
        )


class TestBuildParamsNoParams:
    def test_bmunits_no_query_params(self):
        ep = ENDPOINTS["bmunits_reference"]
        params = build_params(ep)
        # NO_PARAMS endpoint with supports_pagination=False => empty dict
        assert "settlementDate" not in params
        assert "publishDateTimeFrom" not in params
        assert "page" not in params

    def test_bmunits_extra_args_ignored(self):
        ep = ENDPOINTS["bmunits_reference"]
        params = build_params(
            ep,
            settlement_date=date(2024, 1, 15),  # ignored
            start=datetime(2024, 1, 15, tzinfo=UTC),  # ignored
        )
        assert params == {}


class TestBuildParamsPagination:
    def test_pagination_included_by_default(self):
        ep = ENDPOINTS["system_prices"]
        params = build_params(ep, settlement_date=date(2024, 1, 15))
        assert "page" in params

    def test_no_pagination_for_bmunits(self):
        ep = ENDPOINTS["bmunits_reference"]
        params = build_params(ep)
        assert "page" not in params

    def test_page_default_is_one(self):
        ep = ENDPOINTS["system_prices"]
        params = build_params(ep, settlement_date=date(2024, 1, 15))
        assert params["page"] == 1
