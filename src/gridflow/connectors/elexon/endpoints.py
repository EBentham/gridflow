"""Elexon Insights API endpoint definitions and parameter builders."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from datetime import date, datetime


class ParamStyle(Enum):
    """Elexon API parameter styles."""

    SETTLEMENT_DATE = "settlement_date"  # ?settlementDate=2024-01-15
    SETTLEMENT_DATE_PERIOD = "settlement_date_period"
    PUBLISH_DATETIME = "publish_datetime"  # ?publishDateTimeFrom=...&publishDateTimeTo=...
    DATE_PATH = "date_path"  # /endpoint/{date}  (e.g. system-prices)
    NO_PARAMS = "no_params"  # Static endpoint (e.g., BM unit reference)


@dataclass(frozen=True)
class ElexonEndpoint:
    """Definition of an Elexon API endpoint."""

    path: str
    description: str
    param_style: ParamStyle = ParamStyle.SETTLEMENT_DATE
    date_param: str = "settlementDate"
    period_param: str | None = "settlementPeriod"
    supports_pagination: bool = True
    # For publish datetime / from-to style endpoints:
    from_param: str = "publishDateTimeFrom"
    to_param: str = "publishDateTimeTo"
    # For stream endpoints (used for backfill):
    stream_path: str | None = None
    # Max hours per request chunk (UOU2T14D has a 4-hour API limit)
    max_chunk_hours: int = 24


# Datasets that are intentionally absent from ENDPOINTS.
EXCLUDED_ENDPOINTS: dict[str, str] = {
    "bod": (
        "Excluded from active inventory because BOD availability has been unstable; "
        "requires a dedicated schema pass before activation."
    ),
    "generation_by_fuel": "Duplicate of fuelhh; both use /datasets/FUELHH.",
    "indicative_imbalance_volumes": (
        "Removed by Elexon; use active imbalance datasets instead."
    ),
}


# === COMPLETE ENDPOINT REGISTRY ===

ENDPOINTS: dict[str, ElexonEndpoint] = {
    # --- DATE_PATH style (path-based date, no query date param) ---
    "system_prices": ElexonEndpoint(
        path="/balancing/settlement/system-prices",
        description="System Sell Price and System Buy Price per settlement period",
        param_style=ParamStyle.DATE_PATH,
    ),

    # --- FROM/TO style (dataset endpoints that use ?from=ISO&to=ISO) ---
    # NOTE: boal uses BOALF path — BOAL was removed by Elexon, BOALF is the replacement.
    "boal": ElexonEndpoint(
        path="/datasets/BOALF",
        description="Bid/Offer Acceptance Levels Final (replaces deprecated BOAL)",
        param_style=ParamStyle.PUBLISH_DATETIME,
        from_param="from",
        to_param="to",
    ),
    # BOD is intentionally excluded; see EXCLUDED_ENDPOINTS.

    "disbsad": ElexonEndpoint(
        path="/datasets/DISBSAD",
        description="Disaggregated Balancing Services Adjustment Data",
        param_style=ParamStyle.PUBLISH_DATETIME,
        from_param="from",
        to_param="to",
    ),
    "mid": ElexonEndpoint(
        path="/datasets/MID",
        description="Market Index Data",
        param_style=ParamStyle.PUBLISH_DATETIME,
        from_param="from",
        to_param="to",
    ),
    "netbsad": ElexonEndpoint(
        path="/datasets/NETBSAD",
        description="Net Balancing Services Adjustment Data",
        param_style=ParamStyle.PUBLISH_DATETIME,
        from_param="from",
        to_param="to",
    ),
    "pn": ElexonEndpoint(
        path="/datasets/PN",
        description="Physical Notifications",
        param_style=ParamStyle.SETTLEMENT_DATE_PERIOD,
    ),

    # --- Publish datetime style (standard publishDateTimeFrom/To params) ---
    # FREQ is the exception: Swagger declares measurementDateTimeFrom/To
    # for /datasets/FREQ. Sending publishDateTimeFrom/To causes the API to
    # silently ignore the window and return the latest ~5761 samples.
    "freq": ElexonEndpoint(
        path="/datasets/FREQ",
        description="System Frequency",
        param_style=ParamStyle.PUBLISH_DATETIME,
        from_param="measurementDateTimeFrom",
        to_param="measurementDateTimeTo",
        supports_pagination=True,
    ),
    "fuelhh": ElexonEndpoint(
        path="/datasets/FUELHH",
        description="Half-hourly Generation Outturn by Fuel Type",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),
    "fuelinst": ElexonEndpoint(
        path="/datasets/FUELINST",
        description="Instantaneous Generation Outturn by Fuel Type",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),
    "imbalngc": ElexonEndpoint(
        path="/datasets/IMBALNGC",
        description="Indicated Imbalance",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),
    "ndf": ElexonEndpoint(
        path="/datasets/NDF",
        description="National Demand Forecast (Day-ahead)",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),
    "ndfd": ElexonEndpoint(
        path="/datasets/NDFD",
        description="National Demand Forecast (2-14 days ahead)",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),
    "melngc": ElexonEndpoint(
        path="/datasets/MELNGC",
        description="Indicated Margin",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),
    "fou2t14d": ElexonEndpoint(
        path="/datasets/FOU2T14D",
        description="2-14 Day Ahead Generation Availability by Fuel Type",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),
    "uou2t14d": ElexonEndpoint(
        path="/datasets/UOU2T14D",
        description="2-14 Day Ahead Generation Availability by BM Unit",
        param_style=ParamStyle.PUBLISH_DATETIME,
        max_chunk_hours=4,  # API rejects ranges > 4 hours
    ),
    "windfor": ElexonEndpoint(
        path="/datasets/WINDFOR",
        description="Wind Generation Forecast",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),
    "temp": ElexonEndpoint(
        path="/datasets/TEMP",
        description="Temperature Data",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),

    # --- ENTSO-E / B-series datasets (AGPT, AGWS, ATL) ---
    "agpt": ElexonEndpoint(
        path="/datasets/AGPT",
        description="Actual Aggregated Generation Per Type (B1620)",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),
    "agws": ElexonEndpoint(
        path="/datasets/AGWS",
        description="Actual or Estimated Wind and Solar Power Generation (B1630)",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),
    "atl": ElexonEndpoint(
        path="/datasets/ATL",
        description="Actual Total Load Per Bidding Zone (B0610)",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),

    # --- Initial demand/generation outturn ---
    "indo": ElexonEndpoint(
        path="/datasets/INDO",
        description="Initial National Demand Outturn",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),
    "itsdo": ElexonEndpoint(
        path="/datasets/ITSDO",
        description="Initial Transmission System Demand Outturn",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),
    "indod": ElexonEndpoint(
        path="/datasets/INDOD",
        description="Initial National Demand Outturn (Daily Total)",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),
    "nonbm": ElexonEndpoint(
        path="/datasets/NONBM",
        description="Non-BM STOR Generation",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),

    # --- Indicated / day-ahead datasets ---
    "inddem": ElexonEndpoint(
        path="/datasets/INDDEM",
        description="Day and Day-Ahead Indicated Demand",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),
    "indgen": ElexonEndpoint(
        path="/datasets/INDGEN",
        description="Day and Day-Ahead Indicated Generation",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),
    "tsdf": ElexonEndpoint(
        path="/datasets/TSDF",
        description="Transmission System Demand Forecast",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),
    "tsdfd": ElexonEndpoint(
        path="/datasets/TSDFD",
        description="2-14 Day Ahead Transmission System Demand Forecast",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),

    # --- Loss of Load Probability ---
    # G7 (2026-05): max_chunk_hours dropped to the dataclass default of 24h.
    # The previous 1h cap was an undocumented defensive default carried since
    # v0.2 with no vendor justification; the mocked E2E test
    # (test_active_datasets_fetch_with_expected_mocked_request_shape[lolpdrm])
    # expects the standard 24h `to_param` boundary like every other
    # PUBLISH_DATETIME endpoint that isn't UOU2T14D / REMIT / SOSO.
    "lolpdrm": ElexonEndpoint(
        path="/datasets/LOLPDRM",
        description="Loss of Load Probability and De-rated Margin",
        param_style=ParamStyle.PUBLISH_DATETIME,
    ),

    # --- REMIT outage messages ---
    # Vendor enforces an undocumented max-1-day query window: requests
    # spanning > 1 day return HTTP 400. Cap chunks at 23h to leave a
    # margin against DST shifts (boundary value).
    "remit": ElexonEndpoint(
        path="/datasets/REMIT",
        description="REMIT Outage and Unavailability Messages",
        param_style=ParamStyle.PUBLISH_DATETIME,
        max_chunk_hours=23,
    ),

    # --- SO-SO prices ---
    # Same undocumented max-1-day cap as REMIT.
    "soso": ElexonEndpoint(
        path="/datasets/SOSO",
        description="SO-SO Prices (Cross-Border Interconnector Trading)",
        param_style=ParamStyle.PUBLISH_DATETIME,
        max_chunk_hours=23,
    ),

    # --- Settlement Market Depth (DATE_PATH) ---
    "market_depth": ElexonEndpoint(
        path="/balancing/settlement/market-depth",
        description="Settlement Market Depth per Settlement Period",
        param_style=ParamStyle.DATE_PATH,
    ),

    # generation_by_fuel is intentionally excluded; see EXCLUDED_ENDPOINTS.

    # --- Reference data (static, no date params) ---
    "bmunits_reference": ElexonEndpoint(
        path="/reference/bmunits/all",
        description="All BM Unit reference data",
        param_style=ParamStyle.NO_PARAMS,
        supports_pagination=False,
    ),

    # indicative_imbalance_volumes is intentionally excluded; see EXCLUDED_ENDPOINTS.
}


def build_params(
    endpoint: ElexonEndpoint,
    settlement_date: date | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
    settlement_period: int | None = None,
    page: int = 1,
) -> dict[str, str | int]:
    """Build query parameters based on endpoint parameter style."""
    params: dict[str, str | int] = {}

    if endpoint.param_style in (
        ParamStyle.SETTLEMENT_DATE,
        ParamStyle.SETTLEMENT_DATE_PERIOD,
    ):
        if settlement_date:
            params[endpoint.date_param] = settlement_date.isoformat()
        if settlement_period is not None and endpoint.period_param:
            params[endpoint.period_param] = settlement_period

    elif endpoint.param_style == ParamStyle.PUBLISH_DATETIME:
        if start:
            params[endpoint.from_param] = start.strftime("%Y-%m-%dT%H:%M:%SZ")
        if end:
            params[endpoint.to_param] = end.strftime("%Y-%m-%dT%H:%M:%SZ")

    elif endpoint.param_style == ParamStyle.DATE_PATH:
        pass  # Path parameter appended by the connector in _fetch_date_path()

    elif endpoint.param_style == ParamStyle.NO_PARAMS:
        pass  # No query parameters needed

    if endpoint.supports_pagination:
        params["page"] = page

    return params
