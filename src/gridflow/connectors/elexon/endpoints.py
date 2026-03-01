"""Elexon Insights API endpoint definitions and parameter builders."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum


class ParamStyle(Enum):
    """Elexon API parameter styles."""

    SETTLEMENT_DATE = "settlement_date"  # ?settlementDate=2024-01-15
    PUBLISH_DATETIME = "publish_datetime"  # ?publishDateTimeFrom=...&publishDateTimeTo=...
    DATE_PATH = "date_path"  # /system-prices/{settlementDate}
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
    # For publish datetime style endpoints:
    from_param: str = "publishDateTimeFrom"
    to_param: str = "publishDateTimeTo"
    # For stream endpoints (used for backfill):
    stream_path: str | None = None


# === COMPLETE ENDPOINT REGISTRY ===

ENDPOINTS: dict[str, ElexonEndpoint] = {
    # --- Settlement date style endpoints ---
    "system_prices": ElexonEndpoint(
        path="/balancing/settlement/system-prices",
        description="System Sell Price and System Buy Price per settlement period",
        param_style=ParamStyle.SETTLEMENT_DATE,
    ),
    "boal": ElexonEndpoint(
        path="/datasets/BOAL",
        description="Bid/Offer Acceptance Levels",
        param_style=ParamStyle.SETTLEMENT_DATE,
    ),
    "bod": ElexonEndpoint(
        path="/datasets/BOD",
        description="Bid/Offer Data",
        param_style=ParamStyle.SETTLEMENT_DATE,
    ),
    "disbsad": ElexonEndpoint(
        path="/datasets/DISBSAD",
        description="Disaggregated Balancing Services Adjustment Data",
        param_style=ParamStyle.SETTLEMENT_DATE,
    ),
    "mid": ElexonEndpoint(
        path="/datasets/MID",
        description="Market Index Data",
        param_style=ParamStyle.SETTLEMENT_DATE,
    ),
    "netbsad": ElexonEndpoint(
        path="/datasets/NETBSAD",
        description="Net Balancing Services Adjustment Data",
        param_style=ParamStyle.SETTLEMENT_DATE,
    ),
    "pn": ElexonEndpoint(
        path="/datasets/PN",
        description="Physical Notifications",
        param_style=ParamStyle.SETTLEMENT_DATE,
    ),
    # --- Publish datetime style endpoints ---
    "freq": ElexonEndpoint(
        path="/datasets/FREQ",
        description="System Frequency",
        param_style=ParamStyle.PUBLISH_DATETIME,
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
    # --- Opinionated (derived) endpoints ---
    "generation_by_fuel": ElexonEndpoint(
        path="/generation/outturn/summary",
        description="Generation outturn summary by fuel type",
        param_style=ParamStyle.SETTLEMENT_DATE,
    ),
    "indicative_imbalance_volumes": ElexonEndpoint(
        path="/balancing/settlement/indicative-imbalance-volumes",
        description="Indicative imbalance volumes",
        param_style=ParamStyle.SETTLEMENT_DATE,
    ),
    # --- Reference data (static, no date params) ---
    "bmunits_reference": ElexonEndpoint(
        path="/reference/bmunits/all",
        description="All BM Unit reference data",
        param_style=ParamStyle.NO_PARAMS,
        supports_pagination=False,
    ),
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

    if endpoint.param_style == ParamStyle.SETTLEMENT_DATE:
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
        pass  # Path parameter handled in the connector

    elif endpoint.param_style == ParamStyle.NO_PARAMS:
        pass  # No query parameters needed

    if endpoint.supports_pagination:
        params["page"] = page

    return params
