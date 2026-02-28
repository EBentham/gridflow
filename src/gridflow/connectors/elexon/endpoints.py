"""Elexon Insights API endpoint definitions and parameter builders."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class ElexonEndpoint:
    """Definition of an Elexon API endpoint."""

    path: str
    description: str
    date_param: str = "settlementDate"
    period_param: str | None = "settlementPeriod"
    supports_pagination: bool = True


# Endpoint registry
ENDPOINTS: dict[str, ElexonEndpoint] = {
    "system_prices": ElexonEndpoint(
        path="/balancing/settlement/system-prices",
        description="System Sell Price and System Buy Price per settlement period",
    ),
    "generation_by_fuel": ElexonEndpoint(
        path="/generation/outturn/summary",
        description="Generation outturn summary by fuel type",
        date_param="settlementDate",
    ),
    "bm_units": ElexonEndpoint(
        path="/balancing/settlement/market-depth",
        description="Balancing Mechanism unit data",
    ),
}


def build_params(
    endpoint: ElexonEndpoint,
    settlement_date: date,
    settlement_period: int | None = None,
    page: int = 1,
) -> dict[str, str | int]:
    """Build query parameters for an Elexon API request."""
    params: dict[str, str | int] = {
        endpoint.date_param: settlement_date.isoformat(),
    }
    if settlement_period is not None and endpoint.period_param:
        params[endpoint.period_param] = settlement_period
    if endpoint.supports_pagination:
        params["page"] = page
    return params
