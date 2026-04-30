"""ENTSO-E Transparency Platform API endpoint definitions."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class EntsoeDocType:
    """ENTSO-E document type definition."""

    document_type: str  # e.g. "A44"
    process_type: str | None  # e.g. "A01" (day-ahead), "A16" (realised)
    description: str
    # "zone"  → in_Domain.mRID + out_Domain.mRID query params (default)
    # "control_area" → controlArea_Domain.mRID query param (balancing datasets)
    domain_style: str = "zone"


# ENTSO-E document type registry (dataset name -> EntsoeDocType)
DOC_TYPES: dict[str, EntsoeDocType] = {
    "day_ahead_prices": EntsoeDocType("A44", None, "Day-ahead prices"),
    "actual_load": EntsoeDocType("A65", "A16", "Actual total load"),
    "load_forecast": EntsoeDocType("A65", "A01", "Day-ahead load forecast"),
    "actual_generation": EntsoeDocType("A75", "A16", "Actual generation per production type"),
    "wind_solar_forecast": EntsoeDocType("A69", "A01", "Day-ahead wind / solar forecast"),
    "cross_border_flows": EntsoeDocType("A88", None, "Physical cross-border flows"),
    "outages_generation": EntsoeDocType("A80", None, "Unavailability of generation units"),
    "installed_capacity": EntsoeDocType("A68", "A33", "Installed generation capacity"),
    # Phase 2 additions
    "generation_forecast": EntsoeDocType("A71", "A01", "Day-ahead generation forecast aggregated"),
    "load_forecast_weekly": EntsoeDocType("A65", "A31", "Week-ahead load forecast"),
    "net_transfer_capacity": EntsoeDocType("A61", "A01", "Net transfer capacity day-ahead"),
    # Phase 3 additions — balancing datasets (controlArea_Domain.mRID)
    "imbalance_prices": EntsoeDocType("A85", None, "Imbalance prices", domain_style="control_area"),
    "imbalance_volume": EntsoeDocType("A86", "A16", "Imbalance volumes", domain_style="control_area"),
    "activated_balancing_qty": EntsoeDocType("A83", "A16", "Activated balancing energy quantity", domain_style="control_area"),
    "activated_balancing_prices": EntsoeDocType("A84", "A16", "Activated balancing energy prices", domain_style="control_area"),
    "contracted_reserves": EntsoeDocType("A81", None, "Contracted reserves", domain_style="control_area"),
}

# EIC (Energy Identification Codes) for key bidding zones
BIDDING_ZONES: dict[str, str] = {
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

# Default zones to include for each dataset (UK-centric defaults)
DEFAULT_ZONES: list[str] = ["GB", "FR", "NL", "BE", "DE-LU", "IE-SEM"]

# Default control areas for balancing datasets (UK-centric).
# Control area EICs may differ from bidding zone EICs in some regions;
# for GB and the European zones we cover they coincide with BIDDING_ZONES.
DEFAULT_CONTROL_AREAS: list[str] = ["GB"]

# ENTSO-E API datetime format
ENTSOE_DT_FORMAT = "%Y%m%d%H%M"
