"""ENTSO-E Transparency Platform API endpoint definitions."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class EntsoeDocType:
    """ENTSO-E document type definition."""

    document_type: str  # e.g. "A44"
    process_type: str | None  # e.g. "A01" (day-ahead), "A16" (realised)
    description: str
    # "zone" -> in_Domain + out_Domain query params (default)
    # "zone_pair" -> in_Domain + out_Domain query params over flow pairs
    # "in_domain" -> in_Domain query param only
    # "out_bidding_zone" -> outBiddingZone_Domain query param
    # "bidding_zone" -> BiddingZone_Domain query param
    # "control_area" -> controlArea_Domain query param
    domain_style: str = "zone"
    extra_params: dict[str, str] = field(default_factory=dict)
    optional_params: tuple[str, ...] = ()
    domain_params: tuple[str, ...] = ()
    date_param: str | None = None


# ENTSO-E document type registry (dataset name -> EntsoeDocType)
DOC_TYPES: dict[str, EntsoeDocType] = {
    "day_ahead_prices": EntsoeDocType("A44", None, "Day-ahead prices"),
    "actual_load": EntsoeDocType(
        "A65", "A16", "Actual total load", domain_style="out_bidding_zone"
    ),
    "load_forecast": EntsoeDocType(
        "A65", "A01", "Day-ahead load forecast", domain_style="out_bidding_zone"
    ),
    "actual_generation": EntsoeDocType(
        "A75", "A16", "Actual generation per production type", domain_style="in_domain"
    ),
    "wind_solar_forecast": EntsoeDocType(
        "A69", "A01", "Day-ahead wind / solar forecast", domain_style="in_domain"
    ),
    "cross_border_flows": EntsoeDocType(
        "A11", None, "Physical cross-border flows", domain_style="zone_pair"
    ),
    "outages_generation": EntsoeDocType(
        "A80",
        None,
        "Unavailability of generation units",
        domain_style="bidding_zone",
        extra_params={"BusinessType": "A53"},
    ),
    "installed_capacity": EntsoeDocType(
        "A68", "A33", "Installed generation capacity", domain_style="in_domain"
    ),
    "installed_capacity_units": EntsoeDocType(
        "A71",
        "A33",
        "Installed capacity per production unit",
        domain_style="in_domain",
    ),
    # Phase 2 additions
    "generation_forecast": EntsoeDocType(
        "A71", "A01", "Day-ahead generation forecast aggregated", domain_style="in_domain"
    ),
    "actual_generation_units": EntsoeDocType(
        "A73",
        "A16",
        "Actual generation per generation unit",
        domain_style="in_domain",
    ),
    "water_reservoirs": EntsoeDocType(
        "A72",
        "A16",
        "Water reservoirs and hydro storage plants",
        domain_style="in_domain",
    ),
    "generation_units_master_data": EntsoeDocType(
        "A95",
        None,
        "Production and generation units master data",
        domain_style="bidding_zone",
        extra_params={"BusinessType": "B11"},
        date_param="Implementation_DateAndOrTime",
    ),
    "load_forecast_weekly": EntsoeDocType(
        "A65", "A31", "Week-ahead load forecast", domain_style="out_bidding_zone"
    ),
    "load_forecast_monthly": EntsoeDocType(
        "A65", "A32", "Month-ahead load forecast", domain_style="out_bidding_zone"
    ),
    "load_forecast_yearly": EntsoeDocType(
        "A65", "A33", "Year-ahead load forecast", domain_style="out_bidding_zone"
    ),
    "forecast_margin": EntsoeDocType(
        "A70", "A33", "Year-ahead forecast margin", domain_style="out_bidding_zone"
    ),
    "net_transfer_capacity": EntsoeDocType(
        "A61",
        None,
        "Forecasted transfer capacity",
        domain_style="zone_pair",
        extra_params={"contract_MarketAgreement.Type": "A01"},
    ),
    "dc_link_intraday_transfer_limits": EntsoeDocType(
        "A93",
        None,
        "Cross-border capacity of DC links - intraday transfer limits",
        domain_style="zone_pair",
    ),
    "commercial_schedules": EntsoeDocType(
        "A09",
        None,
        "Commercial schedules",
        domain_style="zone_pair",
        optional_params=("contract_MarketAgreement.Type",),
    ),
    "commercial_schedules_net_positions": EntsoeDocType(
        "A09",
        None,
        "Commercial schedules - net positions",
        domain_style="zone_pair",
        optional_params=("contract_MarketAgreement.Type",),
    ),
    "redispatching_cross_border": EntsoeDocType(
        "A63",
        None,
        "Redispatching cross-border",
        domain_style="zone_pair",
        extra_params={"businessType": "A46"},
    ),
    "redispatching_internal": EntsoeDocType(
        "A63",
        None,
        "Redispatching internal",
        domain_style="zone_pair",
        extra_params={"businessType": "A85"},
    ),
    "countertrading": EntsoeDocType(
        "A91",
        None,
        "Countertrading",
        domain_style="zone_pair",
    ),
    "congestion_management_costs": EntsoeDocType(
        "A92",
        None,
        "Costs of congestion management",
        domain_style="zone",
    ),
    "offered_transfer_capacity_continuous": EntsoeDocType(
        "A31",
        None,
        "Continuous allocations - offered transfer capacity",
        domain_style="zone_pair",
        extra_params={
            "Auction.Type": "A01",
            "Contract_MarketAgreement.Type": "A01",
        },
        domain_params=("In_Domain", "Out_Domain"),
        optional_params=(
            "Auction.Type",
            "Contract_MarketAgreement.Type",
            "Update_DateAndOrTime",
        ),
    ),
    "offered_transfer_capacity_implicit": EntsoeDocType(
        "A31",
        None,
        "Implicit allocations - offered transfer capacity",
        domain_style="zone_pair",
        extra_params={
            "auction.Type": "A01",
            "contract_MarketAgreement.Type": "A01",
        },
        optional_params=("auction.Type", "contract_MarketAgreement.Type"),
    ),
    "offered_transfer_capacity_explicit": EntsoeDocType(
        "A31",
        None,
        "Explicit allocations - offered transfer capacity",
        domain_style="zone_pair",
        extra_params={
            "auction.Category": "A01",
            "auction.Type": "A01",
            "contract_MarketAgreement.Type": "A01",
        },
        optional_params=(
            "auction.Category",
            "auction.Type",
            "contract_MarketAgreement.Type",
        ),
    ),
    "auction_revenue": EntsoeDocType(
        "A25",
        None,
        "Explicit allocations - auction revenue",
        domain_style="zone_pair",
        extra_params={
            "businessType": "B07",
            "contract_MarketAgreement.Type": "A01",
        },
        optional_params=("contract_MarketAgreement.Type",),
    ),
    "transfer_capacity_use": EntsoeDocType(
        "A25",
        None,
        "Explicit allocations - use of the transfer capacity",
        domain_style="zone_pair",
        extra_params={
            "businessType": "B05",
            "Auction.Category": "A01",
            "contract_MarketAgreement.Type": "A01",
        },
        optional_params=("Auction.Category", "contract_MarketAgreement.Type"),
    ),
    "total_nominated_capacity": EntsoeDocType(
        "A26",
        None,
        "Total nominated capacity",
        domain_style="zone_pair",
        extra_params={"businessType": "B08"},
    ),
    "total_capacity_allocated": EntsoeDocType(
        "A26",
        None,
        "Total capacity already allocated",
        domain_style="zone_pair",
        extra_params={
            "businessType": "A29",
            "auction.Category": "A01",
            "contract_MarketAgreement.Type": "A01",
        },
        optional_params=("auction.Category", "contract_MarketAgreement.Type"),
    ),
    "congestion_income": EntsoeDocType(
        "A25",
        None,
        "Implicit and flow-based allocations - congestion income",
        domain_style="zone_pair",
        extra_params={
            "businessType": "B10",
            "contract_MarketAgreement.Type": "A01",
        },
        optional_params=("contract_MarketAgreement.Type",),
    ),
    "net_positions": EntsoeDocType(
        "A25",
        None,
        "Implicit auction - net positions",
        domain_style="zone",
        extra_params={
            "businessType": "B09",
            "contract_MarketAgreement.Type": "A01",
        },
        optional_params=("contract_MarketAgreement.Type",),
    ),
    # Phase 3 additions - balancing datasets (controlArea_Domain)
    "imbalance_prices": EntsoeDocType("A85", None, "Imbalance prices", domain_style="control_area"),
    "imbalance_volume": EntsoeDocType(
        "A86",
        None,
        "Imbalance volumes",
        domain_style="control_area",
        extra_params={"businessType": "A19"},
    ),
    "activated_balancing_prices": EntsoeDocType(
        "A84",
        "A16",
        "Activated balancing energy prices",
        domain_style="control_area",
        extra_params={"businessType": "A96"},
    ),
    "contracted_reserves": EntsoeDocType(
        "A81",
        "A52",
        "Contracted reserves",
        domain_style="control_area",
        # Type_MarketAgreement.Type is mandatory per ENTSO-E API (despite the
        # Postman catalog listing it as optional). A01=daily products.
        extra_params={"businessType": "B95", "Type_MarketAgreement.Type": "A01"},
    ),
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
