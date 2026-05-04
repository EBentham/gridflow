"""ENTSO-G Transparency Platform API endpoint metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datetime import datetime

# ENTSO-G Transparency Platform API
ENTSOG_OPERATIONAL_PATH = "/operationalData"
ENTSOG_API_PATH = ENTSOG_OPERATIONAL_PATH  # Backwards-compatible alias.

# Standard query parameters
DEFAULT_PERIOD_TYPE = "day"
ENTSOG_TIMEZONE = "UCT"
ENTSOG_TIMEZONE_PARAM = "timeZone"
ENTSOG_ALL_RECORDS_LIMIT = -1
PHYSICAL_FLOW_INDICATOR = "Physical Flow"

# Key GB-relevant operator-point-direction keys. ENTSOG operational data expects
# operatorKey + pointKey + directionKey, not pointKey alone.
DEFAULT_POINT_DIRECTIONS: tuple[str, ...] = (
    "UK-TSO-0001ITP-00005exit",   # Bacton (IUK)
    "UK-TSO-0003ITP-00005entry",  # Bacton (IUK)
    "UK-TSO-0003ITP-00005exit",   # Bacton (IUK)
    "UK-TSO-0001ITP-00207exit",   # Bacton (BBL)
    "UK-TSO-0004ITP-00063entry",  # Julianadorp/Balgzand (BBL)
    "UK-TSO-0004ITP-00063exit",   # Julianadorp/Balgzand (BBL)
    "IE-TSO-0002ITP-00495entry",  # Moffat (IE)
    "IE-TSO-0002ITP-00495exit",   # Moffat (IE)
    "UK-TSO-0001ITP-00090entry",  # Moffat
)

# Legacy point labels retained for endpoint reference docs/tests.
KEY_POINT_KEYS: list[str] = [
    "IUK",
    "BBL",
    "FRAN",
    "IRL",
    "NIRL",
    "NORI",
]

# Aggregate-zone direction keys use bzKey + operatorKey + directionKey +
# adjacentSystemsKey. Keep the initial default narrow for GB zone data.
DEFAULT_AGGREGATED_POINT_DIRECTIONS: tuple[str, ...] = (
    "UK---------IE-TSO-0001entryTransmissionUK-NI------",
    "UK---------IE-TSO-0001exitTransmissionUK-NI------",
    "UK---------UK-TSO-0001entryLNG Terminals",
    "UK---------UK-TSO-0001entryProduction",
    "UK---------UK-TSO-0001entryStorage",
)


@dataclass(frozen=True)
class EntsogEndpoint:
    """Request metadata for one Gridflow ENTSOG dataset."""

    path: str
    response_key: str
    category: str
    parser_family: str
    requires_dates: bool = False
    reference: bool = False
    default_params: dict[str, Any] = field(default_factory=dict)
    description: str = ""


def _operational_endpoint(
    indicator: str,
    description: str,
    *,
    point_directions: tuple[str, ...] | None = DEFAULT_POINT_DIRECTIONS,
) -> EntsogEndpoint:
    default_params: dict[str, Any] = {
        "indicator": indicator,
        "periodType": DEFAULT_PERIOD_TYPE,
    }
    if point_directions is not None:
        default_params["pointDirection"] = point_directions

    return EntsogEndpoint(
        path=ENTSOG_OPERATIONAL_PATH,
        response_key="operationalData",
        category="Point Operational Data",
        parser_family="operational_data",
        requires_dates=True,
        default_params=default_params,
        description=description,
    )


OPERATIONAL_INDICATORS: dict[str, str] = {
    "physical_flows": PHYSICAL_FLOW_INDICATOR,
    "nominations": "Nomination",
    "allocations": "Allocation",
    "renominations": "Renomination",
    "firm_available": "Firm Available",
    "firm_booked": "Firm Booked",
    "firm_technical": "Firm Technical",
    "interruptible_available": "Interruptible Available",
    "interruptible_booked": "Interruptible Booked",
    "interruptible_total": "Interruptible Total",
    "gcv": "GCV",
    "wobbe_index": "Wobbe Index",
    "methane_content": "Methane Content",
    "hydrogen_content": "Hydrogen Content",
    "oxygen_content": "Oxygen Content",
    "available_through_oversubscription": "Available through Oversubscription",
    "available_through_surrender": "Available through Surrender",
    "available_through_uioli_long_term": "Available through UIOLI long-term",
    "available_through_uioli_short_term": "Available through UIOLI short-term",
}


ENDPOINTS: dict[str, EntsogEndpoint] = {
    dataset: _operational_endpoint(
        indicator,
        f"Operational data: {indicator}",
        point_directions=None if dataset == "physical_flows" else DEFAULT_POINT_DIRECTIONS,
    )
    for dataset, indicator in OPERATIONAL_INDICATORS.items()
}

ENDPOINTS.update({
    "cmp_unsuccessful_requests": EntsogEndpoint(
        path="/cmpUnsuccessfulRequests",
        response_key="cmpUnsuccessfulRequests",
        category="CMP Data",
        parser_family="cmp_unsuccessful_requests",
        requires_dates=True,
        default_params={"periodType": DEFAULT_PERIOD_TYPE},
        description="CMP unsuccessful requests.",
    ),
    "cmp_unavailable_firm_capacity": EntsogEndpoint(
        path="/cmpUnavailables",
        response_key="cmpUnavailables",
        category="CMP Data",
        parser_family="cmp_unavailable_firm_capacity",
        requires_dates=True,
        default_params={"periodType": DEFAULT_PERIOD_TYPE},
        description="CMP unavailable firm capacity.",
    ),
    "cmp_auction_premiums": EntsogEndpoint(
        path="/cmpAuctions",
        response_key="cmpAuctions",
        category="CMP Data",
        parser_family="cmp_auction_premiums",
        requires_dates=True,
        default_params={"periodType": DEFAULT_PERIOD_TYPE},
        description="CMP auction premiums.",
    ),
    "interruptions": EntsogEndpoint(
        path="/interruptions",
        response_key="interruptions",
        category="Interruptions",
        parser_family="interruptions",
        requires_dates=True,
        default_params={
            "periodType": DEFAULT_PERIOD_TYPE,
            "pointDirection": DEFAULT_POINT_DIRECTIONS,
        },
        description="Planned and unplanned interruptions.",
    ),
    "aggregated_physical_flows": EntsogEndpoint(
        path="/aggregatedData",
        response_key="aggregatedData",
        category="Zone Data",
        parser_family="aggregated_data",
        requires_dates=True,
        default_params={
            "indicator": PHYSICAL_FLOW_INDICATOR,
            "periodType": DEFAULT_PERIOD_TYPE,
            "pointDirection": DEFAULT_AGGREGATED_POINT_DIRECTIONS,
        },
        description="Aggregated zone-level physical flows.",
    ),
    "tariffs": EntsogEndpoint(
        path="/tariffsFulls",
        response_key="tariffsFulls",
        category="Tariff Data",
        parser_family="tariffs",
        requires_dates=True,
        default_params={"countryKey": "UK"},
        description="Tariff types and components.",
    ),
    "tariff_simulations": EntsogEndpoint(
        path="/tariffsSimulations",
        response_key="tariffsSimulations",
        category="Tariff Data",
        parser_family="tariff_simulations",
        requires_dates=True,
        default_params={"countryKey": "UK"},
        description="Tariff simulation costs.",
    ),
    "urgent_market_messages": EntsogEndpoint(
        path="/urgentMarketMessages",
        response_key="urgentMarketMessages",
        category="UMM Data",
        parser_family="urgent_market_messages",
        reference=True,
        description="Urgent market messages.",
    ),
    "connection_points": EntsogEndpoint(
        path="/connectionPoints",
        response_key="connectionPoints",
        category="Referential Data",
        parser_family="connection_points",
        reference=True,
        description="Interconnection points visible on the map.",
    ),
    "operators": EntsogEndpoint(
        path="/operators",
        response_key="operators",
        category="Referential Data",
        parser_family="operators",
        reference=True,
        default_params={"hasData": 1},
        description="Transmission system operators.",
    ),
    "balancing_zones": EntsogEndpoint(
        path="/balancingZones",
        response_key="balancingZones",
        category="Referential Data",
        parser_family="balancing_zones",
        reference=True,
        description="European balancing zones.",
    ),
    "operator_point_directions": EntsogEndpoint(
        path="/operatorPointDirections",
        response_key="operatorPointDirections",
        category="Referential Data",
        parser_family="operator_point_directions",
        reference=True,
        default_params={"hasData": 1},
        description="Operator, point, and flow-direction combinations.",
    ),
    "interconnections": EntsogEndpoint(
        path="/interconnections",
        response_key="interconnections",
        category="Referential Data",
        parser_family="interconnections",
        reference=True,
        default_params={"fromCountryKey": "UK"},
        description="Interconnections between systems.",
    ),
    "aggregate_interconnections": EntsogEndpoint(
        path="/aggregateInterconnections",
        response_key="aggregateInterconnections",
        category="Referential Data",
        parser_family="aggregate_interconnections",
        reference=True,
        default_params={"countryKey": "UK"},
        description="Connections between TSOs and balancing zones.",
    ),
})


def build_params(
    endpoint: EntsogEndpoint,
    *,
    start: datetime,
    end: datetime,
    **overrides: Any,
) -> dict[str, Any]:
    """Build ENTSOG query parameters for an endpoint."""
    params: dict[str, Any] = {
        "limit": ENTSOG_ALL_RECORDS_LIMIT,
        ENTSOG_TIMEZONE_PARAM: ENTSOG_TIMEZONE,
    }
    if endpoint.requires_dates:
        params["from"] = start.strftime("%Y-%m-%d")
        params["to"] = end.strftime("%Y-%m-%d")

    params.update(endpoint.default_params)
    params.update({key: value for key, value in overrides.items() if value is not None})
    return {
        key: _encode_multi_value(value)
        for key, value in params.items()
        if value is not None
    }


def _encode_multi_value(value: Any) -> Any:
    if isinstance(value, (list, tuple, set)):
        return ",".join(str(item) for item in value)
    return value
