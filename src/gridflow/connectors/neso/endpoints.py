"""NESO Carbon Intensity API endpoint metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datetime import datetime

NESO_DATETIME_FORMAT = "%Y-%m-%dT%H:%MZ"
DEFAULT_POSTCODE = "RG10"
DEFAULT_REGION_ID = 13
DEFAULT_PERIOD = 1
DEFAULT_STATS_BLOCK_HOURS = 24


class ParserFamily(StrEnum):
    """Silver parser families used by NESO transformers."""

    INTENSITY = "intensity"
    FACTORS = "factors"
    STATS = "stats"
    GENERATION = "generation"
    REGIONAL = "regional"


@dataclass(frozen=True)
class NesoEndpoint:
    """Request metadata for one Carbon Intensity API route."""

    path_template: str
    category: str
    parser_family: ParserFamily
    description: str
    requires_window: bool = False
    daily_iteration: bool = False
    settlement_period_iteration: bool = False
    reference: bool = False
    default_values: dict[str, Any] = field(default_factory=dict)


ENDPOINTS: dict[str, NesoEndpoint] = {
    # Carbon Intensity - National
    "intensity_current": NesoEndpoint(
        path_template="/intensity",
        category="Carbon Intensity - National",
        parser_family=ParserFamily.INTENSITY,
        description="Current half-hour national carbon intensity.",
    ),
    "intensity_today": NesoEndpoint(
        path_template="/intensity/date",
        category="Carbon Intensity - National",
        parser_family=ParserFamily.INTENSITY,
        description="All half-hour national carbon intensity records for today.",
    ),
    "intensity_date": NesoEndpoint(
        path_template="/intensity/date/{date}",
        category="Carbon Intensity - National",
        parser_family=ParserFamily.INTENSITY,
        description="All half-hour national carbon intensity records for a date.",
        requires_window=True,
        daily_iteration=True,
    ),
    "intensity_period": NesoEndpoint(
        path_template="/intensity/date/{date}/{period}",
        category="Carbon Intensity - National",
        parser_family=ParserFamily.INTENSITY,
        description="National carbon intensity for a date and settlement period.",
        requires_window=True,
        daily_iteration=True,
        settlement_period_iteration=True,
        default_values={"period": DEFAULT_PERIOD},
    ),
    "intensity_factors": NesoEndpoint(
        path_template="/intensity/factors",
        category="Carbon Intensity - National",
        parser_family=ParserFamily.FACTORS,
        description="Static generation fuel emission factors.",
        reference=True,
    ),
    "intensity_at": NesoEndpoint(
        path_template="/intensity/{from_dt}",
        category="Carbon Intensity - National",
        parser_family=ParserFamily.INTENSITY,
        description="National carbon intensity record ending at a datetime.",
        requires_window=True,
    ),
    "intensity_fw24h": NesoEndpoint(
        path_template="/intensity/{from_dt}/fw24h",
        category="Carbon Intensity - National",
        parser_family=ParserFamily.INTENSITY,
        description="National carbon intensity forecast from a datetime forward 24 hours.",
        requires_window=True,
    ),
    "intensity_fw48h": NesoEndpoint(
        path_template="/intensity/{from_dt}/fw48h",
        category="Carbon Intensity - National",
        parser_family=ParserFamily.INTENSITY,
        description="National carbon intensity forecast from a datetime forward 48 hours.",
        requires_window=True,
    ),
    "intensity_pt24h": NesoEndpoint(
        path_template="/intensity/{from_dt}/pt24h",
        category="Carbon Intensity - National",
        parser_family=ParserFamily.INTENSITY,
        description="National carbon intensity records for the 24 hours before a datetime.",
        requires_window=True,
    ),
    "carbon_intensity": NesoEndpoint(
        path_template="/intensity/{from_dt}/{to_dt}",
        category="Carbon Intensity - National",
        parser_family=ParserFamily.INTENSITY,
        description="National carbon intensity records for a datetime range.",
        requires_window=True,
    ),
    # Statistics - National
    "intensity_stats": NesoEndpoint(
        path_template="/intensity/stats/{from_dt}/{to_dt}",
        category="Statistics - National",
        parser_family=ParserFamily.STATS,
        description="National carbon intensity statistics for a datetime range.",
        requires_window=True,
    ),
    "intensity_stats_block": NesoEndpoint(
        path_template="/intensity/stats/{from_dt}/{to_dt}/{block}",
        category="Statistics - National",
        parser_family=ParserFamily.STATS,
        description="National carbon intensity statistics split into hour blocks.",
        requires_window=True,
        default_values={"block": DEFAULT_STATS_BLOCK_HOURS},
    ),
    # Generation Mix - National beta
    "generation_current": NesoEndpoint(
        path_template="/generation",
        category="Generation Mix - National beta",
        parser_family=ParserFamily.GENERATION,
        description="Current national generation mix.",
    ),
    "generation_pt24h": NesoEndpoint(
        path_template="/generation/{from_dt}/pt24h",
        category="Generation Mix - National beta",
        parser_family=ParserFamily.GENERATION,
        description="National generation mix for the 24 hours before a datetime.",
        requires_window=True,
    ),
    "generation": NesoEndpoint(
        path_template="/generation/{from_dt}/{to_dt}",
        category="Generation Mix - National beta",
        parser_family=ParserFamily.GENERATION,
        description="National generation mix for a datetime range.",
        requires_window=True,
    ),
    # Carbon Intensity - Regional beta
    "regional_current": NesoEndpoint(
        path_template="/regional",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Current regional carbon intensity for all GB regions.",
    ),
    "regional_england": NesoEndpoint(
        path_template="/regional/england",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Current carbon intensity for England.",
    ),
    "regional_scotland": NesoEndpoint(
        path_template="/regional/scotland",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Current carbon intensity for Scotland.",
    ),
    "regional_wales": NesoEndpoint(
        path_template="/regional/wales",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Current carbon intensity for Wales.",
    ),
    "regional_postcode": NesoEndpoint(
        path_template="/regional/postcode/{postcode}",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Current regional carbon intensity for a postcode.",
        default_values={"postcode": DEFAULT_POSTCODE},
    ),
    "regional_regionid": NesoEndpoint(
        path_template="/regional/regionid/{regionid}",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Current regional carbon intensity for a region ID.",
        default_values={"regionid": DEFAULT_REGION_ID},
    ),
    "regional_intensity_fw24h": NesoEndpoint(
        path_template="/regional/intensity/{from_dt}/fw24h",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Regional intensity forecast for all regions forward 24 hours.",
        requires_window=True,
    ),
    "regional_intensity_fw24h_postcode": NesoEndpoint(
        path_template="/regional/intensity/{from_dt}/fw24h/postcode/{postcode}",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Regional intensity forecast for a postcode forward 24 hours.",
        requires_window=True,
        default_values={"postcode": DEFAULT_POSTCODE},
    ),
    "regional_intensity_fw24h_regionid": NesoEndpoint(
        path_template="/regional/intensity/{from_dt}/fw24h/regionid/{regionid}",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Regional intensity forecast for a region ID forward 24 hours.",
        requires_window=True,
        default_values={"regionid": DEFAULT_REGION_ID},
    ),
    "regional_intensity_fw48h": NesoEndpoint(
        path_template="/regional/intensity/{from_dt}/fw48h",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Regional intensity forecast for all regions forward 48 hours.",
        requires_window=True,
    ),
    "regional_intensity_fw48h_postcode": NesoEndpoint(
        path_template="/regional/intensity/{from_dt}/fw48h/postcode/{postcode}",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Regional intensity forecast for a postcode forward 48 hours.",
        requires_window=True,
        default_values={"postcode": DEFAULT_POSTCODE},
    ),
    "regional_intensity_fw48h_regionid": NesoEndpoint(
        path_template="/regional/intensity/{from_dt}/fw48h/regionid/{regionid}",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Regional intensity forecast for a region ID forward 48 hours.",
        requires_window=True,
        default_values={"regionid": DEFAULT_REGION_ID},
    ),
    "regional_intensity_pt24h": NesoEndpoint(
        path_template="/regional/intensity/{from_dt}/pt24h",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Regional intensity for all regions in the prior 24 hours.",
        requires_window=True,
    ),
    "regional_intensity_pt24h_postcode": NesoEndpoint(
        path_template="/regional/intensity/{from_dt}/pt24h/postcode/{postcode}",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Regional intensity for a postcode in the prior 24 hours.",
        requires_window=True,
        default_values={"postcode": DEFAULT_POSTCODE},
    ),
    "regional_intensity_pt24h_regionid": NesoEndpoint(
        path_template="/regional/intensity/{from_dt}/pt24h/regionid/{regionid}",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Regional intensity for a region ID in the prior 24 hours.",
        requires_window=True,
        default_values={"regionid": DEFAULT_REGION_ID},
    ),
    "regional_intensity": NesoEndpoint(
        path_template="/regional/intensity/{from_dt}/{to_dt}",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Regional intensity for all regions in a datetime range.",
        requires_window=True,
    ),
    "regional_intensity_postcode": NesoEndpoint(
        path_template="/regional/intensity/{from_dt}/{to_dt}/postcode/{postcode}",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Regional intensity for a postcode in a datetime range.",
        requires_window=True,
        default_values={"postcode": DEFAULT_POSTCODE},
    ),
    "regional_intensity_regionid": NesoEndpoint(
        path_template="/regional/intensity/{from_dt}/{to_dt}/regionid/{regionid}",
        category="Carbon Intensity - Regional beta",
        parser_family=ParserFamily.REGIONAL,
        description="Regional intensity for a region ID in a datetime range.",
        requires_window=True,
        default_values={"regionid": DEFAULT_REGION_ID},
    ),
}


def build_path(
    endpoint: NesoEndpoint,
    *,
    start: datetime,
    end: datetime,
    **overrides: Any,
) -> tuple[str, dict[str, Any]]:
    """Build a Carbon Intensity API path and provenance path variables."""
    values: dict[str, Any] = {
        "from_dt": start.strftime(NESO_DATETIME_FORMAT),
        "to_dt": end.strftime(NESO_DATETIME_FORMAT),
        "date": start.strftime("%Y-%m-%d"),
        "period": DEFAULT_PERIOD,
        "block": DEFAULT_STATS_BLOCK_HOURS,
        "postcode": DEFAULT_POSTCODE,
        "regionid": DEFAULT_REGION_ID,
    }
    values.update(endpoint.default_values)
    values.update({key: value for key, value in overrides.items() if value is not None})

    path = endpoint.path_template.format(**values)
    return path, {
        key: value
        for key, value in values.items()
        if "{" + key + "}" in endpoint.path_template
    }
