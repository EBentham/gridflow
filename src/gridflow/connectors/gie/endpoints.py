"""GIE AGSI+ / ALSI API endpoint metadata and the AGSI storage request plan."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Any

# GIE API path (same path shape for AGSI and ALSI hosts).
GIE_API_PATH = "/api"

# Countries to fetch for AGSI (gas storage), keeping the previous public constant.
AGSI_COUNTRIES: list[str] = ["AT", "BE", "DE", "ES", "FR", "GB", "IT", "NL", "PL"]

# Countries to fetch for ALSI (LNG terminals), keeping the previous public constant.
ALSI_COUNTRIES: list[str] = ["BE", "ES", "FR", "GB", "IT", "NL", "PL", "PT"]

# GIE documents a 30 row default and a 300 row maximum. Gridflow uses the maximum
# as the fetch default to minimise pagination while respecting the API cap.
DOCUMENTED_DEFAULT_PAGE_SIZE = 30
DEFAULT_PAGE_SIZE = 300
MAX_PAGE_SIZE = 300
GIE_MAX_CALLS_PER_MINUTE = 60
DEFAULT_AGSI_AGGREGATE_TYPES: tuple[str, ...] = ("EU",)


class ParserFamily(StrEnum):
    """Response parser families used by GIE endpoint metadata."""

    STORAGE = "storage"
    LISTING = "listing"
    NEWS = "news"
    UNAVAILABILITY = "unavailability"
    LNG = "lng"


class QueryScope(StrEnum):
    """Supported AGSI query scopes."""

    AGGREGATE_TYPE = "aggregate_type"
    COUNTRY = "country"
    COMPANY = "company"
    FACILITY = "facility"
    LISTING = "listing"
    NEWS = "news"
    UNAVAILABILITY = "unavailability"


@dataclass(frozen=True)
class GieEndpoint:
    """Metadata for one GIE Transparency Platform endpoint family."""

    path: str
    family: ParserFamily
    query_scopes: tuple[QueryScope, ...]
    description: str
    response_key: str | None = "data"
    date_params: tuple[str, ...] = ()
    paginated: bool = False
    status: str = "active"
    implementation_phase: str = "L1"
    default_params: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class GieListedEntity:
    """Company or facility returned by ``/api/about?show=listing``."""

    scope: QueryScope
    name: str
    eic: str
    country: str
    entity_type: str | None = None
    url: str | None = None
    company_eic: str | None = None
    company_name: str | None = None


@dataclass(frozen=True)
class GieListingInventory:
    """Parsed AGSI company/facility inventory."""

    companies: tuple[GieListedEntity, ...]
    facilities: tuple[GieListedEntity, ...]


@dataclass(frozen=True)
class GieQueryRequest:
    """One planned AGSI storage request: one entity on one gas day."""

    gas_day: date
    params: dict[str, Any]


ENDPOINTS: dict[str, GieEndpoint] = {
    "storage_reports": GieEndpoint(
        path=GIE_API_PATH,
        family=ParserFamily.STORAGE,
        query_scopes=(
            QueryScope.AGGREGATE_TYPE,
            QueryScope.COUNTRY,
            QueryScope.COMPANY,
            QueryScope.FACILITY,
        ),
        date_params=("date", "from", "to"),
        paginated=True,
        implementation_phase="L2",
        description="AGSI gas storage reports by aggregate type, country, company, or facility.",
    ),
    "about_summary": GieEndpoint(
        path=f"{GIE_API_PATH}/about",
        family=ParserFamily.LISTING,
        query_scopes=(QueryScope.LISTING,),
        implementation_phase="L1",
        description="AGSI reference information and endpoint discovery.",
    ),
    "about_listing": GieEndpoint(
        path=f"{GIE_API_PATH}/about",
        family=ParserFamily.LISTING,
        query_scopes=(QueryScope.LISTING,),
        implementation_phase="L1",
        default_params={"show": "listing"},
        description="Flat AGSI operator and facility listing used for query inventory planning.",
    ),
    "news": GieEndpoint(
        path=f"{GIE_API_PATH}/news",
        family=ParserFamily.NEWS,
        query_scopes=(QueryScope.NEWS,),
        paginated=True,
        implementation_phase="deferred",
        description="AGSI news listing.",
    ),
    "news_item": GieEndpoint(
        path=f"{GIE_API_PATH}/news",
        family=ParserFamily.NEWS,
        query_scopes=(QueryScope.NEWS,),
        default_params={"turl": "{id}"},
        implementation_phase="deferred",
        description="AGSI news item details by turl identifier.",
    ),
    "unavailability": GieEndpoint(
        path=f"{GIE_API_PATH}/unavailability",
        family=ParserFamily.UNAVAILABILITY,
        query_scopes=(QueryScope.UNAVAILABILITY,),
        date_params=("start", "end"),
        paginated=True,
        implementation_phase="L3",
        description="AGSI storage unavailability reports.",
    ),
}


def gas_day_range(start: date | datetime, end: date | datetime | None = None) -> tuple[date, ...]:
    """Return inclusive gas days covered by an AGSI request window."""
    start_date = _coerce_date(start)
    end_date = _coerce_date(end) if end is not None else start_date
    if end_date < start_date:
        raise ValueError("end date must be on or after start date")

    days = (end_date - start_date).days
    return tuple(start_date + timedelta(days=offset) for offset in range(days + 1))


def parse_listing_inventory(payload: dict[str, Any] | list[dict[str, Any]]) -> GieListingInventory:
    """Parse ``/api/about?show=listing`` into company and facility inventory."""
    rows = _listing_rows(payload)
    companies: list[GieListedEntity] = []
    facilities: list[GieListedEntity] = []

    for company in rows:
        company_eic = _required_text(company, "eic", "code")
        company_name = _optional_text(company, "short_name", "name") or company_eic
        company_country = _optional_text(company, "country") or ""

        companies.append(
            GieListedEntity(
                scope=QueryScope.COMPANY,
                name=company_name,
                eic=company_eic,
                country=company_country,
                entity_type=_optional_text(company, "type"),
                url=_optional_text(company, "url"),
            )
        )

        for facility in _facilities(company):
            facility_eic = _required_text(facility, "eic", "code")
            facility_name = _optional_text(facility, "short_name", "name") or facility_eic
            facilities.append(
                GieListedEntity(
                    scope=QueryScope.FACILITY,
                    name=facility_name,
                    eic=facility_eic,
                    country=_optional_text(facility, "country") or company_country,
                    entity_type=_optional_text(facility, "type"),
                    url=_optional_text(facility, "url"),
                    company_eic=_optional_text(facility, "company") or company_eic,
                    company_name=company_name,
                )
            )

    return GieListingInventory(companies=tuple(companies), facilities=tuple(facilities))


def build_storage_query_plan(
    *,
    scope: QueryScope,
    start: date | datetime,
    end: date | datetime | None = None,
) -> tuple[GieQueryRequest, ...]:
    """Plan one AGSI storage request per entity per gas day.

    ``COUNTRY`` covers every AGSI country (the ``storage`` dataset) and
    ``AGGREGATE_TYPE`` the EU aggregate (``storage_reports``). The vendor also
    offers company and facility scopes; nothing fetches them.
    """
    if scope == QueryScope.COUNTRY:
        param, entities = "country", tuple(AGSI_COUNTRIES)
    elif scope == QueryScope.AGGREGATE_TYPE:
        param, entities = "type", DEFAULT_AGSI_AGGREGATE_TYPES
    else:
        raise ValueError(f"{scope.value} is not a planned storage scope")
    return tuple(
        GieQueryRequest(
            gas_day=gas_day,
            params={
                param: entity,
                "date": gas_day.isoformat(),
                "page": 1,
                "size": DEFAULT_PAGE_SIZE,
            },
        )
        for entity in entities
        for gas_day in gas_day_range(start, end)
    )


def _coerce_date(value: date | datetime) -> date:
    return value.date() if isinstance(value, datetime) else value


def _listing_rows(payload: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return payload

    data = payload.get("data", payload.get("companies", []))
    if not isinstance(data, list):
        raise ValueError("listing payload must contain a list at data or companies")
    if not all(isinstance(row, dict) for row in data):
        raise ValueError("listing rows must be objects")
    return data


def _facilities(company: dict[str, Any]) -> list[dict[str, Any]]:
    facilities = company.get("facilities", [])
    if facilities is None:
        return []
    if not isinstance(facilities, list):
        raise ValueError("company facilities must be a list")
    if not all(isinstance(row, dict) for row in facilities):
        raise ValueError("facility rows must be objects")
    return facilities


def _required_text(row: dict[str, Any], *keys: str) -> str:
    value = _optional_text(row, *keys)
    if value is None:
        joined = ", ".join(keys)
        raise ValueError(f"listing row is missing one of: {joined}")
    return value


def _optional_text(row: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = row.get(key)
        if value is not None and str(value):
            return str(value)
    return None
