"""GIE AGSI+ / ALSI API endpoint metadata and query planning helpers."""

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

    def entities_for_scope(self, scope: QueryScope | str) -> tuple[GieListedEntity, ...]:
        """Return listing entities for a company or facility query scope."""
        normalised = QueryScope(scope)
        if normalised == QueryScope.COMPANY:
            return self.companies
        if normalised == QueryScope.FACILITY:
            return self.facilities
        raise ValueError(f"Listing inventory does not contain {normalised.value} entities")


@dataclass(frozen=True)
class GieQueryRequest:
    """One planned AGSI storage request plus its expected gas-day coverage."""

    dataset: str
    path: str
    scope: QueryScope
    entity_key: str
    params: dict[str, Any]
    expected_gas_days: tuple[date, ...]
    page: int = 1
    size: int = DEFAULT_PAGE_SIZE

    @property
    def expected_records(self) -> int:
        """Expected time-series rows before pagination expansion."""
        return len(self.expected_gas_days)


@dataclass(frozen=True)
class _StorageTarget:
    scope: QueryScope
    entity_key: str
    country: str | None = None
    company: str | None = None


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


def gas_day_range(
    start: date | datetime | str,
    end: date | datetime | str | None = None,
) -> tuple[date, ...]:
    """Return inclusive gas days covered by an AGSI request window."""
    start_date = _coerce_date(start)
    end_date = _coerce_date(end) if end is not None else start_date
    if end_date < start_date:
        raise ValueError("end date must be on or after start date")

    days = (end_date - start_date).days
    return tuple(start_date + timedelta(days=offset) for offset in range(days + 1))


def storage_params_for_date(
    *,
    target_date: date | datetime | str,
    scope: QueryScope | str,
    entity_key: str,
    country: str | None = None,
    company: str | None = None,
    page: int = 1,
    size: int = DEFAULT_PAGE_SIZE,
) -> dict[str, Any]:
    """Build AGSI storage params for one exact gas day and query scope."""
    params = _storage_scope_params(
        scope=scope,
        entity_key=entity_key,
        country=country,
        company=company,
    )
    params.update({
        "date": _coerce_date(target_date).isoformat(),
        "page": page,
        "size": _normalise_page_size(size),
    })
    return params


def storage_params_for_range(
    *,
    start: date | datetime | str,
    end: date | datetime | str,
    scope: QueryScope | str,
    entity_key: str,
    country: str | None = None,
    company: str | None = None,
    page: int = 1,
    size: int = DEFAULT_PAGE_SIZE,
) -> dict[str, Any]:
    """Build AGSI storage params for an inclusive gas-day range."""
    start_date = _coerce_date(start)
    end_date = _coerce_date(end)
    if end_date < start_date:
        raise ValueError("end date must be on or after start date")

    params = _storage_scope_params(
        scope=scope,
        entity_key=entity_key,
        country=country,
        company=company,
    )
    params.update({
        "from": start_date.isoformat(),
        "to": end_date.isoformat(),
        "page": page,
        "size": _normalise_page_size(size),
    })
    return params


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
    scope: QueryScope | str,
    start: date | datetime | str,
    end: date | datetime | str | None = None,
    aggregate_types: tuple[str, ...] = DEFAULT_AGSI_AGGREGATE_TYPES,
    countries: tuple[str, ...] | None = None,
    listing_payload: dict[str, Any] | list[dict[str, Any]] | None = None,
    date_mode: str = "exact",
    page: int = 1,
    size: int = DEFAULT_PAGE_SIZE,
) -> tuple[GieQueryRequest, ...]:
    """Plan expected AGSI storage requests for a scope and gas-day window.

    ``date_mode="exact"`` creates one request per entity per gas day. This is
    the most explicit shape for bronze completeness tests. ``date_mode="range"``
    creates one request per entity whose expected rows cover the whole window.
    """
    normalised_scope = QueryScope(scope)
    gas_days = gas_day_range(start, end)
    targets = _scope_targets(
        scope=normalised_scope,
        aggregate_types=aggregate_types,
        countries=countries,
        listing_payload=listing_payload,
    )
    normalised_size = _normalise_page_size(size)

    if date_mode == "exact":
        requests: list[GieQueryRequest] = []
        for target in targets:
            for gas_day in gas_days:
                requests.append(
                    GieQueryRequest(
                        dataset="storage_reports",
                        path=GIE_API_PATH,
                        scope=normalised_scope,
                        entity_key=target.entity_key,
                        params=storage_params_for_date(
                            target_date=gas_day,
                            scope=normalised_scope,
                            entity_key=target.entity_key,
                            country=target.country,
                            company=target.company,
                            page=page,
                            size=normalised_size,
                        ),
                        expected_gas_days=(gas_day,),
                        page=page,
                        size=normalised_size,
                    )
                )
        return tuple(requests)

    if date_mode == "range":
        return tuple(
            GieQueryRequest(
                dataset="storage_reports",
                path=GIE_API_PATH,
                scope=normalised_scope,
                entity_key=target.entity_key,
                params=storage_params_for_range(
                    start=gas_days[0],
                    end=gas_days[-1],
                    scope=normalised_scope,
                    entity_key=target.entity_key,
                    country=target.country,
                    company=target.company,
                    page=page,
                    size=normalised_size,
                ),
                expected_gas_days=gas_days,
                page=page,
                size=normalised_size,
            )
            for target in targets
        )

    raise ValueError("date_mode must be 'exact' or 'range'")


def expected_records_for_plan(plan: tuple[GieQueryRequest, ...]) -> int:
    """Return the expected pre-pagination time-series row count for a plan."""
    return sum(request.expected_records for request in plan)


def _coerce_date(value: date | datetime | str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _normalise_page_size(size: int) -> int:
    if size < 1:
        raise ValueError("size must be at least 1")
    return min(size, MAX_PAGE_SIZE)


def _storage_scope_params(
    *,
    scope: QueryScope | str,
    entity_key: str,
    country: str | None = None,
    company: str | None = None,
) -> dict[str, str]:
    normalised_scope = QueryScope(scope)
    if normalised_scope == QueryScope.AGGREGATE_TYPE:
        return {"type": entity_key}
    if normalised_scope == QueryScope.COUNTRY:
        return {"country": entity_key}
    if normalised_scope == QueryScope.COMPANY:
        params = {"company": entity_key}
        if country:
            params = {"country": country, **params}
        return params
    if normalised_scope == QueryScope.FACILITY:
        params = {"facility": entity_key}
        if company:
            params = {"company": company, **params}
        if country:
            params = {"country": country, **params}
        return params
    raise ValueError(f"{normalised_scope.value} is not a storage query scope")


def _scope_targets(
    *,
    scope: QueryScope,
    aggregate_types: tuple[str, ...],
    countries: tuple[str, ...] | None,
    listing_payload: dict[str, Any] | list[dict[str, Any]] | None,
) -> tuple[_StorageTarget, ...]:
    if scope == QueryScope.AGGREGATE_TYPE:
        return tuple(
            _StorageTarget(scope=scope, entity_key=aggregate_type)
            for aggregate_type in aggregate_types
        )
    if scope == QueryScope.COUNTRY:
        return tuple(
            _StorageTarget(scope=scope, entity_key=country)
            for country in (countries or tuple(AGSI_COUNTRIES))
        )
    if scope in (QueryScope.COMPANY, QueryScope.FACILITY):
        if listing_payload is None:
            raise ValueError(f"{scope.value} planning requires a listing payload")
        inventory = parse_listing_inventory(listing_payload)
        return tuple(
            _StorageTarget(
                scope=scope,
                entity_key=entity.eic,
                country=entity.country or None,
                company=entity.company_eic if scope == QueryScope.FACILITY else None,
            )
            for entity in inventory.entities_for_scope(scope)
        )
    raise ValueError(f"{scope.value} is not a storage query scope")


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
