"""Tests for the GIE AGSI endpoint catalog and query inventory contract."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml

from gridflow.config.settings import load_settings
from gridflow.connectors.gie.endpoints import (
    AGSI_COUNTRIES,
    DEFAULT_PAGE_SIZE,
    ENDPOINTS,
    QueryScope,
    build_storage_query_plan,
    gas_day_range,
    parse_listing_inventory,
)

ROOT = Path(__file__).resolve().parents[2]
CATALOG = ROOT / "docs" / "gie_agsi_endpoint_catalog.yaml"
FIXTURES = ROOT / "tests" / "fixtures" / "gie"
REQUIRED_ENDPOINT_FIELDS = {
    "id",
    "path",
    "method",
    "status",
    "family",
    "query_scopes",
    "date_params",
    "pagination",
    "response_key",
    "source_doc",
    "implementation_phase",
    "notes",
}


def _load_catalog() -> dict[str, Any]:
    return yaml.safe_load(CATALOG.read_text())


def _load_listing_fixture() -> dict[str, Any]:
    return json.loads((FIXTURES / "agsi_listing_response.json").read_text())


def _base_path(path: str) -> str:
    return path.split("?", maxsplit=1)[0]


def test_catalog_parses_and_statuses_are_explicit() -> None:
    catalog = _load_catalog()
    allowed_statuses = set(catalog["allowed_statuses"])
    rows = catalog["endpoints"]

    assert catalog["source"] == "gie_agsi"
    assert allowed_statuses == {"active", "deferred", "out_of_scope"}
    assert rows

    for row in rows:
        assert set(row) >= REQUIRED_ENDPOINT_FIELDS
        assert row["status"] in allowed_statuses


def test_active_catalog_rows_have_matching_endpoint_metadata() -> None:
    catalog = _load_catalog()
    active_rows = [row for row in catalog["endpoints"] if row["status"] == "active"]
    active_metadata_ids = {
        endpoint_id for endpoint_id, endpoint in ENDPOINTS.items() if endpoint.status == "active"
    }

    assert {row["id"] for row in active_rows} == active_metadata_ids

    for row in active_rows:
        endpoint = ENDPOINTS[row["id"]]
        assert endpoint.family.value == row["family"]
        assert endpoint.response_key == row["response_key"]
        assert endpoint.path == _base_path(row["path"])
        assert {scope.value for scope in endpoint.query_scopes} == set(row["query_scopes"])


def test_active_catalog_rows_are_exposed_by_source_config() -> None:
    catalog = _load_catalog()
    active_catalog_ids = {row["id"] for row in catalog["endpoints"] if row["status"] == "active"}
    config_datasets = set(load_settings().get_source_config("gie_agsi").datasets)

    assert active_catalog_ids <= config_datasets
    assert "storage" in config_datasets


def test_catalog_documents_last_page_as_pagination_authority() -> None:
    catalog = _load_catalog()
    paginated_rows = [
        row for row in catalog["endpoints"] if row["pagination"]["authoritative_total_pages"]
    ]

    assert paginated_rows
    for row in paginated_rows:
        assert row["pagination"]["authoritative_total_pages"] == "last_page"
        assert row["pagination"]["per_page_count"] == "total"
        assert row["pagination"]["max_size"] == DEFAULT_PAGE_SIZE


def test_listing_fixture_parses_companies_and_facilities() -> None:
    inventory = parse_listing_inventory(_load_listing_fixture())

    assert len(inventory.companies) == 3
    assert len(inventory.facilities) == 4
    assert {company.country for company in inventory.companies} == {"DE", "FR", "GB"}
    assert [company.eic for company in inventory.companies] == [
        "21X-DEMO-ALPHA",
        "21X-DEMO-BETA",
        "21X-DEMO-GAMMA",
    ]
    assert inventory.facilities[0].company_eic == "21X-DEMO-ALPHA"
    assert inventory.facilities[-1].country == "GB"


def test_expected_plan_for_aggregate_type_exact_date() -> None:
    plan = build_storage_query_plan(scope=QueryScope.AGGREGATE_TYPE, start=date(2026, 5, 1))

    assert len(plan) == 1
    assert plan[0].gas_day == date(2026, 5, 1)
    assert plan[0].params == {
        "type": "EU",
        "date": "2026-05-01",
        "page": 1,
        "size": DEFAULT_PAGE_SIZE,
    }


def test_expected_plan_for_countries_exact_date() -> None:
    plan = build_storage_query_plan(scope=QueryScope.COUNTRY, start=date(2026, 5, 1))

    assert [request.params["country"] for request in plan] == AGSI_COUNTRIES
    assert {request.params["date"] for request in plan} == {"2026-05-01"}


@pytest.mark.parametrize("scope", [QueryScope.COMPANY, QueryScope.FACILITY])
def test_company_and_facility_scopes_are_not_planned(scope: QueryScope) -> None:
    with pytest.raises(ValueError, match="not a planned storage scope"):
        build_storage_query_plan(scope=scope, start=date(2026, 5, 1))


def test_multi_day_window_plans_one_request_per_gas_day() -> None:
    plan = build_storage_query_plan(
        scope=QueryScope.AGGREGATE_TYPE,
        start=date(2026, 5, 1),
        end=date(2026, 5, 2),
    )

    assert gas_day_range(date(2026, 5, 1), date(2026, 5, 2)) == (
        date(2026, 5, 1),
        date(2026, 5, 2),
    )
    assert [request.gas_day for request in plan] == [date(2026, 5, 1), date(2026, 5, 2)]
    assert [request.params["date"] for request in plan] == ["2026-05-01", "2026-05-02"]


def test_same_start_and_end_date_is_one_gas_day() -> None:
    plan = build_storage_query_plan(
        scope=QueryScope.AGGREGATE_TYPE,
        start=date(2026, 5, 1),
        end=date(2026, 5, 1),
    )

    assert len(plan) == 1
    assert plan[0].gas_day == date(2026, 5, 1)
