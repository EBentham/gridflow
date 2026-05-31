"""Tests for the GIE AGSI endpoint catalog and query inventory contract."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from gridflow.config.settings import load_settings
from gridflow.connectors.gie.endpoints import (
    DEFAULT_PAGE_SIZE,
    ENDPOINTS,
    GIE_API_PATH,
    QueryScope,
    build_storage_query_plan,
    expected_records_for_plan,
    gas_day_range,
    parse_listing_inventory,
    storage_params_for_range,
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
    plan = build_storage_query_plan(
        scope=QueryScope.AGGREGATE_TYPE,
        start=date(2026, 5, 1),
        aggregate_types=("EU",),
    )

    assert len(plan) == 1
    assert expected_records_for_plan(plan) == 1
    assert plan[0].path == GIE_API_PATH
    assert plan[0].entity_key == "EU"
    assert plan[0].expected_gas_days == (date(2026, 5, 1),)
    assert plan[0].params == {
        "type": "EU",
        "date": "2026-05-01",
        "page": 1,
        "size": DEFAULT_PAGE_SIZE,
    }


def test_expected_plan_for_countries_exact_date() -> None:
    plan = build_storage_query_plan(
        scope=QueryScope.COUNTRY,
        start=date(2026, 5, 1),
        countries=("DE", "FR"),
    )

    assert len(plan) == 2
    assert expected_records_for_plan(plan) == 2
    assert [request.params["country"] for request in plan] == ["DE", "FR"]
    assert {request.params["date"] for request in plan} == {"2026-05-01"}


def test_expected_plan_for_listing_companies_and_facilities() -> None:
    listing = _load_listing_fixture()
    company_plan = build_storage_query_plan(
        scope=QueryScope.COMPANY,
        start=date(2026, 5, 1),
        listing_payload=listing,
    )
    facility_plan = build_storage_query_plan(
        scope=QueryScope.FACILITY,
        start=date(2026, 5, 1),
        listing_payload=listing,
    )

    assert len(company_plan) == 3
    assert expected_records_for_plan(company_plan) == 3
    assert [request.params["company"] for request in company_plan] == [
        "21X-DEMO-ALPHA",
        "21X-DEMO-BETA",
        "21X-DEMO-GAMMA",
    ]
    assert [request.params["country"] for request in company_plan] == [
        "DE",
        "FR",
        "GB",
    ]

    assert len(facility_plan) == 4
    assert expected_records_for_plan(facility_plan) == 4
    assert facility_plan[0].params["facility"] == "21W-DEMO-ALPHA-1"
    assert facility_plan[0].params["company"] == "21X-DEMO-ALPHA"
    assert facility_plan[0].params["country"] == "DE"
    assert facility_plan[-1].params["facility"] == "21W-DEMO-GAMMA-1"
    assert facility_plan[-1].params["company"] == "21X-DEMO-GAMMA"
    assert facility_plan[-1].params["country"] == "GB"


def test_date_range_planning_exposes_every_gas_day_target() -> None:
    plan = build_storage_query_plan(
        scope=QueryScope.COUNTRY,
        start=date(2026, 5, 1),
        end=date(2026, 5, 2),
        countries=("DE",),
    )
    range_plan = build_storage_query_plan(
        scope=QueryScope.COUNTRY,
        start=date(2026, 5, 1),
        end=date(2026, 5, 2),
        countries=("DE",),
        date_mode="range",
    )

    assert gas_day_range(date(2026, 5, 1), date(2026, 5, 2)) == (
        date(2026, 5, 1),
        date(2026, 5, 2),
    )
    assert len(plan) == 2
    assert expected_records_for_plan(plan) == 2
    assert [request.expected_gas_days[0] for request in plan] == [
        date(2026, 5, 1),
        date(2026, 5, 2),
    ]
    assert [request.params["date"] for request in plan] == ["2026-05-01", "2026-05-02"]

    assert len(range_plan) == 1
    assert expected_records_for_plan(range_plan) == 2
    assert range_plan[0].expected_gas_days == (date(2026, 5, 1), date(2026, 5, 2))
    assert range_plan[0].params == storage_params_for_range(
        start=date(2026, 5, 1),
        end=date(2026, 5, 2),
        scope=QueryScope.COUNTRY,
        entity_key="DE",
    )


def test_same_start_and_end_date_is_one_gas_day() -> None:
    plan = build_storage_query_plan(
        scope=QueryScope.COUNTRY,
        start=date(2026, 5, 1),
        end=date(2026, 5, 1),
        countries=("DE",),
    )

    assert len(plan) == 1
    assert expected_records_for_plan(plan) == 1
    assert plan[0].expected_gas_days == (date(2026, 5, 1),)
