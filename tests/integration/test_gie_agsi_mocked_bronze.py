"""Mocked AGSI request-shape and bronze completeness tests."""

from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from gridflow.bronze.writer import BronzeWriter
from gridflow.config.settings import SourceConfig, load_settings
from gridflow.connectors.gie.client import GieConnector
from gridflow.connectors.gie.endpoints import (
    DEFAULT_PAGE_SIZE,
    QueryScope,
    build_storage_query_plan,
    expected_records_for_plan,
)

BASE_URL = "https://agsi.gie.eu"
TARGET_DATE = date(2026, 5, 1)
START = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
END = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "gie"


def _gie_config() -> SourceConfig:
    return load_settings().get_source_config("gie_agsi").model_copy(
        update={"api_key": "test-key", "rate_limit_per_second": 1000, "timeout": 5}
    )


def _load_listing_fixture() -> dict[str, Any]:
    return json.loads((FIXTURES / "agsi_listing_response.json").read_text())


def _load_fixture(filename: str) -> dict[str, Any]:
    return json.loads((FIXTURES / filename).read_text())


def _storage_body(
    gas_day: date,
    *,
    last_page: int = 1,
    rows: int = 1,
    entity: str = "EU",
) -> bytes:
    payload = {
        "last_page": last_page,
        "total": rows,
        "gas_day": gas_day.isoformat(),
        "data": [
            {
                "name": entity,
                "code": entity,
                "gasDayStart": f"{gas_day.isoformat()}T06:00:00Z",
                "gasInStorage": "100.0",
            }
            for _ in range(rows)
        ],
    }
    return json.dumps(payload).encode()


def _mock_storage(handler: Any) -> respx.Route:
    return respx.get(re.compile(rf"^{re.escape(BASE_URL)}/.*")).mock(
        side_effect=handler
    )


def _request_params(requests: list[httpx.Request]) -> list[dict[str, str]]:
    return [dict(request.url.params) for request in requests]


@respx.mock
@pytest.mark.asyncio
async def test_aggregate_scope_uses_documented_request_params() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            content=_storage_body(TARGET_DATE),
            headers={"content-type": "application/json"},
        )

    route = _mock_storage(handler)

    async with GieConnector(_gie_config()) as connector:
        responses = await connector.fetch(
            "storage_reports",
            START,
            END,
            scope=QueryScope.AGGREGATE_TYPE,
            aggregate_types=("EU",),
        )

    assert route.called
    assert len(responses) == 1
    assert requests[0].url.path == "/api"
    assert _request_params(requests) == [
        {"type": "EU", "date": "2026-05-01", "page": "1", "size": "300"}
    ]
    assert responses[0].data_date == TARGET_DATE
    assert responses[0].total_pages == 1


@respx.mock
@pytest.mark.asyncio
async def test_country_scope_fetches_selected_countries_only() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        params = dict(request.url.params)
        return httpx.Response(
            200,
            content=_storage_body(TARGET_DATE, entity=params["country"]),
            headers={"content-type": "application/json"},
        )

    _mock_storage(handler)

    async with GieConnector(_gie_config()) as connector:
        responses = await connector.fetch(
            "storage_reports",
            START,
            END,
            scope=QueryScope.COUNTRY,
            countries=("DE", "FR"),
        )

    assert len(responses) == 2
    assert _request_params(requests) == [
        {"country": "DE", "date": "2026-05-01", "page": "1", "size": "300"},
        {"country": "FR", "date": "2026-05-01", "page": "1", "size": "300"},
    ]


@respx.mock
@pytest.mark.asyncio
async def test_company_scope_uses_listing_country_and_company() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        params = dict(request.url.params)
        return httpx.Response(
            200,
            content=_storage_body(TARGET_DATE, entity=params["company"]),
            headers={"content-type": "application/json"},
        )

    _mock_storage(handler)

    async with GieConnector(_gie_config()) as connector:
        responses = await connector.fetch(
            "storage_reports",
            START,
            END,
            scope=QueryScope.COMPANY,
            listing_payload=_load_listing_fixture(),
        )

    assert len(responses) == 3
    assert _request_params(requests) == [
        {
            "country": "DE",
            "company": "21X-DEMO-ALPHA",
            "date": "2026-05-01",
            "page": "1",
            "size": "300",
        },
        {
            "country": "FR",
            "company": "21X-DEMO-BETA",
            "date": "2026-05-01",
            "page": "1",
            "size": "300",
        },
        {
            "country": "GB",
            "company": "21X-DEMO-GAMMA",
            "date": "2026-05-01",
            "page": "1",
            "size": "300",
        },
    ]


@respx.mock
@pytest.mark.asyncio
async def test_facility_scope_uses_listing_country_company_and_facility() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        params = dict(request.url.params)
        return httpx.Response(
            200,
            content=_storage_body(TARGET_DATE, entity=params["facility"]),
            headers={"content-type": "application/json"},
        )

    _mock_storage(handler)

    async with GieConnector(_gie_config()) as connector:
        responses = await connector.fetch(
            "storage_reports",
            START,
            END,
            scope=QueryScope.FACILITY,
            listing_payload=_load_listing_fixture(),
        )

    assert len(responses) == 4
    assert _request_params(requests)[0] == {
        "country": "DE",
        "company": "21X-DEMO-ALPHA",
        "facility": "21W-DEMO-ALPHA-1",
        "date": "2026-05-01",
        "page": "1",
        "size": "300",
    }
    assert _request_params(requests)[-1] == {
        "country": "GB",
        "company": "21X-DEMO-GAMMA",
        "facility": "21W-DEMO-GAMMA-1",
        "date": "2026-05-01",
        "page": "1",
        "size": "300",
    }


@respx.mock
@pytest.mark.asyncio
async def test_storage_paginates_with_last_page_not_total() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        page = int(dict(request.url.params)["page"])
        return httpx.Response(
            200,
            content=_storage_body(
                TARGET_DATE,
                last_page=2,
                rows=300 if page == 1 else 1,
                entity=f"page-{page}",
            ),
            headers={"content-type": "application/json"},
        )

    _mock_storage(handler)

    async with GieConnector(_gie_config()) as connector:
        responses = await connector.fetch(
            "storage_reports",
            START,
            END,
            scope=QueryScope.AGGREGATE_TYPE,
            aggregate_types=("EU",),
        )

    assert len(responses) == 2
    assert [response.page for response in responses] == [1, 2]
    assert [response.total_pages for response in responses] == [2, 2]
    assert [params["page"] for params in _request_params(requests)] == ["1", "2"]


@respx.mock
@pytest.mark.asyncio
async def test_bronze_completeness_matches_expected_query_plan(
    tmp_data_dir: Path,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        params = dict(request.url.params)
        entity = (
            params.get("type")
            or params.get("country")
            or params.get("facility")
            or params.get("company")
            or "unknown"
        )
        return httpx.Response(
            200,
            content=_storage_body(TARGET_DATE, entity=f"{entity}-{len(requests)}"),
            headers={"content-type": "application/json"},
        )

    _mock_storage(handler)

    listing = _load_listing_fixture()
    expected_plan = (
        build_storage_query_plan(
            scope=QueryScope.AGGREGATE_TYPE,
            start=TARGET_DATE,
            aggregate_types=("EU",),
        )
        + build_storage_query_plan(
            scope=QueryScope.COUNTRY,
            start=TARGET_DATE,
            countries=("DE", "FR"),
        )
        + build_storage_query_plan(
            scope=QueryScope.COMPANY,
            start=TARGET_DATE,
            listing_payload=listing,
        )
        + build_storage_query_plan(
            scope=QueryScope.FACILITY,
            start=TARGET_DATE,
            listing_payload=listing,
        )
    )

    async with GieConnector(_gie_config()) as connector:
        responses = []
        responses.extend(
            await connector.fetch(
                "storage_reports",
                START,
                END,
                scope=QueryScope.AGGREGATE_TYPE,
                aggregate_types=("EU",),
            )
        )
        responses.extend(
            await connector.fetch(
                "storage_reports",
                START,
                END,
                scope=QueryScope.COUNTRY,
                countries=("DE", "FR"),
            )
        )
        responses.extend(
            await connector.fetch(
                "storage_reports",
                START,
                END,
                scope=QueryScope.COMPANY,
                listing_payload=listing,
            )
        )
        responses.extend(
            await connector.fetch(
                "storage_reports",
                START,
                END,
                scope=QueryScope.FACILITY,
                listing_payload=listing,
            )
        )

    writer = BronzeWriter(tmp_data_dir)
    paths = [writer.write(response) for response in responses]

    assert len(paths) == len(expected_plan)
    assert len(paths) == expected_records_for_plan(expected_plan)
    assert all(path.exists() for path in paths)
    assert all("2026" in path.parts and "05" in path.parts and "01" in path.parts for path in paths)

    sidecars = [path.with_name(f"{path.stem}.meta.json") for path in paths]
    assert all(sidecar.exists() for sidecar in sidecars)
    for sidecar in sidecars:
        metadata = json.loads(sidecar.read_text())
        assert metadata["source"] == "gie_agsi"
        assert metadata["dataset"] == "storage_reports"
        assert metadata["page"] == 1
        assert metadata["total_pages"] == 1
        assert metadata["request_params"]["date"] == TARGET_DATE.isoformat()


@respx.mock
@pytest.mark.asyncio
async def test_exact_day_rejects_out_of_window_storage_rows() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=_storage_body(date(2026, 5, 2)),
            headers={"content-type": "application/json"},
        )

    _mock_storage(handler)

    async with GieConnector(_gie_config()) as connector:
        with pytest.raises(ValueError, match="expected 2026-05-01"):
            await connector.fetch(
                "storage_reports",
                START,
                END,
                scope=QueryScope.AGGREGATE_TYPE,
                aggregate_types=("EU",),
            )


def test_default_page_size_matches_documented_fetch_cap() -> None:
    assert DEFAULT_PAGE_SIZE == 300


@respx.mock
@pytest.mark.asyncio
async def test_reference_endpoint_bronze_uses_requested_date_partition(
    tmp_data_dir: Path,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=_load_listing_fixture(),
            headers={"content-type": "application/json"},
        )

    _mock_storage(handler)

    async with GieConnector(_gie_config()) as connector:
        responses = await connector.fetch("about_listing", START, END)

    path = BronzeWriter(tmp_data_dir).write(responses[0])
    metadata = json.loads(path.with_name(f"{path.stem}.meta.json").read_text())

    assert "2026" in path.parts
    assert "05" in path.parts
    assert "01" in path.parts
    assert responses[0].data_date == TARGET_DATE
    assert metadata["data_date"] == TARGET_DATE.isoformat()
    assert metadata["request_params"] == {"show": "listing"}


@respx.mock
@pytest.mark.asyncio
async def test_news_item_without_turl_fetches_listing_details() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        params = dict(request.url.params)
        fixture = (
            "agsi_news_item_response.json"
            if params.get("turl") == "demo-maintenance"
            else "agsi_news_response.json"
        )
        return httpx.Response(
            200,
            json=_load_fixture(fixture),
            headers={"content-type": "application/json"},
        )

    _mock_storage(handler)

    async with GieConnector(_gie_config()) as connector:
        responses = await connector.fetch("news_item", START, END)

    assert len(responses) == 1
    assert responses[0].dataset == "news_item"
    assert responses[0].data_date == TARGET_DATE
    assert responses[0].request_params == {"turl": "demo-maintenance"}
    assert _request_params(requests) == [
        {"page": "1"},
        {"turl": "demo-maintenance"},
    ]


@respx.mock
@pytest.mark.asyncio
async def test_news_item_discards_listing_shaped_detail_response() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            json=_load_fixture("agsi_news_response.json"),
            headers={"content-type": "application/json"},
        )

    _mock_storage(handler)

    async with GieConnector(_gie_config()) as connector:
        responses = await connector.fetch("news_item", START, END)

    assert responses == []
    assert _request_params(requests) == [
        {"page": "1"},
        {"turl": "demo-maintenance"},
    ]
