"""Mocked Elexon connector and bronze-to-silver integration tests."""

from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import polars as pl
import pytest
import respx

import gridflow.silver.elexon  # noqa: F401
from gridflow.bronze.writer import BronzeWriter
from gridflow.config.settings import SourceConfig, load_settings
from gridflow.connectors.base import RawResponse
from gridflow.connectors.elexon.client import ElexonConnector
from gridflow.connectors.elexon.endpoints import ENDPOINTS, ParamStyle
from gridflow.silver.registry import get_transformer

if TYPE_CHECKING:
    from collections.abc import Callable


FIXTURES = Path(__file__).parent.parent / "fixtures" / "elexon"
BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"
TARGET_DATE = date(2024, 1, 15)
START = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
END = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)

FIXTURE_NAMES = {
    "bmunits_reference": "bmunits_response.json",
}

SILVER_CASES = [
    (
        "system_prices",
        {
            "settlement_date",
            "settlement_period",
            "system_sell_price",
            "system_buy_price",
        },
    ),
    (
        "boal",
        {
            "settlement_date",
            "settlement_period",
            "bm_unit_id",
            "acceptance_number",
            "timestamp_utc",
            "data_provider",
        },
    ),
    (
        "freq",
        {
            "timestamp_utc",
            "frequency_hz",
        },
    ),
    (
        "pn",
        {
            "settlement_date",
            "settlement_period",
            "bm_unit_id",
            "timestamp_utc",
        },
    ),
    (
        "bmunits_reference",
        {
            "bm_unit_id",
            "bm_unit_name",
            "fuel_type",
            "national_grid_bm_unit",
        },
    ),
]


def _active_elexon_config() -> SourceConfig:
    return load_settings().get_source_config("elexon")


def _fixture_body(dataset: str) -> bytes:
    fixture_name = FIXTURE_NAMES.get(dataset, f"{dataset}_response.json")
    return (FIXTURES / fixture_name).read_bytes()


def _synthetic_body(
    dataset: str,
    *,
    page: int = 1,
    total_pages: int = 1,
    data: list[dict[str, Any]] | None = None,
) -> bytes:
    payload = {
        "data": data if data is not None else [{"dataset": dataset, "page": page}],
        "metadata": {"currentPage": page, "totalPages": total_pages},
    }
    return json.dumps(payload).encode()


def _raw_response(
    dataset: str,
    body: bytes,
    *,
    data_date: date | None = TARGET_DATE,
    request_url: str | None = None,
    request_params: dict[str, Any] | None = None,
    page: int = 1,
    total_pages: int = 1,
    ) -> RawResponse:
    return RawResponse(
        body=body,
        source="elexon",
        dataset=dataset,
        request_url=request_url or f"{BASE_URL}/{dataset}",
        request_params=request_params or {"page": page},
        fetched_at=datetime(2024, 1, 15, 12, page, tzinfo=UTC),
        http_status=200,
        content_type="application/json",
        api_version="v1",
        data_date=data_date,
        page=page,
        total_pages=total_pages,
    )


def _silver_parquet_path(data_dir: Path, dataset: str) -> Path:
    if dataset == "bmunits_reference":
        return data_dir / "silver" / "elexon" / dataset / "bmunits_reference.parquet"

    return (
        data_dir
        / "silver"
        / "elexon"
        / dataset
        / f"year={TARGET_DATE.year}"
        / f"month={TARGET_DATE.month:02d}"
        / f"{dataset}_{TARGET_DATE:%Y%m%d}.parquet"
    )


def _assert_bronze_metadata(
    bronze_path: Path,
    *,
    dataset: str,
    request_params: dict[str, Any],
    page: int = 1,
    total_pages: int = 1,
) -> None:
    sidecar_path = bronze_path.with_name(f"{bronze_path.stem}.meta.json")
    assert sidecar_path.exists()

    metadata = json.loads(sidecar_path.read_text())
    assert metadata["source"] == "elexon"
    assert metadata["dataset"] == dataset
    assert metadata["request_url"]
    assert metadata["request_params"] == request_params
    assert metadata["api_version"] == "v1"
    assert metadata["http_status"] == 200
    assert metadata["content_type"] == "application/json"
    assert metadata["body_sha256"]
    assert metadata["body_size_bytes"] == bronze_path.stat().st_size
    assert metadata["page"] == page
    assert metadata["total_pages"] == total_pages


def _mock_all_elexon_gets(
    handler: Callable[[httpx.Request], httpx.Response],
) -> respx.Route:
    return respx.get(re.compile(rf"^{re.escape(BASE_URL)}/.*")).mock(
        side_effect=handler
    )


def _fetch_end_for_style(style: ParamStyle) -> datetime:
    if style in {ParamStyle.DATE_PATH, ParamStyle.SETTLEMENT_DATE_PERIOD}:
        return START
    return END


class TestElexonMockedRequestShape:
    """Plan marker for mocked Elexon request-shape coverage."""


@pytest.mark.parametrize("dataset", sorted(_active_elexon_config().datasets))
@respx.mock
@pytest.mark.asyncio
async def test_active_datasets_fetch_with_expected_mocked_request_shape(
    dataset: str,
) -> None:
    endpoint = ENDPOINTS[dataset]
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        params = dict(request.url.params)

        if endpoint.param_style == ParamStyle.SETTLEMENT_DATE_PERIOD:
            period = int(params["settlementPeriod"])
            data = [{"dataset": dataset, "settlementPeriod": period}] if period == 1 else []
            return httpx.Response(
                200,
                content=_synthetic_body(dataset, data=data),
            )

        return httpx.Response(200, content=_synthetic_body(dataset))

    route = _mock_all_elexon_gets(handler)

    async with ElexonConnector(_active_elexon_config()) as connector:
        responses = await connector.fetch(
            dataset, START, _fetch_end_for_style(endpoint.param_style)
        )

    assert route.called
    assert responses
    assert all(response.source == "elexon" for response in responses)
    assert all(response.dataset == dataset for response in responses)
    assert all(response.request_url.startswith(BASE_URL) for response in responses)

    first_request = requests[0]
    first_params = dict(first_request.url.params)
    first_response = responses[0]

    if endpoint.param_style == ParamStyle.DATE_PATH:
        assert first_request.url.path.endswith(f"{endpoint.path}/2024-01-15")
        assert first_params["page"] == "1"
        assert first_response.data_date == TARGET_DATE
        assert first_response.request_params["page"] == 1
    elif endpoint.param_style == ParamStyle.SETTLEMENT_DATE_PERIOD:
        assert first_params["settlementDate"] == "2024-01-15"
        assert first_params["settlementPeriod"] == "1"
        assert first_response.request_params["settlementPeriod"] == 1
        assert first_response.data_date == TARGET_DATE
    elif endpoint.param_style == ParamStyle.PUBLISH_DATETIME:
        assert endpoint.from_param is not None
        assert endpoint.to_param is not None
        assert endpoint.from_param in first_params
        assert endpoint.to_param in first_params
        assert first_response.request_params[endpoint.from_param].startswith(
            "2024-01-15T00:00:00"
        )
        if dataset == "uou2t14d":
            assert len(responses) == 6
            assert first_response.request_params[endpoint.to_param].startswith(
                "2024-01-15T04:00:00"
            )
        else:
            assert first_response.request_params[endpoint.to_param].startswith(
                "2024-01-16T00:00:00"
            )
    elif endpoint.param_style == ParamStyle.NO_PARAMS:
        assert first_params == {}
        assert first_response.request_params == {}
        assert first_response.data_date is None


@respx.mock
@pytest.mark.asyncio
async def test_mocked_elexon_fetch_follows_metadata_pagination() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        page = int(request.url.params.get("page", "1"))
        return httpx.Response(
            200,
            content=_synthetic_body("system_prices", page=page, total_pages=2),
        )

    _mock_all_elexon_gets(handler)

    async with ElexonConnector(_active_elexon_config()) as connector:
        responses = await connector.fetch("system_prices", START, START)

    assert [response.page for response in responses] == [1, 2]
    assert [response.total_pages for response in responses] == [2, 2]
    assert [request.url.params["page"] for request in requests] == ["1", "2"]


@respx.mock
@pytest.mark.asyncio
async def test_no_param_mocked_response_has_no_query_params_or_data_date() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert dict(request.url.params) == {}
        return httpx.Response(
            200,
            content=_synthetic_body("bmunits_reference"),
        )

    _mock_all_elexon_gets(handler)

    async with ElexonConnector(_active_elexon_config()) as connector:
        responses = await connector.fetch("bmunits_reference", START, END)

    assert len(responses) == 1
    assert responses[0].request_params == {}
    assert responses[0].data_date is None


class TestElexonFixtureBackedBronzeToSilver:
    """Plan marker for fixture-backed Elexon bronze-to-silver coverage."""


@pytest.mark.parametrize(("dataset", "expected_columns"), SILVER_CASES)
def test_fixture_response_writes_bronze_and_transforms_to_silver(
    tmp_path: Path,
    dataset: str,
    expected_columns: set[str],
) -> None:
    raw_response = _raw_response(
        dataset,
        _fixture_body(dataset),
        request_params={"page": 1},
    )

    bronze_path = BronzeWriter(tmp_path).write(raw_response)

    assert "bronze" in bronze_path.parts
    assert "elexon" in bronze_path.parts
    assert dataset in bronze_path.parts
    assert "2024" in bronze_path.parts
    assert "01" in bronze_path.parts
    assert "15" in bronze_path.parts
    _assert_bronze_metadata(
        bronze_path,
        dataset=dataset,
        request_params={"page": 1},
    )

    transformer = get_transformer("elexon", dataset, tmp_path)
    rows_written = transformer.run(TARGET_DATE)

    parquet_path = _silver_parquet_path(tmp_path, dataset)
    assert rows_written > 0
    assert parquet_path.exists()
    if dataset != "bmunits_reference":
        assert "year=2024" in parquet_path.parts

    silver_df = pl.read_parquet(parquet_path)
    assert len(silver_df) == rows_written
    assert expected_columns <= set(silver_df.columns)


def test_bronze_metadata_preserves_pagination_fields(tmp_path: Path) -> None:
    writer = BronzeWriter(tmp_path)

    first_path = writer.write(
        _raw_response(
            "system_prices",
            _synthetic_body("system_prices", page=1, total_pages=2),
            page=1,
            total_pages=2,
        )
    )
    second_path = writer.write(
        _raw_response(
            "system_prices",
            _synthetic_body("system_prices", page=2, total_pages=2),
            page=2,
            total_pages=2,
        )
    )

    _assert_bronze_metadata(
        first_path,
        dataset="system_prices",
        request_params={"page": 1},
        page=1,
        total_pages=2,
    )
    _assert_bronze_metadata(
        second_path,
        dataset="system_prices",
        request_params={"page": 2},
        page=2,
        total_pages=2,
    )
