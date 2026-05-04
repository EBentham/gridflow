"""Mocked NESO connector and bronze-to-silver integration tests."""

from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx
import polars as pl
import pytest
import respx

import gridflow.silver.neso  # noqa: F401
from gridflow.bronze.writer import BronzeWriter
from gridflow.config.settings import SourceConfig, load_settings
from gridflow.connectors.base import RawResponse
from gridflow.connectors.neso.carbon_intensity import CarbonIntensityConnector
from gridflow.connectors.neso.endpoints import ENDPOINTS, ParserFamily, build_path
from gridflow.silver.registry import get_transformer

if TYPE_CHECKING:
    from pathlib import Path

BASE_URL = "https://api.carbonintensity.org.uk"
TARGET_DATE = date(2024, 1, 15)
START = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
END = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)


def _neso_config() -> SourceConfig:
    return load_settings().get_source_config("neso").model_copy(
        update={"rate_limit_per_second": 1000, "timeout": 5}
    )


def _body_for(dataset: str) -> bytes:
    endpoint = ENDPOINTS[dataset]
    match endpoint.parser_family:
        case ParserFamily.INTENSITY:
            payload = {
                "data": [
                    {
                        "from": "2024-01-15T00:00Z",
                        "to": "2024-01-15T00:30Z",
                        "intensity": {
                            "forecast": 245,
                            "actual": 239,
                            "index": "moderate",
                        },
                    }
                ]
            }
        case ParserFamily.STATS:
            payload = {
                "data": [
                    {
                        "from": "2024-01-15T00:00Z",
                        "to": "2024-01-16T00:00Z",
                        "intensity": {
                            "max": 250,
                            "average": 180,
                            "min": 120,
                            "index": "moderate",
                        },
                    }
                ]
            }
        case ParserFamily.FACTORS:
            payload = {"data": [{"Gas (Combined Cycle)": 394, "Wind": 0}]}
        case ParserFamily.GENERATION:
            record = {
                "from": "2024-01-15T00:00Z",
                "to": "2024-01-15T00:30Z",
                "generationmix": [
                    {"fuel": "gas", "perc": 41.5},
                    {"fuel": "wind", "perc": 20.2},
                ],
            }
            payload = {"data": record if dataset == "generation_current" else [record]}
        case ParserFamily.REGIONAL:
            payload = _regional_payload(dataset)

    return json.dumps(payload).encode()


def _regional_payload(dataset: str) -> dict[str, Any]:
    period = {
        "from": "2024-01-15T00:00Z",
        "to": "2024-01-15T00:30Z",
        "intensity": {"forecast": 120, "index": "low"},
        "generationmix": [
            {"fuel": "gas", "perc": 30.0},
            {"fuel": "wind", "perc": 40.0},
        ],
    }
    region = {
        "regionid": 13,
        "dnoregion": "UKPN London",
        "shortname": "London",
        "postcode": "RG10",
    }
    if dataset in {
        "regional_current",
        "regional_intensity",
        "regional_intensity_fw24h",
        "regional_intensity_fw48h",
        "regional_intensity_pt24h",
    }:
        return {"data": [{**period, "regions": [region]}]}
    if dataset.startswith("regional_intensity_") and (
        dataset.endswith("_postcode") or dataset.endswith("_regionid")
    ):
        return {"data": {**region, "data": [period]}}
    return {"data": [{**region, "data": [period]}]}


def _raw_response(dataset: str) -> RawResponse:
    endpoint = ENDPOINTS[dataset]
    return RawResponse(
        body=_body_for(dataset),
        content_type="application/json",
        source="neso",
        dataset=dataset,
        request_url=f"{BASE_URL}{endpoint.path_template}",
        request_params={},
        fetched_at=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
        http_status=200,
        api_version="v1",
        data_date=TARGET_DATE if not endpoint.reference else None,
    )


def _silver_path(data_dir: Path, dataset: str) -> Path:
    if ENDPOINTS[dataset].reference:
        return data_dir / "silver" / "neso" / dataset / f"{dataset}.parquet"
    return (
        data_dir
        / "silver"
        / "neso"
        / dataset
        / f"year={TARGET_DATE.year}"
        / f"month={TARGET_DATE.month:02d}"
        / f"{dataset}_{TARGET_DATE:%Y%m%d}.parquet"
    )


def _expected_columns(dataset: str) -> set[str]:
    family = ENDPOINTS[dataset].parser_family
    if family == ParserFamily.INTENSITY:
        return {"timestamp_utc", "forecast_gco2_kwh", "actual_gco2_kwh"}
    if family == ParserFamily.STATS:
        return {"timestamp_utc", "max_gco2_kwh", "average_gco2_kwh", "min_gco2_kwh"}
    if family == ParserFamily.FACTORS:
        return {"fuel", "factor_gco2_kwh"}
    if family == ParserFamily.GENERATION:
        return {"timestamp_utc", "fuel", "generation_percentage"}
    return {"timestamp_utc", "regionid", "fuel", "generation_percentage"}


@pytest.mark.parametrize("dataset", sorted(ENDPOINTS))
@respx.mock
@pytest.mark.asyncio
async def test_active_datasets_fetch_with_expected_mocked_request_shape(
    dataset: str,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            content=_body_for(dataset),
            headers={"content-type": "application/json"},
        )

    route = respx.get(re.compile(rf"^{re.escape(BASE_URL)}/.*")).mock(
        side_effect=handler
    )

    async with CarbonIntensityConnector(_neso_config()) as connector:
        responses = await connector.fetch(dataset, START, END)

    assert route.called
    expected_response_count = 48 if dataset == "intensity_period" else 1
    assert len(responses) == expected_response_count

    expected_path, path_values = build_path(ENDPOINTS[dataset], start=START, end=END)
    assert requests[0].url.path == expected_path
    assert dict(requests[0].url.params) == {}

    response = responses[0]
    assert response.source == "neso"
    assert response.dataset == dataset
    assert response.request_params == path_values
    assert response.data_date == (
        None if ENDPOINTS[dataset].reference else TARGET_DATE
    )


@respx.mock
@pytest.mark.asyncio
async def test_period_dataset_fetches_all_settlement_periods_for_each_day() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            content=_body_for("intensity_period"),
            headers={"content-type": "application/json"},
        )

    respx.get(re.compile(rf"^{re.escape(BASE_URL)}/.*")).mock(side_effect=handler)

    async with CarbonIntensityConnector(_neso_config()) as connector:
        responses = await connector.fetch("intensity_period", START, START + timedelta(days=2))

    assert len(responses) == 96
    assert requests[0].url.path == "/intensity/date/2024-01-15/1"
    assert requests[47].url.path == "/intensity/date/2024-01-15/48"
    assert requests[48].url.path == "/intensity/date/2024-01-16/1"
    assert requests[95].url.path == "/intensity/date/2024-01-16/48"
    assert responses[0].request_params["period"] == 1
    assert responses[47].request_params["period"] == 48
    assert responses[95].data_date == date(2024, 1, 16)


@respx.mock
@pytest.mark.asyncio
async def test_period_dataset_same_day_window_means_full_settlement_day() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            content=_body_for("intensity_period"),
            headers={"content-type": "application/json"},
        )

    respx.get(re.compile(rf"^{re.escape(BASE_URL)}/.*")).mock(side_effect=handler)

    async with CarbonIntensityConnector(_neso_config()) as connector:
        responses = await connector.fetch("intensity_period", START, START)

    assert len(responses) == 48
    assert requests[0].url.path == "/intensity/date/2024-01-15/1"
    assert requests[-1].url.path == "/intensity/date/2024-01-15/48"


@pytest.mark.parametrize(
    "dataset",
    [
        "carbon_intensity",
        "intensity_stats",
        "intensity_stats_block",
        "generation",
        "regional_intensity",
        "regional_intensity_postcode",
        "regional_intensity_regionid",
    ],
)
@respx.mock
@pytest.mark.asyncio
async def test_range_datasets_same_day_window_means_full_api_day(
    dataset: str,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            content=_body_for(dataset),
            headers={"content-type": "application/json"},
        )

    respx.get(re.compile(rf"^{re.escape(BASE_URL)}/.*")).mock(side_effect=handler)

    async with CarbonIntensityConnector(_neso_config()) as connector:
        responses = await connector.fetch(dataset, START, START)

    assert len(responses) == 1
    assert "2024-01-15T00:00Z/2024-01-16T00:00Z" in requests[0].url.path


@pytest.mark.parametrize(
    ("start", "end", "expected_count"),
    [
        (
            datetime(2026, 3, 29, 0, 0, tzinfo=UTC),
            datetime(2026, 3, 30, 0, 0, tzinfo=UTC),
            46,
        ),
        (
            datetime(2025, 10, 26, 0, 0, tzinfo=UTC),
            datetime(2025, 10, 27, 0, 0, tzinfo=UTC),
            50,
        ),
    ],
)
@respx.mock
@pytest.mark.asyncio
async def test_period_dataset_uses_gb_settlement_period_count_for_dst_days(
    start: datetime,
    end: datetime,
    expected_count: int,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            content=_body_for("intensity_period"),
            headers={"content-type": "application/json"},
        )

    respx.get(re.compile(rf"^{re.escape(BASE_URL)}/.*")).mock(side_effect=handler)

    async with CarbonIntensityConnector(_neso_config()) as connector:
        responses = await connector.fetch("intensity_period", start, end)

    assert len(responses) == expected_count
    assert requests[-1].url.path.endswith(f"/{expected_count}")


@respx.mock
@pytest.mark.asyncio
async def test_current_dataset_uses_requested_start_date_for_bronze_partition(
    tmp_data_dir: Path,
) -> None:
    respx.get(f"{BASE_URL}/intensity").mock(
        return_value=httpx.Response(
            200,
            content=_body_for("intensity_current"),
            headers={"content-type": "application/json"},
        )
    )

    writer = BronzeWriter(tmp_data_dir)
    async with CarbonIntensityConnector(_neso_config()) as connector:
        first = (await connector.fetch("intensity_current", START, END))[0]
        second_start = START + timedelta(days=1)
        second_end = END + timedelta(days=1)
        second = (await connector.fetch("intensity_current", second_start, second_end))[0]

    first_path = writer.write(first)
    second_path = writer.write(second)

    assert first.data_date == date(2024, 1, 15)
    assert second.data_date == date(2024, 1, 16)
    assert "2024" in first_path.parts
    assert "01" in first_path.parts
    assert "15" in first_path.parts
    assert "16" in second_path.parts

    transformer = get_transformer("neso", "intensity_current", tmp_data_dir)
    assert transformer.run(date(2024, 1, 15)) > 0
    assert transformer.run(date(2024, 1, 16)) > 0


@respx.mock
@pytest.mark.asyncio
async def test_daily_date_path_datasets_fetch_each_requested_day() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            content=_body_for("intensity_date"),
            headers={"content-type": "application/json"},
        )

    respx.get(re.compile(rf"^{re.escape(BASE_URL)}/.*")).mock(side_effect=handler)

    async with CarbonIntensityConnector(_neso_config()) as connector:
        responses = await connector.fetch("intensity_date", START, START + timedelta(days=3))

    assert [request.url.path for request in requests] == [
        "/intensity/date/2024-01-15",
        "/intensity/date/2024-01-16",
        "/intensity/date/2024-01-17",
    ]
    assert [response.data_date for response in responses] == [
        date(2024, 1, 15),
        date(2024, 1, 16),
        date(2024, 1, 17),
    ]


@pytest.mark.parametrize("dataset", sorted(ENDPOINTS))
def test_fixture_response_writes_bronze_and_transforms_to_silver(
    tmp_data_dir: Path,
    dataset: str,
) -> None:
    bronze_path = BronzeWriter(tmp_data_dir).write(_raw_response(dataset))
    sidecar_path = bronze_path.with_name(f"{bronze_path.stem}.meta.json")

    assert bronze_path.exists()
    assert sidecar_path.exists()

    transformer = get_transformer("neso", dataset, tmp_data_dir)
    rows_written = transformer.run(TARGET_DATE)

    parquet_path = _silver_path(tmp_data_dir, dataset)
    assert rows_written > 0
    assert parquet_path.exists()

    df = pl.read_parquet(parquet_path)
    assert len(df) == rows_written
    assert "data_provider" in df.columns
    assert df["data_provider"].unique().to_list() == ["neso"]
    assert _expected_columns(dataset) <= set(df.columns)

    if ENDPOINTS[dataset].parser_family == ParserFamily.FACTORS:
        assert set(df["fuel"].to_list()) == {"gas_combined_cycle", "wind"}
    if ENDPOINTS[dataset].parser_family == ParserFamily.REGIONAL:
        assert set(df["fuel"].to_list()) == {"gas", "wind"}
