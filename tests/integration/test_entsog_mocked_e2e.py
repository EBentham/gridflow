"""Mocked ENTSO-G connector and bronze-to-silver integration tests."""

from __future__ import annotations

import json
import re
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import httpx
import polars as pl
import pytest
import respx

import gridflow.silver.entsog  # noqa: F401
from gridflow.bronze.writer import BronzeWriter
from gridflow.config.settings import SourceConfig, load_settings
from gridflow.connectors.base import RawResponse
from gridflow.connectors.entsog.client import EntsogConnector
from gridflow.connectors.entsog.endpoints import ENDPOINTS
from gridflow.silver.registry import get_transformer

if TYPE_CHECKING:
    from pathlib import Path

BASE_URL = "https://transparency.entsog.eu/api/v1"
TARGET_DATE = date(2024, 1, 15)
START = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
END = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)


def _entsog_config() -> SourceConfig:
    source = load_settings().get_source_config("entsog")
    return source.model_copy(update={"rate_limit_per_second": 1000, "timeout": 5})


def _records_for(dataset: str) -> list[dict[str, Any]]:
    endpoint = ENDPOINTS[dataset]
    if endpoint.parser_family == "operational_data":
        return [
            {
                "id": f"{dataset}-1",
                "dataSet": "1",
                "indicator": endpoint.default_params["indicator"],
                "periodType": "day",
                "periodFrom": "2024-01-15 06:00:00",
                "periodTo": "2024-01-16 06:00:00",
                "operatorKey": "UK-TSO-0001",
                "operatorLabel": "National Gas Transmission",
                "pointKey": "ITP-00005",
                "pointLabel": "Bacton (IUK)",
                "directionKey": "exit",
                "unit": "kWh/d",
                "value": "1000000",
            }
        ]
    if endpoint.parser_family == "aggregated_data":
        return [
            {
                "id": f"{dataset}-1",
                "indicator": "Physical Flow",
                "periodType": "day",
                "periodFrom": "2024-01-15 06:00:00",
                "periodTo": "2024-01-16 06:00:00",
                "countryKey": "UK",
                "bzKey": "UK---------",
                "bzLong": "British Balancing Zone",
                "operatorKey": "UK-TSO-0001",
                "directionKey": "entry",
                "unit": "kWh/d",
                "value": "2500000",
            }
        ]
    if endpoint.parser_family.startswith("cmp"):
        record = {
            "id": f"{dataset}-1",
            "periodFrom": "2024-01-15 06:00:00",
            "periodTo": "2024-01-16 06:00:00",
            "operatorKey": "UK-TSO-0001",
            "pointKey": "ITP-00005",
            "directionKey": "exit",
            "unit": "kWh/d",
            "value": "1000",
        }
        if dataset == "cmp_auction_premiums":
            record.update(
                {
                    "isCAMRelevant": None,
                    "isCamRelevant": True,
                }
            )
        return [
            {
                **record,
            }
        ]
    if endpoint.parser_family == "interruptions":
        return [
            {
                "id": f"{dataset}-1",
                "periodFrom": "2024-01-15 06:00:00",
                "periodTo": "2024-01-16 06:00:00",
                "operatorKey": "UK-TSO-0001",
                "pointKey": "ITP-00005",
                "directionKey": "exit",
                "interruptionType": "Planned",
                "unit": "kWh/d",
                "value": "1000",
            }
        ]
    if endpoint.parser_family == "tariffs":
        return [
            {
                "id": f"{dataset}-1",
                "operator": "National Gas Transmission",
                "operatorKey": "UK-TSO-0001",
                "pointKey": "ITP-00005",
                "directionKey": "exit",
                "productPeriodFrom": "2024-01-15 00:00:00",
                "productPeriodTo": "2024-02-15 00:00:00",
                "applicableTariffCommonUnit": "0.1",
            }
        ]
    if endpoint.parser_family == "tariff_simulations":
        return [
            {
                "id": f"{dataset}-1",
                "operator": "National Gas Transmission",
                "operatorKey": "UK-TSO-0001",
                "pointKey": "ITP-00005",
                "directionKey": "exit",
                "simulationCostEur": "12.5",
            }
        ]
    if endpoint.parser_family == "urgent_market_messages":
        return [
            {
                "id": f"{dataset}-1",
                "messageId": "UMM-1",
                "publicationDateTime": "2024-01-15 06:00:00",
                "marketParticipantName": "National Gas Transmission",
                "eventStatus": "Active",
            }
        ]
    return [
        {
            "id": f"{dataset}-1",
            "operatorKey": "UK-TSO-0001",
            "operatorLabel": "National Gas Transmission",
            "pointKey": "ITP-00005",
            "pointLabel": "Bacton (IUK)",
            "directionKey": "exit",
            "countryKey": "UK",
            "lastUpdateDateTime": "2024-01-15 06:00:00",
        }
    ]


def _body_for(dataset: str) -> bytes:
    endpoint = ENDPOINTS[dataset]
    payload = {
        "meta": {
            "limit": 1,
            "offset": 0,
            "count": 1,
            "total": 1,
            "fields": list(_records_for(dataset)[0]),
        },
        endpoint.response_key: _records_for(dataset),
    }
    return json.dumps(payload).encode()


def _raw_response(dataset: str) -> RawResponse:
    endpoint = ENDPOINTS[dataset]
    return RawResponse(
        body=_body_for(dataset),
        content_type="application/json",
        source="entsog",
        dataset=dataset,
        request_url=f"{BASE_URL}{endpoint.path}",
        request_params={"limit": 1},
        fetched_at=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
        http_status=200,
        api_version="v1",
        data_date=TARGET_DATE if endpoint.requires_dates else None,
    )


def _silver_path(data_dir: Path, dataset: str) -> Path:
    endpoint = ENDPOINTS[dataset]
    if endpoint.reference:
        return data_dir / "silver" / "entsog" / dataset / f"{dataset}.parquet"
    return (
        data_dir
        / "silver"
        / "entsog"
        / dataset
        / f"year={TARGET_DATE.year}"
        / f"month={TARGET_DATE.month:02d}"
        / f"{dataset}_{TARGET_DATE:%Y%m%d}.parquet"
    )


@pytest.mark.parametrize("dataset", sorted(ENDPOINTS))
@respx.mock
@pytest.mark.asyncio
async def test_active_datasets_fetch_with_expected_mocked_request_shape(
    dataset: str,
) -> None:
    endpoint = ENDPOINTS[dataset]
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            200,
            content=_body_for(dataset),
            headers={"content-type": "application/json"},
        )

    route = respx.get(re.compile(rf"^{re.escape(BASE_URL)}/.*")).mock(side_effect=handler)

    async with EntsogConnector(_entsog_config()) as connector:
        responses = await connector.fetch(dataset, START, END, limit=1)

    assert route.called
    assert len(responses) == 1
    assert requests[0].url.path.endswith(endpoint.path)
    params = dict(requests[0].url.params)
    response = responses[0]

    assert params["limit"] == "1"
    assert params["timeZone"] == "UCT"
    assert response.request_params["limit"] == 1
    assert response.data_date == (TARGET_DATE if endpoint.requires_dates else None)

    if endpoint.requires_dates:
        assert params["from"] == "2024-01-15"
        assert params["to"] == "2024-01-16"
    else:
        assert "from" not in params
        assert "to" not in params

    for key, expected in endpoint.default_params.items():
        assert key in response.request_params
        if isinstance(expected, tuple):
            assert response.request_params[key] == ",".join(expected)
        else:
            assert response.request_params[key] == expected


@pytest.mark.parametrize("dataset", sorted(ENDPOINTS))
def test_fixture_response_writes_bronze_and_transforms_to_silver(
    tmp_data_dir: Path,
    dataset: str,
) -> None:
    bronze_path = BronzeWriter(tmp_data_dir).write(_raw_response(dataset))
    sidecar_path = bronze_path.with_name(f"{bronze_path.stem}.meta.json")

    assert bronze_path.exists()
    assert sidecar_path.exists()

    transformer = get_transformer("entsog", dataset, tmp_data_dir)
    rows_written = transformer.run(TARGET_DATE)

    assert rows_written > 0
    parquet_path = _silver_path(tmp_data_dir, dataset)
    assert parquet_path.exists()

    df = pl.read_parquet(parquet_path)
    assert len(df) == rows_written
    assert "data_provider" in df.columns
    assert df["data_provider"].unique().to_list() == ["entsog"]
    if ENDPOINTS[dataset].parser_family == "operational_data" and dataset != "physical_flows":
        assert "indicator" in df.columns
    if dataset == "physical_flows":
        assert "flow_gwh_per_day" in df.columns
    if dataset == "cmp_auction_premiums":
        assert "is_cam_relevant" in df.columns
        assert "isCAMRelevant" not in df.columns
        assert "isCamRelevant" not in df.columns
        assert df["is_cam_relevant"].to_list() == [True]


def test_date_window_transform_filters_bronze_records_to_target_date(
    tmp_data_dir: Path,
) -> None:
    dataset = "nominations"
    endpoint = ENDPOINTS[dataset]
    records = [
        {
            **_records_for(dataset)[0],
            "id": "nominations-20260417",
            "periodFrom": "2026-04-17T05:00:00+02:00",
            "periodTo": "2026-04-18T05:00:00+02:00",
        },
        {
            **_records_for(dataset)[0],
            "id": "nominations-20260418",
            "periodFrom": "2026-04-18T05:00:00+02:00",
            "periodTo": "2026-04-19T05:00:00+02:00",
        },
    ]
    body = json.dumps(
        {
            "meta": {"count": 2, "total": 2},
            endpoint.response_key: records,
        }
    ).encode()
    response = RawResponse(
        body=body,
        content_type="application/json",
        source="entsog",
        dataset=dataset,
        request_url=f"{BASE_URL}{endpoint.path}",
        request_params={"limit": -1},
        fetched_at=datetime(2026, 4, 17, 12, 0, tzinfo=UTC),
        http_status=200,
        api_version="v1",
        data_date=date(2026, 4, 17),
    )
    BronzeWriter(tmp_data_dir).write(response)

    transformer = get_transformer("entsog", dataset, tmp_data_dir)
    rows_written = transformer.run(date(2026, 4, 17))

    assert rows_written == 1
    df = pl.read_parquet(
        tmp_data_dir
        / "silver"
        / "entsog"
        / dataset
        / "year=2026"
        / "month=04"
        / "nominations_20260417.parquet"
    )
    assert df["id"].to_list() == ["nominations-20260417"]


class TestV2ENTSOG404ShortCircuit:
    """V2-FIX-07: ENTSOG vendor empty convention is HTTP 404 + body
    `{"message":"No result found"}`. Pre-V2 the @RETRY_POLICY-decorated
    `_request` retried 404 up to 5 times before reraising — wasted
    budget for an expected response. Short-circuit so the call returns
    the empty response immediately. Genuine non-empty 404s preserve
    the existing retry path."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_404_no_result_found_short_circuits_no_retry(self) -> None:
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(
                404,
                json={"message": "No result found"},
                headers={"content-type": "application/json"},
            )

        respx.get(re.compile(rf"^{re.escape(BASE_URL)}/.*")).mock(side_effect=handler)

        async with EntsogConnector(_entsog_config()) as connector:
            responses = await connector.fetch("methane_content", START, END, limit=1)

        assert len(requests) == 1, (
            f"expected exactly 1 request for vendor empty convention; "
            f"got {len(requests)} — RETRY_POLICY did not short-circuit"
        )
        assert len(responses) == 1
        assert responses[0].http_status == 404

    @respx.mock
    @pytest.mark.asyncio
    async def test_genuine_404_preserves_retry(self) -> None:
        """A 404 with a different message is treated as a real error
        and goes through the existing retry+raise path."""
        requests: list[httpx.Request] = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(
                404,
                json={"message": "Endpoint not found"},
                headers={"content-type": "application/json"},
            )

        respx.get(re.compile(rf"^{re.escape(BASE_URL)}/.*")).mock(side_effect=handler)

        async with EntsogConnector(_entsog_config()) as connector:
            with pytest.raises(httpx.HTTPStatusError):
                await connector.fetch("methane_content", START, END, limit=1)

        assert len(requests) > 1, f"expected retries for non-empty 404; got {len(requests)}"
