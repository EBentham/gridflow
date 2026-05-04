"""Live GIE AGSI API-to-silver integration tests."""

from __future__ import annotations

import json
import os
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import httpx
import polars as pl
import pytest

import gridflow.silver.gie  # noqa: F401
from gridflow.bronze.writer import BronzeWriter
from gridflow.config.settings import SourceConfig, load_settings
from gridflow.connectors.gie.client import GieConnector
from gridflow.connectors.gie.endpoints import (
    GIE_MAX_CALLS_PER_MINUTE,
    QueryScope,
    build_storage_query_plan,
    expected_records_for_plan,
)
from gridflow.silver.registry import get_transformer

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.connectors.base import RawResponse

LIVE_DATE = date(2026, 5, 1)
LIVE_START = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
LIVE_END = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)
BASE_URL = "https://agsi.gie.eu"
FULL_INVENTORY = os.environ.get("GRIDFLOW_AGSI_FULL_INVENTORY_LIVE") == "1"


def _gie_config() -> SourceConfig:
    config = load_settings().get_source_config("gie_agsi").model_copy(
        update={"timeout": 30}
    )
    if not config.api_key and not os.environ.get("GIE_API_KEY"):
        pytest.skip("source=gie_agsi stage=setup outcome=missing GIE_API_KEY")
    return config


def _response_preview(body: bytes, limit: int = 500) -> str:
    return body[:limit].decode("utf-8", errors="replace").replace("\n", " ")


def _records_from_response(response: RawResponse) -> list[dict[str, Any]]:
    parsed = json.loads(response.body)
    records = parsed.get("data", []) if isinstance(parsed, dict) else []
    return records if isinstance(records, list) else []


def _silver_path(data_dir: Path, dataset: str, target_date: date) -> Path:
    return (
        data_dir
        / "silver"
        / "gie_agsi"
        / dataset
        / f"year={target_date.year}"
        / f"month={target_date.month:02d}"
        / f"{dataset}_{target_date:%Y%m%d}.parquet"
    )


def _assert_live_response(response: RawResponse, *, dataset: str, stage: str) -> None:
    content_type = response.content_type.lower()
    assert response.source == "gie_agsi", (
        f"source=gie_agsi dataset={dataset} stage={stage} "
        f"actual_source={response.source}"
    )
    assert response.dataset == dataset, (
        f"source=gie_agsi dataset={dataset} stage={stage} "
        f"actual_dataset={response.dataset}"
    )
    assert response.http_status == 200, (
        f"source=gie_agsi dataset={dataset} stage={stage} "
        f"status={response.http_status} url={response.request_url} "
        f"body_preview={_response_preview(response.body)}"
    )
    assert "json" in content_type, (
        f"source=gie_agsi dataset={dataset} stage={stage} "
        f"content_type={response.content_type} url={response.request_url}"
    )
    assert response.request_url.startswith(BASE_URL), (
        f"source=gie_agsi dataset={dataset} stage={stage} "
        f"url={response.request_url}"
    )
    assert response.body, (
        f"source=gie_agsi dataset={dataset} stage={stage} empty_body=true"
    )
    assert response.page >= 1, (
        f"source=gie_agsi dataset={dataset} stage={stage} page={response.page}"
    )
    assert response.total_pages >= 1, (
        f"source=gie_agsi dataset={dataset} stage={stage} "
        f"total_pages={response.total_pages}"
    )


def _assert_bronze_sidecar(bronze_path: Path, *, dataset: str) -> None:
    sidecar_path = bronze_path.with_name(f"{bronze_path.stem}.meta.json")
    assert sidecar_path.exists(), (
        f"source=gie_agsi dataset={dataset} stage=bronze sidecar={sidecar_path}"
    )
    metadata = json.loads(sidecar_path.read_text())
    assert metadata["source"] == "gie_agsi"
    assert metadata["dataset"] == dataset
    assert metadata["request_url"].startswith(BASE_URL)
    assert isinstance(metadata["request_params"], dict)
    assert metadata["api_version"] == "v1"
    assert metadata["http_status"] == 200
    assert metadata["body_sha256"]
    assert metadata["body_size_bytes"] == bronze_path.stat().st_size
    assert metadata["page"] >= 1
    assert metadata["total_pages"] >= 1


def _classify_empty_or_skip(response: RawResponse, *, dataset: str, stage: str) -> None:
    if _records_from_response(response):
        return
    pytest.skip(
        f"source=gie_agsi dataset={dataset} stage={stage} outcome=empty-no-data "
        f"url={response.request_url} status={response.http_status} "
        f"body_preview={_response_preview(response.body)}"
    )


def _trim_listing_payload(payload: dict[str, Any] | list[dict[str, Any]]) -> dict[str, Any]:
    rows = payload if isinstance(payload, list) else payload.get("data", [])
    if not isinstance(rows, list):
        pytest.skip("source=gie_agsi dataset=about_listing stage=listing invalid_shape=true")

    for company in rows:
        if not isinstance(company, dict):
            continue
        facilities = company.get("facilities") or []
        if isinstance(facilities, list) and facilities:
            trimmed = dict(company)
            trimmed["facilities"] = [facilities[0]]
            return {"data": [trimmed]}

    pytest.skip(
        "source=gie_agsi dataset=about_listing stage=listing "
        "outcome=no-company-with-facility"
    )


def _scope_kwargs(
    scope: QueryScope,
    listing_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    if scope == QueryScope.AGGREGATE_TYPE:
        return {"scope": scope, "aggregate_types": ("EU",)}
    if scope == QueryScope.COUNTRY:
        return {"scope": scope, "countries": ("DE",)}
    if scope in {QueryScope.COMPANY, QueryScope.FACILITY}:
        return {"scope": scope, "listing_payload": listing_payload}
    raise AssertionError(f"unsupported live storage scope: {scope}")


def _expected_entity_level(scope: QueryScope) -> str:
    return "aggregate_type" if scope == QueryScope.AGGREGATE_TYPE else scope.value


async def _listing_payload(connector: GieConnector) -> dict[str, Any]:
    payload = await connector._fetch_listing_payload()  # noqa: SLF001
    return _trim_listing_payload(payload)


@pytest.mark.parametrize(
    "scope",
    [
        QueryScope.AGGREGATE_TYPE,
        QueryScope.COUNTRY,
        QueryScope.COMPANY,
        QueryScope.FACILITY,
    ],
    ids=["aggregate", "country", "company", "facility"],
)
@pytest.mark.live
@pytest.mark.asyncio
async def test_live_agsi_storage_scopes_fetch_transform_or_classify_empty(
    tmp_data_dir: Path,
    scope: QueryScope,
) -> None:
    try:
        async with GieConnector(_gie_config()) as connector:
            listing_payload = (
                await _listing_payload(connector)
                if scope in {QueryScope.COMPANY, QueryScope.FACILITY}
                else None
            )
            responses = await connector.fetch(
                "storage_reports",
                LIVE_START,
                LIVE_END,
                **_scope_kwargs(scope, listing_payload),
            )
    except httpx.HTTPStatusError as exc:
        response = exc.response
        pytest.fail(
            f"source=gie_agsi dataset=storage_reports scope={scope.value} "
            f"stage=live fetch status={response.status_code} url={response.url} "
            f"body_preview={response.text[:500].replace(chr(10), ' ')}"
        )

    assert responses, (
        f"source=gie_agsi dataset=storage_reports scope={scope.value} "
        "stage=live fetch no responses"
    )

    selected_responses: list[RawResponse] = []
    for response in responses:
        _assert_live_response(response, dataset="storage_reports", stage="live fetch")
        if _records_from_response(response):
            selected_responses.append(response)

    if not selected_responses:
        _classify_empty_or_skip(
            responses[0],
            dataset="storage_reports",
            stage=f"live fetch scope={scope.value}",
        )

    writer = BronzeWriter(tmp_data_dir)
    for response in selected_responses:
        bronze_path = writer.write(response)
        assert tmp_data_dir in bronze_path.parents
        _assert_bronze_sidecar(bronze_path, dataset="storage_reports")

    target_date = selected_responses[0].data_date or LIVE_DATE
    rows_written = get_transformer("gie_agsi", "storage_reports", tmp_data_dir).run(
        target_date
    )
    assert rows_written > 0, (
        f"source=gie_agsi dataset=storage_reports scope={scope.value} "
        f"stage=silver target_date={target_date}"
    )
    parquet_path = _silver_path(tmp_data_dir, "storage_reports", target_date)
    assert parquet_path.exists()
    df = pl.read_parquet(parquet_path)
    assert len(df) == rows_written
    assert df["data_provider"].unique().to_list() == ["gie_agsi"]
    assert _expected_entity_level(scope) in set(df["entity_level"].to_list())


@pytest.mark.live
@pytest.mark.asyncio
async def test_live_agsi_unavailability_fetches_or_classifies_documented_ambiguity(
    tmp_data_dir: Path,
) -> None:
    try:
        async with GieConnector(_gie_config()) as connector:
            responses = await connector.fetch("unavailability", LIVE_START, LIVE_END)
    except httpx.HTTPStatusError as exc:
        response = exc.response
        pytest.skip(
            f"source=gie_agsi dataset=unavailability stage=live fetch "
            f"outcome=documented-ambiguity status={response.status_code} "
            f"url={response.url} body_preview={response.text[:500].replace(chr(10), ' ')}"
        )

    assert responses, "source=gie_agsi dataset=unavailability stage=live fetch no responses"
    selected_responses = [response for response in responses if _records_from_response(response)]
    if not selected_responses:
        response = responses[0]
        pytest.skip(
            "source=gie_agsi dataset=unavailability stage=live fetch "
            "outcome=empty-documented-ambiguity "
            f"url={response.request_url} status={response.http_status} "
            f"body_preview={_response_preview(response.body)}"
        )

    writer = BronzeWriter(tmp_data_dir)
    for response in selected_responses:
        _assert_live_response(response, dataset="unavailability", stage="live fetch")
        bronze_path = writer.write(response)
        _assert_bronze_sidecar(bronze_path, dataset="unavailability")

    target_date = selected_responses[0].data_date or LIVE_DATE
    rows_written = get_transformer("gie_agsi", "unavailability", tmp_data_dir).run(
        target_date
    )
    assert rows_written > 0
    df = pl.read_parquet(_silver_path(tmp_data_dir, "unavailability", target_date))
    assert len(df) == rows_written
    assert df["data_provider"].unique().to_list() == ["gie_agsi"]


@pytest.mark.live
@pytest.mark.asyncio
async def test_live_agsi_full_inventory_expected_counts_gate() -> None:
    async with GieConnector(_gie_config()) as connector:
        payload = await connector._fetch_listing_payload()  # noqa: SLF001

    planning_payload = payload if FULL_INVENTORY else _trim_listing_payload(payload)
    company_plan = build_storage_query_plan(
        scope=QueryScope.COMPANY,
        start=LIVE_DATE,
        listing_payload=planning_payload,
    )
    facility_plan = build_storage_query_plan(
        scope=QueryScope.FACILITY,
        start=LIVE_DATE,
        listing_payload=planning_payload,
    )

    assert GIE_MAX_CALLS_PER_MINUTE == 60
    assert expected_records_for_plan(company_plan) > 0
    assert expected_records_for_plan(facility_plan) > 0
    if not FULL_INVENTORY:
        pytest.skip(
            "source=gie_agsi dataset=storage_reports stage=full-inventory-gate "
            "outcome=representative-only set GRIDFLOW_AGSI_FULL_INVENTORY_LIVE=1 "
            "for the slow 60-calls-per-minute inventory gate"
        )
