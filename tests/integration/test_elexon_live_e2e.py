"""Live Elexon API-to-silver integration tests."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import httpx
import polars as pl
import pytest

import gridflow.silver.elexon  # noqa: F401
from gridflow.bronze.writer import BronzeWriter
from gridflow.config.settings import SourceConfig, load_settings
from gridflow.connectors.elexon.client import ElexonConnector
from gridflow.connectors.elexon.endpoints import ENDPOINTS, EXCLUDED_ENDPOINTS, ParamStyle
from gridflow.silver.registry import get_transformer

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.connectors.base import RawResponse

LIVE_DATE = date(2026, 2, 1)
LIVE_START = datetime(2026, 2, 1, 0, 0, tzinfo=UTC)
LIVE_END = datetime(2026, 2, 2, 0, 0, tzinfo=UTC)
BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"


@dataclass(frozen=True)
class LiveCase:
    dataset: str
    expected_style: ParamStyle
    expected_columns: frozenset[str]


LIVE_CASES = [
    LiveCase(
        dataset="system_prices",
        expected_style=ParamStyle.DATE_PATH,
        expected_columns=frozenset({"system_sell_price", "system_buy_price"}),
    ),
    LiveCase(
        dataset="boal",
        expected_style=ParamStyle.PUBLISH_DATETIME,
        expected_columns=frozenset({"bm_unit_id", "acceptance_number"}),
    ),
    LiveCase(
        dataset="freq",
        expected_style=ParamStyle.PUBLISH_DATETIME,
        expected_columns=frozenset({"timestamp_utc", "frequency_hz"}),
    ),
    LiveCase(
        dataset="pn",
        expected_style=ParamStyle.SETTLEMENT_DATE_PERIOD,
        expected_columns=frozenset({"bm_unit_id", "level_from", "level_to"}),
    ),
    LiveCase(
        dataset="bmunits_reference",
        expected_style=ParamStyle.NO_PARAMS,
        expected_columns=frozenset({"bm_unit_id", "fuel_type"}),
    ),
]


def _active_elexon_config() -> SourceConfig:
    return load_settings().get_source_config("elexon")


def _response_preview(body: bytes, limit: int = 500) -> str:
    return body[:limit].decode("utf-8", errors="replace").replace("\n", " ")


def _silver_parquet_path(data_dir: Path, dataset: str, target_date: date) -> Path:
    if dataset == "bmunits_reference":
        return data_dir / "silver" / "elexon" / dataset / "bmunits_reference.parquet"

    return (
        data_dir
        / "silver"
        / "elexon"
        / dataset
        / f"year={target_date.year}"
        / f"month={target_date.month:02d}"
        / f"{dataset}_{target_date:%Y%m%d}.parquet"
    )


def _assert_live_response(
    response: RawResponse,
    *,
    dataset: str,
    stage: str,
) -> None:
    content_type = response.content_type.lower()

    assert response.source == "elexon", (
        f"source=elexon dataset={dataset} stage={stage} "
        f"expected_source=elexon actual_source={response.source}"
    )
    assert response.dataset == dataset, (
        f"source=elexon dataset={dataset} stage={stage} actual_dataset={response.dataset}"
    )
    assert response.http_status == 200, (
        f"source=elexon dataset={dataset} stage={stage} "
        f"status={response.http_status} url={response.request_url} "
        f"body_preview={_response_preview(response.body)}"
    )
    assert "json" in content_type, (
        f"source=elexon dataset={dataset} stage={stage} "
        f"content_type={response.content_type} url={response.request_url}"
    )
    assert response.request_url.startswith(BASE_URL), (
        f"source=elexon dataset={dataset} stage={stage} url={response.request_url}"
    )
    assert response.body, (
        f"source=elexon dataset={dataset} stage={stage} url={response.request_url} empty_body=true"
    )
    assert response.page >= 1, f"source=elexon dataset={dataset} stage={stage} page={response.page}"
    assert response.total_pages >= 1, (
        f"source=elexon dataset={dataset} stage={stage} total_pages={response.total_pages}"
    )


def _assert_bronze_sidecar(bronze_path: Path, *, dataset: str) -> None:
    sidecar_path = bronze_path.with_name(f"{bronze_path.stem}.meta.json")

    assert sidecar_path.exists(), (
        f"source=elexon dataset={dataset} stage=bronze sidecar={sidecar_path}"
    )
    metadata = json.loads(sidecar_path.read_text())
    assert metadata["source"] == "elexon"
    assert metadata["dataset"] == dataset
    assert metadata["request_url"].startswith(BASE_URL)
    assert isinstance(metadata["request_params"], dict)
    assert metadata["api_version"] == "v1"
    assert metadata["http_status"] == 200
    assert metadata["body_sha256"]
    assert metadata["body_size_bytes"] == bronze_path.stat().st_size
    assert metadata["page"] >= 1
    assert metadata["total_pages"] >= 1


def _records_from_response(response: RawResponse) -> list[dict[str, Any]]:
    parsed = json.loads(response.body)
    if isinstance(parsed, dict):
        records = parsed.get("data", [])
        return records if isinstance(records, list) else []
    if isinstance(parsed, list):
        return parsed
    return []


def _classify_empty_or_skip(response: RawResponse, *, dataset: str, stage: str) -> None:
    records = _records_from_response(response)
    if records:
        return

    pytest.skip(
        f"source=elexon dataset={dataset} stage={stage} outcome=empty-no-data "
        f"url={response.request_url} status={response.http_status} "
        f"body_preview={_response_preview(response.body)}"
    )


def _fetch_end_for(case: LiveCase) -> datetime:
    if case.expected_style in {ParamStyle.DATE_PATH, ParamStyle.SETTLEMENT_DATE_PERIOD}:
        return LIVE_START
    return LIVE_END


def _assert_silver_output(
    *,
    data_dir: Path,
    dataset: str,
    target_date: date,
    rows_written: int,
    expected_columns: frozenset[str],
) -> None:
    parquet_path = _silver_parquet_path(data_dir, dataset, target_date)
    assert parquet_path.exists(), (
        f"source=elexon dataset={dataset} stage=silver expected_path={parquet_path}"
    )

    df = pl.read_parquet(parquet_path)
    assert len(df) == rows_written
    assert expected_columns <= set(df.columns), (
        f"source=elexon dataset={dataset} stage=silver "
        f"missing_columns={sorted(expected_columns - set(df.columns))} "
        f"actual_columns={df.columns}"
    )
    if "data_provider" in df.columns:
        assert df["data_provider"].unique().to_list() == ["elexon"]


@pytest.mark.parametrize("case", LIVE_CASES, ids=[case.dataset for case in LIVE_CASES])
@pytest.mark.live
@pytest.mark.asyncio
async def test_live_representative_datasets_fetch_successfully_or_classify_empty(
    tmp_data_dir: Path,
    case: LiveCase,
) -> None:
    config = _active_elexon_config()
    assert case.dataset in config.datasets
    assert case.dataset in ENDPOINTS
    assert ENDPOINTS[case.dataset].param_style == case.expected_style

    try:
        async with ElexonConnector(config) as connector:
            responses = await connector.fetch(
                case.dataset,
                LIVE_START,
                _fetch_end_for(case),
            )
    except httpx.HTTPStatusError as exc:
        response = exc.response
        pytest.fail(
            f"source=elexon dataset={case.dataset} stage=live fetch "
            f"status={response.status_code} url={response.url} "
            f"body_preview={response.text[:500].replace(chr(10), ' ')}"
        )

    assert responses, f"source=elexon dataset={case.dataset} stage=live fetch no responses"

    selected_responses: list[RawResponse] = []
    for response in responses:
        _assert_live_response(response, dataset=case.dataset, stage="live fetch")
        if not selected_responses and _records_from_response(response):
            selected_responses.append(response)

    if not selected_responses:
        _classify_empty_or_skip(responses[0], dataset=case.dataset, stage="live fetch")

    writer = BronzeWriter(tmp_data_dir)
    target_date = selected_responses[0].data_date or LIVE_DATE
    for response in selected_responses:
        bronze_path = writer.write(response)
        assert tmp_data_dir in bronze_path.parents
        assert "bronze" in bronze_path.parts
        assert "elexon" in bronze_path.parts
        assert case.dataset in bronze_path.parts
        _assert_bronze_sidecar(bronze_path, dataset=case.dataset)

    transformer = get_transformer("elexon", case.dataset, tmp_data_dir)
    rows_written = transformer.run(target_date)
    assert rows_written > 0, (
        f"source=elexon dataset={case.dataset} stage=silver "
        f"target_date={target_date} rows_written={rows_written}"
    )
    _assert_silver_output(
        data_dir=tmp_data_dir,
        dataset=case.dataset,
        target_date=target_date,
        rows_written=rows_written,
        expected_columns=case.expected_columns,
    )


def test_live_known_excluded_endpoints_are_documented() -> None:
    assert {"bod", "generation_by_fuel", "indicative_imbalance_volumes"} <= set(EXCLUDED_ENDPOINTS)
    for dataset in ("bod", "generation_by_fuel", "indicative_imbalance_volumes"):
        assert EXCLUDED_ENDPOINTS[dataset]
