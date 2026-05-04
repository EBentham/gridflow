"""Live NESO API-to-silver integration tests."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import httpx
import polars as pl
import pytest

import gridflow.silver.neso  # noqa: F401
from gridflow.bronze.writer import BronzeWriter
from gridflow.config.settings import SourceConfig, load_settings
from gridflow.connectors.neso.carbon_intensity import CarbonIntensityConnector
from gridflow.connectors.neso.endpoints import ENDPOINTS, ParserFamily
from gridflow.silver.registry import get_transformer

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.connectors.base import RawResponse

LIVE_DATE = date(2026, 2, 1)
LIVE_START = datetime(2026, 2, 1, 0, 0, tzinfo=UTC)
LIVE_END = datetime(2026, 2, 2, 0, 0, tzinfo=UTC)
BASE_URL = "https://api.carbonintensity.org.uk"


def _neso_config() -> SourceConfig:
    return load_settings().get_source_config("neso").model_copy(update={"timeout": 20})


def _response_preview(body: bytes, limit: int = 500) -> str:
    return body[:limit].decode("utf-8", errors="replace").replace("\n", " ")


def _records_from_response(response: RawResponse) -> list[dict[str, Any]]:
    parsed = json.loads(response.body)
    endpoint = ENDPOINTS[response.dataset]
    data = parsed.get("data") if isinstance(parsed, dict) else parsed

    if data is None:
        return []
    if endpoint.parser_family == ParserFamily.FACTORS and isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if endpoint.parser_family == ParserFamily.GENERATION and isinstance(data, dict):
        return data.get("generationmix", [])
    if endpoint.parser_family == ParserFamily.REGIONAL:
        return _regional_records(data)
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        return [data]
    return []


def _regional_records(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, dict):
        nested = data.get("data", [])
        return nested if isinstance(nested, list) else [data]
    if not isinstance(data, list):
        return []

    records: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        if isinstance(item.get("regions"), list):
            records.extend(item["regions"])
        elif isinstance(item.get("data"), list):
            records.extend(item["data"])
        else:
            records.append(item)
    return records


def _target_date(response: RawResponse) -> date:
    return response.data_date or response.fetched_at.date()


def _silver_path(data_dir: Path, dataset: str, target_date: date) -> Path:
    if ENDPOINTS[dataset].reference:
        return data_dir / "silver" / "neso" / dataset / f"{dataset}.parquet"
    return (
        data_dir
        / "silver"
        / "neso"
        / dataset
        / f"year={target_date.year}"
        / f"month={target_date.month:02d}"
        / f"{dataset}_{target_date:%Y%m%d}.parquet"
    )


def _assert_expected_columns(df: pl.DataFrame, dataset: str) -> None:
    family = ENDPOINTS[dataset].parser_family
    if family == ParserFamily.INTENSITY:
        expected = {"timestamp_utc", "forecast_gco2_kwh", "intensity_index"}
    elif family == ParserFamily.STATS:
        expected = {"timestamp_utc", "max_gco2_kwh", "average_gco2_kwh", "min_gco2_kwh"}
    elif family == ParserFamily.FACTORS:
        expected = {"fuel", "factor_gco2_kwh"}
    elif family == ParserFamily.GENERATION:
        expected = {"timestamp_utc", "fuel", "generation_percentage"}
    else:
        expected = {"timestamp_utc", "regionid", "fuel", "generation_percentage"}

    assert expected <= set(df.columns), (
        f"source=neso dataset={dataset} stage=silver "
        f"missing_columns={sorted(expected - set(df.columns))} columns={df.columns}"
    )


@pytest.mark.parametrize("dataset", sorted(ENDPOINTS))
@pytest.mark.live
@pytest.mark.asyncio
async def test_live_neso_endpoint_fetches_and_transforms_or_classifies_empty(
    tmp_data_dir: Path,
    dataset: str,
) -> None:
    try:
        async with CarbonIntensityConnector(_neso_config()) as connector:
            responses = await connector.fetch(dataset, LIVE_START, LIVE_END)
    except httpx.HTTPStatusError as exc:
        response = exc.response
        pytest.fail(
            f"source=neso dataset={dataset} stage=live fetch "
            f"status={response.status_code} url={response.url} "
            f"body_preview={response.text[:500].replace(chr(10), ' ')}"
        )

    assert responses, f"source=neso dataset={dataset} stage=live fetch no responses"
    selected_responses: list[RawResponse] = []
    for response in responses:
        assert response.source == "neso"
        assert response.dataset == dataset
        assert response.http_status == 200
        assert response.request_url.startswith(BASE_URL)
        assert "json" in response.content_type.lower()
        if _records_from_response(response):
            selected_responses.append(response)

    if not selected_responses:
        response = responses[0]
        pytest.skip(
            f"source=neso dataset={dataset} stage=live fetch "
            f"outcome=empty url={response.request_url} "
            f"body_preview={_response_preview(response.body)}"
        )

    writer = BronzeWriter(tmp_data_dir)
    for response in selected_responses:
        bronze_path = writer.write(response)
        assert bronze_path.exists()
        assert bronze_path.with_name(f"{bronze_path.stem}.meta.json").exists()

    target_date = _target_date(selected_responses[0])
    transformer = get_transformer("neso", dataset, tmp_data_dir)
    rows_written = transformer.run(target_date)
    assert rows_written > 0, (
        f"source=neso dataset={dataset} stage=silver target_date={target_date}"
    )

    parquet_path = _silver_path(tmp_data_dir, dataset, target_date)
    assert parquet_path.exists()
    df = pl.read_parquet(parquet_path)
    assert len(df) == rows_written
    assert "data_provider" in df.columns
    assert df["data_provider"].unique().to_list() == ["neso"]
    _assert_expected_columns(df, dataset)
