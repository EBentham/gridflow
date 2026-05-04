"""Live ENTSO-G API-to-silver integration tests."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import httpx
import polars as pl
import pytest

import gridflow.silver.entsog  # noqa: F401
from gridflow.bronze.writer import BronzeWriter
from gridflow.config.settings import SourceConfig, load_settings
from gridflow.connectors.entsog.client import EntsogConnector
from gridflow.connectors.entsog.endpoints import ENDPOINTS
from gridflow.silver.registry import get_transformer

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.connectors.base import RawResponse

LIVE_DATE = date(2024, 1, 15)
LIVE_START = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
LIVE_END = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)
BASE_URL = "https://transparency.entsog.eu/api/v1"


def _entsog_config() -> SourceConfig:
    return load_settings().get_source_config("entsog").model_copy(
        update={"timeout": 20}
    )


def _response_preview(body: bytes, limit: int = 500) -> str:
    return body[:limit].decode("utf-8", errors="replace").replace("\n", " ")


def _records_from_response(response: RawResponse) -> list[dict[str, Any]]:
    endpoint = ENDPOINTS[response.dataset]
    parsed = json.loads(response.body)
    records = parsed.get(endpoint.response_key, []) if isinstance(parsed, dict) else []
    return records if isinstance(records, list) else []


def _silver_path(data_dir: Path, dataset: str, target_date: date) -> Path:
    endpoint = ENDPOINTS[dataset]
    if endpoint.reference:
        return data_dir / "silver" / "entsog" / dataset / f"{dataset}.parquet"
    return (
        data_dir
        / "silver"
        / "entsog"
        / dataset
        / f"year={target_date.year}"
        / f"month={target_date.month:02d}"
        / f"{dataset}_{target_date:%Y%m%d}.parquet"
    )


def _skip_if_no_data(exc: httpx.HTTPStatusError, *, dataset: str) -> None:
    response = exc.response
    body = response.text[:500].replace("\n", " ")
    if response.status_code == 404 and "No result found" in body:
        pytest.skip(
            f"source=entsog dataset={dataset} stage=live fetch "
            f"outcome=no-result url={response.url} body_preview={body}"
        )
    pytest.fail(
        f"source=entsog dataset={dataset} stage=live fetch "
        f"status={response.status_code} url={response.url} body_preview={body}"
    )


@pytest.mark.parametrize("dataset", sorted(ENDPOINTS))
@pytest.mark.live
@pytest.mark.asyncio
async def test_live_entsog_endpoint_fetches_and_transforms_or_classifies_empty(
    tmp_data_dir: Path,
    dataset: str,
) -> None:
    try:
        async with EntsogConnector(_entsog_config()) as connector:
            responses = await connector.fetch(dataset, LIVE_START, LIVE_END, limit=1)
    except httpx.HTTPStatusError as exc:
        _skip_if_no_data(exc, dataset=dataset)

    assert responses, f"source=entsog dataset={dataset} stage=live fetch no responses"
    response = responses[0]
    records = _records_from_response(response)
    if not records:
        pytest.skip(
            f"source=entsog dataset={dataset} stage=live fetch "
            f"outcome=empty url={response.request_url} "
            f"body_preview={_response_preview(response.body)}"
        )

    assert response.source == "entsog"
    assert response.dataset == dataset
    assert response.http_status == 200
    assert response.request_url.startswith(BASE_URL)
    assert "json" in response.content_type.lower()

    bronze_path = BronzeWriter(tmp_data_dir).write(response)
    assert bronze_path.exists()
    assert bronze_path.with_name(f"{bronze_path.stem}.meta.json").exists()

    target_date = response.data_date or LIVE_DATE
    transformer = get_transformer("entsog", dataset, tmp_data_dir)
    rows_written = transformer.run(target_date)
    assert rows_written > 0, (
        f"source=entsog dataset={dataset} stage=silver target_date={target_date}"
    )

    parquet_path = _silver_path(tmp_data_dir, dataset, target_date)
    assert parquet_path.exists()
    df = pl.read_parquet(parquet_path)
    assert len(df) == rows_written
    assert "data_provider" in df.columns
    assert df["data_provider"].unique().to_list() == ["entsog"]
