"""Integration tests for the Elexon connector with mocked HTTP."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
import respx

from gridflow.config.settings import DatasetConfig, SourceConfig
from gridflow.connectors.elexon.client import ElexonConnector

FIXTURES = Path(__file__).parent.parent / "fixtures" / "elexon"


@pytest.fixture
def elexon_config() -> SourceConfig:
    return SourceConfig(
        base_url="https://data.elexon.co.uk/bmrs/api/v1",
        api_key="test-key",
        api_key_header="x-api-key",
        rate_limit_per_second=100,
        timeout=5,
        datasets={
            "system_prices": DatasetConfig(
                endpoint="/balancing/settlement/system-prices",
            ),
        },
    )


@respx.mock
@pytest.mark.asyncio
async def test_fetch_system_prices(elexon_config: SourceConfig):
    """Test fetching system prices with mocked HTTP."""
    fixture = (FIXTURES / "system_prices_response.json").read_text()

    respx.get(
        "https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices"
    ).mock(return_value=httpx.Response(200, text=fixture))

    async with ElexonConnector(elexon_config) as connector:
        responses = await connector.fetch(
            dataset="system_prices",
            start=datetime(2024, 1, 15, tzinfo=timezone.utc),
            end=datetime(2024, 1, 15, tzinfo=timezone.utc),
        )

    assert len(responses) == 1
    assert responses[0].http_status == 200
    assert responses[0].source == "elexon"
    assert responses[0].dataset == "system_prices"

    # Verify the response body contains expected data
    data = json.loads(responses[0].body)
    assert "data" in data
    assert len(data["data"]) == 4


@respx.mock
@pytest.mark.asyncio
async def test_connector_lists_datasets(elexon_config: SourceConfig):
    """Test that the connector correctly lists available datasets."""
    connector = ElexonConnector(elexon_config)
    datasets = connector.list_datasets()
    assert "system_prices" in datasets


@respx.mock
@pytest.mark.asyncio
async def test_fetch_unknown_dataset_raises(elexon_config: SourceConfig):
    """Test that fetching an unknown dataset raises ValueError."""
    async with ElexonConnector(elexon_config) as connector:
        with pytest.raises(ValueError, match="Unknown Elexon dataset"):
            await connector.fetch(
                dataset="nonexistent",
                start=datetime(2024, 1, 15, tzinfo=timezone.utc),
                end=datetime(2024, 1, 15, tzinfo=timezone.utc),
            )
