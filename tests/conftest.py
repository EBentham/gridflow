"""Shared test fixtures for gridflow tests."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from gridflow.config.settings import (
    DatasetConfig,
    GridflowConfig,
    PipelineSettings,
    QualityConfig,
    SourceConfig,
)
from gridflow.connectors.base import RawResponse


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    """Create a temporary data directory structure."""
    for layer in ["bronze", "silver", "gold"]:
        (tmp_path / layer).mkdir()
    return tmp_path


@pytest.fixture
def sample_config(tmp_data_dir: Path) -> GridflowConfig:
    """Create a sample GridflowConfig for testing."""
    return GridflowConfig(
        pipeline=PipelineSettings(
            data_dir=tmp_data_dir,
            log_dir=tmp_data_dir / "logs",
            duckdb_path=tmp_data_dir / "test.duckdb",
            default_lookback_hours=24,
            log_level="DEBUG",
        ),
        quality=QualityConfig(),
        sources={
            "elexon": SourceConfig(
                base_url="https://data.elexon.co.uk/bmrs/api/v1",
                api_key="test-key",
                api_key_header="x-api-key",
                rate_limit_per_second=100,
                timeout=5,
                datasets={
                    "system_prices": DatasetConfig(
                        endpoint="/balancing/settlement/system-prices",
                        schedule="hourly",
                        max_query_days=1,
                    ),
                },
            ),
        },
    )


@pytest.fixture
def sample_elexon_response_data() -> dict:
    """Sample Elexon system prices API response."""
    return {
        "data": [
            {
                "settlementDate": "2024-01-15",
                "settlementPeriod": 1,
                "systemSellPrice": 45.50,
                "systemBuyPrice": 55.00,
                "netImbalanceVolume": -120.5,
                "settlementRunType": "SF",
            },
            {
                "settlementDate": "2024-01-15",
                "settlementPeriod": 2,
                "systemSellPrice": 46.75,
                "systemBuyPrice": 56.25,
                "netImbalanceVolume": 80.3,
                "settlementRunType": "SF",
            },
            {
                "settlementDate": "2024-01-15",
                "settlementPeriod": 3,
                "systemSellPrice": 48.00,
                "systemBuyPrice": 58.50,
                "netImbalanceVolume": -45.0,
                "settlementRunType": "SF",
            },
        ]
    }


@pytest.fixture
def sample_raw_response(sample_elexon_response_data: dict) -> RawResponse:
    """Create a sample RawResponse for testing."""
    body = json.dumps(sample_elexon_response_data).encode()
    return RawResponse(
        body=body,
        content_type="application/json",
        source="elexon",
        dataset="system_prices",
        fetched_at=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc),
        request_url="https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices",
        request_params={"settlementDate": "2024-01-15"},
        api_version="v1",
        page=1,
        total_pages=1,
        http_status=200,
    )


@pytest.fixture
def elexon_source_config() -> SourceConfig:
    """Elexon source config for testing."""
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
