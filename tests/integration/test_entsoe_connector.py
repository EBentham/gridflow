"""Integration tests for the ENTSO-E connector with mocked HTTP."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest
import respx

from gridflow.config.settings import DatasetConfig, SourceConfig
from gridflow.connectors.entsoe.client import EntsoeConnector

FIXTURES = Path(__file__).parent.parent / "fixtures" / "entsoe"

_ENTSOE_BASE = "https://web-api.tp.entsoe.eu"


@pytest.fixture
def entsoe_config() -> SourceConfig:
    return SourceConfig(
        base_url=_ENTSOE_BASE,
        api_key="test-token",
        api_key_header="",          # ENTSO-E uses query-param auth, not a header
        rate_limit_per_second=10,
        timeout=5,
        datasets={
            "imbalance_prices": DatasetConfig(document_type="A85"),
            "imbalance_volume": DatasetConfig(document_type="A86", process_type="A16"),
        },
    )


# ---------------------------------------------------------------------------
# _fetch_control_area
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_fetch_control_area_uses_correct_query_param(
    entsoe_config: SourceConfig,
) -> None:
    """_fetch_control_area must send controlArea_Domain.mRID, not in_Domain.mRID."""
    xml_body = (FIXTURES / "imbalance_prices_gb.xml").read_bytes()

    route = respx.get(f"{_ENTSOE_BASE}/api").mock(
        return_value=httpx.Response(200, content=xml_body, headers={"content-type": "text/xml"})
    )

    async with EntsoeConnector(entsoe_config) as connector:
        responses = await connector.fetch(
            dataset="imbalance_prices",
            start=datetime(2024, 1, 15, tzinfo=timezone.utc),
            end=datetime(2024, 1, 16, tzinfo=timezone.utc),
        )

    assert len(responses) == 1, "Expected one response per default control area (GB)"
    assert responses[0].source == "entsoe"
    assert responses[0].dataset == "imbalance_prices"
    assert responses[0].http_status == 200

    # The request must use controlArea_Domain.mRID, not in_Domain.mRID
    sent_params = dict(route.calls[0].request.url.params)
    assert "controlArea_Domain.mRID" in sent_params, (
        "_fetch_control_area must send controlArea_Domain.mRID"
    )
    assert "in_Domain.mRID" not in sent_params, (
        "_fetch_control_area must NOT send in_Domain.mRID"
    )
    assert sent_params["documentType"] == "A85"
    assert "securityToken" in sent_params


@respx.mock
@pytest.mark.asyncio
async def test_fetch_control_area_omits_process_type_when_none(
    entsoe_config: SourceConfig,
) -> None:
    """A85 has process_type=None — processType param must not be in the request."""
    xml_body = (FIXTURES / "imbalance_prices_gb.xml").read_bytes()
    route = respx.get(f"{_ENTSOE_BASE}/api").mock(
        return_value=httpx.Response(200, content=xml_body, headers={"content-type": "text/xml"})
    )

    async with EntsoeConnector(entsoe_config) as connector:
        await connector.fetch(
            dataset="imbalance_prices",
            start=datetime(2024, 1, 15, tzinfo=timezone.utc),
            end=datetime(2024, 1, 16, tzinfo=timezone.utc),
        )

    sent_params = dict(route.calls[0].request.url.params)
    assert "processType" not in sent_params


@respx.mock
@pytest.mark.asyncio
async def test_fetch_control_area_includes_process_type_when_set(
    entsoe_config: SourceConfig,
) -> None:
    """A86 has process_type='A16' — processType must appear in the request."""
    xml_body = (FIXTURES / "imbalance_volume_gb.xml").read_bytes()
    route = respx.get(f"{_ENTSOE_BASE}/api").mock(
        return_value=httpx.Response(200, content=xml_body, headers={"content-type": "text/xml"})
    )

    async with EntsoeConnector(entsoe_config) as connector:
        await connector.fetch(
            dataset="imbalance_volume",
            start=datetime(2024, 1, 15, tzinfo=timezone.utc),
            end=datetime(2024, 1, 16, tzinfo=timezone.utc),
        )

    sent_params = dict(route.calls[0].request.url.params)
    assert sent_params.get("processType") == "A16"
    assert sent_params["documentType"] == "A86"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_zone_style_uses_in_domain(entsoe_config: SourceConfig) -> None:
    """Zone-style datasets must NOT use controlArea_Domain.mRID."""
    # Extend config with a zone-style dataset
    config = entsoe_config.model_copy(
        update={"datasets": {**entsoe_config.datasets, "actual_load": DatasetConfig()}}
    )
    xml_body = (FIXTURES / "actual_load_gb.xml").read_bytes()
    respx.get(f"{_ENTSOE_BASE}/api").mock(
        return_value=httpx.Response(200, content=xml_body, headers={"content-type": "text/xml"})
    )

    async with EntsoeConnector(config) as connector:
        responses = await connector.fetch(
            dataset="actual_load",
            start=datetime(2024, 1, 15, tzinfo=timezone.utc),
            end=datetime(2024, 1, 16, tzinfo=timezone.utc),
        )

    assert len(responses) > 0
    # Verify first call used in_Domain.mRID, not controlArea_Domain.mRID
    import respx as _respx  # noqa: PLC0415
    for call in _respx.calls:
        params = dict(call.request.url.params)
        assert "controlArea_Domain.mRID" not in params
        assert "in_Domain.mRID" in params
        break  # just check one call
