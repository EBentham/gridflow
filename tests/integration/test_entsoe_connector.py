"""Integration tests for the ENTSO-E connector with mocked HTTP."""

from __future__ import annotations

from datetime import UTC, datetime
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
    """_fetch_control_area must send controlArea_Domain, not zone params."""
    xml_body = (FIXTURES / "imbalance_prices_gb.xml").read_bytes()

    route = respx.get(f"{_ENTSOE_BASE}/api").mock(
        return_value=httpx.Response(200, content=xml_body, headers={"content-type": "text/xml"})
    )

    async with EntsoeConnector(entsoe_config) as connector:
        responses = await connector.fetch(
            dataset="imbalance_prices",
            start=datetime(2024, 1, 15, tzinfo=UTC),
            end=datetime(2024, 1, 16, tzinfo=UTC),
        )

    assert len(responses) == 1, "Expected one response per default control area (GB)"
    assert responses[0].source == "entsoe"
    assert responses[0].dataset == "imbalance_prices"
    assert responses[0].http_status == 200

    # The request must use ENTSO-E's canonical control-area query param.
    sent_params = dict(route.calls[0].request.url.params)
    assert "controlArea_Domain" in sent_params
    assert "controlArea_Domain.mRID" not in sent_params
    assert "in_Domain" not in sent_params
    assert "in_Domain.mRID" not in sent_params
    assert "out_Domain" not in sent_params
    assert "out_Domain.mRID" not in sent_params
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
            start=datetime(2024, 1, 15, tzinfo=UTC),
            end=datetime(2024, 1, 16, tzinfo=UTC),
        )

    sent_params = dict(route.calls[0].request.url.params)
    assert "processType" not in sent_params


@respx.mock
@pytest.mark.asyncio
async def test_fetch_imbalance_volume_omits_process_type(
    entsoe_config: SourceConfig,
) -> None:
    """A86 imbalance_volume has process_type=None — processType must NOT appear in the request."""
    xml_body = (FIXTURES / "imbalance_volume_gb.xml").read_bytes()
    route = respx.get(f"{_ENTSOE_BASE}/api").mock(
        return_value=httpx.Response(200, content=xml_body, headers={"content-type": "text/xml"})
    )

    async with EntsoeConnector(entsoe_config) as connector:
        await connector.fetch(
            dataset="imbalance_volume",
            start=datetime(2024, 1, 15, tzinfo=UTC),
            end=datetime(2024, 1, 16, tzinfo=UTC),
        )

    sent_params = dict(route.calls[0].request.url.params)
    assert "processType" not in sent_params
    assert sent_params["documentType"] == "A86"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_actual_load_uses_out_bidding_zone_domain(
    entsoe_config: SourceConfig,
) -> None:
    """Load datasets must use ENTSO-E's documented outBiddingZone_Domain param."""
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
            start=datetime(2024, 1, 15, tzinfo=UTC),
            end=datetime(2024, 1, 16, tzinfo=UTC),
        )

    assert len(responses) > 0
    # Verify first call used outBiddingZone_Domain, not legacy .mRID or in/out params.
    import respx as _respx  # noqa: PLC0415
    for call in _respx.calls:
        params = dict(call.request.url.params)
        assert "controlArea_Domain" not in params
        assert "controlArea_Domain.mRID" not in params
        assert "outBiddingZone_Domain" in params
        assert "in_Domain" not in params
        assert "out_Domain" not in params
        assert "in_Domain.mRID" not in params
        assert "out_Domain.mRID" not in params
        break  # just check one call


@respx.mock
@pytest.mark.asyncio
async def test_acknowledgement_error_includes_reason_and_redacts_token(
    entsoe_config: SourceConfig,
) -> None:
    """ENTSO-E acknowledgement XML should surface reason text without leaking token."""
    acknowledgement = (
        b'<?xml version="1.0" encoding="UTF-8"?>'
        b'<Acknowledgement_MarketDocument '
        b'xmlns="urn:iec62325.351:tc57wg16:451-1:acknowledgementdocument:7:0">'
        b"""
      <Reason>
        <code>999</code>
        <text>Input parameter does not exist: in_Domain.mRID</text>
      </Reason>
    </Acknowledgement_MarketDocument>
    """
    )
    respx.get(f"{_ENTSOE_BASE}/api").mock(
        return_value=httpx.Response(
            400,
            content=acknowledgement,
            headers={"content-type": "text/xml"},
        )
    )

    async with EntsoeConnector(entsoe_config) as connector:
        with pytest.raises(httpx.HTTPStatusError) as exc_info:
            await connector.fetch(
                dataset="actual_load",
                start=datetime(2024, 1, 15, tzinfo=UTC),
                end=datetime(2024, 1, 16, tzinfo=UTC),
            )

    message = str(exc_info.value)
    assert "reason code 999" in message
    assert "Input parameter does not exist: in_Domain.mRID" in message
    assert "test-token" not in message
    assert "securityToken=<redacted>" in message
