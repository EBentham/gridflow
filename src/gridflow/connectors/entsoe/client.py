"""ENTSO-E Transparency Platform API connector."""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any
from xml.etree import ElementTree

import httpx

from gridflow.connectors.base import BaseConnector, RawResponse
from gridflow.connectors.entsoe.endpoints import (
    BIDDING_ZONES,
    DEFAULT_CONTROL_AREAS,
    DEFAULT_ZONES,
    DOC_TYPES,
    ENTSOE_DT_FORMAT,
)
from gridflow.connectors.registry import register_connector
from gridflow.utils.retry import RETRY_POLICY

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from datetime import datetime

    from gridflow.config.settings import SourceConfig

# Datasets that are fetched per zone-pair rather than per zone
_ZONE_PAIR_DATASETS: frozenset[str] = frozenset(
    {"cross_border_flows", "net_transfer_capacity"}
)

# Cross-border zone pairs (in_zone, out_zone)
# Only GB interconnectors and adjacent European pairs are included by default
_FLOW_PAIRS: list[tuple[str, str]] = [
    ("GB", "FR"),
    ("GB", "NL"),
    ("GB", "BE"),
    ("GB", "IE-SEM"),
    ("FR", "BE"),
    ("FR", "DE-LU"),
    ("NL", "DE-LU"),
    ("NL", "BE"),
]

_ENTSOE_API_PATH = "/api"
_ACK_REASON_NS = {
    "ack": "urn:iec62325.351:tc57wg16:451-1:acknowledgementdocument:7:0",
}


class EntsoeConnector(BaseConnector):
    """Connector for the ENTSO-E Transparency Platform API.

    Authentication is via query parameter (``securityToken``) rather than
    an HTTP header, so ``_auth_headers()`` returns an empty dict and the
    token is injected into every request's query params.
    """

    source_name = "entsoe"

    def __init__(self, config: SourceConfig) -> None:
        super().__init__(config)

    # ------------------------------------------------------------------
    # Auth: ENTSO-E uses query-param auth, not headers
    # ------------------------------------------------------------------

    def _auth_headers(self) -> dict[str, str]:
        return {}

    async def __aenter__(self) -> EntsoeConnector:
        import asyncio

        self._semaphore = asyncio.Semaphore(self.config.rate_limit_per_second)
        self._client = httpx.AsyncClient(
            base_url=self.config.base_url,
            timeout=self.config.timeout,
            # No auth headers — token goes in query params
        )
        return self

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def list_datasets(self) -> list[str]:
        return list(DOC_TYPES.keys())

    async def fetch(
        self,
        dataset: str,
        start: datetime,
        end: datetime,
        **params: Any,
    ) -> list[RawResponse]:
        """Fetch XML data for a date range from ENTSO-E.

        Dispatches per-zone (or per-zone-pair for cross-border flows).
        """
        if dataset not in DOC_TYPES:
            raise ValueError(
                f"Unknown ENTSO-E dataset: {dataset!r}. "
                f"Available: {list(DOC_TYPES.keys())}"
            )

        doc_type = DOC_TYPES[dataset]
        period_start = start.strftime(ENTSOE_DT_FORMAT)
        period_end = end.strftime(ENTSOE_DT_FORMAT)

        responses: list[RawResponse] = []

        if dataset in _ZONE_PAIR_DATASETS:
            # Fetch one response per (in, out) zone pair
            for in_zone, out_zone in _FLOW_PAIRS:
                in_mrid = BIDDING_ZONES.get(in_zone)
                out_mrid = BIDDING_ZONES.get(out_zone)
                if not in_mrid or not out_mrid:
                    continue
                resp = await self._fetch_zone(
                    dataset=dataset,
                    doc_type_code=doc_type.document_type,
                    process_type=doc_type.process_type,
                    in_domain=in_mrid,
                    out_domain=out_mrid,
                    period_start=period_start,
                    period_end=period_end,
                )
                responses.append(resp)
        elif doc_type.domain_style == "control_area":
            # Fetch one response per control area (balancing datasets)
            for area in DEFAULT_CONTROL_AREAS:
                mrid = BIDDING_ZONES.get(area)
                if not mrid:
                    continue
                resp = await self._fetch_control_area(
                    dataset=dataset,
                    doc_type_code=doc_type.document_type,
                    process_type=doc_type.process_type,
                    control_area_domain=mrid,
                    period_start=period_start,
                    period_end=period_end,
                )
                responses.append(resp)
        else:
            # Fetch one response per bidding zone
            for zone in DEFAULT_ZONES:
                mrid = BIDDING_ZONES.get(zone)
                if not mrid:
                    continue
                resp = await self._fetch_zone(
                    dataset=dataset,
                    doc_type_code=doc_type.document_type,
                    process_type=doc_type.process_type,
                    in_domain=mrid,
                    out_domain=mrid,
                    period_start=period_start,
                    period_end=period_end,
                )
                responses.append(resp)

        logger.info(
            "Fetched %d responses for entsoe/%s from %s to %s",
            len(responses),
            dataset,
            start,
            end,
        )
        return responses

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _fetch_zone(
        self,
        *,
        dataset: str,
        doc_type_code: str,
        process_type: str | None,
        in_domain: str,
        out_domain: str,
        period_start: str,
        period_end: str,
    ) -> RawResponse:
        """Fetch a single zone (or zone pair) response from the ENTSO-E API."""
        query_params: dict[str, str] = {
            "documentType": doc_type_code,
            "in_Domain": in_domain,
            "out_Domain": out_domain,
            "periodStart": period_start,
            "periodEnd": period_end,
        }
        if process_type:
            query_params["processType"] = process_type
        if self.config.api_key:
            query_params["securityToken"] = self.config.api_key

        raw = await self._request(_ENTSOE_API_PATH, query_params)
        return RawResponse(
            body=raw.content,
            content_type=raw.headers.get("content-type", "text/xml"),
            source="entsoe",
            dataset=dataset,
            request_url=str(raw.url),
            request_params=dict(query_params),
            api_version="v1",
            http_status=raw.status_code,
        )

    async def _fetch_control_area(
        self,
        *,
        dataset: str,
        doc_type_code: str,
        process_type: str | None,
        control_area_domain: str,
        period_start: str,
        period_end: str,
    ) -> RawResponse:
        """Fetch a single control-area response from the ENTSO-E API.

        Used for balancing datasets (A83, A84, A85, A86, A81) that require
        ``controlArea_Domain`` instead of ``in_Domain`` / ``out_Domain``.
        """
        query_params: dict[str, str] = {
            "documentType": doc_type_code,
            "controlArea_Domain": control_area_domain,
            "periodStart": period_start,
            "periodEnd": period_end,
        }
        if process_type:
            query_params["processType"] = process_type
        if self.config.api_key:
            query_params["securityToken"] = self.config.api_key

        raw = await self._request(_ENTSOE_API_PATH, query_params)
        return RawResponse(
            body=raw.content,
            content_type=raw.headers.get("content-type", "text/xml"),
            source="entsoe",
            dataset=dataset,
            request_url=str(raw.url),
            request_params=dict(query_params),
            api_version="v1",
            http_status=raw.status_code,
        )

    @RETRY_POLICY
    async def _request(
        self, path: str, params: dict[str, Any]
    ) -> httpx.Response:
        """Make a rate-limited, retried HTTP GET request."""
        if self._client is None:
            raise RuntimeError(
                "Connector not initialized. Use 'async with' context manager."
            )
        if self._semaphore is None:
            raise RuntimeError(
                "Semaphore not initialized. Use 'async with' context manager."
            )

        async with self._semaphore:
            resp = await self._client.get(path, params=params)
            self._raise_for_status(resp)
            return resp

    def _raise_for_status(self, response: httpx.Response) -> None:
        """Raise an ENTSO-E-aware HTTP error with acknowledgement details."""
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            reason = _extract_acknowledgement_reason(response.content)
            url = _redact_security_token(str(response.url))
            message = f"ENTSO-E API error {response.status_code} for url '{url}'"
            if reason:
                message = f"{message}: {reason}"
            raise httpx.HTTPStatusError(
                message,
                request=exc.request,
                response=exc.response,
            ) from exc


def _extract_acknowledgement_reason(content: bytes) -> str:
    """Extract code/text from an ENTSO-E Acknowledgement_MarketDocument."""
    try:
        root = ElementTree.fromstring(content)
    except ElementTree.ParseError:
        return ""

    reason = root.find(".//ack:Reason", _ACK_REASON_NS)
    if reason is None:
        reason = root.find(".//Reason")
    if reason is None:
        return ""

    code = reason.find("ack:code", _ACK_REASON_NS)
    if code is None:
        code = reason.find("code")
    text = reason.find("ack:text", _ACK_REASON_NS)
    if text is None:
        text = reason.find("text")

    parts = []
    if code is not None and code.text:
        parts.append(f"reason code {code.text.strip()}")
    if text is not None and text.text:
        parts.append(text.text.strip())
    return " - ".join(parts)


def _redact_security_token(url: str) -> str:
    """Redact ENTSO-E query-token values from URLs."""
    return re.sub(r"(securityToken=)[^&]+", r"\1<redacted>", url)


# Register connector
register_connector("entsoe", EntsoeConnector)
