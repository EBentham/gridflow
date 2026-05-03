"""ENTSO-E Transparency Platform API connector."""

from __future__ import annotations

import logging
import re
from datetime import datetime
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
    EntsoeDocType,
)
from gridflow.connectors.registry import register_connector
from gridflow.utils.retry import RETRY_POLICY

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from gridflow.config.settings import SourceConfig

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

        if doc_type.domain_style == "zone_pair":
            # Fetch one response per (in, out) zone pair
            for in_zone, out_zone in _FLOW_PAIRS:
                in_mrid = BIDDING_ZONES.get(in_zone)
                out_mrid = BIDDING_ZONES.get(out_zone)
                if not in_mrid or not out_mrid:
                    continue
                resp = await self._fetch_document(
                    dataset=dataset,
                    doc_type=doc_type,
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
                resp = await self._fetch_document(
                    dataset=dataset,
                    doc_type=doc_type,
                    in_domain=mrid,
                    out_domain=None,
                    period_start=period_start,
                    period_end=period_end,
                )
                responses.append(resp)
        else:
            # Fetch one response per bidding zone using the dataset's documented
            # domain parameter style.
            for zone in DEFAULT_ZONES:
                mrid = BIDDING_ZONES.get(zone)
                if not mrid:
                    continue
                resp = await self._fetch_document(
                    dataset=dataset,
                    doc_type=doc_type,
                    in_domain=mrid,
                    out_domain=mrid if doc_type.domain_style == "zone" else None,
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

    async def _fetch_document(
        self,
        *,
        dataset: str,
        doc_type: EntsoeDocType,
        in_domain: str,
        out_domain: str | None,
        period_start: str,
        period_end: str,
    ) -> RawResponse:
        """Fetch a single ENTSO-E document using the dataset's request style."""
        query_params: dict[str, str] = {"documentType": doc_type.document_type}
        if doc_type.date_param:
            data_date = datetime.strptime(period_start, ENTSOE_DT_FORMAT).date()
            query_params[doc_type.date_param] = data_date.isoformat()
        else:
            query_params.update({
                "periodStart": period_start,
                "periodEnd": period_end,
            })
        query_params.update(
            _domain_params(
                doc_type.domain_style,
                in_domain,
                out_domain,
                doc_type.domain_params,
            )
        )
        query_params.update(doc_type.extra_params)
        if doc_type.process_type:
            query_params["processType"] = doc_type.process_type
        if self.config.api_key:
            query_params["securityToken"] = self.config.api_key

        raw = await self._request(_ENTSOE_API_PATH, query_params)
        data_date = datetime.strptime(period_start, ENTSOE_DT_FORMAT).date()
        return RawResponse(
            body=raw.content,
            content_type=raw.headers.get("content-type", "text/xml"),
            source="entsoe",
            dataset=dataset,
            request_url=str(raw.url),
            request_params=dict(query_params),
            api_version="v1",
            http_status=raw.status_code,
            data_date=data_date,
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


def _domain_params(
    domain_style: str,
    in_domain: str,
    out_domain: str | None,
    domain_params: tuple[str, ...] = (),
) -> dict[str, str]:
    """Build ENTSO-E's endpoint-specific area query parameters."""
    if domain_params:
        if len(domain_params) == 1:
            return {domain_params[0]: in_domain}
        if len(domain_params) == 2:
            if out_domain is None:
                raise ValueError(
                    f"{domain_params!r} domain params require out_domain"
                )
            return {domain_params[0]: in_domain, domain_params[1]: out_domain}
        raise ValueError(f"Unsupported ENTSO-E domain params: {domain_params!r}")
    if domain_style == "zone":
        if out_domain is None:
            raise ValueError("zone domain style requires out_domain")
        return {"in_Domain": in_domain, "out_Domain": out_domain}
    if domain_style == "zone_pair":
        if out_domain is None:
            raise ValueError("zone_pair domain style requires out_domain")
        return {"in_Domain": in_domain, "out_Domain": out_domain}
    if domain_style == "in_domain":
        return {"in_Domain": in_domain}
    if domain_style == "out_bidding_zone":
        return {"outBiddingZone_Domain": in_domain}
    if domain_style == "bidding_zone":
        return {"BiddingZone_Domain": in_domain}
    if domain_style == "control_area":
        return {"controlArea_Domain": in_domain}
    raise ValueError(f"Unsupported ENTSO-E domain style: {domain_style!r}")


def _redact_security_token(url: str) -> str:
    """Redact ENTSO-E query-token values from URLs."""
    return re.sub(r"(securityToken=)[^&]+", r"\1<redacted>", url)


# Register connector
register_connector("entsoe", EntsoeConnector)
