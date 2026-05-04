"""ENTSO-G Transparency Platform API connector."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from datetime import datetime

    import httpx

from gridflow.connectors.base import BaseConnector, RawResponse
from gridflow.connectors.entsog.endpoints import (
    ENDPOINTS,
    build_params,
)
from gridflow.connectors.registry import register_connector
from gridflow.utils.retry import RETRY_POLICY

logger = logging.getLogger(__name__)


class EntsogConnector(BaseConnector):
    """Connector for the ENTSO-G Transparency Platform API.

    ENTSO-G is a fully public API (no authentication required). Dataset-specific
    endpoint metadata controls paths, required date filters, operational
    indicators, and default point-direction filters.
    """

    source_name = "entsog"

    def list_datasets(self) -> list[str]:
        return list(ENDPOINTS)

    async def fetch(
        self,
        dataset: str,
        start: datetime,
        end: datetime,
        **params: Any,
    ) -> list[RawResponse]:
        """Fetch ENTSO-G JSON data for a date range or reference endpoint."""
        if dataset not in ENDPOINTS:
            raise ValueError(
                f"Unknown ENTSO-G dataset: {dataset!r}. "
                f"Available: {list(ENDPOINTS)}"
            )

        endpoint = ENDPOINTS[dataset]
        query_params = build_params(endpoint, start=start, end=end, **params)
        raw = await self._request(endpoint.path, query_params)

        response = RawResponse(
            body=raw.content,
            content_type=raw.headers.get("content-type", "application/json"),
            source="entsog",
            dataset=dataset,
            request_url=str(raw.url),
            request_params=dict(query_params),
            api_version="v1",
            http_status=raw.status_code,
            data_date=start.date() if endpoint.requires_dates else None,
        )

        logger.info(
            "Fetched ENTSO-G %s from %s to %s via %s (%d bytes)",
            dataset,
            start.date(),
            end.date(),
            endpoint.path,
            len(raw.content),
        )
        return [response]

    @RETRY_POLICY
    async def _request(
        self, path: str, params: dict[str, Any]
    ) -> httpx.Response:
        """Rate-limited, retried HTTP GET request."""
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
            resp.raise_for_status()
            return resp


register_connector("entsog", EntsogConnector)
