"""ENTSO-G Transparency Platform API connector."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import httpx

from gridflow.connectors.base import BaseConnector, RawResponse
from gridflow.connectors.entsog.endpoints import (
    DEFAULT_PERIOD_TYPE,
    ENTSOG_ALL_RECORDS_LIMIT,
    ENTSOG_API_PATH,
    ENTSOG_TIMEZONE,
    PHYSICAL_FLOW_INDICATOR,
)
from gridflow.connectors.registry import register_connector
from gridflow.utils.retry import RETRY_POLICY

logger = logging.getLogger(__name__)


class EntsogConnector(BaseConnector):
    """Connector for the ENTSO-G Transparency Platform API.

    ENTSO-G is a fully public API (no authentication required).
    Physical flow data is fetched as daily aggregates using ``limit=-1``
    to retrieve all interconnection points in a single request.
    """

    source_name = "entsog"

    def list_datasets(self) -> list[str]:
        return list(self.config.datasets.keys())

    async def fetch(
        self,
        dataset: str,
        start: datetime,
        end: datetime,
        **params: Any,
    ) -> list[RawResponse]:
        """Fetch ENTSO-G physical flow data for a date range.

        Returns one ``RawResponse`` per request (ENTSO-G returns all records
        in a single paginated response when ``limit=-1``).
        """
        query_params: dict[str, Any] = {
            "from": start.strftime("%Y-%m-%d"),
            "to": end.strftime("%Y-%m-%d"),
            "indicator": PHYSICAL_FLOW_INDICATOR,
            "periodType": DEFAULT_PERIOD_TYPE,
            "timezone": ENTSOG_TIMEZONE,
            "limit": ENTSOG_ALL_RECORDS_LIMIT,
        }

        raw = await self._request(ENTSOG_API_PATH, query_params)

        response = RawResponse(
            body=raw.content,
            content_type=raw.headers.get("content-type", "application/json"),
            source="entsog",
            dataset=dataset,
            request_url=str(raw.url),
            request_params=dict(query_params),
            api_version="v1",
            http_status=raw.status_code,
        )

        logger.info(
            "Fetched ENTSO-G %s from %s to %s (%d bytes)",
            dataset,
            start.date(),
            end.date(),
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
