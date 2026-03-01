"""NESO / Carbon Intensity API connector."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import httpx

from gridflow.connectors.base import BaseConnector, RawResponse
from gridflow.connectors.registry import register_connector
from gridflow.utils.retry import RETRY_POLICY

logger = logging.getLogger(__name__)

# Maximum date range the API accepts in a single request (14 days)
_MAX_DAYS_PER_REQUEST = 14


class CarbonIntensityConnector(BaseConnector):
    """Connector for the NESO/National Grid Carbon Intensity API.

    Public API — no authentication required.
    Uses path-based date range: ``/intensity/{from}/{to}``.
    Returns half-hourly carbon intensity forecasts and actuals.
    """

    source_name = "neso"

    def list_datasets(self) -> list[str]:
        return list(self.config.datasets.keys())

    async def fetch(
        self,
        dataset: str,
        start: datetime,
        end: datetime,
        **params: Any,
    ) -> list[RawResponse]:
        """Fetch carbon intensity data for a date range.

        The API returns half-hourly records. Requests are chunked to
        ``_MAX_DAYS_PER_REQUEST`` days each.
        """
        from datetime import timedelta

        responses: list[RawResponse] = []
        chunk_start = start
        chunk_delta = timedelta(days=_MAX_DAYS_PER_REQUEST)

        while chunk_start < end:
            chunk_end = min(chunk_start + chunk_delta, end)
            path = (
                f"/intensity"
                f"/{chunk_start.strftime('%Y-%m-%dT%H:%MZ')}"
                f"/{chunk_end.strftime('%Y-%m-%dT%H:%MZ')}"
            )
            try:
                raw = await self._request(path, {})
                responses.append(
                    RawResponse(
                        body=raw.content,
                        content_type=raw.headers.get(
                            "content-type", "application/json"
                        ),
                        source="neso",
                        dataset=dataset,
                        request_url=str(raw.url),
                        api_version="v1",
                        http_status=raw.status_code,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to fetch carbon intensity %s to %s: %s",
                    chunk_start.date(),
                    chunk_end.date(),
                    exc,
                )

            chunk_start = chunk_end

        logger.info(
            "Fetched %d responses for neso/%s from %s to %s",
            len(responses),
            dataset,
            start.date(),
            end.date(),
        )
        return responses

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


register_connector("neso", CarbonIntensityConnector)
