"""Open-Meteo weather API connector."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

import httpx

from gridflow.connectors.base import BaseConnector, RawResponse
from gridflow.connectors.openmeteo.endpoints import (
    ARCHIVE_BASE_URL,
    FORECAST_BASE_URL,
    HOURLY_VARIABLES,
    LOCATIONS,
    WeatherLocation,
)
from gridflow.connectors.registry import register_connector

logger = logging.getLogger(__name__)


class OpenMeteoConnector(BaseConnector):
    """Connector for the Open-Meteo weather API.

    Open-Meteo is a public API with no authentication. It uses two different
    base URLs for historical (archive) vs forecast data, so we override
    ``__aenter__`` to create a client without a fixed base_url.

    One ``RawResponse`` is produced per location per call.
    The ``dataset`` field on each response is ``f"{prefix}_{location.name}"``
    (e.g. ``"historical_london"``).
    """

    source_name = "open_meteo"

    async def __aenter__(self) -> OpenMeteoConnector:
        """Create an httpx client without a fixed base_url (uses absolute URLs)."""
        self._semaphore = asyncio.Semaphore(self.config.rate_limit_per_second)
        self._client = httpx.AsyncClient(timeout=self.config.timeout)
        return self

    def list_datasets(self) -> list[str]:
        return list(self.config.datasets.keys())

    async def fetch(
        self,
        dataset: str,
        start: datetime,
        end: datetime,
        **params: Any,
    ) -> list[RawResponse]:
        """Fetch weather data for all locations for the given date range.

        ``dataset`` is ``"historical"`` or ``"forecast"``.
        """
        responses: list[RawResponse] = []
        for location in LOCATIONS:
            try:
                resp = await self._fetch_location(dataset, location, start, end)
                responses.append(resp)
            except Exception as exc:
                logger.warning(
                    "Failed to fetch open_meteo/%s for %s: %s",
                    dataset,
                    location.name,
                    exc,
                )
        return responses

    async def _fetch_location(
        self,
        dataset: str,
        location: WeatherLocation,
        start: datetime,
        end: datetime,
    ) -> RawResponse:
        """Fetch one location's data and return a RawResponse."""
        if dataset == "historical":
            url = f"{ARCHIVE_BASE_URL}/archive"
        else:
            url = f"{FORECAST_BASE_URL}/forecast"

        request_params: dict[str, Any] = {
            "latitude": location.latitude,
            "longitude": location.longitude,
            "hourly": ",".join(HOURLY_VARIABLES),
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": end.strftime("%Y-%m-%d"),
            "timezone": "UTC",
        }

        assert self._client is not None
        async with self._semaphore:
            response = await self._client.get(url, params=request_params)
            response.raise_for_status()

        location_dataset = f"{dataset}_{location.name}"
        return RawResponse(
            body=response.content,
            content_type="application/json",
            source="open_meteo",
            dataset=location_dataset,
            fetched_at=datetime.now(UTC),
            request_url=str(response.url),
            request_params=request_params,
        )


register_connector("open_meteo", OpenMeteoConnector)
