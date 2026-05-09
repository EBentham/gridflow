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
    DATASET_SPECS,
    FORECAST_BASE_URL,
    WeatherLocation,
)
from gridflow.connectors.registry import register_connector

logger = logging.getLogger(__name__)


class OpenMeteoConnector(BaseConnector):
    """Connector for the Open-Meteo weather API.

    Open-Meteo is a public API with no authentication. It uses two different
    base URLs for historical (archive) vs forecast data, so we override
    ``__aenter__`` to create a client without a fixed base_url.

    Datasets are role-split into six keys
    (``historical_demand``/``historical_wind``/``historical_solar`` and the
    three matching forecast keys); each looks up its locations + variables
    + extra params from ``DATASET_SPECS``. One ``RawResponse`` is produced
    per location per call; its ``dataset`` field is
    ``f"{dataset}__{location.name}"`` (double-underscore separator).
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
        """Fetch weather data for all locations of the dataset's role.

        ``dataset`` is one of the six role keys in ``DATASET_SPECS``.
        """
        if dataset not in DATASET_SPECS:
            raise ValueError(
                f"Unknown open_meteo dataset {dataset!r}; expected one of "
                f"{sorted(DATASET_SPECS.keys())}"
            )
        spec = DATASET_SPECS[dataset]
        responses: list[RawResponse] = []
        for location in spec.locations:
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
        spec = DATASET_SPECS[dataset]
        is_historical = dataset.startswith("historical")
        if is_historical:
            url = f"{ARCHIVE_BASE_URL}/archive"
        else:
            url = f"{FORECAST_BASE_URL}/forecast"

        request_params: dict[str, Any] = {
            "latitude": location.latitude,
            "longitude": location.longitude,
            "hourly": ",".join(spec.hourly),
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": end.strftime("%Y-%m-%d"),
            "timezone": "UTC",
        }
        if spec.extra_params:
            request_params.update(dict(spec.extra_params))

        assert self._client is not None
        async with self._semaphore:
            response = await self._client.get(url, params=request_params)
            response.raise_for_status()

        location_dataset = f"{dataset}__{location.name}"
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
