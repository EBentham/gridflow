"""Open-Meteo weather API connector."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from gridflow.connectors.base import BaseConnector, RawResponse
from gridflow.connectors.openmeteo.endpoints import (
    ARCHIVE_BASE_URL,
    DATASET_SPECS,
    FORECAST_BASE_URL,
    WeatherLocation,
)
from gridflow.connectors.registry import register_connector
from gridflow.utils.time import window_dates

if TYPE_CHECKING:
    from datetime import datetime

logger = logging.getLogger(__name__)


class OpenMeteoConnector(BaseConnector):
    """Connector for the Open-Meteo weather API.

    Open-Meteo is a public API with no authentication. Historical (archive)
    and forecast data live on two hosts, so every request uses an absolute
    URL, which httpx sends as given whatever the client's ``base_url``.

    Datasets are role-split into six keys
    (``historical_demand``/``historical_wind``/``historical_solar`` and the
    three matching forecast keys); each looks up its locations + variables
    + extra params from ``DATASET_SPECS``. One ``RawResponse`` is produced
    per location per call; its ``dataset`` field is
    ``f"{dataset}__{location.name}"`` (double-underscore separator).
    """

    source_name = "open_meteo"

    def list_datasets(self) -> list[str]:
        return list(self.config.datasets.keys())

    async def fetch(
        self,
        dataset: str,
        start: datetime,
        end: datetime,
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

        # issue-13 (finding 184): a location that fails *after* the retry policy
        # is exhausted must surface, not be silently downgraded to a warning.
        # Open-Meteo locations are capacity-weighted (offshore wind sites, demand
        # population centres); silently dropping one re-weights that run's
        # aggregate with no error raised. ``_request`` already retries transients
        # (429 / archive-host timeout), so only a persistent failure reaches here.
        # Locations are independent units, so they fetch concurrently (CH3-03 /
        # C2-7); one bad location fails the whole run.
        return await self._gather_units(
            [self._fetch_location(dataset, location, start, end) for location in spec.locations]
        )

    async def _fetch_location(
        self,
        dataset: str,
        location: WeatherLocation,
        start: datetime,
        end: datetime,
    ) -> list[RawResponse]:
        """Fetch one location's data as a one-response list."""
        spec = DATASET_SPECS[dataset]
        is_historical = dataset.startswith("historical")
        url = f"{ARCHIVE_BASE_URL}/archive" if is_historical else f"{FORECAST_BASE_URL}/forecast"

        request_params: dict[str, Any] = {
            "latitude": location.latitude,
            "longitude": location.longitude,
            "hourly": ",".join(spec.hourly),
            "start_date": start.strftime("%Y-%m-%d"),
            "end_date": window_dates(start, end)[-1].isoformat(),
            "timezone": "UTC",
        }
        if spec.extra_params:
            request_params.update(dict(spec.extra_params))

        response = await self._get(url, request_params)
        # The sidecar has always carried an empty api_version for this source.
        return [
            RawResponse.from_httpx(
                response,
                source="open_meteo",
                dataset=f"{dataset}__{location.name}",
                params=request_params,
                content_type="application/json",
                api_version="",
                data_date=start.date(),
            )
        ]


register_connector("open_meteo", OpenMeteoConnector)
