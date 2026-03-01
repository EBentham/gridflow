"""GIE AGSI+ and ALSI API connector."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

import httpx

from gridflow.connectors.base import BaseConnector, RawResponse
from gridflow.connectors.gie.endpoints import AGSI_COUNTRIES, ALSI_COUNTRIES, DEFAULT_PAGE_SIZE, GIE_API_PATH
from gridflow.connectors.registry import register_connector
from gridflow.utils.retry import RETRY_POLICY

logger = logging.getLogger(__name__)

# Map source name -> country list
_COUNTRY_MAP: dict[str, list[str]] = {
    "gie_agsi": AGSI_COUNTRIES,
    "gie_alsi": ALSI_COUNTRIES,
}


class GieConnector(BaseConnector):
    """Connector for the GIE AGSI+ (gas storage) and ALSI (LNG) APIs.

    Both APIs share the same structure: authenticate via ``x-key`` header,
    query ``/api`` with country + date range params, return paginated JSON.
    """

    # source_name is set dynamically at class-registration time
    source_name = "gie_agsi"

    def list_datasets(self) -> list[str]:
        return list(self.config.datasets.keys())

    async def fetch(
        self,
        dataset: str,
        start: datetime,
        end: datetime,
        **params: Any,
    ) -> list[RawResponse]:
        """Fetch GIE data per country for the given date range."""
        countries = _COUNTRY_MAP.get(self.source_name, AGSI_COUNTRIES)
        responses: list[RawResponse] = []

        for country in countries:
            try:
                country_responses = await self._fetch_country(
                    dataset=dataset,
                    country=country,
                    start=start,
                    end=end,
                )
                responses.extend(country_responses)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to fetch %s/%s for country %s: %s",
                    self.source_name,
                    dataset,
                    country,
                    exc,
                )

        logger.info(
            "Fetched %d responses for %s/%s from %s to %s",
            len(responses),
            self.source_name,
            dataset,
            start.date(),
            end.date(),
        )
        return responses

    async def _fetch_country(
        self,
        *,
        dataset: str,
        country: str,
        start: datetime,
        end: datetime,
    ) -> list[RawResponse]:
        """Fetch all pages for one country."""
        responses: list[RawResponse] = []
        page = 1

        while True:
            query_params: dict[str, Any] = {
                "country": country,
                "from": start.strftime("%Y-%m-%d"),
                "till": end.strftime("%Y-%m-%d"),
                "page": page,
                "size": DEFAULT_PAGE_SIZE,
            }
            raw = await self._request(GIE_API_PATH, query_params)
            body = raw.content

            responses.append(
                RawResponse(
                    body=body,
                    content_type=raw.headers.get("content-type", "application/json"),
                    source=self.source_name,
                    dataset=dataset,
                    request_url=str(raw.url),
                    request_params=dict(query_params),
                    api_version="v1",
                    page=page,
                    http_status=raw.status_code,
                )
            )

            # Determine if there are more pages from JSON response
            try:
                import json
                data = json.loads(body)
                total = int(data.get("total", 0))
                page_size = int(data.get("pageSize", DEFAULT_PAGE_SIZE))
                if page * page_size >= total:
                    break
            except Exception:  # noqa: BLE001
                break

            page += 1

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


class AgsiConnector(GieConnector):
    """AGSI+ gas storage connector (source_name='gie_agsi')."""

    source_name = "gie_agsi"


class AlsiConnector(GieConnector):
    """ALSI LNG terminal connector (source_name='gie_alsi')."""

    source_name = "gie_alsi"


register_connector("gie_agsi", AgsiConnector)
register_connector("gie_alsi", AlsiConnector)
