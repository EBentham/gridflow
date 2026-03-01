"""Elexon BMRS / Insights API connector."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

import httpx

from gridflow.config.settings import SourceConfig
from gridflow.connectors.base import BaseConnector, RawResponse
from gridflow.connectors.elexon.endpoints import ENDPOINTS, ElexonEndpoint, ParamStyle, build_params
from gridflow.connectors.elexon.parsers import get_pagination_info
from gridflow.connectors.registry import register_connector
from gridflow.utils.retry import RETRY_POLICY

logger = logging.getLogger(__name__)


class ElexonConnector(BaseConnector):
    """Connector for the Elexon Insights API."""

    source_name = "elexon"

    def __init__(self, config: SourceConfig):
        super().__init__(config)

    def list_datasets(self) -> list[str]:
        return list(ENDPOINTS.keys())

    async def fetch(
        self,
        dataset: str,
        start: datetime,
        end: datetime,
        **params: Any,
    ) -> list[RawResponse]:
        """Fetch raw data for a date range from Elexon."""
        if dataset not in ENDPOINTS:
            raise ValueError(
                f"Unknown Elexon dataset: {dataset}. Available: {list(ENDPOINTS.keys())}"
            )

        endpoint = ENDPOINTS[dataset]
        responses: list[RawResponse] = []

        if endpoint.param_style == ParamStyle.NO_PARAMS:
            responses.extend(await self._fetch_single(dataset, endpoint))

        elif endpoint.param_style == ParamStyle.SETTLEMENT_DATE:
            current_date = start.date() if isinstance(start, datetime) else start
            end_date = end.date() if isinstance(end, datetime) else end
            while current_date <= end_date:
                date_responses = await self._fetch_date(dataset, endpoint, current_date)
                responses.extend(date_responses)
                current_date += timedelta(days=1)

        elif endpoint.param_style == ParamStyle.PUBLISH_DATETIME:
            current = start
            while current < end:
                chunk_end = min(current + timedelta(days=1), end)
                chunk_responses = await self._fetch_datetime_range(
                    dataset, endpoint, current, chunk_end
                )
                responses.extend(chunk_responses)
                current = chunk_end

        logger.info(
            f"Fetched {len(responses)} responses for elexon/{dataset} "
            f"from {start} to {end}"
        )
        return responses

    async def _fetch_date(
        self,
        dataset: str,
        endpoint: ElexonEndpoint,
        settlement_date: Any,
    ) -> list[RawResponse]:
        """Fetch all pages for a single settlement date."""
        responses: list[RawResponse] = []
        page = 1

        while True:
            query_params = build_params(endpoint, settlement_date=settlement_date, page=page)
            raw = await self._request(endpoint.path, query_params)

            current_page, total_pages = get_pagination_info(raw.content)
            responses.append(
                RawResponse(
                    body=raw.content,
                    content_type=raw.headers.get("content-type", "application/json"),
                    source="elexon",
                    dataset=dataset,
                    request_url=str(raw.url),
                    request_params=dict(query_params),
                    api_version="v1",
                    page=page,
                    total_pages=total_pages,
                    http_status=raw.status_code,
                )
            )

            if page >= total_pages:
                break
            page += 1

        return responses

    async def _fetch_single(
        self, dataset: str, endpoint: ElexonEndpoint
    ) -> list[RawResponse]:
        """Fetch a single-request endpoint (e.g., reference data)."""
        query_params = build_params(endpoint)
        raw = await self._request(endpoint.path, query_params)
        return [
            RawResponse(
                body=raw.content,
                content_type=raw.headers.get("content-type", "application/json"),
                source="elexon",
                dataset=dataset,
                request_url=str(raw.url),
                request_params=dict(query_params),
                api_version="v1",
                http_status=raw.status_code,
            )
        ]

    async def _fetch_datetime_range(
        self,
        dataset: str,
        endpoint: ElexonEndpoint,
        start: datetime,
        end: datetime,
    ) -> list[RawResponse]:
        """Fetch paginated data using publishDateTimeFrom/To."""
        responses: list[RawResponse] = []
        page = 1

        while True:
            query_params = build_params(endpoint, start=start, end=end, page=page)
            raw = await self._request(endpoint.path, query_params)

            current_page, total_pages = get_pagination_info(raw.content)
            responses.append(
                RawResponse(
                    body=raw.content,
                    content_type=raw.headers.get("content-type", "application/json"),
                    source="elexon",
                    dataset=dataset,
                    request_url=str(raw.url),
                    request_params=dict(query_params),
                    api_version="v1",
                    page=page,
                    total_pages=total_pages,
                    http_status=raw.status_code,
                )
            )

            if page >= total_pages:
                break
            page += 1

        return responses

    @RETRY_POLICY
    async def _request(
        self, path: str, params: dict[str, Any]
    ) -> httpx.Response:
        """Make a rate-limited, retried HTTP request."""
        if self._client is None:
            raise RuntimeError("Connector not initialized. Use 'async with' context manager.")
        if self._semaphore is None:
            raise RuntimeError("Semaphore not initialized. Use 'async with' context manager.")

        async with self._semaphore:
            resp = await self._client.get(path, params=params)
            resp.raise_for_status()
            return resp


# Register this connector
register_connector("elexon", ElexonConnector)
