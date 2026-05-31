"""Elexon BMRS / Insights API connector."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx

from gridflow.connectors.base import BaseConnector, RawResponse
from gridflow.connectors.elexon.endpoints import ENDPOINTS, ParamStyle, build_params
from gridflow.connectors.elexon.parsers import get_pagination_info
from gridflow.connectors.registry import register_connector
from gridflow.utils.retry import RETRY_POLICY

if TYPE_CHECKING:
    from gridflow.config.settings import SourceConfig
    from gridflow.connectors.elexon.endpoints import ElexonEndpoint

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

        elif endpoint.param_style == ParamStyle.SETTLEMENT_DATE_PERIOD:
            current_date = start.date() if isinstance(start, datetime) else start
            end_date = end.date() if isinstance(end, datetime) else end
            while current_date <= end_date:
                date_responses = await self._fetch_date_period(
                    dataset, endpoint, current_date
                )
                responses.extend(date_responses)
                current_date += timedelta(days=1)

        elif endpoint.param_style == ParamStyle.DATE_PATH:
            current_date = start.date() if isinstance(start, datetime) else start
            end_date = end.date() if isinstance(end, datetime) else end
            while current_date <= end_date:
                date_responses = await self._fetch_date_path(dataset, endpoint, current_date)
                responses.extend(date_responses)
                current_date += timedelta(days=1)

        elif endpoint.param_style == ParamStyle.PUBLISH_DATETIME:
            chunk_delta = timedelta(hours=endpoint.max_chunk_hours)
            current = start
            while current < end:
                chunk_end = min(current + chunk_delta, end)
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
                    data_date=settlement_date,
                )
            )

            if page >= total_pages:
                break
            page += 1

        return responses

    async def _fetch_date_period(
        self,
        dataset: str,
        endpoint: ElexonEndpoint,
        settlement_date: Any,
        max_periods: int = 50,
    ) -> list[RawResponse]:
        """Fetch all pages for each settlement period on a given date.

        Iterates through periods 1..max_periods, fetching paginated data for each.
        Stops early when a period returns either an HTTP error or an HTTP 200 with
        an empty ``data`` array — the latter occurs for periods 49–50 on non-DST days
        and avoids writing empty 11-byte files to the bronze layer.
        """
        responses: list[RawResponse] = []

        for period in range(1, max_periods + 1):
            page = 1
            while True:
                query_params = build_params(
                    endpoint,
                    settlement_date=settlement_date,
                    settlement_period=period,
                    page=page,
                )
                try:
                    raw = await self._request(endpoint.path, query_params)
                except httpx.HTTPStatusError as exc:
                    # Probed live (2026-05-31): a genuinely-absent period returns
                    # HTTP 200 with an empty data array (handled below), and an
                    # out-of-range period returns 4xx. So a 4xx here is a
                    # definitive "this period cannot exist" -> stop cleanly; a 5xx
                    # that survived the 5-attempt retry policy is a real upstream
                    # transient and MUST surface (not be recorded as a clean stop
                    # with the partial bronze of this date's earlier pages).
                    if exc.response.status_code < 500:
                        break
                    raise

                # HTTP 200 with empty data array means this period doesn't exist
                # (e.g. periods 49–50 on non-DST days). Stop without writing bronze.
                try:
                    parsed = json.loads(raw.content)
                    if isinstance(parsed.get("data"), list) and not parsed["data"]:
                        logger.debug(
                            "PN period %d on %s returned empty data — stopping period iteration",
                            period,
                            settlement_date,
                        )
                        break
                except (json.JSONDecodeError, AttributeError, TypeError):
                    pass  # Not JSON or unexpected shape — treat as real data

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
                        data_date=settlement_date,
                    )
                )

                if page >= total_pages:
                    break
                page += 1

        return responses

    async def _fetch_date_path(
        self,
        dataset: str,
        endpoint: ElexonEndpoint,
        settlement_date: date,
    ) -> list[RawResponse]:
        """Fetch all pages for a date embedded in the URL path (DATE_PATH style)."""
        responses: list[RawResponse] = []
        page = 1
        path = f"{endpoint.path}/{settlement_date.isoformat()}"

        while True:
            query_params = build_params(endpoint, page=page)
            raw = await self._request(path, query_params)

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
                    data_date=settlement_date,
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
        data_date = start.date() if isinstance(start, datetime) else start

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
                    data_date=data_date,
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
