"""Elexon BMRS / Insights API connector."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Any

from gridflow.connectors.base import BaseConnector, RawResponse
from gridflow.connectors.elexon.endpoints import ENDPOINTS, ParamStyle, build_params
from gridflow.connectors.elexon.parsers import (
    pagination_from,
    parse_json_response,
    record_count_from,
)
from gridflow.connectors.registry import register_connector
from gridflow.utils.time import settlement_periods_in_day, window_dates

if TYPE_CHECKING:
    from collections.abc import Coroutine

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
    ) -> list[RawResponse]:
        """Fetch raw data for a date range from Elexon."""
        if dataset not in ENDPOINTS:
            raise ValueError(
                f"Unknown Elexon dataset: {dataset}. Available: {list(ENDPOINTS.keys())}"
            )

        endpoint = ENDPOINTS[dataset]

        if endpoint.param_style == ParamStyle.NO_PARAMS:
            responses = await self._fetch_single(dataset, endpoint)
            logger.info(
                f"Fetched {len(responses)} responses for elexon/{dataset} from {start} to {end}"
            )
            return responses

        # Each date (or publish-datetime chunk) is an independent unit: no chunk
        # reads another chunk's result, so they fetch concurrently (CH3-03 /
        # C2-7). Paging within a unit, and PN's settlement-period loop, stay
        # sequential. A failing unit fails the whole run (raise-on-any).
        tasks: list[Coroutine[Any, Any, list[RawResponse]]] = []

        if endpoint.param_style == ParamStyle.SETTLEMENT_DATE_PERIOD:
            for current_date in window_dates(start, end):
                tasks.append(self._fetch_date_period(dataset, endpoint, current_date))

        elif endpoint.param_style == ParamStyle.DATE_PATH:
            for current_date in window_dates(start, end):
                path = f"{endpoint.path}/{current_date.isoformat()}"
                tasks.append(self._fetch_pages(dataset, endpoint, path, current_date))

        elif endpoint.param_style == ParamStyle.PUBLISH_DATETIME:
            chunk_delta = timedelta(hours=endpoint.max_chunk_hours)
            current = start
            while current < end:
                chunk_end = min(current + chunk_delta, end)
                unit = self._fetch_pages(
                    dataset, endpoint, endpoint.path, current.date(), start=current, end=chunk_end
                )
                tasks.append(unit)
                current = chunk_end

        responses = await self._gather_units(tasks)

        logger.info(
            f"Fetched {len(responses)} responses for elexon/{dataset} from {start} to {end}"
        )
        return responses

    async def _fetch_date_period(
        self,
        dataset: str,
        endpoint: ElexonEndpoint,
        settlement_date: date,
    ) -> list[RawResponse]:
        """Fetch every page of every settlement period of one date (PN).

        Periods run 1..``settlement_periods_in_day`` (46/48/50 by the UK clock
        calendar). The vendor accepts 1..50 on any date and answers a period
        outside the day's own calendar with an empty page (probed 2026-07-27,
        R2-B / F-22), so the bound saves requests rather than errors.
        """
        responses: list[RawResponse] = []
        for period in range(1, settlement_periods_in_day(settlement_date) + 1):
            responses.extend(
                await self._fetch_pages(
                    dataset, endpoint, endpoint.path, settlement_date, period=period
                )
            )
        return responses

    async def _fetch_pages(
        self,
        dataset: str,
        endpoint: ElexonEndpoint,
        path: str,
        data_date: date,
        *,
        period: int | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[RawResponse]:
        """Fetch every page of one request, parsing each body once (D-18).

        For a settlement ``period`` an empty first page means the period has no
        data, and an empty later page is a truncated pagination, which raises
        (F-22). HTTP errors propagate once the retry policy is exhausted.
        """
        responses: list[RawResponse] = []
        page = 1

        while True:
            query_params = build_params(
                endpoint,
                settlement_date=data_date,
                settlement_period=period,
                start=start,
                end=end,
                page=page,
            )
            raw = await self._get(path, query_params)
            # A body that fails to parse yields {} and is kept as real data.
            parsed = parse_json_response(raw.content)

            if period is not None:
                data = parsed.get("data") if isinstance(parsed, dict) else None
                if isinstance(data, list) and not data:
                    if page > 1:
                        raise RuntimeError(
                            "Elexon pagination truncated for "
                            f"{dataset} on {data_date}: period {period}, page {page} "
                            "returned empty data after an earlier populated page"
                        )
                    logger.debug(
                        "PN period %d on %s returned empty data; continuing to the next period",
                        period,
                        data_date,
                    )
                    break

            _, total_pages = pagination_from(parsed)
            responses.append(
                RawResponse.from_httpx(
                    raw,
                    source="elexon",
                    dataset=dataset,
                    params=query_params,
                    page=page,
                    total_pages=total_pages,
                    data_date=data_date,
                    record_count=record_count_from(parsed),
                )
            )

            if page >= total_pages:
                break
            page += 1

        return responses

    async def _fetch_single(self, dataset: str, endpoint: ElexonEndpoint) -> list[RawResponse]:
        """Fetch a single-request endpoint (e.g., reference data)."""
        query_params = build_params(endpoint)
        raw = await self._get(endpoint.path, query_params)
        return [RawResponse.from_httpx(raw, source="elexon", dataset=dataset, params=query_params)]


# Register this connector
register_connector("elexon", ElexonConnector)
