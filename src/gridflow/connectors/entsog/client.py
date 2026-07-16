"""ENTSO-G Transparency Platform API connector."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime, time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import httpx

from gridflow.connectors.base import BaseConnector, RawResponse
from gridflow.connectors.entsog.endpoints import (
    ENDPOINTS,
    build_params,
)
from gridflow.connectors.registry import register_connector
from gridflow.utils.retry import RETRY_POLICY
from gridflow.utils.time import day_subwindows

logger = logging.getLogger(__name__)

# ENTSO-G's documented empty convention: HTTP 404 with body
# `{"message": "No result found"}`. Treat as empty-bronze success
# (no retry); any other 404 body falls through to the standard
# raise-and-retry path.
_ENTSOG_EMPTY_MESSAGE = "No result found"


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
        """Fetch ENTSO-G JSON data for a date range or reference endpoint.

        Datasets without date filters (``requires_dates=False``: reference
        endpoints) keep the single-request path, byte-identical to before.

        Datasets WITH date filters are chunked into one request PER COVERED
        UTC calendar day (P0.8 / R2-F08): bronze ``data_date`` must honour its
        documented contract (the calendar date the data refers to,
        ``connectors/base.py:47-49``), and the generic ENTSO-G silver family
        reads only the exact-date bronze partition
        (``silver/entsog/generic.py:181-193``) — pre-chunking, a multi-day
        window fetched days 2..N of content but stamped it all under day 1's
        partition, permanently stranding it from its own day's transform. The
        half-open ``day_subwindows`` derivation (not an inclusive
        ``date_range``) keeps a date-aligned ``run_backfill`` chunk
        (``[D, D+1T00Z)``) at exactly one ``from=to=D`` request — no boundary
        double-fetch (see the connector module's Sol-review rationale in the
        P0.8 plan).
        """
        if dataset not in ENDPOINTS:
            raise ValueError(f"Unknown ENTSO-G dataset: {dataset!r}. Available: {list(ENDPOINTS)}")

        endpoint = ENDPOINTS[dataset]

        if not endpoint.requires_dates:
            return [await self._fetch_one(dataset, endpoint.path, start, end, None, **params)]

        windows = day_subwindows(start, end)
        if not windows:
            # Degenerate guard: start == end -> single legacy-shape request,
            # preserving today's behaviour for a zero-width window.
            return [
                await self._fetch_one(dataset, endpoint.path, start, end, start.date(), **params)
            ]

        covered_days = sorted({sub_start.date() for sub_start, _ in windows})
        responses: list[RawResponse] = []
        for day in covered_days:
            day_dt = datetime.combine(day, time.min, tzinfo=UTC)
            responses.append(
                await self._fetch_one(dataset, endpoint.path, day_dt, day_dt, day, **params)
            )

        logger.info(
            "Fetched ENTSO-G %s for %d covered day(s) (%s..%s) via %s",
            dataset,
            len(covered_days),
            covered_days[0],
            covered_days[-1],
            endpoint.path,
        )
        return responses

    async def _fetch_one(
        self,
        dataset: str,
        path: str,
        start: datetime,
        end: datetime,
        data_date: date | None,
        **params: Any,
    ) -> RawResponse:
        """Issue one ENTSO-G request and wrap it into a ``RawResponse``.

        ``data_date`` is passed explicitly by the caller (``None`` for
        reference endpoints, the request's single covered day otherwise) —
        this method never derives it from ``start``/``end`` itself, so the
        per-day chunking loop in ``fetch()`` stays the single source of truth.
        """
        endpoint = ENDPOINTS[dataset]
        query_params = build_params(endpoint, start=start, end=end, **params)
        raw = await self._request(path, query_params)
        return RawResponse(
            body=raw.content,
            content_type=raw.headers.get("content-type", "application/json"),
            source="entsog",
            dataset=dataset,
            request_url=str(raw.url),
            request_params=dict(query_params),
            api_version="v1",
            http_status=raw.status_code,
            data_date=data_date,
        )

    @RETRY_POLICY
    async def _request(self, path: str, params: dict[str, Any]) -> httpx.Response:
        """Rate-limited, retried HTTP GET request.

        ENTSO-G empty convention is HTTP 404 + body
        ``{"message": "No result found"}``. That case short-circuits
        to an empty-bronze response so RETRY_POLICY does not waste
        budget on an expected vendor outcome (V2-FIX-07). Any other
        4xx/5xx (including a 404 with a different body) falls through
        to the standard `raise_for_status()` path and is retried per
        the surrounding RETRY_POLICY.
        """
        if self._client is None:
            raise RuntimeError("Connector not initialized. Use 'async with' context manager.")
        if self._semaphore is None:
            raise RuntimeError("Semaphore not initialized. Use 'async with' context manager.")

        async with self._semaphore:
            resp = await self._client.get(path, params=params)
            if resp.status_code == 404 and _is_empty_no_result_body(resp):
                logger.info(
                    "ENTSO-G empty result (404 'No result found') for %s — "
                    "short-circuit, no retry.",
                    path,
                )
                return resp
            resp.raise_for_status()
            return resp


def _is_empty_no_result_body(resp: httpx.Response) -> bool:
    """True iff `resp` body is the documented ENTSO-G empty marker."""
    try:
        body = resp.json()
    except (ValueError, json.JSONDecodeError):
        return False
    return isinstance(body, dict) and body.get("message") == _ENTSOG_EMPTY_MESSAGE


register_connector("entsog", EntsogConnector)
