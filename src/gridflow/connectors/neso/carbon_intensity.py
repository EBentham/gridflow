"""NESO / Carbon Intensity API connector."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

if TYPE_CHECKING:
    import httpx

from gridflow.connectors.base import BaseConnector, RawResponse
from gridflow.connectors.neso.endpoints import ENDPOINTS, NesoEndpoint, build_path
from gridflow.connectors.registry import register_connector
from gridflow.utils.retry import RETRY_POLICY

logger = logging.getLogger(__name__)

# Maximum date range the API accepts in a single request.
_MAX_DAYS_PER_REQUEST = 14
_UK_TZ = ZoneInfo("Europe/London")


class CarbonIntensityConnector(BaseConnector):
    """Connector for the public NESO Carbon Intensity API."""

    source_name = "neso"

    def list_datasets(self) -> list[str]:
        return list(ENDPOINTS)

    async def fetch(
        self,
        dataset: str,
        start: datetime,
        end: datetime,
        **params: Any,
    ) -> list[RawResponse]:
        """Fetch one NESO Carbon Intensity API dataset.

        A single request window is split into many sub-requests (settlement
        periods, 14-day chunks). One bad window must not abort the rest, so each
        is fetched in its own ``try``; but a partial result must surface, not be
        silently recorded as success. The number of failed windows is tallied
        into ``last_skipped_units`` once after the loop (CH-COR-01), and if
        *every* attempted window failed the last error is re-raised so the run
        is a hard ``failed`` (raise-on-all-fail, parallel to the GIE legacy
        path).
        """
        self.last_skipped_units = 0
        if dataset not in ENDPOINTS:
            raise ValueError(f"Unknown NESO dataset: {dataset!r}. Available: {list(ENDPOINTS)}")

        endpoint = ENDPOINTS[dataset]
        requests = _request_specs(endpoint, start, end, params)
        responses: list[RawResponse] = []
        failures: list[tuple[datetime, datetime, Exception]] = []

        for window_start, window_end, path_overrides in requests:
            path, path_values = build_path(
                endpoint,
                start=window_start,
                end=window_end,
                **path_overrides,
            )
            try:
                raw = await self._request(path, {})
                responses.append(
                    RawResponse(
                        body=raw.content,
                        content_type=raw.headers.get("content-type", "application/json"),
                        source="neso",
                        dataset=dataset,
                        request_url=str(raw.url),
                        request_params=path_values,
                        api_version="v1",
                        http_status=raw.status_code,
                        data_date=(None if endpoint.reference else window_start.date()),
                    )
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Failed to fetch NESO %s %s to %s: %s",
                    dataset,
                    window_start.date(),
                    window_end.date(),
                    exc,
                )
                failures.append((window_start, window_end, exc))

        # Tally once after the loop (not incremented mid-loop) so the CH3 swap to
        # asyncio.gather(..., return_exceptions=True) is a drop-in replacement.
        self.last_skipped_units = len(failures)

        # All attempted windows failed → re-raise so the run is recorded as
        # 'failed', not an empty 'success'/'completed_with_warnings' (C2-6).
        if not responses and failures:
            raise failures[-1][2]

        logger.info(
            "Fetched %d responses for neso/%s from %s to %s (%d windows skipped)",
            len(responses),
            dataset,
            start.date(),
            end.date(),
            len(failures),
        )
        return responses

    @RETRY_POLICY
    async def _request(self, path: str, params: dict[str, Any]) -> httpx.Response:
        """Rate-limited, retried HTTP GET request."""
        if self._client is None:
            raise RuntimeError("Connector not initialized. Use 'async with' context manager.")
        if self._semaphore is None:
            raise RuntimeError("Semaphore not initialized. Use 'async with' context manager.")

        async with self._semaphore:
            resp = await self._client.get(path, params=params)
            resp.raise_for_status()
            return resp


def _request_specs(
    endpoint: NesoEndpoint,
    start: datetime,
    end: datetime,
    base_overrides: dict[str, Any],
) -> list[tuple[datetime, datetime, dict[str, Any]]]:
    """Split date ranges into API request windows and path overrides."""
    if not endpoint.requires_window:
        return [(start, end, base_overrides)]

    if endpoint.daily_iteration:
        windows: list[tuple[datetime, datetime, dict[str, Any]]] = []
        current = start
        effective_end = end if end > start else start + timedelta(days=1)
        while current < effective_end:
            window_end = min(current + timedelta(days=1), effective_end)
            if endpoint.settlement_period_iteration:
                for period in range(1, _settlement_period_count(current.date()) + 1):
                    windows.append((current, window_end, {**base_overrides, "period": period}))
            else:
                windows.append((current, window_end, base_overrides))
            current = window_end
        return windows

    effective_end = (
        start + timedelta(days=1) if end <= start and "{to_dt}" in endpoint.path_template else end
    )

    # Annotation already established on the daily-iteration branch above; this branch
    # is reached only when that one is skipped, so re-annotating triggers [no-redef].
    windows = []
    chunk_start = start
    chunk_delta = timedelta(days=_MAX_DAYS_PER_REQUEST)
    while chunk_start < effective_end:
        chunk_end = min(chunk_start + chunk_delta, effective_end)
        windows.append((chunk_start, chunk_end, base_overrides))
        chunk_start = chunk_end
    if not windows:
        windows.append((start, start, base_overrides))
    return windows


def _settlement_period_count(settlement_date: date) -> int:
    """Return the GB settlement period count for a UK-local settlement date."""
    local_start = datetime(
        settlement_date.year,
        settlement_date.month,
        settlement_date.day,
        tzinfo=_UK_TZ,
    )
    local_end = local_start + timedelta(days=1)
    seconds = (local_end.astimezone(UTC) - local_start.astimezone(UTC)).total_seconds()
    return int(seconds // (30 * 60))


register_connector("neso", CarbonIntensityConnector)
