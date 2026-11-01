"""NESO / Carbon Intensity API connector."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from gridflow.connectors.base import BaseConnector, RawResponse
from gridflow.connectors.neso.endpoints import ENDPOINTS, NesoEndpoint, build_path
from gridflow.connectors.registry import register_connector
from gridflow.utils.time import day_subwindows

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
        requests = _request_specs(endpoint, start, end)
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
                raw = await self._get(path, {})
                responses.append(
                    RawResponse.from_httpx(
                        raw,
                        source="neso",
                        dataset=dataset,
                        params=path_values,
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


def _request_specs(
    endpoint: NesoEndpoint,
    start: datetime,
    end: datetime,
) -> list[tuple[datetime, datetime, dict[str, Any]]]:
    """Split date ranges into API request windows and path overrides."""
    if not endpoint.requires_window:
        return [(start, end, {})]

    if endpoint.daily_iteration:
        # One request per UTC calendar day the window touches, so a window ending
        # mid-day still fetches that day; a zero-width window keeps its one day.
        windows: list[tuple[datetime, datetime, dict[str, Any]]] = []
        for day_start, day_end in day_subwindows(start, end) or [
            (start, start + timedelta(days=1))
        ]:
            if endpoint.settlement_period_iteration:
                for period in range(1, _settlement_period_count(day_start.date()) + 1):
                    windows.append((day_start, day_end, {"period": period}))
            else:
                windows.append((day_start, day_end, {}))
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
        windows.append((chunk_start, chunk_end, {}))
        chunk_start = chunk_end
    if not windows:
        windows.append((start, start, {}))
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
