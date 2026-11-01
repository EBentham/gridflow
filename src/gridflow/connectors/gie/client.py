"""GIE AGSI+ and ALSI API connector."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, time
from typing import Any, Literal, cast
from urllib.parse import parse_qs, urlparse

from gridflow.connectors.base import BaseConnector, RawResponse
from gridflow.connectors.gie.endpoints import (
    AGSI_COUNTRIES,
    ALSI_COUNTRIES,
    DEFAULT_PAGE_SIZE,
    ENDPOINTS,
    GIE_API_PATH,
    QueryScope,
    build_storage_query_plan,
)
from gridflow.connectors.registry import register_connector
from gridflow.utils.time import window_dates

logger = logging.getLogger(__name__)

# Map source name -> country list
_COUNTRY_MAP: dict[str, list[str]] = {
    "gie_agsi": AGSI_COUNTRIES,
    "gie_alsi": ALSI_COUNTRIES,
}
_AGSI_STORAGE_DATASETS = {"storage", "storage_reports"}


class GieConnector(BaseConnector):
    """Connector for the GIE AGSI+ (gas storage) and ALSI (LNG) APIs.

    Both APIs share the same structure: authenticate via ``x-key`` header,
    query ``/api`` with country + date range params, return paginated JSON.
    """

    # source_name is set dynamically at class-registration time
    source_name = "gie_agsi"
    # GIE allows 60 calls a minute: every send, retries included, is paced at
    # ``rate_limit_per_second`` (1 in ``sources.yaml``).
    PACED_SENDS = True

    def list_datasets(self) -> list[str]:
        return list(self.config.datasets.keys())

    async def fetch(
        self,
        dataset: str,
        start: datetime,
        end: datetime,
    ) -> list[RawResponse]:
        """Fetch GIE data for the half-open window ``[start, end)``."""
        # GIE takes inclusive gas days; convert the half-open window once, here.
        end = datetime.combine(window_dates(start, end)[-1], time.min, tzinfo=end.tzinfo)
        if self.source_name == "gie_agsi":
            if dataset in _AGSI_STORAGE_DATASETS:
                return await self._fetch_agsi_storage(dataset=dataset, start=start, end=end)
            return await self._fetch_agsi_endpoint(dataset=dataset, start=start, end=end)

        return await self._fetch_legacy_country_dataset(
            dataset=dataset,
            start=start,
            end=end,
        )

    async def _fetch_legacy_country_dataset(
        self,
        *,
        dataset: str,
        start: datetime,
        end: datetime,
    ) -> list[RawResponse]:
        """Fetch legacy country-scoped GIE datasets.

        One bad country must not abort the rest (the API serves countries
        independently), so each is fetched concurrently and its failure is
        captured rather than propagated. But a partial result must be visible
        downstream, not silently recorded as success: the number of failed
        countries is tallied into ``last_skipped_units`` once after the gather
        (CH-COR-01), and if *every* attempted country failed the last error is
        re-raised so the run is a hard ``failed`` rather than an empty
        ``completed_with_warnings`` (raise-on-all-fail, matching the Open-Meteo
        contract).

        Countries are independent units, so they fetch concurrently (CH3-03 /
        C2-7). Paging *within* a country stays sequential inside
        ``_fetch_country``; the semaphore keeps in-flight HTTP bounded.
        """
        countries = _COUNTRY_MAP.get(self.source_name, AGSI_COUNTRIES)
        responses = await self._gather_units(
            [
                self._fetch_country(dataset=dataset, country=country, start=start, end=end)
                for country in countries
            ],
            skip_labels=[f"{dataset} for country {country}" for country in countries],
        )

        logger.info(
            "Fetched %d responses for %s/%s from %s to %s (%d countries skipped)",
            len(responses),
            self.source_name,
            dataset,
            start.date(),
            end.date(),
            self.last_skipped_units,
        )
        return responses

    async def _fetch_agsi_storage(
        self,
        *,
        dataset: str,
        start: datetime,
        end: datetime,
    ) -> list[RawResponse]:
        """Fetch AGSI storage: every country for ``storage``, the EU aggregate otherwise."""
        scope = QueryScope.COUNTRY if dataset == "storage" else QueryScope.AGGREGATE_TYPE
        responses: list[RawResponse] = []
        for planned in build_storage_query_plan(scope=scope, start=start, end=end):
            responses.extend(
                await self._fetch_paginated(
                    dataset=dataset,
                    path=GIE_API_PATH,
                    params=planned.params,
                    data_date=planned.gas_day,
                    paginated=True,
                    validate_gas_day=True,
                )
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

    async def _fetch_agsi_endpoint(
        self,
        *,
        dataset: str,
        start: datetime,
        end: datetime,
        turl: str | None = None,
    ) -> list[RawResponse]:
        """Fetch a non-storage AGSI endpoint family (one news item when ``turl`` is set)."""
        if dataset not in ENDPOINTS:
            raise ValueError(f"Unknown GIE AGSI dataset: {dataset!r}")

        endpoint = ENDPOINTS[dataset]
        query_params = dict(endpoint.default_params)
        if dataset == "news_item":
            if turl is None:
                return await self._fetch_agsi_news_items_from_listing(start=start, end=end)
            query_params["turl"] = turl
        if dataset == "unavailability":
            query_params.update(
                {
                    "start": start.date().isoformat(),
                    "end": end.date().isoformat(),
                }
            )

        responses = await self._fetch_paginated(
            dataset=dataset,
            path=endpoint.path,
            params=query_params,
            data_date=start.date(),
            paginated=endpoint.paginated,
            validate_gas_day=False,
        )
        if dataset == "news_item":
            valid = [response for response in responses if _is_news_item_detail(response.body)]
            if len(valid) != len(responses):
                logger.warning("Discarded listing-shaped AGSI news_item response")
            return valid
        return responses

    async def _fetch_agsi_news_items_from_listing(
        self,
        *,
        start: datetime,
        end: datetime,
    ) -> list[RawResponse]:
        """Fetch news item details by discovering item URLs from the news listing."""
        listing_responses = await self._fetch_agsi_endpoint(dataset="news", start=start, end=end)
        turls = list(
            dict.fromkeys(
                turl
                for response in listing_responses
                for turl in _news_item_turls(
                    response.body,
                    start=start.date(),
                    end=end.date(),
                )
            )
        )

        responses: list[RawResponse] = []
        for turl in turls:
            responses.extend(
                await self._fetch_agsi_endpoint(
                    dataset="news_item",
                    start=start,
                    end=end,
                    turl=turl,
                )
            )
        return responses

    async def _fetch_paginated(
        self,
        *,
        dataset: str,
        path: str,
        params: dict[str, Any],
        data_date: date | None,
        paginated: bool,
        validate_gas_day: bool,
    ) -> list[RawResponse]:
        responses: list[RawResponse] = []
        page = 1

        while True:
            query_params = {**params, "page": page} if paginated else dict(params)
            raw = await self._get(path, query_params)
            body = raw.content
            total_pages = _last_page(body) if paginated else 1
            if validate_gas_day and data_date is not None:
                _validate_storage_gas_day(body, data_date)

            responses.append(
                RawResponse.from_httpx(
                    raw,
                    source=self.source_name,
                    dataset=dataset,
                    params=query_params,
                    page=page,
                    total_pages=total_pages,
                    data_date=data_date,
                    record_count=_record_count(body),
                )
            )

            if not paginated or page >= total_pages:
                break
            page += 1

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
            raw = await self._get(GIE_API_PATH, query_params)
            body = raw.content

            responses.append(
                RawResponse.from_httpx(
                    raw,
                    source=self.source_name,
                    dataset=dataset,
                    params=query_params,
                    page=page,
                    data_date=start.date(),
                )
            )

            # Pagination terminator: ALSI/AGSI echo the authoritative total page
            # count under ``last_page`` (live-probed 2026-05-31 for ALSI lng —
            # envelope keys data/dataset/gas_day/last_page/total, where ``total``
            # is the per-page row count (== size), NOT the grand total, and
            # ``pageSize`` is absent). The old ``total``/``pageSize`` terminator
            # broke after page 1 — int(total)=5 (per-page) with a defaulted
            # pageSize=300 made ``1*300 >= 5`` immediately true — silently
            # truncating a multi-page country. Reuse the ``_last_page`` helper the
            # AGSI path already uses (it fails closed to 1 page on a malformed or
            # ``last_page``-less body).
            total_pages = _last_page(body)
            if page >= total_pages:
                break

            page += 1

        return responses


class AgsiConnector(GieConnector):
    """AGSI+ gas storage connector (source_name='gie_agsi')."""

    source_name = "gie_agsi"


class AlsiConnector(GieConnector):
    """ALSI LNG terminal connector (source_name='gie_alsi')."""

    source_name = "gie_alsi"


register_connector("gie_agsi", AgsiConnector)
register_connector("gie_alsi", AlsiConnector)


def _record_count(body: bytes) -> int | None:
    """The number of records in an AGSI body, stamped as ``record_count``.

    An HTTP 200 ``{"data": []}`` (a gas day not yet published, a quiet window)
    is a confirmed zero, so it is not evidence that advances the watermark
    (C-8). ``None`` when the body has no ``data`` list to count.
    """
    try:
        payload = json.loads(body)
    except ValueError:  # not JSON, or not decodable text
        return None
    data = payload.get("data") if isinstance(payload, dict) else None
    return len(data) if isinstance(data, list) else None


def _last_page(body: bytes) -> int:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return 1
    try:
        return max(1, int(payload.get("last_page", 1)))
    except (TypeError, ValueError):
        return 1


def _news_item_turls(
    body: bytes,
    *,
    start: date | None = None,
    end: date | None = None,
) -> list[str]:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return []

    if isinstance(payload, dict):
        records = payload.get("data", [])
        if isinstance(records, dict):
            records = [records]
    elif isinstance(payload, list):
        records = payload
    else:
        return []

    turls: list[str] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        if (
            start is not None
            and end is not None
            and not _news_record_in_window(
                record,
                start,
                end,
            )
        ):
            continue
        value = record.get("turl") or record.get("url") or record.get("id")
        turl = _normalise_news_turl(value)
        if turl is not None:
            turls.append(turl)
    return turls


def _normalise_news_turl(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    parsed = urlparse(text)
    query_turl = parse_qs(parsed.query).get("turl")
    if query_turl:
        return query_turl[0]
    if parsed.scheme and parsed.path:
        return parsed.path.rstrip("/").rsplit("/", 1)[-1]
    if text.startswith("/"):
        return text.rstrip("/").rsplit("/", 1)[-1]
    return text


def _is_news_item_detail(body: bytes) -> bool:
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return False
    if not isinstance(payload, dict):
        return False
    records = payload.get("data")
    if isinstance(records, list):
        return False
    return isinstance(records, dict) or bool(payload.get("turl") or payload.get("title"))


_NEWS_EVENT_DATE_KEYS = ("start_at", "startAt", "event_start", "eventStart")
_NEWS_EVENT_END_DATE_KEYS = ("end_at", "endAt", "event_end", "eventEnd")
_NEWS_REFERENCE_DATE_KEYS = (
    "updatedAt",
    "updated_at",
    "createdAt",
    "created_at",
    "publication_date",
    "publicationDate",
)


def classify_news_record_for_window(
    record: dict[str, Any],
    start: date,
    end: date,
) -> Literal["overlap", "outside", "undated"]:
    """Classify an AGSI news/news_item record against a ``[start, end]`` window.

    P0.8 tri-state hedge (Sol finding 3): ``"undated"`` covers BOTH a record
    with none of the event/reference date keys present AND a record whose key
    IS present but fails to parse (e.g. a vendor date-format drift such as
    ``start_at="vendor-new-format"``) — ``_date_from_record`` swallows
    ``ValueError`` into ``None`` for both cases, so a naive "keep when keys
    absent" hedge would still silently drop the latter. Callers must treat
    ``"undated"`` as "keep, but count/flag it" — never as "outside".

    Returns:
        ``"overlap"`` if a parseable event window overlaps ``[start, end]``,
        or (no event dates) a parseable reference date falls in
        ``[start, end]``. ``"outside"`` if a parseable date resolves and does
        NOT overlap/fall within the window. ``"undated"`` if no date among the
        event or reference key sets parses at all.
    """
    event_start = _date_from_record(record, *_NEWS_EVENT_DATE_KEYS)
    event_end = _date_from_record(record, *_NEWS_EVENT_END_DATE_KEYS)
    if event_start is not None or event_end is not None:
        # At least one bound is set; coalescing makes both non-None here.
        resolved_start = cast("date", event_start or event_end)
        resolved_end = cast("date", event_end or event_start)
        return "overlap" if (resolved_start <= end and resolved_end >= start) else "outside"

    reference_date = _date_from_record(record, *_NEWS_REFERENCE_DATE_KEYS)
    if reference_date is None:
        return "undated"
    return "overlap" if start <= reference_date <= end else "outside"


def _news_record_in_window(record: dict[str, Any], start: date, end: date) -> bool:
    """Byte-identical to the pre-P0.8 boolean contract, reimplemented atop
    the tri-state classifier — ``news_item`` discovery (``_news_item_turls``,
    which SKIPS non-window records) is unaffected: ``"undated"`` still means
    "not in window" here, exactly as the old code's ``False`` return did."""
    return classify_news_record_for_window(record, start, end) == "overlap"


def _date_from_record(record: dict[str, Any], *keys: str) -> date | None:
    for key in keys:
        value = record.get(key)
        if value in (None, ""):
            continue
        try:
            return date.fromisoformat(str(value)[:10])
        except ValueError:
            continue
    return None


def _validate_storage_gas_day(body: bytes, expected: date) -> None:
    payload = json.loads(body)
    rows = payload.get("data", [])
    if not isinstance(rows, list):
        raise ValueError("AGSI storage response data must be a list")

    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("AGSI storage response rows must be objects")
        gas_day = _row_gas_day(row, payload)
        if gas_day is not None and gas_day != expected:
            raise ValueError(
                f"AGSI storage response contains gas day {gas_day}, expected {expected}"
            )


def _row_gas_day(row: dict[str, Any], payload: dict[str, Any]) -> date | None:
    value = (
        row.get("gasDayStart")
        or row.get("gas_day")
        or row.get("gasDay")
        or payload.get("gas_day")
        or payload.get("gasDay")
    )
    if value is None:
        return None
    text = str(value)
    return date.fromisoformat(text[:10])
