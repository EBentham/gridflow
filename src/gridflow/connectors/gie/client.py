"""GIE AGSI+ and ALSI API connector."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

if TYPE_CHECKING:
    import httpx

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
from gridflow.utils.retry import RETRY_POLICY

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

    def list_datasets(self) -> list[str]:
        return list(self.config.datasets.keys())

    async def fetch(
        self,
        dataset: str,
        start: datetime,
        end: datetime,
        **params: Any,
    ) -> list[RawResponse]:
        """Fetch GIE data for the given date range."""
        if self.source_name == "gie_agsi":
            if dataset in _AGSI_STORAGE_DATASETS:
                return await self._fetch_agsi_storage(
                    dataset=dataset,
                    start=start,
                    end=end,
                    **params,
                )
            return await self._fetch_agsi_endpoint(
                dataset=dataset,
                start=start,
                end=end,
                **params,
            )

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
        """Fetch legacy country-scoped GIE datasets."""
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

    async def _fetch_agsi_storage(
        self,
        *,
        dataset: str,
        start: datetime,
        end: datetime,
        **params: Any,
    ) -> list[RawResponse]:
        """Fetch AGSI storage reports for a documented query scope."""
        scope = QueryScope(
            params.pop(
                "scope",
                QueryScope.COUNTRY if dataset == "storage" else QueryScope.AGGREGATE_TYPE,
            )
        )
        listing_payload = params.pop("listing_payload", None)
        if scope in (QueryScope.COMPANY, QueryScope.FACILITY) and listing_payload is None:
            listing_payload = await self._fetch_listing_payload()

        plan = build_storage_query_plan(
            scope=scope,
            start=start,
            end=end,
            aggregate_types=_as_tuple(
                params.pop("aggregate_types", None),
                default=("EU",),
            ),
            countries=_as_optional_tuple(params.pop("countries", None)),
            listing_payload=listing_payload,
            size=int(params.pop("size", DEFAULT_PAGE_SIZE)),
        )

        responses: list[RawResponse] = []
        for planned in plan:
            if len(planned.expected_gas_days) != 1:
                raise ValueError("AGSI exact-date storage plan must target one gas day")
            gas_day = planned.expected_gas_days[0]
            responses.extend(
                await self._fetch_paginated(
                    dataset=dataset,
                    path=planned.path,
                    params=planned.params,
                    data_date=gas_day,
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
        **params: Any,
    ) -> list[RawResponse]:
        """Fetch a non-storage AGSI endpoint family."""
        if dataset not in ENDPOINTS:
            raise ValueError(f"Unknown GIE AGSI dataset: {dataset!r}")

        endpoint = ENDPOINTS[dataset]
        query_params = dict(endpoint.default_params)
        if dataset == "news_item":
            turl = params.pop("turl", params.pop("id", None))
            if turl is None:
                return await self._fetch_agsi_news_items_from_listing(
                    start=start,
                    end=end,
                    **params,
                )
            query_params["turl"] = turl
        if dataset == "unavailability":
            query_params.update(
                {
                    "start": start.date().isoformat(),
                    "end": end.date().isoformat(),
                }
            )
        query_params.update(params)

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
        **params: Any,
    ) -> list[RawResponse]:
        """Fetch news item details by discovering item URLs from the news listing."""
        listing_responses = await self._fetch_agsi_endpoint(
            dataset="news",
            start=start,
            end=end,
            **params,
        )
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

    async def _fetch_listing_payload(self) -> dict[str, Any] | list[dict[str, Any]]:
        raw = await self._request(
            ENDPOINTS["about_listing"].path,
            dict(ENDPOINTS["about_listing"].default_params),
        )
        return json.loads(raw.content)

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
        page = int(params.get("page", 1))

        while True:
            query_params = {**params, "page": page} if paginated else dict(params)
            raw = await self._request(path, query_params)
            body = raw.content
            total_pages = _last_page(body) if paginated else 1
            if validate_gas_day and data_date is not None:
                _validate_storage_gas_day(body, data_date)

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
                    total_pages=total_pages,
                    http_status=raw.status_code,
                    data_date=data_date,
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


class AgsiConnector(GieConnector):
    """AGSI+ gas storage connector (source_name='gie_agsi')."""

    source_name = "gie_agsi"


class AlsiConnector(GieConnector):
    """ALSI LNG terminal connector (source_name='gie_alsi')."""

    source_name = "gie_alsi"


register_connector("gie_agsi", AgsiConnector)
register_connector("gie_alsi", AlsiConnector)


def _as_tuple(value: Any, *, default: tuple[str, ...]) -> tuple[str, ...]:
    if value is None:
        return default
    if isinstance(value, str):
        return (value,)
    return tuple(value)


def _as_optional_tuple(value: Any) -> tuple[str, ...] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return (value,)
    return tuple(value)


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


def _news_record_in_window(record: dict[str, Any], start: date, end: date) -> bool:
    event_start = _date_from_record(record, "start_at", "startAt", "event_start", "eventStart")
    event_end = _date_from_record(record, "end_at", "endAt", "event_end", "eventEnd")
    if event_start is not None or event_end is not None:
        event_start = event_start or event_end
        event_end = event_end or event_start
        return event_start <= end and event_end >= start

    reference_date = _date_from_record(
        record,
        "updatedAt",
        "updated_at",
        "createdAt",
        "created_at",
        "publication_date",
        "publicationDate",
    )
    return reference_date is not None and start <= reference_date <= end


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
