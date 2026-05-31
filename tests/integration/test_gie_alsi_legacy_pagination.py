"""Mocked ALSI legacy-paginator behavioural test (issue-13 criterion 5).

The ALSI ``lng`` dataset routes through ``GieConnector.fetch`` ->
``_fetch_legacy_country_dataset`` -> ``_fetch_country`` (because
``source_name != "gie_agsi"``). That paginator previously terminated on
``total``/``pageSize``, which — per a live probe of alsi.gie.eu on
2026-05-31 — is wrong: the envelope echoes ``last_page`` (the authoritative
page count) and ``total`` (the *per-page* row count, == size), and omits
``pageSize``. So ``int(total)=5`` with a defaulted ``pageSize=300`` made
``page*page_size >= total`` true on page 1, silently truncating every
multi-page country to its first page. The fix reuses the AGSI ``_last_page``
strategy; this test pins page-2 retrieval against the real envelope shape.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime

import httpx
import pytest
import respx

from gridflow.config.settings import SourceConfig, load_settings
from gridflow.connectors.gie.client import AlsiConnector
from gridflow.connectors.gie.endpoints import ALSI_COUNTRIES

ALSI_BASE_URL = "https://alsi.gie.eu"
START = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)
END = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)


def _alsi_config() -> SourceConfig:
    return (
        load_settings()
        .get_source_config("gie_alsi")
        .model_copy(update={"api_key": "test-key", "rate_limit_per_second": 1000, "timeout": 5})
    )


def _alsi_lng_body(*, last_page: int, page: int, rows: int = 5) -> bytes:
    """Mirror the live ALSI lng envelope: ``last_page`` authoritative,
    ``total`` == per-page row count, no ``pageSize``."""
    return json.dumps(
        {
            "last_page": last_page,
            "total": rows,
            "gas_day": "2024-01-15",
            "dataset": "lng",
            "data": [{"name": "ZEE", "lngInventory": "1.0", "page": page} for _ in range(rows)],
        }
    ).encode()


@respx.mock
@pytest.mark.asyncio
async def test_alsi_legacy_paginator_follows_last_page_not_total() -> None:
    """Every ALSI country's page 2 is fetched (``last_page=2``), not just page 1.

    FAILS against the pre-fix ``total``/``pageSize`` terminator, which breaks
    after page 1 on the real envelope shape (``total`` is per-page, ``pageSize``
    absent), returning only ``len(ALSI_COUNTRIES)`` responses.
    """
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        page = int(request.url.params.get("page", "1"))
        return httpx.Response(
            200,
            content=_alsi_lng_body(last_page=2, page=page),
            headers={"content-type": "application/json"},
        )

    respx.get(re.compile(rf"^{re.escape(ALSI_BASE_URL)}/.*")).mock(side_effect=handler)

    async with AlsiConnector(_alsi_config()) as connector:
        responses = await connector.fetch("lng", START, END)

    pages_requested = sorted({int(dict(r.url.params)["page"]) for r in requests})
    assert pages_requested == [1, 2], f"expected pages 1 and 2, got {pages_requested}"
    # Every country contributes both pages — not just page 1.
    assert len(responses) == 2 * len(ALSI_COUNTRIES)
    assert all(r.source == "gie_alsi" for r in responses)
    assert all(r.dataset == "lng" for r in responses)
