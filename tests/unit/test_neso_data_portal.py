"""NESO Data Portal connector unit tests (offline).

Every test here is offline: HTTP is mocked at the transport with respx, and
name resolution is stubbed through the shared ``stub_neso_resolver`` fixture —
respx mocks HTTP but **not** DNS, and under D-39 §1a every send validates its
target by resolving the host. The module-level ``pytestmark`` below is that
opt-in; the live module (T-24) deliberately does not declare it.

T-26 owns the window-admission suite, T-11 the catalogue suite, and T-04
everything from the transport-marker proof onward.
"""

from __future__ import annotations

import ast
import asyncio
import ipaddress
import json
import logging
import pickle
import socket
import traceback
from datetime import UTC, date, datetime, timedelta, timezone
from pathlib import Path
from time import monotonic
from typing import TYPE_CHECKING, Any, ClassVar

import httpx
import polars as pl
import pytest
import respx
from pydantic import Field
from tenacity import wait_none

from gridflow.config.settings import DatasetConfig, PipelineSettings, SourceConfig
from gridflow.connectors.neso_data_portal import client as client_module
from gridflow.connectors.neso_data_portal.client import (
    _MAX_INGEST_WINDOW,
    _MAX_REDIRECT_HOPS,
    _VALIDATED_MARKER,
    CkanActionError,
    CkanPaginationMismatch,
    NesoDataPortalConnector,
    NesoEmptyResourceError,
    NesoFutureWindowError,
    NesoHistoricalWindowError,
    NesoResourceSelectionError,
    NesoResponseTooLargeError,
    NesoTruncatedBodyError,
    NesoUnexpectedEncodingError,
    NesoUnexpectedResourceUrlError,
    NesoUnexpectedStatusError,
    NesoUnsafeRedirectError,
    NesoWindowTooLongError,
    _resolve_host_addresses,
)
from gridflow.connectors.neso_data_portal.endpoints import DATASETS
from gridflow.schemas.common import BaseSchema
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.csv_bronze import CsvHeaderDriftError, NotCsvBodyError
from gridflow.silver.elexon.system_prices import SystemPriceTransformer
from gridflow.silver.neso_data_portal._bronze import provenance_for
from gridflow.silver.neso_data_portal.daily_wind_availability import (
    EXPECTED_COLUMNS,
    DailyWindAvailabilityTransformer,
)
from gridflow.silver.registry import get_transformer_class, list_transformers

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator

pytestmark = pytest.mark.usefixtures("stub_neso_resolver")

BASE_URL = "https://api.neso.energy"
PACKAGE_SHOW_URL = f"{BASE_URL}/api/3/action/package_show"

DAILY_WIND_PACKAGE_ID = "3758a0ed-6c96-4e36-88d0-107f5020ddf3"
DAILY_WIND_RESOURCE_ID = "7aa508eb-36f5-4298-820f-2fa6745ae2e7"
DAILY_WIND_RESOURCE_URL = (
    f"{BASE_URL}/dataset/{DAILY_WIND_PACKAGE_ID}/resource/{DAILY_WIND_RESOURCE_ID}"
    "/download/windavailability.csv"
)
DAILY_WIND_CSV = b"BMU_ID,Date,MW\nT_ABC-1,2026-08-16,120.5\nT_ABC-2,2026-08-16,98\n"

DATASET = "daily_wind_availability"


def _package_show_payload() -> dict[str, Any]:
    """The CKAN envelope for ``daily-wind-availability``, trimmed from _probe."""
    return {
        "success": True,
        "result": {
            "id": DAILY_WIND_PACKAGE_ID,
            "name": "daily-wind-availability",
            "license_id": "ESO",
            "resources": [
                {
                    "id": DAILY_WIND_RESOURCE_ID,
                    "name": "Daily Wind Availability",
                    "format": "CSV",
                    "url_type": "upload",
                    "last_modified": "2026-08-16T18:20:11.953941",
                    "url": DAILY_WIND_RESOURCE_URL,
                }
            ],
        },
    }


def _source_config(*, max_query_days: int = 1, rate_limit_per_second: int = 1000) -> SourceConfig:
    """A NESO source config.

    ``rate_limit_per_second`` defaults high so the suite does not pay a real
    1 s interval per send; the throttle's own pacing is asserted deliberately,
    by the tests that pass ``rate_limit_per_second=1``.
    """
    return SourceConfig(
        base_url=BASE_URL,
        api_key_env="",
        api_key_header="",
        rate_limit_per_second=rate_limit_per_second,
        timeout=30,
        max_retries=3,
        datasets={
            DATASET: DatasetConfig(
                endpoint="/api/3/action/package_show",
                schedule="daily",
                max_query_days=max_query_days,
            )
        },
    )


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    """A GLOBAL respx router — never ``base_url``-scoped.

    A scoped router only patches traffic matching its base URL, so a request
    aimed elsewhere would never be observed and every zero-request assertion
    below would silently narrow to "no request to the host we expected".
    """
    with respx.mock(assert_all_called=False) as mock_router:
        yield mock_router


def _wire_happy_path(router: respx.MockRouter) -> None:
    """Route the two sends of a clean ``daily_wind_availability`` fetch."""
    router.get(url__startswith=PACKAGE_SHOW_URL).mock(
        return_value=httpx.Response(200, json=_package_show_payload())
    )
    router.get(url__startswith=DAILY_WIND_RESOURCE_URL).mock(
        return_value=httpx.Response(200, content=DAILY_WIND_CSV)
    )


def _fetch(
    config: SourceConfig,
    start: datetime,
    end: datetime,
    dataset: str = DATASET,
) -> list[Any]:
    """Drive one ``fetch()`` through the connector's real async entry point."""

    async def _run() -> list[Any]:
        async with NesoDataPortalConnector(config) as connector:
            return await connector.fetch(dataset, start, end)

    return asyncio.run(_run())


# ---------------------------------------------------------------------------
# T-11: discover_catalog (D-17)
# ---------------------------------------------------------------------------

PACKAGE_SEARCH_URL = f"{BASE_URL}/api/3/action/package_search"
PACKAGE_LIST_URL = f"{BASE_URL}/api/3/action/package_list"


def _catalog_pages(
    page_names: list[list[str]],
    *,
    count: int | None = None,
    counts: list[int] | None = None,
    headers: dict[str, str] | None = None,
) -> list[httpx.Response]:
    """Build one ``package_search`` response per page of package names."""
    total = count if count is not None else sum(len(names) for names in page_names)
    responses = []
    for index, names in enumerate(page_names):
        page_count = counts[index] if counts is not None else total
        responses.append(
            httpx.Response(
                200,
                headers=headers,
                json={
                    "success": True,
                    "result": {
                        "count": page_count,
                        "results": [{"name": name, "resources": []} for name in names],
                    },
                },
            )
        )
    return responses


def _wire_catalog(
    router: respx.MockRouter,
    page_names: list[list[str]],
    listed: list[str] | None = None,
    *,
    counts: list[int] | None = None,
    count: int | None = None,
    headers: dict[str, str] | None = None,
) -> None:
    """Route a paginated ``package_search`` plus the reconciling ``package_list``."""
    router.get(url__startswith=PACKAGE_SEARCH_URL).mock(
        side_effect=_catalog_pages(page_names, count=count, counts=counts, headers=headers)
    )
    if listed is None:
        listed = [name for names in page_names for name in names]
    router.get(url__startswith=PACKAGE_LIST_URL).mock(
        return_value=httpx.Response(200, headers=headers, json={"success": True, "result": listed})
    )


def _discover(config: SourceConfig) -> Any:
    """Drive ``discover_catalog()`` through the connector's real entry point."""

    async def _run() -> Any:
        async with NesoDataPortalConnector(config) as connector:
            return await connector.discover_catalog()

    return asyncio.run(_run())


class TestDiscoverCatalog:
    """D-17: page the catalogue, then RECONCILE it — permanently, not once.

    ``rows``/``start`` pagination is CKAN-generic and works today, but NESO does
    not contract it. A silently short catalogue is worse than no catalogue,
    because it looks complete — so every failure mode below is a raise, never a
    truncated result.
    """

    def test_three_page_happy_path_reconciles_exactly(self, router: respx.MockRouter) -> None:
        """The real shape of the Stage-A capture: 129 packages, 50/50/29."""
        pages = [
            [f"pkg-{i:03d}" for i in range(0, 50)],
            [f"pkg-{i:03d}" for i in range(50, 100)],
            [f"pkg-{i:03d}" for i in range(100, 129)],
        ]
        _wire_catalog(router, pages)

        discovery = _discover(_source_config())

        assert len(discovery.packages) == 129
        assert [p["name"] for p in discovery.packages] == [n for page in pages for n in page]
        assert len(router.calls) == 4, "three package_search pages + one package_list"

    def test_the_trace_has_one_populated_entry_per_http_call(
        self, router: respx.MockRouter
    ) -> None:
        """FM-14: ``provenance.json`` has no source other than this trace, so a
        missing field must fail the build rather than produce a snapshot that
        merely *has* a provenance file.

        Each required field is asserted INDIVIDUALLY. A single "is not None"
        over the dataclass would pass with every field defaulted.
        """
        vendor_headers = {
            "date": "Sun, 16 Aug 2026 18:54:01 GMT",
            "content-type": "application/json;charset=utf-8",
            "etag": '"2f733b738a4970f150601ca2b7da5df5"',
            "last-modified": "Sun, 16 Aug 2026 18:21:38 GMT",
            # Deliberately present and deliberately NOT recorded: an ephemeral
            # CDN id changes every run and would defeat snapshot comparison.
            "cf-ray": "a2c2a5209a41bed0-LHR",
        }
        _wire_catalog(router, [["alpha", "beta"]], headers=vendor_headers)

        discovery = _discover(_source_config())

        assert len(discovery.traces) == len(router.calls) == 2
        actions = [trace.action for trace in discovery.traces]
        assert actions == ["package_search", "package_list"]

        for trace in discovery.traces:
            assert trace.headers == {
                "date": vendor_headers["date"],
                "content-type": vendor_headers["content-type"],
                "etag": vendor_headers["etag"],
                "last-modified": vendor_headers["last-modified"],
            }, f"{trace.action}: the four recorded headers drifted"
            assert "cf-ray" not in trace.headers, (
                "an ephemeral per-request header was captured into provenance"
            )

        for trace in discovery.traces:
            assert trace.action, "action is empty"
            assert trace.started_at.tzinfo is not None, "started_at is naive"
            assert trace.started_at.utcoffset() == timedelta(0), "started_at is not UTC"
            assert trace.finished_at.tzinfo is not None, "finished_at is naive"
            assert trace.finished_at.utcoffset() == timedelta(0), "finished_at is not UTC"
            assert trace.finished_at >= trace.started_at, "timings are inverted"
            assert trace.status_code == 200
            assert len(trace.body_sha256) == 64, "body hash is not a sha256 hex digest"
            assert int(trace.body_sha256, 16) >= 0, "body hash is not hex"

        search_trace = discovery.traces[0]
        assert search_trace.params == {"rows": "50", "start": "0"}, (
            "the paginator's own constructed params must be recorded verbatim"
        )

    def test_a_count_that_changes_mid_pagination_raises(self, router: respx.MockRouter) -> None:
        """The catalogue moved under the paginator, so the collected set is
        neither the old catalogue nor the new one."""
        pages = [[f"pkg-{i}" for i in range(50)], ["pkg-50"]]
        _wire_catalog(router, pages, counts=[51, 60])

        with pytest.raises(CkanPaginationMismatch) as excinfo:
            _discover(_source_config())

        message = str(excinfo.value)
        assert "51" in message
        assert "60" in message

    def test_a_duplicate_package_name_across_pages_raises(self, router: respx.MockRouter) -> None:
        """A repeat means the page window shifted — so some package was SKIPPED,
        and the result would be short while looking complete."""
        pages = [[f"pkg-{i}" for i in range(50)], ["pkg-0", "pkg-50"]]
        _wire_catalog(router, pages, count=52)

        with pytest.raises(CkanPaginationMismatch) as excinfo:
            _discover(_source_config())

        assert "pkg-0" in str(excinfo.value)

    def test_an_early_empty_page_cannot_pass_as_a_complete_catalogue(
        self, router: respx.MockRouter
    ) -> None:
        """The vendor declared 5 packages and served 2, then an empty page.

        ``package_list`` is wired to AGREE with the short set, which is what
        makes this dangerous: the name-set reconciliation below would pass and
        the run would return a catalogue missing three packages while reporting
        it as verified. A reconciliation that can succeed on an incomplete set
        launders the incompleteness — strictly worse than having no check.
        """
        _wire_catalog(router, [["alpha", "beta"], []], listed=["alpha", "beta"], count=5)

        with pytest.raises(CkanPaginationMismatch) as excinfo:
            _discover(_source_config())

        message = str(excinfo.value)
        assert "count=5" in message
        assert "2 packages" in message

    def test_more_results_than_the_declared_count_also_raises(
        self, router: respx.MockRouter
    ) -> None:
        """The other direction. An overfull page means ``count`` and the result
        set disagree, and there is no basis for choosing which to believe."""
        _wire_catalog(router, [["alpha", "beta", "gamma"]], count=2)

        with pytest.raises(CkanPaginationMismatch) as excinfo:
            _discover(_source_config())

        assert "count=2" in str(excinfo.value)
        assert "3 packages" in str(excinfo.value)

    def test_an_empty_catalogue_is_not_itself_an_error(self, router: respx.MockRouter) -> None:
        """The completeness rule must not misfire on a legitimately empty
        answer: 0 collected against a declared 0 is consistent."""
        _wire_catalog(router, [[]], listed=[], count=0)

        discovery = _discover(_source_config())

        assert discovery.packages == ()

    def test_a_package_list_longer_than_the_paginated_set_raises(
        self, router: respx.MockRouter
    ) -> None:
        _wire_catalog(router, [["alpha", "beta"]], listed=["alpha", "beta", "gamma"])

        with pytest.raises(CkanPaginationMismatch) as excinfo:
            _discover(_source_config())

        message = str(excinfo.value)
        assert "2" in message and "3" in message, "both totals must be named"
        assert "gamma" in message, "the example names must be listed"

    def test_a_package_list_shorter_than_the_paginated_set_raises(
        self, router: respx.MockRouter
    ) -> None:
        """The inverse direction. Asserted separately because a set-difference
        implemented in one direction only would pass the case above."""
        _wire_catalog(router, [["alpha", "beta", "gamma"]], listed=["alpha", "beta"])

        with pytest.raises(CkanPaginationMismatch) as excinfo:
            _discover(_source_config())

        assert "gamma" in str(excinfo.value)


class TestWindowAdmission:
    """D-34: four ordered checks, every one before a byte leaves the process.

    Each refusal asserts the router recorded **zero** requests — not merely
    that an exception was raised. "It raised" is satisfied by a guard placed
    after the first send; "zero requests" is not.
    """

    def test_window_ending_now_proceeds_and_issues_its_sends(
        self, router: respx.MockRouter
    ) -> None:
        """The ``--last 24h`` shape: the ordinary command must not be refused."""
        _wire_happy_path(router)
        end = datetime.now(UTC)

        responses = _fetch(_source_config(), end - timedelta(hours=24), end)

        assert len(responses) == 1
        assert len(router.calls) == 2, "one package_show + one resource download"

    @pytest.mark.parametrize(
        ("start", "end", "case"),
        [
            (
                datetime(2026, 8, 16, 0, 0),
                datetime.now(UTC),
                "naive start",
            ),
            (
                datetime.now(UTC) - timedelta(hours=24),
                datetime(2026, 8, 16, 0, 0),
                "naive end",
            ),
            (
                datetime.now(timezone(timedelta(hours=2))) - timedelta(hours=24),
                datetime.now(timezone(timedelta(hours=2))),
                "aware but non-UTC offset",
            ),
        ],
    )
    def test_malformed_endpoints_raise_before_any_request(
        self, router: respx.MockRouter, start: datetime, end: datetime, case: str
    ) -> None:
        """Defence for direct programmatic callers — the CLI already rejects
        naive input. It also protects D-13: a non-UTC ``end`` would silently
        partition bronze to the wrong day."""
        _wire_happy_path(router)

        with pytest.raises(ValueError):
            _fetch(_source_config(), start, end)

        assert len(router.calls) == 0, f"{case}: refused after a request was sent"

    def test_end_before_start_raises_before_any_request(self, router: respx.MockRouter) -> None:
        _wire_happy_path(router)
        now = datetime.now(UTC)

        with pytest.raises(ValueError):
            _fetch(_source_config(), now, now - timedelta(hours=1))

        assert len(router.calls) == 0

    @pytest.mark.parametrize("ahead", [timedelta(hours=1), timedelta(hours=24)])
    def test_future_end_is_refused_and_names_the_remedy(
        self, router: respx.MockRouter, ahead: timedelta
    ) -> None:
        """A future ``end`` is unserveable by definition and is the one window
        shape that defeats D-13 outright: the capture would land under a future
        partition no ``--last``-style transform window ever reaches.

        24 h ahead is the likely trigger — a bare ``--end <tomorrow>`` meant as
        "through today", which ``_parse_window_bound`` reads as midnight
        tomorrow — so the message names the remedy.
        """
        _wire_happy_path(router)
        end = datetime.now(UTC) + ahead

        with pytest.raises(NesoFutureWindowError) as excinfo:
            _fetch(_source_config(), end - timedelta(hours=24), end)

        message = str(excinfo.value)
        assert "--last 24h" in message
        assert "--end" in message
        assert len(router.calls) == 0

    def test_end_two_minutes_ahead_is_within_the_skew_tolerance(
        self, router: respx.MockRouter
    ) -> None:
        """The 5 min tolerance covers host clock skew and nothing else — both
        sides of it are exercised."""
        _wire_happy_path(router)
        end = datetime.now(UTC) + timedelta(minutes=2)

        responses = _fetch(_source_config(), end - timedelta(hours=24), end)

        assert len(responses) == 1

    def test_end_49_hours_stale_is_refused(self, router: respx.MockRouter) -> None:
        """The portal serves only the vendor's current snapshot, so a historical
        window cannot be honoured. NOT the backfill guard — D-35 is that."""
        _wire_happy_path(router)
        end = datetime.now(UTC) - timedelta(hours=49)

        with pytest.raises(NesoHistoricalWindowError) as excinfo:
            _fetch(_source_config(), end - timedelta(hours=24), end)

        assert "neso_data_portal" in str(excinfo.value)
        assert len(router.calls) == 0

    def test_end_47_hours_stale_still_proceeds(self, router: respx.MockRouter) -> None:
        """``--end 2026-08-16`` parses to midnight UTC, so a legitimate
        "yesterday to today" window can end ~24 h behind the wall clock; 48 h
        clears that with margin. The tolerance is deliberately not tightened."""
        _wire_happy_path(router)
        end = datetime.now(UTC) - timedelta(hours=47)

        responses = _fetch(_source_config(), end - timedelta(hours=1), end)

        assert len(responses) == 1

    def test_span_of_eight_days_is_refused_naming_the_span_and_the_bound(
        self, router: respx.MockRouter
    ) -> None:
        _wire_happy_path(router)
        end = datetime.now(UTC)

        with pytest.raises(NesoWindowTooLongError) as excinfo:
            _fetch(_source_config(), end - timedelta(days=8), end)

        message = str(excinfo.value)
        assert "8 days" in message
        assert "7 days" in message
        assert len(router.calls) == 0

    def test_span_of_six_days_proceeds(self, router: respx.MockRouter) -> None:
        """The bound refuses exactly the windows no automated path can produce,
        and no others — both sides of 168 h are exercised."""
        _wire_happy_path(router)
        end = datetime.now(UTC)

        responses = _fetch(_source_config(), end - timedelta(days=6), end)

        assert len(responses) == 1

    def test_sol_case_a_2020_start_is_refused(self, router: respx.MockRouter) -> None:
        """``--start 2020-01-01 --end <now>`` — ~2,400 days."""
        _wire_happy_path(router)

        with pytest.raises(NesoWindowTooLongError):
            _fetch(
                _source_config(),
                datetime(2020, 1, 1, tzinfo=UTC),
                datetime.now(UTC),
            )

        assert len(router.calls) == 0

    def test_span_over_max_query_days_but_within_168h_proceeds_with_one_warning(
        self, router: respx.MockRouter, caplog: pytest.LogCaptureFixture
    ) -> None:
        """The ~4-day ``--incremental`` shape. The request is HONOURED, and what
        was honoured is said out loud rather than reinterpreted in silence.

        A span refusal here would false-refuse an ordinary legitimate command on
        every post-first-run ``--incremental`` invocation.
        """
        _wire_happy_path(router)
        end = datetime.now(UTC)

        with caplog.at_level(logging.WARNING, logger="gridflow.connectors.neso_data_portal.client"):
            responses = _fetch(_source_config(max_query_days=1), end - timedelta(days=4), end)

        assert len(responses) == 1, "the window is not a selector; the fetch is not refused"
        warnings = [
            record
            for record in caplog.records
            if record.levelno == logging.WARNING and "max_query_days" in record.getMessage()
        ]
        assert len(warnings) == 1, f"expected exactly one reinterpretation WARNING, got {warnings}"
        message = warnings[0].getMessage()
        assert "4 days" in message
        assert "1" in message
        assert "one" in message.lower() or "single" in message.lower()

    def test_max_ingest_window_is_coupled_to_the_declared_pipeline_ceiling(self) -> None:
        """The bound is not this plan's taste: it is the widest window
        ``run_ingest`` itself can ever resolve, so widening the declared ceiling
        fails HERE rather than silently turning check 4 into a false refusal.

        Read from ``model_fields[...].default`` and never from a constructed
        ``PipelineSettings()``, which reads env vars and ``.env`` and would make
        the assertion pass or fail on a local environment — the CI-parity
        failure class.
        """
        declared_default = PipelineSettings.model_fields["max_incremental_lookback_hours"].default

        assert timedelta(hours=declared_default) <= _MAX_INGEST_WINDOW


class TestResolverStub:
    """No real name resolution leaves the process in the default suite."""

    def test_the_resolver_stub_is_actually_consulted(
        self, router: respx.MockRouter, stub_neso_resolver: list[tuple[str, int]]
    ) -> None:
        """A refactor that bypasses the named helper would otherwise
        reintroduce real DNS while this suite stayed green."""
        _wire_happy_path(router)
        end = datetime.now(UTC)

        _fetch(_source_config(), end - timedelta(hours=24), end)

        assert stub_neso_resolver, "no send validated its target through the resolver helper"
        assert [host for host, _ in stub_neso_resolver] == ["api.neso.energy"] * len(
            stub_neso_resolver
        )
        assert len(stub_neso_resolver) == len(router.calls), (
            "every send must resolve its target — one lookup per send"
        )


# ---------------------------------------------------------------------------
# T-04: connector behaviour — the primitive, the policy, and the ladder
# ---------------------------------------------------------------------------

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal"

# The real presigned shape from `_probe/sample_historic-generation-mix.headers`,
# trimmed. `X-Amz-SignedHeaders=host` is why the query must arrive
# byte-identical: any re-encoding or re-ordering invalidates the signature.
FILE_HOST = "https://83025b28472d6aa2bf5ae59f3724aa78.eu.r2.cloudflarestorage.com"
FILE_PATH = f"{FILE_HOST}/dx-national-grid/national-grid/resources/x/windavailability.csv"
PRESIGNED_QUERY = (
    "?response-content-disposition=attachment%3B%20filename%3Dwindavailability.csv"
    "&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expires=604800"
    "&X-Amz-SignedHeaders=host&X-Amz-Date=20260816T185401Z"
    "&X-Amz-Signature=b4f0da8dbcf4e5c46e06a16556fcc90257a632b4684f5d6d4c4d0da7565bceef"
)
PRESIGNED_URL = FILE_PATH + PRESIGNED_QUERY

# The three real Set-Cookie headers the 302 carries. They are domain-scoped to
# api.neso.energy, so they must not cross to the file host.
REDIRECT_COOKIES = [
    ("set-cookie", "token=bcd152a46575b1f173c240ae634c32367e06bf14; Path=/; secure; HttpOnly"),
    ("set-cookie", "token-fresh=1; Max-Age=600; Path=/; secure; HttpOnly"),
    ("set-cookie", "ckan=b32b304ccf1bcefab4967c9d467aa892; Domain=api.neso.energy; Path=/"),
]


def _fixture(name: str) -> dict[str, Any]:
    """Load a trimmed ``package_show`` capture."""
    return json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _now_window() -> tuple[datetime, datetime]:
    end = datetime.now(UTC)
    return end - timedelta(hours=24), end


def _run_fetch(
    config: SourceConfig,
    dataset: str = DATASET,
    *,
    sink: list[NesoDataPortalConnector] | None = None,
) -> list[Any]:
    """Drive ``fetch()`` over a ≈now window, capturing the live connector.

    The connector is appended to ``sink`` BEFORE ``fetch()`` runs, so a test can
    still interrogate ``_issued_send_tokens`` on a fetch that raises.
    """
    start, end = _now_window()

    async def _run() -> list[Any]:
        async with NesoDataPortalConnector(config) as connector:
            if sink is not None:
                sink.append(connector)
            return await connector.fetch(dataset, start, end)

    return asyncio.run(_run())


def _wire_package_show(
    router: respx.MockRouter,
    payload: dict[str, Any] | None = None,
) -> None:
    router.get(url__startswith=PACKAGE_SHOW_URL).mock(
        return_value=httpx.Response(
            200, json=payload or _fixture("package_show_daily_wind_availability.json")
        )
    )


def _wire_redirect_download(
    router: respx.MockRouter,
    *,
    location: str = PRESIGNED_URL,
    file_response: httpx.Response | None = None,
    with_cookies: bool = False,
    redirect_status: int = 302,
) -> None:
    """Route the redirector 302 and the file-host leg of a download."""
    headers = [("location", location)]
    if with_cookies:
        headers.extend(REDIRECT_COOKIES)
    router.get(url__startswith=DAILY_WIND_RESOURCE_URL).mock(
        return_value=httpx.Response(
            redirect_status, headers=headers, content=b"<html>redirecting</html>"
        )
    )
    router.get(url__startswith=FILE_HOST).mock(
        return_value=file_response or httpx.Response(200, content=DAILY_WIND_CSV)
    )


class ChunkStream(httpx.AsyncByteStream):
    """A streamed body that records how much of itself was actually pulled.

    That counter is the only honest way to assert the connector aborted
    mid-stream rather than buffering the whole body and checking afterwards —
    which is the difference between a memory bound and a post-hoc complaint.
    """

    def __init__(self, chunks: list[bytes], *, raise_at: int | None = None) -> None:
        self.chunks = chunks
        self.pulled = 0
        self.raise_at = raise_at

    async def __aiter__(self) -> AsyncIterator[bytes]:
        for index, chunk in enumerate(self.chunks):
            if self.raise_at is not None and index == self.raise_at:
                raise httpx.RemoteProtocolError("peer closed before the body completed")
            self.pulled += 1
            yield chunk


def _add_catch_all(router: respx.MockRouter) -> None:
    """Register the catch-all LAST, after every specific route.

    respx resolves routes in registration order, so a catch-all added first
    would swallow every specific route and the proof would run against an empty
    payload — green, and proving nothing.

    Its job: an unexpected request must be **recorded** rather than raise an
    unrouted-request error. That distinction is what makes the positive control
    work — a rogue request has to show up in the recorded list as unmarked
    evidence, not blow up as a mock-configuration failure someone later "fixes"
    by adding a route.
    """
    router.route(url__regex=r".*").mock(
        return_value=httpx.Response(200, json={"success": True, "result": {}})
    )


@pytest.fixture
def no_retry_backoff() -> Iterator[None]:
    """Neutralise tenacity's wait so retry cases do not sleep.

    ``RETRY_POLICY`` is ``wait_random_exponential``, so an un-neutralised retry
    test costs seconds of real sleep. Restored afterwards because the
    ``Retrying`` object is shared class state.
    """
    retrying = NesoDataPortalConnector._send.retry
    original = retrying.wait
    retrying.wait = wait_none()
    try:
        yield
    finally:
        retrying.wait = original


@pytest.fixture
def raw_response_spy(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Record every ``RawResponse`` construction on the fetch path.

    "It raised" is not the same claim as "nothing was admitted to bronze". The
    admission-ladder tests assert the second.
    """
    constructed: list[dict[str, Any]] = []
    real = client_module.RawResponse

    def _spy(*args: Any, **kwargs: Any) -> Any:
        constructed.append(kwargs)
        return real(*args, **kwargs)

    monkeypatch.setattr(client_module, "RawResponse", _spy)
    return constructed


@pytest.fixture
def sent_responses(monkeypatch: pytest.MonkeyPatch) -> list[httpx.Response]:
    """Record the actual ``Response`` objects ``_send`` handed to its callers.

    Lifecycle must be observed on THESE objects, not on ``router.calls[i]
    .response``: respx's recorded response is the route's template, a different
    instance from the one the client constructs, and for a streamed route its
    ``is_closed`` stays ``False`` no matter what the connector does. Asserting
    on the template would have reported a leak that does not exist — and would
    equally have missed a real one.
    """
    captured: list[httpx.Response] = []
    real_send = NesoDataPortalConnector._send

    async def _spy(
        self: NesoDataPortalConnector,
        request: httpx.Request,
        target: Any,
        *,
        stream: bool = False,
    ) -> httpx.Response:
        response = await real_send(self, request, target, stream=stream)
        captured.append(response)
        return response

    monkeypatch.setattr(NesoDataPortalConnector, "_send", _spy)
    return captured


def _stub_addresses(monkeypatch: pytest.MonkeyPatch, *addresses: str) -> None:
    """Override the module resolver with a specific DNS answer."""

    async def _stub(host: str, port: int) -> list[Any]:
        return [ipaddress.ip_address(value) for value in addresses]

    monkeypatch.setattr(client_module, "_resolve_host_addresses", _stub)


def _request_kind(request: httpx.Request) -> str:
    """Classify a recorded request into one of D-39 §1a's five inventory kinds."""
    url = str(request.url)
    if "/api/3/action/package_show" in url:
        return "package_show"
    if "/api/3/action/package_search" in url:
        return "package_search"
    if "/api/3/action/package_list" in url:
        return "package_list"
    if url.startswith(DAILY_WIND_RESOURCE_URL):
        return "resource_url"
    if url.startswith(FILE_HOST):
        return "redirect_hop"
    return "unclassified"


class TestResourceSelection:
    """D-04: an EXACT ``resources[].name`` match, and nothing else."""

    @pytest.mark.parametrize(
        ("fixture_name", "dataset"),
        [
            ("package_show_daily_wind_availability.json", "daily_wind_availability"),
            ("package_show_historic_generation_mix.json", "historic_generation_mix"),
            (
                "package_show_embedded_wind_and_solar_forecasts.json",
                "embedded_wind_solar_forecast",
            ),
        ],
    )
    def test_exact_name_selection_finds_the_right_resource(
        self, fixture_name: str, dataset: str
    ) -> None:
        payload = _fixture(fixture_name)["result"]
        spec = DATASETS[dataset]
        connector = NesoDataPortalConnector(_source_config())

        resource, _redirector = connector._select_resource(payload, spec, dataset)

        assert resource["name"] == spec.resource_name
        expected = next(r for r in payload["resources"] if r["name"] == spec.resource_name)
        assert resource["id"] == expected["id"]

    def test_every_archive_sibling_in_the_embedded_package_is_rejected(self) -> None:
        """The embedded package carries 11 resources; exactly one is current.

        Two of the rejected ten are ``url_type: datastore`` 2026 archives, whose
        URLs are ``/datastore/dump/<id>`` — the path D-05 bars outright. A
        substring or "most recent" selector would pick one of these.
        """
        payload = _fixture("package_show_embedded_wind_and_solar_forecasts.json")["result"]
        spec = DATASETS["embedded_wind_solar_forecast"]
        connector = NesoDataPortalConnector(_source_config())

        resource, _redirector = connector._select_resource(
            payload, spec, "embedded_wind_solar_forecast"
        )

        assert len(payload["resources"]) == 11
        rejected = [r for r in payload["resources"] if r["id"] != resource["id"]]
        assert len(rejected) == 10
        assert all("Archive" in r["name"] or r["format"] != "CSV" for r in rejected)

        datastore_archives = [r for r in rejected if r.get("url_type") == "datastore"]
        assert len(datastore_archives) == 2, "the two 2026 datastore archives must be present"
        assert all("/datastore/dump/" in r["url"] for r in datastore_archives)
        assert "/datastore/dump/" not in resource["url"], "D-05: never the datastore path"

    def test_a_renamed_resource_raises_listing_the_actual_names(self) -> None:
        payload = _fixture("package_show_daily_wind_availability.json")["result"]
        payload["resources"][0]["name"] = "Wind Availability (Daily)"
        spec = DATASETS["daily_wind_availability"]
        connector = NesoDataPortalConnector(_source_config())

        with pytest.raises(NesoResourceSelectionError) as excinfo:
            connector._select_resource(payload, spec, "daily_wind_availability")

        message = str(excinfo.value)
        assert "Daily Wind Availability" in message, "the expected name must be named"
        assert "Wind Availability (Daily)" in message, "the ACTUAL names must be listed"

    def test_a_duplicated_resource_name_raises(self) -> None:
        """Two matches is as much a failure as none: picking either silently
        would make the capture depend on CKAN's list ordering."""
        payload = _fixture("package_show_daily_wind_availability.json")["result"]
        payload["resources"].append(dict(payload["resources"][0]))
        spec = DATASETS["daily_wind_availability"]
        connector = NesoDataPortalConnector(_source_config())

        with pytest.raises(NesoResourceSelectionError) as excinfo:
            connector._select_resource(payload, spec, "daily_wind_availability")

        assert "found 2" in str(excinfo.value)

    def test_a_non_csv_format_raises(self) -> None:
        """D-10: ``content_type`` is stamped from this field, so an unverified
        format would put a mislabelled body into immutable bronze."""
        payload = _fixture("package_show_daily_wind_availability.json")["result"]
        payload["resources"][0]["format"] = "XLSX"
        spec = DATASETS["daily_wind_availability"]
        connector = NesoDataPortalConnector(_source_config())

        with pytest.raises(NesoResourceSelectionError) as excinfo:
            connector._select_resource(payload, spec, "daily_wind_availability")

        assert "XLSX" in str(excinfo.value)

    def test_a_success_false_envelope_raises_on_the_envelope_not_the_status(
        self, router: respx.MockRouter
    ) -> None:
        """NESO returns action errors as HTTP **200** with ``success: false``.

        A status-only check would treat the error envelope as a payload and the
        failure would surface much later as a bewildering selection error.
        """
        router.get(url__startswith=PACKAGE_SHOW_URL).mock(
            return_value=httpx.Response(
                200, json={"success": False, "error": {"message": "Not found"}}
            )
        )

        with pytest.raises(CkanActionError) as excinfo:
            _run_fetch(_source_config())

        assert router.calls[0].response.status_code == 200, "the vendor DID answer 200"
        assert "success=false" in str(excinfo.value)


class TestTransportMarkerProof:
    """D-39 §1b: the single-primitive invariant, proven at the transport.

    Not by matching source text. The set of syntactic forms that can send is
    open-ended — a module-level ``httpx.get``, a second ``AsyncClient``, an
    alias — and the set of requests that reach the transport is not. respx
    patches httpx's transport globally, so a bypass is caught by its EFFECT.
    """

    def _drive_all_five_kinds(
        self, router: respx.MockRouter
    ) -> tuple[NesoDataPortalConnector, list[httpx.Request]]:
        _wire_package_show(router)
        _wire_redirect_download(router)
        _wire_catalog(router, [["alpha", "beta"]])
        _add_catch_all(router)

        sink: list[NesoDataPortalConnector] = []
        start, end = _now_window()

        async def _run() -> None:
            async with NesoDataPortalConnector(_source_config()) as connector:
                sink.append(connector)
                await connector.fetch(DATASET, start, end)
                await connector.discover_catalog()

        asyncio.run(_run())
        return sink[0], [call.request for call in router.calls]

    def test_every_request_reaching_the_transport_carries_a_live_token(
        self, router: respx.MockRouter
    ) -> None:
        """Assertion (1): EVERY recorded request, iterated — not the ones we
        expected. The observer CONSUMES each token, so a replayed request fails
        on its second appearance exactly as an absent one fails on its first.
        """
        connector, recorded = self._drive_all_five_kinds(router)
        issued = connector._issued_send_tokens

        assert recorded, "no traffic was recorded at all"
        for request in recorded:
            token = request.extensions.get(_VALIDATED_MARKER)
            assert token is not None, f"unmarked request reached the transport: {request.url}"
            assert token in issued, (
                f"request to {request.url} carried a token the connector never issued "
                "(or one already consumed — tokens are single-use)"
            )
            issued.remove(token)

        assert issued == set(), (
            "the connector issued tokens for sends that never reached the transport"
        )

    def test_the_recorded_traffic_covers_all_five_inventory_kinds(
        self, router: respx.MockRouter
    ) -> None:
        """Assertion (2): the coverage claim the proof rests on is itself
        asserted. A stated-completeness claim that is off by one is how this
        family decays — revision 6 said "four" and omitted ``package_list``.
        """
        _connector, recorded = self._drive_all_five_kinds(router)

        kinds = {_request_kind(request) for request in recorded}

        assert kinds == {
            "package_show",
            "resource_url",
            "redirect_hop",
            "package_search",
            "package_list",
        }, f"observed kinds {sorted(kinds)}"

    def test_positive_control_an_unmarked_client_is_detected(
        self, router: respx.MockRouter
    ) -> None:
        """Assertion (3): a SEPARATE phase over a SEPARATE list.

        The control must not contaminate assertion (1)'s list, or it would force
        an ad-hoc URL exemption inside the very property being proven — which is
        the hole, not the proof. So: complete (1) over a snapshot, then issue an
        unmarked request and inspect only what came after.
        """
        connector, recorded = self._drive_all_five_kinds(router)
        already_seen = len(recorded)
        assert all(request.extensions.get(_VALIDATED_MARKER) is not None for request in recorded)

        async def _rogue() -> None:
            async with httpx.AsyncClient() as rogue_client:
                await rogue_client.get("https://api.neso.energy/api/3/action/package_list")

        asyncio.run(_rogue())

        new_calls = [call.request for call in router.calls][already_seen:]
        assert len(new_calls) == 1, "the rogue request was not intercepted at all"
        assert new_calls[0].extensions.get(_VALIDATED_MARKER) is None, (
            "the rogue request carried a marker — the mechanism cannot detect a bypass"
        )
        assert new_calls[0].extensions.get(_VALIDATED_MARKER) not in connector._issued_send_tokens

    def test_two_sends_yield_different_tokens(self, router: respx.MockRouter) -> None:
        """Assertion (4a): a per-session nonce would be replayable."""
        _connector, recorded = self._drive_all_five_kinds(router)

        tokens = [request.extensions[_VALIDATED_MARKER] for request in recorded]

        assert len(tokens) == len(set(tokens)), f"tokens repeated across sends: {tokens}"
        assert len(tokens) >= 2

    def test_a_replayed_request_is_rejected_because_its_token_was_consumed(
        self, router: respx.MockRouter
    ) -> None:
        """Assertion (4c): resending an already-sent ``Request`` object must not
        satisfy the observer.

        This is the failure the per-attempt design exists to close: a
        session-long nonce would still sit in ``extensions``, so a replay would
        pass validation-by-attestation while bypassing the real check.
        """
        connector, recorded = self._drive_all_five_kinds(router)
        already_seen = len(recorded)

        # The observer runs over the real traffic and CONSUMES each token, on
        # the connector's own set — not a local copy. Asserting against a copy
        # after removing from it is a tautology, not a test.
        for request in recorded:
            connector._issued_send_tokens.remove(request.extensions[_VALIDATED_MARKER])
        assert connector._issued_send_tokens == set()

        replayed = recorded[0]
        original_token = replayed.extensions[_VALIDATED_MARKER]

        async def _resend() -> None:
            async with httpx.AsyncClient() as replay_client:
                await replay_client.send(replayed)

        asyncio.run(_resend())

        new_calls = [call.request for call in router.calls][already_seen:]
        assert len(new_calls) == 1, "the replayed request was not intercepted"
        observed = new_calls[0].extensions.get(_VALIDATED_MARKER)
        assert observed == original_token, (
            "precondition: the replay must carry the ORIGINAL token — _send never "
            "touched it, so a session-long nonce would still be riding along here"
        )
        assert observed not in connector._issued_send_tokens, (
            "a replayed request satisfied the observer — the token was not single-use, "
            "so resending an already-sent Request would bypass validation and the throttle"
        )

    def test_the_package_contains_no_retired_streaming_constructs(self) -> None:
        """Two textual assertions whose job is D-09's RETIREMENT, not bypass
        detection — that is the marker proof's job.

        The streaming context-manager helper cannot coexist with the
        single-primitive invariant (it is ``build_request`` + ``send`` +
        ``aclose`` in a ``finally``, so it bypasses the throttle and returns a
        closed response), and an ``aiter_bytes`` count would compare DECODED
        bytes against an ENCODED ``Content-Length``.
        """
        package_dir = Path(client_module.__file__).parent

        for path in sorted(package_dir.rglob("*.py")):
            source = path.read_text(encoding="utf-8")
            assert "client.stream" not in source, f"{path.name} reintroduced the stream helper"
            assert "aiter_bytes" not in source, f"{path.name} reintroduced aiter_bytes"

    def test_assert_safe_target_has_exactly_one_production_call_site(self) -> None:
        """D-39 §1a. The one static check that survives, and deliberately NOT
        about network syntax.

        A second *production* call site is a regression, not an improvement — it
        would mean the guarantee had drifted back to being remembered at call
        sites, which is the defect two consecutive review passes each found.
        Scoped to production code because this test module calls the policy
        directly to exercise its SSRF cases, and that is not an enforcement site.
        """
        package_dir = Path(client_module.__file__).parent
        call_sites: list[tuple[str, str]] = []

        for path in sorted(package_dir.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                    continue
                for inner in ast.walk(node):
                    if (
                        isinstance(inner, ast.Call)
                        and isinstance(inner.func, ast.Attribute)
                        and inner.func.attr == "_assert_safe_target"
                    ):
                        call_sites.append((path.name, node.name))

        assert call_sites == [("client.py", "_send")], (
            f"_assert_safe_target must be called exactly once, from _send; found {call_sites}"
        )


class TestSendClassification:
    """D-39 §1: the gate is ``is_success``, NOT ``is_error``.

    A 304, or a 3xx carrying no ``Location``, is neither an error nor a
    redirect. An ``is_error`` gate would return one as though it were a body and
    the failure would surface later as a bewildering parse error.
    """

    def test_a_302_is_returned_not_raised_and_does_not_trip_the_retry_policy(
        self, router: respx.MockRouter, no_retry_backoff: None
    ) -> None:
        _wire_package_show(router)
        _wire_redirect_download(router)

        responses = _run_fetch(_source_config())

        assert len(responses) == 1
        hops = [c for c in router.calls if _request_kind(c.request) == "resource_url"]
        assert len(hops) == 1, (
            "the redirector was requested more than once — the 302 tripped RETRY_POLICY "
            "instead of being returned as a legitimate outcome"
        )

    @pytest.mark.parametrize(
        ("status", "headers", "case"),
        [
            (304, {}, "304 Not Modified"),
            (302, {}, "302 with no Location"),
        ],
    )
    def test_a_non_success_non_redirect_response_raises(
        self,
        router: respx.MockRouter,
        no_retry_backoff: None,
        raw_response_spy: list[dict[str, Any]],
        status: int,
        headers: dict[str, str],
        case: str,
    ) -> None:
        _wire_package_show(router)
        router.get(url__startswith=DAILY_WIND_RESOURCE_URL).mock(
            return_value=httpx.Response(status, headers=headers)
        )

        with pytest.raises(httpx.HTTPStatusError):
            _run_fetch(_source_config())

        assert raw_response_spy == [], f"{case}: a RawResponse was constructed anyway"

    def test_a_500_then_200_on_the_file_leg_succeeds(
        self, router: respx.MockRouter, no_retry_backoff: None
    ) -> None:
        """A returned 5xx enters RETRY_POLICY because status classification sits
        INSIDE the retry boundary — the repo's existing idiom."""
        _wire_package_show(router)
        router.get(url__startswith=DAILY_WIND_RESOURCE_URL).mock(
            side_effect=[
                httpx.Response(500, content=b"upstream error"),
                httpx.Response(200, content=DAILY_WIND_CSV),
            ]
        )

        responses = _run_fetch(_source_config())

        assert len(responses) == 1
        file_leg = [c for c in router.calls if _request_kind(c.request) == "resource_url"]
        assert len(file_leg) == 2, "expected exactly one retry of the file leg"

    def test_a_403_on_the_final_response_raises_and_admits_nothing(
        self,
        router: respx.MockRouter,
        no_retry_backoff: None,
        raw_response_spy: list[dict[str, Any]],
    ) -> None:
        """The presigned signature expired or was rejected."""
        _wire_package_show(router)
        _wire_redirect_download(
            router, file_response=httpx.Response(403, content=b"SignatureDoesNotMatch")
        )

        with pytest.raises(httpx.HTTPStatusError):
            _run_fetch(_source_config())

        assert raw_response_spy == []


class TestResponseLifecycle:
    """D-39 §1: ``_send`` never closes a response it returns — the CALLER owns
    the lifecycle, in a ``finally``.

    Without that, the 302's own chunked ``text/html`` body — which nobody reads
    — leaks its connection on every single fetch.
    """

    def test_a_followed_302_is_closed_even_though_its_body_was_never_read(
        self, router: respx.MockRouter, sent_responses: list[httpx.Response]
    ) -> None:
        """The 302's own body is a chunked ``text/html`` payload nobody reads.
        Without an explicit close a streamed 3xx leaks its connection."""
        _wire_package_show(router)
        _wire_redirect_download(router)

        _run_fetch(_source_config())

        redirect = next(r for r in sent_responses if r.has_redirect_location)
        assert redirect.is_closed, "the unread 302 leaked its connection"
        assert all(response.is_closed for response in sent_responses), (
            "every response the primitive returned must be closed by its caller"
        )

    def test_a_cap_exceeded_abort_closes_the_response(
        self, router: respx.MockRouter, sent_responses: list[httpx.Response]
    ) -> None:
        cap = DATASETS[DATASET].max_download_bytes
        chunk = b"x" * 65536
        stream = ChunkStream([chunk] * (cap // 65536 + 8))
        _wire_package_show(router)
        _wire_redirect_download(router, file_response=httpx.Response(200, stream=stream))

        with pytest.raises(NesoResponseTooLargeError):
            _run_fetch(_source_config())

        # The cap really was what fired, not the stream simply ending.
        assert stream.pulled < len(stream.chunks)
        assert sent_responses, "no response was ever returned by the primitive"
        assert all(response.is_closed for response in sent_responses), (
            "an aborted download left its response open"
        )

    def test_a_fully_read_body_is_closed_and_a_second_close_is_a_no_op(
        self, router: respx.MockRouter, sent_responses: list[httpx.Response]
    ) -> None:
        """``Response.aclose()`` is idempotent, which is what lets the rule be
        unconditional rather than "close it unless you already did"."""
        _wire_package_show(router)
        _wire_redirect_download(router)

        _run_fetch(_source_config())

        body_response = sent_responses[-1]
        assert not body_response.has_redirect_location
        assert body_response.is_closed

        asyncio.run(body_response.aclose())
        assert body_response.is_closed, "a second aclose() must remain a harmless no-op"


class TestResourceUrlShape:
    """D-11: ``resources[].url`` must be the stable NESO redirector.

    That decision exists so the presigned target's ``X-Amz-Signature`` and its
    7-day expiry never reach the bronze sidecar. But the field is
    vendor-controlled, so the shape has to be VERIFIED rather than assumed: if
    CKAN ever returned an already-resolved presigned URL, the connector would
    copy it straight into an **immutable** provenance file, where it cannot be
    cleaned up afterwards.
    """

    def _fetch_with_resource_url(self, router: respx.MockRouter, url: str) -> None:
        payload = _fixture("package_show_daily_wind_availability.json")
        payload["result"]["resources"][0]["url"] = url
        _wire_package_show(router, payload)
        _add_catch_all(router)
        _run_fetch(_source_config())

    def test_an_already_resolved_presigned_url_is_refused(
        self, router: respx.MockRouter, raw_response_spy: list[dict[str, Any]]
    ) -> None:
        """The finding this check exists for: a signature in immutable bronze."""
        with pytest.raises(NesoUnexpectedResourceUrlError) as excinfo:
            self._fetch_with_resource_url(router, PRESIGNED_URL)

        message = str(excinfo.value)
        signature = "b4f0da8dbcf4e5c46e06a16556fcc90257a632b4684f5d6d4c4d0da7565bceef"
        assert signature not in message, (
            "the refusal echoed the signature VALUE it exists to protect"
        )
        assert "X-Amz-Credential" not in message, "the refusal echoed the credential id"
        assert "?" not in message.split("resource url ")[1].split("'")[1], (
            "the reported URL still carried its query string"
        )
        assert "query string" in message
        assert raw_response_spy == [], "a RawResponse carrying the signature was built"
        assert [c for c in router.calls if _request_kind(c.request) != "package_show"] == []

    @pytest.mark.parametrize(
        ("url", "case"),
        [
            (
                f"{BASE_URL}/dataset/pkg/resource/{DAILY_WIND_RESOURCE_ID}"
                "/download/windavailability.csv?X-Amz-Signature=deadbeef",
                "redirector shape but query-bearing",
            ),
            (
                "https://evil.example/dataset/pkg/resource/"
                f"{DAILY_WIND_RESOURCE_ID}/download/windavailability.csv",
                "foreign host",
            ),
            (
                f"http://api.neso.energy/dataset/pkg/resource/{DAILY_WIND_RESOURCE_ID}"
                "/download/windavailability.csv",
                "plain http",
            ),
            (f"{BASE_URL}/datastore/dump/{DAILY_WIND_RESOURCE_ID}", "datastore dump path"),
            (
                f"https://api.neso.energy:8443/anything/resource/{DAILY_WIND_RESOURCE_ID}"
                "/download/file.csv#access_token=secret",
                "Sol's case: off-port, off-path, fragment-bearing",
            ),
            (
                f"https://api.neso.energy:8443/dataset/{DAILY_WIND_PACKAGE_ID}/resource/"
                f"{DAILY_WIND_RESOURCE_ID}/download/windavailability.csv",
                "unexpected port",
            ),
            (
                DAILY_WIND_RESOURCE_URL + "#access_token=secret",
                "fragment on an otherwise valid redirector",
            ),
            (
                f"{BASE_URL}/prefix/dataset/{DAILY_WIND_PACKAGE_ID}/resource/"
                f"{DAILY_WIND_RESOURCE_ID}/download/windavailability.csv",
                "extra leading path segment (substring match would pass)",
            ),
            (
                f"{BASE_URL}/dataset/{DAILY_WIND_PACKAGE_ID}/resource/"
                f"{DAILY_WIND_RESOURCE_ID}/download/",
                "empty filename segment",
            ),
            (f"{BASE_URL}/x.csv", "arbitrary path"),
            (
                f"{BASE_URL}/dataset/pkg/resource/some-other-id/download/windavailability.csv",
                "path names a different resource id",
            ),
        ],
    )
    def test_a_non_redirector_url_is_refused_before_any_download(
        self,
        router: respx.MockRouter,
        raw_response_spy: list[dict[str, Any]],
        url: str,
        case: str,
    ) -> None:
        with pytest.raises(NesoUnexpectedResourceUrlError):
            self._fetch_with_resource_url(router, url)

        downloads = [c for c in router.calls if _request_kind(c.request) != "package_show"]
        assert downloads == [], f"{case}: a request was sent anyway"
        assert raw_response_spy == [], f"{case}: a RawResponse was constructed"

    def test_a_resource_url_carrying_userinfo_is_refused(
        self, router: respx.MockRouter, raw_response_spy: list[dict[str, Any]]
    ) -> None:
        """httpx would turn ``user:pass@`` into Basic credentials for that host.

        Owned by the SHAPE guard rather than by the target policy: the
        redirector simply never carries userinfo, so this is a contract
        violation before it is a routing question. ``_assert_safe_target``'s own
        userinfo rule stays exercised through the redirect-``Location`` path in
        :class:`TestRedirectPolicy`, so nothing lost coverage in the move.
        """
        payload = _fixture("package_show_daily_wind_availability.json")
        payload["result"]["resources"][0]["url"] = DAILY_WIND_RESOURCE_URL.replace(
            "https://", "https://user:pass@"
        )
        _wire_package_show(router, payload)
        _add_catch_all(router)

        with pytest.raises(NesoUnexpectedResourceUrlError) as excinfo:
            _run_fetch(_source_config())

        assert "user:pass" not in str(excinfo.value), "the refusal echoed the credentials"
        downloads = [c for c in router.calls if _request_kind(c.request) != "package_show"]
        assert downloads == []
        assert raw_response_spy == []

    def test_the_real_redirector_shape_is_accepted(self, router: respx.MockRouter) -> None:
        """The positive control: the check must not refuse the real thing.

        A shape guard that rejected the genuine vendor URL would be caught by
        every other test in this module, but stating it here keeps the refusal
        cases from being trivially satisfiable by a guard that refuses always.
        """
        _wire_package_show(router)
        _wire_redirect_download(router)

        (raw,) = _run_fetch(_source_config())

        assert raw.request_url == DAILY_WIND_RESOURCE_URL


class TestInitialUrlValidation:
    """D-39 §1a: the vendor-supplied ``resources[].url`` gets exactly the
    guarantee the redirect hops get, because both go through ``_send``.

    Revision 5 sent this URL unvalidated because validation was wired into the
    redirect step rather than into sending. A poisoned catalogue entry is the
    same SSRF vector as a poisoned ``Location``.

    **These cases are deliberately SHAPE-VALID.** D-11's shape check now runs
    first and would otherwise mask them, and the two guards answer different
    questions: shape asks "is this the redirector we contracted for", the
    target policy asks "where does it actually resolve". A URL can satisfy the
    first and fail the second — DNS is not part of a URL's shape — which is
    exactly what these tests exercise.

    The resolver is driven **per lookup order** rather than per host, because
    the shape check forces the resource URL onto our own host: answering by
    host would make ``package_show``'s own send fail first and the test would
    pass without ever validating the resource URL. (That is not hypothetical —
    it is how the first version of this class passed.)
    """

    def _resolver_good_then(self, monkeypatch: pytest.MonkeyPatch, *later_addresses: str) -> None:
        """First lookup (``package_show``) resolves publicly; the next — the
        resource URL's own send — resolves to ``later_addresses``."""
        lookups = {"n": 0}

        async def _stub(host: str, port: int) -> list[Any]:
            lookups["n"] += 1
            if lookups["n"] == 1:
                return [ipaddress.ip_address("93.184.216.34")]
            return [ipaddress.ip_address(value) for value in later_addresses]

        monkeypatch.setattr(client_module, "_resolve_host_addresses", _stub)

    @pytest.mark.parametrize(
        ("addresses", "case"),
        [
            (("127.0.0.1",), "loopback"),
            (("10.0.0.7",), "RFC-1918"),
            (("93.184.216.34", "192.168.1.5"), "MIXED public+private answer"),
            ((), "empty DNS answer"),
        ],
    )
    def test_a_resource_url_resolving_somewhere_unsafe_is_refused(
        self,
        router: respx.MockRouter,
        monkeypatch: pytest.MonkeyPatch,
        raw_response_spy: list[dict[str, Any]],
        addresses: tuple[str, ...],
        case: str,
    ) -> None:
        _wire_package_show(router)
        _add_catch_all(router)
        self._resolver_good_then(monkeypatch, *addresses)

        with pytest.raises(NesoUnsafeRedirectError):
            _run_fetch(_source_config())

        downloads = [c for c in router.calls if _request_kind(c.request) != "package_show"]
        assert downloads == [], f"{case}: a request reached the rejected target"
        assert raw_response_spy == [], f"{case}: a RawResponse was constructed"


class TestRedirectPolicy:
    """D-08: manual redirects, one VALIDATED hop at a time."""

    def test_a_cross_host_302_is_followed_and_the_presigned_query_is_byte_identical(
        self, router: respx.MockRouter
    ) -> None:
        """``X-Amz-SignedHeaders=host`` means any query normalisation,
        re-ordering or re-encoding invalidates the signature."""
        _wire_package_show(router)
        _wire_redirect_download(router)

        _run_fetch(_source_config())

        hops = [c for c in router.calls if _request_kind(c.request) == "redirect_hop"]
        assert len(hops) == 1
        assert str(hops[0].request.url) == PRESIGNED_URL, (
            "the presigned target was not sent verbatim"
        )

    def test_a_relative_location_resolves_against_the_sending_host(
        self, router: respx.MockRouter
    ) -> None:
        """Left unresolved it would either be rejected as schemeless or, worse,
        joined against ``base_url`` and sent to the wrong host."""
        _wire_package_show(router)
        router.get(url__startswith=DAILY_WIND_RESOURCE_URL).mock(
            return_value=httpx.Response(302, headers={"location": "/files/relative.csv"})
        )
        router.get(url__startswith=f"{BASE_URL}/files/relative.csv").mock(
            return_value=httpx.Response(200, content=DAILY_WIND_CSV)
        )

        _run_fetch(_source_config())

        followed = [
            str(c.request.url) for c in router.calls if "/files/relative.csv" in str(c.request.url)
        ]
        assert followed == [f"{BASE_URL}/files/relative.csv"]

    @pytest.mark.parametrize(
        ("location", "addresses", "case"),
        [
            ("http://elsewhere.example/x.csv", ("93.184.216.34",), "http scheme"),
            ("https://user:pass@elsewhere.example/x.csv", ("93.184.216.34",), "userinfo"),
            ("https://elsewhere.example/x.csv", ("127.0.0.1",), "loopback"),
            ("https://elsewhere.example/x.csv", ("172.16.4.9",), "RFC-1918"),
            (
                "https://elsewhere.example/x.csv",
                ("93.184.216.34", "192.168.1.5"),
                "MIXED public+private answer",
            ),
        ],
    )
    def test_an_unsafe_redirect_target_is_refused_before_the_next_send(
        self,
        router: respx.MockRouter,
        monkeypatch: pytest.MonkeyPatch,
        location: str,
        addresses: tuple[str, ...],
        case: str,
    ) -> None:
        """The mixed-answer case is the one an "ANY address is global"
        implementation passes while httpx connects to the private one."""
        _wire_package_show(router)
        router.get(url__startswith=DAILY_WIND_RESOURCE_URL).mock(
            return_value=httpx.Response(302, headers={"location": location})
        )
        _add_catch_all(router)

        real_resolver = client_module._resolve_host_addresses

        async def _selective(host: str, port: int) -> list[Any]:
            if host == "api.neso.energy":
                return [ipaddress.ip_address("93.184.216.34")]
            return [ipaddress.ip_address(value) for value in addresses]

        monkeypatch.setattr(client_module, "_resolve_host_addresses", _selective)
        assert real_resolver is not _selective

        with pytest.raises(NesoUnsafeRedirectError):
            _run_fetch(_source_config())

        sent_elsewhere = [
            str(c.request.url) for c in router.calls if "elsewhere.example" in str(c.request.url)
        ]
        assert sent_elsewhere == [], f"{case}: a request reached the rejected target"

    def test_a_chain_longer_than_the_hop_cap_raises(self, router: respx.MockRouter) -> None:
        _wire_package_show(router)
        router.get(url__startswith=DAILY_WIND_RESOURCE_URL).mock(
            return_value=httpx.Response(302, headers={"location": f"{FILE_HOST}/hop1.csv"})
        )
        router.get(url__startswith=FILE_HOST).mock(
            return_value=httpx.Response(302, headers={"location": f"{FILE_HOST}/next.csv"})
        )

        with pytest.raises(client_module.NesoRedirectLoopError):
            _run_fetch(_source_config())

        hops = [c for c in router.calls if _request_kind(c.request) == "redirect_hop"]
        assert len(hops) == _MAX_REDIRECT_HOPS, (
            f"expected the loop to stop after {_MAX_REDIRECT_HOPS} hops, got {len(hops)}"
        )


class TestCrossHostHygiene:
    """D-39 §2: a FRESH request per hop, so nothing crosses the host boundary."""

    def test_no_cookie_from_the_redirector_reaches_the_file_host(
        self, router: respx.MockRouter
    ) -> None:
        """The 302 sets three real cookies on ``api.neso.energy``. They are
        domain-scoped, so ``build_request`` does not attach them to the file
        host — asserted rather than assumed."""
        _wire_package_show(router)
        _wire_redirect_download(router, with_cookies=True)

        _run_fetch(_source_config())

        hop = next(c for c in router.calls if _request_kind(c.request) == "redirect_hop")
        assert "cookie" not in {k.lower() for k in hop.request.headers}, (
            f"a cookie crossed to the file host: {dict(hop.request.headers)}"
        )

    def test_the_host_header_is_regenerated_for_the_file_host(
        self, router: respx.MockRouter
    ) -> None:
        _wire_package_show(router)
        _wire_redirect_download(router, with_cookies=True)

        _run_fetch(_source_config())

        hop = next(c for c in router.calls if _request_kind(c.request) == "redirect_hop")
        assert hop.request.headers["host"] == httpx.URL(PRESIGNED_URL).netloc.decode()
        assert hop.request.headers["host"] != "api.neso.energy"


class TestContentEncoding:
    """D-39 §3: identity, asked for AND enforced.

    Under any content coding ``Content-Length`` describes the ENCODED
    representation while a decoded read yields different bytes. Comparing those
    two would classify every compressed CSV as truncated. Rather than reconcile
    two counters, the coding is removed from the path.
    """

    def test_the_file_leg_asks_for_identity(self, router: respx.MockRouter) -> None:
        """httpx's default is ``gzip, deflate``. Adding this header cannot break
        the presigned signature: only ``Host`` is signed."""
        _wire_package_show(router)
        _wire_redirect_download(router)

        _run_fetch(_source_config())

        for kind in ("resource_url", "redirect_hop"):
            call = next(c for c in router.calls if _request_kind(c.request) == kind)
            assert call.request.headers["accept-encoding"] == "identity", kind

    def test_an_unexpected_content_encoding_raises_and_admits_nothing(
        self, router: respx.MockRouter, raw_response_spy: list[dict[str, Any]]
    ) -> None:
        """We asked; we do not guess what the vendor did instead.

        The mock uses ``stream=`` rather than ``content=`` because
        ``httpx.Response(content=..., headers={"Content-Encoding": "gzip"})``
        decodes EAGERLY at construction and would fail in the test's own setup.
        That is incidentally the same reason the connector reads with
        ``aiter_raw()``: the raw path never decodes, so the guard can inspect
        the declared coding instead of tripping over it.
        """
        stream = ChunkStream([DAILY_WIND_CSV])
        _wire_package_show(router)
        _wire_redirect_download(
            router,
            file_response=httpx.Response(200, stream=stream, headers={"Content-Encoding": "gzip"}),
        )

        with pytest.raises(NesoUnexpectedEncodingError) as excinfo:
            _run_fetch(_source_config())

        assert "gzip" in str(excinfo.value)
        assert stream.pulled == 0, "the guard must fire before a byte is read"
        assert raw_response_spy == []

    def test_an_explicit_identity_encoding_is_accepted(self, router: respx.MockRouter) -> None:
        _wire_package_show(router)
        _wire_redirect_download(
            router,
            file_response=httpx.Response(
                200, content=DAILY_WIND_CSV, headers={"Content-Encoding": "identity"}
            ),
        )

        assert len(_run_fetch(_source_config())) == 1


class TestSizeCap:
    """D-39 §4 / T-NDP-02: the vendor controls the body size and may omit or
    understate ``Content-Length``, so the running raw-byte count — not the
    header — is what actually bounds memory."""

    def test_a_declared_oversize_is_rejected_before_a_byte_is_read(
        self, router: respx.MockRouter
    ) -> None:
        stream = ChunkStream([b"x" * 1024])
        cap = DATASETS[DATASET].max_download_bytes
        _wire_package_show(router)
        _wire_redirect_download(
            router,
            file_response=httpx.Response(
                200, stream=stream, headers={"Content-Length": str(cap + 1)}
            ),
        )

        with pytest.raises(NesoResponseTooLargeError) as excinfo:
            _run_fetch(_source_config())

        assert stream.pulled == 0, "the body was read despite a declared oversize"
        assert str(cap) in str(excinfo.value)

    def test_an_undeclared_oversized_body_aborts_mid_stream(self, router: respx.MockRouter) -> None:
        """A chunked response carries no ``Content-Length``, so the fast reject
        cannot fire and the running total is the only bound."""
        cap = DATASETS[DATASET].max_download_bytes
        chunk = b"y" * 65536
        stream = ChunkStream([chunk] * (cap // 65536 + 8))
        _wire_package_show(router)
        _wire_redirect_download(router, file_response=httpx.Response(200, stream=stream))

        with pytest.raises(NesoResponseTooLargeError):
            _run_fetch(_source_config())

        assert stream.pulled < len(stream.chunks), (
            "the connector buffered the whole body instead of aborting mid-stream"
        )
        assert stream.pulled * len(chunk) <= cap + len(chunk), "peak memory exceeded cap+1 chunk"

    def test_an_understated_content_length_still_aborts_mid_stream(
        self, router: respx.MockRouter
    ) -> None:
        """A header under the cap passes the fast reject; the body is oversized
        anyway. This is why the running check is load-bearing on its own."""
        cap = DATASETS[DATASET].max_download_bytes
        chunk = b"z" * 65536
        stream = ChunkStream([chunk] * (cap // 65536 + 8))
        _wire_package_show(router)
        _wire_redirect_download(
            router,
            file_response=httpx.Response(200, stream=stream, headers={"Content-Length": "1024"}),
        )

        with pytest.raises(NesoResponseTooLargeError):
            _run_fetch(_source_config())

        assert stream.pulled < len(stream.chunks), "the whole body was buffered"


class TestTransportCompleteness:
    """D-39 §4-§5. Never inferred from a successful parse."""

    def test_a_body_short_of_its_declared_length_raises_naming_both_numbers(
        self, router: respx.MockRouter, raw_response_spy: list[dict[str, Any]]
    ) -> None:
        _wire_package_show(router)
        _wire_redirect_download(
            router,
            file_response=httpx.Response(
                200, content=DAILY_WIND_CSV, headers={"Content-Length": "999999"}
            ),
        )

        with pytest.raises(NesoTruncatedBodyError) as excinfo:
            _run_fetch(_source_config())

        message = str(excinfo.value)
        assert "999999" in message
        assert str(len(DAILY_WIND_CSV)) in message
        assert raw_response_spy == []

    def test_a_mid_stream_protocol_error_is_translated(
        self, router: respx.MockRouter, raw_response_spy: list[dict[str, Any]]
    ) -> None:
        """The peer closed before the declared body completed."""
        stream = ChunkStream([b"BMU_ID,Date,MW\n", b"T_A,2026-08-16,1\n"], raise_at=1)
        _wire_package_show(router)
        _wire_redirect_download(router, file_response=httpx.Response(200, stream=stream))

        with pytest.raises(NesoTruncatedBodyError):
            _run_fetch(_source_config())

        assert raw_response_spy == []

    def test_an_interrupted_body_read_is_not_retried(
        self, router: respx.MockRouter, no_retry_backoff: None
    ) -> None:
        """D-39 §5, stated rather than hidden: the body is consumed AFTER
        ``_send`` returns, so a mid-stream error falls outside its retry
        boundary. A retry there would re-enter at the file host on a presigned
        URL that may already have been consumed.
        """
        streams = [
            ChunkStream([b"BMU_ID,Date,MW\n", b"T_A,2026-08-16,1\n"], raise_at=1) for _ in range(5)
        ]
        _wire_package_show(router)
        router.get(url__startswith=DAILY_WIND_RESOURCE_URL).mock(
            side_effect=[httpx.Response(200, stream=s) for s in streams]
        )

        with pytest.raises(NesoTruncatedBodyError):
            _run_fetch(_source_config())

        attempts = [c for c in router.calls if _request_kind(c.request) == "resource_url"]
        assert len(attempts) == 1, f"the interrupted read was retried {len(attempts)} times"


class TestAdmissionLadder:
    """D-36 rung 3: a real parse, at fetch time, before immutable bronze.

    ``content_type`` is stamped ``text/csv`` from CKAN metadata (D-10), so
    without this rung a JSON error envelope, an HTML interstitial or a binary
    body would be written to bronze under a ``.csv`` name — where re-running
    cannot recover it.
    """

    def test_a_json_error_envelope_on_the_file_leg_is_refused(
        self, router: respx.MockRouter, raw_response_spy: list[dict[str, Any]]
    ) -> None:
        _wire_package_show(router)
        _wire_redirect_download(
            router,
            file_response=httpx.Response(200, content=b'{"error": "not available"}'),
        )

        with pytest.raises((NotCsvBodyError, CsvHeaderDriftError)):
            _run_fetch(_source_config())

        assert raw_response_spy == []

    def test_a_binary_body_is_refused(
        self, router: respx.MockRouter, raw_response_spy: list[dict[str, Any]]
    ) -> None:
        _wire_package_show(router)
        _wire_redirect_download(
            router,
            file_response=httpx.Response(200, content=b"\x89PNG\r\n\x1a\n\xff\xfe\x00binary"),
        )

        with pytest.raises(NotCsvBodyError):
            _run_fetch(_source_config())

        assert raw_response_spy == []

    def test_a_renamed_column_raises_and_is_not_retried(
        self, router: respx.MockRouter, raw_response_spy: list[dict[str, Any]]
    ) -> None:
        """Header drift is a vendor change, not a transient fault. Retrying it
        five times would be five pointless downloads — which is why this rung
        sits deliberately OUTSIDE the retry boundary."""
        _wire_package_show(router)
        _wire_redirect_download(
            router,
            file_response=httpx.Response(200, content=b"BMU_ID,Date,MEGAWATTS\nT_A,2026-08-16,1\n"),
        )

        with pytest.raises(CsvHeaderDriftError) as excinfo:
            _run_fetch(_source_config())

        assert "MEGAWATTS" in str(excinfo.value)
        assert raw_response_spy == []
        attempts = [c for c in router.calls if _request_kind(c.request) == "redirect_hop"]
        assert len(attempts) == 1, "a vendor schema change was retried"

    @pytest.mark.parametrize(
        ("body", "case"),
        [(b"", "empty body"), (b"BMU_ID,Date,MW\n", "header-only body")],
    )
    def test_a_rowless_resource_is_refused(
        self,
        router: respx.MockRouter,
        raw_response_spy: list[dict[str, Any]],
        body: bytes,
        case: str,
    ) -> None:
        """ADR-023 definitive-absent: ``record_count`` stays ``None`` and is
        never replaced by ``0``, and an empty capture never enters bronze."""
        _wire_package_show(router)
        _wire_redirect_download(router, file_response=httpx.Response(200, content=body))

        with pytest.raises(NesoEmptyResourceError):
            _run_fetch(_source_config())

        assert raw_response_spy == [], case


class TestRawResponseShape:
    """The one construction site, and what it stamps."""

    def test_the_constructed_raw_response_carries_the_d10_to_d14_contract(
        self, router: respx.MockRouter
    ) -> None:
        _wire_package_show(router)
        _wire_redirect_download(router)
        start, end = _now_window()

        async def _run() -> list[Any]:
            async with NesoDataPortalConnector(_source_config()) as connector:
                return await connector.fetch(DATASET, start, end)

        (raw,) = asyncio.run(_run())

        assert raw.content_type == "text/csv", "D-10: from CKAN metadata, not the header"
        assert raw.request_url == DAILY_WIND_RESOURCE_URL, "D-11: the redirector, never R2"
        assert "X-Amz-Signature" not in raw.request_url, (
            "D-11/T-NDP-03: a presigned signature must never reach an irreproducible sidecar"
        )
        assert raw.data_date == end.date(), "D-13: the resolved window end, not the wall clock"
        assert raw.record_count is None, "D-14: never replaced by 0"
        assert raw.body == DAILY_WIND_CSV

    def test_the_provenance_params_carry_exactly_the_d12_keys(
        self, router: respx.MockRouter
    ) -> None:
        _wire_package_show(router)
        _wire_redirect_download(router)

        (raw,) = _run_fetch(_source_config())

        assert set(raw.request_params) == {
            "package",
            "package_id",
            "resource_id",
            "resource_name",
            "resource_filename",
            "ckan_last_modified",
            "ckan_format",
            "body_sha256",
        }
        assert raw.request_params["package"] == "daily-wind-availability"
        assert raw.request_params["resource_filename"] == "windavailability.csv"
        assert raw.request_params["ckan_last_modified"] == "2026-08-16T18:20:11.953941"
        assert len(raw.request_params["body_sha256"]) == 64


# The prescribed measurement point for the pacing assertion is the transport
# (a respx side-effect hook), which sits AFTER the throttle releases. Per-send
# overhead therefore lands between the two clocks, and when send N carries more
# overhead than send N+1 the observed gap dips a hair under the interval — a
# measured 0.99941 s for a throttle that paced correctly, i.e. a FLAKY test
# rather than a real violation. This tolerance absorbs that jitter and nothing
# else: a MISSING throttle shows a gap near 0.000 s, so the assertion keeps all
# of its discriminating power. Mutation-checked by neutralising
# _throttle_request and confirming both tests below still fail.
_PACING_JITTER = 0.01


class TestThrottle:
    """D-07: the throttle gates EVERY outbound send without exception.

    "Send", not "logical request", is the unit deliberately — which is why
    redirects are handled manually, so each hop is a separate throttled send.
    """

    def test_every_adjacent_pair_of_sends_is_at_least_one_second_apart(
        self, router: respx.MockRouter
    ) -> None:
        """Pairwise across the FULL sequence, hops included. An aggregate
        elapsed-time assertion is not sufficient and must not be substituted: it
        passes when one gap is 0 s and another is 2 s.
        """
        sent_at: list[float] = []

        def _stamp(request: httpx.Request) -> httpx.Response:
            sent_at.append(monotonic())
            if "/api/3/action/package_show" in str(request.url):
                return httpx.Response(
                    200, json=_fixture("package_show_daily_wind_availability.json")
                )
            if str(request.url).startswith(DAILY_WIND_RESOURCE_URL):
                return httpx.Response(302, headers={"location": PRESIGNED_URL})
            return httpx.Response(200, content=DAILY_WIND_CSV)

        router.route(url__regex=r".*").mock(side_effect=_stamp)

        _run_fetch(_source_config(rate_limit_per_second=1))

        assert len(sent_at) == 3, "a three-send happy path must record three timestamps"
        gaps = [b - a for a, b in zip(sent_at, sent_at[1:], strict=False)]
        assert all(gap >= 1.0 - _PACING_JITTER for gap in gaps), (
            f"adjacent sends were not paced: {gaps}"
        )

    def test_one_throttle_call_per_send_including_retries(
        self, router: respx.MockRouter, no_retry_backoff: None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The PRIMARY, timing-independent proof (Sol pass 4, major 5).

        A wall-clock assertion over a 500-then-200 sequence passes even when the
        throttle is absent from the retry attempt, because tenacity's own
        ``wait_random_exponential`` backoff may already exceed 1 s. Counting
        throttle calls against recorded sends cannot be fooled that way.
        """
        calls: list[float] = []
        real_throttle = NesoDataPortalConnector._throttle_request

        async def _spy(self: NesoDataPortalConnector) -> None:
            calls.append(monotonic())
            await real_throttle(self)

        monkeypatch.setattr(NesoDataPortalConnector, "_throttle_request", _spy)

        _wire_package_show(router)
        router.get(url__startswith=DAILY_WIND_RESOURCE_URL).mock(
            side_effect=[
                httpx.Response(500, content=b"boom"),
                httpx.Response(200, content=DAILY_WIND_CSV),
            ]
        )

        _run_fetch(_source_config())

        assert len(calls) == len(router.calls), (
            f"{len(calls)} throttle calls for {len(router.calls)} sends — a retry attempt "
            "reached the network without being throttled"
        )

    def test_retry_spacing_is_the_throttles_and_not_tenacitys_backoff(
        self, router: respx.MockRouter, no_retry_backoff: None
    ) -> None:
        """The COMPLEMENTARY proof: with tenacity's wait neutralised by the
        ``no_retry_backoff`` fixture, any spacing observed is the throttle's."""
        sent_at: list[float] = []

        def _stamp(request: httpx.Request) -> httpx.Response:
            sent_at.append(monotonic())
            if "/api/3/action/package_show" in str(request.url):
                return httpx.Response(
                    200, json=_fixture("package_show_daily_wind_availability.json")
                )
            if len(sent_at) == 2:
                return httpx.Response(500, content=b"boom")
            return httpx.Response(200, content=DAILY_WIND_CSV)

        router.route(url__regex=r".*").mock(side_effect=_stamp)

        _run_fetch(_source_config(rate_limit_per_second=1))

        assert len(sent_at) == 3, "expected package_show + a failed attempt + its retry"
        gaps = [b - a for a, b in zip(sent_at, sent_at[1:], strict=False)]
        assert all(gap >= 1.0 - _PACING_JITTER for gap in gaps), (
            f"a retry attempt was not throttled: {gaps}"
        )


class TestRealResolverHelper:
    """The named helper's OWN body — the sockaddr extraction — must be tested.

    Stubbing ``_resolve_host_addresses`` everywhere leaves the ``getaddrinfo``
    call and the tuple indexing completely untested, so broken wiring (awaiting
    the wrong thing, reading the wrong element) would leave the suite green
    while every real send failed or, worse, validated the wrong value.

    Offline: only ``loop.getaddrinfo`` itself is monkeypatched, so no real name
    resolution leaves the process here either.
    """

    def test_the_helper_extracts_addresses_from_both_address_families(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``sockaddr[0]`` is the element that differs between the 2-tuple IPv4
        and 4-tuple IPv6 shapes, and the one a refactor gets wrong."""
        answer = [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443)),
            (
                socket.AF_INET6,
                socket.SOCK_STREAM,
                6,
                "",
                ("2606:2800:220:1:248:1893:25c8:1946", 443, 0, 0),
            ),
        ]
        seen: list[tuple[str, int]] = []

        async def _run() -> list[Any]:
            loop = asyncio.get_running_loop()

            async def _fake_getaddrinfo(
                host: str, port: int, **kwargs: Any
            ) -> list[tuple[Any, ...]]:
                seen.append((host, port))
                return answer

            monkeypatch.setattr(loop, "getaddrinfo", _fake_getaddrinfo)
            return await _resolve_host_addresses("api.neso.energy", 443)

        addresses = asyncio.run(_run())

        assert seen == [("api.neso.energy", 443)]
        assert addresses == [
            ipaddress.ip_address("93.184.216.34"),
            ipaddress.ip_address("2606:2800:220:1:248:1893:25c8:1946"),
        ]
        assert all(address.is_global for address in addresses)


class TestBronzeSidecarRoundTrip:
    """The provenance the silver layer reads back must survive the write.

    Bronze is **irreproducible**: a key that is dropped or masked here cannot be
    recovered by re-running, and D-23 skips a whole vintage when any required
    element is missing. So this drives the real ``BronzeWriter``, not a mock.
    """

    def _raw_response(self, *, data_date: Any, fetched_at: datetime) -> Any:
        return client_module.RawResponse(
            body=DAILY_WIND_CSV,
            content_type="text/csv",
            source="neso_data_portal",
            dataset=DATASET,
            fetched_at=fetched_at,
            request_url=DAILY_WIND_RESOURCE_URL,
            request_params={
                "package": "daily-wind-availability",
                "package_id": DAILY_WIND_PACKAGE_ID,
                "resource_id": DAILY_WIND_RESOURCE_ID,
                "resource_name": "Daily Wind Availability",
                "resource_filename": "windavailability.csv",
                "ckan_last_modified": "2026-08-16T18:20:11.953941",
                "ckan_format": "CSV",
                "body_sha256": "0" * 64,
            },
            api_version="3",
            data_date=data_date,
        )

    def test_every_d12_key_survives_sanitize_params_unredacted(self, tmp_path: Path) -> None:
        """``sanitize_params`` masks by exact key name. None of D-12's keys
        collide with the secret list, and this is what proves it stays true —
        a masked ``resource_filename`` would silently cost the embedded
        forecast its ``issue_time``.
        """
        from gridflow.bronze.writer import BronzeWriter

        end = datetime(2026, 8, 16, 23, 58, tzinfo=UTC)
        raw = self._raw_response(data_date=end.date(), fetched_at=end)

        path = BronzeWriter(tmp_path).write(raw)
        sidecar = json.loads(path.with_suffix("").with_suffix(".meta.json").read_text())

        assert sidecar["request_params"] == raw.request_params, (
            "a D-12 provenance key was dropped or redacted on the way to bronze"
        )
        for key, value in raw.request_params.items():
            assert sidecar["request_params"][key] == value
            assert "<redacted>" not in str(sidecar["request_params"][key]), key

    def test_the_body_lands_with_a_csv_extension(self, tmp_path: Path) -> None:
        """D-10: the ``.csv`` extension comes from the stamped ``text/csv``.

        The presigned host serves ``application/octet-stream``, which the writer
        maps to ``.bin`` — and a ``.bin`` body is invisible to the
        transformer's ``raw_*.csv`` glob, so silver would read zero rows from a
        bronze tree that is not empty.
        """
        from gridflow.bronze.writer import BronzeWriter

        end = datetime(2026, 8, 16, 12, 0, tzinfo=UTC)
        path = BronzeWriter(tmp_path).write(
            self._raw_response(data_date=end.date(), fetched_at=end)
        )

        assert path.suffix == ".csv"
        assert path.name.startswith("raw_")
        assert path.read_bytes() == DAILY_WIND_CSV

    def test_the_partition_follows_the_window_end_not_the_wall_clock(self, tmp_path: Path) -> None:
        """D-13 / FM-13, the case that motivated the decision.

        A ``--last 24h`` run started at 23:58 UTC whose download finishes at
        00:01 the next day. ``fetched_at`` is stamped at ``RawResponse``
        construction — i.e. AFTER the download — so a ``fetched_at``-derived
        partition would land on day N+1 while the transform leg, working from
        the window resolved at 23:58, only looks at day N. Ingest succeeds,
        transform finds nothing, and nothing anywhere reports a problem.
        """
        from gridflow.bronze.writer import BronzeWriter

        end = datetime(2026, 8, 16, 23, 58, tzinfo=UTC)
        finished_next_day = datetime(2026, 8, 17, 0, 1, tzinfo=UTC)
        assert finished_next_day.date() != end.date(), "precondition: the clock crossed midnight"

        path = BronzeWriter(tmp_path).write(
            self._raw_response(data_date=end.date(), fetched_at=finished_next_day)
        )

        partition = path.parent
        assert partition.parts[-3:] == ("2026", "08", "16"), (
            f"bronze landed at {'/'.join(partition.parts[-3:])} — the partition followed "
            "the wall clock instead of the resolved window end"
        )
        assert (
            partition == tmp_path / "bronze" / "neso_data_portal" / DATASET / "2026" / "08" / "16"
        )


class TestObservedHttpStatus:
    """``http_status`` is written to the immutable bronze sidecar, so it must
    record what was actually observed rather than a constant.

    Recording a status we did not see is false provenance whether or not the
    falsehood is currently reachable — and bronze cannot be corrected later.
    """

    def test_a_200_is_recorded_as_observed(self, router: respx.MockRouter) -> None:
        _wire_package_show(router)
        _wire_redirect_download(router)

        (raw,) = _run_fetch(_source_config())

        assert raw.http_status == 200

    def test_a_206_partial_response_is_refused_rather_than_recorded_as_200(
        self, router: respx.MockRouter, raw_response_spy: list[dict[str, Any]]
    ) -> None:
        """No Range header is ever sent, so a 206 means the transfer is not what
        was asked for — and its body is a FRAGMENT that still parses as valid
        CSV, so nothing downstream would notice.
        """
        _wire_package_show(router)
        _wire_redirect_download(
            router,
            file_response=httpx.Response(
                206,
                content=DAILY_WIND_CSV,
                headers={"Content-Range": "bytes 0-62/620"},
            ),
        )

        with pytest.raises(NesoUnexpectedStatusError) as excinfo:
            _run_fetch(_source_config())

        assert "206" in str(excinfo.value)
        assert raw_response_spy == [], "a partial body reached RawResponse construction"

    def test_a_203_success_is_also_refused(self, router: respx.MockRouter) -> None:
        """The rule is "complete-file 200", not "not 206" — an allow-list, so a
        status nobody anticipated cannot slip through as provenance."""
        _wire_package_show(router)
        _wire_redirect_download(router, file_response=httpx.Response(203, content=DAILY_WIND_CSV))

        with pytest.raises(NesoUnexpectedStatusError):
            _run_fetch(_source_config())


# The credential components of the presigned test URL. If either of these
# strings appears in ANY exception message or log record the suite produces,
# a credential has escaped the connector.
_TEST_SIGNATURE = "b4f0da8dbcf4e5c46e06a16556fcc90257a632b4684f5d6d4c4d0da7565bceef"
_TEST_CREDENTIAL = "564ecf1b9cb7e605192d2953e7a993b9"
_LEAKY_PRESIGNED_URL = (
    FILE_PATH
    + "?response-content-disposition=attachment"
    + f"&X-Amz-Credential={_TEST_CREDENTIAL}%2F20260816%2Fauto%2Fs3%2Faws4_request"
    + "&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expires=604800"
    + f"&X-Amz-SignedHeaders=host&X-Amz-Signature={_TEST_SIGNATURE}"
)


def _connector_log_text(caplog: pytest.LogCaptureFixture) -> str:
    """Log output emitted BY THIS CONNECTOR (and the reader it calls).

    Scoped deliberately, and the scope is itself a finding. httpx's own logger
    emits ``HTTP Request: GET <full url> "HTTP/1.1 200 OK"`` at INFO on every
    single request, query string included -- so a presigned URL's
    X-Amz-Signature reaches gridflow's log file on every successful fetch, and
    ENTSO-E's securityToken does the same today. That is a real leak, it is
    repo-wide and pre-existing, and it is NOT this connector's to fix: the
    logger is module-global in httpx, so any suppression would change logging
    for all six sources. Recorded in deferred-items.md and reported.

    What this connector CAN guarantee is that nothing it emits itself carries
    credentials, and that is what these tests assert.
    """
    return " | ".join(
        record.getMessage() for record in caplog.records if record.name.startswith("gridflow.")
    )


class TestCredentialsCannotLeave:
    """The emission invariant: credential-bearing material cannot leave this
    connector by ANY path — sidecar, exception message or log line.

    **Why this is a property test and not more redacted f-strings.** The first
    fix for this class checked a URL's shape at one site and redacted at
    another; the next review found both a hole in the shape check and a leak
    the fix itself had introduced. Site-by-site defence against a leak class
    loses to the site nobody enumerated. So the connector has ONE safe
    rendering, :func:`~...client._safe_url`, and these tests assert the
    property over observed output rather than over the list of sites we
    happened to think of — the same move as proving the send invariant at the
    transport instead of by matching source text.
    """

    @pytest.mark.parametrize(
        ("file_response", "expected", "case"),
        [
            (
                httpx.Response(200, content=DAILY_WIND_CSV, headers={"Content-Encoding": "br"}),
                NesoUnexpectedEncodingError,
                "unexpected content encoding",
            ),
            (
                httpx.Response(206, content=DAILY_WIND_CSV),
                NesoUnexpectedStatusError,
                "partial content status",
            ),
            (
                httpx.Response(200, content=DAILY_WIND_CSV, headers={"Content-Length": "999999"}),
                NesoTruncatedBodyError,
                "short body against a declared length",
            ),
            (
                httpx.Response(
                    200, content=b"x", headers={"Content-Length": str(64 * 1024 * 1024)}
                ),
                NesoResponseTooLargeError,
                "declared oversize",
            ),
            (
                httpx.Response(200, content=b"BMU_ID,Date,RENAMED\nT_A,2026-08-16,1\n"),
                CsvHeaderDriftError,
                "header drift (message built in csv_bronze, not here)",
            ),
            (
                httpx.Response(200, content=b"<html>nope</html>"),
                NotCsvBodyError,
                "non-CSV body (message built in csv_bronze, not here)",
            ),
        ],
    )
    def test_no_failure_path_on_the_file_leg_leaks_the_presigned_credentials(
        self,
        router: respx.MockRouter,
        caplog: pytest.LogCaptureFixture,
        file_response: httpx.Response,
        expected: type[Exception],
        case: str,
    ) -> None:
        """Every failure mode reachable AFTER the redirect to the presigned URL.

        Each one previously interpolated ``response.request.url`` — the
        presigned target — straight into its message. Only one of them was
        flagged in review; the rest are why the fix had to be structural.
        """
        _wire_package_show(router)
        _wire_redirect_download(router, location=_LEAKY_PRESIGNED_URL, file_response=file_response)

        with caplog.at_level(logging.DEBUG), pytest.raises(expected) as excinfo:
            _run_fetch(_source_config())

        emitted = str(excinfo.value) + "\n" + _connector_log_text(caplog)
        assert _TEST_SIGNATURE not in emitted, f"{case}: X-Amz-Signature escaped"
        assert _TEST_CREDENTIAL not in emitted, f"{case}: X-Amz-Credential escaped"
        assert "X-Amz-Signature=" not in emitted, f"{case}: a signed query escaped"

    @pytest.mark.parametrize(
        ("status", "case"),
        [(403, "signature rejected"), (500, "terminal 5xx after retries")],
    )
    def test_a_non_2xx_on_the_presigned_leg_does_not_leak_it(
        self,
        router: respx.MockRouter,
        caplog: pytest.LogCaptureFixture,
        no_retry_backoff: None,
        status: int,
        case: str,
    ) -> None:
        """Finding 1. ``raise_for_status()`` builds its message from the FULL
        signed URL, and tenacity's ``before_sleep_log`` writes that string to
        the log before every retry — so a 5xx leaked it five times over.

        The whole exception chain is inspected, not just ``str(exc)``: a raw
        ``__cause__`` would surface in any traceback the operator sees.
        """
        _wire_package_show(router)
        _wire_redirect_download(
            router, location=_LEAKY_PRESIGNED_URL, file_response=httpx.Response(status)
        )

        with caplog.at_level(logging.DEBUG), pytest.raises(httpx.HTTPStatusError) as excinfo:
            _run_fetch(_source_config())

        chain = "".join(
            traceback.format_exception(
                type(excinfo.value), excinfo.value, excinfo.value.__traceback__
            )
        )
        emitted = (
            str(excinfo.value)
            + repr(excinfo.value)
            + chain
            + _connector_log_text(caplog)
            + "".join(str(r.getMessage()) for r in caplog.records if r.name.startswith("tenacity"))
        )
        assert _TEST_SIGNATURE not in emitted, f"{case}: signature escaped"
        assert _TEST_CREDENTIAL not in emitted, f"{case}: credential escaped"
        assert excinfo.value.__cause__ is None, f"{case}: a raw cause was chained"

    def test_a_rejected_presigned_redirect_target_does_not_leak_it(
        self,
        router: respx.MockRouter,
        caplog: pytest.LogCaptureFixture,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The refusal path itself must not echo what it refused.

        A ``Location`` pointing at a presigned URL that fails the address policy
        is the case where the connector holds vendor credentials and is about to
        describe them in an error.
        """
        _wire_package_show(router)
        _wire_redirect_download(router, location=_LEAKY_PRESIGNED_URL)

        lookups = {"n": 0}

        async def _stub(host: str, port: int) -> list[Any]:
            lookups["n"] += 1
            if lookups["n"] <= 2:
                return [ipaddress.ip_address("93.184.216.34")]
            return [ipaddress.ip_address("127.0.0.1")]

        monkeypatch.setattr(client_module, "_resolve_host_addresses", _stub)

        with caplog.at_level(logging.DEBUG), pytest.raises(NesoUnsafeRedirectError) as excinfo:
            _run_fetch(_source_config())

        emitted = str(excinfo.value) + "\n" + _connector_log_text(caplog)
        assert _TEST_SIGNATURE not in emitted
        assert _TEST_CREDENTIAL not in emitted

    def test_the_bronze_provenance_never_carries_the_presigned_target(
        self, router: respx.MockRouter
    ) -> None:
        """The original finding: the sidecar records the redirector only."""
        _wire_package_show(router)
        _wire_redirect_download(router, location=_LEAKY_PRESIGNED_URL)

        (raw,) = _run_fetch(_source_config())

        recorded = raw.request_url + "\n" + repr(raw.request_params)
        assert _TEST_SIGNATURE not in recorded
        assert _TEST_CREDENTIAL not in recorded
        assert raw.request_url == DAILY_WIND_RESOURCE_URL

    def test_no_rendering_of_a_safe_url_can_produce_credentials(self) -> None:
        """The representation-level guarantee, tested through EVERY spelling.

        This is the list the previous name-based static check could be evaded
        by — ``str()``, an f-string, ``.format()``, ``%``, an alias, ``repr``.
        Under a sanitising helper each was a separate site someone had to
        remember. Under a type they are all the same code path, so the test is
        exhaustive over spellings rather than hopeful about them.
        """
        hostile = (
            f"https://user:pw@evil.example:8443/p/a/t/h?X-Amz-Signature={_TEST_SIGNATURE}"
            f"&X-Amz-Credential={_TEST_CREDENTIAL}#access_token=frag"
        )
        safe = client_module.SafeUrl.opaque(hostile)
        alias = safe

        renderings = [
            str(safe),
            repr(safe),
            f"{safe}",
            f"{safe!s}",
            f"{safe!r}",
            # These legacy spellings are the POINT: they are the forms a
            # name-based check could be evaded by, so they are exercised
            # deliberately rather than modernised away.
            "{}".format(safe),  # noqa: UP032
            "{0!s} {0!r}".format(safe),  # noqa: UP032
            "%s" % safe,  # noqa: UP031
            "%r" % safe,  # noqa: UP031
            format(safe, ""),
            format(safe, ">80"),
            str(alias),
            f"{alias}",
            str([safe]),
            str({"u": safe}),
            str((safe,)),
        ]

        for rendered in renderings:
            for forbidden in (
                _TEST_SIGNATURE,
                _TEST_CREDENTIAL,
                "user:pw",
                "access_token",
                "X-Amz-",
                "/p/a/t/h",
            ):
                assert forbidden not in rendered, f"{forbidden!r} leaked via {rendered!r}"

    def test_an_unconstrained_hop_renders_origin_only(self) -> None:
        """Finding 3: ``_assert_safe_target`` permits any globally-routable host
        and ANY path, so an arbitrary redirect target's path may itself carry a
        bearer token. Only the redirector's path has been proven, so only the
        redirector's path may be rendered.
        """
        token_in_path = "https://cdn.example/download/bearer-abc123secret/file.csv"

        assert str(client_module.SafeUrl.opaque(token_in_path)) == "https://cdn.example"
        assert "bearer-abc123secret" not in str(client_module.SafeUrl.opaque(token_in_path))

        # The shape-validated redirector keeps its path, which is the whole
        # point of validating it.
        verified = client_module.SafeUrl.verified(DAILY_WIND_RESOURCE_URL)
        assert str(verified) == DAILY_WIND_RESOURCE_URL

    def test_the_supported_door_returns_the_raw_bytes_intact(self) -> None:
        """``unsafe_raw`` is the single SUPPORTED door to the raw URL."""
        safe = client_module.SafeUrl.opaque(PRESIGNED_URL)

        assert _TEST_SIGNATURE not in str(safe)
        assert str(safe.unsafe_raw()) == PRESIGNED_URL, (
            "sending must get the bytes back byte-identical — the presigned "
            "signature covers the query"
        )

    def test_unsafe_raw_is_called_only_where_a_request_is_built(self) -> None:
        """The narrow static check that survives, in the shape Sol accepted for
        ``_assert_safe_target``: pin the accessor's production call sites.

        Deliberately NOT a check about how URLs are spelled — that model failed
        twice. This asserts only that the one SUPPORTED door to the raw form is
        opened where requests are constructed and nowhere else, so a new call
        site is a deliberate act rather than an accident.
        """
        package_dir = Path(client_module.__file__).parent
        call_sites: list[tuple[str, str]] = []

        for path in sorted(package_dir.rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                    continue
                for inner in ast.walk(node):
                    if (
                        isinstance(inner, ast.Call)
                        and isinstance(inner.func, ast.Attribute)
                        and inner.func.attr == "unsafe_raw"
                    ):
                        call_sites.append((path.name, node.name))

        assert call_sites == [
            ("client.py", "_download_resource"),
            ("client.py", "_download_resource"),
        ], f"unsafe_raw() is called somewhere new: {call_sites}"

    def test_the_type_exposes_no_second_supported_door_to_the_raw_url(self) -> None:
        """4.3: ``unsafe_raw()`` must be the only SUPPORTED way out.

        The previous version claimed to be a closed representation and was not:
        ``_raw``, ``path``, ``query``, ``fragment`` and ``userinfo`` were all
        public, and pickle walked the slot. A type whose docstring promises
        closure and whose surface does not deliver it is worse than an honest
        helper, because it invites callers to trust it. The claim is therefore
        narrowed, not the boundary strengthened: Python has no private
        attributes, so the name-mangled slot stays reachable — and this test
        says so instead of pretending otherwise.
        """
        hostile = f"https://user:pw@evil.example/p?X-Amz-Signature={_TEST_SIGNATURE}#frag"
        safe = client_module.SafeUrl.opaque(hostile)

        for attribute in ("path", "query", "fragment", "userinfo"):
            assert not hasattr(safe, attribute), (
                f"SafeUrl.{attribute} is a second supported door to the raw URL"
            )

        # Honesty check for the narrowed claim: the mangled slot IS reachable
        # (no Python object can prevent deliberate extraction). If this ever
        # fails, the representation changed and the docstring's residual-risk
        # paragraph must be re-derived, not deleted.
        assert isinstance(safe._SafeUrl__raw, httpx.URL), (
            "the name-mangled slot moved — re-derive SafeUrl's narrowed claim"
        )

        # The credential-bearing components are answerable as questions only.
        assert safe.has_query() is True
        assert safe.has_userinfo() is True
        assert safe.has_fragment() is True

        # Safe components stay available — they cannot carry credentials.
        assert safe.scheme == "https"
        assert safe.host

    def test_the_type_refuses_serialisation(self) -> None:
        """Pickle would write the raw URL into a byte stream no rendering rule
        governs — a hole straight through the guarantee."""
        safe = client_module.SafeUrl.opaque(_LEAKY_PRESIGNED_URL)

        with pytest.raises(TypeError, match="not serialisable"):
            pickle.dumps(safe)

    def test_path_matching_happens_inside_the_object(self) -> None:
        """No supported accessor exposes the path, so an unproven one cannot
        be rendered by accident."""
        redirector = client_module.SafeUrl.opaque(DAILY_WIND_RESOURCE_URL)
        pattern = (
            "dataset",
            frozenset({DAILY_WIND_PACKAGE_ID}),
            "resource",
            DAILY_WIND_RESOURCE_ID,
            "download",
            None,
        )

        assert redirector.path_matches(pattern) is True
        assert client_module.SafeUrl.opaque(f"{BASE_URL}/x.csv").path_matches(pattern) is False
        # `None` means "any NON-EMPTY segment", so a trailing slash fails.
        assert (
            client_module.SafeUrl.opaque(
                f"{BASE_URL}/dataset/{DAILY_WIND_PACKAGE_ID}/resource/"
                f"{DAILY_WIND_RESOURCE_ID}/download/"
            ).path_matches(pattern)
            is False
        )

    def test_a_rejected_redirector_never_echoes_its_unproven_path(
        self, router: respx.MockRouter, raw_response_spy: list[dict[str, Any]]
    ) -> None:
        """4.1: the value is OPAQUE until the checks pass, so the message that
        explains a rejection cannot render the path it just rejected."""
        odd_path = "/dataset/pkg/resource/other-id/download/leaky-path-segment.csv"
        payload = _fixture("package_show_daily_wind_availability.json")
        payload["result"]["resources"][0]["url"] = BASE_URL + odd_path
        _wire_package_show(router, payload)
        _add_catch_all(router)

        with pytest.raises(NesoUnexpectedResourceUrlError) as excinfo:
            _run_fetch(_source_config())

        message = str(excinfo.value)
        assert "leaky-path-segment" not in message, "the unproven path was echoed"
        assert "its path is not exactly" in message
        assert raw_response_spy == []

    def test_a_malformed_location_is_replaced_at_the_boundary(
        self, router: respx.MockRouter
    ) -> None:
        """4.2: httpx's own ``InvalidURL`` repeats the offending value.

        A malformed ``Location`` is a real vendor failure mode, so it is caught
        where it is resolved and replaced with a connector error raised
        ``from None``.
        """
        _wire_package_show(router)
        router.get(url__startswith=DAILY_WIND_RESOURCE_URL).mock(
            return_value=httpx.Response(302, headers={"location": "https://host:notaport/x.csv"})
        )
        _add_catch_all(router)

        with pytest.raises(NesoUnsafeRedirectError) as excinfo:
            _run_fetch(_source_config())

        message = str(excinfo.value)
        assert "not a resolvable URL" in message
        assert "notaport" not in message, "the malformed vendor value was echoed"
        assert excinfo.value.__cause__ is None, "httpx's own error was chained"


# --------------------------------------------------------------------------- #
# T-06 -- shared silver plumbing
#
# Two additive changes to `silver/base.py`, both audited by CALLER ENUMERATION
# rather than by reasoning about intent:
#
#   D-20  `BRONZE_BODY_GLOB`, a read-path ClassVar with WRITE-path consequences
#         (it selects which bodies are seen, hence which vintages are assigned,
#         hence which silver FILENAMES are written). So both branch tests below
#         assert on written FILENAMES, not on row counts -- a row-count
#         assertion cannot see a filename move, which is the failure this
#         guards.
#   D-40  `last_excluded_row_count`, the counter that makes an EXCLUDED row
#         reach the run status. Its two tests are on UNTOUCHED transformers,
#         because the point of a defaulted counter is that nothing moves.
# --------------------------------------------------------------------------- #


_SP_VINTAGES: tuple[tuple[str, datetime, float], ...] = (
    ("first", datetime(2024, 1, 15, 8, tzinfo=UTC), 44.0),
    ("second", datetime(2024, 1, 15, 12, tzinfo=UTC), 45.5),
)
"""The exact fixture `test_run_writes_one_file_per_bronze_vintage` already
pins on master, reused so the filename expectations below are the SAME
expectations, not a new set that happens to agree."""


def _system_prices_payload(day: date, sell_price: float) -> str:
    return json.dumps(
        {
            "data": [
                {
                    "settlementDate": day.isoformat(),
                    "settlementPeriod": 1,
                    "systemSellPrice": sell_price,
                    "systemBuyPrice": 55.0,
                    "netImbalanceVolume": -120.5,
                }
            ]
        }
    )


class _CsvGlobTransformer(BaseSilverTransformer):
    """A `VINTAGE_PER_BRONZE_FILE` transformer whose bodies are CSV, not JSON.

    The other branch of D-20's claim: overriding `BRONZE_BODY_GLOB` must make
    the per-file loop see `raw_*.csv` bodies and nothing else -- not the
    `.meta.json` sidecars beside them, and not a `raw_*.json` decoy sharing
    the partition.
    """

    source = "test_glob"
    dataset = "csv_bodies"
    schema_cls = None
    APPEND_ONLY: ClassVar[bool] = True
    VINTAGE_PER_BRONZE_FILE: ClassVar[bool] = True
    BRONZE_BODY_GLOB: ClassVar[str] = "raw_*.csv"
    ENTITY_KEY_COLUMNS: ClassVar[tuple[str, ...]] = ("value",)

    seen_bodies: list[str]
    """Set per INSTANCE by the test. Annotation only -- a class-level ``[]``
    default would be shared mutable state across every instance."""

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        """Unused on the per-file branch; declared so the contract is complete."""
        return pl.DataFrame()

    def read_bronze_file(self, raw_path: Path) -> pl.DataFrame:
        """Record which bodies the loop offered, then read the CSV."""
        self.seen_bodies.append(raw_path.name)
        return pl.read_csv(raw_path)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        """Identity: this fixture is about body SELECTION, not normalisation."""
        return raw_df


def test_bronze_body_glob_defaults_to_json_on_the_base_and_its_sole_inheritor() -> None:
    """D-20, the default: the ClassVar is additive, so the one existing
    `VINTAGE_PER_BRONZE_FILE` opt-in in the repo must inherit master's literal
    unchanged. A default that drifted would move silver filenames for
    `elexon/system_prices` without a single test naming it."""
    assert BaseSilverTransformer.BRONZE_BODY_GLOB == "raw_*.json"
    assert SystemPriceTransformer.BRONZE_BODY_GLOB == "raw_*.json"


def test_json_glob_branch_writes_exactly_the_silver_filenames_it_wrote_before(
    tmp_path: Path,
) -> None:
    """D-20, JSON branch, BEHAVIOURAL: the ClassVar's reach is the silver
    filename, so the regression guard is the filename set."""
    target_date = date(2024, 1, 15)
    bronze_dir = tmp_path / "bronze" / "elexon" / "system_prices" / "2024" / "01" / "15"
    bronze_dir.mkdir(parents=True)
    for name, written_at, sell_price in _SP_VINTAGES:
        (bronze_dir / f"raw_{name}.json").write_text(
            _system_prices_payload(target_date, sell_price)
        )
        (bronze_dir / f"raw_{name}.meta.json").write_text(
            json.dumps({"written_at": written_at.isoformat()})
        )

    transformer = SystemPriceTransformer(tmp_path)
    assert transformer.run(target_date, run_id="glob") == 2

    silver_dir = tmp_path / "silver" / "elexon" / "system_prices" / "year=2024" / "month=01"
    assert sorted(path.name for path in silver_dir.glob("*.parquet")) == [
        "system_prices_20240115_run2024-01-15T08-00-00-00-00.parquet",
        "system_prices_20240115_run2024-01-15T12-00-00-00-00.parquet",
    ], "the vintage->filename mapping moved; the glob reaches further than a read"


def test_csv_glob_branch_sees_csv_bodies_and_never_their_sidecars(tmp_path: Path) -> None:
    """D-20, CSV branch, BEHAVIOURAL: the override selects CSV bodies, the
    `.meta.json` guard is still correct under a non-JSON glob, and a JSON
    decoy in the same partition is invisible."""
    target_date = date(2024, 1, 15)
    bronze_dir = tmp_path / "bronze" / "test_glob" / "csv_bodies" / "2024" / "01" / "15"
    bronze_dir.mkdir(parents=True)
    for name, written_at, value in (
        ("first", datetime(2024, 1, 15, 8, tzinfo=UTC), 1),
        ("second", datetime(2024, 1, 15, 12, tzinfo=UTC), 2),
    ):
        (bronze_dir / f"raw_{name}.csv").write_text(f"value\n{value}\n")
        (bronze_dir / f"raw_{name}.meta.json").write_text(
            json.dumps({"written_at": written_at.isoformat()})
        )
    (bronze_dir / "raw_decoy.json").write_text(json.dumps({"value": 99}))
    (bronze_dir / "raw_decoy.meta.json").write_text(
        json.dumps({"written_at": datetime(2024, 1, 15, 16, tzinfo=UTC).isoformat()})
    )

    transformer = _CsvGlobTransformer(tmp_path)
    transformer.seen_bodies = []
    assert transformer.run(target_date, run_id="glob") == 2

    assert transformer.seen_bodies == ["raw_first.csv", "raw_second.csv"], (
        "a sidecar or the JSON decoy was offered to read_bronze_file as a body"
    )
    silver_dir = tmp_path / "silver" / "test_glob" / "csv_bodies" / "year=2024" / "month=01"
    assert sorted(path.name for path in silver_dir.glob("*.parquet")) == [
        "csv_bodies_20240115_run2024-01-15T08-00-00-00-00.parquet",
        "csv_bodies_20240115_run2024-01-15T12-00-00-00-00.parquet",
    ]


class _ExcludingVintageTransformer(BaseSilverTransformer):
    """A ``VINTAGE_PER_BRONZE_FILE`` transformer that DECLINES one row per body.

    The framework hazard D-27 names, made testable: ``transform()`` runs once
    per bronze file against a SINGLE reset in ``run()``, so the counter must be
    accumulated with ``+=``. An assignment would report the last file's count
    and silently lose every earlier file's exclusions.
    """

    source = "test_excl"
    dataset = "vintage_rows"
    schema_cls = None
    APPEND_ONLY: ClassVar[bool] = True
    VINTAGE_PER_BRONZE_FILE: ClassVar[bool] = True
    ENTITY_KEY_COLUMNS: ClassVar[tuple[str, ...]] = ("value",)

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        """Unused on the per-file branch; declared so the contract is complete."""
        return pl.DataFrame()

    def read_bronze_file(self, raw_path: Path) -> pl.DataFrame:
        """Read one body: one kept row and one row flagged for exclusion."""
        return pl.DataFrame(json.loads(raw_path.read_text()))

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        """Drop every row flagged ``bad``, accumulating the count with ``+=``."""
        declined = raw_df.filter(pl.col("bad"))
        self.last_excluded_row_count += declined.height
        return raw_df.filter(~pl.col("bad")).select("value")


class _TinyBoundedSchema(BaseSchema):
    """Minimal bounded schema (``BaseSchema`` is strict) for the fail-soft half."""

    value: int = Field(ge=0, le=10)


class _ValidatingPlainTransformer(BaseSilverTransformer):
    """An UNTOUCHED-shape transformer: plain branch, no exclusions, real schema."""

    source = "test_plain"
    dataset = "validated"
    schema_cls = _TinyBoundedSchema
    ENTITY_KEY_COLUMNS: ClassVar[tuple[str, ...]] = ("value",)

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        """Read the whole date partition as one frame (master's default shape)."""
        partition = (
            self.bronze_dir
            / str(target_date.year)
            / f"{target_date.month:02d}"
            / f"{target_date.day:02d}"
        )
        rows: list[int] = []
        for body in sorted(partition.glob("raw_*.json")):
            if body.name.endswith(".meta.json"):
                continue
            rows.extend(json.loads(body.read_text()))
        return pl.DataFrame({"value": rows}) if rows else pl.DataFrame()

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        """Identity: validation, not normalisation, is what this fixture pins."""
        return raw_df


def _isolated_transform(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    seed: Any,
    source: str,
    dataset: str,
    day: date,
    *,
    transformer_cls: type[BaseSilverTransformer] | None = None,
) -> Any:
    """Drive the REAL ``pipeline.runner.run_transform`` over a seeded bronze tree.

    A unit assertion on the transformer's own counters could not prove the
    reported STATUS, which is the whole point of D-40.

    Args:
        tmp_path: Per-test temporary root.
        monkeypatch: Fixture used to isolate every gridflow path env var.
        seed: Callable receiving the data dir; writes the bronze tree.
        source: Source name passed to ``run_transform``.
        dataset: Dataset name passed to ``run_transform``.
        day: The single target date transformed.
        transformer_cls: When set, ``get_transformer`` is stubbed to return an
            instance of it, so a fixture transformer never has to be entered
            into the process-wide registry.

    Returns:
        The single ``DatasetResult`` for the requested dataset.
    """
    from gridflow.config.settings import load_settings
    from gridflow.pipeline import runner as pipeline_runner
    from gridflow.storage.duckdb import get_connection, init_catalogue

    data_dir = tmp_path / "data"
    db_path = tmp_path / "catalogue" / "gridflow.duckdb"
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    if transformer_cls is not None:
        monkeypatch.setattr(
            "gridflow.silver.registry.get_transformer",
            lambda _source, _dataset, root: transformer_cls(root),
        )
    seed(data_dir)

    settings = load_settings()
    pipeline_runner.import_transformers()
    init_catalogue(db_path, data_dir)
    con = get_connection(db_path)
    try:
        ctx = pipeline_runner.PipelineContext(con=con, settings=settings)
        results = pipeline_runner.run_transform(
            ctx,
            source,
            [dataset],
            datetime(day.year, day.month, day.day, tzinfo=UTC),
            datetime(day.year, day.month, day.day, tzinfo=UTC),
        )
    finally:
        con.close()
    assert len(results) == 1
    return results[0]


def _write_vintage_body(partition: Path, name: str, payload: Any, written_at: datetime) -> None:
    """Write one bronze body and the sidecar that vouches for it."""
    (partition / f"raw_{name}.json").write_text(json.dumps(payload))
    (partition / f"raw_{name}.meta.json").write_text(
        json.dumps({"written_at": written_at.isoformat()})
    )


def test_an_untouched_source_reports_identical_counts_and_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D-40, the whole point of a defaulted counter: NOTHING moves.

    A transformer that never touches ``last_excluded_row_count`` contributes
    exactly zero to a total it already contributed to. Asserted on the VALUES:
    a test that only checked the call succeeded would pass with the fold
    silently doubling ``rows_invalid``.
    """
    target_date = date(2026, 1, 5)

    def seed(data_dir: Path) -> None:
        partition = data_dir / "bronze" / "test_plain" / "validated" / "2026" / "01" / "05"
        partition.mkdir(parents=True)
        _write_vintage_body(partition, "clean", [1, 2, 3], datetime(2026, 1, 5, 9, tzinfo=UTC))

    clean = _isolated_transform(
        tmp_path,
        monkeypatch,
        seed,
        "test_plain",
        "validated",
        target_date,
        transformer_cls=_ValidatingPlainTransformer,
    )
    assert (clean.status, clean.rows_out, clean.rows_skipped, clean.rows_invalid) == (
        "success",
        3,
        0,
        0,
    )


def test_the_fold_carries_a_validation_failure_through_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D-40, the fold's FIRST term: a transformer with no exclusions whose rows
    fail validation still reports exactly the validation-failure count.

    The fold gained a second addend; this is the assertion that the first one
    was not disturbed, and that the second contributes 0 rather than a copy.
    """
    target_date = date(2026, 1, 6)

    def seed(data_dir: Path) -> None:
        partition = data_dir / "bronze" / "test_plain" / "validated" / "2026" / "01" / "06"
        partition.mkdir(parents=True)
        # 99 breaches le=10; fail-soft, so it is COUNTED and still WRITTEN.
        _write_vintage_body(partition, "dirty", [5, 7, 99], datetime(2026, 1, 6, 9, tzinfo=UTC))

    result = _isolated_transform(
        tmp_path,
        monkeypatch,
        seed,
        "test_plain",
        "validated",
        target_date,
        transformer_cls=_ValidatingPlainTransformer,
    )
    assert result.status == "completed_with_warnings"
    assert result.rows_out == 3, "fail-soft never drops a row"
    assert (result.rows_invalid, result.rows_skipped, result.rows_unmapped) == (1, 1, 0)


def test_excluded_rows_accumulate_across_two_vintages_in_one_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """D-40 / D-27's framework hazard: ``+=`` against ONE reset per ``run()``.

    Two bronze vintages, one declined row each. ``run_transform`` must report
    2, not 1. 1 is exactly what an assignment inside ``transform()`` would
    report on the ``VINTAGE_PER_BRONZE_FILE`` branch, and it would be wrong
    silently.
    """
    target_date = date(2026, 1, 7)

    def seed(data_dir: Path) -> None:
        partition = data_dir / "bronze" / "test_excl" / "vintage_rows" / "2026" / "01" / "07"
        partition.mkdir(parents=True)
        for name, hour in (("first", 8), ("second", 12)):
            _write_vintage_body(
                partition,
                name,
                [{"value": 1, "bad": False}, {"value": 2, "bad": True}],
                datetime(2026, 1, 7, hour, tzinfo=UTC),
            )

    result = _isolated_transform(
        tmp_path,
        monkeypatch,
        seed,
        "test_excl",
        "vintage_rows",
        target_date,
        transformer_cls=_ExcludingVintageTransformer,
    )
    assert result.status == "completed_with_warnings", (
        "an excluded row must reach the run status, never only the log"
    )
    assert result.rows_out == 2, "the kept row of each vintage is still written"
    assert (result.rows_invalid, result.rows_skipped) == (2, 2), (
        "one vintage's exclusions were lost -- the counter was assigned, not accumulated"
    )


# --------------------------------------------------------------------------- #
# T-09 -- the `daily_wind_availability` transformer
#
# FIXTURE PROVENANCE, DISCLOSED. Stage A captured no CSV sample for this
# resource: `_probe/` holds sample bodies for the other four candidates only.
# `tests/fixtures/neso_data_portal/daily_wind_availability.csv` is therefore
# HAND-AUTHORED from the research-asserted header `BMU_ID, Date, MW`
# (RESEARCH-vendor S3.2), with realistic BMU ids drawn verbatim from
# `_probe/sample_national-demand-bmu.csv`. That is a guess about the vendor,
# and it is a LOUD one: D-19's exact-header contract turns a wrong header into
# a `CsvHeaderDriftError` at transform time AND -- via D-36's admission parse --
# at fetch time, before a byte reaches immutable bronze, and T-24's live test
# pins the real header against reality.
# --------------------------------------------------------------------------- #

DAILY_WIND_FIXTURE_PATH = FIXTURE_DIR / "daily_wind_availability.csv"
DAILY_WIND_FIXTURE_BYTES = DAILY_WIND_FIXTURE_PATH.read_bytes()
DAILY_WIND_FIXTURE_ROWS = 6

# NESO's own naive CKAN stamp, read as UTC per D-15.
CKAN_LAST_MODIFIED = "2026-08-16T18:20:11.953941"
PUBLISHED_AT = datetime(2026, 8, 16, 18, 20, 11, 953941, tzinfo=UTC)

# 2026-08-16 is BST, so its GB availability day starts at 23:00Z the day
# BEFORE; 2026-01-15 is GMT, so its day starts at 00:00Z the same day (D-25).
BST_DATE = date(2026, 8, 16)
BST_DAY_START = datetime(2026, 8, 15, 23, 0, tzinfo=UTC)
GMT_DATE = date(2026, 1, 15)
GMT_DAY_START = datetime(2026, 1, 15, 0, 0, tzinfo=UTC)


def _sidecar_payload(
    *,
    written_at: datetime,
    ckan_last_modified: str | None = CKAN_LAST_MODIFIED,
    resource_filename: str = "windavailability.csv",
    drop: str | None = None,
) -> dict[str, Any]:
    """Build a bronze sidecar carrying the D-12 provenance the connector writes."""
    request_params: dict[str, Any] = {
        "package": "daily-wind-availability",
        "package_id": DAILY_WIND_PACKAGE_ID,
        "resource_id": DAILY_WIND_RESOURCE_ID,
        "resource_name": "Daily Wind Availability",
        "resource_filename": resource_filename,
        "ckan_last_modified": "" if ckan_last_modified is None else ckan_last_modified,
        "ckan_format": "CSV",
        "body_sha256": "0" * 64,
    }
    if drop is not None:
        del request_params[drop]
    return {
        "source": "neso_data_portal",
        "dataset": DATASET,
        "written_at": written_at.isoformat(),
        "data_date": written_at.date().isoformat(),
        "request_url": DAILY_WIND_RESOURCE_URL,
        "request_params": request_params,
        "content_type": "text/csv",
        "http_status": 200,
    }


def _seed_daily_wind_bronze(
    data_dir: Path,
    target_date: date,
    *,
    name: str = "20260816T182500Z_abcd1234",
    body: bytes = DAILY_WIND_FIXTURE_BYTES,
    written_at: datetime | None = None,
    sidecar: dict[str, Any] | None = None,
    write_sidecar: bool = True,
) -> Path:
    """Write one `raw_*.csv` body plus its sidecar into the exact date partition."""
    partition = (
        data_dir
        / "bronze"
        / "neso_data_portal"
        / DATASET
        / str(target_date.year)
        / f"{target_date.month:02d}"
        / f"{target_date.day:02d}"
    )
    partition.mkdir(parents=True, exist_ok=True)
    body_path = partition / f"raw_{name}.csv"
    body_path.write_bytes(body)
    if write_sidecar:
        stamp = written_at or datetime(2026, 8, 16, 18, 25, tzinfo=UTC)
        payload = sidecar if sidecar is not None else _sidecar_payload(written_at=stamp)
        (partition / f"raw_{name}.meta.json").write_text(json.dumps(payload, indent=2))
    return body_path


def _csv(rows: str) -> bytes:
    return ("BMU_ID,Date,MW\n" + rows).encode()


def _transform_body(tmp_path: Path, body: bytes, *, target_date: date = BST_DATE) -> pl.DataFrame:
    """Read one bronze body through the real reader and transform it."""
    raw_path = _seed_daily_wind_bronze(tmp_path, target_date, body=body)
    transformer = DailyWindAvailabilityTransformer(tmp_path)
    return transformer.transform(transformer.read_bronze_file(raw_path))


class TestDailyWindProvenanceReader:
    """D-23: the one site that turns a sidecar into a vintage."""

    def test_it_reads_every_d12_key_and_parses_ckan_time_as_utc(self, tmp_path: Path) -> None:
        raw_path = _seed_daily_wind_bronze(tmp_path, BST_DATE)

        provenance = provenance_for(raw_path)

        assert provenance is not None
        assert provenance.package == "daily-wind-availability"
        assert provenance.resource_id == DAILY_WIND_RESOURCE_ID
        assert provenance.resource_name == "Daily Wind Availability"
        assert provenance.resource_filename == "windavailability.csv"
        assert provenance.published_at == PUBLISHED_AT
        assert provenance.published_at.tzinfo is not None
        assert provenance.issue_time is None, (
            "only the embedded forecast's filename carries a 12-digit issue token"
        )

    def test_a_missing_sidecar_is_none_and_a_warning_naming_the_file(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """FM-01: the body is durable before its sidecar, so this window is real."""
        raw_path = _seed_daily_wind_bronze(tmp_path, BST_DATE, write_sidecar=False)

        with caplog.at_level(logging.WARNING):
            assert provenance_for(raw_path) is None

        assert "raw_20260816T182500Z_abcd1234.meta.json" in caplog.text

    @pytest.mark.parametrize(
        ("sidecar_kwargs", "needle"),
        [
            ({"ckan_last_modified": None}, "ckan_last_modified"),
            ({"ckan_last_modified": "not-a-timestamp"}, "unparseable ckan_last_modified"),
            ({"drop": "ckan_last_modified"}, "ckan_last_modified"),
            ({"drop": "resource_filename"}, "resource_filename"),
        ],
    )
    def test_unusable_provenance_is_none_and_never_a_fetch_clock_substitute(
        self,
        tmp_path: Path,
        caplog: pytest.LogCaptureFixture,
        sidecar_kwargs: dict[str, Any],
        needle: str,
    ) -> None:
        """FM-05: no fabricated ``published_at``, ever -- a loud skip instead."""
        sidecar = _sidecar_payload(
            written_at=datetime(2026, 8, 16, 18, 25, tzinfo=UTC), **sidecar_kwargs
        )
        raw_path = _seed_daily_wind_bronze(tmp_path, BST_DATE, sidecar=sidecar)

        with caplog.at_level(logging.WARNING):
            assert provenance_for(raw_path) is None

        assert needle in caplog.text


class TestDailyWindReadBronze:
    """``read_bronze_file`` is the branch ``VINTAGE_PER_BRONZE_FILE`` uses."""

    def test_it_yields_a_frame_carrying_published_at_from_the_sidecar(self, tmp_path: Path) -> None:
        raw_path = _seed_daily_wind_bronze(tmp_path, BST_DATE)

        frame = DailyWindAvailabilityTransformer(tmp_path).read_bronze_file(raw_path)

        assert frame.height == DAILY_WIND_FIXTURE_ROWS
        assert list(frame.columns) == [*EXPECTED_COLUMNS, "published_at"]
        assert frame["published_at"].to_list() == [PUBLISHED_AT] * DAILY_WIND_FIXTURE_ROWS

    def test_unusable_provenance_yields_an_empty_frame_rather_than_a_guess(
        self, tmp_path: Path
    ) -> None:
        """D-23 -> `base.py`'s `UNUSABLE_PROVENANCE` skip, which D-41 accounts."""
        raw_path = _seed_daily_wind_bronze(tmp_path, BST_DATE, write_sidecar=False)

        assert DailyWindAvailabilityTransformer(tmp_path).read_bronze_file(raw_path).is_empty()

    def test_a_drifted_vendor_header_raises_rather_than_being_absorbed(
        self, tmp_path: Path
    ) -> None:
        """D-19: the header contract is exact and ordered, and it fails loud."""
        raw_path = _seed_daily_wind_bronze(
            tmp_path, BST_DATE, body=b"BMU_ID,Date,MWh\nABRTW-1,2026-08-16,120.5\n"
        )

        with pytest.raises(CsvHeaderDriftError):
            DailyWindAvailabilityTransformer(tmp_path).read_bronze_file(raw_path)

    def test_read_bronze_returns_exactly_what_the_per_file_loop_would_read(
        self, tmp_path: Path
    ) -> None:
        """The ABC method is unused on this branch, so it is implemented HONESTLY.

        A ``raise NotImplementedError`` would be a lie about a method the base
        class declares; a loop over the same glob in the same partition is the
        truthful implementation, and this pins that the two agree.
        """
        first = _seed_daily_wind_bronze(tmp_path, BST_DATE, name="first")
        _seed_daily_wind_bronze(tmp_path, BST_DATE, name="second")
        transformer = DailyWindAvailabilityTransformer(tmp_path)

        combined = transformer.read_bronze(BST_DATE)

        assert combined.height == 2 * DAILY_WIND_FIXTURE_ROWS
        assert list(combined.columns) == list(transformer.read_bronze_file(first).columns)

    def test_read_bronze_is_empty_for_a_date_with_no_partition(self, tmp_path: Path) -> None:
        assert DailyWindAvailabilityTransformer(tmp_path).read_bronze(GMT_DATE).is_empty()


class TestDailyWindTransform:
    """D-24's column contract and D-25's derived instant."""

    def test_it_maps_the_vendor_header_onto_the_d24_silver_columns(self, tmp_path: Path) -> None:
        frame = _transform_body(tmp_path, DAILY_WIND_FIXTURE_BYTES)

        assert {
            "bmu_id",
            "availability_date",
            "availability_mw",
            "timestamp_utc",
            "published_at",
        } <= set(frame.columns)
        assert frame.schema["bmu_id"] == pl.Utf8
        assert frame.schema["availability_date"] == pl.Date
        assert frame.schema["availability_mw"] == pl.Float64
        assert frame.schema["timestamp_utc"] == pl.Datetime("us", "UTC")
        assert frame.schema["published_at"] == pl.Datetime("us", "UTC")

    def test_bm_unit_ids_are_stored_verbatim_with_no_normalisation(self, tmp_path: Path) -> None:
        """CLAUDE.md hard rule: BM unit ids are stored as-is."""
        frame = _transform_body(
            tmp_path, _csv("t_abru-1,2026-08-16,10.0\nABRTW-1 ,2026-08-16,11.0\n")
        )

        assert sorted(frame["bmu_id"].to_list()) == ["ABRTW-1 ", "t_abru-1"]

    def test_a_bst_availability_date_yields_2300z_the_previous_day(self, tmp_path: Path) -> None:
        """D-25: ``settlement_period_to_utc(availability_date, 1)``, not midnight UTC."""
        frame = _transform_body(tmp_path, _csv("ABRTW-1,2026-08-16,120.5\n"))

        assert frame["timestamp_utc"].to_list() == [BST_DAY_START]

    def test_a_gmt_availability_date_yields_0000z_the_same_day(self, tmp_path: Path) -> None:
        frame = _transform_body(tmp_path, _csv("ABRTW-1,2026-01-15,110.75\n"))

        assert frame["timestamp_utc"].to_list() == [GMT_DAY_START]

    def test_the_derived_instant_is_never_the_ingest_windows_end(self, tmp_path: Path) -> None:
        """Why D-25 exists: with no ``timestamp_utc``, ``_event_time_expr`` falls
        back to midnight of the TARGET DATE -- which, since D-13 partitions on the
        ingest window's end, would stamp every row with the fetch window rather
        than its own availability day.
        """
        target_date = date(2026, 8, 19)
        frame = _transform_body(
            tmp_path, _csv("ABRTW-1,2026-08-16,120.5\n"), target_date=target_date
        )

        assert frame["timestamp_utc"].to_list() == [BST_DAY_START]
        assert frame["timestamp_utc"].to_list() != [
            datetime(target_date.year, target_date.month, target_date.day, tzinfo=UTC)
        ]

    def test_output_is_uniquely_grained_by_the_entity_key(self, tmp_path: Path) -> None:
        """D-24: ``(bmu_id, availability_date, published_at)``. One vendor response
        cannot carry two truths for one key at one publication instant.
        """
        frame = _transform_body(
            tmp_path,
            _csv("ABRTW-1,2026-08-16,120.5\nABRTW-1,2026-08-16,131.0\nACHLW-1,2026-08-16,98\n"),
        )

        assert frame.height == 2
        assert (
            frame.select(list(DailyWindAvailabilityTransformer.ENTITY_KEY_COLUMNS))
            .is_unique()
            .all()
        )
        assert frame.filter(pl.col("bmu_id") == "ABRTW-1")["availability_mw"].to_list() == [131.0]

    def test_a_non_numeric_mw_value_raises_at_cast_rather_than_being_nulled(
        self, tmp_path: Path
    ) -> None:
        """D-19: ``strict=True``. A silently-nulled MW is a fabricated availability."""
        with pytest.raises(pl.exceptions.InvalidOperationError):
            _transform_body(tmp_path, _csv("ABRTW-1,2026-08-16,not-a-number\n"))

    def test_a_non_iso_date_raises_at_cast(self, tmp_path: Path) -> None:
        """The date format is part of the same hand-authored guess as the header,
        so it must fail loud rather than null the authoritative date column.
        """
        with pytest.raises(pl.exceptions.InvalidOperationError):
            _transform_body(tmp_path, _csv("ABRTW-1,16/08/2026,120.5\n"))

    def test_no_output_column_is_a_naive_datetime(self, tmp_path: Path) -> None:
        frame = _transform_body(tmp_path, DAILY_WIND_FIXTURE_BYTES)

        naive = [
            name
            for name, dtype in frame.schema.items()
            if isinstance(dtype, pl.Datetime) and dtype.time_zone is None
        ]
        assert naive == []

    def test_an_empty_frame_transforms_to_an_empty_frame(self, tmp_path: Path) -> None:
        assert DailyWindAvailabilityTransformer(tmp_path).transform(pl.DataFrame()).is_empty()

    def test_it_never_touches_the_exclusion_counter(self, tmp_path: Path) -> None:
        """D-40: this dataset excludes no row, so the counter must stay 0.

        The exclusion behaviour belongs to B3a's embedded forecast (D-27). A
        transformer that incremented it here would report
        ``completed_with_warnings`` for a clean run.
        """
        raw_path = _seed_daily_wind_bronze(tmp_path, BST_DATE)
        transformer = DailyWindAvailabilityTransformer(tmp_path)

        transformer.transform(transformer.read_bronze_file(raw_path))

        assert transformer.last_excluded_row_count == 0


class TestDailyWindRunAndRegistration:
    """The class attributes and the base-class contracts they have to satisfy."""

    def test_the_class_attributes_match_d02_d21_and_d24(self) -> None:
        cls = DailyWindAvailabilityTransformer
        assert (cls.source, cls.dataset) == ("neso_data_portal", DATASET)
        assert cls.APPEND_ONLY is True
        assert cls.VINTAGE_PER_BRONZE_FILE is True
        assert cls.BRONZE_BODY_GLOB == "raw_*.csv"
        assert cls.DATASET_VERSION == "1.0.0"
        assert cls.ENTITY_KEY_COLUMNS == ("bmu_id", "availability_date", "published_at")
        assert DATASETS[DATASET].expected_columns == EXPECTED_COLUMNS, (
            "the transformer's header contract drifted from the connector's, so a body "
            "admitted at fetch time would be rejected at transform time (or worse)"
        )

    def test_the_transformer_is_registered_for_this_source(self) -> None:
        """D-38: the ``__init__.py`` import line is what fires registration."""
        assert ("neso_data_portal", DATASET) in list_transformers("neso_data_portal")
        assert (
            get_transformer_class("neso_data_portal", DATASET) is DailyWindAvailabilityTransformer
        )

    def test_run_writes_a_vintage_suffixed_silver_file_with_utc_bitemporal_columns(
        self, tmp_path: Path
    ) -> None:
        """The ``published_at`` dtype contract at ``base.py:1681`` is exercised
        here: a String or naive ``published_at`` raises ``TypeError`` inside
        ``_add_bitemporal_columns`` rather than mistyping ``available_at``.
        """
        _seed_daily_wind_bronze(tmp_path, BST_DATE)
        transformer = DailyWindAvailabilityTransformer(tmp_path)

        rows = transformer.run(BST_DATE, run_id="t09")

        assert rows == DAILY_WIND_FIXTURE_ROWS
        silver_dir = tmp_path / "silver" / "neso_data_portal" / DATASET / "year=2026" / "month=08"
        written = sorted(silver_dir.glob("*.parquet"))
        assert len(written) == 1
        frame = pl.read_parquet(written[0])
        assert frame.schema["event_time"] == pl.Datetime("us", "UTC")
        assert frame.schema["available_at"] == pl.Datetime("us", "UTC")
        assert frame["available_at"].to_list() == frame["published_at"].to_list(), (
            "ADR-025 S3: NESO's publication instant, not our capture instant, is the "
            "vintage axis (D-22)"
        )
        assert transformer.last_validation_failure_count == 0
        assert transformer.last_excluded_row_count == 0

