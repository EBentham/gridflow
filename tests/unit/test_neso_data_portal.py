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
import socket
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from time import monotonic
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import respx
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
    NesoUnsafeRedirectError,
    NesoWindowTooLongError,
    _resolve_host_addresses,
)
from gridflow.connectors.neso_data_portal.endpoints import DATASETS
from gridflow.silver.csv_bronze import CsvHeaderDriftError, NotCsvBodyError

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
    page_names: list[list[str]], *, count: int | None = None, counts: list[int] | None = None
) -> list[httpx.Response]:
    """Build one ``package_search`` response per page of package names."""
    total = count if count is not None else sum(len(names) for names in page_names)
    responses = []
    for index, names in enumerate(page_names):
        page_count = counts[index] if counts is not None else total
        responses.append(
            httpx.Response(
                200,
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
) -> None:
    """Route a paginated ``package_search`` plus the reconciling ``package_list``."""
    router.get(url__startswith=PACKAGE_SEARCH_URL).mock(
        side_effect=_catalog_pages(page_names, count=count, counts=counts)
    )
    if listed is None:
        listed = [name for names in page_names for name in names]
    router.get(url__startswith=PACKAGE_LIST_URL).mock(
        return_value=httpx.Response(200, json={"success": True, "result": listed})
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
        _wire_catalog(router, [["alpha", "beta"]])

        discovery = _discover(_source_config())

        assert len(discovery.traces) == len(router.calls) == 2
        actions = [trace.action for trace in discovery.traces]
        assert actions == ["package_search", "package_list"]

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
        self: NesoDataPortalConnector, request: httpx.Request, *, stream: bool = False
    ) -> httpx.Response:
        response = await real_send(self, request, stream=stream)
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

        resource = connector._select_resource(payload, spec, dataset)

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

        resource = connector._select_resource(payload, spec, "embedded_wind_solar_forecast")

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
        issued = set(connector._issued_send_tokens)

        replayed = recorded[0]
        token = replayed.extensions[_VALIDATED_MARKER]
        assert token in issued, "precondition: the token was live before observation"
        issued.remove(token)

        assert token not in issued, "a consumed token must not satisfy the observer twice"

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


class TestInitialUrlValidation:
    """D-39 §1a: the vendor-supplied ``resources[].url`` gets exactly the
    guarantee the redirect hops get, because both go through ``_send``.

    Revision 5 sent this URL unvalidated because validation was wired into the
    redirect step rather than into sending. A poisoned catalogue entry is the
    same SSRF vector as a poisoned ``Location``.
    """

    @pytest.mark.parametrize(
        ("url", "addresses", "case"),
        [
            ("http://api.neso.energy/x.csv", ("93.184.216.34",), "plain http"),
            ("https://internal.invalid/x.csv", ("127.0.0.1",), "loopback"),
            ("https://internal.invalid/x.csv", ("10.0.0.7",), "RFC-1918"),
            (
                "https://user:pass@api.neso.energy/x.csv",
                ("93.184.216.34",),
                "userinfo credentials",
            ),
        ],
    )
    def test_a_poisoned_resource_url_is_refused_before_any_request(
        self,
        router: respx.MockRouter,
        monkeypatch: pytest.MonkeyPatch,
        raw_response_spy: list[dict[str, Any]],
        url: str,
        addresses: tuple[str, ...],
        case: str,
    ) -> None:
        payload = _fixture("package_show_daily_wind_availability.json")
        payload["result"]["resources"][0]["url"] = url
        _wire_package_show(router, payload)
        _add_catch_all(router)
        _stub_addresses(monkeypatch, *addresses)

        with pytest.raises(NesoUnsafeRedirectError):
            _run_fetch(_source_config())

        targets = [
            str(call.request.url)
            for call in router.calls
            if _request_kind(call.request) != "package_show"
        ]
        assert targets == [], f"{case}: a request was sent to the rejected target {targets}"
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
