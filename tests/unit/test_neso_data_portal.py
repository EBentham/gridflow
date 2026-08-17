"""NESO Data Portal connector unit tests (offline).

Every test here is offline: HTTP is mocked at the transport with respx, and
name resolution is stubbed through the shared ``stub_neso_resolver`` fixture —
respx mocks HTTP but **not** DNS, and under D-39 §1a every send validates its
target by resolving the host. The module-level ``pytestmark`` below is that
opt-in; the live module (T-24) deliberately does not declare it.

T-26 owns the window-admission suite in this file.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import respx

from gridflow.config.settings import DatasetConfig, PipelineSettings, SourceConfig
from gridflow.connectors.neso_data_portal.client import (
    _MAX_INGEST_WINDOW,
    CkanPaginationMismatch,
    NesoDataPortalConnector,
    NesoFutureWindowError,
    NesoHistoricalWindowError,
    NesoWindowTooLongError,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

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


def _source_config(*, max_query_days: int = 1) -> SourceConfig:
    """A NESO source config. ``rate_limit_per_second`` is deliberately high:
    the throttle's own pacing is T-04's subject, not this module's."""
    return SourceConfig(
        base_url=BASE_URL,
        api_key_env="",
        api_key_header="",
        rate_limit_per_second=1000,
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
