"""Abstract base connector and RawResponse data class."""

from __future__ import annotations

import asyncio
import logging
import ssl
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from time import monotonic
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Protocol, runtime_checkable

import certifi
import httpx

from gridflow.utils.retry import RETRY_POLICY

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Awaitable, Mapping, Sequence
    from pathlib import Path

    from gridflow.config.settings import SourceConfig

logger = logging.getLogger(__name__)

# The next free send slot per source, shared by every connector instance in the
# process: the runner builds a new connector for each dataset and backfill
# chunk, and a vendor's rate limit spans them all.
_NEXT_SEND_AT: dict[str, float] = {}


def _make_ssl_context() -> ssl.SSLContext:
    """Return the SSL context every connector verifies vendors with.

    truststore verifies against the OS certificate store, so a corporate proxy
    whose root only the OS trusts works from the CLI as it does in a browser.
    Without truststore, fall back to the certifi bundle.

    Python 3.13 enables ssl.VERIFY_X509_STRICT by default, which rejects CA
    certificates that don't mark Basic Constraints as critical. Several public
    CA chains (ENTSO-E, GIE) pre-date this requirement, so clear the flag either way.
    """
    try:
        import truststore
    except ImportError:
        ctx = ssl.create_default_context()
        ctx.load_verify_locations(certifi.where())
    else:
        ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.verify_flags &= ~ssl.VERIFY_X509_STRICT
    return ctx


@dataclass(frozen=True)
class RawResponse:
    """Immutable container for a raw API response + provenance metadata."""

    body: bytes
    content_type: str  # application/json, text/xml, text/csv
    source: str  # e.g. "elexon"
    dataset: str  # e.g. "system_prices"
    fetched_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    request_url: str = ""
    request_params: dict[str, Any] = field(default_factory=dict)
    api_version: str = ""
    page: int = 1
    total_pages: int = 1
    http_status: int = 200
    # The calendar date the data refers to (used for bronze directory partitioning).
    # When set, the writer partitions by data_date rather than fetched_at.
    data_date: date | None = None
    # C-8 (D-8): the number of records this response carries, three-valued.
    #   None  -- the connector did not determine a count (today's behaviour;
    #            treated as evidence at the ingest boundary, same as before C-8).
    #   0     -- the vendor returned zero records (a parsed, empty body).
    #   > 0   -- that many records were parsed.
    # A parse FAILURE stamps None, never 0 -- conflating "could not count" with
    # "counted zero" would turn every malformed body into a permanent frontier
    # freeze. Deliberately absent from the bronze sidecar (D-7): it is an
    # in-process signal only, never written to the immutable bronze metadata.
    record_count: int | None = None

    @classmethod
    def from_httpx(
        cls,
        resp: httpx.Response,
        *,
        source: str,
        dataset: str,
        params: Mapping[str, Any],
        body: bytes | None = None,
        content_type: str | None = None,
        api_version: str = "v1",
        **fields: Any,
    ) -> RawResponse:
        """Record one httpx reply with its request provenance.

        ``body`` and ``content_type`` replace the reply's own (an ENTSO-E ZIP
        member); ``fields`` sets the paging, ``data_date`` and ``record_count``.
        """
        return cls(
            body=resp.content if body is None else body,
            content_type=content_type or resp.headers.get("content-type", "application/json"),
            source=source,
            dataset=dataset,
            request_url=str(resp.url),
            request_params=dict(params),
            api_version=api_version,
            http_status=resp.status_code,
            **fields,
        )


@dataclass(frozen=True)
class MemberEvent:
    """The outcome of one family member in a bounded capture (ADR-033 P-5).

    Attributes:
        resource_id: The vendor resource the event is about.
        outcome: ``captured`` (``response`` set, publish it), ``unchanged``
            (its newest capture is current: nothing fetched, or a dump
            fetched and byte-identical to it, or a frozen-class dump not yet
            due; ADR-035), ``deferred``
            (another leg captures it; nothing sent), ``absent`` (a listed
            member the vendor no longer serves), or ``failed``.
        response: The captured response, for ``captured`` only.
        extension: The admitted bronze extension, for ``captured`` only.
        detail: Credential-free text for anything but ``captured``.
    """

    resource_id: str
    outcome: Literal["captured", "unchanged", "deferred", "absent", "failed"]
    response: RawResponse | None = None
    extension: str | None = None
    detail: str = ""


class MemberCaptureError(Exception):
    """A bounded capture in which every attempted member failed (ADR-033 P-5)."""


@runtime_checkable
class MemberCaptureConnector(Protocol):
    """A connector whose datasets are families of independently captured members.

    ``run_ingest`` consumes :meth:`iter_members` one member at a time and
    publishes each capture before the next download, so memory stays bounded
    by one body whatever the family size (ADR-033 A9). ``fetch()`` remains the
    contract for every other connector.
    """

    async def __aenter__(self) -> Any:
        """Open the session the members are fetched in."""
        ...

    async def __aexit__(self, *exc: Any) -> None:
        """Close the session."""
        ...

    def bind_data_dir(self, data_dir: Path) -> None:
        """Bind to the data root before the first send."""
        ...

    def iter_members(
        self, dataset: str, start: datetime, end: datetime
    ) -> AsyncIterator[MemberEvent]:
        """Yield one :class:`MemberEvent` per family member."""
        ...


class BaseConnector(ABC):
    """Abstract base for all API connectors."""

    source_name: str  # Must be set by subclasses

    SNAPSHOT_ONLY: ClassVar[bool] = False
    """Declare ``True`` when this source serves only the vendor's CURRENT state.

    What declaring it costs the source: **backfill, permanently**. Every
    ``gridflow backfill`` invocation against it is refused before the chunk
    loop, whatever the window and whatever ``--chunk-days``. There is no
    override flag; a source that can serve history should not declare it.

    Declare it when the vendor publishes whole-file snapshots with no
    server-side date filter. A backfill there re-downloads the identical bytes
    once per chunk, retains one duplicate vintage per chunk, and fires the whole
    series at a vendor that may rate-limit or block — cost with no information.

    Interrogated **generically**, through
    :func:`gridflow.pipeline.runner.assert_backfillable`, which resolves the
    connector class from the registry and reads this attribute. The CLI names no
    source: a capability is a property of the connector, not a literal in a
    command. Defaulting to ``False`` is what keeps every existing source
    unaffected.
    """

    PACED_SENDS: ClassVar[bool] = False
    """Space this source's sends by ``rate_limit_per_second`` (see ``_throttle_request``)."""

    last_skipped_units: int = 0
    """Count of sub-fetch units skipped in the most recent ``fetch()`` (CH-COR-01).

    A "unit" is one independently-fetched slice of a dataset whose failure a
    connector tolerates while continuing the rest — a GIE country or a NESO
    request window. Reset to 0 at the start of every fetch (so a reused
    connector never inherits a prior call's count) and set once, from a
    post-loop tally of the failures, after all units have been attempted.

    The CLI reads it after the fetch to thread the skipped total into
    ``PipelineRunTracker.complete_with_warnings`` — so a partial ingest is
    recorded as ``completed_with_warnings`` rather than silently ``success``
    (parallel to the transformer's ``last_unmapped_count``; CLAUDE.md hard rule
    that swallowed failures must surface). Connectors that raise on any failure
    leave this at 0; connectors that tolerate per-unit failures (GIE, NESO)
    re-raise only when *every* attempted unit failed, so an all-fail run is a
    hard ``failed`` and a partial run carries the count.
    """

    def __init__(self, config: SourceConfig):
        self.config = config
        self._client: httpx.AsyncClient | None = None
        self._semaphore: Any = None  # Set in __aenter__

    @abstractmethod
    async def fetch(
        self,
        dataset: str,
        start: datetime,
        end: datetime,
    ) -> list[RawResponse]:
        """Fetch raw data for a date range. Returns list of raw responses
        (one per API page/call). Each RawResponse includes body + metadata."""
        ...

    @abstractmethod
    def list_datasets(self) -> list[str]:
        """Return available datasets for this source."""
        ...

    async def __aenter__(self) -> BaseConnector:
        self._semaphore = asyncio.Semaphore(self.config.rate_limit_per_second)
        self._client = httpx.AsyncClient(
            base_url=self.config.base_url,
            timeout=self.config.timeout,
            headers=self._auth_headers(),
            verify=_make_ssl_context(),
        )
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._client:
            await self._client.aclose()

    async def _throttle_request(self) -> None:
        """Start this source's sends at least ``1 / rate_limit_per_second`` apart.

        The semaphore only caps how many requests are in flight; a vendor that
        limits the request rate (ENTSO-E rejects bursts, GIE allows 60 calls a
        minute) needs this too. Each call reserves the next free slot before it
        waits, so concurrent requests and the next connector instance queue
        behind it. Call it inside the semaphore, before each send.
        """
        if self.config.rate_limit_per_second <= 0:
            return
        now = monotonic()
        slot = max(now, _NEXT_SEND_AT.get(self.source_name, now))
        _NEXT_SEND_AT[self.source_name] = slot + 1.0 / self.config.rate_limit_per_second
        if slot > now:
            await asyncio.sleep(slot - now)

    async def _get_once(self, path: str, params: dict[str, Any]) -> httpx.Response:
        """One GET inside the concurrency cap, paced when the source asks for it."""
        if self._client is None:
            raise RuntimeError("Connector not initialized. Use 'async with' context manager.")
        if self._semaphore is None:
            raise RuntimeError("Semaphore not initialized. Use 'async with' context manager.")

        async with self._semaphore:
            if self.PACED_SENDS:
                await self._throttle_request()
            resp = await self._client.get(path, params=params)
            self._raise_for_status(resp)
            return resp

    _get = RETRY_POLICY(_get_once)
    """:meth:`_get_once` under the connectors' retry policy: the call fetch code makes."""

    def _raise_for_status(self, resp: httpx.Response) -> None:
        """Reject an error reply. Override for a vendor's error body or empty marker."""
        resp.raise_for_status()

    async def _gather_units(
        self,
        tasks: Sequence[Awaitable[list[RawResponse]]],
        *,
        skip_labels: Sequence[str] | None = None,
    ) -> list[RawResponse]:
        """Fetch independent units concurrently; flatten their responses in input order.

        Without ``skip_labels`` the first failure in input order is raised.
        With one label per task, a unit that raises an ``Exception`` is logged
        and counted in ``last_skipped_units`` instead, and the last such error
        is raised only when no unit returned anything. Cancellation and other
        ``BaseException``s always propagate.
        """
        self.last_skipped_units = 0
        results = await asyncio.gather(*tasks, return_exceptions=True)
        labels = skip_labels if skip_labels is not None else [""] * len(results)
        responses: list[RawResponse] = []
        failures: list[Exception] = []
        for label, result in zip(labels, results, strict=True):
            if isinstance(result, BaseException):
                if skip_labels is None or not isinstance(result, Exception):
                    raise result
                logger.warning("Failed to fetch %s/%s: %s", self.source_name, label, result)
                failures.append(result)
            else:
                responses.extend(result)
        self.last_skipped_units = len(failures)
        if failures and not responses:
            raise failures[-1]
        return responses

    def _auth_headers(self) -> dict[str, str]:
        """Build auth headers from config. Override for non-header auth."""
        if self.config.api_key and self.config.api_key_header:
            return {self.config.api_key_header: self.config.api_key}
        return {}
