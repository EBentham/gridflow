"""Abstract base connector and RawResponse data class."""

from __future__ import annotations

import ssl
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any, ClassVar

import certifi
import httpx

if TYPE_CHECKING:
    from gridflow.config.settings import SourceConfig


def _make_ssl_context() -> ssl.SSLContext:
    """Return an SSL context compatible with Python 3.12 and 3.13.

    Python 3.13 enables ssl.VERIFY_X509_STRICT by default, which rejects CA
    certificates that don't mark Basic Constraints as critical. Several public
    CA chains (ENTSO-E, GIE) pre-date this requirement. Load the certifi CA
    bundle and clear the strict flag so TLS handshakes succeed on both versions.
    """
    ctx = ssl.create_default_context()
    ctx.load_verify_locations(certifi.where())
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

    last_skipped_units: int = 0
    """Count of sub-fetch units skipped in the most recent ``fetch()`` (CH-COR-01).

    A "unit" is one independently-fetched slice of a dataset whose failure a
    connector tolerates while continuing the rest — a GIE country or a NESO
    request window. Reset to 0 at the top of every ``fetch()`` (so a reused
    connector never inherits a prior call's count) and set once, from a
    post-loop tally of the failures, after all units have been attempted.

    The CLI reads it after the fetch to thread the skipped total into
    ``PipelineRunTracker.complete_with_warnings`` — so a partial ingest is
    recorded as ``completed_with_warnings`` rather than silently ``success``
    (parallel to the transformer's ``last_unmapped_count``; CLAUDE.md hard rule
    that swallowed failures must surface). Connectors that raise on any failure
    (Open-Meteo) leave this at 0; connectors that tolerate per-unit failures
    (GIE, NESO) re-raise only when *every* attempted unit failed, so an
    all-fail run is a hard ``failed`` and a partial run carries the count.

    Set once from a post-loop tally rather than incremented mid-coroutine so the
    CH3 swap to ``asyncio.gather(..., return_exceptions=True)`` is a drop-in.
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
        **params: Any,
    ) -> list[RawResponse]:
        """Fetch raw data for a date range. Returns list of raw responses
        (one per API page/call). Each RawResponse includes body + metadata."""
        ...

    @abstractmethod
    def list_datasets(self) -> list[str]:
        """Return available datasets for this source."""
        ...

    async def __aenter__(self) -> BaseConnector:
        import asyncio

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

    def _auth_headers(self) -> dict[str, str]:
        """Build auth headers from config. Override for non-header auth."""
        if self.config.api_key and self.config.api_key_header:
            return {self.config.api_key_header: self.config.api_key}
        return {}
