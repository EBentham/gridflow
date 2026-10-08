"""NESO Open Data Portal (CKAN) connector — gridflow's first file-download source.

Every other gridflow connector fetches JSON or XML from a query API. This one
discovers a resource in a CKAN catalogue and downloads a whole CSV **file**:
``package_show`` resolves the resource, the resource's ``url`` is a 302
redirector, and the redirect target is a presigned object-store URL. Three
network sends for one logical fetch, all of them vendor-directed.

That shape is why the fetch path here is built around **one primitive** (D-39).
:meth:`NesoDataPortalConnector._send` is the only site in this package that
performs network I/O, and it owns — in one place, so no caller can forget any
of them — target validation (D-08), the 1 req/s throttle (D-07), the retry
boundary, and status classification. Redirects are followed manually, one
validated hop at a time, because an auto-following client would issue two
network sends inside one throttled call.

Distinct from the existing ``neso`` source, which is the Carbon Intensity API
and is not touched by anything here (D-01).

**Accepted residual — URLs held inside httpx's own objects.** ``SafeUrl`` governs
every URL this connector holds, but it cannot govern objects it does not own:
``repr(exc.request)`` on an ``httpx.HTTPStatusError``, ``exc.response.request``,
and httpx's own INFO log line all render the raw URL, including the presigned
query. This is recorded as a decision, not an oversight.

Why it is accepted here, and why the reasoning does NOT transfer:

- **This source is keyless and its data is public.** No API key exists — 24 live
  probes returned 2xx unauthenticated, ``_auth_headers()`` returns ``{}``, and
  every package carries the NESO Open Data Licence with ``private: false``. The
  presigned Cloudflare URL is a 7-day capability for a CSV that anybody can
  obtain by calling ``package_show``. It is a capability URL for open data, not
  a credential, so its appearance in a traceback exposes nothing that was not
  already public.
- The httpx-logger half is filed separately, with the measurement showing that
  gridflow's logging configures the ``"gridflow"`` logger rather than the true
  root, so those records are dropped today.

**A source with real credentials would need this closed**, not merely
documented — a query-parameter API key (ENTSO-E's shape) reaching a traceback is
a different matter entirely. The rationale above is about what the value IS, not
about the mechanism being sufficient.
"""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import logging
import re
import socket
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, ClassVar, NamedTuple, final
from uuid import uuid4

import httpx

from gridflow.bronze.sanitize import sanitize_url
from gridflow.connectors.base import BaseConnector, MemberEvent, RawResponse, _make_ssl_context
from gridflow.connectors.neso_data_portal import captures as captures_module
from gridflow.connectors.neso_data_portal import endpoints
from gridflow.connectors.neso_data_portal import files as files_module
from gridflow.connectors.neso_data_portal import pacer as pacer_module
from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.endpoints import (
    DATASETS,
    CkanDataset,
    build_action_url,
)
from gridflow.connectors.neso_data_portal.pacer import Lane, RunPacer
from gridflow.connectors.registry import register_connector
from gridflow.silver.csv_bronze import CsvBronzeError, read_csv_bronze_body
from gridflow.storage.paths import PathBuilder
from gridflow.utils.retry import RETRY_POLICY

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence
    from pathlib import Path

    from gridflow.config.settings import SourceConfig
    from gridflow.connectors.neso_data_portal.captures import Capture
    from gridflow.connectors.neso_data_portal.endpoints import FamilySpec

logger = logging.getLogger(__name__)

# ``httpx.Request.extensions`` is a per-request dict httpx hands to the
# transport and NEVER serialises onto the wire — the same channel httpx itself
# uses for ``timeout``. So the attestation marker is invisible to NESO and
# cannot perturb a presigned request (D-39 §1b).
_VALIDATED_MARKER = "gridflow_neso_send_token"

# httpx's default is ``accept-encoding: gzip, deflate``. Under any content
# coding ``Content-Length`` describes the ENCODED representation while a decoded
# read yields different bytes, so the two counters cannot be compared. Rather
# than reconcile them, the coding is removed from the path (D-39 §3). Adding
# this header cannot break the presigned signature: the R2 URL is signed with
# ``X-Amz-SignedHeaders=host``, so ``Host`` is the only signed header.
_FILE_LEG_HEADERS = {"Accept-Encoding": "identity"}

_MAX_REDIRECT_HOPS = 3

# ADR-035 P-7 (decision 11): a frozen-class dump is re-checked at most weekly,
# on gridflow's own clock (``end`` dates), never on a CKAN field.
_FROZEN_CADENCE = timedelta(days=7)

_REHASH_CHUNK = 1024 * 1024

# CKAN's own default page size, and what the Stage-A capture used: 129 packages
# in three pages of 50/50/29. Ours to choose, and we send it explicitly rather
# than relying on a server default that could change under us.
_PACKAGE_SEARCH_PAGE_SIZE = 50

# The response headers the snapshot contract records. Not "all headers": a
# provenance file is evidence, and evidence that carries a Set-Cookie or an
# ephemeral CDN ray id is noise that changes every run and defeats hash
# comparison between snapshots.
_TRACED_RESPONSE_HEADERS = ("date", "content-type", "etag", "last-modified")

# D-34 window admission. Covers host clock skew and nothing else.
_FUTURE_WINDOW_TOLERANCE = timedelta(minutes=5)

# ``--end 2026-08-16`` parses to midnight UTC, so a legitimate "yesterday to
# today" explicit window can end up to ~24 h behind the wall clock; 48 h clears
# that with margin. Deliberately NOT tightened: a *recent* historical window is
# indistinguishable from a live one by recency, definitionally, so no value of
# this constant makes it a backfill guard. D-35's capability check is that.
_HISTORICAL_WINDOW_TOLERANCE = timedelta(hours=48)

# Not this connector's taste and not the per-dataset ``max_query_days``: it is
# ``PipelineSettings.max_incremental_lookback_hours`` (168 h), the widest window
# ``run_ingest`` itself can ever resolve. ``resolve_incremental_window`` clamps
# every incremental window to it, so this bound refuses exactly the windows no
# automated path can produce, and no others.
#
# Do NOT "tighten" this to ``max_query_days``. A one-day bound would false-refuse
# an ordinary command recurringly: ``ingest --incremental`` resolves each
# dataset's start from its watermark widened by ``incremental_overlap_hours``
# (72 h), so every run after the first resolves a span of roughly four days.
# ``max_query_days`` is, additionally, dead config — no code in the repo reads
# it. The coupling to the declared 168 h ceiling is pinned by assertion in
# ``tests/unit/test_neso_data_portal.py``, so widening that ceiling fails there
# rather than silently turning this check into a false refusal.
_MAX_INGEST_WINDOW = timedelta(days=7)


class NesoDataPortalError(Exception):
    """Base class for every NESO Data Portal connector failure."""


class CkanActionError(NesoDataPortalError):
    """A CKAN action call failed.

    CKAN reports errors as HTTP 200 with ``{"success": false}`` (verified
    against the live portal), so the envelope — not the status code — is what
    this connector checks.
    """


class CkanPaginationMismatch(NesoDataPortalError):  # noqa: N818
    """The paginated catalogue did not reconcile (D-17).

    The missing ``Error`` suffix is deliberate, not an oversight: D-17 names
    this identifier, and the plan, the ADR and the snapshot materializer all
    refer to it by that name. Renaming it to satisfy N818 would silently break
    a ``grep`` from the decision record to the code, which is worth more here
    than suffix uniformity. Flagged rather than quietly renamed.

    ``rows``/``start`` pagination is CKAN-generic and works today, but it is
    **not** contracted by NESO. A silently short catalogue is worse than no
    catalogue, because it looks complete.
    """


class NesoResourceSelectionError(NesoDataPortalError):
    """The package did not yield exactly one resource matching the contract.

    Zero matches, more than one match, or a matched resource whose CKAN
    ``format`` is not the expected one. Names every resource the package
    actually returned, so a vendor rename is diagnosable from the log alone.
    """


class NesoHttpStatusError(httpx.HTTPStatusError, NesoDataPortalError):
    """A non-2xx response, rendered without the request URL.

    Subclasses ``httpx.HTTPStatusError`` deliberately: ``RETRY_POLICY`` retries
    on that type, so a 5xx must remain retryable. What it does NOT do is let
    httpx build the message — ``raise_for_status()`` interpolates the complete
    signed URL, and tenacity's ``before_sleep_log`` then writes that string to
    the log before every retry. The message here is built from a
    :class:`SafeUrl`, and it is raised ``from None`` so no raw cause is chained
    behind it.
    """


class NesoUnexpectedStatusError(NesoDataPortalError):
    """A successful response that is not a complete-file HTTP 200.

    ``RawResponse.http_status`` is written to the immutable bronze sidecar, so
    it must record what was actually observed. Rather than stamp a status we
    did not see, any non-200 success on the file leg is refused.
    """


class NesoUnexpectedResourceUrlError(NesoDataPortalError):
    """``resources[].url`` is not the stable NESO redirector D-11 contracts for.

    The field is vendor-controlled, and D-11's guarantee — that the presigned
    URL's ``X-Amz-Signature`` never reaches the bronze sidecar — rests entirely
    on it being a redirector rather than an already-resolved target. Bronze is
    **immutable**, so a signature written there cannot be cleaned up afterwards:
    the shape has to be verified before the fetch, not assumed.
    """


class NesoUnsafeRedirectError(NesoDataPortalError):
    """An outbound URL failed D-08's target policy and was never requested."""


class NesoRedirectLoopError(NesoDataPortalError):
    """The redirect chain exceeded :data:`_MAX_REDIRECT_HOPS`."""


class NesoResponseTooLargeError(NesoDataPortalError):
    """The body exceeded the dataset's ``max_download_bytes`` (T-NDP-02)."""


class NesoUnexpectedEncodingError(NesoDataPortalError):
    """The vendor applied a content coding after we asked for ``identity``.

    We asked; we do not guess what the vendor did instead.
    """


class NesoTruncatedBodyError(NesoDataPortalError):
    """The transfer ended early, or fell short of a declared ``Content-Length``."""


class NesoUnexpectedBodyError(NesoDataPortalError):
    """A member body's signature is not one its CKAN format admits (ADR-033 P-7).

    An HTML interstitial or a JSON error envelope served under a ``PDF`` or
    ``CSV`` resource would otherwise reach immutable bronze under a trusted
    extension.
    """


class NesoDatastoreMemberError(NesoDataPortalError):
    """A live datastore member is not one the registry seeds as a CSV dump (ADR-035 P-2).

    The dump URL is built only from a resource id the registry seeds as a
    ``datastore`` resource of the family being captured; anything else fails
    loud before any send, until a registry commit seeds it.
    """


class NesoDumpRedirectError(NesoDataPortalError):
    """A dump download redirected off the ``base_url`` origin (ADR-035 P-3).

    Raised before the off-origin hop is sent. Not an ``httpx`` error, so it is
    never retried.
    """


class NesoFutureWindowError(NesoDataPortalError):
    """The requested window ends in the future (D-34 check 2)."""


class NesoHistoricalWindowError(NesoDataPortalError):
    """The requested window ends too far in the past (D-34 check 3)."""


class NesoWindowTooLongError(NesoDataPortalError):
    """The requested span exceeds what the pipeline itself can resolve (D-34 check 4)."""


class NesoEmptyResourceError(NesoDataPortalError):
    """The resource carried no data row (ADR-023 definitive-absent guard).

    ``record_count`` stays ``None`` and is never replaced by ``0``; an empty
    body is refused before bronze rather than written as a zero-row capture.
    """


@dataclass(frozen=True)
class RequestTrace:
    """Per-HTTP-call evidence for the vault snapshot's ``provenance.json`` (D-32).

    Every field has a **real source** — the request we constructed, the clock,
    the response. None is defaulted or synthesised: a provenance file whose
    fields can be placeholders is a hash-verified record of nothing.

    Attributes:
        action: The CKAN action name.
        params: The normalized query params, key-sorted so two snapshots of the
            same call compare equal.
        started_at: Immediately before the send, tz-aware UTC.
        finished_at: Immediately after the response was received, tz-aware UTC.
        status_code: The final HTTP status.
        headers: Only :data:`_TRACED_RESPONSE_HEADERS`, and only those present.
        body_sha256: Hex digest of the exact response bytes.
    """

    action: str
    params: dict[str, str]
    started_at: datetime
    finished_at: datetime
    status_code: int
    headers: dict[str, str]
    body_sha256: str


@dataclass(frozen=True)
class CatalogDiscovery:
    """A reconciled catalogue snapshot plus the evidence of how it was obtained.

    Returning bare payloads would leave the snapshot materializer with no source
    for ``provenance.json``, which PHASE.md ruling 4 requires in full — so the
    traces travel with the packages rather than being reconstructed later.
    """

    packages: tuple[dict[str, Any], ...]
    traces: tuple[RequestTrace, ...]


@final
class SafeUrl:
    """A URL whose EVERY string form is credential-free, by construction.

    **Why a type and not a sanitising helper.** Three consecutive review passes
    found credential leaks in this connector, and each fix was the same shape:
    render the safe form *at the sites we remembered*. A helper cannot win that
    argument — ``str(url)``, an f-string, ``"{}".format(url)``, an alias, a
    ``logger`` arg, ``raise ... from``, or an exception some library builds out
    of the value are all ordinary spellings, and enumerating them is the losing
    game the plan already abandoned once when it replaced its AST gate with a
    behavioural proof.

    So the credential-bearing form is not reachable by rendering at all. This
    object holds the raw URL privately and defines ``__str__``, ``__repr__`` and
    ``__format__`` to produce the safe form, so every one of those spellings is
    safe **without anyone remembering anything**. The only SUPPORTED accessor
    for the raw bytes is :meth:`unsafe_raw` — deliberately ugly, greppable, and
    asserted by test to be called only where a request is actually built.

    **The claim is "only supported accessor", not "only reachable form".**
    Python has no private attributes: the name-mangled slot is reachable as
    ``_SafeUrl__raw`` and ``dir()`` advertises it. That is acceptable because
    the defect this type prevents is *accidental rendering* — a log line, an
    exception message, a format string — not deliberate extraction by hostile
    code, which no Python object can prevent. The vendor is keyless and the
    URLs are capability URLs for openly licensed data, so a deliberate
    extractor gains nothing a public download would not give them.

    **Two renderings, because the path is only safe when it has been proven.**
    ``_assert_safe_target`` permits any globally-routable host and any path, so
    an arbitrary redirect target's path may itself carry a bearer token. Only
    the redirector — whose path this connector has shape-validated against
    ``/dataset/<pkg>/resource/<id>/download/<file>`` — has a path known to be
    credential-free.

    - :meth:`verified` keeps the path. Use for URLs this connector CONSTRUCTED
      (the CKAN action calls) or has SHAPE-VALIDATED (the redirector).
    - :meth:`opaque` renders origin only. Use for everything vendor-supplied
      and unconstrained — every redirect hop.
    """

    __slots__ = ("__raw", "_display")

    __raw: httpx.URL
    _display: str

    def __init__(self, raw: httpx.URL | str, *, keep_path: bool) -> None:
        parsed = raw if isinstance(raw, httpx.URL) else httpx.URL(raw)
        # `netloc` is host[:port] and EXCLUDES userinfo (verified on the pinned
        # httpx), so the origin cannot carry credentials even when the source
        # URL does.
        origin = f"{parsed.scheme}://{parsed.netloc.decode('ascii')}"
        self.__raw = parsed
        self._display = origin + parsed.path if keep_path else origin

    @classmethod
    def verified(cls, url: httpx.URL | str) -> SafeUrl:
        """For a URL whose path we built or PROVED. Renders scheme://host[:port]/path.

        Never construct this before the proof: a value that says "verified"
        before verification has happened is worse than no marker at all.
        """
        return cls(url, keep_path=True)

    @classmethod
    def opaque(cls, url: httpx.URL | str) -> SafeUrl:
        """For an unconstrained vendor URL. Renders scheme://host[:port] only."""
        return cls(url, keep_path=False)

    # -- every rendering path, all safe ------------------------------------
    def __str__(self) -> str:
        return self._display

    def __repr__(self) -> str:
        return f"SafeUrl({self._display!r})"

    def __format__(self, format_spec: str) -> str:
        return format(self._display, format_spec)

    def __getstate__(self) -> object:
        """Refuse serialisation.

        Pickle would otherwise walk ``__slots__`` and write the raw URL into a
        byte stream that no rendering rule governs — a hole straight through
        the representation guarantee.
        """
        raise TypeError("SafeUrl is not serialisable: it would carry the raw URL")

    # -- components that CANNOT carry credentials --------------------------
    @property
    def scheme(self) -> str:
        return str(self.__raw.scheme)

    @property
    def host(self) -> str:
        return str(self.__raw.host)

    @property
    def port(self) -> int | None:
        port: int | None = self.__raw.port
        return port

    # -- questions, not values ---------------------------------------------
    # The credential-bearing components are exposed as PREDICATES so validation
    # can do its job without handing anyone a string to render. An accessor
    # returning `.query` would be a second supported door out of the type, which
    # is what `unsafe_raw` exists to be the only one of.
    def has_userinfo(self) -> bool:
        return bool(self.__raw.userinfo)

    def has_query(self) -> bool:
        return bool(self.__raw.query)

    def has_fragment(self) -> bool:
        return bool(self.__raw.fragment)

    def path_matches(self, pattern: Sequence[str | frozenset[str] | None]) -> bool:
        """Whole-path segment match, evaluated INSIDE the object.

        The path leaves through no supported accessor, so a caller cannot
        render an unproven one *by accident*. A ``None`` element matches any
        non-empty segment; a ``frozenset`` matches any of its members.
        """
        segments = str(self.__raw.path).split("/")
        if len(segments) != len(pattern) + 1 or segments[0] != "":
            return False
        for actual, expected in zip(segments[1:], pattern, strict=True):
            if expected is None:
                if not actual:
                    return False
            elif isinstance(expected, frozenset):
                if actual not in expected:
                    return False
            elif actual != expected:
                return False
        return True

    def join(self, location: str) -> SafeUrl:
        """Resolve a relative ``Location`` against this URL (RFC 3986).

        The result is :meth:`opaque`: a redirect target is vendor-controlled and
        unproven, path included.

        A malformed ``Location`` is a real vendor failure mode, so httpx's own
        ``InvalidURL`` — which repeats the offending value — is caught here and
        replaced at the boundary rather than propagated.
        """
        try:
            joined = self.__raw.join(location)
        except (httpx.InvalidURL, ValueError):
            raise NesoUnsafeRedirectError(
                f"{self} returned a Location header that is not a resolvable URL"
            ) from None
        return SafeUrl.opaque(joined)

    def unsafe_raw(self) -> httpx.URL:
        """Return the RAW URL, query and all. **Sending only.**

        The only legitimate caller is the code that builds an outbound request:
        the bytes have to go on the wire intact — the presigned target is signed
        with ``X-Amz-SignedHeaders=host`` and any alteration invalidates it.

        Never pass the result to a log, an exception, a format string or the
        bronze sidecar. A test pins this method's production call sites, so a
        new one is a deliberate act rather than an accident.
        """
        raw: httpx.URL = self.__raw
        return raw


async def _resolve_host_addresses(host: str, port: int) -> list[Any]:
    """Resolve ``host`` to every address the connector might connect to.

    A **named, module-level** helper on purpose: it is the single injection
    point tests use to drive a mixed public/private DNS answer, and it is what
    every NESO test module stubs so no real name resolution leaves the default
    suite.

    ``loop.getaddrinfo`` and never the blocking ``socket.getaddrinfo`` — this is
    an async connector and a blocking lookup inside the event loop would stall
    every other coroutine.

    Args:
        host: The hostname to resolve.
        port: The port, passed through to ``getaddrinfo`` so the service-based
            answer matches what httpx will actually connect to.

    Returns:
        Every resolved address as an ``ipaddress`` object.
    """
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return [ipaddress.ip_address(info[4][0]) for info in infos]


class NesoDataPortalConnector(BaseConnector):
    """CKAN two-stage connector: resolve a resource, then download its file.

    The **only** class in this package (D-17). There is deliberately no separate
    ``CkanClient`` type: catalogue discovery and dataset ingest share the same
    throttle, the same retry boundary and the same target policy, and splitting
    them would create a second object that could send.
    """

    source_name = "neso_data_portal"

    SNAPSHOT_ONLY: ClassVar[bool] = True
    """This source serves only the vendor's current snapshot (D-35).

    Every resource is a whole-file republication with no server-side date
    filter, so a backfill would re-download the same file once per chunk and
    retain one identical vintage each time. Refused generically before any
    chunk loop.
    """

    def __init__(self, config: SourceConfig, *, pacer: RunPacer | None = None) -> None:
        """Build an unentered connector.

        Args:
            config: The source configuration.
            pacer: The pacer every send is admitted by. Defaults to the
                process-wide, unbound :func:`~.pacer.shared_pacer`, so every
                instance in the process shares one interval (ADR-033 P-12).
                :meth:`bind_data_dir` swaps in the bound one.
        """
        super().__init__(config)
        self._pacer: RunPacer = pacer if pacer is not None else pacer_module.shared_pacer(config)
        self._issued_send_tokens: set[str] = set()
        self._entered_once = False
        self._bronze_root: Path | None = None
        self._data_paths: PathBuilder | None = None

    def bind_data_dir(self, data_dir: Path) -> None:
        """Bind this connector to a data root before its first send (ADR-033 P-5).

        Runs P-4's runtime freeze pin (every bronze directory of this source is
        a registry key), then swaps in the process-wide pacer bound to
        ``<data_dir>/state/neso_data_portal/pacer.lock`` (P-12), and records the
        bronze root the capture index reads and the data paths the dump leg's
        check stamps live under (ADR-035 P-7).

        Raises:
            RuntimeError: Called after this instance was entered: a send may
                already have escaped the bound pacer and its lock.
            RegistryFreezeError: A bronze directory is not a registry key.
            NesoPacerBusyError: Another bound NESO process holds the lock.
        """
        if self._entered_once:
            raise RuntimeError(
                "bind_data_dir must be called before the connector is first entered; "
                "an earlier session may have sent outside the bound pacer"
            )
        paths = PathBuilder(data_dir)
        captures_module.assert_bronze_dirs_registered(data_dir, registry_module.load_registry())
        self._pacer = pacer_module.shared_pacer(self.config, paths.state_dir(self.source_name))
        self._bronze_root = paths.bronze_source_dir(self.source_name)
        self._data_paths = paths

    async def __aenter__(self) -> NesoDataPortalConnector:
        """Build the client with redirects DISABLED.

        ``follow_redirects=False`` at the client level is D-08's first half:
        redirects are handled manually, one validated hop at a time, so each hop
        is a separate throttled send that has passed the target policy.

        Pacing state is NOT reset here (ADR-033 P-12): it lives in the shared
        pacer, so a dataset handoff or a recreated connector cannot shorten the
        interval.
        """
        self._entered_once = True
        self._semaphore = asyncio.Semaphore(self.config.rate_limit_per_second)
        self._issued_send_tokens = set()
        self._client = httpx.AsyncClient(
            base_url=self.config.base_url,
            timeout=self.config.timeout,
            headers=self._auth_headers(),
            verify=_make_ssl_context(),
            follow_redirects=False,
        )
        return self

    # ------------------------------------------------------------------
    # The fetch primitive — one owner for every byte on the wire (D-39 §1)
    # ------------------------------------------------------------------

    @RETRY_POLICY
    async def _send(
        self,
        request: httpx.Request,
        target: SafeUrl,
        *,
        stream: bool = False,
        lane: Lane = Lane.CKAN,
    ) -> httpx.Response:
        """Send one request. **The only network-I/O site in this package.**

        Nothing else may send — not the client's streaming context-manager
        helper (D-09, retired: it is ``build_request`` + ``send`` + ``aclose``
        in a ``finally``, so it bypasses the throttle and hands back a closed
        response), not ``client.get``, not a module-level ``httpx.get``, not a
        second ``AsyncClient``. The invariant
        is proven behaviourally at the transport rather than by matching source
        text, because the set of syntactic forms that can send is open-ended and
        the set of requests that reach the transport is not.

        Target validation happens **here**, before anything else, so every
        outbound URL is validated by construction: the CKAN action calls, the
        vendor-supplied ``resources[].url``, and every resolved redirect hop
        alike. There is no second call site to forget. Validation runs before
        the semaphore and the throttle, so a rejected target never consumes a
        pacing slot; it sits inside the retry boundary, so each attempt
        re-resolves the host.

        Args:
            request: The fully-built request. Its ``extensions`` are stamped
                with a fresh single-use attestation token.
            stream: ``True`` for the file leg, so the body is consumed by
                :meth:`_read_capped_body` rather than buffered by httpx.
            lane: The pacer lane this send is admitted on (ADR-035). A
                parameter of the retried primitive, so every retry attempt and
                every redirect hop is admitted on the caller's lane. The CKAN
                lane calls :meth:`_throttle_request` with no argument (I-1).

        Returns:
            A 2xx response, or a 3xx that carries a ``Location``. **The caller
            owns ``aclose()``**, in a ``finally``. A returned redirect is
            legitimate only for :meth:`_download_resource`; every other caller
            treats one as a bug and raises.

        Raises:
            NesoUnsafeRedirectError: The target failed D-08's policy. Not an
                ``httpx`` error type, so it propagates on the first attempt
                instead of being retried five times.
            httpx.HTTPStatusError: The response was neither 2xx nor a redirect
                carrying a ``Location`` — including a 304 and a
                ``Location``-less 3xx, which an ``is_error`` gate would return
                as though they were bodies.
        """
        if self._client is None or self._semaphore is None:
            raise RuntimeError("Connector not initialized. Use 'async with' context manager.")

        await self._assert_safe_target(target)

        # Per SEND ATTEMPT, never a per-session nonce: a session-long token
        # would still sit in ``extensions`` on an already-sent request, so
        # resending that object — or copying its extensions onto another —
        # would satisfy the observer while bypassing validation and the
        # throttle. A fresh token per attempt also attests each retry
        # independently rather than letting it inherit the first attempt's word.
        token = uuid4().hex
        self._issued_send_tokens.add(token)
        request.extensions[_VALIDATED_MARKER] = token

        async with self._semaphore:
            if lane is Lane.CKAN:
                await self._throttle_request()
            else:
                await self._throttle_request(lane)
            try:
                response = await self._client.send(request, stream=stream, follow_redirects=False)
            except httpx.RemoteProtocolError as exc:
                # httpx parses the Location header inside send(), even with
                # follow_redirects=False, and its error repeats the offending
                # value. A malformed Location is an ordinary vendor failure
                # mode, so it is translated here — the one point every response
                # passes through — into a typed connector error. Any OTHER
                # protocol error is re-raised untouched. The discriminator is a
                # prefix match on httpx's canonical message (raised verbatim as
                # "Invalid URL in location header: ..." in httpx._client), so a
                # protocol error that merely *mentions* a location header is
                # not swallowed.
                if not str(exc).startswith("Invalid URL in location header:"):
                    raise
                raise NesoUnsafeRedirectError(
                    f"{target} returned a Location header that is not a resolvable URL"
                ) from None

        if response.has_redirect_location:
            return response
        if not response.is_success:
            await response.aclose()
            # NOT raise_for_status(): httpx builds that message from the full
            # signed URL, and tenacity logs it before each retry. `from None`
            # keeps a raw cause off the chain too.
            raise NesoHttpStatusError(
                f"{target} returned HTTP {response.status_code}",
                request=request,
                response=response,
            ) from None
        return response

    async def _throttle_request(self, lane: Lane = Lane.CKAN) -> None:
        """Pace outbound sends to the vendor's published guidance, per lane.

        Admission is the shared :class:`~.pacer.RunPacer` (ADR-033 P-12): the
        CKAN lane (1 req/s) by default, and the datastore lane (2 req/min) for
        dump downloads and ``datastore_search`` calls (ADR-035). The interval
        holds across connector instances, dataset handoffs and, once bound,
        processes. Not hoisted into ``BaseConnector``: that would change the
        pacing of every other source.

        Gates **every** outbound send without exception: each CKAN action call,
        the redirector request, each redirect hop, and each retry attempt —
        because it sits inside :meth:`_send`, which is what ``RETRY_POLICY``
        decorates, directly before the transport call.
        """
        await self._pacer.acquire(lane)

    async def _assert_safe_target(self, url: SafeUrl) -> None:
        """Raise unless ``url`` satisfies D-08's target policy.

        **Called from :meth:`_send` and from nowhere else** (D-39 §1a), and that
        is load-bearing rather than tidy: two consecutive cross-model review
        passes each found an unvalidated URL at a call site someone had to
        remember — first the redirect hops, then the initial vendor-supplied
        ``resources[].url``. Both were the same defect, applied at remembered
        call sites. Inside the send primitive there is nothing to remember.

        Everything CKAN returns is untrusted input: a ``resources[].url`` is
        vendor-controlled catalogue content exactly as a ``Location`` header is,
        and both are SSRF vectors.

        Args:
            url: The absolute target of a request that has not been sent.

        Raises:
            NesoUnsafeRedirectError: Non-``https`` scheme; userinfo present
                (httpx would attach Basic credentials to that host); no host; an
                unresolvable host; an empty DNS answer; or **any** resolved
                address that is not globally routable. Every address, not any:
                an answer mixing a public and a private address passes an
                any-check while httpx may connect to the private one.
        """
        if url.scheme != "https":
            raise NesoUnsafeRedirectError(
                f"refusing to send to {url}: scheme must be https, got {url.scheme!r}"
            )
        if url.has_userinfo():
            raise NesoUnsafeRedirectError(
                f"refusing to send to {url}: the URL carries "
                "userinfo, which httpx would turn into Basic credentials for that host"
            )
        host = url.host
        if not host:
            raise NesoUnsafeRedirectError(f"refusing to send to {url}: no host component")

        port = url.port or 443
        try:
            addresses = await _resolve_host_addresses(host, port)
        except OSError as exc:
            raise NesoUnsafeRedirectError(
                f"refusing to send to {url}: host {host!r} did not resolve ({exc})"
            ) from exc

        if not addresses:
            raise NesoUnsafeRedirectError(
                f"refusing to send to {url}: host {host!r} resolved to no addresses"
            )
        for address in addresses:
            if not address.is_global:
                raise NesoUnsafeRedirectError(
                    f"refusing to send to {url}: host {host!r} resolves to "
                    f"{address}, which is not globally routable"
                )

    def _resolve_redirect_target(self, response: httpx.Response, sent_to: SafeUrl) -> SafeUrl:
        """Resolve a redirect ``Location`` against the URL that sent it.

        **Resolution only, never validation** — validation is :meth:`_send`'s,
        applied to every request without exception. The split exists because
        resolution is the one step that needs the response, and folding the
        policy in here would recreate the remembered-call-site defect.

        RFC-3986 resolution, so a relative ``Location`` (``/path/x.csv``)
        resolves against the host that sent it. Left unresolved it would either
        be rejected as schemeless or, worse, joined against ``base_url`` and
        sent to the wrong host.
        """
        location = response.headers.get("location", "")
        if not location:
            raise NesoDataPortalError(
                "internal error: _resolve_redirect_target called on a response with no Location"
            )
        return sent_to.join(location)

    async def _read_capped_body(
        self, response: httpx.Response, max_bytes: int, target: SafeUrl
    ) -> tuple[bytes, int | None]:
        """Read a streamed body under a hard size cap, and prove it is complete.

        In D-39 §4's order, which is not arbitrary: a declared oversize is
        rejected before a byte is read, the coding is checked before the bytes
        are interpreted, and the running total is what actually bounds memory.

        The body is read with ``aiter_raw()``, so the bytes counted, the bytes
        capped and the bytes written to bronze are the same bytes
        ``Content-Length`` describes. One counter, one meaning.

        Args:
            response: An open, streamed 2xx response.
            max_bytes: The dataset's or family's ``max_download_bytes``.

        Returns:
            The complete raw body, and the response's well-formed
            ``Content-Length`` (``None`` when absent or unparseable).

        Raises:
            NesoResponseTooLargeError: A declared length above the cap, or a
                running total that crosses it mid-stream. The running check is
                load-bearing on its own: ``Content-Length`` may be absent
                (chunked) or understate the body.
            NesoUnexpectedEncodingError: A content coding survived our
                ``identity`` request.
            NesoTruncatedBodyError: The peer closed before the declared body
                completed, or the accumulated total did not equal a declared
                ``Content-Length``.
        """
        declared = _declared_content_length(response)
        if declared is not None and declared > max_bytes:
            raise NesoResponseTooLargeError(
                f"refusing {target}: declared Content-Length {declared} B "
                f"exceeds the {max_bytes} B cap"
            )

        encoding = response.headers.get("content-encoding", "").strip().lower()
        if encoding and encoding != "identity":
            raise NesoUnexpectedEncodingError(
                f"{target} returned Content-Encoding {encoding!r} after "
                "the request asked for 'identity'; refusing to guess what the vendor did"
            )

        chunks: list[bytes] = []
        total = 0
        try:
            async for chunk in response.aiter_raw():
                total += len(chunk)
                if total > max_bytes:
                    raise NesoResponseTooLargeError(
                        f"aborting {target}: body exceeded the {max_bytes} B cap after {total} B"
                    )
                chunks.append(chunk)
        except httpx.RemoteProtocolError as exc:
            raise NesoTruncatedBodyError(
                f"{target} closed mid-transfer after {total} B ({exc})"
            ) from exc

        if declared is not None and total != declared:
            raise NesoTruncatedBodyError(
                f"{target} declared Content-Length {declared} B but delivered {total} B"
            )
        return b"".join(chunks), declared

    # ------------------------------------------------------------------
    # CKAN two-stage fetch
    # ------------------------------------------------------------------

    async def _ckan_action(
        self, action: str, *, lane: Lane = Lane.CKAN, **params: str
    ) -> tuple[Any, RequestTrace]:
        """Call one CKAN action, returning its ``result`` and a request trace.

        **One envelope check, every action.** NESO returns action errors as HTTP
        **200** with ``{"success": false}``, so a status-only check would treat
        an error envelope as a payload. Both callers — the ingest path's
        ``package_show`` and ``discover_catalog``'s ``package_search`` /
        ``package_list`` — go through here so that check cannot diverge.

        The trace is built unconditionally rather than behind a flag: it is the
        sole source for the vault snapshot's ``provenance.json`` (D-32), and a
        field that is only sometimes populated is a field that will one day be a
        placeholder in a hash-verified evidence file.

        Args:
            action: The CKAN action name.
            lane: The pacer lane; :attr:`Lane.DATASTORE` only for
                ``datastore_search`` (ADR-035). The CKAN lane keeps master's
                ``_send(request, target)`` call shape (I-1).
            **params: Query parameters, sent as constructed against ``base_url``
                — never a URL taken from a response body (D-39 §1a).

        Returns:
            The envelope's ``result`` value, and the :class:`RequestTrace` for
            the call.

        Raises:
            CkanActionError: A redirect (action calls are not redirected), a
                non-JSON body, or ``success: false``.
        """
        if self._client is None:
            raise RuntimeError("Connector not initialized. Use 'async with' context manager.")

        path, query = build_action_url(action, **params)
        request = self._client.build_request("GET", path, params=query)
        target = SafeUrl.verified(request.url)

        started_at = datetime.now(UTC)
        if lane is Lane.CKAN:
            response = await self._send(request, target)
        else:
            response = await self._send(request, target, lane=lane)
        try:
            if response.has_redirect_location:
                raise CkanActionError(
                    f"CKAN {action} {query!r} answered with a redirect to "
                    f"{response.headers.get('location')!r}; action calls are not redirected"
                )
            body = response.content
            trace = RequestTrace(
                action=action,
                params=dict(sorted(query.items())),
                started_at=started_at,
                finished_at=datetime.now(UTC),
                status_code=response.status_code,
                headers={
                    name: response.headers[name]
                    for name in _TRACED_RESPONSE_HEADERS
                    if name in response.headers
                },
                body_sha256=hashlib.sha256(body).hexdigest(),
            )
            try:
                payload = json.loads(body)
            except json.JSONDecodeError as exc:
                raise CkanActionError(
                    f"CKAN {action} {query!r} returned a body that is not JSON ({exc})"
                ) from exc
        finally:
            await response.aclose()

        if not isinstance(payload, dict) or not payload.get("success"):
            detail = payload.get("error") if isinstance(payload, dict) else payload
            raise CkanActionError(f"CKAN {action} {query!r} returned success=false: {detail!r}")
        return payload.get("result"), trace

    async def _package_show(self, package: str) -> dict[str, Any]:
        """Resolve one CKAN package.

        Raises:
            CkanActionError: The envelope reported failure, the body was not a
                CKAN envelope, or the package is definitively absent (HTTP 404 —
                ADR-023 definitive-absent; one dataset per ``fetch()``, so there
                is no sibling to keep going for).
        """
        try:
            result, _trace = await self._ckan_action("package_show", id=package)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise CkanActionError(
                    f"CKAN package {package!r} does not exist (HTTP 404)"
                ) from exc
            raise

        if not isinstance(result, dict):
            raise CkanActionError(f"CKAN package_show for {package!r} returned no result object")
        return result

    async def discover_catalog(self) -> CatalogDiscovery:
        """Page the whole CKAN catalogue and reconcile it against ``package_list``.

        **Placement — do NOT "helpfully" move this onto the ingest path.** The
        per-dataset fetch never calls ``package_search`` at all (D-06 uses
        ``package_show``), so there would be nothing to reconcile there, and it
        would cost 4+ extra CKAN requests per ingest run against a 1 req/s
        budget. It is created here because it is a method on the connector; it
        is *invoked* by the snapshot materializer and by an opt-in live smoke
        test. Permanent check, cheapest correct placement.

        **The reconciliation is a permanent sanity check, not a one-off audit.**
        ``rows``/``start`` pagination is CKAN-generic and working today, but it
        is **not** contracted by NESO. So the paginated name-set is compared
        against ``package_list`` on every run and a mismatch fails loudly rather
        than silently returning a short catalogue — a snapshot missing packages
        is worse than no snapshot, because it looks complete.

        Pagination advances by ``rows``/``start`` parameters **we construct
        ourselves** against ``base_url``. No URL from a response body is ever
        fetched (D-39 §1a).

        D-34's window guard does not apply: this is a catalogue call, not a
        dataset fetch.

        Returns:
            Every package payload, plus one ordered :class:`RequestTrace` per
            HTTP call — the sole source for the snapshot's ``provenance.json``.

        Raises:
            CkanPaginationMismatch: ``count`` changed mid-pagination, a package
                name repeated across pages, or the paginated name-set differs
                from ``package_list``.
            CkanActionError: Any action returned a failed envelope.
        """
        packages: list[dict[str, Any]] = []
        traces: list[RequestTrace] = []
        seen: dict[str, None] = {}
        declared_count: int | None = None
        start = 0

        while True:
            result, trace = await self._ckan_action(
                "package_search", rows=str(_PACKAGE_SEARCH_PAGE_SIZE), start=str(start)
            )
            traces.append(trace)
            if not isinstance(result, dict):
                raise CkanActionError("CKAN package_search returned no result object")

            count = result.get("count")
            if not isinstance(count, int):
                raise CkanActionError(f"CKAN package_search returned a non-integer count {count!r}")
            if declared_count is None:
                declared_count = count
            elif count != declared_count:
                raise CkanPaginationMismatch(
                    f"CKAN package_search count changed mid-pagination: {declared_count} at "
                    f"start=0, {count} at start={start}. The catalogue moved under the "
                    "paginator, so the collected set is neither the old catalogue nor the new."
                )

            page = result.get("results")
            if not isinstance(page, list):
                raise CkanActionError("CKAN package_search returned no results list")
            if not page:
                break

            for entry in page:
                name = str(entry.get("name", ""))
                if name in seen:
                    raise CkanPaginationMismatch(
                        f"CKAN package_search returned package {name!r} on more than one "
                        f"page (at start={start}); a duplicate means the page window "
                        "shifted and some package was skipped"
                    )
                seen[name] = None
                packages.append(entry)

            start += len(page)
            if start >= declared_count:
                break

        # The paginated set must ACCOUNT FOR the count the vendor declared.
        # Without this, a run that stops on an early empty page returns a short
        # catalogue, and if package_list happens to agree with the short set the
        # reconciliation below reports success — laundering an incomplete
        # snapshot as a verified one, which is strictly worse than no check at
        # all. Verified against the Stage-A capture: 50 + 50 + 29 == count 129.
        if declared_count is None:
            raise CkanActionError("CKAN package_search returned no pages at all")
        if len(seen) != declared_count:
            raise CkanPaginationMismatch(
                f"CKAN package_search declared count={declared_count} but pagination "
                f"collected {len(seen)} packages. The catalogue is incomplete, so it must "
                "not be reconciled or snapshotted as though it were whole."
            )

        listed, trace = await self._ckan_action("package_list")
        traces.append(trace)
        if not isinstance(listed, list):
            raise CkanActionError("CKAN package_list returned no result list")
        listed_names = {str(name) for name in listed}

        if listed_names != set(seen):
            missing = sorted(listed_names - set(seen))[:10]
            unexpected = sorted(set(seen) - listed_names)[:10]
            raise CkanPaginationMismatch(
                f"CKAN catalogue reconciliation failed: package_search yielded "
                f"{len(seen)} packages, package_list yielded {len(listed_names)}. "
                f"In package_list but not paginated (up to 10): {missing}. "
                f"Paginated but not in package_list (up to 10): {unexpected}."
            )

        return CatalogDiscovery(packages=tuple(packages), traces=tuple(traces))

    async def datastore_fields(self, resource_id: str) -> tuple[Any, RequestTrace]:
        """Ask ``datastore_search`` for one resource's field info and no rows (ADR-035 P-11).

        The parameters are built here (``limit=0``) and sent on the datastore
        lane; no ``_links`` URL in a response is ever followed. Row refusal is
        the caller's (the field-info evidence builder), so a vendor that ignores
        ``limit`` is refused before anything is written.

        Args:
            resource_id: A canonical lowercase resource UUID.

        Returns:
            The envelope's ``result`` and the request trace.

        Raises:
            NesoDatastoreMemberError: ``resource_id`` is not canonical.
            CkanActionError: The envelope reported failure.
        """
        if not endpoints.is_canonical_resource_id(resource_id):
            raise NesoDatastoreMemberError(
                f"datastore resource id {resource_id!r} is not a canonical lowercase UUID"
            )
        return await self._ckan_action(
            "datastore_search", lane=Lane.DATASTORE, resource_id=resource_id, limit="0"
        )

    def _select_resource(
        self,
        package_payload: dict[str, Any],
        spec: CkanDataset,
        dataset: str,
    ) -> tuple[dict[str, Any], SafeUrl]:
        """Select the one resource whose name matches the contract exactly (D-04).

        Exact-string match and nothing else: no fuzzy match, no
        "Archive"-substring fallback, no ``last_modified`` tie-break. The raw
        filenames are date-stamped and change on refresh, so the name is the
        only stable selector, and the UUIDs are provenance rather than
        selectors.

        Raises:
            NesoResourceSelectionError: Zero matches, more than one match, or a
                matched resource whose CKAN ``format`` is not the expected one
                (D-10 — the format is what ``content_type`` is stamped from).
        """
        resources = package_payload.get("resources")
        if not isinstance(resources, list):
            raise NesoResourceSelectionError(
                f"{dataset}: CKAN package {spec.package!r} carried no resources list"
            )

        actual_names = [str(item.get("name")) for item in resources if isinstance(item, dict)]
        matches = [
            item
            for item in resources
            if isinstance(item, dict) and item.get("name") == spec.resource_name
        ]
        if len(matches) != 1:
            raise NesoResourceSelectionError(
                f"{dataset}: expected exactly one resource named {spec.resource_name!r} in "
                f"CKAN package {spec.package!r}, found {len(matches)}; the package returned "
                f"{actual_names!r}"
            )

        resource = matches[0]
        declared_format = str(resource.get("format", ""))
        if declared_format.upper() != spec.expected_format:
            raise NesoResourceSelectionError(
                f"{dataset}: resource {spec.resource_name!r} declares CKAN format "
                f"{declared_format!r}, expected {spec.expected_format!r}; refusing to stamp "
                "content_type from a format we did not verify"
            )
        return resource, self._assert_redirector_url(package_payload, resource, dataset)

    def _assert_redirector_url(
        self,
        package_payload: dict[str, Any],
        resource: dict[str, Any],
        dataset: str,
    ) -> SafeUrl:
        """Raise unless ``resources[].url`` is the stable NESO redirector (D-11).

        **Why this is a check and not an assumption.** ``request_url`` is copied
        into the bronze sidecar, and D-11 requires it to be the redirector
        precisely so the presigned target's ``X-Amz-Signature`` and 7-day expiry
        never land in provenance. But the field is vendor-controlled: if CKAN
        ever returned an already-resolved presigned URL — or a
        ``url_type: datastore`` dump path — the connector would record it.
        Bronze is immutable, so that is not correctable afterwards.

        **Every component is checked, and the path is matched WHOLE.** An
        earlier version searched for the resource-path substring and ignored
        port and fragment, which admitted
        ``https://host:8443/anything/resource/<id>/download/f.csv#access_token=x``
        — right host, right substring, arbitrary origin and a fragment carrying
        anything at all. A URL is only the redirector if every component says
        so, so the segments are compared as a list rather than searched.

        This is **one instance of the emission invariant, not the whole of it**:
        :class:`SafeUrl` is what guarantees credentials cannot leave by any
        path. This check guarantees the *sidecar* records the redirector, which
        is a stronger statement than "records something without a query".

        It does not replace D-08's target policy either: shape asks "is this the
        redirector we contracted for", the address check asks "where does this
        actually resolve". Shape here, addresses in :meth:`_send`.

        Raises:
            NesoUnexpectedResourceUrlError: Naming every way the URL departed
                from the contract, rendered through :class:`SafeUrl` so the
                refusal cannot leak what it exists to protect.
        """
        raw_url = str(resource.get("url", ""))
        if not raw_url:
            raise NesoUnexpectedResourceUrlError(f"{dataset}: the selected resource carries no url")

        # OPAQUE while unproven. Constructing a `verified` value before the
        # checks run would mark something verified that has not been, and the
        # unproven path would then be renderable by the very messages that
        # report why it failed.
        url = SafeUrl.opaque(raw_url)
        base = httpx.URL(self.config.base_url)
        resource_id = str(resource.get("id", ""))
        # CKAN builds this path from the package's id; its name is the other
        # stable identifier for the same package, so both are accepted. Neither
        # is secret and neither is attacker-chosen — they come from the payload
        # whose resource we already selected by exact name.
        package_keys = {
            str(package_payload.get("id", "")),
            str(package_payload.get("name", "")),
        } - {""}
        problems: list[str] = []

        if url.scheme != "https":
            problems.append(f"scheme is {url.scheme!r}, expected 'https'")
        if url.host != base.host:
            problems.append(f"host is {url.host!r}, expected {base.host!r}")
        if url.port != base.port:
            problems.append(f"port is {url.port!r}, expected {base.port!r}")
        if url.has_userinfo():
            problems.append("it carries userinfo, which the redirector never does")
        if url.has_query():
            problems.append(
                "it carries a query string, which the redirector never does — an "
                "already-resolved presigned URL would put X-Amz-Signature into the "
                "immutable bronze sidecar (D-11)"
            )
        if url.has_fragment():
            problems.append(
                "it carries a fragment, which the redirector never does and which can "
                "carry credentials of its own"
            )

        if not resource_id:
            problems.append("the resource declares no id to match the path against")
        elif not package_keys:
            problems.append("the package declares neither an id nor a name")
        elif not url.path_matches(
            ("dataset", frozenset(package_keys), "resource", resource_id, "download", None)
        ):
            # The offending path is NOT echoed: it is unproven, and explaining
            # why a path was rejected is a poor reason to render it.
            problems.append(
                "its path is not exactly "
                f"/dataset/<package>/resource/{resource_id}/download/<filename>"
            )

        if problems:
            raise NesoUnexpectedResourceUrlError(
                f"{dataset}: resource url {url!r} is not the stable NESO "
                f"redirector D-11 requires: {'; '.join(problems)}"
            )

        # Only NOW is the path proven, so only now is a `verified` value —
        # whose rendering includes the path — a true statement about it.
        return SafeUrl.verified(raw_url)

    async def _download_resource(
        self,
        resource: dict[str, Any],
        redirector: SafeUrl,
        max_bytes: int,
        dataset: str,
    ) -> tuple[bytes, str, int, int | None]:
        """Download one resource through its redirector, validating every hop.

        Each iteration builds a **fresh** GET, which regenerates ``Host`` from
        the target and copies no per-host header forward — the 302 sets three
        cookies on ``api.neso.energy`` and none of them may cross to the file
        host. The ``finally`` is the whole lifecycle answer: the 302's own body
        is a chunked ``text/html`` payload nobody reads, and without an explicit
        close a streamed 3xx leaks its connection.

        Admission is the caller's: the legacy path applies
        :meth:`_assert_admissible_csv`, the member path :func:`_admit_member_body`.

        Returns:
            The body bytes, the **redirector** URL, the OBSERVED final HTTP
            status — never the presigned target, which carries
            ``X-Amz-Signature`` and a 7-day expiry and must not reach an
            irreproducible bronze sidecar (D-11) — and the final response's
            declared ``Content-Length``.

        Raises:
            NesoRedirectLoopError: The chain exceeded :data:`_MAX_REDIRECT_HOPS`.
        """
        if self._client is None:
            raise RuntimeError("Connector not initialized. Use 'async with' context manager.")

        # The redirector is shape-validated, so its path is proven and may be
        # rendered. Every hop AFTER it is `opaque` — an unconstrained vendor URL
        # whose path could itself carry a bearer token.
        target = redirector
        request = self._client.build_request("GET", target.unsafe_raw(), headers=_FILE_LEG_HEADERS)
        body: bytes | None = None
        final_status: int | None = None
        declared: int | None = None
        for _ in range(_MAX_REDIRECT_HOPS + 1):
            response = await self._send(request, target, stream=True)
            try:
                if response.has_redirect_location:
                    target = self._resolve_redirect_target(response, target)
                    # No validate() call here: _send validates every URL it is
                    # handed (D-39 §1a). There is no second call site to forget.
                    request = self._client.build_request(
                        "GET", target.unsafe_raw(), headers=_FILE_LEG_HEADERS
                    )
                    continue
                # A 2xx that is not 200 does not describe a whole file. 206 in
                # particular is a PARTIAL representation: we never send a Range
                # header, so receiving one means the transfer is not what we
                # asked for, and its body would be a fragment that still parses
                # as valid CSV. Refused rather than recorded.
                if response.status_code != 200:
                    raise NesoUnexpectedStatusError(
                        f"{dataset}: {target} answered HTTP "
                        f"{response.status_code}, which is not a complete-file 200 "
                        "response; no Range request was made, so a partial or "
                        "alternative representation cannot be admitted to bronze"
                    )
                final_status = response.status_code
                body, declared = await self._read_capped_body(response, max_bytes, target)
                break
            finally:
                await response.aclose()

        if body is None or final_status is None:
            raise NesoRedirectLoopError(
                f"{dataset}: {redirector} exceeded {_MAX_REDIRECT_HOPS} redirect hops"
            )

        return body, str(redirector), final_status, declared

    async def _download_dump(self, resource_id: str, max_bytes: int, dataset: str) -> _DumpBody:
        """Download one datastore dump on the datastore lane (ADR-035 P-3).

        The target is built from the registry-seeded id alone (D-39); no URL
        from the payload is ever read. Every send (each hop and each retry) is
        admitted on :attr:`Lane.DATASTORE`. A redirect is followed only to the
        ``base_url`` origin; an off-origin ``Location`` is refused before that
        hop is sent.

        The hop is resolved against the request this method built rather than
        through :meth:`_resolve_redirect_target`, because building the next
        request from a :class:`SafeUrl` would need a new ``unsafe_raw`` call
        site, which the D-39 pin forbids. Both are the same RFC 3986 join.

        Returns:
            The body, the BUILT dump URL (never a hop), the observed status, and
            the final response's ``Content-Length`` and ``Last-Modified``.

        Raises:
            NesoDumpRedirectError: A ``Location`` outside the ``base_url`` origin.
            NesoUnsafeRedirectError: A ``Location`` that is not a resolvable URL.
            NesoUnexpectedStatusError: A 2xx that is not a complete-file 200.
            NesoRedirectLoopError: More than :data:`_MAX_REDIRECT_HOPS` hops.
        """
        if self._client is None:
            raise RuntimeError("Connector not initialized. Use 'async with' context manager.")

        request = self._client.build_request(
            "GET", endpoints.build_dump_path(resource_id), headers=_FILE_LEG_HEADERS
        )
        target = SafeUrl.verified(request.url)
        built = target
        base = httpx.URL(self.config.base_url)
        origin = (base.scheme, base.host, base.port or 443)
        for _ in range(_MAX_REDIRECT_HOPS + 1):
            response = await self._send(request, target, stream=True, lane=Lane.DATASTORE)
            try:
                if response.has_redirect_location:
                    location = response.headers.get("location", "")
                    try:
                        hop = request.url.join(location)
                    except (httpx.InvalidURL, ValueError):
                        raise NesoUnsafeRedirectError(
                            f"{target} returned a Location header that is not a resolvable URL"
                        ) from None
                    if (hop.scheme, hop.host, hop.port or 443) != origin:
                        raise NesoDumpRedirectError(
                            f"{dataset}: dump {resource_id} redirected to {SafeUrl.opaque(hop)}, "
                            f"outside the {SafeUrl.opaque(base)} origin; the hop was not sent"
                        )
                    target = SafeUrl.opaque(hop)
                    request = self._client.build_request("GET", hop, headers=_FILE_LEG_HEADERS)
                    continue
                if response.status_code != 200:
                    raise NesoUnexpectedStatusError(
                        f"{dataset}: dump {resource_id} answered HTTP {response.status_code}, "
                        "which is not a complete-file 200 response; no Range request was made"
                    )
                body, declared = await self._read_capped_body(response, max_bytes, target)
                return _DumpBody(
                    body=body,
                    request_url=str(built),
                    http_status=response.status_code,
                    declared_length=declared,
                    last_modified_header=response.headers.get("last-modified"),
                )
            finally:
                await response.aclose()
        raise NesoRedirectLoopError(
            f"{dataset}: dump {built} exceeded {_MAX_REDIRECT_HOPS} redirect hops"
        )

    def _assert_admissible_csv(
        self,
        body: bytes,
        spec: CkanDataset,
        dataset: str,
        source_label: str,
    ) -> None:
        """D-36 rung 3: parse the body before it can reach immutable bronze.

        ``content_type`` is stamped ``text/csv`` from CKAN metadata rather than
        from the response header (D-10), which is correct for the ``.bin``
        problem but means a JSON error envelope, an HTML interstitial or a
        binary body would otherwise be labelled ``.csv`` and written to bronze,
        where re-running cannot recover it. So the same call silver will make
        later is made once here, as an admission check, and its result is
        discarded — bronze stores the vendor's bytes, never the parsed frame.

        Deliberately **outside** the retry boundary: header drift is a vendor
        change, not a transient fault, and retrying it would be five pointless
        62 MB downloads.

        Raises:
            NotCsvBodyError, CsvHeaderDriftError: From the shared reader.
            NesoEmptyResourceError: The body has no data row after the header.
                ``record_count`` stays ``None``; it is never replaced by ``0``.
        """
        if not body.strip():
            raise NesoEmptyResourceError(
                f"{dataset}: {source_label} returned an empty body; refusing to write "
                "an empty capture to immutable bronze"
            )
        frame = read_csv_bronze_body(
            body,
            expected_columns=spec.expected_columns,
            source_label=source_label,
        )
        if frame.is_empty():
            raise NesoEmptyResourceError(
                f"{dataset}: {source_label} returned a header-only body with no data rows"
            )

    def _assert_window_admissible(self, dataset: str, start: datetime, end: datetime) -> None:
        """Screen the requested window before any network I/O (D-34).

        Four checks, in order, every one of which raises before a byte leaves
        the process. Then — separately, and deliberately **not** a refusal — a
        reinterpretation notice when the span exceeds the dataset's configured
        ``max_query_days``.

        **Scope, stated because it is easy to mistake.** Check 3 is *not* the
        backfill guard: ``SNAPSHOT_ONLY`` (D-35) is, and it holds for every
        window shape and every chunk size because it is decided by what the
        source *is*, not by what the window looks like. Check 3's 48 h
        tolerance is deliberately not tightened — a recent historical window is
        indistinguishable from a live one by recency, definitionally, so
        shaving the constant would be patch-first convergence against a bound
        that cannot be made tight. Its cost is D-13's second residual: bronze
        lands on that older date, so an immediately-following default
        ``--last 24h`` transform may not reach back far enough to see it.

        Check 4's bound comes from ``max_incremental_lookback_hours``, not from
        ``max_query_days`` — see :data:`_MAX_INGEST_WINDOW` for why tightening
        it would false-refuse ``--incremental`` on every run after the first.

        Args:
            dataset: The dataset key, for the ``max_query_days`` notice.
            start: Window start.
            end: Window end.

        Raises:
            ValueError: An endpoint is naive, carries a non-zero UTC offset, or
                ``end < start``. The CLI already rejects naive input, so this is
                defence for direct programmatic callers — tests, notebooks,
                future schedulers — and protection for D-13, which derives a
                bronze partition from ``end.date()``: a non-UTC ``end`` would
                silently partition to the wrong day.
            NesoFutureWindowError: ``end`` is beyond the clock-skew tolerance.
            NesoHistoricalWindowError: ``end`` is more than 48 h stale.
            NesoWindowTooLongError: The span exceeds what any automated path can
                resolve.
        """
        for label, value in (("start", start), ("end", end)):
            offset = value.utcoffset()
            if offset is None:
                raise ValueError(
                    f"neso_data_portal.fetch: {label} must be timezone-aware UTC, got the "
                    f"naive value {value!r}"
                )
            if offset != timedelta(0):
                raise ValueError(
                    f"neso_data_portal.fetch: {label} must carry a zero UTC offset, got "
                    f"{value!r} (offset {offset}); D-13 partitions bronze at end.date(), so "
                    "a non-UTC endpoint would land the capture on the wrong day"
                )
        if end < start:
            raise ValueError(
                f"neso_data_portal.fetch: end ({end.isoformat()}) precedes start "
                f"({start.isoformat()})"
            )

        now = datetime.now(UTC)
        if end > now + _FUTURE_WINDOW_TOLERANCE:
            raise NesoFutureWindowError(
                f"neso_data_portal.fetch: window end {end.isoformat()} is in the future "
                f"(now {now.isoformat()}). The portal has no future snapshot, and a future "
                "partition is the one shape D-13 cannot recover from: ingest would report "
                "success while transform stayed permanently silent. Use --last 24h, or "
                f"--end {now.date().isoformat()} — note that a bare --end <date> means "
                "midnight at the START of that date."
            )
        if end < now - _HISTORICAL_WINDOW_TOLERANCE:
            raise NesoHistoricalWindowError(
                f"neso_data_portal: window end {end.isoformat()} is more than "
                f"{_HISTORICAL_WINDOW_TOLERANCE} before now ({now.isoformat()}). This source "
                "serves only the vendor's CURRENT snapshot, so a historical window cannot be "
                "honoured; NESO's per-year Archive resources are a separate, deferred scope."
            )

        span = end - start
        if span > _MAX_INGEST_WINDOW:
            raise NesoWindowTooLongError(
                f"neso_data_portal: requested span {span} exceeds the {_MAX_INGEST_WINDOW} "
                "maximum, which is the widest window the pipeline itself can resolve "
                "(PipelineSettings.max_incremental_lookback_hours). No automated path can "
                "produce a wider window."
            )

        configured = self.config.datasets.get(dataset)
        max_query_days = configured.max_query_days if configured is not None else 0
        if max_query_days > 0 and span > timedelta(days=max_query_days):
            logger.warning(
                "neso_data_portal/%s: requested span %s exceeds the configured "
                "max_query_days of %d, and is being HONOURED rather than reinterpreted: "
                "the window is not a selector for this source, so one whole-file capture "
                "will be made and partitioned at %s (D-16).",
                dataset,
                span,
                max_query_days,
                end.date().isoformat(),
            )

    async def fetch(
        self,
        dataset: str,
        start: datetime,
        end: datetime,
        **params: Any,
    ) -> list[RawResponse]:
        """Capture one dataset's current whole-file snapshot.

        The window is **not a selector** (D-16): every resource is a whole-file
        snapshot with no server-side date filter, so one ``fetch()`` issues
        exactly one ``package_show``, one redirector request plus its hops, and
        returns exactly one :class:`RawResponse`. Two invocations differing only
        in ``start`` produce identical request URLs and params.

        The window is not *unused*, though: ``end`` is screened by D-34 and is
        what the bronze partition is derived from.

        Args:
            dataset: One of :data:`~...endpoints.DATASETS`.
            start: Window start, tz-aware UTC.
            end: Window end, tz-aware UTC. ``end.date()`` becomes the bronze
                partition (D-13), so it agrees **by construction** with the last
                date ``run_transform`` iterates — a download that crosses UTC
                midnight cannot land where transform is not looking.
            **params: Unused; accepted for the ``BaseConnector`` signature.

        Returns:
            A single-element list.

        Raises:
            ValueError: Unknown dataset, or a malformed window.
            CkanActionError: The package is absent or CKAN reported failure —
                definitive-absent for this dataset (ADR-023). Post-retry 5xx and
                timeouts propagate as ``httpx`` errors.
        """
        spec = DATASETS.get(dataset)
        if spec is None:
            if dataset in endpoints.FAMILIES:
                raise ValueError(
                    f"neso_data_portal dataset {dataset!r} is captured through iter_members "
                    "(bounded) only; fetch() serves the three legacy keys"
                )
            raise ValueError(
                f"unknown neso_data_portal dataset {dataset!r}; available: {sorted(DATASETS)}"
            )

        self.last_skipped_units = 0
        self._assert_window_admissible(dataset, start, end)

        package_payload = await self._package_show(spec.package)
        resource, redirector = self._select_resource(package_payload, spec, dataset)
        body, redirector_url, http_status, _declared = await self._download_resource(
            resource, redirector, spec.max_download_bytes, dataset
        )
        self._assert_admissible_csv(body, spec, dataset, redirector_url)

        return [
            RawResponse(
                body=body,
                # From the CKAN format check, NEVER from the response header:
                # the presigned host serves application/octet-stream, which the
                # bronze writer maps to `.bin` — invisible to the transformer's
                # `raw_*.csv` glob, so silver would read zero rows from a bronze
                # tree that is not empty (D-10).
                content_type="text/csv",
                source=self.source_name,
                dataset=dataset,
                request_url=redirector_url,
                request_params=_provenance_params(spec.package, package_payload, resource, body),
                api_version="3",
                # The status actually observed on the final leg, never a
                # constant: this is written to the immutable bronze sidecar,
                # and recording a status we did not see is false provenance
                # whether or not the falsehood is currently reachable.
                http_status=http_status,
                data_date=end.date(),
            )
        ]

    # ------------------------------------------------------------------
    # Member capture (ADR-033 P-5..P-8)
    # ------------------------------------------------------------------

    async def iter_members(
        self, dataset: str, start: datetime, end: datetime
    ) -> AsyncIterator[MemberEvent]:
        """Capture one family, one member at a time (ADR-033 P-5).

        One ``package_show``, then selection (P-6), then the newest-capture
        index (P-10), then per member in payload order: a ``datastore`` member
        goes through the dump leg (:meth:`_dump_member`, ADR-035) and is
        ``captured``, or ``unchanged`` when its body is byte-identical to its
        newest capture or a frozen-class dump is not yet due; an upload member
        whose newest usable capture carries the live ``last_modified`` is
        ``unchanged``; every other member is downloaded, admitted (P-7) and
        yielded as ``captured``. A per-member failure is a ``failed`` event and
        the family continues. Only one body is alive at a time: the consumer
        publishes each capture before the next download starts.

        Args:
            dataset: A registry family key.
            start: Window start, tz-aware UTC (D-34).
            end: Window end, tz-aware UTC; ``end.date()`` is the partition.

        Yields:
            One :class:`MemberEvent` per member, plus one ``absent`` event per
            listed member the live package no longer serves.

        Raises:
            ValueError: Unknown family, or a malformed window.
            RuntimeError: :meth:`bind_data_dir` was not called.
            NesoResourceSelectionError: No member at all, or a listed member
                matched twice (definitive-absent, ADR-023).
        """
        family = endpoints.FAMILIES.get(dataset)
        if family is None:
            raise ValueError(f"unknown neso_data_portal dataset {dataset!r}")
        if self._bronze_root is None:
            raise RuntimeError(
                "iter_members needs bind_data_dir() first: the unchanged-member skip reads "
                "the bound bronze tree"
            )
        self.last_skipped_units = 0
        self._assert_window_admissible(dataset, start, end)

        package_payload = await self._package_show(family.package)
        live = package_payload.get("resources")
        if not isinstance(live, list):
            raise NesoResourceSelectionError(
                f"{dataset}: CKAN package {family.package!r} carried no resources list"
            )
        registry = registry_module.load_registry()
        self._warn_unassigned(family.package, live)

        legacy_spec = DATASETS.get(dataset) if family.legacy else None
        absent: list[MemberEvent] = []
        if legacy_spec is not None:
            resource, _redirector = self._select_resource(package_payload, legacy_spec, dataset)
            members = [resource]
        else:
            members, absent = _select_members(live, family, registry, dataset)

        scan = captures_module.scan_dataset(self._bronze_root / dataset, registry)
        newest = captures_module.newest_by_resource(scan.captures)
        # ADR-035 P-8 (ii)/(iii): read before the scan is dropped.
        unusable_ids = frozenset(
            item.resource_id for item in scan.unusable if item.resource_id is not None
        )
        unattributed_unusable = any(item.resource_id is None for item in scan.unusable)
        del scan

        for event in absent:
            logger.warning("neso_data_portal/%s: %s", dataset, event.detail)
            yield event

        for resource in members:
            resource_id = str(resource.get("id", ""))
            if resource.get("url_type") == "datastore":
                basis_blocked = resource_id in unusable_ids or unattributed_unusable
                dump_event = await self._dump_member(
                    package_payload,
                    resource,
                    family,
                    dataset,
                    registry,
                    None if basis_blocked else newest.get(resource_id),
                    end,
                )
                yield dump_event
                del dump_event
                continue
            capture = newest.get(resource_id)
            live_modified = resource.get("last_modified")
            # A dump capture is never the skip basis for an upload (ADR-035 P-6):
            # its last_modified is not a file stamp (decision 9).
            if (
                capture is not None
                and capture.url_type != "datastore"
                and isinstance(live_modified, str)
                and live_modified
                and capture.ckan_last_modified == live_modified
            ):
                yield MemberEvent(
                    resource_id,
                    "unchanged",
                    detail=f"newest capture {capture.sidecar.name} carries last_modified "
                    f"{live_modified}",
                )
                continue
            try:
                response, extension = await self._capture_member(
                    package_payload, resource, family, legacy_spec, dataset, end
                )
            except (NesoDataPortalError, CsvBronzeError, httpx.HTTPError, OSError) as exc:
                yield MemberEvent(resource_id, "failed", detail=_safe_detail(exc))
                continue
            event = MemberEvent(resource_id, "captured", response, extension)
            del response
            yield event
            del event

    async def _capture_member(
        self,
        package_payload: dict[str, Any],
        resource: dict[str, Any],
        family: FamilySpec,
        legacy_spec: CkanDataset | None,
        dataset: str,
        end: datetime,
    ) -> tuple[RawResponse, str]:
        """Download and admit one upload member; return its response and extension."""
        redirector = self._assert_redirector_url(package_payload, resource, dataset)
        max_bytes = (
            legacy_spec.max_download_bytes if legacy_spec is not None else family.max_download_bytes
        )
        body, redirector_url, http_status, declared = await self._download_resource(
            resource, redirector, max_bytes, dataset
        )
        if legacy_spec is not None:
            self._assert_admissible_csv(body, legacy_spec, dataset, redirector_url)
            extension, empty_capture = "csv", False
        else:
            extension, empty_capture = _admit_member_body(
                body,
                declared_format=str(resource.get("format", "")),
                filename=_resource_filename(resource),
                empty_allowed=family.empty_allowed,
                label=f"{dataset}: {redirector_url}",
            )
        request_params = _provenance_params(family.package, package_payload, resource, body)
        request_params.update(
            {
                "capture_family": dataset,
                "url_type": str(resource.get("url_type", "")),
                "empty_capture": empty_capture,
                "declared_content_length": declared,
            }
        )
        response = RawResponse(
            body=body,
            content_type=_CONTENT_TYPES[extension],
            source=self.source_name,
            dataset=dataset,
            request_url=redirector_url,
            request_params=request_params,
            api_version="3",
            http_status=http_status,
            data_date=end.date(),
        )
        return response, extension

    def _dump_member_target(self, resource: dict[str, Any], dataset: str, registry: Any) -> str:
        """Return the resource id a dump may be built from (ADR-035 P-2), or raise.

        "The registry's resource id" is literal: the live id must be canonical,
        seeded in the registry under ``dataset`` as a ``datastore`` resource,
        and its live format must be CSV. A dump recreated under a new UUID fails
        loud here until a registry commit seeds it.

        Raises:
            NesoDatastoreMemberError: Any of those conditions fails. Nothing has
                been sent.
        """
        resource_id = str(resource.get("id", ""))
        if not endpoints.is_canonical_resource_id(resource_id):
            raise NesoDatastoreMemberError(
                f"{dataset}: datastore member id {resource_id!r} is not a canonical lowercase "
                "UUID; no dump path is built from it"
            )
        seeded = registry.resources.get(resource_id)
        if seeded is None:
            raise NesoDatastoreMemberError(
                f"{dataset}: datastore member {resource_id} is not seeded in the registry; a "
                "registry commit must seed it before its dump is fetched (ADR-035)"
            )
        _package_entry, entry = seeded
        if entry.family != dataset:
            raise NesoDatastoreMemberError(
                f"{dataset}: datastore member {resource_id} is seeded under family "
                f"{entry.family!r}, not {dataset!r}"
            )
        if entry.url_type != "datastore":
            raise NesoDatastoreMemberError(
                f"{dataset}: member {resource_id} is live as a datastore resource but seeded "
                f"as {entry.url_type!r}"
            )
        live_format = str(resource.get("format", ""))
        if live_format.upper() != "CSV":
            raise NesoDatastoreMemberError(
                f"{dataset}: datastore member {resource_id} declares format {live_format!r}; "
                "a datastore dump is CSV"
            )
        return resource_id

    async def _dump_member(
        self,
        package_payload: dict[str, Any],
        resource: dict[str, Any],
        family: FamilySpec,
        dataset: str,
        registry: Any,
        candidate: Capture | None,
        end: datetime,
    ) -> MemberEvent:
        """Capture, suppress or skip one datastore member (ADR-035 P-9).

        In order: the registry gate (P-2), the dedup basis (P-8, computed once
        and read by both the cadence and the suppression), the frozen-class
        cadence (P-7), then download (P-3), admission (P-4), content-hash
        suppression (P-8, stamping a frozen family) and provenance (P-5).

        Args:
            package_payload: The live ``package_show`` result.
            resource: The live datastore resource.
            family: The family being captured.
            dataset: The family key.
            registry: The loaded registry.
            candidate: The newest usable capture of this resource, or ``None``
                when there is none or an unusable sidecar blocks the basis
                (P-8 (i)-(iii)).
            end: The window end; ``end.date()`` is the partition and the
                cadence clock.

        Returns:
            One ``captured``, ``unchanged`` or ``failed`` event.
        """
        resource_id = str(resource.get("id", ""))
        try:
            rid = self._dump_member_target(resource, dataset, registry)
        except NesoDatastoreMemberError as exc:
            return MemberEvent(resource_id, "failed", detail=_safe_detail(exc))

        basis = _verified_basis(candidate, dataset)
        stamp_path = self._dump_stamp_path(dataset, rid) if family.refresh == "frozen" else None
        if stamp_path is not None:
            not_due = _frozen_not_due(basis, stamp_path, rid, end, dataset)
            if not_due is not None:
                return MemberEvent(rid, "unchanged", detail=not_due)

        try:
            dump = await self._download_dump(rid, family.max_download_bytes, dataset)
            extension, empty_capture = _admit_member_body(
                dump.body,
                declared_format="CSV",
                filename=f"{rid}.csv",
                empty_allowed=family.empty_allowed,
                label=f"{dataset}: dump {rid}",
            )
            if extension != "csv":
                raise NesoUnexpectedBodyError(
                    f"{dataset}: dump {rid} returned a {extension} body; a datastore dump is "
                    "CSV text"
                )
        except (NesoDataPortalError, CsvBronzeError, httpx.HTTPError, OSError) as exc:
            return MemberEvent(rid, "failed", detail=_safe_detail(exc))

        request_params = _dump_provenance_params(
            family.package,
            package_payload,
            resource,
            dump,
            dataset=dataset,
            empty_capture=empty_capture,
        )
        if basis is not None and request_params["body_sha256"] == basis.body_sha256:
            if stamp_path is not None:
                _write_dump_stamp(stamp_path, rid, basis.body_sha256, end, dataset)
            return MemberEvent(
                rid, "unchanged", detail=f"dump body identical to newest capture {basis.body.name}"
            )
        response = RawResponse(
            body=dump.body,
            content_type=_CONTENT_TYPES["csv"],
            source=self.source_name,
            dataset=dataset,
            request_url=dump.request_url,
            request_params=request_params,
            api_version="3",
            http_status=dump.http_status,
            data_date=end.date(),
        )
        return MemberEvent(rid, "captured", response, "csv")

    def _dump_stamp_path(self, dataset: str, resource_id: str) -> Path:
        """The check-stamp file of one dump member (ADR-035 P-7)."""
        if self._data_paths is None:
            raise RuntimeError("the dump leg needs bind_data_dir() first")
        return self._data_paths.dump_check_dir(self.source_name, dataset) / f"{resource_id}.json"

    def _warn_unassigned(self, package: str, live: list[Any]) -> None:
        """Log, once per process, each live resource no family of ``package`` claims."""
        families = [spec for spec in endpoints.FAMILIES.values() if spec.package == package]
        for item in live:
            if not isinstance(item, dict):
                continue
            name, fmt = str(item.get("name", "")), str(item.get("format", ""))
            key = (package, str(item.get("id", "")))
            if key in _WARNED_UNASSIGNED or any(spec.selects(name, fmt) for spec in families):
                continue
            _WARNED_UNASSIGNED.add(key)
            logger.warning(
                "neso_data_portal: resource %s %r (%s) in package %r is unassigned and was "
                "not fetched; a registry commit is needed to file it under a family",
                key[1],
                name,
                fmt,
                package,
            )

    def list_datasets(self) -> list[str]:
        """Return every registry family key this connector serves (P-3)."""
        return list(endpoints.FAMILIES)


# Every (package, resource id) already warned as unassigned in this process (P-6).
_WARNED_UNASSIGNED: set[tuple[str, str]] = set()

_CONTENT_TYPES: dict[str, str] = {
    "csv": "text/csv",
    "txt": "text/plain",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "xlsm": "application/vnd.ms-excel.sheet.macroEnabled.12",
    "xls": "application/vnd.ms-excel",
    "zip": "application/zip",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "doc": "application/msword",
    "ppt": "application/vnd.ms-powerpoint",
    "pdf": "application/pdf",
    "png": "image/png",
    "gpkg": "application/geopackage+sqlite3",
    "geojson": "application/geo+json",
}

# P-7: the body signature classes, what each CKAN format admits, the extensions
# each class may carry, and the per-format default when the filename's suffix
# is not one of them.
_OLE2_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"
_GEOJSON_TYPES = frozenset(
    {
        "FeatureCollection",
        "Feature",
        "Point",
        "MultiPoint",
        "LineString",
        "MultiLineString",
        "Polygon",
        "MultiPolygon",
        "GeometryCollection",
    }
)
_ALLOWED_CLASSES: dict[str, frozenset[str]] = {
    "CSV": frozenset({"TEXT", "PK"}),
    "XLSX": frozenset({"PK"}),
    "XLSM": frozenset({"PK"}),
    "ZIP": frozenset({"PK"}),
    "DOC": frozenset({"PK", "OLE2"}),
    "PPT": frozenset({"PK", "OLE2"}),
    "PDF": frozenset({"PDF"}),
    "PNG": frozenset({"PNG"}),
    "GEOJSON": frozenset({"GEOJSON"}),
    "GPKG": frozenset({"SQLITE"}),
    "TXT": frozenset({"TEXT"}),
}
_CLASS_EXTENSIONS: dict[str, frozenset[str]] = {
    "PK": frozenset({"xlsx", "xlsm", "zip", "docx", "pptx"}),
    "OLE2": frozenset({"doc", "ppt", "xls"}),
    "PDF": frozenset({"pdf"}),
    "PNG": frozenset({"png"}),
    "SQLITE": frozenset({"gpkg"}),
    "GEOJSON": frozenset({"geojson"}),
    "TEXT": frozenset({"csv", "txt"}),
}
_DEFAULT_EXTENSIONS: dict[tuple[str, str], str] = {
    ("CSV", "TEXT"): "csv",
    ("CSV", "PK"): "zip",
    ("XLSX", "PK"): "xlsx",
    ("XLSM", "PK"): "xlsm",
    ("ZIP", "PK"): "zip",
    ("DOC", "PK"): "docx",
    ("DOC", "OLE2"): "doc",
    ("PPT", "PK"): "pptx",
    ("PPT", "OLE2"): "ppt",
    ("PDF", "PDF"): "pdf",
    ("PNG", "PNG"): "png",
    ("GPKG", "SQLITE"): "gpkg",
    ("GEOJSON", "GEOJSON"): "geojson",
    ("TXT", "TEXT"): "txt",
}
_UTF8_BOM = b"\xef\xbb\xbf"
_CSV_STRUCTURE = re.compile(rb'[,"\r\n]')
_DATA_BYTE = re.compile(rb'[^ \t\r\n\x0b\x0c,"]')


def _signature_class(body: bytes) -> str | None:
    """Classify ``body`` by its leading bytes (P-7); ``None`` if it is none of them."""
    if body.startswith(b"PK\x03\x04") or (len(body) == 22 and body.startswith(b"PK\x05\x06")):
        return "PK"
    if body.startswith(b"%PDF-"):
        return "PDF"
    if body.startswith(b"\x89PNG\r\n\x1a\n"):
        return "PNG"
    if body.startswith(_OLE2_MAGIC):
        return "OLE2"
    if body.startswith(b"SQLite format 3\x00"):
        return "SQLITE"
    if _text_head(body).startswith(b"{") and _is_geojson(body):
        return "GEOJSON"
    if b"\x00" not in body[:8192]:
        return "TEXT"
    return None


def _text_head(body: bytes) -> bytes:
    """The body after a UTF-8 BOM and leading ASCII whitespace (first 64 bytes)."""
    head = body[:4096]
    if head.startswith(_UTF8_BOM):
        head = head[len(_UTF8_BOM) :]
    return head.lstrip()[:64]


def _is_geojson(body: bytes) -> bool:
    try:
        payload = json.loads(body.removeprefix(_UTF8_BOM).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    return isinstance(payload, dict) and payload.get("type") in _GEOJSON_TYPES


def _is_json_array(body: bytes) -> bool:
    """Whether ``body`` parses as a JSON array (an error envelope, not CSV).

    Parsed rather than keyed on a leading ``[`` so a CSV whose header starts
    with ``[`` (``[Date],[MW]``) is still admitted (P-7 refuses only
    signature-incompatible bodies). Non-JSON fails at the first bad token.
    """
    try:
        payload = json.loads(body.removeprefix(_UTF8_BOM).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    return isinstance(payload, list)


def _header_end(body: bytes, start: int) -> int:
    """Byte offset just past the first CSV record of ``body[start:]`` (P-7).

    Scans the bytes with CSV's own record grammar rather than a line split, so
    no terminator form can misplace the boundary: an unquoted CR, LF or CRLF
    ends the record, and a ``"`` opens a quoted field only at a field start
    (as the stdlib reader treats it), inside which terminators and doubled
    ``""`` are field text. The delimiting bytes are ASCII in UTF-8 and cp1252
    alike, so nothing is decoded or copied (the A9 memory gate) and nothing
    can fail to parse. An unterminated quote runs to the end of the body.
    """
    size = len(body)
    position = field_start = start
    while True:
        match = _CSV_STRUCTURE.search(body, position)
        if match is None:
            return size
        at = match.start()
        token = body[at : at + 1]
        if token == b",":
            position = field_start = at + 1
        elif token == b'"':
            position = at + 1
            if at != field_start:
                continue
            while True:
                close = body.find(b'"', position)
                if close < 0:
                    return size
                if body[close + 1 : close + 2] == b'"':
                    position = close + 2
                    continue
                position = close + 1
                break
        else:
            return at + 1


def _is_header_only(body: bytes) -> bool:
    """A CSV body with a header record and nothing data-bearing after it (P-7).

    The header is the first CSV record, bounded by :func:`_header_end` under
    every terminator CSV accepts and with quoted terminators kept inside it
    (REVIEW-DIFF-1 #2, REVIEW-DIFF-2). The remainder keeps P-7's rule: only
    ASCII whitespace, ``,`` and ``"``. There is no parse step that can fail,
    so no body is admitted as non-empty for want of a parse. Residual: an
    unterminated opening quote makes the whole body one record, so it reads as
    header-only: refused where empty captures are forbidden, captured and
    marked empty in an ``empty_allowed`` family.
    """
    start = len(_UTF8_BOM) if body.startswith(_UTF8_BOM) else 0
    return _DATA_BYTE.search(body, _header_end(body, start)) is None


def _resource_filename(resource: dict[str, Any]) -> str:
    url = str(resource.get("url", ""))
    return url.rstrip("/").rsplit("/", 1)[-1] if url else ""


def _admit_member_body(
    body: bytes,
    *,
    declared_format: str,
    filename: str,
    empty_allowed: bool,
    label: str,
) -> tuple[str, bool]:
    """Admit one member body by signature (ADR-033 P-7); return ``(extension, empty)``.

    Encoding is deliberately not checked: strict UTF-8 is unit E's to measure,
    and refusing a cp1252 file would lose a capture.

    Raises:
        NesoEmptyResourceError: A zero-byte body, or a header-only CSV in a
            family that does not allow empty captures.
        NesoUnexpectedBodyError: The signature is not one the format admits, or
            a text body is markup or a JSON envelope.
    """
    fmt = declared_format.upper()
    if not body:
        raise NesoEmptyResourceError(f"{label} returned an empty body; nothing to capture")
    signature = _signature_class(body)
    allowed = _ALLOWED_CLASSES.get(fmt, frozenset())
    if signature is None or signature not in allowed:
        raise NesoUnexpectedBodyError(
            f"{label}: a {fmt or '<no format>'} resource returned a body whose signature is "
            f"{signature or 'binary/unknown'}; admitted classes are {sorted(allowed)}"
        )
    head = _text_head(body)
    if (
        fmt == "CSV"
        and signature == "TEXT"
        and (head[:1] in (b"<", b"{") or (head[:1] == b"[" and _is_json_array(body)))
    ):
        raise NesoUnexpectedBodyError(
            f"{label}: a CSV resource returned markup or a JSON envelope, not CSV"
        )
    if fmt == "TXT" and head[:1] == b"<":
        raise NesoUnexpectedBodyError(f"{label}: a TXT resource returned markup")

    suffix = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
    extension = (
        suffix if suffix in _CLASS_EXTENSIONS[signature] else _DEFAULT_EXTENSIONS[(fmt, signature)]
    )

    empty = False
    if fmt == "CSV" and signature == "TEXT" and _is_header_only(body):
        if not empty_allowed:
            raise NesoEmptyResourceError(
                f"{label} returned a header-only body with no data rows, and its family "
                "does not allow empty captures"
            )
        empty = True
    return extension, empty


def _select_members(
    live: list[Any],
    family: FamilySpec,
    registry: Any,
    dataset: str,
) -> tuple[list[dict[str, Any]], list[MemberEvent]]:
    """P-6: the family's live members in payload order, and its absent listed members.

    Raises:
        NesoResourceSelectionError: No live member at all, or one listed
            ``(name, format)`` matched by two live resources.
    """
    members = [
        item
        for item in live
        if isinstance(item, dict)
        and family.selects(str(item.get("name", "")), str(item.get("format", "")))
    ]
    matched: dict[tuple[str, str], int] = {}
    for item in members:
        pair = (str(item.get("name", "")), str(item.get("format", "")).upper())
        if pair in family.names:
            matched[pair] = matched.get(pair, 0) + 1
    ambiguous = sorted(pair for pair, count in matched.items() if count > 1)
    if ambiguous:
        raise NesoResourceSelectionError(
            f"{dataset}: listed member(s) {ambiguous!r} match more than one live resource in "
            f"CKAN package {family.package!r}; refusing to guess"
        )
    if not members:
        actual = [str(item.get("name")) for item in live if isinstance(item, dict)]
        raise NesoResourceSelectionError(
            f"{dataset}: no live resource in CKAN package {family.package!r} is a member; "
            f"the package returned {actual!r}"
        )
    package_entry, _family_entry = registry.families[dataset]
    seeded = {
        (resource.name, resource.format): resource.id
        for resource in package_entry.resources
        if resource.family == dataset
    }
    absent = [
        MemberEvent(
            seeded.get(pair, ""),
            "absent",
            detail=f"listed member {pair[0]!r} ({pair[1]}) is not in the live package",
        )
        for pair in sorted(family.names)
        if pair not in matched
    ]
    return members, absent


def _safe_detail(exc: BaseException) -> str:
    """One credential-free line naming ``exc`` (connector errors render SafeUrl)."""
    message = " ".join(str(exc).split())
    text = f"{type(exc).__name__}: {message}" if message else type(exc).__name__
    return sanitize_url(text, value_chars=r"[^&\s)]")


def _declared_content_length(response: httpx.Response) -> int | None:
    """Return a well-formed ``Content-Length``, or ``None`` if absent/unparseable."""
    raw = response.headers.get("content-length")
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


class _DumpBody(NamedTuple):
    """One downloaded datastore dump and what its final response said (ADR-035 P-3)."""

    body: bytes
    request_url: str
    http_status: int
    declared_length: int | None
    last_modified_header: str | None


def _verified_basis(candidate: Capture | None, dataset: str) -> Capture | None:
    """P-8 (iv): ``candidate`` only if its body on disk still hashes to its record.

    A streamed re-hash in 1 MiB chunks. A mismatch or a read error means no
    basis, so the member is due and an identical body is captured: the dedup
    fails open toward capture.
    """
    if candidate is None:
        return None
    digest = hashlib.sha256()
    try:
        with candidate.body.open("rb") as handle:
            for chunk in iter(lambda: handle.read(_REHASH_CHUNK), b""):
                digest.update(chunk)
    except OSError as exc:
        logger.warning(
            "neso_data_portal/%s: cannot re-hash %s (%s); it is not a dedup basis",
            dataset,
            candidate.body.name,
            exc,
        )
        return None
    if digest.hexdigest() != candidate.body_sha256:
        logger.warning(
            "neso_data_portal/%s: %s no longer hashes to its recorded body_sha256; it is not "
            "a dedup basis",
            dataset,
            candidate.body.name,
        )
        return None
    return candidate


def _read_dump_stamp(
    stamp_path: Path, resource_id: str, body_sha256: str, dataset: str
) -> datetime | None:
    """Return an honoured check stamp's ``verified_at``, else ``None`` (ADR-035 P-7).

    Honoured only when the file is a JSON object naming this resource and this
    basis hash, with a tz-aware ``verified_at``. Anything else is ignored, with
    a warning when the file exists.
    """
    try:
        raw = stamp_path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        logger.warning(
            "neso_data_portal/%s: check stamp %s is unreadable (%s); ignored",
            dataset,
            stamp_path.name,
            exc,
        )
        return None
    problem: str | None = None
    verified_at: datetime | None = None
    try:
        document: Any = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        problem = "is not JSON"
    else:
        if not isinstance(document, dict):
            problem = "is not a JSON object"
        elif document.get("resource_id") != resource_id:
            problem = "names another resource"
        elif document.get("body_sha256") != body_sha256:
            problem = "records another basis hash"
        else:
            verified_at = _parse_aware(document.get("verified_at"))
            if verified_at is None:
                problem = "has no tz-aware verified_at"
    if problem is not None:
        logger.warning(
            "neso_data_portal/%s: check stamp %s %s; ignored", dataset, stamp_path.name, problem
        )
    return verified_at


def _parse_aware(value: Any) -> datetime | None:
    """Parse an ISO-8601 string to a tz-aware datetime; ``None`` for anything else."""
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.utcoffset() is not None else None


def _frozen_not_due(
    basis: Capture | None, stamp_path: Path, resource_id: str, end: datetime, dataset: str
) -> str | None:
    """The frozen-class due rule (ADR-035 P-7); the not-due detail, or ``None`` if due.

    Not due iff a verified basis exists and the latest of its ``written_at``
    and an honoured stamp's ``verified_at``, ignoring any dated after
    ``end.date()``, falls inside the 7-day cadence measured in ``end`` dates.
    No CKAN field enters the rule.
    """
    if basis is None:
        return None
    end_date = end.astimezone(UTC).date()
    candidates: list[tuple[datetime, str]] = [(basis.written_at, f"capture {basis.body.name}")]
    verified_at = _read_dump_stamp(stamp_path, resource_id, basis.body_sha256, dataset)
    if verified_at is not None:
        candidates.append((verified_at, "check stamp"))
    admissible = [item for item in candidates if item[0].astimezone(UTC).date() <= end_date]
    if not admissible:
        return None
    latest, label = max(admissible, key=lambda item: item[0])
    latest_date = latest.astimezone(UTC).date()
    if latest_date <= end_date - _FROZEN_CADENCE:
        return None
    return (
        f"not due: frozen-class dump, last verified {latest_date.isoformat()} ({label}), "
        "inside the 7-day cadence (decision 11)"
    )


def _write_dump_stamp(
    stamp_path: Path, resource_id: str, body_sha256: str, end: datetime, dataset: str
) -> None:
    """Record a byte-identical frozen-class check (ADR-035 P-7); never raises.

    A failed write only leaves the clock older, so the member is due sooner.
    """
    document = {
        "body_sha256": body_sha256,
        "resource_id": resource_id,
        "verified_at": end.astimezone(UTC).isoformat(),
    }
    data = (json.dumps(document, sort_keys=True) + "\n").encode("utf-8")
    try:
        stamp_path.parent.mkdir(parents=True, exist_ok=True)
        files_module.replace_atomically(stamp_path, data)
    except OSError as exc:
        logger.warning(
            "neso_data_portal/%s: could not write check stamp %s (%s); the member is due "
            "on the next run",
            dataset,
            stamp_path.name,
            exc,
        )


def _dump_provenance_params(
    package: str,
    package_payload: dict[str, Any],
    resource: dict[str, Any],
    dump: _DumpBody,
    *,
    dataset: str,
    empty_capture: bool,
) -> dict[str, Any]:
    """Build a dump capture's ``request_params`` (ADR-035 P-5).

    D-12's keys from :func:`_provenance_params`, with ``ckan_last_modified``
    as the CKAN string or ``""`` (never ``"None"``) and ``resource_filename``
    as the resource id (a dump has no vendor filename); then A's member keys,
    and two evidence keys for the open class-3 questions:
    ``ckan_metadata_modified`` and ``response_last_modified``.
    """
    params = _provenance_params(package, package_payload, resource, dump.body)
    last_modified = resource.get("last_modified")
    metadata_modified = resource.get("metadata_modified")
    params["ckan_last_modified"] = last_modified if isinstance(last_modified, str) else ""
    params["resource_filename"] = str(resource.get("id", ""))
    params.update(
        {
            "capture_family": dataset,
            "url_type": "datastore",
            "empty_capture": empty_capture,
            "declared_content_length": dump.declared_length,
            "ckan_metadata_modified": (
                metadata_modified if isinstance(metadata_modified, str) else ""
            ),
            "response_last_modified": dump.last_modified_header,
        }
    )
    return params


def _provenance_params(
    package: str,
    package_payload: dict[str, Any],
    resource: dict[str, Any],
    body: bytes,
) -> dict[str, Any]:
    """Build the D-12 provenance the silver layer later needs.

    Exactly these keys, because ``silver/neso_data_portal/_bronze.py`` reads
    them back out of the sidecar and a missing one is a skipped vintage. The
    filename is taken from the redirector path rather than invented: the
    embedded forecast's ``issue_time`` is parsed from its ``YYYYMMDDHHMM``
    token.
    """
    url = str(resource.get("url", ""))
    filename = url.rstrip("/").rsplit("/", 1)[-1] if url else ""
    return {
        "package": package,
        "package_id": str(package_payload.get("id", "")),
        "resource_id": str(resource.get("id", "")),
        "resource_name": str(resource.get("name", "")),
        "resource_filename": filename,
        "ckan_last_modified": str(resource.get("last_modified", "")),
        "ckan_format": str(resource.get("format", "")),
        "body_sha256": hashlib.sha256(body).hexdigest(),
    }


register_connector("neso_data_portal", NesoDataPortalConnector)
