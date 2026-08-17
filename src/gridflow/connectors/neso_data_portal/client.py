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
"""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import logging
import socket
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import TYPE_CHECKING, Any, ClassVar
from uuid import uuid4

import httpx

from gridflow.connectors.base import BaseConnector, RawResponse, _make_ssl_context
from gridflow.connectors.neso_data_portal.endpoints import (
    DATASETS,
    CkanDataset,
    build_action_url,
)
from gridflow.connectors.registry import register_connector
from gridflow.silver.csv_bronze import read_csv_bronze_body
from gridflow.utils.retry import RETRY_POLICY

if TYPE_CHECKING:
    from gridflow.config.settings import SourceConfig

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


class NesoResourceSelectionError(NesoDataPortalError):
    """The package did not yield exactly one resource matching the contract.

    Zero matches, more than one match, or a matched resource whose CKAN
    ``format`` is not the expected one. Names every resource the package
    actually returned, so a vendor rename is diagnosable from the log alone.
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

    def __init__(self, config: SourceConfig) -> None:
        super().__init__(config)
        self._rate_limit_lock: asyncio.Lock | None = None
        self._last_request_at: float = 0.0
        self._issued_send_tokens: set[str] = set()

    async def __aenter__(self) -> NesoDataPortalConnector:
        """Build the client with redirects DISABLED and initialise pacing state.

        ``follow_redirects=False`` at the client level is D-08's first half:
        redirects are handled manually, one validated hop at a time, so each hop
        is a separate throttled send that has passed the target policy.
        """
        self._semaphore = asyncio.Semaphore(self.config.rate_limit_per_second)
        self._rate_limit_lock = asyncio.Lock()
        self._last_request_at = 0.0
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
    async def _send(self, request: httpx.Request, *, stream: bool = False) -> httpx.Response:
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

        await self._assert_safe_target(request.url)

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
            await self._throttle_request()
            response = await self._client.send(request, stream=stream, follow_redirects=False)

        if response.has_redirect_location:
            return response
        if not response.is_success:
            await response.aclose()
            response.raise_for_status()
        return response

    async def _throttle_request(self) -> None:
        """Pace outbound sends to the vendor's published 1 req/s guidance.

        Copied from ``connectors/entsoe/client.py:411-426`` — **copied, not
        hoisted into ``BaseConnector``**, because hoisting would change the
        request pacing of all six existing sources for the benefit of one.
        ``rate_limit_per_second: 1`` in YAML then yields both the inherited
        ``Semaphore(1)`` (a concurrency cap despite its name) and a real 1.0 s
        minimum interval, which is the part that honours the guidance.

        Gates **every** outbound send without exception: each CKAN action call,
        the redirector request, each redirect hop, and each retry attempt —
        because it sits inside :meth:`_send`, which is what ``RETRY_POLICY``
        decorates.
        """
        if self.config.rate_limit_per_second <= 0:
            return
        lock = self._rate_limit_lock
        if lock is None:
            return

        min_interval = 1.0 / self.config.rate_limit_per_second
        async with lock:
            elapsed = monotonic() - self._last_request_at
            if elapsed < min_interval:
                await asyncio.sleep(min_interval - elapsed)
            self._last_request_at = monotonic()

    async def _assert_safe_target(self, url: httpx.URL) -> None:
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
                f"refusing to send to {url!s}: scheme must be https, got {url.scheme!r}"
            )
        if url.userinfo:
            raise NesoUnsafeRedirectError(
                f"refusing to send to {url.copy_with(userinfo=b'')!s}: the URL carries "
                "userinfo, which httpx would turn into Basic credentials for that host"
            )
        host = url.host
        if not host:
            raise NesoUnsafeRedirectError(f"refusing to send to {url!s}: no host component")

        port = url.port or 443
        try:
            addresses = await _resolve_host_addresses(host, port)
        except OSError as exc:
            raise NesoUnsafeRedirectError(
                f"refusing to send to {url!s}: host {host!r} did not resolve ({exc})"
            ) from exc

        if not addresses:
            raise NesoUnsafeRedirectError(
                f"refusing to send to {url!s}: host {host!r} resolved to no addresses"
            )
        for address in addresses:
            if not address.is_global:
                raise NesoUnsafeRedirectError(
                    f"refusing to send to {url!s}: host {host!r} resolves to "
                    f"{address}, which is not globally routable"
                )

    def _resolve_redirect_target(self, response: httpx.Response) -> httpx.URL:
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
        return response.request.url.join(location)

    async def _read_capped_body(self, response: httpx.Response, spec: CkanDataset) -> bytes:
        """Read a streamed body under a hard size cap, and prove it is complete.

        In D-39 §4's order, which is not arbitrary: a declared oversize is
        rejected before a byte is read, the coding is checked before the bytes
        are interpreted, and the running total is what actually bounds memory.

        The body is read with ``aiter_raw()``, so the bytes counted, the bytes
        capped and the bytes written to bronze are the same bytes
        ``Content-Length`` describes. One counter, one meaning.

        Args:
            response: An open, streamed 2xx response.
            spec: The dataset's contract, for ``max_download_bytes``.

        Returns:
            The complete raw body.

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
        if declared is not None and declared > spec.max_download_bytes:
            raise NesoResponseTooLargeError(
                f"refusing {response.request.url!s}: declared Content-Length {declared} B "
                f"exceeds the {spec.max_download_bytes} B cap"
            )

        encoding = response.headers.get("content-encoding", "").strip().lower()
        if encoding and encoding != "identity":
            raise NesoUnexpectedEncodingError(
                f"{response.request.url!s} returned Content-Encoding {encoding!r} after "
                "the request asked for 'identity'; refusing to guess what the vendor did"
            )

        chunks: list[bytes] = []
        total = 0
        try:
            async for chunk in response.aiter_raw():
                total += len(chunk)
                if total > spec.max_download_bytes:
                    raise NesoResponseTooLargeError(
                        f"aborting {response.request.url!s}: body exceeded the "
                        f"{spec.max_download_bytes} B cap after {total} B"
                    )
                chunks.append(chunk)
        except httpx.RemoteProtocolError as exc:
            raise NesoTruncatedBodyError(
                f"{response.request.url!s} closed mid-transfer after {total} B ({exc})"
            ) from exc

        if declared is not None and total != declared:
            raise NesoTruncatedBodyError(
                f"{response.request.url!s} declared Content-Length {declared} B but "
                f"delivered {total} B"
            )
        return b"".join(chunks)

    # ------------------------------------------------------------------
    # CKAN two-stage fetch
    # ------------------------------------------------------------------

    async def _package_show(self, package: str) -> dict[str, Any]:
        """Resolve one CKAN package, checking the envelope rather than the status.

        NESO returns action errors as HTTP **200** with ``{"success": false}``,
        so a status-only check would treat an error envelope as a payload.

        Raises:
            CkanActionError: The envelope reported failure, the body was not a
                CKAN envelope, or the package is definitively absent (HTTP 404 —
                ADR-023 definitive-absent; one dataset per ``fetch()``, so there
                is no sibling to keep going for).
        """
        if self._client is None:
            raise RuntimeError("Connector not initialized. Use 'async with' context manager.")

        path, params = build_action_url("package_show", id=package)
        request = self._client.build_request("GET", path, params=params)
        try:
            response = await self._send(request)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise CkanActionError(
                    f"CKAN package {package!r} does not exist (HTTP 404)"
                ) from exc
            raise

        try:
            if response.has_redirect_location:
                raise CkanActionError(
                    f"CKAN package_show for {package!r} answered with a redirect to "
                    f"{response.headers.get('location')!r}; action calls are not redirected"
                )
            try:
                payload = json.loads(response.content)
            except json.JSONDecodeError as exc:
                raise CkanActionError(
                    f"CKAN package_show for {package!r} returned a body that is not JSON ({exc})"
                ) from exc
        finally:
            await response.aclose()

        if not isinstance(payload, dict) or not payload.get("success"):
            detail = payload.get("error") if isinstance(payload, dict) else payload
            raise CkanActionError(
                f"CKAN package_show for {package!r} returned success=false: {detail!r}"
            )
        result = payload.get("result")
        if not isinstance(result, dict):
            raise CkanActionError(f"CKAN package_show for {package!r} returned no result object")
        return result

    def _select_resource(
        self,
        package_payload: dict[str, Any],
        spec: CkanDataset,
        dataset: str,
    ) -> dict[str, Any]:
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
        return resource

    async def _download_resource(
        self,
        resource: dict[str, Any],
        spec: CkanDataset,
        dataset: str,
    ) -> tuple[bytes, str]:
        """Download one resource through its redirector, validating every hop.

        Each iteration builds a **fresh** GET, which regenerates ``Host`` from
        the target and copies no per-host header forward — the 302 sets three
        cookies on ``api.neso.energy`` and none of them may cross to the file
        host. The ``finally`` is the whole lifecycle answer: the 302's own body
        is a chunked ``text/html`` payload nobody reads, and without an explicit
        close a streamed 3xx leaks its connection.

        Returns:
            The body bytes and the **redirector** URL — never the presigned
            target, which carries ``X-Amz-Signature`` and a 7-day expiry and
            must not reach an irreproducible bronze sidecar (D-11).

        Raises:
            NesoRedirectLoopError: The chain exceeded :data:`_MAX_REDIRECT_HOPS`.
            NesoEmptyResourceError: The body carried no data row (D-14).
        """
        if self._client is None:
            raise RuntimeError("Connector not initialized. Use 'async with' context manager.")

        redirector_url = str(resource.get("url", ""))
        if not redirector_url:
            raise NesoResourceSelectionError(
                f"{dataset}: resource {spec.resource_name!r} carries no url"
            )

        request = self._client.build_request("GET", redirector_url, headers=_FILE_LEG_HEADERS)
        body: bytes | None = None
        for _ in range(_MAX_REDIRECT_HOPS + 1):
            response = await self._send(request, stream=True)
            try:
                if response.has_redirect_location:
                    target = self._resolve_redirect_target(response)
                    # No validate() call here: _send validates every URL it is
                    # handed (D-39 §1a). There is no second call site to forget.
                    request = self._client.build_request("GET", target, headers=_FILE_LEG_HEADERS)
                    continue
                body = await self._read_capped_body(response, spec)
                break
            finally:
                await response.aclose()

        if body is None:
            raise NesoRedirectLoopError(
                f"{dataset}: {redirector_url} exceeded {_MAX_REDIRECT_HOPS} redirect hops"
            )

        self._assert_admissible_csv(body, spec, dataset, redirector_url)
        return body, redirector_url

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
            raise ValueError(
                f"unknown neso_data_portal dataset {dataset!r}; available: {sorted(DATASETS)}"
            )

        self.last_skipped_units = 0
        self._assert_window_admissible(dataset, start, end)

        package_payload = await self._package_show(spec.package)
        resource = self._select_resource(package_payload, spec, dataset)
        body, redirector_url = await self._download_resource(resource, spec, dataset)

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
                request_params=_provenance_params(spec, package_payload, resource, body),
                api_version="3",
                data_date=end.date(),
            )
        ]

    def list_datasets(self) -> list[str]:
        """Return the dataset keys this connector serves."""
        return list(DATASETS)


def _declared_content_length(response: httpx.Response) -> int | None:
    """Return a well-formed ``Content-Length``, or ``None`` if absent/unparseable."""
    raw = response.headers.get("content-length")
    if raw is None:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _provenance_params(
    spec: CkanDataset,
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
        "package": spec.package,
        "package_id": str(package_payload.get("id", "")),
        "resource_id": str(resource.get("id", "")),
        "resource_name": str(resource.get("name", "")),
        "resource_filename": filename,
        "ckan_last_modified": str(resource.get("last_modified", "")),
        "ckan_format": str(resource.get("format", "")),
        "body_sha256": hashlib.sha256(body).hexdigest(),
    }


register_connector("neso_data_portal", NesoDataPortalConnector)
