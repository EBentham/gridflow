"""Opt-in LIVE checks for the NESO Data Portal (T-24).

**Never in the default run.** Every test here is ``@pytest.mark.live``, which
the repo-root conftest skips unless ``live`` is selected explicitly, so
``-m "not live and not slow"`` — the Stop-hook gate — collects them and
deselects them.

**What these exist for.** The mocked suite pins this connector against fixtures
captured on 2026-08-16. Fixtures cannot notice the vendor moving: a renamed
resource, a column added to a CSV header, a redirector that stops redirecting,
or a ``last_modified`` that turns out to be Europe/London after all are all
silent to every offline test in the repo. Each test below states in its
docstring which decision it pins, and a failure here is a **vendor change to
investigate**, not a bug to patch away.

**What they deliberately do not do.** No bronze write, no ``gridflow ingest``,
no ``fetch()``: these are read-only GETs made one at a time through the
connector's own ``_send`` primitive, so D-07's 1 req/s throttle and D-08's
target validation apply to every one of them exactly as they do in production.
The file leg's stream is abandoned after its first line — the header is all
these tests need, and pulling the whole ~62 MB
``historic_generation_mix`` body three times over would be traffic spent on
nothing.

``pytestmark`` carries **no** ``stub_neso_resolver``, and that is the point:
this module resolves ``api.neso.energy`` for real. Its absence here is what
makes the mocked module's opt-in meaningful.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import TYPE_CHECKING, Any

import pytest

from gridflow.config.settings import load_settings
from gridflow.connectors.neso_data_portal.client import (
    _PACKAGE_SEARCH_PAGE_SIZE,
    CatalogDiscovery,
    NesoDataPortalConnector,
    SafeUrl,
)
from gridflow.connectors.neso_data_portal.endpoints import DATASETS

if TYPE_CHECKING:
    import httpx

    from gridflow.config.settings import SourceConfig

pytestmark = pytest.mark.live

SOURCE = "neso_data_portal"

# Mirrors the connector's own hop budget; a live chain that needs more is a
# vendor change, which is exactly what this module is for.
_MAX_HOPS = 4

# The vendor's own bytes are wanted verbatim, never a transfer-coded copy —
# the same header the connector's file leg sends (D-39 §4).
_IDENTITY = {"Accept-Encoding": "identity"}

# D-15's tolerance. Wide enough that a slow republication or clock skew does
# not fail the check, and far narrower than the one-hour error it is designed
# to catch.
_D15_TOLERANCE = timedelta(minutes=5)
_BST_OFFSET = timedelta(hours=1)


def _live_config() -> SourceConfig:
    """The REAL shipped config, pacing included.

    Nothing is overridden — ``rate_limit_per_second: 1`` is honoured, so these
    tests are slow by design (D-07). A test that quietly raised the rate would
    be testing a connector nobody runs.
    """
    return load_settings().get_source_config(SOURCE)


async def _read_first_line(response: httpx.Response, limit: int = 8192) -> bytes:
    """Pull just enough of a streamed body to see its CSV header row."""
    buffered = b""
    async for chunk in response.aiter_bytes():
        buffered += chunk
        if b"\n" in buffered or len(buffered) >= limit:
            break
    return buffered.split(b"\n", 1)[0].rstrip(b"\r")


async def _walk_to_the_terminal_hop(
    connector: NesoDataPortalConnector, redirector: SafeUrl
) -> tuple[SafeUrl, int, dict[str, str], bytes]:
    """Follow the live redirect chain with the connector's OWN send primitive.

    This mirrors ``_download_resource``'s loop rather than calling it, because
    that method deliberately returns the **redirector** and never the presigned
    target (D-11: the signature and its 7-day expiry must not reach an
    irreproducible sidecar). These tests need the terminal hop's identity and
    its response headers, which is precisely the information D-11 keeps out of
    provenance — so it is obtained here, in a test, and never stored.

    Every hop still goes through ``_send``, so every one is validated by D-08
    and throttled by D-07. Nothing sends outside the primitive, here either.

    Returns:
        The terminal target, its status, its response headers, and the first
        line of its body.
    """
    assert connector._client is not None, "use the connector as an async context manager"

    target = redirector
    for _ in range(_MAX_HOPS):
        request = connector._client.build_request("GET", target.unsafe_raw(), headers=_IDENTITY)
        response = await connector._send(request, target, stream=True)
        try:
            if response.has_redirect_location:
                target = connector._resolve_redirect_target(response, target)
                continue
            return (
                target,
                response.status_code,
                dict(response.headers),
                await _read_first_line(response),
            )
        finally:
            await response.aclose()

    raise AssertionError(f"the live redirect chain exceeded {_MAX_HOPS} hops")


@pytest.mark.asyncio
@pytest.mark.parametrize("dataset", sorted(DATASETS))
async def test_the_package_still_carries_the_d03_resource_by_exact_name(dataset: str) -> None:
    """Pins **D-03/D-04**: resources are selected by exact ``resources[].name``.

    The single most likely silent breakage in this connector. The raw filenames
    are date-stamped and the UUIDs are provenance only, so the name is the one
    stable selector — and a vendor rename would leave every offline test green
    while production raised ``NesoResourceSelectionError`` on the next run.

    Driven through ``_select_resource``, not a hand-written comparison, so the
    D-10 format check and D-11's redirector shape check are exercised too.
    """
    spec = DATASETS[dataset]
    async with NesoDataPortalConnector(_live_config()) as connector:
        payload = await connector._package_show(spec.package)
        resource, redirector = connector._select_resource(payload, spec, dataset)

    assert resource["name"] == spec.resource_name
    assert str(resource.get("format", "")).upper() == spec.expected_format
    assert redirector.scheme == "https"


@pytest.mark.asyncio
@pytest.mark.parametrize("dataset", sorted(DATASETS))
async def test_the_live_csv_header_still_equals_the_d24_column_contract(dataset: str) -> None:
    """Pins **D-24**: this is what holds T-09's hand-authored fixtures to reality.

    The header contract is enforced twice offline (D-36's admission parse at
    fetch, the reader again at transform) — but both enforce it against the
    same declared tuple, so a vendor that adds a column breaks production and
    nothing else. Only a live read can tell the two apart.

    Exact **and ordered**: the offline contract is ordered, so a reordered
    live header is a real drift even though the column set is unchanged.
    """
    spec = DATASETS[dataset]
    async with NesoDataPortalConnector(_live_config()) as connector:
        payload = await connector._package_show(spec.package)
        _, redirector = connector._select_resource(payload, spec, dataset)
        _, status, _, header_line = await _walk_to_the_terminal_hop(connector, redirector)

    assert status == 200
    live_columns = tuple(header_line.decode("utf-8-sig").split(","))
    assert live_columns == spec.expected_columns, (
        f"{dataset}: NESO's live CSV header no longer matches D-24's contract. "
        "This is a vendor change to investigate — the fix is a deliberate "
        "contract update plus a re-transform from bronze, never a quiet edit "
        "to make the check pass"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("dataset", sorted(DATASETS))
async def test_the_live_redirect_chain_still_ends_on_a_host_d08_accepts(dataset: str) -> None:
    """Pins **D-08/D-11**: the one check no mocked test can make — a real signature.

    Offline, the presigned target is a string this repo wrote. Live, it is
    generated by the vendor and signed, and it must still satisfy D-08's target
    policy: ``https``, a globally-routable host, no userinfo. That every hop
    passed is proven by construction — ``_send`` validates before it sends, so
    reaching this assertion at all means the whole chain was accepted — and the
    terminal properties are asserted explicitly on top.

    A 2xx here is the live proof that the signature the vendor minted is
    honoured by the file host. An expired or malformed one answers 403.
    """
    spec = DATASETS[dataset]
    async with NesoDataPortalConnector(_live_config()) as connector:
        payload = await connector._package_show(spec.package)
        _, redirector = connector._select_resource(payload, spec, dataset)
        terminal, status, _, _ = await _walk_to_the_terminal_hop(connector, redirector)

    assert status == 200, f"{dataset}: the presigned target answered {status}, not 200"
    assert terminal.scheme == "https"
    # A CALL, not an attribute read: `has_userinfo` is a predicate METHOD on
    # `SafeUrl` (the credential-bearing components are deliberately exposed as
    # questions rather than values), and a bound method is always truthy — so
    # `assert not terminal.has_userinfo` would be a guaranteed failure and
    # `assert terminal.has_userinfo` a guaranteed pass. Neither would test
    # anything.
    assert not terminal.has_userinfo()
    assert terminal.host != redirector.host, (
        "the chain no longer leaves api.neso.energy, so D-11's redirector "
        "assumption — and the cross-host hop D-08 exists to police — no longer "
        "describes this vendor"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("dataset", sorted(DATASETS))
async def test_ckan_last_modified_still_reads_as_utc_against_the_http_header(dataset: str) -> None:
    """Pins **D-15**: the corroborated (never documented) UTC reading.

    CKAN's ``last_modified`` is naive. gridflow reads it as UTC on the strength
    of three observations, one of which is this comparison: RFC-7231's
    ``Last-Modified`` is GMT **by definition**, so a Europe/London reading would
    show up as an offset of almost exactly 3600 s during BST.

    This test DETECTS a change; it does not make the reading a contract —
    TODO-02 records what would. A failure means ``published_at`` for every row
    of this dataset is an hour out, which is silent corruption of the vintage
    axis, so it is worth one live request.
    """
    spec = DATASETS[dataset]
    async with NesoDataPortalConnector(_live_config()) as connector:
        payload = await connector._package_show(spec.package)
        resource, redirector = connector._select_resource(payload, spec, dataset)
        _, _, headers, _ = await _walk_to_the_terminal_hop(connector, redirector)

    raw_http = headers.get("last-modified")
    if raw_http is None:
        pytest.skip(f"{dataset}: the file host served no Last-Modified header to compare against")

    ckan = datetime.fromisoformat(str(resource["last_modified"])).replace(tzinfo=UTC)
    http = parsedate_to_datetime(raw_http).astimezone(UTC)
    delta = abs(ckan - http)

    assert delta < _D15_TOLERANCE, (
        f"{dataset}: CKAN last_modified {ckan.isoformat()} and HTTP "
        f"Last-Modified {http.isoformat()} are {delta} apart"
    )
    assert abs(delta - _BST_OFFSET) > _D15_TOLERANCE, (
        f"{dataset}: the two stamps differ by ~1 h ({delta}), which is exactly "
        "what a Europe/London reading of CKAN's naive stamp looks like during "
        "BST. D-15 would need revisiting before any further ingest"
    )


@pytest.mark.asyncio
async def test_discover_catalog_still_reconciles_and_traces_every_call() -> None:
    """Pins **D-17**: the permanent pagination reconciliation, and full evidence.

    ``rows``/``start`` paging is CKAN-generic but is **not** contracted by
    NESO, so ``discover_catalog`` compares the paginated name-set against
    ``package_list`` on every run and raises ``CkanPaginationMismatch`` rather
    than returning a short catalogue. Reaching the assertions below at all is
    that reconciliation passing live.

    The traces are asserted field by field because ``provenance.json`` (D-32)
    is built from them, and a provenance file whose fields can be placeholders
    is a hash-verified record of nothing. ``params`` is asserted with the rest
    (Sol review): it is the ONLY record of how the catalogue was paged, so a
    ``package_search`` trace that lost its ``rows``/``start`` would leave D-32's
    evidence unable to show which window each page covered — and every other
    field would still look complete.
    """
    async with NesoDataPortalConnector(_live_config()) as connector:
        discovery: CatalogDiscovery = await connector.discover_catalog()

    assert discovery.packages, "the live catalogue came back empty"
    assert discovery.traces, "no request evidence was produced for the snapshot contract"

    names = {str(package.get("name")) for package in discovery.packages}
    missing = sorted({spec.package for spec in DATASETS.values()} - names)
    assert not missing, f"packages this source depends on left the catalogue: {missing}"

    starts: list[int] = []
    for trace in discovery.traces:
        detail: Any = trace
        assert trace.action in {"package_search", "package_list"}, detail
        assert trace.status_code == 200, detail
        assert trace.started_at.tzinfo is not None, detail
        assert trace.finished_at >= trace.started_at, detail
        assert len(trace.body_sha256) == 64, detail
        assert trace.headers, "no response headers were captured for this call"

        if trace.action == "package_search":
            assert set(trace.params) == {"rows", "start"}, (
                f"a package_search trace carries {sorted(trace.params)} instead of "
                "the pagination parameters that are the only record of which "
                f"window this page covered: {detail}"
            )
            assert trace.params["rows"] == str(_PACKAGE_SEARCH_PAGE_SIZE), detail
            starts.append(int(trace.params["start"]))
        else:
            # `package_list` takes no parameters, so an EMPTY dict is the honest
            # record and anything else means a parameter was sent that this
            # connector does not believe it sends.
            assert trace.params == {}, detail

    assert starts, "no package_search page was traced at all"
    assert starts[0] == 0, f"pagination did not start at offset 0: {starts}"
    assert starts == sorted(set(starts)), (
        f"the traced page offsets are not strictly increasing: {starts}. Either a "
        "page was re-requested or the evidence no longer reflects the walk"
    )
