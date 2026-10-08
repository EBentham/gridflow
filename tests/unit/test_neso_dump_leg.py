"""The NESO datastore dump leg (ADR-035; unit D criteria 1, 2 and 4).

Every dump is driven through ``iter_members`` and published with
``BronzeWriter.publish_capture`` exactly as the runner's member branch does,
against a synthetic registry installed through ADR-033 P-1's seam. HTTP is
respx-mocked, the resolver stubbed, and the pacer is a fake-clock
``RunPacer`` patched in through ``shared_pacer`` (``bind_data_dir`` replaces
any injected pacer), so no test waits 30 s. Cadence tests keep one ``end``
near the wall clock (D-34) and move the past instead: they rewrite sidecar
``written_at`` values and the check stamp (operational state under the test's
tmp data root, never real bronze).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import shutil
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import respx
from _neso_registry_support import (
    edit_sidecar,
    family,
    install_registry,
    package,
    resource,
    write_capture,
    write_registry,
)
from tenacity import wait_none

from gridflow.bronze.writer import BronzeWriter
from gridflow.config.settings import DatasetConfig, SourceConfig
from gridflow.connectors.neso_data_portal import files as files_module
from gridflow.connectors.neso_data_portal import pacer as pacer_module
from gridflow.connectors.neso_data_portal.captures import scan_dataset
from gridflow.connectors.neso_data_portal.client import (
    NesoDataPortalConnector,
    NesoDatastoreMemberError,
)
from gridflow.connectors.neso_data_portal.endpoints import build_dump_path
from gridflow.connectors.neso_data_portal.pacer import Lane, RunPacer

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Iterator
    from pathlib import Path

    from gridflow.connectors.base import MemberEvent

pytestmark = pytest.mark.usefixtures("stub_neso_resolver")

SOURCE = "neso_data_portal"
BASE_URL = "https://api.neso.energy"
PKG = "bbbbbbbb-0000-4000-8000-000000000000"
FAMILY = "dump_series"
LM = "2026-09-17T08:45:57.592028"
MM = "2026-10-06T10:00:00.000001"
HEADER = b"SettlementDate,SettlementPeriod,Unit,Value\n"
BODY_P = HEADER + b"2026-10-07,1,A,1.5\n2026-10-07,2,A,2.5\n"
BODY_Q = HEADER + b"2026-10-07,1,A,9.5\n"


def _rid(n: int) -> str:
    return f"bbbbbbbb-0000-4000-8000-{n:012d}"


RID = _rid(10)
DUMP_URL = f"{BASE_URL}/datastore/dump/{RID}"


class FakeClock:
    """A monotonic clock and an async sleep that advances it."""

    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds
        await asyncio.sleep(0)


@pytest.fixture(autouse=True)
def _release_pacers() -> Iterator[None]:
    yield
    pacer_module.reset_shared_pacers()


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeClock]:
    """One fake-clock pacer for the whole test, bound like the real shared one."""
    fake = FakeClock()
    pacer = RunPacer(1.0, 30.0, monotonic=fake.monotonic, sleep=fake.sleep)

    def _shared(config: SourceConfig, state_dir: Path | None = None) -> RunPacer:
        if state_dir is not None:
            pacer.bind(state_dir)
        return pacer

    monkeypatch.setattr(pacer_module, "shared_pacer", _shared)
    yield fake
    pacer.close()


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    with respx.mock(assert_all_called=False) as mock_router:
        yield mock_router


@pytest.fixture
def no_retry_backoff() -> Iterator[None]:
    retrying = NesoDataPortalConnector._send.retry  # type: ignore[attr-defined]
    original = retrying.wait
    retrying.wait = wait_none()
    try:
        yield
    finally:
        retrying.wait = original


@pytest.fixture
def data_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("d")


def _config(*datasets: str) -> SourceConfig:
    return SourceConfig(
        base_url=BASE_URL,
        rate_limit_per_second=1,
        timeout=30,
        datasets={d: DatasetConfig(endpoint="/api/3/action/package_show") for d in datasets},
    )


def _install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    refresh: str = "daily",
    empty_allowed: bool = False,
    max_download_bytes: int = 64 * 1024 * 1024,
) -> Any:
    return install_registry(
        monkeypatch,
        write_registry(
            tmp_path / "registry",
            [
                package(
                    "pkg-dump",
                    PKG,
                    [
                        family(
                            FAMILY,
                            refresh=refresh,
                            empty_allowed=empty_allowed,
                            max_download_bytes=max_download_bytes,
                        ),
                        family("dump_other", refresh=refresh),
                        family("upload_family"),
                        family("other_family"),
                    ],
                    [
                        resource(RID, "Dump Member", FAMILY, url_type="datastore"),
                        resource(_rid(12), "Dump Upload Twin", FAMILY),
                        resource(_rid(30), "Other Dump", "dump_other", url_type="datastore"),
                        resource(_rid(1), "Upload Member", "upload_family"),
                        resource(_rid(20), "Foreign Dump", "other_family", url_type="datastore"),
                    ],
                )
            ],
        ),
    )


def _live(
    rid: str = RID,
    name: str = "Dump Member",
    *,
    fmt: str = "CSV",
    url_type: str = "datastore",
    last_modified: str | None = None,
    metadata_modified: str = MM,
    url: str | None = None,
) -> dict[str, Any]:
    return {
        "id": rid,
        "name": name,
        "format": fmt,
        "url_type": url_type,
        "last_modified": last_modified,
        "metadata_modified": metadata_modified,
        "url": url if url is not None else f"{BASE_URL}/datastore/dump/{rid}",
    }


class _Server:
    """A respx handler: ``package_show`` plus queued replies per URL.

    Each URL's queue is consumed in order and its last reply repeats. A reply
    is bytes (a 200), an ``httpx.Response`` or a zero-argument factory of one.
    Every request is recorded with the fake-clock instant it reached the
    transport.
    """

    def __init__(self, router: respx.MockRouter, clock: FakeClock, live: list[Any]) -> None:
        self.live = live
        self.clock = clock
        self.replies: dict[str, list[Any]] = {}
        self.calls: list[tuple[str, float]] = []
        router.route(url__regex=r".*").mock(side_effect=self._handle)

    def serve(self, url: str, *replies: Any) -> None:
        self.replies[url] = list(replies)

    def dumps(self) -> list[str]:
        return [url for url, _at in self.calls if "/api/3/action/" not in url]

    def _handle(self, request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        self.calls.append((url, self.clock.now))
        if "/api/3/action/package_show" in url:
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "result": {"id": PKG, "name": "pkg-dump", "resources": self.live},
                },
            )
        queue = self.replies.get(url)
        if not queue:
            return httpx.Response(404)
        reply = queue.pop(0) if len(queue) > 1 else queue[0]
        if callable(reply):
            reply = reply()
        if isinstance(reply, bytes):
            return httpx.Response(200, content=reply)
        assert isinstance(reply, httpx.Response)
        return reply


def _consume(
    data_dir: Path,
    dataset: str = FAMILY,
    *,
    end: datetime | None = None,
) -> tuple[list[MemberEvent], list[Path]]:
    end = end or _end()
    events: list[MemberEvent] = []
    published: list[Path] = []
    writer = BronzeWriter(data_dir)

    async def _run() -> None:
        connector = NesoDataPortalConnector(_config(dataset))
        connector.bind_data_dir(data_dir)
        async with connector:
            async for event in connector.iter_members(dataset, end - timedelta(hours=1), end):
                if event.outcome == "captured":
                    assert event.response is not None and event.extension is not None
                    published.append(
                        writer.publish_capture(event.response, extension=event.extension)
                    )
                events.append(event)

    asyncio.run(_run())
    return events, published


def _end() -> datetime:
    return datetime.now(UTC) - timedelta(minutes=1)


def _next_second() -> None:
    """Wait for the wall-clock second to roll over (capture names carry it)."""
    start = datetime.now(UTC).replace(microsecond=0)
    while datetime.now(UTC).replace(microsecond=0) == start:
        time.sleep(0.02)


def _present(events: list[MemberEvent]) -> list[tuple[str, str]]:
    return [(e.resource_id, e.outcome) for e in events if e.outcome != "absent"]


def _meta(body: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(body.with_suffix(".meta.json").read_text(encoding="utf-8"))
    return loaded


def _sidecars(data_dir: Path, dataset: str = FAMILY) -> list[Path]:
    return sorted((data_dir / "bronze" / SOURCE / dataset).rglob("raw_*.meta.json"))


def _bronze_files(data_dir: Path, dataset: str = FAMILY) -> list[Path]:
    root = data_dir / "bronze" / SOURCE / dataset
    return sorted(p for p in root.rglob("*") if p.is_file()) if root.is_dir() else []


def _set_written_at(body: Path, when: datetime) -> None:
    def _mutate(meta: dict[str, Any]) -> None:
        meta["written_at"] = when.isoformat()

    edit_sidecar(body.with_suffix(".meta.json"), _mutate)


def _stamp(data_dir: Path, rid: str = RID, dataset: str = FAMILY) -> Path:
    return data_dir / "state" / SOURCE / "dump_checks" / dataset / f"{rid}.json"


def _write_stamp(data_dir: Path, **document: Any) -> None:
    path = _stamp(data_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document), encoding="utf-8")


def _rewrite_stamp(data_dir: Path, **changes: Any) -> None:
    document = json.loads(_stamp(data_dir).read_text(encoding="utf-8"))
    document.update(changes)
    _write_stamp(data_dir, **document)


class _BrokenStream(httpx.AsyncByteStream):
    """A body whose connection drops mid-transfer."""

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield HEADER
        raise httpx.RemoteProtocolError("peer closed connection without sending complete body")


# ---------------------------------------------------------------------------
# P-2: the dump target
# ---------------------------------------------------------------------------


class TestDumpTarget:
    """T-D1-4 / T-D1-5 (P-2): the dump path is built only from a registry-seeded id."""

    def test_d1_5_build_dump_path_accepts_only_a_canonical_id(self) -> None:
        """Detects a dump path built from anything but a canonical lowercase UUID (D-39)."""
        assert build_dump_path(RID) == f"/datastore/dump/{RID}"
        for bad in ("../x", RID.upper(), "", f"{RID}/../y", f"{RID}?x=1"):
            with pytest.raises(ValueError, match="canonical"):
                build_dump_path(bad)

    @pytest.mark.parametrize(
        ("live_id", "live_format", "match"),
        [
            (_rid(99), "CSV", "not seeded"),
            (_rid(20), "CSV", "seeded under family 'other_family'"),
            (_rid(12), "CSV", "seeded as 'upload'"),
            (RID.upper(), "CSV", "canonical"),
            (RID, "XLSX", "a datastore dump is CSV"),
        ],
    )
    def test_d1_4_registry_gate(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        live_id: str,
        live_format: str,
        match: str,
    ) -> None:
        """Detects a dump target accepted without the registry seeding it as this family's dump."""
        registry = _install(tmp_path, monkeypatch)
        connector = NesoDataPortalConnector(_config(FAMILY))
        live = {"id": live_id, "format": live_format, "url_type": "datastore"}
        with pytest.raises(NesoDatastoreMemberError, match=match):
            connector._dump_member_target(live, FAMILY, registry)
        good = {"id": RID, "format": "csv", "url_type": "datastore"}
        assert connector._dump_member_target(good, FAMILY, registry) == RID

    @pytest.mark.parametrize("live_id", [_rid(99), _rid(20), _rid(12)])
    def test_d1_4_a_gated_member_fails_with_zero_dump_requests(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        live_id: str,
    ) -> None:
        """Detects a dump fetched for an id the registry does not seed as this family's dump."""
        _install(tmp_path, monkeypatch)
        server = _Server(router, clock, [_live(live_id)])
        server.serve(f"{BASE_URL}/datastore/dump/{live_id}", BODY_P)
        events, paths = _consume(data_dir)
        assert _present(events) == [(live_id, "failed")]
        assert "NesoDatastoreMemberError" in events[-1].detail
        assert server.dumps() == [] and paths == []

    def test_d1_5_the_live_url_is_never_requested(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Detects the dump URL being read from ``resources[].url`` (D-39 nonce)."""
        _install(tmp_path, monkeypatch)
        evil = f"https://evil.example/datastore/dump/{RID}?x=1"
        server = _Server(router, clock, [_live(url=evil)])
        server.serve(DUMP_URL, BODY_P)
        server.serve(evil, BODY_Q)
        events, paths = _consume(data_dir)
        assert _present(events) == [(RID, "captured")]
        assert server.dumps() == [DUMP_URL]
        assert _meta(paths[0])["request_url"] == DUMP_URL


# ---------------------------------------------------------------------------
# Criterion 1: a dump lands with provenance and parses (P-2..P-5, P-9)
# ---------------------------------------------------------------------------


class TestDumpCapture:
    def test_d1_1_dump_lands_with_provenance_and_parses(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Detects a dump captured without D-12 provenance, with ``"None"``, or unparseable."""
        import polars as pl
        from _neso_generic_support import install_generated
        from _neso_registry_support import record

        _registry, generated = install_generated(
            monkeypatch,
            tmp_path / "registry",
            [
                package(
                    "pkg-dump",
                    PKG,
                    [family(FAMILY, record=record(vintage="capture_fallback"))],
                    [resource(RID, "Dump Member", FAMILY, url_type="datastore")],
                )
            ],
        )
        server = _Server(router, clock, [_live(last_modified=None)])
        server.serve(
            DUMP_URL,
            httpx.Response(200, content=BODY_P, headers={"Last-Modified": "Wed, 07 Oct 2026"}),
        )
        end = _end()
        events, (body,) = _consume(data_dir, end=end)
        assert _present(events) == [(RID, "captured")]
        sha = hashlib.sha256(BODY_P).hexdigest()
        assert body.name.startswith("raw_") and body.name.endswith(f"_{RID}_{sha[:8]}.csv")
        meta = _meta(body)
        assert meta["request_url"] == DUMP_URL
        assert meta["http_status"] == 200 and meta["content_type"] == "text/csv"
        assert meta["request_params"] == {
            "package": "pkg-dump",
            "package_id": PKG,
            "resource_id": RID,
            "resource_name": "Dump Member",
            "resource_filename": RID,
            "ckan_last_modified": "",
            "ckan_format": "CSV",
            "body_sha256": sha,
            "capture_family": FAMILY,
            "url_type": "datastore",
            "empty_capture": False,
            "declared_content_length": len(BODY_P),
            "ckan_metadata_modified": MM,
            "response_last_modified": "Wed, 07 Oct 2026",
        }
        assert generated.transformers[FAMILY](data_dir).run(end.date(), run_id="r") == 2
        (output,) = sorted((data_dir / "silver" / SOURCE / FAMILY).rglob("[!.]*.parquet"))
        frame = pl.read_parquet(output, hive_partitioning=False)
        assert frame["value"].to_list() == [1.5, 2.5]

    @pytest.mark.parametrize(
        ("label", "reply", "declared", "last_modified"),
        [
            ("200 with Content-Length", BODY_P, len(BODY_P), None),
            (
                "200 chunked",
                lambda: httpx.Response(200, stream=httpx.ByteStream(BODY_P)),
                None,
                None,
            ),
            (
                "200 with Last-Modified",
                lambda: httpx.Response(200, content=BODY_P, headers={"Last-Modified": "Tue, x"}),
                len(BODY_P),
                "Tue, x",
            ),
            (
                "200 chunked with Last-Modified",
                lambda: httpx.Response(
                    200, stream=httpx.ByteStream(BODY_P), headers={"Last-Modified": "Mon, y"}
                ),
                None,
                "Mon, y",
            ),
        ],
    )
    def test_d1_2_delivery_shapes(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        label: str,
        reply: Any,
        declared: int | None,
        last_modified: str | None,
    ) -> None:
        """Detects a delivery shape (C-2) that is refused or recorded wrongly."""
        _install(tmp_path, monkeypatch)
        server = _Server(router, clock, [_live()])
        server.serve(DUMP_URL, reply)
        _events, (body,) = _consume(data_dir)
        assert body.read_bytes() == BODY_P, label
        params = _meta(body)["request_params"]
        assert params["declared_content_length"] == declared, label
        assert params["response_last_modified"] == last_modified, label

    def test_d1_2_same_host_302_is_two_sends_on_the_datastore_lane(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Detects a same-host hop refused, or sent outside the 30 s datastore lane."""
        _install(tmp_path, monkeypatch)
        hop = f"{BASE_URL}/datastore/dump-files/{RID}.csv"
        server = _Server(router, clock, [_live()])
        server.serve(
            DUMP_URL,
            lambda: httpx.Response(302, headers={"location": f"/datastore/dump-files/{RID}.csv"}),
        )
        server.serve(hop, BODY_P)
        events, (body,) = _consume(data_dir)
        assert _present(events) == [(RID, "captured")]
        dumps = [(url, at) for url, at in server.calls if "/api/3/action/" not in url]
        assert [url for url, _at in dumps] == [DUMP_URL, hop]
        assert dumps[1][1] - dumps[0][1] == 30.0
        assert _meta(body)["request_url"] == DUMP_URL

    @pytest.mark.parametrize(
        ("label", "reply", "error"),
        [
            (
                "off-host 302",
                lambda: httpx.Response(302, headers={"location": "https://files.example.org/x"}),
                "NesoDumpRedirectError",
            ),
            ("HTML 200", b"<!doctype html><html></html>", "NesoUnexpectedBodyError"),
            ("JSON envelope", b'{"success": false}', "NesoUnexpectedBodyError"),
            ("PK body", b"PK\x03\x04" + b"\x00" * 40, "NesoUnexpectedBodyError"),
            ("HTTP 206", lambda: httpx.Response(206, content=BODY_P), "NesoUnexpectedStatusError"),
            (
                "truncated",
                lambda: httpx.Response(
                    200,
                    stream=httpx.ByteStream(BODY_P),
                    headers={"Content-Length": str(len(BODY_P) + 10)},
                ),
                "NesoTruncatedBodyError",
            ),
            (
                "too many same-host hops",
                lambda: httpx.Response(302, headers={"location": f"/datastore/dump/{RID}"}),
                "NesoRedirectLoopError",
            ),
        ],
    )
    def test_d1_3_shape_refusals(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        label: str,
        reply: Any,
        error: str,
    ) -> None:
        """Detects a refused delivery shape reaching bronze (FM-14)."""
        _install(tmp_path, monkeypatch)
        server = _Server(router, clock, [_live()])
        server.serve(DUMP_URL, reply)
        events, paths = _consume(data_dir)
        assert _present(events) == [(RID, "failed")], label
        assert error in events[-1].detail, (label, events[-1].detail)
        assert paths == [] and _bronze_files(data_dir) == []
        assert not any("files.example.org" in url for url, _at in server.calls)

    def test_d1_3_a_body_over_the_cap_is_refused(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Detects ``max_download_bytes`` not bounding a dump (A9)."""
        _install(tmp_path, monkeypatch, max_download_bytes=16)
        server = _Server(router, clock, [_live()])
        server.serve(DUMP_URL, lambda: httpx.Response(200, stream=httpx.ByteStream(BODY_P)))
        events, paths = _consume(data_dir)
        assert _present(events) == [(RID, "failed")]
        assert "NesoResponseTooLargeError" in events[-1].detail
        assert paths == []

    def test_d1_6_a_mid_stream_drop_publishes_nothing_and_a_rerun_captures(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """FM-1: detects a partial body reaching bronze or blocking the next run."""
        _install(tmp_path, monkeypatch)
        server = _Server(router, clock, [_live()])
        server.serve(DUMP_URL, lambda: httpx.Response(200, stream=_BrokenStream()), BODY_P)
        events, _paths = _consume(data_dir)
        assert _present(events) == [(RID, "failed")]
        assert _bronze_files(data_dir) == [] and not _stamp(data_dir).exists()
        events, paths = _consume(data_dir)
        assert _present(events) == [(RID, "captured")]
        assert paths[0].read_bytes() == BODY_P


# ---------------------------------------------------------------------------
# Criterion 4: content-hash dedup (P-8), never metadata
# ---------------------------------------------------------------------------


class TestDedup:
    def test_d4_1_changed_bytes_unchanged_metadata_write_a_capture(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """FM-7 / FM-9: detects a metadata-only skip, or a duplicate write of identical bytes."""
        _install(tmp_path, monkeypatch)
        server = _Server(router, clock, [_live(last_modified=LM)])
        server.serve(DUMP_URL, BODY_P)
        first, _ = _consume(data_dir)
        server.serve(DUMP_URL, BODY_Q)
        second, _ = _consume(data_dir)
        third, _ = _consume(data_dir)
        assert [_present(e) for e in (first, second, third)] == [
            [(RID, "captured")],
            [(RID, "captured")],
            [(RID, "unchanged")],
        ]
        assert "identical" in third[-1].detail
        assert len(_sidecars(data_dir)) == 2
        assert server.dumps() == [DUMP_URL] * 3

    def test_d4_2_populated_empty_populated_writes_three(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """FM-8: detects a body equal to an older (non-newest) capture being suppressed."""
        _install(tmp_path, monkeypatch, empty_allowed=True)
        server = _Server(router, clock, [_live()])
        written: list[Path] = []
        for body in (BODY_P, HEADER, BODY_P):
            _next_second()  # P and the final P share a name within one second (BronzeWriter)
            server.serve(DUMP_URL, body)
            events, paths = _consume(data_dir)
            assert _present(events) == [(RID, "captured")]
            written.extend(paths)
        assert len(written) == 3
        assert [_meta(p)["request_params"]["empty_capture"] for p in written] == [
            False,
            True,
            False,
        ]
        assert written[2].read_bytes() == written[0].read_bytes()

    def test_d4_4_an_unusable_sidecar_removes_the_basis(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """FM-4 / FM-5: detects suppression or a cadence skip resting on an unverified state."""
        _install(tmp_path, monkeypatch, refresh="frozen")
        server = _Server(router, clock, [_live()])
        server.serve(DUMP_URL, BODY_P)
        end = _end()
        _events, (body,) = _consume(data_dir, end=end)
        _set_written_at(body, end - timedelta(days=1))
        sha = hashlib.sha256(BODY_P).hexdigest()
        _write_stamp(
            data_dir,
            body_sha256=sha,
            resource_id=RID,
            verified_at=(end - timedelta(days=1)).isoformat(),
        )

        # A newer unusable sidecar naming RID (a sidecar with no body).
        bogus = body.parent / f"raw_29990101T000000Z_{RID}_deadbeef.meta.json"
        bogus.write_text(json.dumps({"request_params": {"resource_id": RID}}), encoding="utf-8")
        _next_second()
        events, paths = _consume(data_dir, end=end)
        assert _present(events) == [(RID, "captured")] and len(paths) == 1
        bogus.unlink()

        # An unusable sidecar that names no resource at all.
        _set_written_at(paths[0], end - timedelta(days=1))
        (body.parent / "raw_29990101T000000Z_anon.meta.json").write_text(
            "{not json", encoding="utf-8"
        )
        _next_second()
        events, paths = _consume(data_dir, end=end)
        assert _present(events) == [(RID, "captured")] and len(paths) == 1
        assert len(server.dumps()) == 3

    @pytest.mark.parametrize("fault", ["altered", "unreadable"])
    @pytest.mark.parametrize("refresh", ["daily", "frozen"])
    def test_d4_5_a_basis_that_fails_its_rehash_is_no_basis(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        fault: str,
        refresh: str,
    ) -> None:
        """FM-6: detects suppression, or a frozen skip, on a body no longer matching its hash."""
        _install(tmp_path, monkeypatch, refresh=refresh)
        server = _Server(router, clock, [_live()])
        server.serve(DUMP_URL, BODY_P)
        end = _end()
        _events, (body,) = _consume(data_dir, end=end)
        _set_written_at(body, end - timedelta(days=3))
        if refresh == "frozen":
            # A valid stamp as well: the re-hash must still win.
            _write_stamp(
                data_dir,
                body_sha256=hashlib.sha256(BODY_P).hexdigest(),
                resource_id=RID,
                verified_at=(end - timedelta(days=1)).isoformat(),
            )
        if fault == "altered":
            body.write_bytes(BODY_P.replace(b"1.5", b"7.5"))  # same size, other bytes
        else:
            path_type = type(body)
            real_open = path_type.open

            def _refuse(self: Path, *args: Any, **kwargs: Any) -> Any:
                if self == body:
                    raise PermissionError("locked")
                return real_open(self, *args, **kwargs)

            monkeypatch.setattr(path_type, "open", _refuse)
        before = len(server.dumps())
        _next_second()
        events, paths = _consume(data_dir, end=end)
        assert _present(events) == [(RID, "captured")] and len(paths) == 1
        assert len(server.dumps()) == before + 1

    def test_d4_6_orphan_and_temp_are_never_a_basis(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """FM-2 / FM-3: detects an orphan body or a temp file acting as the dedup basis."""
        _install(tmp_path, monkeypatch)
        server = _Server(router, clock, [_live()])
        server.serve(DUMP_URL, BODY_P)
        _events, (committed,) = _consume(data_dir)
        sha_q = hashlib.sha256(BODY_Q).hexdigest()
        (committed.parent / f"raw_29990101T000000Z_{RID}_{sha_q[:8]}.csv").write_bytes(BODY_Q)
        (committed.parent / f".tmp_{RID}_x").write_bytes(BODY_Q)
        server.serve(DUMP_URL, BODY_Q)
        events, paths = _consume(data_dir)
        assert _present(events) == [(RID, "captured")] and len(paths) == 1
        events, paths = _consume(data_dir)
        assert _present(events) == [(RID, "unchanged")] and paths == []

    def test_d4_7_a_written_at_tie_picks_the_capture_b_ranks_first(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """FM-10: detects the dedup basis and B's latest-selection order disagreeing on a tie."""
        from gridflow.silver.neso_data_portal import generic

        registry = _install(tmp_path, monkeypatch)
        server = _Server(router, clock, [_live()])
        server.serve(DUMP_URL, BODY_P)
        _events, (first,) = _consume(data_dir)
        server.serve(DUMP_URL, BODY_Q)
        _events, (second,) = _consume(data_dir)
        tie = datetime.now(UTC) - timedelta(hours=1)
        _set_written_at(first, tie)
        _set_written_at(second, tie)
        winner = max((first, second), key=str)

        assert generic._TIEBREAK == ("capture_written_at", "bronze_capture_id")
        captures = scan_dataset(data_dir / "bronze" / SOURCE / FAMILY, registry).captures
        b_first = max(
            captures, key=lambda c: (c.written_at, c.body.relative_to(data_dir).as_posix())
        )
        assert b_first.body == winner

        server.serve(DUMP_URL, winner.read_bytes())
        events, _paths = _consume(data_dir)
        assert _present(events) == [(RID, "unchanged")]
        assert winner.name in events[-1].detail


class TestUploadSkipConjunct:
    def test_d3_6_a_dump_capture_never_skips_an_upload(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Detects an upload skipped on a datastore sidecar's ``last_modified`` (P-6)."""
        _install(tmp_path, monkeypatch)
        upload = _rid(1)
        _body, sidecar = write_capture(
            data_dir / "bronze" / SOURCE / "upload_family",
            package_slug="pkg-dump",
            package_id=PKG,
            resource_id=upload,
            resource_name="Upload Member",
            ckan_last_modified=LM,
        )

        def _as_dump(meta: dict[str, Any]) -> None:
            meta["request_params"]["url_type"] = "datastore"

        edit_sidecar(sidecar, _as_dump)
        url = f"{BASE_URL}/dataset/{PKG}/resource/{upload}/download/f.csv"
        server = _Server(
            router,
            clock,
            [_live(upload, "Upload Member", url_type="upload", last_modified=LM, url=url)],
        )
        server.serve(url, BODY_P)
        events, paths = _consume(data_dir, "upload_family")
        assert _present(events) == [(upload, "captured")] and len(paths) == 1
        assert server.dumps() == [url]


# ---------------------------------------------------------------------------
# Criterion 4: the frozen-class cadence and its check stamp (P-7)
# ---------------------------------------------------------------------------


class TestFrozenCadence:
    """T-D4-3 / T-D4-8 (P-7, decision 11)."""

    @staticmethod
    def _seed(
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        *,
        age_days: int,
        refresh: str = "frozen",
    ) -> tuple[_Server, datetime]:
        _install(tmp_path, monkeypatch, refresh=refresh)
        server = _Server(router, clock, [_live()])
        server.serve(DUMP_URL, BODY_P)
        end = _end()
        _events, (body,) = _consume(data_dir, end=end)
        _set_written_at(body, end - timedelta(days=age_days))
        server.calls.clear()
        return server, end

    @pytest.mark.parametrize(("age", "due"), [(3, False), (6, False), (7, True), (8, True)])
    def test_d4_3_age_against_the_cadence(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        age: int,
        due: bool,
    ) -> None:
        """Detects a frozen dump skipped past its week or fetched inside it (red: deferred)."""
        server, end = self._seed(router, clock, data_dir, tmp_path, monkeypatch, age_days=age)
        events, _paths = _consume(data_dir, end=end)
        assert _present(events) == [(RID, "unchanged")]
        assert len(server.dumps()) == (1 if due else 0), age
        if not due:
            assert "not due: frozen-class dump" in events[-1].detail

    def test_d4_3_no_capture_is_due(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Detects a frozen member with no basis being skipped."""
        _install(tmp_path, monkeypatch, refresh="frozen")
        server = _Server(router, clock, [_live()])
        server.serve(DUMP_URL, BODY_P)
        events, _paths = _consume(data_dir)
        assert _present(events) == [(RID, "captured")] and server.dumps() == [DUMP_URL]

    def test_d4_3_daily_control_is_fetched_and_never_stamped(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Detects the cadence gate or the stamp leaking to a non-frozen family."""
        server, end = self._seed(
            router, clock, data_dir, tmp_path, monkeypatch, age_days=0, refresh="daily"
        )
        events, _paths = _consume(data_dir, end=end)
        assert _present(events) == [(RID, "unchanged")] and len(server.dumps()) == 1
        assert not (data_dir / "state" / SOURCE / "dump_checks").exists()

    def test_d4_3_repeated_unchanged_checks_follow_the_stamp(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Detects a frozen dump re-fetched every run once its capture is a week old."""
        server, end = self._seed(router, clock, data_dir, tmp_path, monkeypatch, age_days=8)
        sha = hashlib.sha256(BODY_P).hexdigest()

        events, _ = _consume(data_dir, end=end)  # run 1: fetched, identical, stamped
        assert _present(events) == [(RID, "unchanged")] and len(server.dumps()) == 1
        stamp = json.loads(_stamp(data_dir).read_text(encoding="utf-8"))
        assert stamp == {"body_sha256": sha, "resource_id": RID, "verified_at": end.isoformat()}
        assert _stamp(data_dir).read_bytes().endswith(b"}\n")

        _consume(data_dir, end=end)  # run 2: not due
        assert len(server.dumps()) == 1
        _rewrite_stamp(data_dir, verified_at=(end - timedelta(days=6)).isoformat())
        _consume(data_dir, end=end)  # run 3: still inside the week
        assert len(server.dumps()) == 1
        _rewrite_stamp(data_dir, verified_at=(end - timedelta(days=7)).isoformat())
        _consume(data_dir, end=end)  # run 4: due again, re-stamped
        assert len(server.dumps()) == 2
        restamped = json.loads(_stamp(data_dir).read_text(encoding="utf-8"))
        assert restamped["verified_at"] == end.isoformat()
        assert len(_sidecars(data_dir)) == 1

    def test_d4_3_failed_checks_never_stamp(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        no_retry_backoff: None,
    ) -> None:
        """Detects a stamp advancing the clock on a check that never verified the body."""
        server, end = self._seed(router, clock, data_dir, tmp_path, monkeypatch, age_days=8)
        server.serve(DUMP_URL, lambda: httpx.Response(500, content=b"boom"))
        events, _ = _consume(data_dir, end=end)
        assert _present(events) == [(RID, "failed")] and not _stamp(data_dir).exists()
        server.serve(DUMP_URL, b"<html>maintenance</html>")
        events, _ = _consume(data_dir, end=end)
        assert _present(events) == [(RID, "failed")] and not _stamp(data_dir).exists()
        server.serve(DUMP_URL, BODY_P)
        before = len(server.dumps())
        events, _ = _consume(data_dir, end=end)
        assert _present(events) == [(RID, "unchanged")] and _stamp(data_dir).exists()
        assert len(server.dumps()) == before + 1

    @pytest.mark.parametrize(
        "stamp",
        ["malformed", "list", "other resource", "other hash", "naive", "after end", "temp only"],
    )
    def test_d4_8_a_faulty_stamp_is_never_honoured(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        stamp: str,
    ) -> None:
        """FM-19..FM-23: detects a stamp extending a skip it cannot support."""
        server, end = self._seed(router, clock, data_dir, tmp_path, monkeypatch, age_days=10)
        good = {
            "body_sha256": hashlib.sha256(BODY_P).hexdigest(),
            "resource_id": RID,
            "verified_at": (end - timedelta(days=1)).isoformat(),
        }
        content = {
            "malformed": "{not json",
            "list": json.dumps([good]),
            "other resource": json.dumps({**good, "resource_id": _rid(30)}),
            "other hash": json.dumps({**good, "body_sha256": "0" * 64}),
            "naive": json.dumps({**good, "verified_at": "2026-10-07T10:00:00"}),
            "after end": json.dumps({**good, "verified_at": (end + timedelta(days=1)).isoformat()}),
            "temp only": json.dumps(good),
        }[stamp]
        path = _stamp(data_dir)
        path.parent.mkdir(parents=True, exist_ok=True)
        if stamp == "temp only":
            path = path.with_name(f".{path.name}.tmp_0123456789abcdef")
        path.write_text(content, encoding="utf-8")
        _consume(data_dir, end=end)
        assert len(server.dumps()) == 1, stamp

    def test_d4_8_a_valid_stamp_is_honoured(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The positive control for T-D4-8's negatives."""
        server, end = self._seed(router, clock, data_dir, tmp_path, monkeypatch, age_days=10)
        _write_stamp(
            data_dir,
            body_sha256=hashlib.sha256(BODY_P).hexdigest(),
            resource_id=RID,
            verified_at=(end - timedelta(days=1)).isoformat(),
        )
        events, _ = _consume(data_dir, end=end)
        assert server.dumps() == [] and "(check stamp)" in events[-1].detail

    def test_d4_8_a_failed_stamp_write_warns_and_the_next_run_is_due(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """FM-20: detects a stamp-write failure turning a correct suppression into a failure."""
        server, end = self._seed(router, clock, data_dir, tmp_path, monkeypatch, age_days=10)

        def _disk_full(path: Path, data: bytes) -> None:
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(files_module, "replace_atomically", _disk_full)
        with caplog.at_level(logging.WARNING):
            events, _ = _consume(data_dir, end=end)
        assert _present(events) == [(RID, "unchanged")]
        assert any("could not write check stamp" in r.getMessage() for r in caplog.records)
        _consume(data_dir, end=end)
        assert len(server.dumps()) == 2

    def test_d4_8_a_stamp_outliving_its_bronze_is_ignored(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """FM-22: detects a stamp standing without a basis after a bronze reset."""
        _server, end = self._seed(router, clock, data_dir, tmp_path, monkeypatch, age_days=10)
        _consume(data_dir, end=end)  # identical: stamped
        assert _stamp(data_dir).exists()
        shutil.rmtree(data_dir / "bronze" / SOURCE / FAMILY)
        events, paths = _consume(data_dir, end=end)
        assert _present(events) == [(RID, "captured")] and len(paths) == 1


# ---------------------------------------------------------------------------
# Criterion 2: the datastore lane across members, connectors, retries and hops
# ---------------------------------------------------------------------------


class TestDatastorePacing:
    def test_d2_3_two_families_two_connectors_one_lane(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Detects the datastore interval resetting at a dataset or connector change."""
        _install(tmp_path, monkeypatch)
        server = _Server(router, clock, [_live(), _live(_rid(30), "Other Dump")])
        server.serve(DUMP_URL, BODY_P)
        server.serve(f"{BASE_URL}/datastore/dump/{_rid(30)}", BODY_Q)
        _consume(data_dir, FAMILY)
        _consume(data_dir, "dump_other")
        dumps = [at for url, at in server.calls if "/datastore/dump/" in url]
        ckan = [at for url, at in server.calls if "package_show" in url]
        assert len(dumps) == 2 and dumps[1] - dumps[0] == 30.0, dumps
        assert len(ckan) == 2 and ckan[1] - ckan[0] >= 1.0, ckan
        assert ckan[1] < dumps[1], "the CKAN call between the dumps was held on the datastore lane"

    def test_d2_4_retries_and_hops_are_each_admitted(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        no_retry_backoff: None,
    ) -> None:
        """Detects a retry attempt or a redirect hop bypassing the datastore lane."""
        _install(tmp_path, monkeypatch)
        hop = f"{BASE_URL}/datastore/dump-files/{RID}.csv"
        server = _Server(router, clock, [_live()])
        server.serve(
            DUMP_URL,
            lambda: httpx.Response(500, content=b"boom"),
            lambda: httpx.Response(302, headers={"location": hop}),
        )
        server.serve(hop, BODY_P)
        events, _ = _consume(data_dir)
        assert _present(events) == [(RID, "captured")]
        dumps = [at for url, at in server.calls if "/api/3/action/" not in url]
        assert len(dumps) == 3
        assert all(b - a >= 30.0 for a, b in zip(dumps, dumps[1:], strict=False)), dumps

    def test_every_dump_send_is_admitted_on_the_datastore_lane(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Detects a dump send admitted on the CKAN lane (P-1)."""
        _install(tmp_path, monkeypatch)
        lanes: list[Lane] = []
        real = NesoDataPortalConnector._throttle_request

        async def _spy(self: NesoDataPortalConnector, lane: Lane = Lane.CKAN) -> None:
            lanes.append(lane)
            await real(self, lane)

        monkeypatch.setattr(NesoDataPortalConnector, "_throttle_request", _spy)
        server = _Server(router, clock, [_live()])
        server.serve(DUMP_URL, BODY_P)
        _consume(data_dir)
        assert lanes == [Lane.CKAN, Lane.DATASTORE]


class TestRunnerPublishFailure:
    def test_d1_7_a_failed_publish_counts_failed_and_writes_no_stamp(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """FM-12: detects a stamp or a changed artifact after a capture that never published."""
        from _neso_registry_support import ingest_context

        from gridflow.bronze.writer import BronzeCollisionError
        from gridflow.pipeline import runner as pipeline_runner

        _install(tmp_path, monkeypatch, refresh="frozen")
        server = _Server(router, clock, [_live()])
        server.serve(DUMP_URL, BODY_P)
        end = _end()
        _events, (body,) = _consume(data_dir, end=end)
        _set_written_at(body, end - timedelta(days=8))
        before = {p: p.read_bytes() for p in _bronze_files(data_dir)}

        def _collide(self: BronzeWriter, *args: Any, **kwargs: Any) -> Path:
            raise BronzeCollisionError("bronze capture already exists; refusing to replace it")

        monkeypatch.setattr(BronzeWriter, "publish_capture", _collide)
        server.serve(DUMP_URL, BODY_Q)
        with ingest_context(data_dir, monkeypatch) as ctx:
            (result,) = pipeline_runner.run_ingest(
                ctx, SOURCE, [FAMILY], end - timedelta(hours=1), end
            )
        assert result.status == "failed", result
        assert result.error is not None and "all 1 attempted member(s) failed" in result.error
        assert {p: p.read_bytes() for p in _bronze_files(data_dir)} == before
        assert not _stamp(data_dir).exists()


# ---------------------------------------------------------------------------
# Criterion 3: every dump row has published_at null and a capture-time available_at
# ---------------------------------------------------------------------------


class TestDumpVintage:
    def test_d3_dump_vintage_null_and_non_null_last_modified(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """T-D3-1: detects a dump row dated by a CKAN stamp, or a dump capture not accounted."""
        import polars as pl
        from _neso_generic_support import install_generated
        from _neso_registry_support import record

        from gridflow.silver.neso_data_portal.completion import scan_completions
        from gridflow.silver.neso_data_portal.reconcile import reconcile

        registry, generated = install_generated(
            monkeypatch,
            tmp_path / "registry",
            [
                package(
                    "pkg-dump",
                    PKG,
                    [family(FAMILY, record=record(vintage="capture_fallback"))],
                    [resource(RID, "Dump Member", FAMILY, url_type="datastore")],
                )
            ],
        )
        server = _Server(router, clock, [_live(last_modified=LM)])
        server.serve(DUMP_URL, BODY_P)
        end = _end()
        _events, (with_lm,) = _consume(data_dir, end=end)
        server.live[0] = _live(last_modified=None)
        server.serve(DUMP_URL, BODY_Q)
        _events, (without_lm,) = _consume(data_dir, end=end)
        assert _meta(with_lm)["request_params"]["ckan_last_modified"] == LM
        assert _meta(without_lm)["request_params"]["ckan_last_modified"] == ""

        rows = generated.transformers[FAMILY](data_dir).run(end.date(), run_id="r")
        assert rows == 3
        written = {
            body.relative_to(data_dir).as_posix(): datetime.fromisoformat(
                _meta(body)["written_at"]
            ).astimezone(UTC)
            for body in (with_lm, without_lm)
        }
        silver = sorted((data_dir / "silver" / SOURCE / FAMILY).rglob("[!.]*.parquet"))
        frame = pl.concat([pl.read_parquet(p, hive_partitioning=False) for p in silver])
        assert frame.height == 3
        assert frame["published_at"].null_count() == 3
        for capture_id, available_at in frame.select(
            "bronze_capture_id", "available_at"
        ).iter_rows():
            assert available_at == written[capture_id]

        ledger = scan_completions(data_dir, FAMILY).collect()
        assert sorted(ledger["bronze_capture_id"].to_list()) == sorted(written)
        assert ledger["outcome"].to_list() == ["populated", "populated"]
        assert sorted(ledger["row_count"].to_list()) == [1, 2]
        assert reconcile(data_dir, registry, [FAMILY], end.date()).gaps == ()

    def test_d3_5_a_lagging_registry_cannot_date_a_dump(
        self,
        router: respx.MockRouter,
        clock: FakeClock,
        data_dir: Path,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """T-D3-5: detects a dump capture read under ``issue_time_evidenced`` (P-10 run time)."""
        from _neso_registry_support import epoch, record, sp_columns

        from gridflow.connectors.neso_data_portal.registry import SchemaRecord
        from gridflow.silver.neso_data_portal.completion import (
            CaptureContextError,
            capture_context,
        )

        registry = _install(tmp_path, monkeypatch)
        server = _Server(router, clock, [_live()])
        server.serve(DUMP_URL, BODY_P)
        _consume(data_dir)
        (capture,) = scan_dataset(data_dir / "bronze" / SOURCE / FAMILY, registry).captures
        issue = {"kind": "filename_token", "pattern": r"^(\d{12})_f\.csv$", "format": "%Y%m%d%H%M"}
        lagging = SchemaRecord.model_validate(
            record(
                epochs=[epoch(sp_columns(), issue=issue)],
                entity_key=("settlement_date", "settlement_period", "unit", "issue_time"),
                vintage="issue_time_evidenced",
                vintage_evidence="a lagging registry",
            )
        )
        with pytest.raises(CaptureContextError, match="datastore"):
            capture_context(capture, lagging, data_dir)
        fallback = SchemaRecord.model_validate(record(vintage="capture_fallback"))
        context = capture_context(capture, fallback, data_dir)
        assert context.published_at is None and context.url_type == "datastore"
