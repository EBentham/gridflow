"""The catalogue tool's field-info mode (ADR-035 P-11; unit D criterion 5).

A test snapshot is written under ``tmp_path`` through the materializer's own
builders and checksummed, so ``verify_snapshot`` passes; the registry is a
synthetic one installed through ADR-033 P-1's seam. ``datastore_search`` is
respx-mocked against the real connector on an unbound fake-clock pacer, or a
stub ``FieldInfoSource`` stands in. Nothing is written outside ``tmp_path``.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx
import pytest
import respx
from _neso_registry_support import family, install_registry, package, resource, write_registry

from gridflow.config.settings import DatasetConfig, SourceConfig
from gridflow.connectors.neso_data_portal import catalog_snapshot
from gridflow.connectors.neso_data_portal.catalog_snapshot import (
    CHECKSUMS_FILENAME,
    FIELD_INFO_DIRNAME,
    FIELD_INFO_RUN_FILENAME,
    SNAPSHOTS_DIRNAME,
    advance_manifest,
    main,
)
from gridflow.connectors.neso_data_portal.client import NesoDataPortalConnector, RequestTrace
from gridflow.connectors.neso_data_portal.pacer import RunPacer

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

pytestmark = pytest.mark.usefixtures("stub_neso_resolver")

BASE_URL = "https://api.neso.energy"
PKG = "eeeeeeee-0000-4000-8000-000000000000"
SNAPSHOT_ID = "20261006T195819Z"


def _rid(n: int) -> str:
    return f"eeeeeeee-0000-4000-8000-{n:012d}"


A_OLD, A_NEW, B_ONE, C_UPLOAD = _rid(1), _rid(2), _rid(3), _rid(4)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds
        await asyncio.sleep(0)


@pytest.fixture
def router() -> Iterator[respx.MockRouter]:
    with respx.mock(assert_all_called=False) as mock_router:
        yield mock_router


@pytest.fixture
def out(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A ``_generated`` root holding one verified snapshot that the manifest names."""
    install_registry(
        monkeypatch,
        write_registry(
            tmp_path / "registry",
            [
                package(
                    "pkg-fi",
                    PKG,
                    [family("fam_a"), family("fam_b"), family("fam_c")],
                    [
                        resource(A_OLD, "A Old", "fam_a", url_type="datastore"),
                        resource(A_NEW, "A New", "fam_a", url_type="datastore"),
                        resource(B_ONE, "B One", "fam_b", url_type="datastore"),
                        resource(C_UPLOAD, "C Upload", "fam_c"),
                    ],
                )
            ],
        ),
    )
    root = tmp_path / "out"
    snapshot = root / SNAPSHOTS_DIRNAME / SNAPSHOT_ID
    snapshot.mkdir(parents=True)
    packages = [
        {
            "name": "pkg-fi",
            "id": PKG,
            "resources": [
                _snap(A_OLD, "2026-10-01T10:00:00.000001"),
                _snap(A_NEW, "2026-10-05T10:00:00.000001"),
                _snap(B_ONE, "2026-10-02T10:00:00.000001"),
                _snap(C_UPLOAD, "2026-10-03T10:00:00.000001", active=False),
            ],
        }
    ]
    moment = datetime(2026, 10, 6, 19, 58, 19, tzinfo=UTC)
    trace = RequestTrace(
        action="package_search",
        params={"rows": "50", "start": "0"},
        started_at=moment,
        finished_at=moment,
        status_code=200,
        headers={},
        body_sha256="0" * 64,
    )
    catalog_snapshot._write_json(
        snapshot / catalog_snapshot.CATALOG_FILENAME,
        catalog_snapshot._catalog_document(SNAPSHOT_ID, packages),
    )
    catalog_snapshot._write_json(
        snapshot / catalog_snapshot.PROVENANCE_FILENAME,
        catalog_snapshot._provenance_document(SNAPSHOT_ID, [trace]),
    )
    catalog_snapshot._write_checksums(snapshot)
    advance_manifest(root, SNAPSHOT_ID)
    return root


def _snap(rid: str, modified: str, *, active: bool = True) -> dict[str, Any]:
    return {"id": rid, "datastore_active": active, "metadata_modified": modified, "format": "CSV"}


def _result(rid: str, **overrides: Any) -> dict[str, Any]:
    result: dict[str, Any] = {
        "include_total": True,
        "resource_id": rid,
        "fields": [{"type": "int", "id": "_id"}, {"type": "text", "id": "Value"}],
        "records_format": "objects",
        "records": [],
        "limit": 0,
        "_links": {"start": f"/api/3/action/datastore_search?resource_id={rid}"},
        "total": 42,
    }
    result.update(overrides)
    return result


def _serve(router: respx.MockRouter, overrides: dict[str, dict[str, Any]] | None = None) -> None:
    def _handler(request: httpx.Request) -> httpx.Response:
        rid = request.url.params.get("resource_id", "")
        return httpx.Response(
            200, json={"success": True, "result": _result(rid, **(overrides or {}).get(rid, {}))}
        )

    router.route(url__regex=r".*").mock(side_effect=_handler)


def _session() -> NesoDataPortalConnector:
    clock = FakeClock()
    config = SourceConfig(
        base_url=BASE_URL,
        rate_limit_per_second=1,
        timeout=30,
        datasets={"fam_a": DatasetConfig(endpoint="/api/3/action/package_show")},
    )
    return NesoDataPortalConnector(
        config, pacer=RunPacer(1.0, 30.0, monotonic=clock.monotonic, sleep=clock.sleep)
    )


def _run_dirs(out: Path) -> list[Path]:
    root = out / FIELD_INFO_DIRNAME
    return sorted(p for p in root.iterdir() if p.is_dir()) if root.is_dir() else []


def _keys(value: Any) -> set[str]:
    if isinstance(value, dict):
        return set(value) | {k for item in value.values() for k in _keys(item)}
    if isinstance(value, list):
        return {k for item in value for k in _keys(item)}
    return set()


class TestFieldInfo:
    def test_d5_field_info_writes_one_evidence_file_per_family(
        self, router: respx.MockRouter, out: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-D5-1: detects rows reaching the vault, a wrong resource, or the marker not last."""
        _serve(router)
        order: list[str] = []
        real_write, real_sums = catalog_snapshot._write_json, catalog_snapshot._write_checksums

        def _write(path: Path, document: dict[str, Any]) -> None:
            order.append(path.name)
            real_write(path, document)

        def _sums(directory: Path) -> Path:
            order.append(CHECKSUMS_FILENAME)
            return real_sums(directory)

        monkeypatch.setattr(catalog_snapshot, "_write_json", _write)
        monkeypatch.setattr(catalog_snapshot, "_write_checksums", _sums)
        assert main(["--out", str(out), "--field-info"], field_info_session_factory=_session) == 0

        calls = [call.request.url for call in router.calls]
        assert [url.path for url in calls] == ["/api/3/action/datastore_search"] * 2
        assert [dict(url.params) for url in calls] == [
            {"resource_id": A_NEW, "limit": "0"},
            {"resource_id": B_ONE, "limit": "0"},
        ]
        assert order == ["fam_a.json", "fam_b.json", FIELD_INFO_RUN_FILENAME, CHECKSUMS_FILENAME]
        (run_dir,) = _run_dirs(out)
        documents = {
            path.name: json.loads(path.read_bytes()) for path in sorted(run_dir.glob("*.json"))
        }
        assert set(documents) == {"fam_a.json", "fam_b.json", FIELD_INFO_RUN_FILENAME}
        for document in documents.values():
            assert not ({"records", "_links"} & _keys(document)), document
        fam_a = documents["fam_a.json"]
        assert (fam_a["resource_id"], fam_a["snapshot_id"], fam_a["records_returned"]) == (
            A_NEW,
            SNAPSHOT_ID,
            0,
        )
        assert fam_a["fields"][1] == {"type": "text", "id": "Value"} and fam_a["total"] == 42
        assert fam_a["request"]["params"] == {"limit": "0", "resource_id": A_NEW}
        run = documents[FIELD_INFO_RUN_FILENAME]
        assert run["snapshot_id"] == SNAPSHOT_ID
        assert [(f["family"], f["outcome"]) for f in run["families"]] == [
            ("fam_a", "ok"),
            ("fam_b", "ok"),
        ]

    @pytest.mark.parametrize(
        "override", [{"records": [{"_id": 1, "Value": "x"}]}, {"limit": 3}], ids=["rows", "limit"]
    )
    def test_d5_2_rows_are_refused_per_family(
        self, router: respx.MockRouter, out: Path, override: dict[str, Any]
    ) -> None:
        """FM-17: detects a response carrying rows (or able to) being written."""
        _serve(router, {A_NEW: override})
        assert main(["--out", str(out), "--field-info"], field_info_session_factory=_session) == 1
        (run_dir,) = _run_dirs(out)
        assert not (run_dir / "fam_a.json").exists()
        assert (run_dir / "fam_b.json").exists() and (run_dir / CHECKSUMS_FILENAME).exists()
        run = json.loads((run_dir / FIELD_INFO_RUN_FILENAME).read_bytes())
        outcomes = {f["family"]: (f["outcome"], f["detail"]) for f in run["families"]}
        assert outcomes["fam_a"][0] == "failed" and "RowSampleRejectedError" in outcomes["fam_a"][1]
        assert outcomes["fam_b"] == ("ok", "")

    def test_d5_3_an_unverifiable_snapshot_sends_nothing(
        self, router: respx.MockRouter, out: Path
    ) -> None:
        """Detects field info read from a snapshot that no longer verifies."""
        _serve(router)
        catalog = out / SNAPSHOTS_DIRNAME / SNAPSHOT_ID / catalog_snapshot.CATALOG_FILENAME
        catalog.write_bytes(catalog.read_bytes() + b" ")
        assert main(["--out", str(out), "--field-info"], field_info_session_factory=_session) == 1
        assert router.calls.call_count == 0 and _run_dirs(out) == []

    def test_d5_3_an_explicit_snapshot_is_verified_too(
        self, router: respx.MockRouter, out: Path, tmp_path: Path
    ) -> None:
        """Detects ``--snapshot`` bypassing verification."""
        _serve(router)
        empty = tmp_path / "elsewhere" / SNAPSHOT_ID
        empty.mkdir(parents=True)
        argv = ["--out", str(out), "--field-info", "--snapshot", str(empty)]
        assert main(argv, field_info_session_factory=_session) == 1
        assert router.calls.call_count == 0

    def test_d5_4_an_unexpected_error_leaves_an_incomplete_run(
        self, out: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """FM-16: detects an interrupted run that looks complete, or goes unreported."""

        class _Stub:
            def __init__(self) -> None:
                self.calls = 0

            async def __aenter__(self) -> _Stub:
                return self

            async def __aexit__(self, *exc: object) -> None:
                return None

            async def datastore_fields(self, resource_id: str) -> tuple[Any, RequestTrace]:
                self.calls += 1
                if self.calls == 2:
                    raise RuntimeError("process killed")
                moment = datetime.now(UTC)
                trace = RequestTrace(
                    action="datastore_search",
                    params={"limit": "0", "resource_id": resource_id},
                    started_at=moment,
                    finished_at=moment,
                    status_code=200,
                    headers={},
                    body_sha256="1" * 64,
                )
                return _result(resource_id), trace

        with pytest.raises(RuntimeError, match="process killed"):
            main(["--out", str(out), "--field-info"], field_info_session_factory=_Stub)
        (run_dir,) = _run_dirs(out)
        assert (run_dir / "fam_a.json").exists()
        assert not (run_dir / CHECKSUMS_FILENAME).exists()
        later = datetime(2099, 1, 1, tzinfo=UTC)  # a distinct run id for the second run
        monkeypatch.setattr(catalog_snapshot, "_utcnow", lambda: later)
        with caplog.at_level(logging.WARNING), pytest.raises(RuntimeError):
            main(["--out", str(out), "--field-info"], field_info_session_factory=_Stub)
        assert any(run_dir.name in record.getMessage() for record in caplog.records)

    def test_d5_5_dry_run_and_family_narrowing(
        self, router: respx.MockRouter, out: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Detects a dry run that sends or writes, or ``--family`` not narrowing the population."""
        _serve(router)
        argv = ["--out", str(out), "--field-info", "--dry-run", "--family", "fam_b"]
        with caplog.at_level(logging.INFO):
            assert main(argv, field_info_session_factory=_session) == 0
        assert router.calls.call_count == 0 and not (out / FIELD_INFO_DIRNAME).exists()
        planned = [r.getMessage() for r in caplog.records if "would request" in r.getMessage()]
        assert planned == [f"dry run: would request field info for fam_b ({B_ONE})"]

    def test_d5_5_family_narrows_a_real_run(self, router: respx.MockRouter, out: Path) -> None:
        _serve(router)
        argv = ["--out", str(out), "--field-info", "--family", "fam_b"]
        assert main(argv, field_info_session_factory=_session) == 0
        assert [dict(c.request.url.params)["resource_id"] for c in router.calls] == [B_ONE]

    def test_options_need_field_info(self, out: Path) -> None:
        with pytest.raises(SystemExit):
            main(["--out", str(out), "--family", "fam_b"])
