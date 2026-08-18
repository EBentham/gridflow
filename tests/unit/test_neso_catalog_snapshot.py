"""Vault snapshot materializer tests — crash states and provenance (T-13, T-14).

Every test is offline and runs against ``tmp_path``: the connector is a stub
that returns a :class:`CatalogDiscovery`, so no HTTP, no DNS and no vault path
is involved. Crashes are simulated by calling the materializer's internal steps
directly rather than by killing a process — an interrupted run is exactly "some
of the steps ran", and that is reproducible without a signal.

The failure modes pinned here are FM-06 through FM-10 and FM-14; each has a
named test, and the provenance assertions are field by field, because a test
that only checks ``provenance.json`` exists would pass against a file full of
placeholders — which is the defect FM-14 names.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import pytest

from gridflow.connectors.neso_data_portal import catalog_snapshot
from gridflow.connectors.neso_data_portal.catalog_snapshot import (
    CATALOG_FILENAME,
    CHECKSUMS_FILENAME,
    MANIFEST_FILENAME,
    PROVENANCE_FILENAME,
    SNAPSHOTS_DIRNAME,
    IncompleteProvenanceError,
    ManifestAdvanceError,
    RowSampleRejectedError,
    SnapshotVerificationError,
    advance_manifest,
    build_snapshot,
    main,
    verify_snapshot,
)
from gridflow.connectors.neso_data_portal.client import CatalogDiscovery, RequestTrace

if TYPE_CHECKING:
    from pathlib import Path

# ---------------------------------------------------------------------------
# Fixtures and stubs
# ---------------------------------------------------------------------------

MOMENT_ONE = datetime(2026, 8, 16, 18, 52, 0, tzinfo=UTC)
MOMENT_TWO = datetime(2026, 8, 16, 19, 3, 11, tzinfo=UTC)
SNAPSHOT_ONE = "20260816T185200Z"
SNAPSHOT_TWO = "20260816T190311Z"

VENDOR_HEADERS = {
    "date": "Sun, 16 Aug 2026 18:54:01 GMT",
    "content-type": "application/json;charset=utf-8",
    "etag": '"2f733b738a4970f150601ca2b7da5df5"',
    "last-modified": "Sun, 16 Aug 2026 18:21:38 GMT",
}

PACKAGES: tuple[dict[str, Any], ...] = (
    {"name": "daily-wind-availability", "num_resources": 2, "resources": [{"name": "Daily Wind"}]},
    {"name": "historic-generation-mix", "num_resources": 3, "resources": [{"name": "Historic"}]},
)

TRACE_FIELDS = (
    "action",
    "params",
    "started_at",
    "finished_at",
    "status_code",
    "headers",
    "body_sha256",
)


def _trace(action: str, params: dict[str, str], offset_seconds: int) -> RequestTrace:
    """Build one fully-populated request trace."""
    started = MOMENT_ONE + timedelta(seconds=offset_seconds)
    return RequestTrace(
        action=action,
        params=params,
        started_at=started,
        finished_at=started + timedelta(milliseconds=420),
        status_code=200,
        headers=dict(VENDOR_HEADERS),
        body_sha256=hashlib.sha256(action.encode()).hexdigest(),
    )


TRACES: tuple[RequestTrace, ...] = (
    _trace("package_search", {"rows": "50", "start": "0"}, 0),
    _trace("package_list", {}, 2),
)


class _StubConnector:
    """A :class:`CatalogDiscoverer` that returns a canned discovery result."""

    def __init__(self, discovery: CatalogDiscovery) -> None:
        self.discovery = discovery
        self.calls = 0

    async def discover_catalog(self) -> CatalogDiscovery:
        self.calls += 1
        return self.discovery


class _StubSession:
    """An async context manager yielding a :class:`_StubConnector`."""

    def __init__(self, connector: _StubConnector) -> None:
        self.connector = connector

    async def __aenter__(self) -> _StubConnector:
        return self.connector

    async def __aexit__(self, *exc: object) -> None:
        return None


def _discovery(
    packages: tuple[dict[str, Any], ...] = PACKAGES,
    traces: tuple[Any, ...] = TRACES,
) -> CatalogDiscovery:
    """Build a discovery result, defaulting to the fully-populated one."""
    return CatalogDiscovery(packages=packages, traces=traces)


def _at(monkeypatch: pytest.MonkeyPatch, moment: datetime) -> None:
    """Pin the materializer's clock, so snapshot ids are deterministic."""
    monkeypatch.setattr(catalog_snapshot, "_utcnow", lambda: moment)


def _build(discovery: CatalogDiscovery, out_root: Path, *, dry_run: bool = False) -> Path:
    """Drive ``build_snapshot`` through a stub connector."""
    connector = _StubConnector(discovery)
    return asyncio.run(build_snapshot(connector, out_root, dry_run=dry_run))


def _complete_and_advance(
    monkeypatch: pytest.MonkeyPatch, out_root: Path, moment: datetime
) -> Path:
    """Write one complete snapshot and advance the manifest to it."""
    _at(monkeypatch, moment)
    snapshot_dir = _build(_discovery(), out_root)
    advance_manifest(out_root, snapshot_dir.name)
    return snapshot_dir


def _manifest_bytes(out_root: Path) -> bytes:
    return (out_root / MANIFEST_FILENAME).read_bytes()


def _tree(root: Path) -> list[str]:
    return sorted(str(path.relative_to(root)) for path in root.rglob("*"))


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestCompleteSnapshot:
    """A finished run leaves three files, a verifiable directory and a manifest."""

    def test_a_complete_snapshot_carries_the_three_contract_files(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _at(monkeypatch, MOMENT_ONE)

        snapshot_dir = _build(_discovery(), tmp_path)

        assert snapshot_dir == tmp_path / SNAPSHOTS_DIRNAME / SNAPSHOT_ONE
        assert sorted(entry.name for entry in snapshot_dir.iterdir()) == [
            CATALOG_FILENAME,
            PROVENANCE_FILENAME,
            CHECKSUMS_FILENAME,
        ]
        verify_snapshot(snapshot_dir)

    def test_the_snapshot_id_is_utc_and_lexicographically_sortable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Sorting snapshot ids as strings must be chronological order."""
        _at(monkeypatch, MOMENT_ONE)
        first = _build(_discovery(), tmp_path)
        _at(monkeypatch, MOMENT_TWO)
        second = _build(_discovery(), tmp_path)

        assert [first.name, second.name] == [SNAPSHOT_ONE, SNAPSHOT_TWO]
        assert sorted([second.name, first.name]) == [first.name, second.name]

    def test_the_catalogue_document_carries_every_discovered_package(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _at(monkeypatch, MOMENT_ONE)

        snapshot_dir = _build(_discovery(), tmp_path)

        document = json.loads((snapshot_dir / CATALOG_FILENAME).read_bytes())
        assert document["snapshot_id"] == SNAPSHOT_ONE
        assert document["source"] == "neso_data_portal"
        assert document["package_count"] == len(PACKAGES)
        assert [package["name"] for package in document["packages"]] == [
            package["name"] for package in PACKAGES
        ]

    def test_the_manifest_points_at_the_snapshot_it_verified(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        snapshot_dir = _complete_and_advance(monkeypatch, tmp_path, MOMENT_ONE)

        manifest = json.loads(_manifest_bytes(tmp_path))
        assert manifest["snapshot_id"] == SNAPSHOT_ONE
        assert (
            manifest["sha256sums_sha256"]
            == hashlib.sha256((snapshot_dir / CHECKSUMS_FILENAME).read_bytes()).hexdigest()
        )

    def test_a_dry_run_writes_nothing_at_all(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """T-12's acceptance, made durable: --dry-run discovers and validates,
        then writes no file — not even outside ``--out``, because it writes none."""
        _at(monkeypatch, MOMENT_ONE)
        connector = _StubConnector(_discovery())

        exit_code = main(
            ["--out", str(tmp_path), "--dry-run"], session_factory=lambda: _StubSession(connector)
        )

        assert exit_code == 0
        assert connector.calls == 1, "the dry run must still perform discovery"
        assert _tree(tmp_path) == []


# ---------------------------------------------------------------------------
# FM-14 — provenance completeness
# ---------------------------------------------------------------------------


class TestProvenanceCompleteness:
    """FM-14: the trace is provenance's only source, so an absence must raise.

    A snapshot that merely *has* a ``provenance.json`` is the defect; the file
    is hash-verified evidence, and a placeholder inside it is a lie that
    verifies.
    """

    def test_provenance_records_every_required_field_for_every_request(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _at(monkeypatch, MOMENT_ONE)

        snapshot_dir = _build(_discovery(), tmp_path)

        document = json.loads((snapshot_dir / PROVENANCE_FILENAME).read_bytes())
        assert document["snapshot_id"] == SNAPSHOT_ONE
        assert document["source"] == "neso_data_portal"
        assert len(document["requests"]) == len(TRACES)

        for entry, trace in zip(document["requests"], TRACES, strict=True):
            assert entry["action"] == trace.action
            assert entry["params"] == trace.params
            assert entry["started_at"].endswith("Z"), "timings are not rendered as UTC"
            assert entry["started_at"] == trace.started_at.isoformat().replace("+00:00", "Z")
            assert entry["finished_at"] == trace.finished_at.isoformat().replace("+00:00", "Z")
            assert entry["status_code"] == trace.status_code
            assert entry["headers"] == {
                "date": VENDOR_HEADERS["date"],
                "content-type": VENDOR_HEADERS["content-type"],
                "etag": VENDOR_HEADERS["etag"],
                "last-modified": VENDOR_HEADERS["last-modified"],
            }
            assert entry["body_sha256"] == trace.body_sha256
            assert set(entry) == {
                "action",
                "params",
                "started_at",
                "finished_at",
                "status_code",
                "headers",
                "body_sha256",
            }

    def test_the_package_list_call_with_no_params_records_an_empty_mapping(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An empty ``params`` is a real value, not a missing field: ``package_list``
        legitimately takes none, so the guard must not conflate the two."""
        _at(monkeypatch, MOMENT_ONE)

        snapshot_dir = _build(_discovery(), tmp_path)

        document = json.loads((snapshot_dir / PROVENANCE_FILENAME).read_bytes())
        assert document["requests"][1]["action"] == "package_list"
        assert document["requests"][1]["params"] == {}

    @pytest.mark.parametrize("field", TRACE_FIELDS)
    def test_a_trace_missing_a_field_makes_the_build_raise(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, field: str
    ) -> None:
        _at(monkeypatch, MOMENT_ONE)
        crippled = SimpleNamespace(
            **{name: getattr(TRACES[0], name) for name in TRACE_FIELDS if name != field}
        )

        with pytest.raises(IncompleteProvenanceError, match=field):
            _build(_discovery(traces=(crippled,)), tmp_path)

        assert _tree(tmp_path) == [], "a snapshot was written despite incomplete provenance"

    def test_a_naive_trace_timestamp_makes_the_build_raise(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _at(monkeypatch, MOMENT_ONE)
        naive = SimpleNamespace(
            **{name: getattr(TRACES[0], name) for name in TRACE_FIELDS},
        )
        naive.started_at = MOMENT_ONE.replace(tzinfo=None)

        with pytest.raises(IncompleteProvenanceError, match="naive"):
            _build(_discovery(traces=(naive,)), tmp_path)

    def test_a_placeholder_body_hash_makes_the_build_raise(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The field being *present* is not the contract — it must be a real digest."""
        _at(monkeypatch, MOMENT_ONE)
        placeholder = SimpleNamespace(
            **{name: getattr(TRACES[0], name) for name in TRACE_FIELDS},
        )
        placeholder.body_sha256 = ""

        with pytest.raises(IncompleteProvenanceError, match="body_sha256"):
            _build(_discovery(traces=(placeholder,)), tmp_path)

    def test_a_discovery_with_no_traces_at_all_makes_the_build_raise(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _at(monkeypatch, MOMENT_ONE)

        with pytest.raises(IncompleteProvenanceError, match="no request traces"):
            _build(_discovery(traces=()), tmp_path)


# ---------------------------------------------------------------------------
# FM-06 — an incomplete directory is never advanced to
# ---------------------------------------------------------------------------


class TestIncompleteSnapshot:
    """FM-06: no ``sha256sums.txt`` means incomplete *by definition*."""

    def test_a_directory_without_checksums_is_not_advanced_to(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The crash is simulated by running the write steps that precede the
        checksum write, which is exactly what an interrupted run leaves behind."""
        _complete_and_advance(monkeypatch, tmp_path, MOMENT_ONE)
        before = _manifest_bytes(tmp_path)

        partial = tmp_path / SNAPSHOTS_DIRNAME / SNAPSHOT_TWO
        partial.mkdir(parents=True)
        catalog_snapshot._write_json(partial / CATALOG_FILENAME, {"snapshot_id": SNAPSHOT_TWO})
        catalog_snapshot._write_json(partial / PROVENANCE_FILENAME, {"requests": []})

        with pytest.raises(SnapshotVerificationError, match=CHECKSUMS_FILENAME):
            advance_manifest(tmp_path, SNAPSHOT_TWO)

        assert _manifest_bytes(tmp_path) == before
        assert json.loads(before)["snapshot_id"] == SNAPSHOT_ONE

    def test_an_incomplete_directory_is_left_in_place_and_named_in_the_log(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """ADR-029 spirit: never deleted, never rewritten, but never silent."""
        partial = tmp_path / SNAPSHOTS_DIRNAME / SNAPSHOT_ONE
        partial.mkdir(parents=True)
        catalog_snapshot._write_json(partial / CATALOG_FILENAME, {"snapshot_id": SNAPSHOT_ONE})

        _at(monkeypatch, MOMENT_TWO)
        with caplog.at_level("WARNING", logger=catalog_snapshot.logger.name):
            _build(_discovery(), tmp_path)

        assert SNAPSHOT_ONE in caplog.text
        assert (partial / CATALOG_FILENAME).exists(), "an incomplete snapshot was deleted"

    def test_verify_refuses_a_directory_that_has_no_checksums(self, tmp_path: Path) -> None:
        empty = tmp_path / SNAPSHOTS_DIRNAME / SNAPSHOT_ONE
        empty.mkdir(parents=True)

        with pytest.raises(SnapshotVerificationError, match="incomplete by definition"):
            verify_snapshot(empty)


# ---------------------------------------------------------------------------
# FM-07 — complete but unadvanced
# ---------------------------------------------------------------------------


class TestCompleteButUnadvanced:
    """FM-07: a crash before the advance leaves the previous snapshot active."""

    def test_a_complete_but_unadvanced_snapshot_leaves_the_previous_manifest(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _complete_and_advance(monkeypatch, tmp_path, MOMENT_ONE)
        before = _manifest_bytes(tmp_path)

        _at(monkeypatch, MOMENT_TWO)
        second = _build(_discovery(), tmp_path)
        verify_snapshot(second)

        assert _manifest_bytes(tmp_path) == before
        assert json.loads(before)["snapshot_id"] == SNAPSHOT_ONE

    def test_a_re_run_advances_the_unadvanced_snapshot_cleanly(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _complete_and_advance(monkeypatch, tmp_path, MOMENT_ONE)
        _at(monkeypatch, MOMENT_TWO)
        second = _build(_discovery(), tmp_path)

        advance_manifest(tmp_path, second.name)

        assert json.loads(_manifest_bytes(tmp_path))["snapshot_id"] == SNAPSHOT_TWO
        assert (tmp_path / SNAPSHOTS_DIRNAME / SNAPSHOT_ONE).is_dir(), (
            "the superseded snapshot was removed; snapshots are immutable, not transient"
        )


# ---------------------------------------------------------------------------
# FM-09 — a mutated snapshot is refused
# ---------------------------------------------------------------------------


class TestMutationIsRefused:
    """FM-09: the manifest never points at content that failed verification."""

    def test_a_file_mutated_after_the_checksums_makes_the_advance_raise(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _complete_and_advance(monkeypatch, tmp_path, MOMENT_ONE)
        before = _manifest_bytes(tmp_path)

        _at(monkeypatch, MOMENT_TWO)
        second = _build(_discovery(), tmp_path)
        (second / CATALOG_FILENAME).write_bytes(b'{"snapshot_id": "tampered"}\n')

        with pytest.raises(SnapshotVerificationError, match=CATALOG_FILENAME):
            advance_manifest(tmp_path, second.name)

        assert _manifest_bytes(tmp_path) == before
        assert json.loads(before)["snapshot_id"] == SNAPSHOT_ONE

    def test_a_file_deleted_after_the_checksums_makes_the_advance_raise(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _at(monkeypatch, MOMENT_ONE)
        snapshot_dir = _build(_discovery(), tmp_path)
        (snapshot_dir / PROVENANCE_FILENAME).unlink()

        with pytest.raises(SnapshotVerificationError, match=PROVENANCE_FILENAME):
            advance_manifest(tmp_path, snapshot_dir.name)

        assert not (tmp_path / MANIFEST_FILENAME).exists()

    def test_a_file_injected_after_the_checksums_makes_the_advance_raise(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An added file is a mutation of an immutable directory just as much as
        an edited one, and an unlisted file is exactly how row samples would
        arrive unnoticed."""
        _at(monkeypatch, MOMENT_ONE)
        snapshot_dir = _build(_discovery(), tmp_path)
        (snapshot_dir / "rows.csv").write_bytes(b"DATETIME,GAS\n2026-08-16,1\n")

        with pytest.raises(SnapshotVerificationError, match="rows.csv"):
            advance_manifest(tmp_path, snapshot_dir.name)

        assert not (tmp_path / MANIFEST_FILENAME).exists()


# ---------------------------------------------------------------------------
# Immutability
# ---------------------------------------------------------------------------


class TestImmutability:
    """A completed snapshot is never rewritten — by construction, not by care."""

    def test_a_second_build_never_rewrites_the_first_snapshots_files(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _at(monkeypatch, MOMENT_ONE)
        first = _build(_discovery(), tmp_path)
        before = {
            path.name: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
            for path in sorted(first.iterdir())
        }

        _at(monkeypatch, MOMENT_TWO)
        second = _build(_discovery(), tmp_path)

        assert second != first
        after = {
            path.name: (path.stat().st_mtime_ns, hashlib.sha256(path.read_bytes()).hexdigest())
            for path in sorted(first.iterdir())
        }
        assert after == before
        verify_snapshot(first)

    def test_a_colliding_snapshot_id_refuses_rather_than_reopening_the_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Two runs inside the same second must not merge into one directory."""
        _at(monkeypatch, MOMENT_ONE)
        first = _build(_discovery(), tmp_path)
        before = {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(first.iterdir())
        }

        with pytest.raises(FileExistsError):
            _build(_discovery(), tmp_path)

        after = {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(first.iterdir())
        }
        assert after == before


# ---------------------------------------------------------------------------
# Metadata only
# ---------------------------------------------------------------------------


class TestRowSampleGuard:
    """PHASE.md ruling 4: metadata evidence may be committed, rows may not."""

    def test_a_payload_carrying_datastore_records_is_rejected(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _at(monkeypatch, MOMENT_ONE)
        with_rows = (
            {
                "name": "historic-generation-mix",
                "resources": [
                    {
                        "name": "Historic GB Generation Mix",
                        "records": [{"DATETIME": "2026-08-16T00:00:00", "GAS": 1234}],
                    }
                ],
            },
        )

        with pytest.raises(RowSampleRejectedError, match="records"):
            _build(_discovery(packages=with_rows), tmp_path)

        assert _tree(tmp_path) == [], "a row-bearing payload reached the filesystem"

    def test_ordinary_metadata_is_not_mistaken_for_rows(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The guard is one key, deliberately: a padded guess list would refuse
        legitimate metadata such as ``num_resources`` or ``datastore_active``."""
        _at(monkeypatch, MOMENT_ONE)
        metadata = (
            {
                "name": "daily-wind-availability",
                "num_resources": 2,
                "datastore_active_count": 2,
                "resources": [{"name": "Daily Wind Availability", "datastore_active": True}],
            },
        )

        snapshot_dir = _build(_discovery(packages=metadata), tmp_path)

        assert (snapshot_dir / CATALOG_FILENAME).is_file()


# ---------------------------------------------------------------------------
# T-14 — FM-08 and FM-10: the atomic manifest advance
# ---------------------------------------------------------------------------


class TestAtomicManifestAdvance:
    """FM-08 and FM-10: the manifest is wholly old or wholly new, and a failed
    advance is loud."""

    def test_the_advance_goes_through_a_temp_file_and_os_replace(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """FM-08: the destination is never opened for direct write, so an
        interrupted advance cannot truncate the manifest."""
        _at(monkeypatch, MOMENT_ONE)
        snapshot_dir = _build(_discovery(), tmp_path)
        recorded: list[tuple[str, str]] = []

        def _record(src: Any, dst: Any) -> None:
            recorded.append((str(src), str(dst)))

        monkeypatch.setattr(os, "replace", _record)
        advance_manifest(tmp_path, snapshot_dir.name)

        assert len(recorded) == 1, "the advance did not go through exactly one os.replace"
        source, destination = recorded[0]
        assert destination == str(tmp_path / MANIFEST_FILENAME)
        assert source != destination, "the manifest was written in place, not via a temp file"
        assert source.endswith(".tmp")
        assert not (tmp_path / MANIFEST_FILENAME).exists(), (
            "the destination was opened for direct write"
        )
        staged = json.loads((tmp_path / source.rsplit(os.sep, 1)[-1]).read_bytes())
        assert staged["snapshot_id"] == snapshot_dir.name

    def test_a_transient_replace_failure_is_retried_and_then_succeeds(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """FM-10: OneDrive takes transient handles on the vault path."""
        _at(monkeypatch, MOMENT_ONE)
        snapshot_dir = _build(_discovery(), tmp_path)

        real_replace = os.replace
        attempts: list[int] = []

        def _flaky(src: Any, dst: Any) -> None:
            attempts.append(1)
            if len(attempts) <= 2:
                raise PermissionError(32, "The process cannot access the file")
            real_replace(src, dst)

        monkeypatch.setattr(os, "replace", _flaky)
        monkeypatch.setattr(catalog_snapshot.time, "sleep", lambda _seconds: None)

        advance_manifest(tmp_path, snapshot_dir.name)

        assert len(attempts) == 3
        assert json.loads(_manifest_bytes(tmp_path))["snapshot_id"] == snapshot_dir.name

    def test_a_permanently_failing_replace_fails_loudly_and_leaves_the_manifest(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """FM-10's other half: never a silent skip reported as success."""
        _complete_and_advance(monkeypatch, tmp_path, MOMENT_ONE)
        before = _manifest_bytes(tmp_path)

        _at(monkeypatch, MOMENT_TWO)
        second = _build(_discovery(), tmp_path)
        attempts: list[int] = []

        def _always_locked(src: Any, dst: Any) -> None:
            attempts.append(1)
            raise PermissionError(32, "The process cannot access the file")

        monkeypatch.setattr(os, "replace", _always_locked)
        monkeypatch.setattr(catalog_snapshot.time, "sleep", lambda _seconds: None)

        with pytest.raises(ManifestAdvanceError, match="previous snapshot"):
            advance_manifest(tmp_path, second.name)

        assert len(attempts) == catalog_snapshot.REPLACE_ATTEMPTS
        assert _manifest_bytes(tmp_path) == before
        assert json.loads(before)["snapshot_id"] == SNAPSHOT_ONE
        assert not list(tmp_path.glob(f"{MANIFEST_FILENAME}.*.tmp")), (
            "a failed advance left its temp file behind"
        )

    def test_the_command_reports_a_failed_replace_with_a_non_zero_exit(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A stale manifest reported as success is the silent-data-bug class."""
        _at(monkeypatch, MOMENT_ONE)
        monkeypatch.setattr(catalog_snapshot.time, "sleep", lambda _seconds: None)
        monkeypatch.setattr(
            os,
            "replace",
            lambda src, dst: (_ for _ in ()).throw(PermissionError(32, "locked")),
        )
        connector = _StubConnector(_discovery())

        exit_code = main(["--out", str(tmp_path)], session_factory=lambda: _StubSession(connector))

        assert exit_code == 1
        assert not (tmp_path / MANIFEST_FILENAME).exists()


# ---------------------------------------------------------------------------
# Sol pass 1 — the contract members, the staged marker, and a real interruption
# ---------------------------------------------------------------------------


class TestASelfConsistentSubsetIsNotASnapshot:
    """Sol pass 1, major 1: internal consistency is not completeness.

    ``sha256sums.txt`` proves a directory agrees with itself. An empty marker
    over an empty directory agrees with itself perfectly, and so does any
    subset of a real snapshot — so verification must require the contract
    members by name, or the manifest can be advanced to a directory carrying
    neither catalogue nor provenance.
    """

    def test_an_empty_marker_over_an_empty_directory_fails_verification(
        self, tmp_path: Path
    ) -> None:
        hollow = tmp_path / SNAPSHOTS_DIRNAME / SNAPSHOT_ONE
        hollow.mkdir(parents=True)
        (hollow / CHECKSUMS_FILENAME).write_bytes(b"")

        with pytest.raises(SnapshotVerificationError, match=CATALOG_FILENAME):
            verify_snapshot(hollow)

    def test_an_empty_marker_over_an_empty_directory_is_not_advanceable(
        self, tmp_path: Path
    ) -> None:
        hollow = tmp_path / SNAPSHOTS_DIRNAME / SNAPSHOT_ONE
        hollow.mkdir(parents=True)
        (hollow / CHECKSUMS_FILENAME).write_bytes(b"")

        with pytest.raises(SnapshotVerificationError, match=CATALOG_FILENAME):
            advance_manifest(tmp_path, SNAPSHOT_ONE)

        assert not (tmp_path / MANIFEST_FILENAME).exists()

    def test_a_self_consistent_subset_without_provenance_is_not_advanceable(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Every listed file present and hashing correctly — and still refused,
        because a snapshot without its provenance is evidence of nothing."""
        _complete_and_advance(monkeypatch, tmp_path, MOMENT_ONE)
        before = _manifest_bytes(tmp_path)

        subset = tmp_path / SNAPSHOTS_DIRNAME / SNAPSHOT_TWO
        subset.mkdir(parents=True)
        catalog_snapshot._write_json(subset / CATALOG_FILENAME, {"snapshot_id": SNAPSHOT_TWO})
        digest = hashlib.sha256((subset / CATALOG_FILENAME).read_bytes()).hexdigest()
        (subset / CHECKSUMS_FILENAME).write_bytes(f"{digest}  {CATALOG_FILENAME}\n".encode())

        with pytest.raises(SnapshotVerificationError, match=PROVENANCE_FILENAME):
            advance_manifest(tmp_path, SNAPSHOT_TWO)

        assert _manifest_bytes(tmp_path) == before
        assert json.loads(before)["snapshot_id"] == SNAPSHOT_ONE


class TestTheCompletenessMarkerIsInstalledAtomically:
    """Sol pass 1, minor 2: everything downstream tests the marker by existence,
    so a torn-but-present marker would make an incomplete snapshot look
    complete. Staging it makes that impossible rather than unlikely."""

    def test_the_marker_is_staged_and_installed_with_os_replace(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _at(monkeypatch, MOMENT_ONE)
        monkeypatch.setattr(catalog_snapshot.time, "sleep", lambda _seconds: None)
        recorded: list[tuple[str, str]] = []

        def _record_and_fail(src: Any, dst: Any) -> None:
            recorded.append((str(src), str(dst)))
            raise PermissionError(32, "The process cannot access the file")

        monkeypatch.setattr(os, "replace", _record_and_fail)

        with pytest.raises(catalog_snapshot.SnapshotWriteError, match="completeness marker"):
            _build(_discovery(), tmp_path)

        monkeypatch.undo()
        partial = tmp_path / SNAPSHOTS_DIRNAME / SNAPSHOT_ONE
        assert [dst for _, dst in recorded] == [str(partial / CHECKSUMS_FILENAME)] * (
            catalog_snapshot.REPLACE_ATTEMPTS
        ), "the marker was not installed through os.replace onto its own path"
        assert all(src.endswith(catalog_snapshot.TEMP_SUFFIX) for src, _ in recorded), (
            "the marker was written in place rather than staged"
        )
        assert not (partial / CHECKSUMS_FILENAME).exists(), (
            "a marker appeared even though every os.replace failed, so it was written directly"
        )
        assert not list(partial.glob(f"*{catalog_snapshot.TEMP_SUFFIX}")), (
            "a failed marker install left its staged file behind"
        )


class TestARealInterruptedBuild:
    """Sol pass 1, minor 3: FM-06 pinned through the REAL ``build_snapshot``.

    The constructed-directory test above pins what verification does with an
    incomplete directory; this one pins that ``build_snapshot`` actually
    *produces* that shape when it is interrupted — the write order itself,
    rather than a hand-made stand-in for it.
    """

    def test_a_crash_before_the_marker_leaves_data_files_and_no_marker(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        def _interrupted(_path: Path) -> str:
            raise OSError("hashing interrupted")

        _at(monkeypatch, MOMENT_ONE)
        monkeypatch.setattr(catalog_snapshot, "_sha256_file", _interrupted)

        with pytest.raises(OSError, match="hashing interrupted"):
            _build(_discovery(), tmp_path)

        monkeypatch.undo()
        partial = tmp_path / SNAPSHOTS_DIRNAME / SNAPSHOT_ONE
        assert sorted(entry.name for entry in partial.iterdir()) == [
            CATALOG_FILENAME,
            PROVENANCE_FILENAME,
        ]
        assert not (partial / CHECKSUMS_FILENAME).exists()

        with pytest.raises(SnapshotVerificationError, match="incomplete by definition"):
            verify_snapshot(partial)
        with pytest.raises(SnapshotVerificationError, match="incomplete by definition"):
            advance_manifest(tmp_path, SNAPSHOT_ONE)
        assert not (tmp_path / MANIFEST_FILENAME).exists()

        _at(monkeypatch, MOMENT_TWO)
        with caplog.at_level("WARNING", logger=catalog_snapshot.logger.name):
            _build(_discovery(), tmp_path)

        assert SNAPSHOT_ONE in caplog.text, "the interrupted snapshot was not named in the log"
        assert (partial / CATALOG_FILENAME).exists(), "an interrupted snapshot was deleted"

    def test_a_crash_before_the_marker_leaves_the_previous_manifest_active(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def _interrupted(_path: Path) -> str:
            raise OSError("hashing interrupted")

        _complete_and_advance(monkeypatch, tmp_path, MOMENT_ONE)
        before = _manifest_bytes(tmp_path)

        _at(monkeypatch, MOMENT_TWO)
        monkeypatch.setattr(catalog_snapshot, "_sha256_file", _interrupted)
        with pytest.raises(OSError, match="hashing interrupted"):
            _build(_discovery(), tmp_path)

        monkeypatch.undo()
        assert _manifest_bytes(tmp_path) == before
        assert json.loads(before)["snapshot_id"] == SNAPSHOT_ONE


# ---------------------------------------------------------------------------
# Sol pass 2 — present and correctly hashed is not valid
# ---------------------------------------------------------------------------


def _restate(snapshot_dir: Path, name: str, body: bytes) -> None:
    """Replace a member's bytes and re-record every digest.

    This is the attack the pass-1 fix did not cover: after this the directory
    is complete, carries both contract members, and agrees with its own marker
    exactly. Only the *contents* are wrong.
    """
    (snapshot_dir / name).write_bytes(body)
    lines = [
        f"{hashlib.sha256(entry.read_bytes()).hexdigest()}  {entry.name}\n"
        for entry in sorted(
            path for path in snapshot_dir.iterdir() if path.name != CHECKSUMS_FILENAME
        )
    ]
    (snapshot_dir / CHECKSUMS_FILENAME).write_bytes("".join(lines).encode("utf-8"))


class TestMemberContentsAreValidated:
    """Sol pass 2, major: hash agreement proves the bytes did not change since
    the marker was installed. It proves nothing about what those bytes are.

    Every case here leaves the directory complete, both members present, and
    every digest matching — so it passes the whole of the pass-1 fix.
    """

    def test_zero_byte_members_hashed_as_empty_fail_verification(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _at(monkeypatch, MOMENT_ONE)
        snapshot_dir = _build(_discovery(), tmp_path)
        _restate(snapshot_dir, CATALOG_FILENAME, b"")
        _restate(snapshot_dir, PROVENANCE_FILENAME, b"")

        with pytest.raises(SnapshotVerificationError, match="is empty"):
            verify_snapshot(snapshot_dir)

    def test_zero_byte_members_hashed_as_empty_cannot_be_advanced_to(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _complete_and_advance(monkeypatch, tmp_path, MOMENT_ONE)
        before = _manifest_bytes(tmp_path)

        _at(monkeypatch, MOMENT_TWO)
        second = _build(_discovery(), tmp_path)
        _restate(second, CATALOG_FILENAME, b"")
        _restate(second, PROVENANCE_FILENAME, b"")

        with pytest.raises(SnapshotVerificationError, match="is empty"):
            advance_manifest(tmp_path, second.name)

        assert _manifest_bytes(tmp_path) == before
        assert json.loads(before)["snapshot_id"] == SNAPSHOT_ONE

    def test_a_torn_json_member_with_a_matching_hash_fails_verification(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A write interrupted mid-flush, then re-hashed by a later marker."""
        _at(monkeypatch, MOMENT_ONE)
        snapshot_dir = _build(_discovery(), tmp_path)
        whole = (snapshot_dir / CATALOG_FILENAME).read_bytes()
        _restate(snapshot_dir, CATALOG_FILENAME, whole[: len(whole) // 2])

        with pytest.raises(SnapshotVerificationError, match="not valid JSON"):
            verify_snapshot(snapshot_dir)

    def test_a_member_that_is_json_but_not_an_object_fails_verification(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _at(monkeypatch, MOMENT_ONE)
        snapshot_dir = _build(_discovery(), tmp_path)
        _restate(snapshot_dir, PROVENANCE_FILENAME, b"[]\n")

        with pytest.raises(SnapshotVerificationError, match="JSON list"):
            verify_snapshot(snapshot_dir)

    def test_a_member_missing_a_written_key_fails_verification(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _at(monkeypatch, MOMENT_ONE)
        snapshot_dir = _build(_discovery(), tmp_path)
        document = json.loads((snapshot_dir / CATALOG_FILENAME).read_bytes())
        del document["packages"]
        _restate(snapshot_dir, CATALOG_FILENAME, json.dumps(document).encode("utf-8"))

        with pytest.raises(SnapshotVerificationError, match="packages"):
            verify_snapshot(snapshot_dir)

    def test_a_catalogue_whose_count_disagrees_with_its_packages_fails(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _at(monkeypatch, MOMENT_ONE)
        snapshot_dir = _build(_discovery(), tmp_path)
        document = json.loads((snapshot_dir / CATALOG_FILENAME).read_bytes())
        document["packages"] = []
        _restate(snapshot_dir, CATALOG_FILENAME, json.dumps(document).encode("utf-8"))

        with pytest.raises(SnapshotVerificationError, match="claims 2 packages but carries 0"):
            verify_snapshot(snapshot_dir)

    def test_a_provenance_document_recording_no_requests_fails(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``build_snapshot`` cannot produce this, so the verifier refuses it."""
        _at(monkeypatch, MOMENT_ONE)
        snapshot_dir = _build(_discovery(), tmp_path)
        document = json.loads((snapshot_dir / PROVENANCE_FILENAME).read_bytes())
        document["requests"] = []
        _restate(snapshot_dir, PROVENANCE_FILENAME, json.dumps(document).encode("utf-8"))

        with pytest.raises(SnapshotVerificationError, match="records no requests"):
            verify_snapshot(snapshot_dir)

    def test_a_provenance_request_missing_a_field_fails(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _at(monkeypatch, MOMENT_ONE)
        snapshot_dir = _build(_discovery(), tmp_path)
        document = json.loads((snapshot_dir / PROVENANCE_FILENAME).read_bytes())
        del document["requests"][0]["body_sha256"]
        _restate(snapshot_dir, PROVENANCE_FILENAME, json.dumps(document).encode("utf-8"))

        with pytest.raises(SnapshotVerificationError, match="request 0"):
            verify_snapshot(snapshot_dir)

    def test_a_member_carrying_another_snapshots_id_fails_verification(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _at(monkeypatch, MOMENT_ONE)
        snapshot_dir = _build(_discovery(), tmp_path)
        document = json.loads((snapshot_dir / CATALOG_FILENAME).read_bytes())
        document["snapshot_id"] = SNAPSHOT_TWO
        _restate(snapshot_dir, CATALOG_FILENAME, json.dumps(document).encode("utf-8"))

        with pytest.raises(SnapshotVerificationError, match="names snapshot"):
            verify_snapshot(snapshot_dir)


class TestTheVerifierAcceptsWhatTheWriterWrites:
    """The other half of the class: a verifier that drifts stricter than the
    writer fails on the real vault rather than on a bad snapshot."""

    def test_the_real_build_output_verifies_and_advances(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _at(monkeypatch, MOMENT_ONE)
        snapshot_dir = _build(_discovery(), tmp_path)

        verify_snapshot(snapshot_dir)
        advance_manifest(tmp_path, snapshot_dir.name)

        assert json.loads(_manifest_bytes(tmp_path))["snapshot_id"] == SNAPSHOT_ONE

    def test_a_legitimately_empty_catalogue_still_verifies_and_advances(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``discover_catalog`` treats zero packages reconciling against a
        declared count of zero as a legitimate answer, so ``build_snapshot`` can
        honestly produce a zero-package snapshot and the verifier must accept
        one. Provenance is the asymmetric case: the writer refuses to produce an
        empty one, so the verifier refuses to accept one."""
        _at(monkeypatch, MOMENT_ONE)
        snapshot_dir = _build(_discovery(packages=()), tmp_path)

        verify_snapshot(snapshot_dir)
        advance_manifest(tmp_path, snapshot_dir.name)

        document = json.loads((snapshot_dir / CATALOG_FILENAME).read_bytes())
        assert document["packages"] == []
        assert document["package_count"] == 0
        assert json.loads(_manifest_bytes(tmp_path))["snapshot_id"] == SNAPSHOT_ONE

    def test_the_writer_emits_exactly_the_keys_the_verifier_requires(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """One source of truth: if the writer's shape ever moves, it moves
        because the shared tuples moved, and the verifier moves with it."""
        _at(monkeypatch, MOMENT_ONE)
        snapshot_dir = _build(_discovery(), tmp_path)

        catalog = json.loads((snapshot_dir / CATALOG_FILENAME).read_bytes())
        provenance = json.loads((snapshot_dir / PROVENANCE_FILENAME).read_bytes())

        assert set(catalog) == set(catalog_snapshot.CATALOG_DOCUMENT_KEYS)
        assert set(provenance) == set(catalog_snapshot.PROVENANCE_DOCUMENT_KEYS)
        for entry in provenance["requests"]:
            assert set(entry) == set(catalog_snapshot.PROVENANCE_REQUEST_KEYS)
        assert tuple(catalog_snapshot.MEMBER_DOCUMENT_KEYS) == catalog_snapshot.REQUIRED_MEMBERS
        assert set(catalog_snapshot.MEMBER_DOCUMENT_KEYS) == {
            CATALOG_FILENAME,
            PROVENANCE_FILENAME,
        }
