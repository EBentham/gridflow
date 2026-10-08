"""Snapshot coverage command (ADR-033 P-11, A4; V-1..V-5)."""

from __future__ import annotations

import builtins
import errno
import io
import json
import os
from pathlib import Path
from typing import Any

import pytest
from _neso_registry_support import (
    edit_sidecar,
    family,
    install_registry,
    package,
    resource,
    write_capture,
    write_registry,
)

from gridflow.connectors.neso_data_portal import captures, coverage
from gridflow.connectors.neso_data_portal.captures import scan_dataset

PKG_A = "aaaaaaaa-0000-4000-8000-000000000000"
R1 = "aaaaaaaa-0000-4000-8000-000000000001"
R1B = "aaaaaaaa-0000-4000-8000-0000000000b1"
R2 = "aaaaaaaa-0000-4000-8000-000000000002"

_PACKAGE = package(
    "pkg-alpha",
    PKG_A,
    [family("alpha_series"), family("alpha_extra")],
    [
        resource(R1, "Alpha Series", "alpha_series"),
        resource(R1B, "Alpha Series 2025", "alpha_series"),
        resource(R2, "Alpha Extra", "alpha_extra"),
    ],
)


def _snapshot(tmp_path: Path, ids: list[str]) -> Path:
    names = {R1: "Alpha Series", R1B: "Alpha Series 2025", R2: "Alpha Extra"}
    path = tmp_path / "snapshot.json"
    path.write_text(
        json.dumps(
            {
                "snapshot_id": "synthetic",
                "packages": [
                    {
                        "name": "pkg-alpha",
                        "id": PKG_A,
                        "resources": [{"id": i, "name": names[i]} for i in ids],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def _install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    frozen: list[dict[str, str]] | None = None,
    adjudications: list[dict[str, str]] | None = None,
) -> Path:
    directory = write_registry(
        tmp_path / "registry",
        [_PACKAGE],
        frozen=frozen if frozen is not None else [{"key": "alpha_series", "package": "pkg-alpha"}],
        adjudications=adjudications,
    )
    install_registry(monkeypatch, directory)
    return directory


def _capture(data_dir: Path, key: str, **kwargs: Any) -> tuple[Path, Path]:
    defaults: dict[str, Any] = {
        "package_slug": "pkg-alpha",
        "package_id": PKG_A,
        "resource_id": R1,
        "resource_name": "Alpha Series",
    }
    defaults.update(kwargs)
    return write_capture(captures.bronze_source_dir(data_dir) / key, **defaults)


def _run(snapshot: Path, data_dir: Path, *extra: str) -> int:
    return coverage.main(["--snapshot", str(snapshot), "--data-dir", str(data_dir), *extra])


def test_v1_uncaptured_snapshot_resource_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _install(tmp_path, monkeypatch)
    data_dir = tmp_path / "data"
    _capture(data_dir, "alpha_series")
    assert _run(_snapshot(tmp_path, [R1, R2]), data_dir) == 1
    out = capsys.readouterr().out
    assert "captured 1" in out and "missing 1" in out
    assert f"missing: pkg-alpha {R2} 'Alpha Extra'" in out


def test_v1_positive_control_everything_captured_is_clean(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install(tmp_path, monkeypatch)
    data_dir = tmp_path / "data"
    _capture(data_dir, "alpha_series")
    assert _run(_snapshot(tmp_path, [R1]), data_dir, "--verify-sha") == 0


def test_v2_all_unusable_sidecars_report_unusable_with_reasons(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install(
        tmp_path,
        monkeypatch,
        frozen=[
            {"key": "alpha_series", "package": "pkg-alpha"},
            {"key": "alpha_extra", "package": "pkg-alpha"},
        ],
    )
    data_dir = tmp_path / "data"
    _b, no_name = _capture(data_dir, "alpha_series", body=b"A\n1\n")
    edit_sidecar(no_name, lambda meta: meta["request_params"].pop("resource_name"))
    _capture(data_dir, "alpha_extra", body=b"A\n2\n")
    _b, other_package = _capture(data_dir, "alpha_series", body=b"A\n3\n")
    edit_sidecar(other_package, lambda meta: meta["request_params"].update(package="pkg-other"))

    report = coverage.build_report(json.loads(_snapshot(tmp_path, [R1]).read_text()), data_dir)
    assert report.counts["unusable"] == 1 and report.counts["captured"] == 0
    ((ref, reasons),) = report.unusable
    assert ref.resource_id == R1
    assert len(reasons) == 3, reasons
    joined = "\n".join(reasons)
    assert "provenance_for" in joined
    assert "does not select" in joined
    assert "pkg-other" in joined
    assert _run(_snapshot(tmp_path, [R1]), data_dir) == 1


def test_v3_adjudicated_resource_and_orphan_temp_counts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _install(
        tmp_path,
        monkeypatch,
        adjudications=[
            {
                "resource_id": R2,
                "package": "pkg-alpha",
                "reason": "vendor returns 404",
                "evidence": "probe log",
                "ruling": "RULINGS 999",
            }
        ],
    )
    data_dir = tmp_path / "data"
    body, _sidecar = _capture(data_dir, "alpha_series")
    orphan, orphan_sidecar = _capture(data_dir, "alpha_series", body=b"A\n7\n")
    orphan_sidecar.unlink()
    (orphan.parent / f".tmp_{orphan.name}.feed").write_bytes(b"x")
    json_out = tmp_path / "report.json"
    assert _run(_snapshot(tmp_path, [R1, R2]), data_dir, "--json", str(json_out)) == 0
    report = json.loads(json_out.read_text(encoding="utf-8"))
    assert report["counts"]["adjudicated"] == 1
    assert report["counts"]["captured"] == 1
    assert report["counts"]["orphans"] == 1
    assert report["counts"]["temps"] == 1
    assert body.exists()
    assert "orphans 1, temps 1" in capsys.readouterr().out


def test_v4_coverage_is_independent_of_bronze(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """S3 probe shape: every capture in bronze is valid, yet one member was never captured."""
    _install(tmp_path, monkeypatch)
    data_dir = tmp_path / "data"
    _capture(data_dir, "alpha_series")
    scan = scan_dataset(captures.bronze_source_dir(data_dir) / "alpha_series", _loaded())
    assert len(scan.captures) == 1 and scan.unusable == () and scan.orphans == ()
    report = coverage.build_report(json.loads(_snapshot(tmp_path, [R1, R1B]).read_text()), data_dir)
    assert report.counts["missing"] == 1
    assert [ref.resource_id for ref in report.missing] == [R1B]


def _loaded() -> Any:
    from gridflow.connectors.neso_data_portal import registry as registry_module

    return registry_module.load_registry()


def test_v5_freeze_in_coverage(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    registry_dir = _install(tmp_path, monkeypatch, frozen=[])
    data_dir = tmp_path / "data"
    snapshot = _snapshot(tmp_path, [R1])
    _capture(data_dir, "alpha_series")

    report = coverage.build_report(json.loads(snapshot.read_text()), data_dir)
    assert report.unfrozen_keys == ["alpha_series"]
    assert _run(snapshot, data_dir) == 1

    (registry_dir / "_frozen_keys.json").write_text(
        json.dumps([{"key": "alpha_series", "package": "pkg-alpha"}]), encoding="utf-8"
    )
    assert _run(snapshot, data_dir) == 0

    (captures.bronze_source_dir(data_dir) / "renamed_key").mkdir()
    report = coverage.build_report(json.loads(snapshot.read_text()), data_dir)
    assert report.unregistered_keys == ["renamed_key"]
    assert _run(snapshot, data_dir) == 1


def test_usage_errors_exit_2(tmp_path: Path) -> None:
    missing = tmp_path / "absent.json"
    assert coverage.main(["--snapshot", str(missing), "--data-dir", str(tmp_path)]) == 2
    not_snapshot = tmp_path / "list.json"
    not_snapshot.write_text("[]", encoding="utf-8")
    assert coverage.main(["--snapshot", str(not_snapshot), "--data-dir", str(tmp_path)]) == 2


class _DiskFull:
    """Writes a short prefix, then fails as a full disk would."""

    def __init__(self, handle: Any) -> None:
        self._handle = handle

    def __enter__(self) -> _DiskFull:
        return self

    def __exit__(self, *exc: object) -> None:
        self._handle.close()

    def close(self) -> None:
        self._handle.close()

    def write(self, data: bytes | str) -> int:
        self._handle.write(data[:64])
        self._handle.flush()
        raise OSError(errno.ENOSPC, "No space left on device")


def _report_dir_with_existing_report(tmp_path: Path) -> tuple[Path, bytes]:
    out_dir = tmp_path / "out"
    out_dir.mkdir()
    previous = (json.dumps({"previous": "x" * 3200}, indent=2) + "\n").encode("utf-8")
    report = out_dir / "report.json"
    report.write_bytes(previous)
    return report, previous


def test_json_report_replaces_an_existing_report_and_leaves_no_temp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _install(tmp_path, monkeypatch)
    data_dir = tmp_path / "data"
    _capture(data_dir, "alpha_series")
    report, _previous = _report_dir_with_existing_report(tmp_path)
    assert _run(_snapshot(tmp_path, [R1]), data_dir, "--json", str(report)) == 0
    assert json.loads(report.read_text(encoding="utf-8"))["counts"]["captured"] == 1
    assert [p.name for p in report.parent.iterdir()] == ["report.json"]


def test_a_failed_json_write_leaves_the_previous_report_intact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A disk-full ``--json`` write must not truncate the report it replaces."""
    _install(tmp_path, monkeypatch)
    data_dir = tmp_path / "data"
    _capture(data_dir, "alpha_series")
    snapshot = _snapshot(tmp_path, [R1])
    report, previous = _report_dir_with_existing_report(tmp_path)
    out_dir = report.parent.resolve()
    real_open = io.open

    def failing_open(file: Any, mode: str = "r", *args: Any, **kwargs: Any) -> Any:
        handle = real_open(file, mode, *args, **kwargs)
        if (
            "w" in mode
            and isinstance(file, (str, os.PathLike))
            and Path(file).resolve().parent == out_dir
        ):
            return _DiskFull(handle)
        return handle

    monkeypatch.setattr(io, "open", failing_open)
    monkeypatch.setattr(builtins, "open", failing_open)

    with pytest.raises(OSError, match="No space left"):
        _run(snapshot, data_dir, "--json", str(report))

    monkeypatch.undo()
    assert report.read_bytes() == previous, (
        f"report truncated to {len(report.read_bytes())} of {len(previous)} bytes"
    )
    assert [p.name for p in report.parent.iterdir()] == ["report.json"]


def test_d3_4_a_null_last_modified_dump_capture_counts_captured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """T-D3-4: detects a dump capture reported ``unusable`` (red on master, A4)."""
    _install(tmp_path, monkeypatch)
    data_dir = tmp_path / "data"
    _body, sidecar = _capture(data_dir, "alpha_series", ckan_last_modified="")

    def _as_dump(meta: dict[str, Any]) -> None:
        meta["request_params"]["url_type"] = "datastore"
        meta["request_params"]["resource_filename"] = R1

    edit_sidecar(sidecar, _as_dump)
    assert _run(_snapshot(tmp_path, [R1]), data_dir, "--verify-sha") == 0
    out = capsys.readouterr().out
    assert "captured 1" in out and "unusable 0" in out, out
