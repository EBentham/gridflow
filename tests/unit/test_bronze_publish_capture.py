"""``BronzeWriter.publish_capture`` — no-clobber member publication (ADR-033 P-9, A3).

W-1 drives the primitive directly with constructed same-second responses, so it
does not depend on production pacing to keep two captures apart.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

import pytest
from _neso_registry_support import family, install_registry, package, resource, write_registry

from gridflow.bronze.writer import BronzeCollisionError, BronzeWriter
from gridflow.connectors.base import RawResponse
from gridflow.connectors.neso_data_portal.captures import scan_dataset

if TYPE_CHECKING:
    from pathlib import Path

PKG = "aaaaaaaa-0000-4000-8000-000000000000"
R1 = "aaaaaaaa-0000-4000-8000-000000000001"
R2 = "aaaaaaaa-0000-4000-8000-000000000002"
FETCHED = datetime(2026, 10, 7, 12, 0, 0, tzinfo=UTC)
HEADER_ONLY = b"DATE,VALUE\n"


def _member(rid: str, body: bytes = HEADER_ONLY, **params: Any) -> RawResponse:
    resource_id = rid
    request_params: dict[str, Any] = {
        "package": "pkg-alpha",
        "package_id": PKG,
        "resource_id": resource_id,
        "resource_name": f"Alpha {rid[-1]}",
        "resource_filename": "alpha.csv",
        "ckan_last_modified": "2026-10-07T10:00:00.000001",
        "ckan_format": "CSV",
        "body_sha256": hashlib.sha256(body).hexdigest(),
        "capture_family": "alpha_series",
        "url_type": "upload",
        "empty_capture": True,
        "declared_content_length": len(body),
    }
    request_params.update(params)
    return RawResponse(
        body=body,
        content_type="text/csv",
        source="neso_data_portal",
        dataset="alpha_series",
        fetched_at=FETCHED,
        request_url=f"https://api.neso.energy/dataset/{PKG}/resource/{resource_id}/download/a.csv",
        request_params=request_params,
        api_version="3",
        data_date=date(2026, 10, 7),
    )


def _all_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*") if p.is_file())


def test_w1_identical_bodies_same_second_two_resources_both_survive(tmp_path: Path) -> None:
    writer = BronzeWriter(tmp_path)
    first = writer.publish_capture(_member(R1), extension="csv")
    second = writer.publish_capture(_member(R2), extension="csv")
    assert first != second
    assert first.read_bytes() == second.read_bytes() == HEADER_ONLY
    sidecars = sorted(p for p in _all_files(tmp_path) if p.name.endswith(".meta.json"))
    assert sidecars == sorted([first.with_suffix(".meta.json"), second.with_suffix(".meta.json")])
    for path, rid in ((first, R1), (second, R2)):
        meta = json.loads(path.with_suffix(".meta.json").read_text(encoding="utf-8"))
        assert meta["request_params"]["resource_id"] == rid
        assert meta["body_size_bytes"] == len(HEADER_ONLY)
        assert rid in path.name


def test_w1_masters_write_maps_both_to_one_path(tmp_path: Path) -> None:
    """The defect W-1 guards against: ``write()`` names by time and hash only."""
    writer = BronzeWriter(tmp_path)
    assert writer.write(_member(R1)) == writer.write(_member(R2))


def test_w2_existing_target_is_never_replaced(tmp_path: Path) -> None:
    writer = BronzeWriter(tmp_path)
    path = writer.publish_capture(_member(R1), extension="csv")
    original = path.read_bytes()
    original_meta = path.with_suffix(".meta.json").read_bytes()
    with pytest.raises(BronzeCollisionError, match="already exists"):
        writer.publish_capture(_member(R1), extension="csv")
    assert path.read_bytes() == original
    assert path.with_suffix(".meta.json").read_bytes() == original_meta
    assert not [p for p in _all_files(tmp_path) if p.name.startswith(".tmp_")]


def test_w2_collision_is_an_oserror(tmp_path: Path) -> None:
    """The runner counts any OSError as a failed member; a collision must be one."""
    assert issubclass(BronzeCollisionError, OSError)


def test_w3_failure_publishing_the_body_leaves_nothing_visible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _boom(src: object, dst: object) -> None:
        raise OSError("simulated crash at link")

    monkeypatch.setattr(os, "link", _boom)
    with pytest.raises(OSError, match="simulated crash"):
        BronzeWriter(tmp_path).publish_capture(_member(R1), extension="csv")
    files = _all_files(tmp_path)
    assert not [p for p in files if p.name.startswith("raw_")]
    assert not [p for p in files if p.name.startswith(".tmp_")]


def test_w4_failure_publishing_the_sidecar_leaves_an_orphan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    registry_dir = write_registry(
        tmp_path / "registry",
        [
            package(
                "pkg-alpha",
                PKG,
                [family("alpha_series", archetype="REG", empty_allowed=True)],
                [resource(R1, "Alpha 1", "alpha_series")],
            )
        ],
    )
    registry = install_registry(monkeypatch, registry_dir)
    real_link = os.link
    calls: list[str] = []

    def _second_link_fails(src: str, dst: str) -> None:
        calls.append(str(dst))
        if str(dst).endswith(".meta.json"):
            raise OSError("simulated crash publishing the sidecar")
        real_link(src, dst)

    monkeypatch.setattr(os, "link", _second_link_fails)
    data_dir = tmp_path / "data"
    with pytest.raises(OSError, match="sidecar"):
        BronzeWriter(data_dir).publish_capture(_member(R1), extension="csv")
    assert len(calls) == 2
    dataset_dir = data_dir / "bronze" / "neso_data_portal" / "alpha_series"
    scan = scan_dataset(dataset_dir, registry)
    assert scan.captures == ()
    assert len(scan.orphans) == 1 and R1 in scan.orphans[0].name
    assert scan.temps == ()


def test_w5_non_uuid_resource_id_is_refused(tmp_path: Path) -> None:
    writer = BronzeWriter(tmp_path)
    for bad in ("../../etc", "AAAAAAAA-0000-4000-8000-000000000001", "", None):
        with pytest.raises(ValueError, match="canonical UUID"):
            writer.publish_capture(_member(R1, resource_id=bad), extension="csv")
    assert _all_files(tmp_path) == []


def test_w5_non_allowlisted_extension_is_refused(tmp_path: Path) -> None:
    writer = BronzeWriter(tmp_path)
    for bad in ("bin", "exe", "csv/../x", "CSV"):
        with pytest.raises(ValueError, match="allowlisted"):
            writer.publish_capture(_member(R1), extension=bad)
    assert _all_files(tmp_path) == []


def test_w5_write_naming_is_unchanged_for_another_source(tmp_path: Path) -> None:
    body = b'{"data": [1, 2, 3]}'
    response = RawResponse(
        body=body,
        content_type="application/json; charset=utf-8",
        source="elexon",
        dataset="system_prices",
        fetched_at=datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
        data_date=date(2024, 1, 15),
    )
    path = BronzeWriter(tmp_path).write(response)
    sha8 = hashlib.sha256(body).hexdigest()[:8]
    assert path == (
        tmp_path
        / "bronze"
        / "elexon"
        / "system_prices"
        / "2024"
        / "01"
        / "15"
        / f"raw_20240115T120000Z_{sha8}.json"
    )
    assert path.with_suffix(".meta.json").exists()
