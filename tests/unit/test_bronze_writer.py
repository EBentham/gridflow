"""Behavioural tests for the bronze writer (issue 12).

Bronze is the irreproducible system-of-record. These tests assert:
- the data file and sidecar are written atomically (temp + os.replace), so a
  crash mid-write never leaves a torn body a later reader silently skips;
- a write-time ``written_at`` stamp is emitted (not only the pre-write
  ``fetched_at``);
- the on-disk ``body_sha256`` actually matches the stored bytes.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from gridflow.bronze.writer import BronzeWriter
from gridflow.connectors.base import RawResponse


def _response(body: bytes = b'{"data": [1, 2, 3]}') -> RawResponse:
    return RawResponse(
        body=body,
        content_type="application/json",
        source="elexon",
        dataset="system_prices",
        fetched_at=datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
        data_date=datetime(2024, 1, 15, tzinfo=UTC).date(),
    )


def test_write_returns_existing_data_path(tmp_path: Path) -> None:
    """The writer keeps returning the path to the written data file."""
    writer = BronzeWriter(tmp_path)
    data_path = writer.write(_response())
    assert data_path.exists()
    assert data_path.read_bytes() == _response().body


def test_data_file_written_atomically(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A failure during the atomic replace must leave no torn data file.

    Pre-fix: write_bytes writes the body directly, so the data file exists even
    when the write is interrupted. Post-fix: temp + os.replace means an
    interrupted replace leaves the final path absent (only a .tmp_ file).
    """
    writer = BronzeWriter(tmp_path)
    real_replace = __import__("os").replace

    def boom(src: object, dst: object) -> None:
        raise OSError("simulated crash during replace")

    monkeypatch.setattr("os.replace", boom)
    with pytest.raises(OSError):
        writer.write(_response())
    monkeypatch.setattr("os.replace", real_replace)

    # No final data file should be visible to a reader, only temp leftovers.
    final_files = [p for p in tmp_path.rglob("raw_*") if not p.name.startswith(".tmp_")]
    assert final_files == [], f"torn final files visible: {final_files}"


def test_no_tmp_files_remain_after_success(tmp_path: Path) -> None:
    """A successful write leaves no .tmp_ leftovers in the partition."""
    writer = BronzeWriter(tmp_path)
    data_path = writer.write(_response())
    leftovers = list(data_path.parent.glob(".tmp_*"))
    assert leftovers == []


def test_sidecar_carries_write_time_written_at(tmp_path: Path) -> None:
    """The sidecar must carry a write-time ``written_at`` distinct from fetched_at.

    Pre-fix: only fetched_at is emitted; written_at is never written.
    """
    before = datetime.now(UTC)
    writer = BronzeWriter(tmp_path)
    data_path = writer.write(_response())
    after = datetime.now(UTC)

    meta_path = data_path.with_suffix("").with_suffix(".meta.json")
    # filename is raw_<ts>_<hash>.json -> sidecar is raw_<ts>_<hash>.meta.json
    meta_path = data_path.parent / (data_path.stem + ".meta.json")
    meta = json.loads(meta_path.read_text())

    assert "written_at" in meta, "sidecar must stamp a write-time written_at"
    written_at = datetime.fromisoformat(meta["written_at"])
    assert before <= written_at <= after
    # fetched_at (pre-write, from the fixture) is earlier than the write time.
    assert meta["fetched_at"] == "2024-01-15T12:00:00+00:00"


def test_body_sha256_matches_stored_bytes(tmp_path: Path) -> None:
    """The on-disk body_sha256 must equal the hash of the bytes on disk."""
    writer = BronzeWriter(tmp_path)
    data_path = writer.write(_response())

    meta_path = data_path.parent / (data_path.stem + ".meta.json")
    meta = json.loads(meta_path.read_text())

    on_disk_hash = hashlib.sha256(data_path.read_bytes()).hexdigest()
    assert meta["body_sha256"] == on_disk_hash


def test_body_sha256_detects_mutated_body(tmp_path: Path) -> None:
    """A body that does not match its recorded digest is detectable.

    Guards the integrity invariant: mutating the stored body breaks the match.
    """
    writer = BronzeWriter(tmp_path)
    data_path = writer.write(_response())
    meta_path = data_path.parent / (data_path.stem + ".meta.json")
    recorded = json.loads(meta_path.read_text())["body_sha256"]

    data_path.write_bytes(b"tampered")
    mutated_hash = hashlib.sha256(data_path.read_bytes()).hexdigest()
    assert mutated_hash != recorded
