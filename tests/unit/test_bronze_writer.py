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
from typing import TYPE_CHECKING

import pytest

from gridflow.bronze.sanitize import sanitize_params
from gridflow.bronze.writer import BronzeWriter
from gridflow.connectors.base import RawResponse

if TYPE_CHECKING:
    from pathlib import Path


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


# --- Secret sanitization in the sidecar (CH1-01 / CH-SEC-01) -----------------
# The sidecar is the irreproducible system-of-record; an API key written there
# is a durable on-disk leak. These tests assert the writer masks secret values
# while preserving the key's presence (CLAUDE.md "surface, never drop").


def _sidecar(data_path: Path) -> dict[str, object]:
    meta_path = data_path.parent / (data_path.stem + ".meta.json")
    return json.loads(meta_path.read_text())


def _sidecar_text(data_path: Path) -> str:
    meta_path = data_path.parent / (data_path.stem + ".meta.json")
    return meta_path.read_text()


def test_sidecar_redacts_security_token(tmp_path: Path) -> None:
    """A securityToken in both the url and params must never reach the sidecar.

    The raw secret string must be absent from the serialized sidecar text, the
    param key must survive masked, the url token value masked, and a non-secret
    param (documentType) must pass through untouched.
    """
    secret = "SECRET123"  # noqa: S105 - test fixture, not a real credential
    response = RawResponse(
        body=b'{"data": []}',
        content_type="application/json",
        source="entsoe",
        dataset="actual_load",
        fetched_at=datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
        data_date=datetime(2024, 1, 15, tzinfo=UTC).date(),
        request_url=(f"https://web-api.tp.entsoe.eu/api?documentType=A44&securityToken={secret}"),
        request_params={"documentType": "A44", "securityToken": secret},
    )
    writer = BronzeWriter(tmp_path)
    data_path = writer.write(response)

    assert secret not in _sidecar_text(data_path)
    meta = _sidecar(data_path)
    assert meta["request_params"]["securityToken"] == "<redacted>"  # type: ignore[index]
    assert meta["request_params"]["documentType"] == "A44"  # type: ignore[index]
    assert "securityToken=<redacted>" in meta["request_url"]  # type: ignore[operator]
    assert "documentType=A44" in meta["request_url"]  # type: ignore[operator]


def test_redaction_case_insensitive(tmp_path: Path) -> None:
    """Key matching is case-folded: SecurityToken / SECURITYTOKEN both masked."""
    secret = "SECRET123"  # noqa: S105 - test fixture, not a real credential
    response = RawResponse(
        body=b"{}",
        content_type="application/json",
        source="entsoe",
        dataset="actual_load",
        fetched_at=datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
        data_date=datetime(2024, 1, 15, tzinfo=UTC).date(),
        request_url=f"https://example.com/api?SecurityToken={secret}",
        request_params={"SECURITYTOKEN": secret},
    )
    writer = BronzeWriter(tmp_path)
    data_path = writer.write(response)

    assert secret not in _sidecar_text(data_path)
    meta = _sidecar(data_path)
    assert meta["request_params"]["SECURITYTOKEN"] == "<redacted>"  # type: ignore[index]
    assert "SecurityToken=<redacted>" in meta["request_url"]  # type: ignore[operator]


def test_redaction_nested_and_list_params(tmp_path: Path) -> None:
    """Secret keys nested inside dicts and lists are still masked."""
    secret = "K"  # noqa: S105 - test fixture, not a real credential
    response = RawResponse(
        body=b"{}",
        content_type="application/json",
        source="gie",
        dataset="storage",
        fetched_at=datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
        data_date=datetime(2024, 1, 15, tzinfo=UTC).date(),
        request_params={"filters": [{"x-key": secret, "country": "DE"}]},
    )
    writer = BronzeWriter(tmp_path)
    data_path = writer.write(response)

    meta = _sidecar(data_path)
    inner = meta["request_params"]["filters"][0]  # type: ignore[index]
    assert inner["x-key"] == "<redacted>"
    assert inner["country"] == "DE"


def test_no_secret_is_byte_identical_noop(tmp_path: Path) -> None:
    """When no secret key is present the sidecar is byte-identical to the
    un-sanitized form, and integrity fields (body_sha256/data_date) are
    untouched. Regression guard against the sanitizer perturbing clean metadata.
    """
    response = RawResponse(
        body=b'{"data": [1, 2, 3]}',
        content_type="application/json",
        source="elexon",
        dataset="system_prices",
        fetched_at=datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC),
        data_date=datetime(2024, 1, 15, tzinfo=UTC).date(),
        request_url="https://data.elexon.co.uk/bmrs/api/v1/balancing?from=2024-01-15",
        request_params={"from": "2024-01-15", "settlementPeriodFrom": 1},
    )
    writer = BronzeWriter(tmp_path)
    data_path = writer.write(response)

    meta = _sidecar(data_path)
    assert meta["request_url"] == response.request_url
    assert meta["request_params"] == response.request_params
    assert meta["data_date"] == "2024-01-15"
    assert meta["body_sha256"] == hashlib.sha256(response.body).hexdigest()


# --- S-2: sanitize_params recurses into tuple/set containers -----------------
# httpx accepts params as a list[tuple[str, str]], so a future connector
# emitting a (key, value) pair tuple, or a secret nested in a tuple/set, must
# still be redacted. These pin sanitize_params directly.


def test_sanitize_params_redacts_secret_in_pair_tuple() -> None:
    """A (secret_key, value) pair tuple nested in a list has its value masked."""
    out = sanitize_params({"a": [("securityToken", "LEAK")]})
    assert out == {"a": [("securityToken", "<redacted>")]}


def test_sanitize_params_redacts_dict_nested_in_tuple() -> None:
    """A secret-keyed dict nested inside a tuple is still masked."""
    out = sanitize_params(({"securityToken": "LEAK"}, "other"))
    assert out == ({"securityToken": "<redacted>"}, "other")


def test_sanitize_params_redacts_secret_in_set() -> None:
    """A secret-keyed dict nested inside a set is still masked.

    The result is the same container type (a set), so it is rebuilt from
    redacted, hashable members.
    """
    out = sanitize_params({frozenset({"keep"}), ("securityToken", "LEAK")})
    assert ("securityToken", "<redacted>") in out
    assert frozenset({"keep"}) in out


def test_sanitize_params_preserves_clean_tuple_and_set() -> None:
    """Clean tuple/set values round-trip unchanged and keep their type."""
    clean_tuple = ("documentType", "A44")
    assert sanitize_params(clean_tuple) == clean_tuple
    assert isinstance(sanitize_params(clean_tuple), tuple)

    clean_set = frozenset({"a", "b"})
    assert sanitize_params(clean_set) == clean_set
    assert isinstance(sanitize_params(clean_set), frozenset)
