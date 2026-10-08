"""Offline evidence inputs for the NESO profiler and skeleton generator (ADR-036).

Two read-only loaders and one vendor-unit rule, shared by
``profile`` and ``skeleton`` so each is stated once:

- :func:`load_snapshot` — a catalogue snapshot directory, checked with
  ``catalog_snapshot.verify_snapshot`` (imported lazily: that module imports
  ``httpx`` and the client at top level, and nothing here needs them);
- :func:`load_field_info` — a field-info run directory (ADR-035 P-11), checked
  against its own ``sha256sums.txt`` in both directions and tied to the
  snapshot by ``field-info-run.json``'s ``snapshot_id``;
- :func:`vendor_unit` — a field-info column's unit, with placeholders read as
  absent.

Neither loader writes anything or opens a network connection.
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

__all__ = [
    "CATALOG_FILENAME",
    "CHECKSUMS_FILENAME",
    "FIELD_INFO_RUN_FILENAME",
    "PLACEHOLDER_UNITS",
    "EvidenceError",
    "field_entries",
    "load_field_info",
    "load_snapshot",
    "vendor_unit",
]

CATALOG_FILENAME = "catalog-snapshot.json"
CHECKSUMS_FILENAME = "sha256sums.txt"
FIELD_INFO_RUN_FILENAME = "field-info-run.json"

PLACEHOLDER_UNITS: frozenset[str] = frozenset({"", "n/a", "NA", "N/A"})
"""Field-info units that carry no unit (compared after stripping whitespace)."""

_CHUNK = 1024 * 1024


class EvidenceError(Exception):
    """A snapshot or field-info directory failed its integrity check."""


def load_snapshot(directory: Path) -> dict[str, Any]:
    """Verify a catalogue snapshot directory and return its catalogue document.

    Args:
        directory: The snapshot directory (``catalog-snapshot.json`` beside
            ``provenance.json`` and ``sha256sums.txt``).

    Returns:
        The parsed ``catalog-snapshot.json``.

    Raises:
        EvidenceError: ``verify_snapshot`` refused the directory or it is
            unreadable.
    """
    from gridflow.connectors.neso_data_portal.catalog_snapshot import (
        SnapshotError,
        verify_snapshot,
    )

    try:
        verify_snapshot(directory)
        document: Any = json.loads((directory / CATALOG_FILENAME).read_bytes())
    except (SnapshotError, OSError, json.JSONDecodeError) as exc:
        raise EvidenceError(f"snapshot {directory}: {exc}") from exc
    if not isinstance(document, dict) or not isinstance(document.get("packages"), list):
        raise EvidenceError(f"snapshot {directory}: {CATALOG_FILENAME} has no packages list")
    return document


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def _read_sums(path: Path) -> dict[str, str]:
    sums: dict[str, str] = {}
    for number, line in enumerate(path.read_bytes().decode("utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        digest, separator, name = line.partition("  ")
        if not separator or len(digest) != 64 or not name:
            raise EvidenceError(f"{path}: line {number} is not a 'sha256  filename' record")
        sums[name] = digest
    return sums


def load_field_info(directory: Path, snapshot_id: str) -> dict[str, dict[str, Any]]:
    """Verify a field-info run directory and return its family documents.

    Every file ``sha256sums.txt`` lists must exist and hash to its value, and
    every ``*.json`` in the directory must be listed; ``field-info-run.json``
    must name ``snapshot_id``.

    Args:
        directory: The field-info run directory.
        snapshot_id: The snapshot the evidence must belong to.

    Returns:
        Family key -> its field-info document (``fields[]``).

    Raises:
        EvidenceError: A listed file is missing or altered, an unlisted JSON
            file is present, the run names another snapshot, or a document is
            unreadable.
    """
    try:
        sums = _read_sums(directory / CHECKSUMS_FILENAME)
        for name, digest in sorted(sums.items()):
            path = directory / name
            if not path.is_file():
                raise EvidenceError(f"field-info {directory}: {name!r} is listed but missing")
            if _sha256(path) != digest:
                raise EvidenceError(f"field-info {directory}: {name!r} does not match its sha256")
        unlisted = sorted(p.name for p in directory.glob("*.json") if p.name not in sums)
        if unlisted:
            raise EvidenceError(f"field-info {directory}: unlisted files {unlisted}")
        run: Any = json.loads((directory / FIELD_INFO_RUN_FILENAME).read_bytes())
        if not isinstance(run, dict) or run.get("snapshot_id") != snapshot_id:
            found = run.get("snapshot_id") if isinstance(run, dict) else None
            raise EvidenceError(
                f"field-info {directory}: run names snapshot {found!r}, not {snapshot_id!r}"
            )
        documents: dict[str, dict[str, Any]] = {}
        for path in sorted(directory.glob("*.json")):
            if path.name == FIELD_INFO_RUN_FILENAME:
                continue
            document: Any = json.loads(path.read_bytes())
            if not isinstance(document, dict) or not isinstance(document.get("fields"), list):
                raise EvidenceError(f"field-info {path}: no fields list")
            documents[str(document.get("family") or path.stem)] = document
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceError(f"field-info {directory}: {exc}") from exc
    return documents


def field_entries(document: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Field id -> field entry of one field-info document, ``_id`` excluded."""
    if document is None:
        return {}
    return {
        str(field["id"]): field
        for field in document.get("fields", [])
        if isinstance(field, dict) and field.get("id") != "_id"
    }


def vendor_unit(field: dict[str, Any] | None) -> str | None:
    """A field's ``info.unit``, stripped; ``None`` when absent or a placeholder."""
    if field is None:
        return None
    info = field.get("info")
    unit = info.get("unit") if isinstance(info, dict) else None
    if not isinstance(unit, str):
        return None
    stripped = unit.strip()
    return None if stripped in PLACEHOLDER_UNITS else stripped
