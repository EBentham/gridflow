"""Shared builders for the NESO profiler and skeleton tests (ADR-036).

Not a test module (no ``test_`` prefix). Builds the two offline evidence
inputs the way their writers do, so the tools' integrity checks run for real:

- :func:`write_snapshot` — a snapshot directory that passes
  ``catalog_snapshot.verify_snapshot`` (documents built and serialised by the
  module's own builders, checksums by its own writer);
- :func:`write_field_info` — a field-info run directory with its
  ``field-info-run.json`` and ``sha256sums.txt``.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

SNAPSHOT_ID = "20261006T195819Z"
PILOT_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "neso_data_portal" / "pilot"


def pilot_snapshot_packages() -> list[dict[str, Any]]:
    """The six pilot packages of the snapshot of record (trimmed fixture)."""
    document = json.loads((PILOT_DIR / "pilot_snapshot.json").read_text(encoding="utf-8"))
    packages: list[dict[str, Any]] = document["packages"]
    return packages


def pilot_field_info(key: str) -> dict[str, Any]:
    """One pilot family's field-info document (verbatim from the S run)."""
    document: dict[str, Any] = json.loads(
        (PILOT_DIR / f"field_info_{key}.json").read_text(encoding="utf-8")
    )
    return document


def write_snapshot(
    parent: Path, packages: list[dict[str, Any]], snapshot_id: str = SNAPSHOT_ID
) -> Path:
    """Write a verifiable snapshot directory ``<parent>/<snapshot_id>``.

    Args:
        parent: Where the snapshot directory goes.
        packages: The ``packages`` of the catalogue document.
        snapshot_id: The directory name and the documents' id.

    Returns:
        The snapshot directory.
    """
    from gridflow.connectors.neso_data_portal import catalog_snapshot as cs

    directory = parent / snapshot_id
    directory.mkdir(parents=True)
    trace = {
        "action": "package_search",
        "params": {"rows": "50", "start": "0"},
        "started_at": "2026-10-06T19:58:15.902186Z",
        "finished_at": "2026-10-06T19:58:16.090081Z",
        "status_code": 200,
        "headers": {"content-type": "application/json"},
        "body_sha256": "0" * 64,
    }
    (directory / cs.CATALOG_FILENAME).write_bytes(
        cs._serialize_document(cs._catalog_document(snapshot_id, packages))
    )
    (directory / cs.PROVENANCE_FILENAME).write_bytes(
        cs._serialize_document(cs._provenance_document(snapshot_id, [trace]))
    )
    cs._write_checksums(directory)
    return directory


def write_field_info(
    parent: Path,
    documents: dict[str, dict[str, Any]],
    snapshot_id: str = SNAPSHOT_ID,
    name: str = "20261008T114228Z",
) -> Path:
    """Write a field-info run directory ``<parent>/<name>``.

    Args:
        parent: Where the run directory goes.
        documents: Family key -> field-info document.
        snapshot_id: The snapshot ``field-info-run.json`` names.
        name: The run directory name.

    Returns:
        The run directory.
    """
    directory = parent / name
    directory.mkdir(parents=True)
    for key, document in documents.items():
        (directory / f"{key}.json").write_text(
            json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
        )
    run = {
        "snapshot_id": snapshot_id,
        "families": [{"family": key, "outcome": "ok"} for key in sorted(documents)],
    }
    (directory / "field-info-run.json").write_text(json.dumps(run, indent=2) + "\n", "utf-8")
    sums = "".join(
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n"
        for path in sorted(directory.glob("*.json"))
    )
    (directory / "sha256sums.txt").write_bytes(sums.encode("utf-8"))
    return directory


def field_doc(key: str, fields: list[tuple[str, str, str | None]]) -> dict[str, Any]:
    """A synthetic field-info document: ``(id, type, unit)`` per column."""
    entries: list[dict[str, Any]] = [{"type": "int", "id": "_id"}]
    for column, kind, unit in fields:
        info: dict[str, Any] = {"title": column, "description": f"{column} description"}
        if unit is not None:
            info["unit"] = unit
        entries.append({"id": column, "type": kind, "info": info})
    return {"family": key, "fields": entries}
