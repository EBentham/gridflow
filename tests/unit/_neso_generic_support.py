"""Shared builders for the NESO generic silver engine tests (ADR-034).

Not a test module (no ``test_`` prefix). Three things live here so every test
uses one copy of each:

- :func:`write_capture` — a synthetic bronze capture in unit A's sidecar shape
  (``publish_capture`` output), streaming large bodies from a writer callable;
- :func:`install_generated` — P-15's test seam: install a tmp registry and the
  generated transformer set, specs and ingest-only reasons through
  ``monkeypatch.setitem`` so nothing leaks into the registry-wide loops;
- :func:`assert_same_output` — P-14's B7 equality helper.

Only the standard library is imported at module level, so the memory probe can
import :func:`write_capture` in a fresh interpreter cheaply.
"""

from __future__ import annotations

import hashlib
import json
import os
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any, BinaryIO

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable
    from pathlib import Path

    import polars as pl
    import pytest

    from gridflow.connectors.neso_data_portal.registry import Registry


SOURCE = "neso_data_portal"


def write_capture(
    data_dir: Path,
    key: str,
    *,
    package_slug: str,
    package_id: str,
    resource_id: str,
    resource_name: str,
    body: bytes | None = None,
    body_writer: Callable[[BinaryIO], None] | None = None,
    written_at: datetime,
    ckan_last_modified: str | None = "2026-10-01T10:00:00.000001",
    resource_filename: str = "file.csv",
    url_type: str = "upload",
    empty_capture: bool | None = False,
    partition: date | None = None,
    ckan_format: str = "CSV",
    extension: str = "csv",
) -> tuple[Path, Path]:
    """Write one committed capture (body, then sidecar) under ``bronze/<source>/<key>``.

    Args:
        data_dir: The data root.
        key: The bronze dataset directory (a registry family key).
        package_slug: The sidecar ``request_params.package``.
        package_id: The CKAN package UUID.
        resource_id: The CKAN resource UUID.
        resource_name: The selector name.
        body: The body bytes; exclusive with ``body_writer``.
        body_writer: Streams the body into an open binary file (large bodies).
        written_at: The sidecar ``written_at``; also names the body.
        ckan_last_modified: ``None`` writes an empty string (a dump capture).
        resource_filename: The vendor filename.
        url_type: ``upload`` or ``datastore``.
        empty_capture: The A marker; ``None`` omits it (a legacy sidecar).
        partition: The bronze date directory; defaults to ``written_at``'s date.
        ckan_format: The recorded CKAN format.
        extension: The body extension.

    Returns:
        ``(body_path, sidecar_path)``.
    """
    if (body is None) == (body_writer is None):
        raise ValueError("pass exactly one of body= and body_writer=")
    day = partition or written_at.date()
    directory = (
        data_dir / "bronze" / SOURCE / key / f"{day.year}" / f"{day.month:02d}" / f"{day.day:02d}"
    )
    directory.mkdir(parents=True, exist_ok=True)
    stamp = written_at.astimezone(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    temp = directory / f".tmp_{resource_id}_{stamp}"
    with temp.open("wb") as handle:
        if body is not None:
            handle.write(body)
        else:
            assert body_writer is not None
            body_writer(handle)
    digest = hashlib.sha256()
    size = 0
    with temp.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
            size += len(chunk)
    sha = digest.hexdigest()
    stem = f"raw_{stamp}_{resource_id}_{sha[:8]}"
    body_path = directory / f"{stem}.{extension}"
    os.replace(temp, body_path)
    params: dict[str, Any] = {
        "package": package_slug,
        "package_id": package_id,
        "resource_id": resource_id,
        "resource_name": resource_name,
        "resource_filename": resource_filename,
        "ckan_last_modified": ckan_last_modified or "",
        "ckan_format": ckan_format,
        "body_sha256": sha,
        "capture_family": key,
        "url_type": url_type,
        "declared_content_length": size,
    }
    if empty_capture is not None:
        params["empty_capture"] = empty_capture
    meta = {
        "source": SOURCE,
        "dataset": key,
        "fetched_at": written_at.isoformat(),
        "written_at": written_at.isoformat(),
        "data_date": day.isoformat(),
        "request_url": f"https://api.neso.energy/dataset/{package_id}/resource/{resource_id}"
        f"/download/{resource_filename}",
        "request_params": params,
        "api_version": "3",
        "http_status": 200,
        "content_type": "text/csv",
        "body_sha256": sha,
        "body_size_bytes": size,
        "page": 1,
        "total_pages": 1,
    }
    sidecar = directory / f"{stem}.meta.json"
    sidecar.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return body_path, sidecar


def install_generated(
    monkeypatch: pytest.MonkeyPatch, directory: Path, packages: list[dict[str, Any]]
) -> tuple[Registry, Any]:
    """Write a tmp registry and install its generated set (P-15's test seam).

    The registry seam is unit A's ``install_registry``; the transformer
    classes, ``_latest`` specs and ingest-only reasons go in through
    ``monkeypatch.setitem``, so they vanish at teardown and never reach the
    registry-wide loops of other tests.

    Args:
        monkeypatch: The test's monkeypatch.
        directory: Where to write the registry files.
        packages: Package documents (``_neso_registry_support.package``).

    Returns:
        ``(registry, generated_set)``.
    """
    from _neso_registry_support import install_registry, write_registry

    from gridflow.silver import registry as silver_registry
    from gridflow.silver.latest_views import LATEST_VIEW_SPECS
    from gridflow.silver.neso_data_portal import generic

    loaded = install_registry(monkeypatch, write_registry(directory, packages))
    generated = generic.generated_registrations(loaded)
    for key, cls in generated.transformers.items():
        monkeypatch.setitem(silver_registry._REGISTRY, (SOURCE, key), cls)
    for spec_key, spec in generated.specs.items():
        monkeypatch.setitem(LATEST_VIEW_SPECS, spec_key, spec)
    for skip_key, skip in generated.ingest_only.items():
        monkeypatch.setitem(silver_registry._INGEST_ONLY, skip_key, skip)
    return loaded, generated


def snapshot(data_dir: Path, family: str) -> dict[str, Any]:
    """Every output and completion record of ``family`` under ``data_dir``.

    Returns:
        ``{"outputs": {relative path: frame}, "ledger": {capture id: record}}``.
    """
    import polars as pl

    silver = data_dir / "silver" / SOURCE / family
    outputs = {
        path.relative_to(data_dir).as_posix(): pl.read_parquet(path, hive_partitioning=False)
        for path in sorted(silver.rglob("[!.]*.parquet"))
    }
    ledger_dir = data_dir / "state" / SOURCE / "completion" / family
    ledger: dict[str, Any] = {}
    for path in sorted(ledger_dir.rglob("[!.]*.parquet")) if ledger_dir.is_dir() else []:
        row = pl.read_parquet(path).to_dicts()[0]
        ledger[row["bronze_capture_id"]] = row
    return {"outputs": outputs, "ledger": ledger}


def assert_same_output(
    left: dict[str, Any], right: dict[str, Any], entity_key: Iterable[str]
) -> None:
    """P-14's B7 equality: same paths, rows and ledger; ``source_run_id`` excluded.

    Rows compare sorted by the entity key, including ``published_at``,
    ``available_at`` and ``bronze_capture_id``; ledger records compare whole.

    Args:
        left: A :func:`snapshot`.
        right: Another :func:`snapshot`.
        entity_key: The family's entity key.
    """
    key = list(entity_key)
    assert sorted(left["outputs"]) == sorted(right["outputs"])
    for path, frame in left["outputs"].items():
        other = right["outputs"][path]
        a = frame.drop("source_run_id").sort(key)
        b = other.drop("source_run_id").sort(key)
        assert a.columns == b.columns, path
        assert a.equals(b), path
    assert left["ledger"] == right["ledger"]


def frame_from(header: list[str], rows: list[list[str | None]]) -> pl.DataFrame:
    """An all-``Utf8`` frame, the shape every reader yields."""
    import polars as pl

    return pl.DataFrame(
        {name: [row[i] for row in rows] for i, name in enumerate(header)},
        schema=dict.fromkeys(header, pl.Utf8),
    )


def utc(*parts: int) -> datetime:
    """A tz-aware UTC datetime."""
    return datetime(*parts, tzinfo=UTC)  # type: ignore[misc]
