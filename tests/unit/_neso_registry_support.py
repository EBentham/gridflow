"""Shared builders for the NESO registry, capture and runner tests (ADR-033).

Not a test module (no ``test_`` prefix). Tests substitute a registry through
P-1's one seam: :func:`install_registry` monkeypatches
``registry.load_registry`` and ``endpoints.FAMILIES`` together.
"""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from gridflow.connectors.neso_data_portal import endpoints
from gridflow.connectors.neso_data_portal import registry as registry_module

if TYPE_CHECKING:
    from pathlib import Path

    import pytest

    from gridflow.connectors.neso_data_portal.registry import Registry

LEGACY_LEDGER = [
    {"key": "daily_wind_availability", "package": "daily-wind-availability"},
    {"key": "embedded_wind_solar_forecast", "package": "embedded-wind-and-solar-forecasts"},
    {"key": "historic_generation_mix", "package": "historic-generation-mix"},
]

_REAL_LOAD = registry_module.load_registry


def family(
    key: str,
    *,
    kind: str = "tabular",
    archetype: str = "SER",
    empty_allowed: bool = False,
    max_download_bytes: int = 64 * 1024 * 1024,
    legacy: bool = False,
    name_regex: str | None = None,
) -> dict[str, Any]:
    """One family entry."""
    return {
        "key": key,
        "kind": kind,
        "legacy": legacy,
        "archetype": archetype,
        "refresh": "daily",
        "empty_allowed": empty_allowed,
        "max_download_bytes": max_download_bytes,
        "name_regex": name_regex,
        "transformer": "bespoke" if legacy else None,
    }


def resource(
    resource_id: str,
    name: str,
    family_key: str,
    *,
    fmt: str = "CSV",
    url_type: str = "upload",
    disposition: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """One resource entry; SILVER for CSV by default, DOC otherwise."""
    if disposition is None:
        disposition = {"kind": "SILVER", "key": family_key} if fmt == "CSV" else {"kind": "DOC"}
    return {
        "id": resource_id,
        "name": name,
        "format": fmt,
        "url_type": url_type,
        "family": family_key,
        "disposition": disposition,
    }


def package(
    slug: str,
    package_id: str,
    families: list[dict[str, Any]],
    resources: list[dict[str, Any]],
) -> dict[str, Any]:
    """One package document."""
    return {
        "package": slug,
        "package_id": package_id,
        "group": "synthetic",
        "archetype": "SER",
        "refresh": "daily",
        "eligibility": {"status": "eligible"},
        "families": families,
        "resources": resources,
    }


def write_registry(
    directory: Path,
    packages: list[dict[str, Any]],
    *,
    frozen: list[dict[str, str]] | None = None,
    adjudications: list[dict[str, str]] | None = None,
) -> Path:
    """Write a registry directory (package files plus both ledgers)."""
    directory.mkdir(parents=True, exist_ok=True)
    for document in packages:
        (directory / f"{document['package']}.json").write_text(
            registry_module.dump_json(document), encoding="utf-8"
        )
    (directory / "_frozen_keys.json").write_text(
        registry_module.dump_json(frozen if frozen is not None else []), encoding="utf-8"
    )
    (directory / "_adjudications.json").write_text(
        registry_module.dump_json(adjudications if adjudications is not None else []),
        encoding="utf-8",
    )
    return directory


def install_registry(monkeypatch: pytest.MonkeyPatch, directory: Path) -> Registry:
    """Point every runtime consumer at the registry in ``directory`` (P-1 seam)."""
    loaded = _REAL_LOAD(directory)

    def _load(path: Path | None = None) -> Registry:
        return loaded if path is None else _REAL_LOAD(path)

    monkeypatch.setattr(registry_module, "load_registry", _load)
    monkeypatch.setattr(endpoints, "FAMILIES", endpoints.build_families(loaded))
    return loaded


def write_capture(
    dataset_dir: Path,
    *,
    package_slug: str,
    package_id: str,
    resource_id: str,
    resource_name: str,
    body: bytes = b"A,B\n1,2\n",
    ckan_last_modified: str = "2026-10-01T10:00:00.000001",
    ckan_format: str = "CSV",
    extension: str = "csv",
    written_at: datetime | None = None,
    fetched_at: datetime | None = None,
    partition: str = "2026/10/07",
    legacy_name: bool = False,
    additive: bool = True,
) -> tuple[Path, Path]:
    """Write one body + sidecar pair shaped like ``publish_capture`` output.

    ``legacy_name`` writes master's ``raw_<ts>_<sha8>`` name; ``additive=False``
    omits P-8's four additive keys (the E8 legacy sidecar shape).
    """
    fetched = fetched_at or datetime(2026, 10, 7, 12, 0, 0, tzinfo=UTC)
    written = written_at or fetched
    sha = hashlib.sha256(body).hexdigest()
    ts = fetched.strftime("%Y%m%dT%H%M%SZ")
    stem = f"raw_{ts}_{sha[:8]}" if legacy_name else f"raw_{ts}_{resource_id}_{sha[:8]}"
    directory = dataset_dir.joinpath(*partition.split("/"))
    directory.mkdir(parents=True, exist_ok=True)
    body_path = directory / f"{stem}.{extension}"
    body_path.write_bytes(body)
    params: dict[str, Any] = {
        "package": package_slug,
        "package_id": package_id,
        "resource_id": resource_id,
        "resource_name": resource_name,
        "resource_filename": f"file.{extension}",
        "ckan_last_modified": ckan_last_modified,
        "ckan_format": ckan_format,
        "body_sha256": sha,
    }
    if additive:
        params.update(
            {
                "capture_family": dataset_dir.name,
                "url_type": "upload",
                "empty_capture": False,
                "declared_content_length": len(body),
            }
        )
    meta = {
        "source": "neso_data_portal",
        "dataset": dataset_dir.name,
        "fetched_at": fetched.isoformat(),
        "written_at": written.isoformat(),
        "data_date": "2026-10-07",
        "request_url": f"https://api.neso.energy/dataset/{package_id}/resource/{resource_id}"
        f"/download/file.{extension}",
        "request_params": params,
        "api_version": "3",
        "http_status": 200,
        "content_type": "text/csv",
        "body_sha256": sha,
        "body_size_bytes": len(body),
        "page": 1,
        "total_pages": 1,
    }
    sidecar = directory / f"{stem}.meta.json"
    sidecar.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return body_path, sidecar


def edit_sidecar(sidecar: Path, mutate: Any) -> None:
    """Load, mutate in place via ``mutate(meta)``, and rewrite a test sidecar."""
    meta = json.loads(sidecar.read_text(encoding="utf-8"))
    mutate(meta)
    sidecar.write_text(json.dumps(meta, indent=2), encoding="utf-8")
