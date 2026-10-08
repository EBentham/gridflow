"""Shared builders for the NESO registry, capture and runner tests (ADR-033).

Not a test module (no ``test_`` prefix). Tests substitute a registry through
P-1's one seam: :func:`install_registry` monkeypatches
``registry.load_registry`` and ``endpoints.FAMILIES`` together.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from gridflow.connectors.neso_data_portal import endpoints
from gridflow.connectors.neso_data_portal import registry as registry_module

if TYPE_CHECKING:
    from collections.abc import Iterator
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
    record: dict[str, Any] | None = None,
    refresh: str = "daily",
) -> dict[str, Any]:
    """One family entry; ``record`` adds a frozen schema record (ADR-034 P-1)."""
    entry: dict[str, Any] = {
        "key": key,
        "kind": kind,
        "legacy": legacy,
        "archetype": archetype,
        "refresh": refresh,
        "empty_allowed": empty_allowed,
        "max_download_bytes": max_download_bytes,
        "name_regex": name_regex,
        "transformer": "bespoke" if legacy else None,
    }
    if record is not None:
        entry["record"] = record
    return entry


def column(
    source: str,
    name: str | None = None,
    dtype: str = "string",
    *,
    nullable: bool = True,
    **extra: Any,
) -> dict[str, Any]:
    """One ``ColumnSpec``; ``name`` defaults to the lower-cased source."""
    spec: dict[str, Any] = {
        "source": source,
        "name": name if name is not None else source.lower(),
        "dtype": dtype,
        "nullable": nullable,
    }
    if dtype == "date":
        spec["format"] = "%Y-%m-%d"
    spec.update(extra)
    return spec


def epoch(columns: list[dict[str, Any]], *, issue: dict[str, Any] | None = None) -> dict[str, Any]:
    """One ``HeaderEpoch``: the header is the columns' sources, in order."""
    return {
        "header": [spec["source"] for spec in columns],
        "columns": columns,
        "issue": issue if issue is not None else {"kind": "none"},
    }


def sp_columns() -> list[dict[str, Any]]:
    """The default sp_pair epoch: date, period, a string key and a value."""
    return [
        column("SettlementDate", "settlement_date", "date", nullable=False),
        column("SettlementPeriod", "settlement_period", "int64", nullable=False),
        column("Unit", "unit", nullable=False),
        column("Value", "value", "float64"),
    ]


def record(
    *,
    epochs: list[dict[str, Any]] | None = None,
    temporal: dict[str, Any] | None = None,
    entity_key: tuple[str, ...] = ("settlement_date", "settlement_period", "unit"),
    latest: str = "key_latest",
    vintage: str = "ckan_last_modified",
    reader: str = "csv",
    encoding: str = "utf-8",
    version: str = "1",
    run_type_column: str | None = None,
    siblings: tuple[str, ...] = (),
    vintage_evidence: str | None = None,
    xlsx: dict[str, Any] | None = None,
    zip_member: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """One frozen ``SchemaRecord``; the defaults are a valid sp_pair family.

    A ``zip_member`` record without a ``zip_member`` spec gets the permissive
    ``{"member_pattern": ".+", "inner": "csv"}`` (V-14), so unit B's container
    records keep loading.
    """
    if reader == "zip_member" and zip_member is None:
        zip_member = {"member_pattern": ".+", "inner": "csv"}
    document: dict[str, Any] = {
        "version": version,
        "reader": reader,
        "encoding": encoding,
        "epochs": epochs if epochs is not None else [epoch(sp_columns())],
        "temporal": temporal
        if temporal is not None
        else {
            "kind": "sp_pair",
            "date_column": "settlement_date",
            "period_column": "settlement_period",
        },
        "entity_key": list(entity_key),
        "latest": latest,
        "run_type_column": run_type_column,
        "siblings": list(siblings),
        "vintage": vintage,
        "vintage_evidence": vintage_evidence,
    }
    if xlsx is not None:
        document["xlsx"] = xlsx
    if zip_member is not None:
        document["zip_member"] = zip_member
    return document


def resource(
    resource_id: str,
    name: str,
    family_key: str,
    *,
    fmt: str = "CSV",
    url_type: str = "upload",
    disposition: dict[str, Any] | None = None,
    children: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """One resource entry; SILVER for CSV by default, DOC otherwise."""
    if disposition is None:
        disposition = {"kind": "SILVER", "key": family_key} if fmt == "CSV" else {"kind": "DOC"}
    entry: dict[str, Any] = {
        "id": resource_id,
        "name": name,
        "format": fmt,
        "url_type": url_type,
        "family": family_key,
        "disposition": disposition,
    }
    if children is not None:
        entry["children"] = children
    return entry


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


@contextlib.contextmanager
def ingest_context(data_dir: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """Yield a real ``PipelineContext`` over a tmp data dir and DuckDB catalogue."""
    from gridflow.config.settings import load_settings
    from gridflow.pipeline import runner as pipeline_runner
    from gridflow.storage.duckdb import get_connection, init_catalogue

    db_path = data_dir / "gridflow.duckdb"
    monkeypatch.setenv("GRIDFLOW_DATA_DIR", str(data_dir))
    monkeypatch.setenv("GRIDFLOW_DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("GRIDFLOW_LOG_DIR", str(data_dir / "logs"))
    monkeypatch.setattr("gridflow.storage.duckdb._register_gold_views", lambda con: None)
    for layer in ("bronze", "silver", "gold"):
        (data_dir / layer).mkdir(parents=True, exist_ok=True)
    settings = load_settings()
    init_catalogue(db_path, data_dir)
    con = get_connection(db_path)
    try:
        yield pipeline_runner.PipelineContext(con=con, settings=settings)
    finally:
        con.close()
