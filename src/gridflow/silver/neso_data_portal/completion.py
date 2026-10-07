"""Capture context and the completion ledger of the NESO generic engine (ADR-034).

**Capture context (P-2).** Everything the engine needs to know about one bronze
capture comes from its one sidecar and the family's record:
:func:`capture_context` reads it once. No vintage value is ever defaulted from
a clock.

**Capture identity.** A capture's id is its body's data-root-relative POSIX
path (``bronze/neso_data_portal/<key>/YYYY/MM/DD/raw_….csv``); its partition
date is that bronze date directory.

**Completion ledger (P-7).** One Parquet row per (capture, family) under
``state/neso_data_portal/completion/<family>/<sha256(capture id)[:32]>.parquet``
and, for a failed attempt, one JSON record under ``completion_failures/``.
``state/`` survives ``gridflow reset``. Write order per pair: output Parquet,
then the completion record (both atomic ``write_parquet``), then the unlink of
the pair's failure record; a failure writes only its failure record and never
removes an earlier output or completion. Every field derives from bronze plus
the record (no run id, no ``recorded_at``), so a rewrite is equal (B7).

**Validity (the one predicate).** :func:`is_valid` is used by the generic
engine's skip-if-valid, the bespoke hook's adoption, and reconcile.

**Bespoke completion (P-8).** The three legacy transformers are untouched; a
post-run hook (:func:`record_bespoke_completions`) records each of their
outputs that passes :func:`is_valid`, and the drain's per-capture step
(:func:`run_bespoke_capture`) adopts or re-transforms one capture.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import polars as pl

from gridflow.silver.base import append_only_run_stamp
from gridflow.silver.neso_data_portal._bronze import provenance_for
from gridflow.storage.parquet import write_parquet
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.connectors.neso_data_portal.captures import Capture
    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord
    from gridflow.silver.base import BaseSilverTransformer

logger = logging.getLogger(__name__)

__all__ = [
    "BESPOKE_ENGINE",
    "COMPLETION_COLUMNS",
    "COMPLETION_RELATION",
    "COMPLETION_SCHEMA",
    "CaptureContext",
    "CaptureContextError",
    "NesoCaptureFailedError",
    "Versions",
    "bespoke_versions",
    "capture_context",
    "capture_id_for",
    "completion_path",
    "completion_row",
    "failure_path",
    "is_valid",
    "partition_date_of",
    "read_completion",
    "read_failure",
    "record_bespoke_completions",
    "record_completion",
    "run_bespoke_capture",
    "scan_completions",
    "write_failure",
]

SOURCE = "neso_data_portal"
BESPOKE_ENGINE = "bespoke"
COMPLETION_RELATION = "state_neso_data_portal_completion"
"""The DuckDB relation over every completion record (ADR-034 P-11)."""

_TS = pl.Datetime("us", "UTC")
COMPLETION_COLUMNS: tuple[tuple[str, pl.DataType], ...] = (
    ("family", pl.Utf8()),
    ("bronze_capture_id", pl.Utf8()),
    ("source_key", pl.Utf8()),
    ("partition_date", pl.Date()),
    ("resource_id", pl.Utf8()),
    ("body_sha256", pl.Utf8()),
    ("capture_written_at", _TS),
    ("published_at", _TS),
    ("available_at", _TS),
    ("outcome", pl.Utf8()),
    ("row_count", pl.Int64()),
    ("rows_excluded", pl.Int64()),
    ("output_path", pl.Utf8()),
    ("children", pl.List(pl.Utf8())),
    ("record_version", pl.Utf8()),
    ("engine_version", pl.Utf8()),
)
"""The one ordered ``(name, dtype)`` schema of a completion record (P-7)."""

COMPLETION_SCHEMA = pl.Schema(COMPLETION_COLUMNS)


class CaptureContextError(Exception):
    """The capture's sidecar cannot supply what its record's vintage needs."""


class NesoCaptureFailedError(Exception):
    """One or more captures of a (family, date) failed; the others were written.

    Attributes:
        failures: ``(capture id, error class, message)`` per failed capture.
    """

    def __init__(self, family: str, target: date, failures: list[tuple[str, str, str]]) -> None:
        self.failures = list(failures)
        detail = "; ".join(f"{cid}: {cls}: {msg}" for cid, cls, msg in self.failures[:5])
        more = f" (+{len(self.failures) - 5} more)" if len(self.failures) > 5 else ""
        super().__init__(
            f"{SOURCE}/{family} {target.isoformat()}: {len(self.failures)} capture(s) failed "
            f"and were not written: {detail}{more}"
        )


@dataclass(frozen=True)
class CaptureContext:
    """What one capture's sidecar says, as the engine needs it.

    Attributes:
        capture_id: The body's data-root-relative POSIX path.
        partition_date: The body's bronze date directory.
        body: The absolute body path.
        sidecar: The absolute sidecar path.
        capture_written_at: The sidecar ``written_at``, UTC.
        resource_id: The CKAN resource UUID.
        resource_filename: The vendor filename (for a token issue time).
        url_type: ``upload``/``datastore``, or ``None`` on a legacy sidecar.
        body_sha256: The recorded body digest.
        empty_capture: Unit A's header-only marker (absent reads ``False``).
        published_at: The capture-level vendor clock: CKAN ``last_modified``
            under ``ckan_last_modified``, else ``None`` (per-row under
            ``issue_time_evidenced``).
    """

    capture_id: str
    partition_date: date
    body: Path
    sidecar: Path
    capture_written_at: datetime
    resource_id: str
    resource_filename: str
    url_type: str | None
    body_sha256: str
    empty_capture: bool
    published_at: datetime | None


@dataclass(frozen=True)
class Versions:
    """The versions a completion record must carry to be valid now.

    Attributes:
        record_version: Generic: ``record.version``; bespoke: ``DATASET_VERSION``.
        engine_version: Generic: ``ENGINE_VERSION``; bespoke: ``"bespoke"``.
        dataset_version: The ``dataset_version`` every output row must carry.
        generic: Whether the output must hold exactly the record's capture id.
    """

    record_version: str
    engine_version: str
    dataset_version: str
    generic: bool


def capture_id_for(body: Path, data_dir: Path) -> str:
    """Return the capture id: ``body`` relative to ``data_dir``, POSIX."""
    return body.relative_to(data_dir).as_posix()


def partition_date_of(body: Path) -> date:
    """Return the bronze date directory ``YYYY/MM/DD`` holding ``body``."""
    day, month, year = body.parent.name, body.parent.parent.name, body.parent.parent.parent.name
    return date(int(year), int(month), int(day))


def capture_context(capture: Capture, record: SchemaRecord, data_dir: Path) -> CaptureContext:
    """Read one capture's context from its sidecar (P-2).

    Args:
        capture: A usable capture from ``scan_dataset``.
        record: The family's record (its vintage recipe decides
            ``published_at``).
        data_dir: The data root the capture id is relative to.

    Returns:
        The capture's context.

    Raises:
        CaptureContextError: Under ``ckan_last_modified``, a ``datastore``
            sidecar (decision 9) or a ``last_modified`` that
            ``provenance_for`` rejects.
    """
    meta: Any = json.loads(capture.sidecar.read_text(encoding="utf-8"))
    params: dict[str, Any] = meta.get("request_params") or {}
    capture_id = capture_id_for(capture.body, data_dir)
    raw_url_type = params.get("url_type")
    url_type = raw_url_type if isinstance(raw_url_type, str) else None
    published_at: datetime | None = None
    if record.vintage == "ckan_last_modified":
        if url_type == "datastore":
            raise CaptureContextError(
                f"{capture_id}: a datastore capture has no file last_modified; the record's "
                "vintage ckan_last_modified cannot apply (decision 9)"
            )
        provenance = provenance_for(capture.body)
        if provenance is None:
            raise CaptureContextError(
                f"{capture_id}: the sidecar carries no usable ckan_last_modified, which the "
                "record's vintage requires; no clock is substituted"
            )
        published_at = provenance.published_at
    return CaptureContext(
        capture_id=capture_id,
        partition_date=partition_date_of(capture.body),
        body=capture.body,
        sidecar=capture.sidecar,
        capture_written_at=capture.written_at.astimezone(UTC),
        resource_id=capture.resource_id,
        resource_filename=str(params.get("resource_filename", "")),
        url_type=url_type,
        body_sha256=capture.body_sha256,
        empty_capture=params.get("empty_capture") is True,
        published_at=published_at,
    )


def bespoke_versions(cls: type[BaseSilverTransformer]) -> Versions:
    """The versions a legacy (bespoke) transformer's completions are judged by."""
    return Versions(cls.DATASET_VERSION, BESPOKE_ENGINE, cls.DATASET_VERSION, generic=False)


def _stem(capture_id: str) -> str:
    return hashlib.sha256(capture_id.encode("utf-8")).hexdigest()[:32]


def completion_path(data_dir: Path, family: str, capture_id: str) -> Path:
    """Return the completion record path of (capture, family)."""
    return PathBuilder(data_dir).completion_dir(SOURCE, family) / f"{_stem(capture_id)}.parquet"


def failure_path(data_dir: Path, family: str, capture_id: str) -> Path:
    """Return the failure record path of (capture, family)."""
    directory = PathBuilder(data_dir).completion_failure_dir(SOURCE, family)
    return directory / f"{_stem(capture_id)}.json"


def completion_row(
    *,
    family: str,
    capture_id: str,
    source_key: str,
    partition_date: date,
    resource_id: str,
    body_sha256: str,
    capture_written_at: datetime,
    published_at: datetime | None,
    outcome: str,
    row_count: int,
    rows_excluded: int,
    output_path: str | None,
    children: list[str],
    versions: Versions,
) -> dict[str, Any]:
    """Build one completion record; ``available_at = coalesce(published_at, written)``."""
    written = capture_written_at.astimezone(UTC)
    published = published_at.astimezone(UTC) if published_at is not None else None
    return {
        "family": family,
        "bronze_capture_id": capture_id,
        "source_key": source_key,
        "partition_date": partition_date,
        "resource_id": resource_id,
        "body_sha256": body_sha256,
        "capture_written_at": written,
        "published_at": published,
        "available_at": published if published is not None else written,
        "outcome": outcome,
        "row_count": row_count,
        "rows_excluded": rows_excluded,
        "output_path": output_path,
        "children": list(children),
        "record_version": versions.record_version,
        "engine_version": versions.engine_version,
    }


def record_completion(data_dir: Path, row: dict[str, Any]) -> Path:
    """Write the completion record, then unlink the pair's failure record (P-7)."""
    path = completion_path(data_dir, row["family"], row["bronze_capture_id"])
    write_parquet(pl.DataFrame([row], schema=COMPLETION_SCHEMA), path)
    failure_path(data_dir, row["family"], row["bronze_capture_id"]).unlink(missing_ok=True)
    return path


def write_failure(
    data_dir: Path, family: str, capture_id: str, partition_date: date, exc: BaseException
) -> Path:
    """Write the pair's failure record (atomic replace); never touches its output."""
    from gridflow.pipeline.runner import describe_exception

    path = failure_path(data_dir, family, capture_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    document = {
        "family": family,
        "bronze_capture_id": capture_id,
        "partition_date": partition_date.isoformat(),
        "error_class": type(exc).__name__,
        "message": describe_exception(exc),
    }
    temp = path.parent / f".tmp_{path.name}.{uuid4().hex[:16]}"
    temp.write_text(json.dumps(document, indent=2), encoding="utf-8")
    os.replace(temp, path)
    return path


def read_failure(data_dir: Path, family: str, capture_id: str) -> dict[str, Any] | None:
    """Return the pair's failure record, or ``None``."""
    path = failure_path(data_dir, family, capture_id)
    if not path.is_file():
        return None
    document: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return document


def read_completion(data_dir: Path, family: str, capture_id: str) -> dict[str, Any] | None:
    """Return the pair's completion record as a dict, or ``None``."""
    path = completion_path(data_dir, family, capture_id)
    if not path.is_file():
        return None
    rows = pl.read_parquet(path, hive_partitioning=False).to_dicts()
    return rows[0] if rows else None


def is_valid(row: dict[str, Any], data_dir: Path, versions: Versions) -> bool:
    """The one validity predicate of a completion record (P-7).

    Valid iff the record's ``(record_version, engine_version)`` are the
    current ones and: ``valid_empty`` has ``row_count == 0`` and no output;
    ``populated`` names an output that exists, holds ``row_count`` rows, every
    row of which carries the current ``dataset_version`` and (generic only)
    exactly this capture's id.

    Args:
        row: A completion record.
        data_dir: The data root ``output_path`` is relative to.
        versions: The current versions.

    Returns:
        Whether the record vouches for a complete, current output.
    """
    if (row["record_version"], row["engine_version"]) != (
        versions.record_version,
        versions.engine_version,
    ):
        return False
    if row["outcome"] == "valid_empty":
        return bool(row["row_count"] == 0 and row["output_path"] is None)
    if row["outcome"] != "populated" or row["output_path"] is None:
        return False
    path = data_dir / row["output_path"]
    if not path.is_file():
        return False
    try:
        frame = pl.scan_parquet(path, hive_partitioning=False)
        count = frame.select(pl.len()).collect().item()
        stored = set(frame.select(pl.col("dataset_version").unique()).collect().to_series())
        ids = (
            set(frame.select(pl.col("bronze_capture_id").unique()).collect().to_series())
            if versions.generic
            else {row["bronze_capture_id"]}
        )
    except (OSError, pl.exceptions.PolarsError):
        return False
    return bool(
        count == row["row_count"]
        and stored == {versions.dataset_version}
        and ids == {row["bronze_capture_id"]}
    )


def scan_completions(data_dir: Path, family: str | None = None) -> pl.LazyFrame:
    """Return every completion record (of ``family``), typed by :data:`COMPLETION_COLUMNS`.

    Args:
        data_dir: The data root.
        family: One family, or ``None`` for all.

    Returns:
        A lazy frame of that schema; empty when no record exists.
    """
    paths = PathBuilder(data_dir)
    root = (
        paths.completion_dir(SOURCE, family)
        if family is not None
        else paths.state_dir(SOURCE) / "completion"
    )
    files = sorted(root.rglob("[!.]*.parquet")) if root.is_dir() else []
    if not files:
        return pl.LazyFrame(schema=COMPLETION_SCHEMA)
    return pl.scan_parquet(files, hive_partitioning=False, schema=COMPLETION_SCHEMA)


# --------------------------------------------------------------------------- #
# Bespoke completion (P-8)
# --------------------------------------------------------------------------- #


def _bespoke_expected_path(transformer: BaseSilverTransformer, capture: Capture) -> Path | None:
    """The append-only path ``_write_silver`` gives this capture's output."""
    stamp = transformer._timestamp_from_sidecar(capture.sidecar)
    if stamp is None:
        return None
    target = partition_date_of(capture.body)
    out_dir = PathBuilder(transformer.data_dir).silver_partition_dir(
        transformer.source, transformer.dataset, target, dataset_dir=transformer.silver_dir
    )
    run_stamp = append_only_run_stamp(stamp)
    return out_dir / f"{transformer.dataset}_{target.strftime('%Y%m%d')}_run{run_stamp}.parquet"


def _bespoke_candidate(
    transformer: BaseSilverTransformer, capture: Capture, path: Path
) -> dict[str, Any] | None:
    """The ``populated`` record this capture's existing output would carry."""
    if not path.is_file():
        return None
    try:
        count = pl.scan_parquet(path, hive_partitioning=False).select(pl.len()).collect().item()
    except (OSError, pl.exceptions.PolarsError):
        return None
    provenance = provenance_for(capture.body)
    data_dir = transformer.data_dir
    return completion_row(
        family=transformer.dataset,
        capture_id=capture_id_for(capture.body, data_dir),
        source_key=transformer.dataset,
        partition_date=partition_date_of(capture.body),
        resource_id=capture.resource_id,
        body_sha256=capture.body_sha256,
        capture_written_at=capture.written_at,
        published_at=provenance.published_at if provenance is not None else None,
        outcome="populated",
        row_count=int(count),
        rows_excluded=0,
        output_path=path.relative_to(data_dir).as_posix(),
        children=[],
        versions=bespoke_versions(type(transformer)),
    )


def bespoke_targets(
    transformer: BaseSilverTransformer, target_date: date
) -> tuple[dict[Path, Capture], list[tuple[Path, list[Capture]]]]:
    """Map each usable capture of the date to its expected output path.

    Args:
        transformer: A legacy transformer instance.
        target_date: The bronze date directory.

    Returns:
        ``(unique, collisions)``: paths claimed by exactly one capture, and
        paths two or more captures resolve to (neither is recorded, RESEARCH §1).
    """
    from gridflow.connectors.neso_data_portal import registry as registry_module
    from gridflow.connectors.neso_data_portal.captures import scan_dataset

    scan = scan_dataset(
        transformer.bronze_dir, registry_module.load_registry(), partition=target_date
    )
    by_path: dict[Path, list[Capture]] = {}
    for capture in scan.captures:
        path = _bespoke_expected_path(transformer, capture)
        if path is not None:
            by_path.setdefault(path, []).append(capture)
    unique = {path: group[0] for path, group in by_path.items() if len(group) == 1}
    collisions = [(path, group) for path, group in by_path.items() if len(group) > 1]
    return unique, collisions


def record_bespoke_completions(transformer: BaseSilverTransformer, target_date: date) -> None:
    """Post-run hook: record each of the date's bespoke outputs that is valid (P-8).

    A capture whose expected output is absent, or fails :func:`is_valid`, gets
    no record (reconcile reports it). Two captures resolving to one output
    path are both left unrecorded and logged (reconcile: ``duplicated``).

    Args:
        transformer: The legacy transformer that just ran.
        target_date: The date it ran for.
    """
    unique, collisions = bespoke_targets(transformer, target_date)
    for path, group in collisions:
        logger.warning(
            "%s/%s: %d captures resolve to one output %s; none is recorded complete",
            SOURCE,
            transformer.dataset,
            len(group),
            path.name,
        )
    versions = bespoke_versions(type(transformer))
    for path, capture in unique.items():
        row = _bespoke_candidate(transformer, capture, path)
        if row is not None and is_valid(row, transformer.data_dir, versions):
            record_completion(transformer.data_dir, row)


def run_bespoke_capture(transformer: BaseSilverTransformer, body: Path, run_id: str) -> None:
    """The drain's step for one bespoke capture: adopt, else re-transform (P-8).

    A valid existing output is adopted (recorded, never rewritten). Otherwise
    exactly the per-file branch's body steps run (sidecar stamp, read,
    ``_process_frame``, ``_write_silver``, which replaces an outdated file at
    the same path), then the hook's step records it.

    Args:
        transformer: A legacy transformer instance over the data root.
        body: The capture's body path.
        run_id: The lineage run id stamped into a rewritten output.

    Raises:
        NesoCaptureFailedError: Anything failed; a failure record was written.
    """
    data_dir = transformer.data_dir
    capture_id = capture_id_for(body, data_dir)
    target = partition_date_of(body)
    try:
        unique, collisions = bespoke_targets(transformer, target)
        if any(body in (c.body for c in group) for _path, group in collisions):
            raise CaptureContextError(f"{capture_id}: shares its output path with another capture")
        match = [(path, capture) for path, capture in unique.items() if capture.body == body]
        if not match:
            raise CaptureContextError(f"{capture_id}: not a usable capture with a sidecar stamp")
        path, capture = match[0]
        versions = bespoke_versions(type(transformer))
        row = _bespoke_candidate(transformer, capture, path)
        if row is not None and is_valid(row, data_dir, versions):
            record_completion(data_dir, row)
            return
        available_at = transformer._timestamp_from_sidecar(capture.sidecar)
        if available_at is None:
            raise CaptureContextError(f"{capture_id}: the sidecar has no usable timestamp")
        raw_df = transformer.read_bronze_file(body)
        if raw_df.is_empty():
            raise CaptureContextError(f"{capture_id}: read as empty (provenance unusable)")
        window_plan = transformer._resolve_publication_window_plan(
            target
        ) or transformer._resolve_event_window_plan(target)
        clean_df = transformer._process_frame(raw_df, target, run_id, available_at, window_plan)
        del raw_df
        if clean_df is None:
            raise CaptureContextError(f"{capture_id}: transform produced no rows")
        transformer._write_silver(clean_df, target, available_at=available_at)
        del clean_df
        row = _bespoke_candidate(transformer, capture, path)
        if row is None or not is_valid(row, data_dir, versions):
            raise CaptureContextError(f"{capture_id}: the rewritten output does not validate")
        record_completion(data_dir, row)
    except Exception as exc:
        write_failure(data_dir, transformer.dataset, capture_id, target, exc)
        raise NesoCaptureFailedError(
            transformer.dataset, target, [(capture_id, type(exc).__name__, str(exc))]
        ) from exc
