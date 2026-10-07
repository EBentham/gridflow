"""Capture context and the completion ledger of the NESO generic engine (ADR-034).

**Capture context (P-2).** Everything the engine needs to know about one bronze
capture comes from its one sidecar and the family's record:
:func:`capture_context` reads it once. No vintage value is ever defaulted from
a clock.

**Capture identity.** A capture's id is its body's data-root-relative POSIX
path (``bronze/neso_data_portal/<key>/YYYY/MM/DD/raw_….csv``); its partition
date is that bronze date directory.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

from gridflow.silver.neso_data_portal._bronze import provenance_for

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.connectors.neso_data_portal.captures import Capture
    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord

__all__ = [
    "CaptureContext",
    "CaptureContextError",
    "capture_context",
    "capture_id_for",
    "partition_date_of",
]


class CaptureContextError(Exception):
    """The capture's sidecar cannot supply what its record's vintage needs."""


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
    url_type = params.get("url_type") if isinstance(params.get("url_type"), str) else None
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
