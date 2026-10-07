"""Bronze layer writer — stores raw API responses with provenance metadata."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from gridflow.bronze.sanitize import sanitize_params, sanitize_url
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.connectors.base import RawResponse

logger = logging.getLogger(__name__)

# Canonical lowercase CKAN resource UUID (ADR-033 E2). Vendor data reaches a
# bronze filename only through this check.
_RESOURCE_ID_PATTERN = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

CAPTURE_EXTENSIONS: frozenset[str] = frozenset(
    {"csv", "txt", "xlsx", "xlsm", "zip", "docx", "pptx", "doc", "ppt", "xls", "pdf", "png"}
    | {"gpkg", "geojson"}
)
"""Every extension :meth:`BronzeWriter.publish_capture` accepts (ADR-033 P-7's union)."""


class BronzeCollisionError(FileExistsError):
    """A capture's final name already exists; the existing artifact was not touched."""


class BronzeWriter:
    """Writes raw API responses to the bronze layer with metadata sidecars."""

    def __init__(self, data_dir: Path):
        self.bronze_dir = data_dir / "bronze"
        self._paths = PathBuilder(data_dir)

    def write(self, response: RawResponse) -> Path:
        """Write a raw response to disk with metadata sidecar.

        Returns the path to the written data file.
        """
        body_hash = hashlib.sha256(response.body).hexdigest()[:8]
        ts = response.fetched_at.strftime("%Y%m%dT%H%M%SZ")
        ext = self._extension(response.content_type)

        # Build directory path — partition by data date when known, else ingestion date
        partition = (
            response.data_date if response.data_date is not None else response.fetched_at.date()
        )
        dir_path = (
            self.bronze_dir
            / response.source
            / response.dataset
            / str(partition.year)
            / f"{partition.month:02d}"
            / f"{partition.day:02d}"
        )
        dir_path.mkdir(parents=True, exist_ok=True)

        # Write data file atomically (temp + os.replace), then the sidecar.
        # Ordering matters: a crash must never leave a sidecar pointing at a
        # missing or torn body. `os.replace` is atomic on Unix and Windows
        # (never Path.rename on Windows). A torn body would otherwise be
        # swallowed by silver's per-file JSONDecodeError handler as silent row
        # loss, since bronze is irreproducible.
        filename = f"raw_{ts}_{body_hash}.{ext}"
        data_path = dir_path / filename
        # written_at marks completion of the durable bronze write. It is the
        # reingest availability anchor (see _timestamp_from_sidecar): unlike
        # fetched_at, which is stamped at RawResponse construction before any
        # paging/retries, written_at reflects when the row became durable.
        written_at = datetime.now(UTC)
        self._atomic_write_bytes(data_path, response.body)

        # Write metadata sidecar
        meta = {
            "source": response.source,
            "dataset": response.dataset,
            "fetched_at": response.fetched_at.isoformat(),
            "written_at": written_at.isoformat(),
            "data_date": response.data_date.isoformat() if response.data_date is not None else None,
            # Mask credentials before they reach the irreproducible sidecar; the
            # key is kept (presence recorded), only the value is redacted.
            "request_url": sanitize_url(response.request_url),
            "request_params": sanitize_params(response.request_params),
            "api_version": response.api_version,
            "http_status": response.http_status,
            "content_type": response.content_type,
            "body_sha256": hashlib.sha256(response.body).hexdigest(),
            "body_size_bytes": len(response.body),
            "page": response.page,
            "total_pages": response.total_pages,
        }
        meta_path = dir_path / f"raw_{ts}_{body_hash}.meta.json"
        self._atomic_write_text(meta_path, json.dumps(meta, indent=2, default=str))

        logger.info(
            f"Bronze write: {response.source}/{response.dataset} "
            f"-> {data_path.name} ({len(response.body)} bytes)"
        )
        return data_path

    def publish_capture(self, response: RawResponse, *, extension: str) -> Path:
        """Publish one member capture without ever replacing an existing file (ADR-033 P-9).

        The name carries the resource id, so two resources with identical bytes
        fetched in the same second publish to distinct paths. Each file is
        written to a unique temp, fsynced, then hard-linked to its final name:
        ``os.link`` refuses an existing target atomically, where ``os.replace``
        would silently clobber it. The sidecar is published last and is the
        commit marker — a body without a sidecar is an orphan, not a capture.

        :meth:`write` is unchanged; this is a separate method so no other
        source's bronze naming can move.

        Args:
            response: The captured member. ``request_params["resource_id"]``
                must be a canonical lowercase UUID.
            extension: The admitted extension (one of :data:`CAPTURE_EXTENSIONS`).

        Returns:
            The published body path.

        Raises:
            ValueError: A non-UUID resource id or a non-allowlisted extension.
            BronzeCollisionError: The body or sidecar name already exists.
            OSError: Any other filesystem failure. There is no fallback to
                ``os.replace``.
        """
        resource_id = response.request_params.get("resource_id")
        if not isinstance(resource_id, str) or not _RESOURCE_ID_PATTERN.fullmatch(resource_id):
            raise ValueError(
                f"publish_capture: resource_id {resource_id!r} is not a canonical UUID"
            )
        if extension not in CAPTURE_EXTENSIONS:
            raise ValueError(f"publish_capture: extension {extension!r} is not allowlisted")

        body_sha256 = hashlib.sha256(response.body).hexdigest()
        ts = response.fetched_at.strftime("%Y%m%dT%H%M%SZ")
        partition = (
            response.data_date if response.data_date is not None else response.fetched_at.date()
        )
        dir_path = self._paths.bronze_date_dir(response.source, response.dataset, partition)
        dir_path.mkdir(parents=True, exist_ok=True)

        data_path = dir_path / f"raw_{ts}_{resource_id}_{body_sha256[:8]}.{extension}"
        self._publish_no_clobber(data_path, response.body)
        # Stamped after the body's link, so it marks when the body became durable.
        written_at = datetime.now(UTC)
        meta: dict[str, Any] = {
            "source": response.source,
            "dataset": response.dataset,
            "fetched_at": response.fetched_at.isoformat(),
            "written_at": written_at.isoformat(),
            "data_date": response.data_date.isoformat() if response.data_date is not None else None,
            "request_url": sanitize_url(response.request_url),
            "request_params": sanitize_params(response.request_params),
            "api_version": response.api_version,
            "http_status": response.http_status,
            "content_type": response.content_type,
            "body_sha256": body_sha256,
            "body_size_bytes": len(response.body),
            "page": response.page,
            "total_pages": response.total_pages,
        }
        meta_path = data_path.with_suffix(".meta.json")
        self._publish_no_clobber(meta_path, json.dumps(meta, indent=2, default=str).encode("utf-8"))

        logger.info(
            "Bronze publish: %s/%s -> %s (%d bytes)",
            response.source,
            response.dataset,
            data_path.name,
            len(response.body),
        )
        return data_path

    @staticmethod
    def _publish_no_clobber(path: Path, data: bytes) -> None:
        """Write ``data`` to a unique temp, fsync, then ``os.link`` it to ``path``.

        The temp suffix is 12 hex digits, not a whole UUID: a capture name is
        already ~80 characters, and Windows without long-path support refuses
        a path over 260. ``open(..., "xb")`` refuses a colliding temp anyway.
        """
        tmp_path = path.parent / f".tmp_{path.name}.{uuid4().hex[:12]}"
        try:
            with open(tmp_path, "xb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            try:
                os.link(tmp_path, path)
            except FileExistsError:
                raise BronzeCollisionError(
                    f"bronze capture {path} already exists; refusing to replace it"
                ) from None
        finally:
            tmp_path.unlink(missing_ok=True)

    @staticmethod
    def _atomic_write_bytes(path: Path, data: bytes) -> None:
        """Write bytes atomically via a temp file + os.replace."""
        tmp_path = path.parent / f".tmp_{path.name}"
        tmp_path.write_bytes(data)
        os.replace(tmp_path, path)

    @staticmethod
    def _atomic_write_text(path: Path, text: str) -> None:
        """Write text atomically via a temp file + os.replace."""
        tmp_path = path.parent / f".tmp_{path.name}"
        tmp_path.write_text(text)
        os.replace(tmp_path, path)

    @staticmethod
    def _extension(content_type: str) -> str:
        """Map content type to file extension."""
        mapping = {
            "application/json": "json",
            "text/xml": "xml",
            "application/xml": "xml",
            "text/csv": "csv",
        }
        # Handle content types with charset, e.g. "application/json; charset=utf-8"
        base_type = content_type.split(";")[0].strip()
        return mapping.get(base_type, "bin")
