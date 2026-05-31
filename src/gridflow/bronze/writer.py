"""Bronze layer writer — stores raw API responses with provenance metadata."""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

    from gridflow.connectors.base import RawResponse

logger = logging.getLogger(__name__)


class BronzeWriter:
    """Writes raw API responses to the bronze layer with metadata sidecars."""

    def __init__(self, data_dir: Path):
        self.bronze_dir = data_dir / "bronze"

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
            "request_url": response.request_url,
            "request_params": response.request_params,
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
