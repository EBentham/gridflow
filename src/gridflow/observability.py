"""Pipeline run tracking for observability."""

from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger(__name__)


class PipelineRunTracker:
    """Tracks pipeline run metadata in DuckDB."""

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        source: str,
        dataset: str,
        operation: str,
    ):
        self.con = con
        self.run_id = str(uuid.uuid4())
        self.source = source
        self.dataset = dataset
        self.operation = operation
        self.started_at = datetime.now(UTC)
        self._record_start()

    def _record_start(self) -> None:
        """Record the start of a pipeline run."""
        try:
            self.con.execute(
                """
                INSERT INTO pipeline_runs
                    (run_id, source, dataset, operation, started_at, status)
                VALUES (?, ?, ?, ?, ?, 'running')
                """,
                [
                    self.run_id,
                    self.source,
                    self.dataset,
                    self.operation,
                    self.started_at,
                ],
            )
        except Exception as e:
            logger.warning(f"Failed to record pipeline start: {e}")

    def complete(
        self,
        rows_in: int = 0,
        rows_out: int = 0,
        rows_skipped: int = 0,
    ) -> None:
        """Record successful completion of a pipeline run."""
        now = datetime.now(UTC)
        duration = (now - self.started_at).total_seconds()
        try:
            self.con.execute(
                """
                UPDATE pipeline_runs
                SET status='success', completed_at=?, rows_in=?, rows_out=?,
                    rows_skipped=?, duration_seconds=?
                WHERE run_id = ?
                """,
                [now, rows_in, rows_out, rows_skipped, duration, self.run_id],
            )
        except Exception as e:
            logger.warning(f"Failed to record pipeline completion: {e}")

    def fail(self, error: str) -> None:
        """Record pipeline run failure."""
        now = datetime.now(UTC)
        duration = (now - self.started_at).total_seconds()
        try:
            self.con.execute(
                """
                UPDATE pipeline_runs
                SET status='failed', completed_at=?, duration_seconds=?, error_message=?
                WHERE run_id = ?
                """,
                [now, duration, error[:2000], self.run_id],
            )
        except Exception as e:
            logger.warning(f"Failed to record pipeline failure: {e}")


def update_watermark(
    con: duckdb.DuckDBPyConnection,
    source: str,
    dataset: str,
    last_end: datetime,
) -> None:
    """Update the pipeline watermark for incremental ingestion."""
    now = datetime.now(UTC)
    try:
        # Upsert watermark
        con.execute(
            """
            INSERT INTO pipeline_watermarks (source, dataset, last_end, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (source, dataset) DO UPDATE
            SET last_end = excluded.last_end, updated_at = excluded.updated_at
            """,
            [source, dataset, last_end, now],
        )
    except Exception as e:
        logger.warning(f"Failed to update watermark: {e}")


def get_watermark(
    con: duckdb.DuckDBPyConnection,
    source: str,
    dataset: str,
) -> datetime | None:
    """Get the last watermark for a source/dataset pair."""
    try:
        result = con.execute(
            """
            SELECT last_end FROM pipeline_watermarks
            WHERE source = ? AND dataset = ?
            """,
            [source, dataset],
        ).fetchone()
        if result:
            return result[0]
    except Exception as e:
        logger.debug(f"Could not read watermark: {e}")
    return None
