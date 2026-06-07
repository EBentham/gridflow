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
        except Exception:
            # Non-fatal: a tracking-DB hiccup must not kill the operation. But a
            # missing 'running' row means later status transitions UPDATE nothing,
            # so make the lost insert loud rather than swallowing it to a warning.
            logger.error(
                "Failed to record pipeline start; run will be untracked "
                "(no 'running' row): run_id=%s operation=%s source=%s dataset=%s",
                self.run_id,
                self.operation,
                self.source,
                self.dataset,
                exc_info=True,
            )

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
        except Exception:
            # Non-fatal: an otherwise-successful run must not fail on a telemetry
            # write. But the lost transition means the row stays 'running' — log
            # that explicitly so the stuck run is visible.
            logger.error(
                "Failed to record pipeline completion; run remains 'running': "
                "run_id=%s operation=%s source=%s dataset=%s",
                self.run_id,
                self.operation,
                self.source,
                self.dataset,
                exc_info=True,
            )

    def complete_with_warnings(
        self,
        rows_in: int = 0,
        rows_out: int = 0,
        rows_skipped: int = 0,
    ) -> None:
        """Record completion of a run that wrote rows but hit recoverable warnings.

        Identical to :meth:`complete` except the terminal ``status`` is
        ``'completed_with_warnings'`` rather than ``'success'``. Used when a
        transform finished and wrote rows but encountered >=1 unmapped enum code
        (ADR-022): the rows survive with a sentinel label, ``rows_skipped`` carries
        the unmapped count, and the run is distinguished from both a clean
        ``'success'`` and a hard ``'failed'``. ``pipeline_runs.status`` is an
        unconstrained VARCHAR, so this needs no schema change.
        """
        now = datetime.now(UTC)
        duration = (now - self.started_at).total_seconds()
        try:
            self.con.execute(
                """
                UPDATE pipeline_runs
                SET status='completed_with_warnings', completed_at=?, rows_in=?,
                    rows_out=?, rows_skipped=?, duration_seconds=?
                WHERE run_id = ?
                """,
                [now, rows_in, rows_out, rows_skipped, duration, self.run_id],
            )
        except Exception:
            # Non-fatal; but the lost transition leaves the row 'running'.
            logger.error(
                "Failed to record pipeline completion-with-warnings; run remains "
                "'running': run_id=%s operation=%s source=%s dataset=%s",
                self.run_id,
                self.operation,
                self.source,
                self.dataset,
                exc_info=True,
            )

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
        except Exception:
            # Non-fatal; but the lost transition leaves the row 'running' even
            # though the operation actually failed — doubly misleading, so ERROR.
            logger.error(
                "Failed to record pipeline failure; run remains 'running' despite "
                "the failure: run_id=%s operation=%s source=%s dataset=%s "
                "original_error=%s",
                self.run_id,
                self.operation,
                self.source,
                self.dataset,
                error[:500],
                exc_info=True,
            )


def _to_naive_utc(dt: datetime) -> datetime:
    """Return ``dt`` as a naive UTC datetime for tz-machinery-free DuckDB storage.

    The ``pipeline_watermarks`` columns are plain ``TIMESTAMP`` so DuckDB never
    invokes its named-timezone path (which requires pytz/ICU, absent in minimal
    environments such as CI). Callers pass tz-aware UTC; the tz-aware-UTC contract
    is re-established on read in ``get_watermark``.
    """
    if dt.tzinfo is not None:
        dt = dt.astimezone(UTC)
    return dt.replace(tzinfo=None)


def update_watermark(
    con: duckdb.DuckDBPyConnection,
    source: str,
    dataset: str,
    last_end: datetime,
) -> None:
    """Advance the pipeline watermark for incremental ingestion (monotonic).

    Args:
        con: Open DuckDB connection.
        source: Data source name (e.g. ``"elexon"``).
        dataset: Dataset name (e.g. ``"fuelhh"``).
        last_end: The requested window end of a *successful* ingest, tz-aware UTC.

    The upsert is monotonic — ``GREATEST(existing, excluded)`` — so an
    out-of-order or backfill ingest can never rewind the frontier backward
    (C3-11). A telemetry-write failure is swallowed to a warning: a watermark
    hiccup must not fail an otherwise-successful ingest.
    """
    now = _to_naive_utc(datetime.now(UTC))
    try:
        # Monotonic upsert: only ever move the frontier forward.
        con.execute(
            """
            INSERT INTO pipeline_watermarks (source, dataset, last_end, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (source, dataset) DO UPDATE
            SET last_end = GREATEST(pipeline_watermarks.last_end, excluded.last_end),
                updated_at = excluded.updated_at
            """,
            [source, dataset, _to_naive_utc(last_end), now],
        )
    except Exception as e:
        logger.warning(f"Failed to update watermark: {e}")


def get_watermark(
    con: duckdb.DuckDBPyConnection,
    source: str,
    dataset: str,
) -> datetime | None:
    """Return the last watermark for a source/dataset pair, or ``None``.

    Args:
        con: Open DuckDB connection.
        source: Data source name.
        dataset: Dataset name.

    Returns:
        The stored ``last_end`` as a tz-aware UTC datetime, or ``None`` if no
        watermark exists for the pair.

    ``last_end`` is stored as a naive UTC ``TIMESTAMP``; re-attaching ``UTC`` here
    restores the tz-aware-UTC contract (CLAUDE.md hard rule) without invoking
    DuckDB's named-timezone machinery.
    """
    try:
        result = con.execute(
            """
            SELECT last_end FROM pipeline_watermarks
            WHERE source = ? AND dataset = ?
            """,
            [source, dataset],
        ).fetchone()
        if result and result[0] is not None:
            return result[0].replace(tzinfo=UTC)
    except Exception as e:
        logger.debug(f"Could not read watermark: {e}")
    return None
