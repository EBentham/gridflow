"""Pipeline run tracking for observability."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Literal, cast

if TYPE_CHECKING:
    import duckdb

logger = logging.getLogger(__name__)


class WatermarkOutcome(StrEnum):
    """The mechanically-defined result of a frontier write attempt (D-21/D-24)."""

    ADVANCED = "advanced"
    NO_OP = "no_op"
    CAS_MISMATCH = "cas_mismatch"
    WRITE_FAILED = "write_failed"


@dataclass(frozen=True)
class WatermarkWrite:
    """Outcome of a frontier write, whichever arm produced it (D-21).

    Attributes:
        outcome: What actually happened, mechanically defined (never inferred
            from rows-changed alone -- see D-24 for the seed/admin arm).
        observed: The CAS-mismatch diagnostic (D-20.4) -- the value re-read
            AFTER a failed write attempt. ``None`` when not applicable, or
            when the diagnostic re-read itself failed.
        error: Redacted write-failure detail (D-20.6). ``None`` unless
            ``outcome is WRITE_FAILED``.
    """

    outcome: WatermarkOutcome
    observed: datetime | None = None
    error: str | None = None


@dataclass(frozen=True)
class WatermarkRead:
    """A single watermark observation (D-10).

    Attributes:
        status: ``"present"`` (a row exists), ``"absent"`` (no row for this
            pair), or ``"unreadable"`` (the read itself failed -- NEVER
            treated as ``"absent"``; an unknown frontier fails closed).
        value: The stored ``last_end`` as tz-aware UTC, when ``present``.
        error: The raw exception text when ``unreadable``, else ``None``.
    """

    status: Literal["present", "absent", "unreadable"]
    value: datetime | None
    error: str | None = None


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
) -> WatermarkWrite:
    """Advance the pipeline watermark for incremental ingestion (monotonic).

    Args:
        con: Open DuckDB connection.
        source: Data source name (e.g. ``"elexon"``).
        dataset: Dataset name (e.g. ``"fuelhh"``).
        last_end: The requested window end of a *successful* ingest, tz-aware UTC.

    Returns:
        A :class:`WatermarkWrite`. ``outcome`` is classified by a pre-write
        ``SELECT`` (D-24): ``ADVANCED`` iff the stored ``last_end`` actually
        increased, else ``NO_OP`` -- even though ``updated_at`` is still
        bumped in the ``NO_OP`` case (existing, unchanged semantics). This
        classification is exact because the upsert's ``GREATEST`` guarantees
        ``final = max(prior, bind)``.

    The upsert itself is unconditional and monotonic — ``GREATEST(existing,
    excluded)`` — so an out-of-order or backfill ingest can never rewind the
    frontier backward (C3-11). This is the seed/admin arm: production code
    advances the frontier exclusively through :func:`advance_watermark` (the
    CAS, D-21/D-22); this function is retained for the 11 existing test call
    sites and as the arm a future admin ``watermark set`` command (Q-6) would
    deliberately use. A telemetry-write failure is swallowed to a WARNING (its
    own, pre-existing log line -- deliberately untouched, D-20.6): a watermark
    hiccup must not fail an otherwise-successful ingest.
    """
    now = _to_naive_utc(datetime.now(UTC))
    bind = _to_naive_utc(last_end)
    try:
        prior_row = con.execute(
            "SELECT last_end FROM pipeline_watermarks WHERE source = ? AND dataset = ?",
            [source, dataset],
        ).fetchone()
        prior = cast("datetime | None", prior_row[0]) if prior_row else None
        # Monotonic upsert: only ever move the frontier forward.
        con.execute(
            """
            INSERT INTO pipeline_watermarks (source, dataset, last_end, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (source, dataset) DO UPDATE
            SET last_end = GREATEST(pipeline_watermarks.last_end, excluded.last_end),
                updated_at = excluded.updated_at
            """,
            [source, dataset, bind, now],
        )
        outcome = (
            WatermarkOutcome.ADVANCED if prior is None or bind > prior else WatermarkOutcome.NO_OP
        )
        return WatermarkWrite(outcome=outcome)
    except Exception as e:
        logger.warning(f"Failed to update watermark: {e}")
        from gridflow.pipeline.runner import safe_error_message

        return WatermarkWrite(
            outcome=WatermarkOutcome.WRITE_FAILED, error=safe_error_message(str(e))
        )


def read_watermark(
    con: duckdb.DuckDBPyConnection,
    source: str,
    dataset: str,
) -> WatermarkRead:
    """Read the current watermark snapshot for a source/dataset pair (D-10).

    Args:
        con: Open DuckDB connection.
        source: Data source name.
        dataset: Dataset name.

    Returns:
        A :class:`WatermarkRead`: ``status="present"`` with a tz-aware UTC
        ``value`` when a row exists; ``status="absent"`` with ``value=None``
        when no row exists for the pair; ``status="unreadable"`` with the raw
        (unredacted) error text when the read itself failed. ``unreadable`` is
        NEVER conflated with ``absent`` -- callers must fail closed on it.

    ``last_end`` is stored as a naive UTC ``TIMESTAMP``; re-attaching ``UTC``
    here restores the tz-aware-UTC contract (CLAUDE.md hard rule) without
    invoking DuckDB's named-timezone machinery.
    """
    try:
        result = con.execute(
            """
            SELECT last_end FROM pipeline_watermarks
            WHERE source = ? AND dataset = ?
            """,
            [source, dataset],
        ).fetchone()
    except Exception as e:
        return WatermarkRead(status="unreadable", value=None, error=str(e))
    if result and result[0] is not None:
        # DuckDB fetchone() returns Any; last_end is a naive datetime here.
        value = cast("datetime", result[0]).replace(tzinfo=UTC)
        return WatermarkRead(status="present", value=value)
    return WatermarkRead(status="absent", value=None)


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
        watermark exists for the pair OR the read failed. Thin wrapper over
        :func:`read_watermark` -- contract unchanged from before this plan.
    """
    snapshot = read_watermark(con, source, dataset)
    return snapshot.value if snapshot.status == "present" else None


def _diagnostic_reread(
    con: duckdb.DuckDBPyConnection,
    source: str,
    dataset: str,
) -> datetime | None:
    """Best-effort post-mismatch read for the emitter's diagnostic detail (D-20.4).

    Runs strictly AFTER the write attempt and cannot influence the decision
    already made; a failure here is swallowed to ``None`` (the record prints
    ``unavailable``) so a telemetry hiccup on the diagnostic path never
    escalates.
    """
    snap = read_watermark(con, source, dataset)
    return snap.value if snap.status == "present" else None


def advance_watermark(
    con: duckdb.DuckDBPyConnection,
    source: str,
    dataset: str,
    last_end: datetime,
    *,
    expected: WatermarkRead,
) -> WatermarkWrite:
    """Compare-and-set the frontier: write only if ``expected`` is still true (D-20).

    Args:
        con: Open DuckDB connection.
        source: Data source name.
        dataset: Dataset name.
        last_end: The requested new frontier, tz-aware UTC.
        expected: The :class:`WatermarkRead` snapshot the caller's advance
            decision was made against -- the ONLY function production code
            uses to move the frontier (D-21/D-22; enforced by an AST pin,
            T1-q).

    Returns:
        A :class:`WatermarkWrite`. ``ADVANCED`` when the conditional write
        landed; ``NO_OP`` when ``expected`` already covers ``last_end`` (D-20.1,
        nothing written); ``CAS_MISMATCH`` when the stored row no longer
        matches ``expected`` (no write; ``.observed`` carries the post-write
        diagnostic re-read, D-20.4); ``WRITE_FAILED`` on any exception
        (``.error`` carries the redacted detail).

    Never raises, and never double-warns (D-20.6): any exception is logged at
    DEBUG only -- the runner's ONE aggregated record is the single WARNING for
    the pair (D-17/D-26).
    """
    try:
        if expected.status == "present":
            expected_value = expected.value
            if expected_value is None:
                # D-20.9: a contract-violating snapshot (read_watermark never
                # produces this) -- total-function guard, not an assert (an
                # assert is stripped under -O and would crash a safety path).
                return WatermarkWrite(outcome=WatermarkOutcome.CAS_MISMATCH)
            if _to_naive_utc(last_end) <= _to_naive_utc(expected_value):
                # D-20.1: nothing to advance -- write nothing, no rewind.
                return WatermarkWrite(outcome=WatermarkOutcome.NO_OP)
            now = _to_naive_utc(datetime.now(UTC))
            row = con.execute(
                """
                UPDATE pipeline_watermarks SET last_end = ?, updated_at = ?
                WHERE source = ? AND dataset = ? AND last_end = ?
                """,
                [
                    _to_naive_utc(last_end),
                    now,
                    source,
                    dataset,
                    _to_naive_utc(expected_value),
                ],
            ).fetchone()
            changed = row[0] if row else 0
            if changed == 1:
                return WatermarkWrite(outcome=WatermarkOutcome.ADVANCED)
            observed = _diagnostic_reread(con, source, dataset)
            return WatermarkWrite(outcome=WatermarkOutcome.CAS_MISMATCH, observed=observed)
        elif expected.status == "absent":
            now = _to_naive_utc(datetime.now(UTC))
            row = con.execute(
                """
                INSERT INTO pipeline_watermarks (source, dataset, last_end, updated_at)
                VALUES (?, ?, ?, ?) ON CONFLICT (source, dataset) DO NOTHING
                """,
                [source, dataset, _to_naive_utc(last_end), now],
            ).fetchone()
            changed = row[0] if row else 0
            if changed == 1:
                return WatermarkWrite(outcome=WatermarkOutcome.ADVANCED)
            observed = _diagnostic_reread(con, source, dataset)
            return WatermarkWrite(outcome=WatermarkOutcome.CAS_MISMATCH, observed=observed)
        else:
            # D-20.5: "unreadable" is unreachable in practice (both call sites
            # deny before reaching a write attempt), but the function is
            # total -- refuse rather than raise.
            return WatermarkWrite(outcome=WatermarkOutcome.CAS_MISMATCH)
    except Exception as e:
        logger.debug("advance_watermark failed for %s/%s: %s", source, dataset, e)
        from gridflow.pipeline.runner import safe_error_message

        return WatermarkWrite(
            outcome=WatermarkOutcome.WRITE_FAILED, error=safe_error_message(str(e))
        )


def ingest_runs_since(
    con: duckdb.DuckDBPyConnection,
    source: str,
    dataset: str,
    since: datetime,
) -> int | None:
    """Count ``ingest`` runs for a pair started after ``since`` (diagnostic only).

    Args:
        con: Open DuckDB connection.
        source: Data source name.
        dataset: Dataset name.
        since: A tz-aware UTC anchor (typically the frontier) -- runs strictly
            after this instant are counted.

    Returns:
        The run count, or ``None`` on any failure (the record prints
        ``unknown``; the ingest itself is unaffected).

    ``pipeline_runs.started_at`` is ``TIMESTAMP WITH TIME ZONE``; reading it
    back into Python needs pytz/ICU, which CI lacks. This compares
    ``epoch_us(started_at)`` (a plain integer) against an integer bound
    instead, so no ``TIMESTAMPTZ`` value ever crosses into Python.
    """
    try:
        bound_us = int(since.timestamp() * 1_000_000)
        result = con.execute(
            """
            SELECT count(*) FROM pipeline_runs
            WHERE source = ? AND dataset = ? AND operation = 'ingest'
              AND epoch_us(started_at) > ?
            """,
            [source, dataset, bound_us],
        ).fetchone()
        return int(result[0]) if result else None
    except Exception as e:
        logger.debug(f"Could not count ingest runs since {since}: {e}")
        return None
