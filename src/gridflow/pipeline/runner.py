"""UI-agnostic pipeline runner — the shared core behind the CLI and the scripts.

This module owns the *behavior* of the bronze / silver / gold pipeline: dataset
and date resolution, the per-dataset step loops, the run-tracker lifecycle,
error redaction before storage, view refresh, and watermark advancement. It
returns structured :class:`RunReport` results and raises only plain exceptions
(:class:`DatasetResolutionError`, :class:`NaiveDatetimeError`, ``TypeError``).

HARD RULE (keystone contract): this module imports no ``typer``/``argparse``,
never calls ``print``/``echo``, and never raises ``typer.Exit``. Adapters
(``gridflow.cli``, ``scripts/*``) translate results into output and exit codes.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator

    import duckdb

    from gridflow.config.settings import GridflowConfig
    from gridflow.connectors.base import BaseConnector, RawResponse

logger = logging.getLogger(__name__)

_CONNECTOR_MODULES = [
    "gridflow.connectors.elexon",
    "gridflow.connectors.openmeteo",
    "gridflow.connectors.entsoe",
    "gridflow.connectors.gie",
    "gridflow.connectors.entsog",
    "gridflow.connectors.neso",
]

_TRANSFORMER_MODULES = [
    "gridflow.silver.elexon",
    "gridflow.silver.openmeteo",
    "gridflow.silver.entsoe",
    "gridflow.silver.gie",
    "gridflow.silver.entsog",
    "gridflow.silver.neso",
]

# The single gold-dataset registry, shared by the runner and every adapter (was
# duplicated as an inline dict in cli.build and run_pipeline.run_gold). Kept as a
# name tuple here; ``_gold_builders()`` resolves the classes lazily so importing
# the runner never pulls in the gold build modules.
GOLD_DATASETS: tuple[str, ...] = ("system_marginal_price",)


class DatasetResolutionError(Exception):
    """Raised when no dataset can be resolved (no name and no ``--all`` flag).

    A plain exception so the runner stays UI-agnostic; the CLI adapter maps it
    to ``typer.BadParameter`` and the script adapters to a ``SystemExit``.
    """


class NaiveDatetimeError(Exception):
    """Raised when a ``--start``/``--end`` bound is a naive (offset-less) datetime.

    Distinct from the ``ValueError`` that :func:`datetime.fromisoformat` raises on
    malformed input, so the CLI adapter can translate *only* this to a
    ``typer.BadParameter`` without also swallowing genuine parse errors.
    """


# --------------------------------------------------------------------------- #
# Structured results
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class DatasetResult:
    """Outcome of one (source, dataset, operation) step.

    Attributes:
        source: Data source name (``"gold"`` for build steps).
        dataset: Dataset name.
        operation: ``"ingest"`` | ``"transform"`` | ``"build"``.
        status: ``"success"`` | ``"completed_with_warnings"`` | ``"failed"``.
        rows_in: Rows read (ingest: responses fetched).
        rows_out: Rows written.
        rows_skipped: Rows/units dropped with a recoverable warning (the SUM of
            ``rows_unmapped`` + ``rows_invalid`` for transforms; the skipped-unit
            count for ingests).
        rows_unmapped: Transform-only: rows kept with an ADR-022 enum sentinel.
        rows_invalid: Transform-only: rows failing full-frame schema validation.
        error: Pre-redacted error message when ``status == "failed"``, else None.
    """

    source: str
    dataset: str
    operation: str
    status: str
    rows_in: int = 0
    rows_out: int = 0
    rows_skipped: int = 0
    rows_unmapped: int = 0
    rows_invalid: int = 0
    error: str | None = None

    @property
    def ok(self) -> bool:
        """True when the step did not hard-fail (success or with-warnings)."""
        return self.status != "failed"


@dataclass(frozen=True)
class RunReport:
    """Aggregate outcome of a run (one or more dataset steps).

    Attributes:
        results: Every :class:`DatasetResult` produced, in execution order.
    """

    results: list[DatasetResult] = field(default_factory=list)

    @property
    def failed(self) -> list[DatasetResult]:
        """The subset of results whose status is ``"failed"``."""
        return [r for r in self.results if r.status == "failed"]

    @property
    def ok(self) -> bool:
        """True when no result hard-failed; adapters map ``not ok`` to exit 1."""
        return not self.failed


@dataclass
class PipelineContext:
    """Open resources shared across a run: the DuckDB connection + settings."""

    con: duckdb.DuckDBPyConnection
    settings: GridflowConfig


# --------------------------------------------------------------------------- #
# Registration (ONE logging-on-failure impl — was duplicated 3x)
# --------------------------------------------------------------------------- #


def import_connectors() -> None:
    """Import connector modules to trigger registration.

    These are core modules shipped with every install, so an ``ImportError``
    here never means "optional dependency absent" — it always indicates a real
    bug in the module. Log it loudly with the module name and keep going so the
    other connectors still register.
    """
    for module in _CONNECTOR_MODULES:
        try:
            __import__(module)
        except ImportError:
            logger.warning("Failed to import connector module %s", module, exc_info=True)


def import_transformers() -> None:
    """Import transformer modules to trigger registration.

    Core modules — an ``ImportError`` always signals a real bug, not a missing
    optional dependency. Log with the module name instead of swallowing it.
    """
    for module in _TRANSFORMER_MODULES:
        try:
            __import__(module)
        except ImportError:
            logger.warning("Failed to import transformer module %s", module, exc_info=True)


# --------------------------------------------------------------------------- #
# Date / dataset resolution (the CORRECT, tested impls — single source of truth)
# --------------------------------------------------------------------------- #


def _parse_window_bound(value: str) -> datetime:
    """Parse a ``--start``/``--end`` bound to a tz-aware UTC datetime.

    Offset-bearing strings are CONVERTED (``astimezone``), not relabelled; a
    bare calendar date (no time component) is taken as midnight UTC
    (unambiguous); a naive datetime (wall-clock time, no offset) is REJECTED
    with :class:`NaiveDatetimeError`. Silently relabelling a naive datetime as
    UTC — what ``.replace(tzinfo=...)`` did — shifts the ingest window by up to
    ±1h under BST (issue-19 site A).
    """
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        if "T" not in value and ":" not in value:
            return parsed.replace(tzinfo=UTC)
        raise NaiveDatetimeError(
            f"Naive datetime {value!r} has no timezone; pass an explicit offset "
            f"(e.g. '2026-02-01T00:00:00Z' or '...+01:00') or a bare date."
        )
    return parsed.astimezone(UTC)


def resolve_dates(
    start: str | None,
    end: str | None,
    last: str | None,
    default_lookback_hours: int,
) -> tuple[datetime, datetime]:
    """Parse date arguments into ``(start_dt, end_dt)`` UTC datetimes.

    Precedence: ``--last`` > ``--start``/``--end`` > default lookback. Offset
    bounds are converted to their UTC instant; naive bounds raise
    :class:`NaiveDatetimeError`; a bare date is midnight UTC.
    """
    now = datetime.now(UTC)
    if last:
        from gridflow.utils.time import parse_lookback

        delta = parse_lookback(last)
        return now - delta, now
    if start:
        start_dt = _parse_window_bound(start)
        end_dt = _parse_window_bound(end) if end else now
        return start_dt, end_dt
    return now - timedelta(hours=default_lookback_hours), now


def resolve_datasets(
    source: str,
    dataset: str | None,
    all_flag: bool,
    settings: object,
) -> list[str]:
    """Resolve which datasets to process for a source.

    Args:
        source: Data source name.
        dataset: A dataset name, the literal ``"all"`` (case-insensitive), or None.
        all_flag: The ``--all`` flag.
        settings: A :class:`~gridflow.config.settings.GridflowConfig`.

    Returns:
        The list of dataset names to process.

    Raises:
        TypeError: If ``settings`` is not a ``GridflowConfig``.
        DatasetResolutionError: If neither a dataset name nor ``--all`` is given.
    """
    from gridflow.config.settings import GridflowConfig

    if not isinstance(settings, GridflowConfig):
        raise TypeError("Expected GridflowConfig")

    if all_flag or (dataset is not None and dataset.lower() == "all"):
        source_config = settings.get_source_config(source)
        return list(source_config.datasets.keys())
    if dataset:
        return [dataset]
    raise DatasetResolutionError("Specify a dataset name or use --all")


def resolve_incremental_start(
    con: duckdb.DuckDBPyConnection,
    source: str,
    dataset: str,
    default_start: datetime,
    overlap: timedelta,
) -> datetime:
    """Resolve the per-dataset incremental start from the stored watermark.

    Args:
        con: Open DuckDB connection.
        source: Data source name.
        dataset: Dataset name.
        default_start: Fallback start used on the first run (no watermark yet).
        overlap: How far before the watermark to re-fetch, to recover late/revised
            publications. Zero is behaviour-preserving.

    Returns:
        ``watermark - overlap`` when a watermark exists for the pair, otherwise
        ``default_start``. Re-fetching the overlap window is safe: bronze is
        immutable and silver dedups on ``(date, period, run_type)``.
    """
    from gridflow.observability import get_watermark

    watermark = get_watermark(con, source, dataset)
    if watermark is None:
        return default_start
    return watermark - overlap


def safe_error_message(message: str) -> str:
    """Redact sensitive query parameters from a stored/displayed error message.

    Centralised here so EVERY adapter's stored ``pipeline_runs.error_message`` is
    clean (the cli used to do this; run_pipeline stored raw exceptions, leaking
    securityToken). The value class stops at the URL boundary (whitespace /
    closing paren) to avoid eating trailing prose.
    """
    from gridflow.bronze.sanitize import sanitize_url

    return sanitize_url(message, value_chars=r"[^&\s)]")


# --------------------------------------------------------------------------- #
# Context
# --------------------------------------------------------------------------- #


@contextmanager
def build_context(settings: GridflowConfig) -> Iterator[PipelineContext]:
    """Open a pipeline context: ensure the catalogue exists, yield an open con.

    Initialises the DuckDB catalogue (idempotent) and opens a read/write
    connection. The connection is closed on exit so a single ``with`` block can
    drive many step loops (e.g. all backfill chunks) on one connection — the
    Windows-safe way to reuse a connection across chunks without two RW handles
    on the same file.

    Args:
        settings: Loaded gridflow configuration.

    Yields:
        A :class:`PipelineContext` whose ``con`` is open for the block's lifetime.
    """
    from gridflow.storage.duckdb import get_connection, init_catalogue

    init_catalogue(settings.pipeline.duckdb_path, settings.pipeline.data_dir)
    con = get_connection(settings.pipeline.duckdb_path)
    try:
        yield PipelineContext(con=con, settings=settings)
    finally:
        con.close()


def refresh_views(settings: GridflowConfig) -> None:
    """Re-register all silver/gold views from the current filesystem state.

    Opens and closes its own connection (the documented invariant: never refresh
    while the run's read/write connection is open — two RW handles on one DuckDB
    file is the Windows lock-contention hazard). Call this AFTER the run's
    connection has been closed (or, within a ``build_context`` block, only once
    the block has exited).
    """
    from gridflow.storage.duckdb import refresh_views as _refresh

    _refresh(settings.pipeline.duckdb_path, settings.pipeline.data_dir)


# --------------------------------------------------------------------------- #
# Step loops
# --------------------------------------------------------------------------- #


def run_ingest(
    ctx: PipelineContext,
    source: str,
    datasets: list[str],
    start_dt: datetime,
    end_dt: datetime,
    *,
    incremental: bool = False,
    write_watermark: bool = True,
) -> list[DatasetResult]:
    """Ingest raw data from an API source into the bronze layer.

    Args:
        ctx: Open pipeline context.
        source: Data source name.
        datasets: Datasets to ingest.
        start_dt: Window start (the default-lookback start when ``incremental``).
        end_dt: Window end.
        incremental: Resolve each dataset's start from its stored watermark
            (first run falls back to ``start_dt``).
        write_watermark: Advance the watermark on success. Backfill passes False.

    Returns:
        One :class:`DatasetResult` per dataset, in input order.
    """
    from gridflow.bronze.writer import BronzeWriter
    from gridflow.connectors.registry import get_connector
    from gridflow.observability import PipelineRunTracker, update_watermark

    con = ctx.con
    settings = ctx.settings
    source_config = settings.get_source_config(source)
    writer = BronzeWriter(settings.pipeline.data_dir)
    overlap = timedelta(hours=settings.pipeline.incremental_overlap_hours)
    results: list[DatasetResult] = []

    for ds in datasets:
        # Resolve start per-dataset: incremental reads each dataset's own
        # watermark (first run falls back to the default-lookback start);
        # otherwise the explicit/lookback start applies to every dataset.
        if incremental:
            ds_start = resolve_incremental_start(con, source, ds, start_dt, overlap)
        else:
            ds_start = start_dt

        tracker = PipelineRunTracker(con, source, ds, "ingest")
        try:
            connector = get_connector(source, source_config)

            async def _do_fetch(
                connector: BaseConnector = connector,
                ds: str = ds,
                ds_start: datetime = ds_start,
            ) -> list[RawResponse]:
                async with connector:
                    return await connector.fetch(ds, ds_start, end_dt)

            responses = asyncio.run(_do_fetch())
            for resp in responses:
                writer.write(resp)
            # A partial fetch (some sub-units skipped after retries, not all) is
            # completed_with_warnings, never silent 'success' (CH-COR-01). The
            # counter persists on the connector instance past the async-with.
            skipped = connector.last_skipped_units
            if skipped:
                tracker.complete_with_warnings(
                    rows_in=len(responses),
                    rows_out=len(responses),
                    rows_skipped=skipped,
                )
                results.append(
                    DatasetResult(
                        source=source,
                        dataset=ds,
                        operation="ingest",
                        status="completed_with_warnings",
                        rows_in=len(responses),
                        rows_out=len(responses),
                        rows_skipped=skipped,
                    )
                )
            else:
                tracker.complete(rows_in=len(responses), rows_out=len(responses))
                results.append(
                    DatasetResult(
                        source=source,
                        dataset=ds,
                        operation="ingest",
                        status="success",
                        rows_in=len(responses),
                        rows_out=len(responses),
                    )
                )
            # Advance the watermark only AFTER a successful write. Never on the
            # except path; never for a backfill chunk-ingest (write_watermark=
            # False). The monotonic upsert is the second guard.
            if write_watermark:
                update_watermark(con, source, ds, end_dt)
        except Exception as e:  # noqa: BLE001 — surfaced as a failed DatasetResult, never swallowed
            error_message = safe_error_message(str(e))
            tracker.fail(error_message)
            logger.error("Ingest failed for %s/%s: %s", source, ds, error_message)
            results.append(
                DatasetResult(
                    source=source,
                    dataset=ds,
                    operation="ingest",
                    status="failed",
                    error=error_message,
                )
            )

    return results


def run_transform(
    ctx: PipelineContext,
    source: str,
    datasets: list[str],
    start_dt: datetime,
    end_dt: datetime,
    *,
    reingest: bool = False,
) -> list[DatasetResult]:
    """Transform bronze data to silver (normalised, validated, deduplicated).

    Args:
        ctx: Open pipeline context.
        source: Data source name.
        datasets: Datasets to transform.
        start_dt: Window start (date taken).
        end_dt: Window end (date taken, inclusive).
        reingest: Use bronze sidecar timestamps for historical available_at.

    Returns:
        One :class:`DatasetResult` per dataset, in input order.
    """
    from gridflow.observability import PipelineRunTracker
    from gridflow.silver.registry import get_transformer
    from gridflow.utils.time import date_range

    con = ctx.con
    settings = ctx.settings
    dates = date_range(start_dt.date(), end_dt.date())
    results: list[DatasetResult] = []

    for ds in datasets:
        tracker = PipelineRunTracker(con, source, ds, "transform")
        total_rows = 0
        total_unmapped = 0
        total_validation_failures = 0
        try:
            transformer = get_transformer(source, ds, settings.pipeline.data_dir)
            # CH3-02 (CH-PERF-02): per-date silver CSV is opt-in (default OFF).
            transformer.write_silver_csv = settings.pipeline.write_silver_csv
            for target_date in dates:
                rows = transformer.run(target_date, run_id=tracker.run_id, reingest=reingest)
                total_rows += rows
                # Per-date warning counts; run() resets both each call, so
                # accumulating never double-counts an empty/missing date.
                total_unmapped += transformer.last_unmapped_count
                total_validation_failures += transformer.last_validation_failure_count
            if total_unmapped or total_validation_failures:
                tracker.complete_with_warnings(
                    rows_out=total_rows,
                    rows_skipped=total_unmapped + total_validation_failures,
                )
                results.append(
                    DatasetResult(
                        source=source,
                        dataset=ds,
                        operation="transform",
                        status="completed_with_warnings",
                        rows_out=total_rows,
                        rows_skipped=total_unmapped + total_validation_failures,
                        rows_unmapped=total_unmapped,
                        rows_invalid=total_validation_failures,
                    )
                )
            else:
                tracker.complete(rows_out=total_rows)
                results.append(
                    DatasetResult(
                        source=source,
                        dataset=ds,
                        operation="transform",
                        status="success",
                        rows_out=total_rows,
                    )
                )
        except Exception as e:  # noqa: BLE001 — surfaced as a failed DatasetResult, never swallowed
            error_message = safe_error_message(str(e))
            tracker.fail(error_message)
            logger.error("Transform failed for %s/%s: %s", source, ds, error_message)
            results.append(
                DatasetResult(
                    source=source,
                    dataset=ds,
                    operation="transform",
                    status="failed",
                    error=error_message,
                )
            )

    return results


def run_build(
    ctx: PipelineContext,
    targets: list[str],
    start_dt: datetime,
    end_dt: datetime,
) -> list[DatasetResult]:
    """Build gold-layer modelling-ready datasets from silver.

    Unknown gold dataset names are skipped (a ``DatasetResult`` with status
    ``"failed"`` and an ``"Unknown gold dataset"`` error) — preserving cli.build,
    which echoes the unknown line and continues. A builder exception is also
    recorded as a failed result and the loop continues; the adapter decides the
    exit code (cli.build historically exits 0 even on builder failure).

    Args:
        ctx: Open pipeline context.
        targets: Gold dataset names to build.
        start_dt: Window start (date taken).
        end_dt: Window end (date taken).

    Returns:
        One :class:`DatasetResult` per requested target, in input order.
    """
    from gridflow.gold.system_marginal_price import SystemMarginalPriceBuilder
    from gridflow.observability import PipelineRunTracker

    con = ctx.con
    settings = ctx.settings
    gold_builders = {
        "system_marginal_price": SystemMarginalPriceBuilder,
    }
    assert set(gold_builders) == set(GOLD_DATASETS)  # noqa: S101 — registry/name-list drift guard
    results: list[DatasetResult] = []

    for name in targets:
        if name not in gold_builders:
            results.append(
                DatasetResult(
                    source="gold",
                    dataset=name,
                    operation="build",
                    status="failed",
                    error=f"Unknown gold dataset: {name}",
                )
            )
            continue

        tracker = PipelineRunTracker(con, "gold", name, "build")
        try:
            builder = gold_builders[name](settings.pipeline.data_dir)
            rows = builder.run(start_dt.date(), end_dt.date())
            tracker.complete(rows_out=rows)
            results.append(
                DatasetResult(
                    source="gold",
                    dataset=name,
                    operation="build",
                    status="success",
                    rows_out=rows,
                )
            )
        except Exception as e:  # noqa: BLE001 — surfaced as a failed DatasetResult, never swallowed
            error_message = safe_error_message(str(e))
            tracker.fail(error_message)
            logger.exception("Build failed for %s", name)
            results.append(
                DatasetResult(
                    source="gold",
                    dataset=name,
                    operation="build",
                    status="failed",
                    error=error_message,
                )
            )

    return results


# --------------------------------------------------------------------------- #
# Orchestrations
# --------------------------------------------------------------------------- #


def run_full(
    settings: GridflowConfig,
    source: str,
    datasets: list[str],
    start_dt: datetime,
    end_dt: datetime,
    *,
    gold_targets: list[str] | None = None,
    reingest: bool = False,
) -> RunReport:
    """Run the full pipeline: ingest -> transform (-> build) on one connection.

    Resolves dates ONCE (the caller passes concrete ``start_dt``/``end_dt``) and
    reuses a single DuckDB connection across all stages, then refreshes views
    once after the connection closes. Mirrors cli.pipeline's stage order.

    Note: unlike the standalone CLI commands, ``run_full`` does NOT abort the run
    when ingest fails — every stage runs and all results are collected. The
    adapter inspects :attr:`RunReport.ok` for the exit code. (cli.pipeline's old
    abort-on-bronze-failure came from sub-command ``Exit(1)``; the equivalent
    adapter behavior is reproduced in the cli adapter, which short-circuits.)

    Args:
        settings: Loaded configuration.
        source: Data source name.
        datasets: Datasets to ingest + transform.
        start_dt: Window start.
        end_dt: Window end.
        gold_targets: Gold datasets to build after silver, or None to skip.
        reingest: Passed through to the transform stage.

    Returns:
        A :class:`RunReport` with every stage's :class:`DatasetResult`.
    """
    results: list[DatasetResult] = []
    with build_context(settings) as ctx:
        results.extend(
            run_ingest(
                ctx, source, datasets, start_dt, end_dt, incremental=False, write_watermark=True
            )
        )
        results.extend(run_transform(ctx, source, datasets, start_dt, end_dt, reingest=reingest))
        if gold_targets:
            results.extend(run_build(ctx, gold_targets, start_dt, end_dt))
    # Refresh once, after the run's connection has closed (Windows lock safety).
    refresh_views(settings)
    return RunReport(results=results)


def run_backfill(
    settings: GridflowConfig,
    source: str,
    datasets: list[str],
    start_dt: datetime,
    end_dt: datetime,
    *,
    chunk_days: int = 1,
) -> RunReport:
    """Backfill historical data in chunks, in-process, on one connection.

    For each dataset, walk ``[start_dt, end_dt)`` in ``chunk_days`` windows. Each
    chunk ingests (``write_watermark=False`` — a historical op must never move the
    forward frontier) then transforms. The transform end is ``chunk_end - 1 day``:
    ``chunk_end`` is the exclusive API boundary but transform date iteration is
    inclusive, so without the -1d the end date would warn "no bronze data"
    (preserved off-by-one from cli.backfill).

    One DuckDB connection is reused across all chunks (Windows-safe: no second RW
    handle), and views are refreshed once at the very end after it closes.

    Args:
        settings: Loaded configuration.
        source: Data source name.
        datasets: Datasets to backfill.
        start_dt: Inclusive range start (tz-aware UTC).
        end_dt: Exclusive range end (tz-aware UTC).
        chunk_days: Days per chunk.

    Returns:
        A :class:`RunReport` aggregating every chunk's ingest + transform results.
    """
    results: list[DatasetResult] = []
    with build_context(settings) as ctx:
        for ds in datasets:
            current = start_dt
            while current < end_dt:
                chunk_end = min(current + timedelta(days=chunk_days), end_dt)
                results.extend(
                    run_ingest(
                        ctx,
                        source,
                        [ds],
                        current,
                        chunk_end,
                        incremental=False,
                        write_watermark=False,
                    )
                )
                transform_end = chunk_end - timedelta(days=1)
                results.extend(run_transform(ctx, source, [ds], current, transform_end))
                current = chunk_end
    refresh_views(settings)
    return RunReport(results=results)
