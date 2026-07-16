"""gridflow CLI — ingest, transform, build, and query energy market data.

The pipeline commands (``ingest``/``transform``/``build``/``pipeline``/
``backfill``) are thin adapters over :mod:`gridflow.pipeline.runner`: the adapter
owns option parsing, settings/logging setup, console output, and exit-code
translation; the runner owns the actual step loops, resolution, tracking, and
error redaction. Stays adapter-only: the ``reset``/``export_csv`` containment
guards and the ``status``/``quality``/``init`` commands.
"""

from __future__ import annotations

import logging
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import typer

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

from gridflow.pipeline import runner
from gridflow.pipeline.runner import (
    DatasetResolutionError,
    DatasetResult,
    NaiveDatetimeError,
    RunReport,
)

app = typer.Typer(name="gridflow", help="UK/EU energy market data pipeline")
logger = logging.getLogger(__name__)


def _is_dangerous_delete_target(target: Path, project_root: Path) -> bool:
    """Return True if recursively deleting ``target`` would be catastrophic.

    A target is dangerous when it is the filesystem root, the user's home
    directory, or an ancestor of (or equal to) the project root — deleting any
    of those would wipe the repository or more. Targets under the project root,
    and unrelated locations such as an OS temp dir, are NOT dangerous; the guard
    is a denylist of the three catastrophic cases, not an allowlist of the repo.

    Args:
        target: The directory or file slated for recursive deletion.
        project_root: The resolved gridflow repository root.

    Returns:
        True if the target must be refused, False if deletion may proceed.
    """
    resolved = target.resolve()
    root = project_root.resolve()
    # Filesystem root: on every platform a path equals its own parent only at the root.
    if resolved == resolved.parent:
        return True
    # Home directory (resolved at call time so tests can monkeypatch Path.home).
    if resolved == Path.home().resolve():
        return True
    # ``root`` lives inside ``resolved`` → ``resolved`` is an ancestor of (or is)
    # the repo, so deleting it would take the repo with it.
    return root.is_relative_to(resolved)


def _assert_safe_delete_target(target: Path, project_root: Path) -> None:
    """Raise ``typer.Exit(1)`` if ``target`` is a catastrophic deletion target.

    Args:
        target: The directory or file slated for recursive deletion.
        project_root: The resolved gridflow repository root.

    Raises:
        typer.Exit: With code 1 if the target is refused by the containment guard.
    """
    if _is_dangerous_delete_target(target, project_root):
        typer.echo(
            f"Refusing to delete {target.resolve()}: target is the filesystem root, "
            "the home directory, or an ancestor of the project — this would wipe the "
            "repository or more. Aborting (nothing deleted).",
            err=True,
        )
        raise typer.Exit(1)


def _realpath_within(path: Path, data_dir: Path) -> bool:
    """Return True if ``path``'s real (symlink/junction-resolved) path stays inside ``data_dir``.

    ``Path.rglob`` descends Windows junctions, which report ``is_symlink() ==
    False``. A junction inside the data tree pointing outside it would let a
    recursive wipe unlink external files. Resolving both sides with
    ``os.path.realpath`` collapses junctions and symlinks so the containment
    check reflects the file's true location, not its apparent one.

    Args:
        path: A candidate file slated for deletion.
        data_dir: The data directory the wipe is confined to.

    Returns:
        True if the real path is inside (or equal to) the real ``data_dir``,
        False if it escapes and must be skipped.
    """
    real_path = Path(os.path.realpath(path))
    real_root = Path(os.path.realpath(data_dir))
    return real_path.is_relative_to(real_root)


def _silver_month_older_than(part_dir: Path, cutoff: date) -> bool | None:
    """Return whether a ``year=YYYY/month=MM`` partition is wholly before ``cutoff``.

    A silver month is unambiguously older than the cutoff only when the entire
    month precedes the cutoff month — i.e. ``(year, month) < (cutoff.year,
    cutoff.month)``. The cutoff month itself straddles the cutoff (it holds days
    both before and on/after it) and is kept.

    Args:
        part_dir: A ``month=MM`` partition directory whose parent is ``year=YYYY``.
        cutoff: The retention cutoff date; partitions before it are prunable.

    Returns:
        True if the partition is wholly older than the cutoff, False if it is
        not, or None if the directory names cannot be parsed (caller must skip
        unparseable partitions rather than risk deleting unknown data).
    """
    year_name = part_dir.parent.name
    month_name = part_dir.name
    if not (year_name.startswith("year=") and month_name.startswith("month=")):
        return None
    try:
        year = int(year_name.removeprefix("year="))
        month = int(month_name.removeprefix("month="))
    except ValueError:
        return None
    if not (1 <= month <= 12):
        return None
    return (year, month) < (cutoff.year, cutoff.month)


def _gold_year_older_than(part_dir: Path, cutoff: date) -> bool | None:
    """Return whether a gold ``year=YYYY`` partition is wholly before ``cutoff``.

    Gold is year-partitioned (``gold/<dataset>/year=YYYY``), so a year is
    unambiguously older only when ``year < cutoff.year``; the cutoff year
    straddles the cutoff and is kept.

    Returns:
        True if wholly older, False if not, None if the name cannot be parsed.
    """
    name = part_dir.name
    if not name.startswith("year="):
        return None
    try:
        year = int(name.removeprefix("year="))
    except ValueError:
        return None
    return year < cutoff.year


def _bronze_day_older_than(day_dir: Path, cutoff: date) -> bool | None:
    """Return whether a bronze ``YYYY/MM/DD`` day partition is before ``cutoff``.

    Bronze partitions are single days, so a day can never straddle a date cutoff:
    its date is wholly before or on/after the cutoff. The day equal to the cutoff
    is the cutoff itself (not older) and is kept; pruning is by the partition's
    filing date (datasets that batch several days under a window-start date are
    pruned on that filed date, not their covered span).

    Args:
        day_dir: A ``DD`` day directory whose parents are ``MM`` and ``YYYY``.
        cutoff: The retention cutoff date.

    Returns:
        True if the day is strictly before the cutoff, False if not, None if the
        directory names cannot be parsed as a date.
    """
    try:
        year = int(day_dir.parent.parent.name)
        month = int(day_dir.parent.name)
        day = int(day_dir.name)
        part_date = date(year, month, day)
    except ValueError:
        return None
    return part_date < cutoff


@app.command()
def ingest(
    source: str = typer.Argument(help="Data source (elexon, entsoe, entsog)"),
    dataset: str | None = typer.Argument(default=None, help="Dataset name, or omit for --all"),
    start: str | None = typer.Option(None, help="Start date (YYYY-MM-DD)"),
    end: str | None = typer.Option(None, help="End date (YYYY-MM-DD)"),
    last: str | None = typer.Option(None, help="Relative lookback (e.g. 24h, 7d)"),
    all_datasets: bool = typer.Option(
        False, "--all", "-all", help="Ingest all datasets for source"
    ),
    incremental: bool = typer.Option(
        False,
        "--incremental",
        "--since-watermark",
        help="Resume each dataset from its stored watermark (minus the configured "
        "overlap, default 72h). Ignored when --start or --last is given. First run "
        "falls back to the default lookback. The watermark advances only on "
        "observed data (not on empty or partial fetches), and the overlap "
        "re-fetches the recent window to recover late publications and Elexon "
        "settlement run_type revisions (II->SF->R1). NOTE: weeks-long settlement "
        "revision tails still need a periodic backfill — the overlap only covers "
        "the recent window.",
    ),
    write_watermark: bool = typer.Option(
        True,
        "--write-watermark/--no-write-watermark",
        hidden=True,
        help="Advance the ingestion watermark on success. Internal: backfill "
        "passes --no-write-watermark so a historical chunk-ingest never moves "
        "the forward frontier.",
    ),
) -> None:
    """Ingest raw data from an API source into the bronze layer."""
    from gridflow.config.settings import load_settings
    from gridflow.utils.logging import setup_logging

    settings = load_settings()
    setup_logging(
        settings.pipeline.log_dir,
        settings.pipeline.log_level,
        settings.pipeline.console_log_level,
    )

    # end_dt is resolved once up front (= now for the lookback/incremental paths);
    # incremental resolves start per-dataset inside the runner loop from each
    # watermark. Precedence: --start > --last > --incremental > default lookback.
    default_start_dt, end_dt = _resolve_dates(
        start, end, last, settings.pipeline.default_lookback_hours
    )
    use_watermark_start = incremental and start is None and last is None
    datasets = _resolve_datasets(source, dataset, all_datasets, settings)

    runner.import_connectors()

    with runner.build_context(settings) as ctx:
        results = runner.run_ingest(
            ctx,
            source,
            datasets,
            default_start_dt,
            end_dt,
            incremental=use_watermark_start,
            write_watermark=write_watermark,
        )

    _echo_ingest_results(source, results)
    failures = [r for r in results if r.status == "failed"]
    if failures:
        typer.echo(f"Ingestion failed for {len(failures)} dataset(s):", err=True)
        for r in failures:
            typer.echo(f"  {source}/{r.dataset}: {r.error}", err=True)
        raise typer.Exit(1)
    typer.echo("Ingestion complete")


@app.command()
def transform(
    source: str = typer.Argument(help="Data source"),
    dataset: str | None = typer.Argument(default=None, help="Dataset name"),
    start: str | None = typer.Option(None, help="Start date (YYYY-MM-DD)"),
    end: str | None = typer.Option(None, help="End date (YYYY-MM-DD)"),
    last: str | None = typer.Option(None, help="Relative lookback (e.g. 24h, 7d)"),
    all_datasets: bool = typer.Option(False, "--all", help="Transform all datasets for source"),
    reingest: bool = typer.Option(
        False,
        "--reingest",
        help="Use bronze sidecar timestamps for historical available_at values",
    ),
) -> None:
    """Transform bronze data to silver (normalised, validated, deduplicated)."""
    from gridflow.config.settings import load_settings
    from gridflow.utils.logging import setup_logging

    settings = load_settings()
    setup_logging(
        settings.pipeline.log_dir,
        settings.pipeline.log_level,
        settings.pipeline.console_log_level,
    )

    start_dt, end_dt = _resolve_dates(start, end, last, settings.pipeline.default_lookback_hours)
    datasets = _resolve_datasets(source, dataset, all_datasets, settings)

    runner.import_transformers()

    with runner.build_context(settings) as ctx:
        results = runner.run_transform(ctx, source, datasets, start_dt, end_dt, reingest=reingest)
    # Refresh views once, after the run connection has closed (Windows lock safety).
    runner.refresh_views(settings)

    _echo_transform_results(source, results)
    failures = [r for r in results if r.status == "failed"]
    if failures:
        typer.echo(f"Transform failed for {len(failures)} dataset(s):", err=True)
        for r in failures:
            typer.echo(f"  {source}/{r.dataset}: {r.error}", err=True)
        raise typer.Exit(1)
    typer.echo("Transform complete")


@app.command()
def build(
    gold_dataset: str | None = typer.Argument(default=None, help="Gold dataset name"),
    start: str | None = typer.Option(None, help="Start date (YYYY-MM-DD)"),
    end: str | None = typer.Option(None, help="End date (YYYY-MM-DD)"),
    last: str | None = typer.Option(None, help="Relative lookback (e.g. 24h, 7d)"),
    all_datasets: bool = typer.Option(False, "--all", "-all", help="Build all gold datasets"),
) -> None:
    """Build gold-layer modelling-ready datasets from silver."""
    from gridflow.config.settings import load_settings
    from gridflow.utils.logging import setup_logging

    settings = load_settings()
    setup_logging(
        settings.pipeline.log_dir,
        settings.pipeline.log_level,
        settings.pipeline.console_log_level,
    )

    start_dt, end_dt = _resolve_dates(start, end, last, settings.pipeline.default_lookback_hours)

    if all_datasets:
        targets = list(runner.GOLD_DATASETS)
    elif gold_dataset:
        targets = [gold_dataset]
    else:
        raise typer.BadParameter("Specify a gold dataset name or use --all")

    with runner.build_context(settings) as ctx:
        results = runner.run_build(ctx, targets, start_dt, end_dt)
    runner.refresh_views(settings)

    # build historically NEVER aborts: an unknown dataset and a builder failure
    # both print a line and the command STILL exits 0 with "Build complete".
    # Preserve that (the runner records both as 'failed' results; the adapter
    # decides — and here it does NOT translate to a non-zero exit).
    _echo_build_results(results)
    typer.echo("Build complete")


@app.command()
def backfill(
    source: str = typer.Argument(help="Data source"),
    dataset: str | None = typer.Argument(default=None, help="Dataset name"),
    start: str = typer.Option(..., help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., help="End date (YYYY-MM-DD)"),
    chunk_days: int = typer.Option(1, help="Days per chunk"),
    all_datasets: bool = typer.Option(False, "--all", help="Backfill all datasets for source"),
) -> None:
    """Backfill historical data in chunks."""
    from gridflow.config.settings import load_settings
    from gridflow.utils.logging import setup_logging

    settings = load_settings()
    setup_logging(
        settings.pipeline.log_dir,
        settings.pipeline.log_level,
        settings.pipeline.console_log_level,
    )

    datasets = _resolve_datasets(source, dataset, all_datasets, settings)

    start_dt = datetime.fromisoformat(start).replace(tzinfo=UTC)
    end_dt = datetime.fromisoformat(end).replace(tzinfo=UTC)

    runner.import_connectors()
    runner.import_transformers()

    # The adapter owns the chunk loop + per-chunk echo; each chunk's ingest and
    # transform delegate to the runner on ONE shared connection (Windows-safe:
    # no second RW handle to the catalogue). Views are refreshed once at the end
    # after the connection closes.
    # Collect every chunk's ingest + transform result so a failed step can no
    # longer hide behind "Backfill complete" / exit 0 (R3-F03).
    all_results: list[DatasetResult] = []
    with runner.build_context(settings) as ctx:
        for ds in datasets:
            typer.echo(f"\n--- Backfilling {source}/{ds} ---")
            current = start_dt
            chunk_num = 0
            while current < end_dt:
                chunk_end = min(current + timedelta(days=chunk_days), end_dt)
                chunk_num += 1
                typer.echo(f"  Chunk {chunk_num}: {current.date()} to {chunk_end.date()}")

                # write_watermark=False: backfill is an explicit historical op and
                # must never advance/rewind the forward incremental frontier (C3-11).
                all_results.extend(
                    runner.run_ingest(
                        ctx,
                        source,
                        [ds],
                        current,
                        chunk_end,
                        incremental=False,
                        write_watermark=False,
                    )
                )

                # chunk_end is the exclusive API boundary; transform date iteration
                # is inclusive, so subtract one day to avoid a spurious "no bronze
                # data" warning for the end date.
                transform_end = chunk_end - timedelta(days=1)
                all_results.extend(runner.run_transform(ctx, source, [ds], current, transform_end))

                current = chunk_end

            typer.echo(f"  {source}/{ds}: {chunk_num} chunks processed")

    runner.refresh_views(settings)

    # R3-F03: surface any failed ingest/transform step and exit non-zero, mirroring
    # cli.ingest / scripts/backfill.py. Only per-dataset step failures are echoed
    # here; an infra exception outside the runner's per-dataset guards still
    # propagates and exits non-zero, just without this summary.
    failures = [r for r in all_results if r.status == "failed"]
    if failures:
        typer.echo(f"Backfill failed for {len(failures)} step(s):", err=True)
        for r in failures:
            typer.echo(f"  {r.operation} {source}/{r.dataset}: {r.error}", err=True)
        raise typer.Exit(1)
    typer.echo("\nBackfill complete")


@app.command()
def export_csv(
    source: str = typer.Argument(help="Data source"),
    dataset: str | None = typer.Argument(default=None, help="Dataset name"),
    output_dir: str | None = typer.Option(
        None, "--output", "-o", help="Output directory (default: data/exports/)"
    ),
    all_datasets: bool = typer.Option(
        False, "--all", "-all", help="Export all datasets for source"
    ),
) -> None:
    """Export silver Parquet data to CSV files."""
    from gridflow.config.settings import load_settings
    from gridflow.storage.parquet import read_parquet_dir
    from gridflow.utils.logging import setup_logging

    settings = load_settings()
    setup_logging(
        settings.pipeline.log_dir,
        settings.pipeline.log_level,
        settings.pipeline.console_log_level,
    )

    datasets = _resolve_datasets(source, dataset, all_datasets, settings)

    export_base = Path(output_dir) if output_dir else settings.pipeline.data_dir / "exports"

    for ds in datasets:
        silver_dir = settings.pipeline.data_dir / "silver" / source / ds
        if not silver_dir.exists():
            typer.echo(f"  {source}/{ds}: no silver data found, skipping")
            continue

        df = read_parquet_dir(silver_dir)
        if df.is_empty():
            typer.echo(f"  {source}/{ds}: empty, skipping")
            continue

        out_dir = export_base / source
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{ds}.csv"
        df.write_csv(out_path)
        typer.echo(f"  {source}/{ds}: {len(df)} rows -> {out_path}")

    typer.echo("Export complete")


@app.command()
def pipeline(
    source: str = typer.Argument(help="Data source"),
    dataset: str | None = typer.Argument(default=None, help="Dataset name"),
    start: str | None = typer.Option(None, help="Start date (YYYY-MM-DD)"),
    end: str | None = typer.Option(None, help="End date (YYYY-MM-DD)"),
    last: str | None = typer.Option(None, help="Relative lookback (e.g. 24h, 7d)"),
    all_datasets: bool = typer.Option(False, "--all", "-all", help="Run all datasets for source"),
    gold_dataset: str | None = typer.Option(
        None, "--gold", help="Gold dataset to build after silver"
    ),
) -> None:
    """Run the full pipeline: ingest (bronze) -> transform (silver) -> build (gold).

    Runs bronze and silver stages for the specified source/dataset. If --gold is
    given, also builds that gold dataset from the resulting silver data.

    Dates and datasets are resolved ONCE here; this command then opens a single
    pipeline context (``runner.build_context``) and drives ``run_ingest`` ->
    ``run_transform`` (-> ``run_build``) inline on that one connection, refreshing
    views once after it closes. Inlining the stages (rather than delegating to a
    runner orchestration) is deliberate: it preserves the abort-on-bronze-failure
    short-circuit and the exact per-stage echo ordering. The old form re-resolved
    dates 2-3x across separate sub-command calls. A bronze (or silver) failure
    aborts before the completion marker and exits 1.
    """
    from gridflow.config.settings import load_settings
    from gridflow.utils.logging import setup_logging

    settings = load_settings()
    setup_logging(
        settings.pipeline.log_dir,
        settings.pipeline.log_level,
        settings.pipeline.console_log_level,
    )

    start_dt, end_dt = _resolve_dates(start, end, last, settings.pipeline.default_lookback_hours)
    datasets = _resolve_datasets(source, dataset, all_datasets, settings)

    runner.import_connectors()
    runner.import_transformers()

    typer.echo(f"=== Pipeline: {source} ===")

    # The bronze/silver/gold stages share ONE connection (the old form opened a
    # fresh connection per sub-command); views are refreshed ONCE after it closes
    # (refresh-per-stage vs refresh-once is internal, not stdout-observable — the
    # Windows lock hazard forbids refreshing while the RW connection is open). The
    # ECHO sequence below reproduces the old delegated output exactly: each
    # stage's per-dataset lines, then that stage's terminal status line, in order.
    silver_failures: list[DatasetResult] = []
    with runner.build_context(settings) as ctx:
        # Bronze
        typer.echo("\n--- Bronze (ingest) ---")
        bronze_results = runner.run_ingest(
            ctx, source, datasets, start_dt, end_dt, incremental=False, write_watermark=True
        )
        _echo_ingest_results(source, bronze_results)
        if not RunReport(bronze_results).ok:
            # The old ``ingest`` sub-call printed its summary block and raised
            # Exit(1) here — WITHOUT refreshing views (ingest never refreshes).
            # Match it (abort inside the block: the ctx-mgr still closes the con,
            # and the refresh after the block is skipped).
            bronze_failures = [r for r in bronze_results if r.status == "failed"]
            typer.echo(f"Ingestion failed for {len(bronze_failures)} dataset(s):", err=True)
            for r in bronze_failures:
                typer.echo(f"  {source}/{r.dataset}: {r.error}", err=True)
            raise typer.Exit(1)
        typer.echo("Ingestion complete")

        # Silver
        typer.echo("\n--- Silver (transform) ---")
        silver_results = runner.run_transform(ctx, source, datasets, start_dt, end_dt)
        _echo_transform_results(source, silver_results)
        silver_failures = [r for r in silver_results if r.status == "failed"]
        # The old ``transform`` sub-call refreshed views (after closing its con)
        # EVEN on failure, then raised. So a silver failure must NOT short-circuit
        # the refresh below — fall through to the post-block refresh, then exit.
        if not silver_failures:
            typer.echo("Transform complete")
            # Gold (optional) — only reached after a successful silver (the old
            # short-circuit: a failed transform raised before --gold).
            if gold_dataset:
                typer.echo("\n--- Gold (build) ---")
                gold_results = runner.run_build(ctx, [gold_dataset], start_dt, end_dt)
                _echo_build_results(gold_results)
                typer.echo("Build complete")

    # Connection closed; refresh once (transform/the completed pipeline always
    # refreshed). A silver failure refreshed-then-exited in the old form — match.
    runner.refresh_views(settings)
    if silver_failures:
        typer.echo(f"Transform failed for {len(silver_failures)} dataset(s):", err=True)
        for r in silver_failures:
            typer.echo(f"  {source}/{r.dataset}: {r.error}", err=True)
        raise typer.Exit(1)
    typer.echo("\n=== Pipeline complete ===")


@app.command()
def status() -> None:
    """Show pipeline run history and data quality summary."""
    from gridflow.config.settings import load_settings
    from gridflow.storage.duckdb import get_connection

    settings = load_settings()

    if not settings.pipeline.duckdb_path.exists():
        typer.echo("No DuckDB catalogue found. Run 'gridflow init' first.")
        raise typer.Exit(1)

    con = get_connection(settings.pipeline.duckdb_path, read_only=True)

    try:
        result = con.sql("""
            SELECT source, dataset, operation, status,
                   rows_out, ROUND(duration_seconds, 1) as duration_s
            FROM pipeline_runs
            WHERE started_at > now() - INTERVAL '24 hours'
            ORDER BY started_at DESC
            LIMIT 20
        """).fetchdf()

        if result.empty:
            typer.echo("No pipeline runs in the last 24 hours.")
        else:
            typer.echo("Last 24h Pipeline Runs:")
            typer.echo(result.to_string(index=False))
    except Exception as e:
        typer.echo(f"Could not query pipeline runs: {e}")

    con.close()


@app.command()
def quality(
    source: str | None = typer.Option(None, help="Filter by source"),
    all_sources: bool = typer.Option(False, "--all", help="Run for all sources"),
) -> None:
    """Run quality checks and write report."""
    from gridflow.config.settings import load_settings
    from gridflow.quality.checks import (
        check_duplicates,
        check_null_rate,
        check_row_count,
        check_time_series_gaps,
    )
    from gridflow.quality.reporter import QualityReporter
    from gridflow.silver.latest_views import LATEST_VIEW_SPECS, select_latest_vintage
    from gridflow.storage.parquet import scan_parquet_dir
    from gridflow.utils.logging import setup_logging

    settings = load_settings()
    setup_logging(
        settings.pipeline.log_dir,
        settings.pipeline.log_level,
        settings.pipeline.console_log_level,
    )

    reporter = QualityReporter(settings.pipeline.data_dir, settings.pipeline.duckdb_path)
    silver_dir = settings.pipeline.data_dir / "silver"

    if not silver_dir.exists():
        typer.echo("No silver data found.")
        raise typer.Exit(1)

    sources = [source] if source else [d.name for d in silver_dir.iterdir() if d.is_dir()]

    for src in sources:
        src_dir = silver_dir / src
        if not src_dir.exists():
            continue
        for dataset_dir in src_dir.iterdir():
            if not dataset_dir.is_dir():
                continue
            ds = dataset_dir.name
            # Quality wants the whole dataset (no date range); non-date/single
            # flat-file datasets (NESO carbon_intensity, entsog generic) route
            # through the rangeless dir scan, never the range helper.
            lf = scan_parquet_dir(dataset_dir)
            # APPEND_ONLY datasets keep every vintage on disk; quality reads the
            # latest-vintage surface (ADR-025 P0.3) so duplicate/gap checks see
            # one row per entity key, not one per vintage.
            spec = LATEST_VIEW_SPECS.get((src, ds))
            if spec is not None:
                lf = select_latest_vintage(lf, spec)
            df = lf.collect()
            if df.is_empty():
                continue

            reporter.add_result(check_row_count(df, source=src, dataset=ds))

            # Check nulls for numeric columns
            for col in df.columns:
                if df[col].dtype in (
                    __import__("polars").Float64,
                    __import__("polars").Float32,
                    __import__("polars").Int32,
                    __import__("polars").Int64,
                ):
                    reporter.add_result(
                        check_null_rate(
                            df,
                            col,
                            source=src,
                            dataset=ds,
                            max_rate=settings.quality.null_rate_threshold,
                        )
                    )

            # Check time series gaps if timestamp exists
            if "timestamp_utc" in df.columns:
                reporter.add_result(
                    check_time_series_gaps(
                        df,
                        source=src,
                        dataset=ds,
                        expected_freq_minutes=settings.quality.expected_freq_minutes,
                    )
                )

            # Check for duplicates on key columns
            if "settlement_date" in df.columns and "settlement_period" in df.columns:
                reporter.add_result(
                    check_duplicates(
                        df,
                        ["settlement_date", "settlement_period"],
                        source=src,
                        dataset=ds,
                    )
                )

    # A failed quality-report write is a real failure of this command, not a
    # clean empty result — surface it as a non-zero exit instead of swallowing.
    try:
        reporter.write_report()
    except Exception as e:
        logger.error("Quality report write failed: %s", e, exc_info=True)
        typer.echo(f"Quality report write failed: {e}", err=True)
        raise typer.Exit(1) from e
    typer.echo(reporter.summary())


@app.command()
def reset(
    source: str | None = typer.Argument(
        default=None, help="Limit reset to this source (e.g. elexon)"
    ),
    dataset: str | None = typer.Argument(default=None, help="Limit reset to this dataset"),
    bronze: bool = typer.Option(False, "--bronze", help="Wipe bronze layer only"),
    silver: bool = typer.Option(False, "--silver", help="Wipe silver layer only"),
    gold: bool = typer.Option(False, "--gold", help="Wipe gold layer only"),
    duckdb: bool = typer.Option(False, "--duckdb", help="Wipe and recreate DuckDB catalogue only"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompt"),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview targets without deleting anything"
    ),
) -> None:
    """Delete bronze / silver / gold data and reset the DuckDB catalogue.

    Without layer flags all layers are wiped. Scope with SOURCE / DATASET args.

    Examples:

      gridflow reset --yes                          # wipe everything\n
      gridflow reset elexon --yes                   # wipe all elexon data\n
      gridflow reset elexon system_prices --yes     # wipe one dataset\n
      gridflow reset elexon system_prices --silver  # silver layer only\n
    """

    from gridflow.config.settings import _project_root, load_settings
    from gridflow.storage.duckdb import init_catalogue

    settings = load_settings()
    data_dir = settings.pipeline.data_dir
    project_root = _project_root()

    # Containment guard: a misconfigured data_dir / duckdb_path that resolves to
    # the filesystem root, the home dir, or an ancestor of the repo would let a
    # recursive wipe destroy the repository or more. Refuse before any deletion.
    _assert_safe_delete_target(data_dir, project_root)
    _assert_safe_delete_target(settings.pipeline.duckdb_path, project_root)

    # If no layer flags given, wipe all layers
    wipe_all_layers = not any([bronze, silver, gold, duckdb])
    wipe_bronze = bronze or wipe_all_layers
    wipe_silver = silver or wipe_all_layers
    wipe_gold = gold or wipe_all_layers
    wipe_duckdb = duckdb or wipe_all_layers

    # Build human-readable scope description
    scope_parts = []
    if source:
        scope_parts.append(source)
        if dataset:
            scope_parts.append(dataset)
    else:
        scope_parts.append("ALL sources")

    layer_parts = []
    if wipe_bronze:
        layer_parts.append("bronze")
    if wipe_silver:
        layer_parts.append("silver")
    if wipe_gold:
        layer_parts.append("gold")
    if wipe_duckdb:
        layer_parts.append("DuckDB")

    description = f"{'/'.join(scope_parts)} [{', '.join(layer_parts)}]"

    def _layer_root(layer: str) -> Path:
        """Return the target directory for a given layer + scope."""
        base = data_dir / layer
        if source:
            base = base / source
            if dataset:
                base = base / dataset
        return base

    # Collect the directory targets in deletion order for preview / wiping.
    dir_targets: list[Path] = []
    if wipe_bronze:
        dir_targets.append(_layer_root("bronze"))
    if wipe_silver:
        dir_targets.append(_layer_root("silver"))
    if wipe_gold:
        dir_targets.append(data_dir / "gold")

    # R-1 containment: source / dataset are user CLI args interpolated into the
    # delete path (data_dir/<layer>/<source>/<dataset>). An absolute source
    # collapses the path to an absolute location, and a `../..` climb escapes
    # above data_dir — either would delete files outside the data tree. Validate
    # every actual target before previewing (dry-run) or wiping. (duckdb_path is
    # not derived from these args and keeps its own catastrophic-target guard
    # above, so it is intentionally excluded from this data_dir check.)
    data_root = data_dir.resolve()
    for target in dir_targets:
        if not target.resolve().is_relative_to(data_root):
            typer.echo(
                f"Refusing to delete {target.resolve()}: target escapes the data "
                f"directory {data_root} (check the SOURCE / DATASET arguments). "
                "Aborting (nothing deleted).",
                err=True,
            )
            raise typer.Exit(1)

    if dry_run:
        typer.echo(f"DRY RUN — would PERMANENTLY DELETE: {description}")
        for root in dir_targets:
            if root.exists():
                for f in sorted(root.rglob("*")):
                    if f.is_file():
                        typer.echo(f"  would delete: {f}")
            else:
                typer.echo(f"  (absent, nothing to delete): {root}")
        if wipe_duckdb and settings.pipeline.duckdb_path.exists():
            typer.echo(f"  would delete: {settings.pipeline.duckdb_path}")
        typer.echo("DRY RUN complete — nothing was deleted.")
        return

    if not yes:
        typer.echo(f"About to PERMANENTLY DELETE: {description}")
        typer.confirm("Are you sure?", abort=True)

    deleted_files = 0
    deleted_dirs = 0

    def _wipe_dir(root: Path) -> None:
        nonlocal deleted_files, deleted_dirs
        if not root.exists():
            return
        for f in root.rglob("*"):
            if f.is_file():
                # rglob descends Windows junctions (is_symlink() == False); a
                # junction pointing outside the data tree would otherwise have
                # its external targets unlinked. Skip anything whose real path
                # escapes data_dir.
                if not _realpath_within(f, data_dir):
                    typer.echo(f"  Skipping (resolves outside data dir): {f}", err=True)
                    continue
                try:
                    f.unlink()
                    deleted_files += 1
                except OSError as e:
                    typer.echo(f"  Warning: could not delete {f}: {e}", err=True)
        # Remove empty dirs bottom-up
        for d in sorted(root.rglob("*"), reverse=True):
            if d.is_dir():
                try:
                    if not any(d.iterdir()):
                        d.rmdir()
                        deleted_dirs += 1
                except OSError:
                    pass

    if wipe_gold and (source or dataset):
        # Gold is not organised by source — warn if scoping doesn't apply.
        typer.echo("  Note: gold layer is not source-scoped; wiping entire gold directory.")

    for root in dir_targets:
        _wipe_dir(root)

    if wipe_duckdb and settings.pipeline.duckdb_path.exists():
        try:
            settings.pipeline.duckdb_path.unlink()
            deleted_files += 1
            typer.echo(f"  Deleted DuckDB: {settings.pipeline.duckdb_path}")
        except OSError as e:
            typer.echo(f"  Warning: could not delete DuckDB: {e}", err=True)

    typer.echo(f"Reset complete — {deleted_files} files and {deleted_dirs} directories removed.")

    # Recreate a fresh DuckDB catalogue (empty — no views yet)
    if wipe_duckdb:
        init_catalogue(settings.pipeline.duckdb_path, data_dir)
        typer.echo("DuckDB catalogue recreated.")


@app.command()
def prune(
    layer: str = typer.Argument(help="Layer to prune: bronze, silver, or gold"),
    source: str | None = typer.Argument(
        default=None, help="Limit to this source (e.g. elexon). Ignored for gold."
    ),
    dataset: str | None = typer.Argument(
        default=None,
        help="Limit to this dataset (silver/bronze), or the gold dataset name.",
    ),
    older_than: str | None = typer.Option(
        None,
        "--older-than",
        help="Retention cutoff (YYYY-MM-DD). Partitions wholly older are pruned.",
    ),
    keep_days: int | None = typer.Option(
        None,
        "--keep-days",
        help="Keep the last N days; cutoff = today (UTC) minus N days.",
    ),
    execute: bool = typer.Option(
        False,
        "--execute",
        help="Actually delete. Without this flag prune runs as a dry-run preview.",
    ),
) -> None:
    """Delete old partitions for a layer/scope past a retention cutoff.

    Removes partitions UNAMBIGUOUSLY older than the cutoff for the selected
    scope. Granularity follows the on-disk partition layout:

      - bronze (``YYYY/MM/DD``): day-dirs strictly before the cutoff date;
      - silver (``year=YYYY/month=MM``): months wholly before the cutoff month
        (the straddling cutoff month is kept);
      - gold (``year=YYYY``): years before the cutoff year (the cutoff year is
        kept).

    Safety: this defaults to a DRY RUN — pass ``--execute`` to delete. Every
    target is run through the same containment guards as ``reset`` so prune can
    never escape the data directory or follow a junction out of the tree. Gold
    is not source-scoped; a SOURCE for gold is ignored and DATASET is the gold
    dataset name.

    Examples:

      gridflow prune silver elexon system_prices --older-than 2025-01-01\n
      gridflow prune bronze --keep-days 90 --execute\n
      gridflow prune gold price_curve --older-than 2024-01-01 --execute\n
    """
    from gridflow.config.settings import _project_root, load_settings
    from gridflow.storage.paths import PathBuilder

    layer = layer.lower()
    if layer not in {"bronze", "silver", "gold"}:
        typer.echo(f"Unknown layer '{layer}'. Choose one of: bronze, silver, gold.", err=True)
        raise typer.Exit(2)

    # Exactly one cutoff source — neither leaves the op undefined, both is ambiguous.
    if (older_than is None) == (keep_days is None):
        typer.echo("Provide exactly one of --older-than YYYY-MM-DD or --keep-days N.", err=True)
        raise typer.Exit(2)

    if older_than is not None:
        try:
            cutoff = date.fromisoformat(older_than)
        except ValueError:
            typer.echo(f"Invalid --older-than date '{older_than}' (expected YYYY-MM-DD).", err=True)
            raise typer.Exit(2) from None
    else:
        if keep_days is None or keep_days < 0:
            typer.echo("--keep-days must be a non-negative integer.", err=True)
            raise typer.Exit(2)
        cutoff = datetime.now(UTC).date() - timedelta(days=keep_days)

    settings = load_settings()
    data_dir = settings.pipeline.data_dir
    project_root = _project_root()

    # Containment guard 1: a misconfigured data_dir resolving to the filesystem
    # root, the home dir, or an ancestor of the repo would let a recursive prune
    # destroy the repository or more. Refuse before any deletion (same as reset).
    _assert_safe_delete_target(data_dir, project_root)

    paths = PathBuilder(data_dir)

    # Resolve the dataset roots to scan and the per-partition prune predicate.
    # source/dataset are user CLI args interpolated into the scan root; gold is
    # not source-keyed so DATASET is the gold dataset name.
    scan_roots: list[Path] = []
    # The three predicates share an identical signature; annotate so mypy keeps
    # the variable type wide across the reassignments below.
    is_older: Callable[[Path, date], bool | None]
    if layer == "gold":
        if source and not dataset:
            # A single positional for gold reads as the gold dataset name.
            dataset = source
        if dataset:
            scan_roots.append(paths.gold_dir(dataset))
        else:
            scan_roots.append(data_dir / "gold")
        is_older = _gold_year_older_than
    elif layer == "silver":
        if source and dataset:
            scan_roots.append(paths.silver_dir(source, dataset))
        elif source:
            scan_roots.append(data_dir / "silver" / source)
        else:
            scan_roots.append(data_dir / "silver")
        is_older = _silver_month_older_than
    else:  # bronze
        if source and dataset:
            scan_roots.append(paths.bronze_dir(source, dataset))
        elif source:
            scan_roots.append(data_dir / "bronze" / source)
        else:
            scan_roots.append(data_dir / "bronze")
        is_older = _bronze_day_older_than

    # Containment guard 2 (R-1): source/dataset are user args; an absolute value
    # or a `../..` climb escapes data_dir. Validate every scan root before any
    # preview or deletion, exactly as reset validates its dir_targets.
    data_root = data_dir.resolve()
    for root in scan_roots:
        if not root.resolve().is_relative_to(data_root):
            typer.echo(
                f"Refusing to prune {root.resolve()}: target escapes the data "
                f"directory {data_root} (check the SOURCE / DATASET arguments). "
                "Aborting (nothing deleted).",
                err=True,
            )
            raise typer.Exit(1)

    scope = "/".join(p for p in [layer, source, dataset] if p)

    # Collect the partition directories to prune (those wholly older than cutoff).
    # Silver/gold partitions live at a fixed depth from the dataset root; bronze
    # day-dirs are the depth-3 leaves. _collect_partitions walks the tree and
    # applies the layer predicate, skipping anything it cannot parse.
    targets: list[Path] = []
    for root in scan_roots:
        if not root.exists():
            continue
        for candidate in _iter_partition_dirs(root, layer):
            verdict = is_older(candidate, cutoff)
            if verdict is True:
                targets.append(candidate)

    targets.sort()

    if not execute:
        typer.echo(
            f"DRY RUN — would prune {scope} partitions older than {cutoff.isoformat()} "
            f"({len(targets)} partition(s)). Pass --execute to delete."
        )
        for t in targets:
            typer.echo(f"  would delete: {t.relative_to(data_dir)}")
        if not targets:
            typer.echo("  (nothing older than the cutoff)")
        typer.echo("DRY RUN complete — nothing was deleted.")
        return

    deleted_files = 0
    deleted_dirs = 0
    for part in targets:
        # Containment guard 3: rglob descends Windows junctions (is_symlink() ==
        # False); a junction inside the partition pointing outside the data tree
        # would otherwise have its external targets unlinked. Skip any file whose
        # real path escapes data_dir, mirroring reset's _wipe_dir.
        for f in part.rglob("*"):
            if f.is_file():
                if not _realpath_within(f, data_dir):
                    typer.echo(f"  Skipping (resolves outside data dir): {f}", err=True)
                    continue
                try:
                    f.unlink()
                    deleted_files += 1
                except OSError as e:
                    typer.echo(f"  Warning: could not delete {f}: {e}", err=True)
        # Remove now-empty dirs bottom-up (the partition dir and its children).
        for d in sorted(part.rglob("*"), reverse=True):
            if d.is_dir():
                try:
                    if not any(d.iterdir()):
                        d.rmdir()
                        deleted_dirs += 1
                except OSError:
                    pass
        try:
            if part.is_dir() and not any(part.iterdir()):
                part.rmdir()
                deleted_dirs += 1
        except OSError:
            pass
        typer.echo(f"  pruned: {part.relative_to(data_dir)}")

    typer.echo(
        f"Prune complete — {deleted_files} files and {deleted_dirs} directories removed "
        f"({scope}, older than {cutoff.isoformat()})."
    )


def _iter_partition_dirs(root: Path, layer: str) -> Iterator[Path]:
    """Yield the prunable partition directories beneath a scan ``root``.

    The partition depth depends on both the layer and how deep the scan root
    already is (a dataset-scoped root vs. a layer-wide root). Rather than hard-
    code depths, this yields every ``year=*`` directory (silver/gold) or every
    numeric ``YYYY/MM/DD`` leaf (bronze) found anywhere under the root, so the
    same logic works for ``silver/elexon/system_prices`` and for ``silver``.

    Args:
        root: An existing directory at or above the partition level.
        layer: One of ``bronze``/``silver``/``gold``.

    Yields:
        Candidate partition directories for the layer's prune predicate.
    """
    if layer == "silver":
        # month=MM dirs whose parent is year=YYYY.
        for month_dir in root.rglob("month=*"):
            if month_dir.is_dir() and month_dir.parent.name.startswith("year="):
                yield month_dir
    elif layer == "gold":
        for year_dir in root.rglob("year=*"):
            if year_dir.is_dir():
                yield year_dir
    else:  # bronze: YYYY/MM/DD numeric day leaves.
        for day_dir in root.rglob("*"):
            if (
                day_dir.is_dir()
                and day_dir.name.isdigit()
                and day_dir.parent.name.isdigit()
                and day_dir.parent.parent.name.isdigit()
            ):
                yield day_dir


@app.command()
def init() -> None:
    """Initialise DuckDB catalogue and register views."""
    from gridflow.config.settings import load_settings
    from gridflow.storage.duckdb import init_catalogue
    from gridflow.utils.logging import setup_logging

    settings = load_settings()
    setup_logging(
        settings.pipeline.log_dir,
        settings.pipeline.log_level,
        settings.pipeline.console_log_level,
    )

    settings.pipeline.data_dir.mkdir(parents=True, exist_ok=True)
    init_catalogue(settings.pipeline.duckdb_path, settings.pipeline.data_dir)
    typer.echo(f"DuckDB catalogue initialised at {settings.pipeline.duckdb_path}")


# --- Helper Functions ---
#
# The resolution/registration/redaction logic now lives in
# ``gridflow.pipeline.runner`` (the single source of truth shared with the
# scripts). These module-level names are kept as thin adapter wrappers so that
# (a) existing unit tests importing ``_resolve_dates``/``_resolve_datasets`` from
# ``gridflow.cli`` stay green, and (b) the runner's plain exceptions are
# translated into the ``typer.BadParameter`` the CLI contract expects.


def _resolve_dates(
    start: str | None,
    end: str | None,
    last: str | None,
    default_lookback_hours: int,
) -> tuple[datetime, datetime]:
    """Parse date arguments into ``(start_dt, end_dt)`` UTC datetimes.

    Delegates to :func:`gridflow.pipeline.runner.resolve_dates`, translating the
    runner's :class:`~gridflow.pipeline.runner.NaiveDatetimeError` into a
    ``typer.BadParameter``. A malformed-string ``ValueError`` from
    ``datetime.fromisoformat`` is intentionally NOT caught here — it surfaces as
    typer's own usage error, exactly as before.
    """
    try:
        return runner.resolve_dates(start, end, last, default_lookback_hours)
    except NaiveDatetimeError as e:
        raise typer.BadParameter(str(e)) from e


def _resolve_datasets(
    source: str,
    dataset: str | None,
    all_flag: bool,
    settings: object,
) -> list[str]:
    """Resolve which datasets to process.

    Delegates to :func:`gridflow.pipeline.runner.resolve_datasets`, translating
    the runner's :class:`~gridflow.pipeline.runner.DatasetResolutionError` into a
    ``typer.BadParameter``. A ``TypeError`` (non-``GridflowConfig`` settings)
    propagates unchanged, preserving ``test_invalid_settings_type``.
    """
    try:
        return runner.resolve_datasets(source, dataset, all_flag, settings)
    except DatasetResolutionError as e:
        raise typer.BadParameter(str(e)) from e


# Re-exported for the two unit tests that import them directly from the cli and
# for any historical caller. The runner is the single implementation.
_resolve_incremental_start = runner.resolve_incremental_start
_safe_error_message = runner.safe_error_message
_import_connectors = runner.import_connectors
_import_transformers = runner.import_transformers


# --- Adapter output helpers (RunReport -> echo) ---


def _echo_ingest_results(source: str, results: list[DatasetResult]) -> None:
    """Echo per-dataset ingest result lines (preserves cli.ingest formatting)."""
    for r in results:
        if r.status == "completed_with_warnings":
            typer.echo(
                f"  {source}/{r.dataset}: {r.rows_in} responses ingested, "
                f"{r.rows_skipped} unit(s) skipped (completed_with_warnings)"
            )
        elif r.status == "success":
            typer.echo(f"  {source}/{r.dataset}: {r.rows_in} responses ingested")
        else:
            typer.echo(f"  {source}/{r.dataset}: FAILED - {r.error}", err=True)


def _echo_transform_results(source: str, results: list[DatasetResult]) -> None:
    """Echo per-dataset transform result lines (preserves cli.transform formatting)."""
    for r in results:
        if r.status == "completed_with_warnings":
            typer.echo(
                f"  {source}/{r.dataset}: {r.rows_out} rows transformed, "
                f"{r.rows_unmapped} unmapped, {r.rows_invalid} schema-invalid "
                f"(completed_with_warnings)"
            )
        elif r.status == "success":
            typer.echo(f"  {source}/{r.dataset}: {r.rows_out} rows transformed")
        else:
            typer.echo(f"  {source}/{r.dataset}: FAILED - {r.error}", err=True)


def _echo_build_results(results: list[DatasetResult]) -> None:
    """Echo per-dataset build result lines (preserves cli.build formatting)."""
    for r in results:
        if r.status == "failed" and r.error and r.error.startswith("Unknown gold dataset:"):
            typer.echo(f"  {r.error}", err=True)
        elif r.status == "failed":
            typer.echo(f"  {r.dataset}: FAILED - {r.error}", err=True)
        else:
            typer.echo(f"  {r.dataset}: {r.rows_out} rows built")
