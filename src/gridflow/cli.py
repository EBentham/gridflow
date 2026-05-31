"""gridflow CLI — ingest, transform, build, and query energy market data."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path

import typer

app = typer.Typer(name="gridflow", help="UK/EU energy market data pipeline")
logger = logging.getLogger(__name__)


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

    start_dt, end_dt = _resolve_dates(start, end, last, settings.pipeline.default_lookback_hours)
    datasets = _resolve_datasets(source, dataset, all_datasets, settings)

    # Import connector registrations
    _import_connectors()

    from gridflow.bronze.writer import BronzeWriter
    from gridflow.connectors.registry import get_connector
    from gridflow.storage.duckdb import get_connection, init_catalogue

    # Ensure catalogue exists
    init_catalogue(settings.pipeline.duckdb_path, settings.pipeline.data_dir)
    con = get_connection(settings.pipeline.duckdb_path)

    from gridflow.observability import PipelineRunTracker

    source_config = settings.get_source_config(source)
    writer = BronzeWriter(settings.pipeline.data_dir)
    failures: list[tuple[str, str]] = []

    for ds in datasets:
        tracker = PipelineRunTracker(con, source, ds, "ingest")
        try:
            connector = get_connector(source, source_config)

            async def _do_fetch(connector=connector, ds=ds) -> list:
                async with connector:
                    return await connector.fetch(ds, start_dt, end_dt)

            responses = asyncio.run(_do_fetch())
            for resp in responses:
                writer.write(resp)
            tracker.complete(rows_in=len(responses), rows_out=len(responses))
            typer.echo(f"  {source}/{ds}: {len(responses)} responses ingested")
        except Exception as e:
            error_message = _safe_error_message(str(e))
            tracker.fail(error_message)
            failures.append((ds, error_message))
            typer.echo(f"  {source}/{ds}: FAILED - {error_message}", err=True)
            logger.error("Ingest failed for %s/%s: %s", source, ds, error_message)
            continue

    con.close()
    if failures:
        typer.echo(f"Ingestion failed for {len(failures)} dataset(s):", err=True)
        for ds, error_message in failures:
            typer.echo(f"  {source}/{ds}: {error_message}", err=True)
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
    from gridflow.utils.time import date_range

    settings = load_settings()
    setup_logging(
        settings.pipeline.log_dir,
        settings.pipeline.log_level,
        settings.pipeline.console_log_level,
    )

    start_dt, end_dt = _resolve_dates(start, end, last, settings.pipeline.default_lookback_hours)
    datasets = _resolve_datasets(source, dataset, all_datasets, settings)

    # Import transformer registrations
    _import_transformers()

    from gridflow.observability import PipelineRunTracker
    from gridflow.silver.registry import get_transformer
    from gridflow.storage.duckdb import get_connection, init_catalogue, refresh_views

    init_catalogue(settings.pipeline.duckdb_path, settings.pipeline.data_dir)
    con = get_connection(settings.pipeline.duckdb_path)

    dates = date_range(start_dt.date(), end_dt.date())
    failures: list[tuple[str, str]] = []

    for ds in datasets:
        tracker = PipelineRunTracker(con, source, ds, "transform")
        total_rows = 0
        try:
            transformer = get_transformer(source, ds, settings.pipeline.data_dir)
            for target_date in dates:
                rows = transformer.run(
                    target_date,
                    run_id=tracker.run_id,
                    reingest=reingest,
                )
                total_rows += rows
            tracker.complete(rows_out=total_rows)
            typer.echo(f"  {source}/{ds}: {total_rows} rows transformed")
        except Exception as e:
            error_message = _safe_error_message(str(e))
            tracker.fail(error_message)
            failures.append((ds, error_message))
            typer.echo(f"  {source}/{ds}: FAILED - {error_message}", err=True)
            logger.error("Transform failed for %s/%s: %s", source, ds, error_message)
            continue

    con.close()
    refresh_views(settings.pipeline.duckdb_path, settings.pipeline.data_dir)
    if failures:
        typer.echo(f"Transform failed for {len(failures)} dataset(s):", err=True)
        for ds, error_message in failures:
            typer.echo(f"  {source}/{ds}: {error_message}", err=True)
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
    from gridflow.gold.system_marginal_price import SystemMarginalPriceBuilder
    from gridflow.utils.logging import setup_logging

    settings = load_settings()
    setup_logging(
        settings.pipeline.log_dir,
        settings.pipeline.log_level,
        settings.pipeline.console_log_level,
    )

    start_dt, end_dt = _resolve_dates(start, end, last, settings.pipeline.default_lookback_hours)

    # Gold dataset registry
    gold_builders = {
        "system_marginal_price": SystemMarginalPriceBuilder,
    }

    if all_datasets:
        targets = list(gold_builders.keys())
    elif gold_dataset:
        targets = [gold_dataset]
    else:
        raise typer.BadParameter("Specify a gold dataset name or use --all")

    from gridflow.observability import PipelineRunTracker
    from gridflow.storage.duckdb import get_connection, init_catalogue, refresh_views

    init_catalogue(settings.pipeline.duckdb_path, settings.pipeline.data_dir)
    con = get_connection(settings.pipeline.duckdb_path)

    for name in targets:
        if name not in gold_builders:
            typer.echo(f"  Unknown gold dataset: {name}", err=True)
            continue

        tracker = PipelineRunTracker(con, "gold", name, "build")
        try:
            builder = gold_builders[name](settings.pipeline.data_dir)
            rows = builder.run(start_dt.date(), end_dt.date())
            tracker.complete(rows_out=rows)
            typer.echo(f"  {name}: {rows} rows built")
        except Exception as e:
            tracker.fail(str(e))
            typer.echo(f"  {name}: FAILED - {e}", err=True)
            logger.exception(f"Build failed for {name}")

    con.close()
    refresh_views(settings.pipeline.duckdb_path, settings.pipeline.data_dir)
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

    for ds in datasets:
        typer.echo(f"\n--- Backfilling {source}/{ds} ---")
        current = start_dt
        chunk_num = 0
        while current < end_dt:
            chunk_end = min(current + timedelta(days=chunk_days), end_dt)
            chunk_num += 1
            typer.echo(f"  Chunk {chunk_num}: {current.date()} to {chunk_end.date()}")

            # Call ingest for this chunk (pass all Optional params explicitly to
            # avoid typer OptionInfo objects leaking in as default values)
            ingest(
                source=source,
                dataset=ds,
                start=current.date().isoformat(),
                end=chunk_end.date().isoformat(),
                last=None,
                all_datasets=False,
            )

            # Call transform for this chunk.  chunk_end is the exclusive API
            # boundary; transform date iteration is inclusive, so subtract one
            # day to avoid a spurious "no bronze data" warning for the end date.
            transform_end = (chunk_end - timedelta(days=1)).date()
            transform(
                source=source,
                dataset=ds,
                start=current.date().isoformat(),
                end=transform_end.isoformat(),
                last=None,
                all_datasets=False,
                reingest=False,
            )

            current = chunk_end

        typer.echo(f"  {source}/{ds}: {chunk_num} chunks processed")

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
    """
    typer.echo(f"=== Pipeline: {source} ===")

    # Bronze
    typer.echo("\n--- Bronze (ingest) ---")
    ingest(
        source=source,
        dataset=dataset,
        start=start,
        end=end,
        last=last,
        all_datasets=all_datasets,
    )

    # Silver
    typer.echo("\n--- Silver (transform) ---")
    transform(
        source=source,
        dataset=dataset,
        start=start,
        end=end,
        last=last,
        all_datasets=all_datasets,
        reingest=False,
    )

    # Gold (optional)
    if gold_dataset:
        typer.echo("\n--- Gold (build) ---")
        build(
            gold_dataset=gold_dataset,
            start=start,
            end=end,
            last=last,
        )

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
    from gridflow.storage.parquet import read_parquet_dir
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
            df = read_parquet_dir(dataset_dir)
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
                    reporter.add_result(check_null_rate(df, col, source=src, dataset=ds))

            # Check time series gaps if timestamp exists
            if "timestamp_utc" in df.columns:
                reporter.add_result(check_time_series_gaps(df, source=src, dataset=ds))

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

    reporter.write_report()
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
) -> None:
    """Delete bronze / silver / gold data and reset the DuckDB catalogue.

    Without layer flags all layers are wiped. Scope with SOURCE / DATASET args.

    Examples:

      gridflow reset --yes                          # wipe everything\n
      gridflow reset elexon --yes                   # wipe all elexon data\n
      gridflow reset elexon system_prices --yes     # wipe one dataset\n
      gridflow reset elexon system_prices --silver  # silver layer only\n
    """

    from gridflow.config.settings import load_settings
    from gridflow.storage.duckdb import init_catalogue

    settings = load_settings()
    data_dir = settings.pipeline.data_dir

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

    def _layer_root(layer: str) -> Path:
        """Return the target directory for a given layer + scope."""
        base = data_dir / layer
        if source:
            base = base / source
            if dataset:
                base = base / dataset
        return base

    if wipe_bronze:
        _wipe_dir(_layer_root("bronze"))

    if wipe_silver:
        _wipe_dir(_layer_root("silver"))

    if wipe_gold:
        if source or dataset:
            # Gold is not organised by source — warn if scoping doesn't apply
            typer.echo("  Note: gold layer is not source-scoped; wiping entire gold directory.")
        _wipe_dir(data_dir / "gold")

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


def _parse_window_bound(value: str) -> datetime:
    """Parse a CLI ``--start``/``--end`` bound to a tz-aware UTC datetime.

    Offset-bearing strings are CONVERTED (``astimezone``), not relabelled; a
    bare calendar date (no time component) is taken as midnight UTC
    (unambiguous); a naive datetime (wall-clock time, no offset) is REJECTED.
    Silently relabelling a naive datetime as UTC — what ``.replace(tzinfo=...)``
    did — shifts the ingest window by up to ±1h under BST, the same
    tz-aware-UTC-contract hazard the leakage barrier guards downstream
    (issue-19 site A).
    """
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        if "T" not in value and ":" not in value:
            return parsed.replace(tzinfo=UTC)
        raise typer.BadParameter(
            f"Naive datetime {value!r} has no timezone; pass an explicit offset "
            f"(e.g. '2026-02-01T00:00:00Z' or '...+01:00') or a bare date."
        )
    return parsed.astimezone(UTC)


def _resolve_dates(
    start: str | None,
    end: str | None,
    last: str | None,
    default_lookback_hours: int,
) -> tuple[datetime, datetime]:
    """Parse date arguments into (start_dt, end_dt) UTC datetimes."""
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


def _safe_error_message(message: str) -> str:
    """Redact sensitive query parameters from user-facing command errors."""
    return re.sub(
        r"(securityToken=)[^&\s)]+",
        r"\1<redacted>",
        message,
        flags=re.IGNORECASE,
    )


def _resolve_datasets(
    source: str,
    dataset: str | None,
    all_flag: bool,
    settings: object,
) -> list[str]:
    """Resolve which datasets to process."""
    from gridflow.config.settings import GridflowConfig

    if not isinstance(settings, GridflowConfig):
        raise TypeError("Expected GridflowConfig")

    if all_flag or (dataset is not None and dataset.lower() == "all"):
        source_config = settings.get_source_config(source)
        return list(source_config.datasets.keys())
    if dataset:
        return [dataset]
    raise typer.BadParameter("Specify a dataset name or use --all")


def _import_connectors() -> None:
    """Import connector modules to trigger registration."""
    for module in [
        "gridflow.connectors.elexon",
        "gridflow.connectors.openmeteo",
        "gridflow.connectors.entsoe",
        "gridflow.connectors.gie",
        "gridflow.connectors.entsog",
        "gridflow.connectors.neso",
    ]:
        with contextlib.suppress(ImportError):
            __import__(module)


def _import_transformers() -> None:
    """Import transformer modules to trigger registration."""
    for module in [
        "gridflow.silver.elexon",
        "gridflow.silver.openmeteo",
        "gridflow.silver.entsoe",
        "gridflow.silver.gie",
        "gridflow.silver.entsog",
        "gridflow.silver.neso",
    ]:
        with contextlib.suppress(ImportError):
            __import__(module)
