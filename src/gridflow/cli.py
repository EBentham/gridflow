"""gridflow CLI — ingest, transform, build, and query energy market data."""

from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import typer

app = typer.Typer(name="gridflow", help="UK/EU energy market data pipeline")
logger = logging.getLogger(__name__)


@app.command()
def ingest(
    source: str = typer.Argument(help="Data source (elexon, entsoe, entsog)"),
    dataset: Optional[str] = typer.Argument(default=None, help="Dataset name, or omit for --all"),
    start: Optional[str] = typer.Option(None, help="Start date (YYYY-MM-DD)"),
    end: Optional[str] = typer.Option(None, help="End date (YYYY-MM-DD)"),
    last: Optional[str] = typer.Option(None, help="Relative lookback (e.g. 24h, 7d)"),
    all_datasets: bool = typer.Option(False, "--all", help="Ingest all datasets for source"),
) -> None:
    """Ingest raw data from an API source into the bronze layer."""
    from gridflow.config.settings import load_settings
    from gridflow.utils.logging import setup_logging

    settings = load_settings()
    setup_logging(settings.pipeline.log_dir, settings.pipeline.log_level)

    start_dt, end_dt = _resolve_dates(
        start, end, last, settings.pipeline.default_lookback_hours
    )
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

    for ds in datasets:
        tracker = PipelineRunTracker(con, source, ds, "ingest")
        try:
            connector = get_connector(source, source_config)

            async def _do_fetch() -> list:
                async with connector:
                    return await connector.fetch(ds, start_dt, end_dt)

            responses = asyncio.run(_do_fetch())
            for resp in responses:
                writer.write(resp)
            tracker.complete(rows_in=len(responses), rows_out=len(responses))
            typer.echo(f"  {source}/{ds}: {len(responses)} responses ingested")
        except Exception as e:
            tracker.fail(str(e))
            typer.echo(f"  {source}/{ds}: FAILED - {e}", err=True)
            logger.exception(f"Ingest failed for {source}/{ds}")
            continue

    con.close()
    typer.echo("Ingestion complete")


@app.command()
def transform(
    source: str = typer.Argument(help="Data source"),
    dataset: Optional[str] = typer.Argument(default=None, help="Dataset name"),
    start: Optional[str] = typer.Option(None, help="Start date (YYYY-MM-DD)"),
    end: Optional[str] = typer.Option(None, help="End date (YYYY-MM-DD)"),
    last: Optional[str] = typer.Option(None, help="Relative lookback (e.g. 24h, 7d)"),
    all_datasets: bool = typer.Option(False, "--all", help="Transform all datasets for source"),
) -> None:
    """Transform bronze data to silver (normalised, validated, deduplicated)."""
    from gridflow.config.settings import load_settings
    from gridflow.utils.logging import setup_logging
    from gridflow.utils.time import date_range

    settings = load_settings()
    setup_logging(settings.pipeline.log_dir, settings.pipeline.log_level)

    start_dt, end_dt = _resolve_dates(
        start, end, last, settings.pipeline.default_lookback_hours
    )
    datasets = _resolve_datasets(source, dataset, all_datasets, settings)

    # Import transformer registrations
    _import_transformers()

    from gridflow.silver.registry import get_transformer
    from gridflow.storage.duckdb import get_connection, init_catalogue
    from gridflow.observability import PipelineRunTracker

    init_catalogue(settings.pipeline.duckdb_path, settings.pipeline.data_dir)
    con = get_connection(settings.pipeline.duckdb_path)

    dates = date_range(start_dt.date(), end_dt.date())

    for ds in datasets:
        tracker = PipelineRunTracker(con, source, ds, "transform")
        total_rows = 0
        try:
            transformer = get_transformer(source, ds, settings.pipeline.data_dir)
            for target_date in dates:
                rows = transformer.run(target_date)
                total_rows += rows
            tracker.complete(rows_out=total_rows)
            typer.echo(f"  {source}/{ds}: {total_rows} rows transformed")
        except Exception as e:
            tracker.fail(str(e))
            typer.echo(f"  {source}/{ds}: FAILED - {e}", err=True)
            logger.exception(f"Transform failed for {source}/{ds}")
            continue

    con.close()
    typer.echo("Transform complete")


@app.command()
def build(
    gold_dataset: Optional[str] = typer.Argument(
        default=None, help="Gold dataset name"
    ),
    start: Optional[str] = typer.Option(None, help="Start date (YYYY-MM-DD)"),
    end: Optional[str] = typer.Option(None, help="End date (YYYY-MM-DD)"),
    last: Optional[str] = typer.Option(None, help="Relative lookback (e.g. 24h, 7d)"),
    all_datasets: bool = typer.Option(False, "--all", help="Build all gold datasets"),
) -> None:
    """Build gold-layer modelling-ready datasets from silver."""
    from gridflow.config.settings import load_settings
    from gridflow.utils.logging import setup_logging
    from gridflow.gold.system_marginal_price import SystemMarginalPriceBuilder

    settings = load_settings()
    setup_logging(settings.pipeline.log_dir, settings.pipeline.log_level)

    start_dt, end_dt = _resolve_dates(
        start, end, last, settings.pipeline.default_lookback_hours
    )

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

    from gridflow.storage.duckdb import get_connection, init_catalogue
    from gridflow.observability import PipelineRunTracker

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
    typer.echo("Build complete")


@app.command()
def backfill(
    source: str = typer.Argument(help="Data source"),
    dataset: str = typer.Argument(help="Dataset name"),
    start: str = typer.Option(..., help="Start date (YYYY-MM-DD)"),
    end: str = typer.Option(..., help="End date (YYYY-MM-DD)"),
    chunk_days: int = typer.Option(7, help="Days per chunk"),
) -> None:
    """Backfill historical data in chunks."""
    from gridflow.config.settings import load_settings
    from gridflow.utils.logging import setup_logging

    settings = load_settings()
    setup_logging(settings.pipeline.log_dir, settings.pipeline.log_level)

    start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
    end_dt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc)

    current = start_dt
    chunk_num = 0
    while current < end_dt:
        chunk_end = min(current + timedelta(days=chunk_days), end_dt)
        chunk_num += 1
        typer.echo(
            f"Chunk {chunk_num}: {current.date()} to {chunk_end.date()}"
        )

        # Call ingest for this chunk
        ingest(
            source=source,
            dataset=dataset,
            start=current.date().isoformat(),
            end=chunk_end.date().isoformat(),
        )

        # Call transform for this chunk
        transform(
            source=source,
            dataset=dataset,
            start=current.date().isoformat(),
            end=chunk_end.date().isoformat(),
        )

        current = chunk_end

    typer.echo(f"Backfill complete: {chunk_num} chunks processed")


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
    source: Optional[str] = typer.Option(None, help="Filter by source"),
    all_sources: bool = typer.Option(False, "--all", help="Run for all sources"),
) -> None:
    """Run quality checks and write report."""
    from gridflow.config.settings import load_settings
    from gridflow.utils.logging import setup_logging
    from gridflow.quality.checks import (
        check_duplicates,
        check_null_rate,
        check_range,
        check_row_count,
        check_time_series_gaps,
    )
    from gridflow.quality.reporter import QualityReporter
    from gridflow.storage.parquet import read_parquet_dir

    settings = load_settings()
    setup_logging(settings.pipeline.log_dir, settings.pipeline.log_level)

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
def init() -> None:
    """Initialise DuckDB catalogue and register views."""
    from gridflow.config.settings import load_settings
    from gridflow.storage.duckdb import init_catalogue
    from gridflow.utils.logging import setup_logging

    settings = load_settings()
    setup_logging(settings.pipeline.log_dir, settings.pipeline.log_level)

    settings.pipeline.data_dir.mkdir(parents=True, exist_ok=True)
    init_catalogue(settings.pipeline.duckdb_path, settings.pipeline.data_dir)
    typer.echo(f"DuckDB catalogue initialised at {settings.pipeline.duckdb_path}")


# --- Helper Functions ---


def _resolve_dates(
    start: str | None,
    end: str | None,
    last: str | None,
    default_lookback_hours: int,
) -> tuple[datetime, datetime]:
    """Parse date arguments into (start_dt, end_dt) UTC datetimes."""
    now = datetime.now(timezone.utc)
    if last:
        from gridflow.utils.time import parse_lookback

        delta = parse_lookback(last)
        return now - delta, now
    if start:
        start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        end_dt = (
            datetime.fromisoformat(end).replace(tzinfo=timezone.utc) if end else now
        )
        return start_dt, end_dt
    return now - timedelta(hours=default_lookback_hours), now


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

    if all_flag:
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
        try:
            __import__(module)
        except ImportError:
            pass


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
        try:
            __import__(module)
        except ImportError:
            pass
