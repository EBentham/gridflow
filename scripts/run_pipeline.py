"""
Run the gridflow data pipeline (bronze / silver / gold) directly from Python.

Designed for IDE debugging — set breakpoints anywhere in the pipeline code
and run this script via your IDE's debugger (F5 in VS Code, Shift+F10 in PyCharm).

Usage Examples
--------------

# Show help and all options
python scripts/run_pipeline.py --help

# --- BRONZE (ingest raw data from APIs) ---

# Ingest Elexon system prices for the last 24 hours
python scripts/run_pipeline.py --step bronze --source elexon --dataset system_prices --last 24h

# Ingest ALL Elexon datasets for a specific date range
python scripts/run_pipeline.py --step bronze --source elexon --all-datasets --start 2024-01-15 --end 2024-01-16

# Ingest ENTSO-E day-ahead prices for the last 7 days
python scripts/run_pipeline.py --step bronze --source entsoe --dataset day_ahead_prices --last 7d

# Ingest GIE gas storage data
python scripts/run_pipeline.py --step bronze --source gie_agsi --dataset storage --last 30d

# Ingest NESO carbon intensity
python scripts/run_pipeline.py --step bronze --source neso --dataset carbon_intensity --last 24h

# Ingest Open-Meteo weather (historical)
python scripts/run_pipeline.py --step bronze --source open_meteo --dataset historical --start 2024-01-01 --end 2024-01-07

# --- SILVER (transform bronze -> normalised parquet) ---

# Transform Elexon system prices for the last 24 hours
python scripts/run_pipeline.py --step silver --source elexon --dataset system_prices --last 24h

# Transform ALL Elexon datasets
python scripts/run_pipeline.py --step silver --source elexon --all-datasets --last 7d

# Transform ENTSO-E actual generation
python scripts/run_pipeline.py --step silver --source entsoe --dataset actual_generation --last 7d

# Transform ENTSO-G physical flows
python scripts/run_pipeline.py --step silver --source entsog --dataset physical_flows --last 7d

# --- GOLD (build analytics-ready datasets from silver) ---

# Build system marginal price gold dataset
python scripts/run_pipeline.py --step gold --dataset system_marginal_price --last 30d

# Build ALL gold datasets
python scripts/run_pipeline.py --step gold --all-datasets --last 30d

# --- FULL PIPELINE (bronze -> silver -> gold) ---

# Run everything for Elexon system prices
python scripts/run_pipeline.py --step all --source elexon --dataset system_prices --last 24h

Available Sources & Datasets
----------------------------
  elexon:       system_prices, fuelhh, boal, bod, mid, freq, ndf, ndfd,
                pn, disbsad, windfor, bmunits_reference, fuelinst, imbalngc,
                netbsad, melngc, fou2t14d, uou2t14d, temp,
                generation_by_fuel, indicative_imbalance_volumes
  open_meteo:   historical, forecast
  entsoe:       day_ahead_prices, actual_load, actual_generation,
                cross_border_flows, load_forecast, wind_solar_forecast,
                outages_generation, installed_capacity
  gie_agsi:     storage
  gie_alsi:     lng
  entsog:       physical_flows
  neso:         carbon_intensity

Gold Datasets
-------------
  system_marginal_price
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so `gridflow` is importable
# regardless of how this script is launched (IDE, terminal, etc.)
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT / "src"))


def _setup() -> tuple:
    """Load settings, initialise logging, and return (settings, con)."""
    from gridflow.config.settings import load_settings
    from gridflow.storage.duckdb import get_connection, init_catalogue
    from gridflow.utils.logging import setup_logging

    settings = load_settings()
    settings.pipeline.data_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(
        settings.pipeline.log_dir,
        settings.pipeline.log_level,
        settings.pipeline.console_log_level,
    )
    init_catalogue(settings.pipeline.duckdb_path, settings.pipeline.data_dir)
    con = get_connection(settings.pipeline.duckdb_path)
    return settings, con


def _import_connectors() -> None:
    """Import connector modules to trigger auto-registration.

    These are core modules present in every healthy install, so an
    ``ImportError`` always indicates a real bug in the module rather than an
    absent optional dependency. Log it with the module name instead of
    swallowing it, so a broken connector is visible rather than masquerading as
    a missing registration.
    """
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
            logging.getLogger(__name__).warning(
                "Failed to import connector module %s", module, exc_info=True
            )


def _import_transformers() -> None:
    """Import transformer modules to trigger auto-registration.

    Core modules — an ``ImportError`` signals a real bug, not a missing optional
    dependency. Log with the module name instead of swallowing it.
    """
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
            logging.getLogger(__name__).warning(
                "Failed to import transformer module %s", module, exc_info=True
            )


def resolve_dates(
    start: str | None,
    end: str | None,
    last: str | None,
    default_lookback_hours: int = 24,
) -> tuple[datetime, datetime]:
    """Parse --start/--end/--last into (start_dt, end_dt) UTC datetimes."""
    now = datetime.now(timezone.utc)
    if last:
        from gridflow.utils.time import parse_lookback

        delta = parse_lookback(last)
        return now - delta, now
    if start:
        start_dt = datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
        end_dt = datetime.fromisoformat(end).replace(tzinfo=timezone.utc) if end else now
        return start_dt, end_dt
    return now - timedelta(hours=default_lookback_hours), now


def resolve_datasets(source: str, dataset: str | None, all_flag: bool, settings) -> list[str]:
    """Resolve which datasets to process for a given source."""
    if all_flag:
        source_config = settings.get_source_config(source)
        return list(source_config.datasets.keys())
    if dataset:
        return [dataset]
    raise SystemExit(f"Error: specify --dataset NAME or --all-datasets for source '{source}'")


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------


def run_bronze(
    source: str, datasets: list[str], start_dt: datetime, end_dt: datetime, settings, con
) -> None:
    """Ingest raw data from APIs into the bronze layer."""
    from gridflow.bronze.writer import BronzeWriter
    from gridflow.connectors.registry import get_connector
    from gridflow.observability import PipelineRunTracker

    _import_connectors()

    source_config = settings.get_source_config(source)
    writer = BronzeWriter(settings.pipeline.data_dir)

    for ds in datasets:
        tracker = PipelineRunTracker(con, source, ds, "ingest")
        print(f"  [bronze] {source}/{ds}  {start_dt.date()} -> {end_dt.date()}")
        try:
            connector = get_connector(source, source_config)

            async def _do_fetch():
                async with connector:
                    return await connector.fetch(ds, start_dt, end_dt)

            responses = asyncio.run(_do_fetch())
            rows_written = 0
            for resp in responses:
                writer.write(resp)
                rows_written += 1
            tracker.complete(rows_out=rows_written)
            print(f"           ->{rows_written} raw files written")
        except Exception as exc:
            tracker.fail(str(exc))
            print(f"           ->FAILED: {exc}", file=sys.stderr)
            logging.getLogger(__name__).exception(f"Bronze ingest failed for {source}/{ds}")


def run_silver(
    source: str,
    datasets: list[str],
    start_dt: datetime,
    end_dt: datetime,
    settings,
    con,
    reingest: bool = False,
) -> None:
    """Transform bronze data to silver (normalised, validated, deduplicated)."""
    from gridflow.silver.registry import get_transformer
    from gridflow.observability import PipelineRunTracker
    from gridflow.utils.time import date_range

    _import_transformers()

    dates = date_range(start_dt.date(), end_dt.date())

    for ds in datasets:
        tracker = PipelineRunTracker(con, source, ds, "transform")
        print(
            f"  [silver] {source}/{ds}  {start_dt.date()} -> {end_dt.date()}  ({len(dates)} days)"
        )
        total_rows = 0
        try:
            transformer = get_transformer(source, ds, settings.pipeline.data_dir)
            # CH3-02 (CH-PERF-02): per-date silver CSV is opt-in (default OFF).
            transformer.write_silver_csv = settings.pipeline.write_silver_csv
            for target_date in dates:
                rows = transformer.run(target_date, run_id=tracker.run_id, reingest=reingest)
                total_rows += rows
            tracker.complete(rows_out=total_rows)
            print(f"           ->{total_rows} rows transformed")
        except Exception as exc:
            tracker.fail(str(exc))
            print(f"           ->FAILED: {exc}", file=sys.stderr)
            logging.getLogger(__name__).exception(f"Silver transform failed for {source}/{ds}")


def run_gold(datasets: list[str], start_dt: datetime, end_dt: datetime, settings, con) -> None:
    """Build gold-layer analytics-ready datasets from silver."""
    from gridflow.gold.system_marginal_price import SystemMarginalPriceBuilder
    from gridflow.observability import PipelineRunTracker

    # Gold dataset registry (same as cli.py)
    gold_builders = {
        "system_marginal_price": SystemMarginalPriceBuilder,
    }

    for ds in datasets:
        if ds not in gold_builders:
            print(f"  [gold]   Unknown gold dataset: {ds}", file=sys.stderr)
            print(f"           Available: {list(gold_builders.keys())}", file=sys.stderr)
            continue

        tracker = PipelineRunTracker(con, "gold", ds, "build")
        print(f"  [gold]   {ds}  {start_dt.date()} -> {end_dt.date()}")
        try:
            builder = gold_builders[ds](settings.pipeline.data_dir)
            rows = builder.run(start_dt.date(), end_dt.date())
            tracker.complete(rows_out=rows)
            print(f"           ->{rows} rows built")
        except Exception as exc:
            tracker.fail(str(exc))
            print(f"           ->FAILED: {exc}", file=sys.stderr)
            logging.getLogger(__name__).exception(f"Gold build failed for {ds}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the gridflow data pipeline (bronze / silver / gold).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/run_pipeline.py --step bronze --source elexon --dataset system_prices --last 24h\n"
            "  python scripts/run_pipeline.py --step silver --source elexon --all-datasets --last 7d\n"
            "  python scripts/run_pipeline.py --step gold --dataset system_marginal_price --last 30d\n"
            "  python scripts/run_pipeline.py --step all --source elexon --dataset system_prices --last 24h\n"
        ),
    )
    parser.add_argument(
        "--step",
        choices=["bronze", "silver", "gold", "all"],
        default="all",
        help="Pipeline step to run (default: all)",
    )
    parser.add_argument("--source", help="Data source name (e.g. elexon, entsoe, gie_agsi)")
    parser.add_argument("--dataset", help="Specific dataset name")
    parser.add_argument(
        "--all-datasets", action="store_true", help="Process all datasets for the source"
    )
    parser.add_argument("--start", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="End date (YYYY-MM-DD)")
    parser.add_argument("--last", help="Relative lookback (e.g. 24h, 7d, 30d)")
    parser.add_argument(
        "--reingest",
        action="store_true",
        help="Use bronze sidecar timestamps for historical available_at values",
    )

    args = parser.parse_args()
    step = args.step

    # Validate arguments
    if step in ("bronze", "silver", "all") and not args.source:
        parser.error(f"--source is required for step '{step}'")

    settings, con = _setup()
    start_dt, end_dt = resolve_dates(
        args.start, args.end, args.last, settings.pipeline.default_lookback_hours
    )

    print(f"gridflow pipeline runner")
    print(f"  Step:  {step}")
    print(f"  Range: {start_dt.date()} -> {end_dt.date()}")
    print()

    try:
        if step in ("bronze", "all"):
            datasets = resolve_datasets(args.source, args.dataset, args.all_datasets, settings)
            run_bronze(args.source, datasets, start_dt, end_dt, settings, con)
            print()

        if step in ("silver", "all"):
            datasets = resolve_datasets(args.source, args.dataset, args.all_datasets, settings)
            run_silver(
                args.source,
                datasets,
                start_dt,
                end_dt,
                settings,
                con,
                reingest=args.reingest,
            )
            print()

        if step in ("gold", "all"):
            if args.all_datasets:
                from gridflow.gold.system_marginal_price import SystemMarginalPriceBuilder

                gold_names = ["system_marginal_price"]
            elif args.dataset:
                gold_names = [args.dataset]
            elif step == "gold":
                parser.error("--dataset or --all-datasets is required for step 'gold'")
                return  # unreachable, but keeps type checker happy
            else:
                # step == "all": build all gold datasets
                gold_names = ["system_marginal_price"]
            run_gold(gold_names, start_dt, end_dt, settings, con)
            print()

    finally:
        con.close()

    print("Done.")


if __name__ == "__main__":
    main()
