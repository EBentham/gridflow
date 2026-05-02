"""
Run ALL gridflow sources for a single date (bronze + silver).

Designed for IDE debugging — set breakpoints anywhere in the pipeline code
and run this script via your IDE's debugger (Shift+F9 in PyCharm, F5 in VS Code).

Usage
-----
    # Run all sources for yesterday (default)
    python scripts/run_all_sources.py

    # Run all sources for a specific date
    python scripts/run_all_sources.py --date 2024-06-15

    # Only run sources that don't need API keys
    python scripts/run_all_sources.py --public-only

    # Run only bronze (ingest) — skip silver transforms
    python scripts/run_all_sources.py --bronze-only

    # Run only silver (transform) — assumes bronze data already exists
    python scripts/run_all_sources.py --silver-only
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure project src is on sys.path for IDE launches
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT / "src"))

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# All sources and their datasets, grouped by auth requirement
# ---------------------------------------------------------------------------
PUBLIC_SOURCES: dict[str, list[str]] = {
    "elexon": [
        "system_prices",
        "fuelhh",
        "fuelinst",
        "boal",
        "bod",
        "mid",
        "freq",
        "ndf",
        "ndfd",
        "pn",
        "disbsad",
        "netbsad",
        "imbalngc",
        "melngc",
        "windfor",
        "temp",
        "fou2t14d",
        "uou2t14d",
        "generation_by_fuel",
        "bmunits_reference",
    ],
    "open_meteo": ["historical", "forecast"],
    "entsog": ["physical_flows"],
    "neso": ["carbon_intensity"],
}

AUTHENTICATED_SOURCES: dict[str, list[str]] = {
    "entsoe": [
        "day_ahead_prices",
        "actual_load",
        "load_forecast",
        "actual_generation",
        "wind_solar_forecast",
        "cross_border_flows",
        "outages_generation",
        "installed_capacity",
    ],
    "gie_agsi": ["storage"],
    "gie_alsi": ["lng"],
}


def _import_connectors() -> None:
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


def run_bronze_for_source(
    source: str,
    datasets: list[str],
    start_dt: datetime,
    end_dt: datetime,
    settings,
    con,
) -> None:
    """Ingest all datasets for a single source."""
    from gridflow.bronze.writer import BronzeWriter
    from gridflow.connectors.registry import get_connector
    from gridflow.observability import PipelineRunTracker

    source_config = settings.get_source_config(source)
    writer = BronzeWriter(settings.pipeline.data_dir)

    for ds in datasets:
        tracker = PipelineRunTracker(con, source, ds, "ingest")
        print(f"  [bronze] {source}/{ds}")
        try:
            connector = get_connector(source, source_config)

            async def _do_fetch():
                async with connector:
                    return await connector.fetch(ds, start_dt, end_dt)

            responses = asyncio.run(_do_fetch())
            for resp in responses:
                writer.write(resp)
            tracker.complete(rows_in=len(responses), rows_out=len(responses))
            print(f"           -> {len(responses)} raw files written")
        except Exception as exc:
            tracker.fail(str(exc))
            print(f"           -> FAILED: {exc}", file=sys.stderr)
            logger.exception(f"Bronze failed: {source}/{ds}")


def run_silver_for_source(
    source: str,
    datasets: list[str],
    target_date: date,
    settings,
    con,
) -> None:
    """Transform all datasets for a single source on a single date."""
    from gridflow.silver.registry import get_transformer
    from gridflow.observability import PipelineRunTracker

    for ds in datasets:
        tracker = PipelineRunTracker(con, source, ds, "transform")
        print(f"  [silver] {source}/{ds}")
        try:
            transformer = get_transformer(source, ds, settings.pipeline.data_dir)
            rows = transformer.run(target_date)
            tracker.complete(rows_out=rows)
            print(f"           -> {rows} rows transformed")
        except Exception as exc:
            tracker.fail(str(exc))
            print(f"           -> FAILED: {exc}", file=sys.stderr)
            logger.exception(f"Silver failed: {source}/{ds}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run ALL gridflow sources for a single date (bronze + silver).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--date",
        help="Target date YYYY-MM-DD (default: yesterday)",
    )
    parser.add_argument(
        "--public-only",
        action="store_true",
        help="Skip sources that require API keys (entsoe, gie_agsi, gie_alsi)",
    )
    parser.add_argument(
        "--bronze-only",
        action="store_true",
        help="Only run bronze (ingest), skip silver transforms",
    )
    parser.add_argument(
        "--silver-only",
        action="store_true",
        help="Only run silver (transform), assumes bronze data exists",
    )

    args = parser.parse_args()

    # Resolve target date
    if args.date:
        target = date.fromisoformat(args.date)
    else:
        target = date.today() - timedelta(days=1)

    start_dt = datetime(target.year, target.month, target.day, tzinfo=timezone.utc)
    end_dt = start_dt + timedelta(days=1)

    # Build the source map
    sources = dict(PUBLIC_SOURCES)
    if not args.public_only:
        sources.update(AUTHENTICATED_SOURCES)

    # Setup
    from gridflow.config.settings import load_settings
    from gridflow.storage.duckdb import get_connection, init_catalogue
    from gridflow.utils.logging import setup_logging

    settings = load_settings()
    settings.pipeline.data_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(settings.pipeline.log_dir, settings.pipeline.log_level)
    init_catalogue(settings.pipeline.duckdb_path, settings.pipeline.data_dir)
    con = get_connection(settings.pipeline.duckdb_path)

    _import_connectors()
    _import_transformers()

    print(f"gridflow — run all sources")
    print(f"  Date:    {target}")
    print(f"  Sources: {len(sources)}")
    print(f"  Mode:    {'bronze only' if args.bronze_only else 'silver only' if args.silver_only else 'bronze + silver'}")
    print()

    run_bronze = not args.silver_only
    run_silver = not args.bronze_only

    try:
        for source, datasets in sources.items():
            print(f"--- {source} ({len(datasets)} datasets) ---")

            if run_bronze:
                run_bronze_for_source(source, datasets, start_dt, end_dt, settings, con)

            if run_silver:
                run_silver_for_source(source, datasets, target, settings, con)

            print()
    finally:
        con.close()

    print("Done.")


if __name__ == "__main__":
    main()
