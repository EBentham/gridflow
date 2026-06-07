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
python scripts/run_pipeline.py --step bronze --source elexon --all-datasets \
    --start 2024-01-15 --end 2024-01-16

# Ingest ENTSO-E day-ahead prices for the last 7 days
python scripts/run_pipeline.py --step bronze --source entsoe --dataset day_ahead_prices --last 7d

# Ingest GIE gas storage data
python scripts/run_pipeline.py --step bronze --source gie_agsi --dataset storage --last 30d

# Ingest NESO carbon intensity
python scripts/run_pipeline.py --step bronze --source neso --dataset carbon_intensity --last 24h

# Ingest Open-Meteo weather (historical)
python scripts/run_pipeline.py --step bronze --source open_meteo --dataset historical \
    --start 2024-01-01 --end 2024-01-07

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
import sys
from pathlib import Path
from typing import TYPE_CHECKING

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path so `gridflow` is importable
# regardless of how this script is launched (IDE, terminal, etc.)
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT / "src"))

if TYPE_CHECKING:
    from gridflow.pipeline.runner import DatasetResult

# This script is now a THIN ADAPTER over gridflow.pipeline.runner — the same core
# the CLI uses. Routing through the runner intentionally FIXES four latent drifts
# the old hand-rolled copies had (CH-ARCH-01 / C3-1):
#   1. non-zero exit on failure (the old loop caught-and-continued, always 0);
#   2. completed_with_warnings is now surfaced (was always 'complete');
#   3. views are refreshed after the run (the old script left stale views);
#   4. stored/echoed errors are redacted (the old script stored raw exceptions,
#      leaking securityToken into pipeline_runs.error_message).
# These are INTENDED behaviour changes, not regressions.


def _print_results(results: list[DatasetResult]) -> None:
    """Echo one line per dataset result in the script's terse format."""
    for r in results:
        label = {
            "ingest": "bronze",
            "transform": "silver",
            "build": "gold",
        }.get(r.operation, r.operation)
        if r.status == "failed":
            print(f"  [{label}] {r.source}/{r.dataset}  ->FAILED: {r.error}", file=sys.stderr)
        elif r.status == "completed_with_warnings":
            print(
                f"  [{label}] {r.source}/{r.dataset}  ->{r.rows_out} rows "
                f"({r.rows_skipped} skipped, completed_with_warnings)"
            )
        else:
            rows = r.rows_out if r.operation != "ingest" else r.rows_in
            print(f"  [{label}] {r.source}/{r.dataset}  ->{rows} rows")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the gridflow data pipeline (bronze / silver / gold).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/run_pipeline.py --step bronze --source elexon "
            "--dataset system_prices --last 24h\n"
            "  python scripts/run_pipeline.py --step silver --source elexon "
            "--all-datasets --last 7d\n"
            "  python scripts/run_pipeline.py --step gold "
            "--dataset system_marginal_price --last 30d\n"
            "  python scripts/run_pipeline.py --step all --source elexon "
            "--dataset system_prices --last 24h\n"
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

    from gridflow.config.settings import load_settings
    from gridflow.pipeline import runner
    from gridflow.pipeline.runner import DatasetResolutionError, NaiveDatetimeError, RunReport
    from gridflow.utils.logging import setup_logging

    settings = load_settings()
    settings.pipeline.data_dir.mkdir(parents=True, exist_ok=True)
    setup_logging(
        settings.pipeline.log_dir,
        settings.pipeline.log_level,
        settings.pipeline.console_log_level,
    )

    try:
        start_dt, end_dt = runner.resolve_dates(
            args.start, args.end, args.last, settings.pipeline.default_lookback_hours
        )
    except NaiveDatetimeError as exc:
        parser.error(str(exc))

    runner.import_connectors()
    runner.import_transformers()

    print("gridflow pipeline runner")
    print(f"  Step:  {step}")
    print(f"  Range: {start_dt.date()} -> {end_dt.date()}")
    print()

    results: list[DatasetResult] = []
    with runner.build_context(settings) as ctx:
        if step in ("bronze", "all"):
            try:
                datasets = runner.resolve_datasets(
                    args.source, args.dataset, args.all_datasets, settings
                )
            except DatasetResolutionError as exc:
                parser.error(str(exc))
            results.extend(
                runner.run_ingest(
                    ctx, args.source, datasets, start_dt, end_dt, write_watermark=True
                )
            )
            print()

        if step in ("silver", "all"):
            try:
                datasets = runner.resolve_datasets(
                    args.source, args.dataset, args.all_datasets, settings
                )
            except DatasetResolutionError as exc:
                parser.error(str(exc))
            results.extend(
                runner.run_transform(
                    ctx, args.source, datasets, start_dt, end_dt, reingest=args.reingest
                )
            )
            print()

        if step in ("gold", "all"):
            if args.all_datasets:
                gold_names = list(runner.GOLD_DATASETS)
            elif args.dataset:
                gold_names = [args.dataset]
            elif step == "gold":
                parser.error("--dataset or --all-datasets is required for step 'gold'")
            else:
                # step == "all": build all gold datasets
                gold_names = list(runner.GOLD_DATASETS)
            results.extend(runner.run_build(ctx, gold_names, start_dt, end_dt))
            print()

    # Refresh views once, after the run connection has closed (Windows lock
    # safety) — fixes the old script's stale-view drift.
    runner.refresh_views(settings)

    _print_results(results)
    print()

    report = RunReport(results)
    if not report.ok:
        # Fixes the old catch-and-continue drift: a failed step now exits non-zero.
        print(f"FAILED: {len(report.failed)} step(s) failed.", file=sys.stderr)
        raise SystemExit(1)

    print("Done.")


if __name__ == "__main__":
    main()
