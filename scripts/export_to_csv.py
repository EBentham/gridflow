"""
Export silver / gold DuckDB views to CSV files for easy analysis and viewing.

Designed for IDE debugging — set breakpoints and run via your IDE's debugger.

Usage Examples
--------------

# List all available views and tables
python scripts/export_to_csv.py --list

# Export a specific silver view to CSV
python scripts/export_to_csv.py --view silver_system_prices

# Export a view with date filtering
python scripts/export_to_csv.py --view silver_system_prices --start 2024-01-15 --end 2024-01-16

# Export a gold view
python scripts/export_to_csv.py --view gold_uk_imbalance_context

# Export ALL views at once
python scripts/export_to_csv.py --all

# Export to a custom directory
python scripts/export_to_csv.py --view silver_fuelhh --output-dir ./my_exports

# Export with a row limit (useful for large datasets)
python scripts/export_to_csv.py --view silver_system_prices --limit 1000

Available Views (after running the pipeline)
--------------------------------------------

Silver views (one per source/dataset):
  silver_system_prices       Elexon system buy/sell prices (half-hourly)
  silver_fuelhh              Elexon fuel-type generation (half-hourly)
  silver_boal                Elexon bid-offer acceptance levels
  silver_bod                 Elexon bid-offer data
  silver_mid                 Elexon market index data
  silver_freq                Elexon system frequency
  silver_demand_forecast     Elexon demand forecasts (NDF)
  silver_wind_forecast       Elexon wind generation forecasts
  silver_pn                  Elexon physical notifications
  silver_disbsad             Elexon disaggregated BSAD
  silver_bmunits             Elexon BM unit reference data
  silver_historical_demand   Open-Meteo historical weather (7 demand sites)
  silver_historical_wind     Open-Meteo historical weather (12 wind sites)
  silver_historical_solar    Open-Meteo historical weather (6 solar sites)
  silver_forecast_demand     Open-Meteo weather forecasts (7 demand sites)
  silver_forecast_wind       Open-Meteo weather forecasts (12 wind sites)
  silver_forecast_solar      Open-Meteo weather forecasts (6 solar sites)
  silver_day_ahead_prices    ENTSO-E day-ahead electricity prices
  silver_actual_load         ENTSO-E actual total load
  silver_actual_generation   ENTSO-E actual generation by type
  silver_cross_border_flows  ENTSO-E cross-border physical flows
  silver_storage             GIE AGSI+ gas storage levels
  silver_lng                 GIE ALSI LNG terminal data
  silver_physical_flows      ENTSO-G physical gas flows
  silver_carbon_intensity    NESO carbon intensity (half-hourly)

Gold views (cross-source analytics):
  gold_uk_imbalance_context  UK prices + carbon intensity
  gold_eu_gas_storage        EU gas storage by country
  gold_system_marginal_price System marginal price features
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT / "src"))


def _get_connection():
    """Load settings and return a DuckDB connection with views registered."""
    from gridflow.config.settings import load_settings
    from gridflow.storage.duckdb import get_connection, init_catalogue

    settings = load_settings()
    settings.pipeline.data_dir.mkdir(parents=True, exist_ok=True)
    init_catalogue(settings.pipeline.duckdb_path, settings.pipeline.data_dir)
    con = get_connection(settings.pipeline.duckdb_path)
    return con, settings


def list_views(con) -> list[str]:
    """Return all view and table names from the DuckDB catalogue."""
    result = con.sql(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'main' "
        "ORDER BY table_name"
    ).fetchall()
    return [row[0] for row in result]


def _detect_date_column(con, view_name: str) -> str | None:
    """Detect the best date/timestamp column for filtering in a view."""
    try:
        cols = con.sql(
            f"SELECT column_name FROM information_schema.columns "
            f"WHERE table_name = '{view_name}' AND table_schema = 'main' "
            f"ORDER BY ordinal_position"
        ).fetchall()
        col_names = [row[0] for row in cols]
    except Exception:
        return None

    # Prefer these columns in order
    for candidate in ["settlement_date", "gas_day", "timestamp_utc", "date"]:
        if candidate in col_names:
            return candidate
    return None


def export_view(
    con,
    view_name: str,
    output_dir: Path,
    start: str | None = None,
    end: str | None = None,
    limit: int | None = None,
) -> Path | None:
    """Export a single view to a CSV file. Returns the output path or None on failure."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{view_name}.csv"

    # Build the query
    where_parts: list[str] = []

    if start or end:
        date_col = _detect_date_column(con, view_name)
        if date_col:
            # Cast to DATE for timestamp columns to enable simple date filtering
            cast = f"{date_col}::DATE" if "timestamp" in date_col else date_col
            if start and end:
                where_parts.append(f"{cast} BETWEEN '{start}' AND '{end}'")
            elif start:
                where_parts.append(f"{cast} >= '{start}'")
            elif end:
                where_parts.append(f"{cast} <= '{end}'")

    where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
    limit_clause = f" LIMIT {limit}" if limit else ""

    query = f"SELECT * FROM {view_name}{where_clause}{limit_clause}"

    try:
        # Use DuckDB COPY for efficient CSV export
        con.execute(
            f"COPY ({query}) TO '{str(out_path)}' (FORMAT CSV, HEADER true)"
        )
        # Get the row count
        row_count = con.sql(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
        print(f"  ✓ {view_name} → {out_path}  ({row_count:,} rows)")
        return out_path
    except Exception as exc:
        print(f"  ✗ {view_name} — skipped: {exc}")
        return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export gridflow silver/gold DuckDB views to CSV files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python scripts/export_to_csv.py --list\n"
            "  python scripts/export_to_csv.py --view silver_system_prices\n"
            "  python scripts/export_to_csv.py --view silver_system_prices --start 2024-01-15 --end 2024-01-16\n"
            "  python scripts/export_to_csv.py --all --output-dir ./my_exports\n"
            "  python scripts/export_to_csv.py --view silver_fuelhh --limit 500\n"
        ),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--list", action="store_true", help="List all available views and tables")
    group.add_argument("--view", help="Export a specific view by name")
    group.add_argument("--all", action="store_true", help="Export all views to CSV")
    parser.add_argument("--output-dir", default="exports", help="Output directory (default: exports/)")
    parser.add_argument("--start", help="Filter start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="Filter end date (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, help="Maximum number of rows to export per view")

    args = parser.parse_args()

    con, settings = _get_connection()

    try:
        views = list_views(con)

        if args.list:
            print(f"\nAvailable views/tables ({len(views)}):\n")
            for v in views:
                # Categorise by prefix
                if v.startswith("silver_"):
                    category = "silver"
                elif v.startswith("gold_"):
                    category = "gold"
                else:
                    category = "table"
                print(f"  [{category:6s}]  {v}")
            print()
            return

        output_dir = Path(args.output_dir)

        if args.view:
            if args.view not in views:
                print(f"Error: view '{args.view}' not found.", file=sys.stderr)
                print(f"Available views: {', '.join(views)}", file=sys.stderr)
                sys.exit(1)
            print(f"\nExporting to {output_dir}/\n")
            export_view(con, args.view, output_dir, args.start, args.end, args.limit)

        elif args.all:
            print(f"\nExporting {len(views)} views to {output_dir}/\n")
            exported = 0
            for v in views:
                result = export_view(con, v, output_dir, args.start, args.end, args.limit)
                if result:
                    exported += 1
            print(f"\n{exported}/{len(views)} views exported successfully.")

    finally:
        con.close()

    print("\nDone.")


if __name__ == "__main__":
    main()
