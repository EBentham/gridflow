"""
Export silver / gold DuckDB views to CSV files for easy analysis and viewing.

Designed for IDE debugging — set breakpoints and run via your IDE's debugger.

Usage Examples
--------------

# List all available views and tables
python scripts/export_to_csv.py --list

# Export a specific silver view to CSV
python scripts/export_to_csv.py --view silver_elexon_system_prices

# Export a view with date filtering
python scripts/export_to_csv.py --view silver_elexon_system_prices \
    --start 2024-01-15 --end 2024-01-16

# Export a gold view
python scripts/export_to_csv.py --view gold_uk_imbalance_context

# Export ALL views at once
python scripts/export_to_csv.py --all

# Export to a custom directory
python scripts/export_to_csv.py --view silver_elexon_fuelhh --output-dir ./my_exports

# Export with a row limit (useful for large datasets)
python scripts/export_to_csv.py --view silver_elexon_system_prices --limit 1000

Available Views (after running the pipeline)
--------------------------------------------

Silver views are source-qualified: silver_{source}_{dataset}. For every dataset
name owned by exactly ONE source, a DEPRECATED legacy unqualified alias
(silver_{dataset}) is also registered as a backward-compat shim; collision names
owned by >1 source get no alias. Prefer the qualified name in new code.

  silver_elexon_system_prices      Elexon system buy/sell prices (half-hourly)
  silver_elexon_fuelhh             Elexon fuel-type generation (half-hourly)
  silver_elexon_boal               Elexon bid-offer acceptance levels
  silver_elexon_bod                Elexon bid-offer data
  silver_elexon_mid                Elexon market index data
  silver_elexon_freq               Elexon system frequency
  silver_elexon_demand_forecast    Elexon demand forecasts (NDF)
  silver_elexon_wind_forecast      Elexon wind generation forecasts
  silver_elexon_pn                 Elexon physical notifications
  silver_elexon_disbsad            Elexon disaggregated BSAD
  silver_elexon_bmunits            Elexon BM unit reference data
  silver_open_meteo_historical_demand   Open-Meteo historical weather (demand sites)
  silver_open_meteo_historical_wind     Open-Meteo historical weather (wind sites)
  silver_open_meteo_historical_solar    Open-Meteo historical weather (solar sites)
  silver_open_meteo_forecast_demand     Open-Meteo weather forecasts (demand sites)
  silver_open_meteo_forecast_wind       Open-Meteo weather forecasts (wind sites)
  silver_open_meteo_forecast_solar      Open-Meteo weather forecasts (solar sites)
  silver_entsoe_day_ahead_prices   ENTSO-E day-ahead electricity prices
  silver_entsoe_actual_load        ENTSO-E actual total load
  silver_entsoe_actual_generation  ENTSO-E actual generation by type
  silver_entsoe_cross_border_flows ENTSO-E cross-border physical flows
  silver_gie_agsi_storage          GIE AGSI+ gas storage levels
  silver_gie_alsi_lng              GIE ALSI LNG terminal data
  silver_entsog_physical_flows     ENTSO-G physical gas flows
  silver_neso_carbon_intensity     NESO carbon intensity (half-hourly)

Gold views (cross-source analytics):
  gold_uk_imbalance_context  UK prices + carbon intensity
  gold_eu_gas_storage        EU gas storage by country
  gold_system_marginal_price System marginal price features
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

# ---------------------------------------------------------------------------
# Ensure the project root is on sys.path
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT / "src"))


def _validate_dates(start: str | None, end: str | None) -> None:
    """Reject `--start` / `--end` values that are not ISO-8601 calendar dates.

    The CLI interpolates these into SQL date literals, so a non-date value is
    both a correctness bug and an injection vector. ``date.fromisoformat``
    raises ``ValueError`` on anything that is not ``YYYY-MM-DD``.

    Args:
        start: Raw ``--start`` value, or ``None`` if unset.
        end: Raw ``--end`` value, or ``None`` if unset.

    Raises:
        ValueError: If either value is non-``None`` and not an ISO date.
    """
    for label, value in (("--start", start), ("--end", end)):
        if value is None:
            continue
        try:
            date.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{label} must be an ISO date (YYYY-MM-DD): {value!r}") from exc


def _validate_limit(limit: int | None) -> int | None:
    """Reject a non-positive `--limit`.

    Args:
        limit: Raw ``--limit`` value, or ``None`` if unset.

    Returns:
        The validated limit (or ``None``).

    Raises:
        ValueError: If ``limit`` is provided and is zero or negative.
    """
    if limit is not None and limit <= 0:
        raise ValueError(f"--limit must be a positive integer: {limit}")
    return limit


def _safe_output_path(output_dir: Path, view_name: str) -> Path:
    """Resolve the CSV output path and confirm it stays inside ``output_dir``.

    The output path is interpolated into a DuckDB ``COPY ... TO '<path>'``
    string literal (DuckDB does not bind the COPY target as a parameter), so a
    ``view_name`` containing ``../`` could redirect the write outside the
    requested directory, and a single quote anywhere in the path could break out
    of the literal and inject SQL. This refuses any path that escapes
    ``output_dir`` or contains a single quote (defense-in-depth alongside the
    quote-doubling applied at the COPY site).

    Args:
        output_dir: Directory the export is allowed to write into.
        view_name: View name used as the CSV file stem.

    Returns:
        The resolved ``<output_dir>/<view_name>.csv`` path.

    Raises:
        ValueError: If the resolved path escapes ``output_dir`` or contains a
            single quote.
    """
    base = output_dir.resolve()
    candidate = (base / f"{view_name}.csv").resolve()
    if not candidate.is_relative_to(base):
        raise ValueError(f"output path escapes {output_dir}: {view_name!r}")
    if "'" in str(candidate):
        raise ValueError(f"output path may not contain a single quote: {str(candidate)!r}")
    return candidate


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
    """Export a single view to a CSV file. Returns the output path or None on failure.

    ``start`` / ``end`` / ``limit`` are bound as DuckDB query parameters rather
    than interpolated, and the output path is contained to ``output_dir``.
    Callers must have validated these via ``_validate_dates`` / ``_validate_limit``
    before reaching here; the containment of the output path is enforced inline.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = _safe_output_path(output_dir, view_name)

    # Build the WHERE clause with placeholders; values are bound, not interpolated.
    where_parts: list[str] = []
    params: list[object] = []

    if start or end:
        date_col = _detect_date_column(con, view_name)
        if date_col:
            # Cast to DATE for timestamp columns to enable simple date filtering.
            cast = f"{date_col}::DATE" if "timestamp" in date_col else date_col
            if start and end:
                where_parts.append(f"{cast} BETWEEN ? AND ?")
                params.extend([start, end])
            elif start:
                where_parts.append(f"{cast} >= ?")
                params.append(start)
            elif end:
                where_parts.append(f"{cast} <= ?")
                params.append(end)

    where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
    limit_clause = " LIMIT ?" if limit else ""
    if limit:
        params.append(limit)

    # view_name is validated against the live catalogue by the caller (main).
    query = f"SELECT * FROM {view_name}{where_clause}{limit_clause}"

    try:
        # Use DuckDB COPY for efficient CSV export. The COPY target is a string
        # literal (DuckDB does not bind it); _safe_output_path contained it and
        # rejected single quotes, and we double any quote (SQL-standard escaping)
        # at the literal as a second layer against breakout.
        out_literal = str(out_path).replace("'", "''")
        con.execute(
            f"COPY ({query}) TO '{out_literal}' (FORMAT CSV, HEADER true)",
            params,
        )
        # Get the row count
        row_count = con.execute(f"SELECT COUNT(*) FROM ({query})", params).fetchone()[0]
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
            "  python scripts/export_to_csv.py --view silver_elexon_system_prices\n"
            "  python scripts/export_to_csv.py --view silver_elexon_system_prices "
            "--start 2024-01-15 --end 2024-01-16\n"
            "  python scripts/export_to_csv.py --all --output-dir ./my_exports\n"
            "  python scripts/export_to_csv.py --view silver_elexon_fuelhh --limit 500\n"
        ),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--list", action="store_true", help="List all available views and tables")
    group.add_argument("--view", help="Export a specific view by name")
    group.add_argument("--all", action="store_true", help="Export all views to CSV")
    parser.add_argument(
        "--output-dir", default="exports", help="Output directory (default: exports/)"
    )
    parser.add_argument("--start", help="Filter start date (YYYY-MM-DD)")
    parser.add_argument("--end", help="Filter end date (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, help="Maximum number of rows to export per view")

    args = parser.parse_args()

    # Validate user-supplied filter values before touching the database so a
    # malformed date / unbounded limit fails fast with a clear, non-zero exit.
    try:
        _validate_dates(args.start, args.end)
        _validate_limit(args.limit)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

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
