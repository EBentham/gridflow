"""GridflowClient — Python SDK for querying gridflow data."""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

if TYPE_CHECKING:
    from datetime import date

    import polars as pl

# WHY: the F0 silver-layer convention adds these bitemporal / partitioning
# columns to every silver and gold view. The user-facing get_* helpers hide
# them via SELECT * EXCLUDE so callers see only the public surface.
# A schema drift on the silver side (column added to public surface, or
# bitemporal column removed) surfaces loudly: a column added flows through
# automatically; a column removed from EXCLUDE raises BinderException.
_BITEMPORAL_EXCLUDE = (
    "event_time",
    "available_at",
    "source_run_id",
    "dataset_version",
    "month",
    "year",
)
_BITEMPORAL_EXCLUDE_SQL = ", ".join(_BITEMPORAL_EXCLUDE)


class GridflowClient:
    """Client for querying gridflow data via DuckDB.

    Usage:
        gf = GridflowClient()
        prices = gf.get_system_prices("2024-01-01", "2024-01-31")
    """

    def __init__(self, db_path: str | Path = "data/gridflow.duckdb"):
        self._db_path = Path(db_path)
        if not self._db_path.exists():
            raise FileNotFoundError(
                "DuckDB catalogue not found at "
                + str(self._db_path)
                + ". Run 'gridflow init' to create it."
            )
        self._con: duckdb.DuckDBPyConnection | None = duckdb.connect(
            str(self._db_path), read_only=True
        )

    def _require_con(self) -> duckdb.DuckDBPyConnection:
        # WHY: close() can leave _con as None; every query path must
        # surface a clear error rather than an opaque AttributeError if
        # callers reach for the connection after closing it.
        if self._con is None:
            raise RuntimeError(
                "GridflowClient connection is closed. "
                "Call reopen_readonly() before issuing queries."
            )
        return self._con

    def query(self, sql: str) -> pl.DataFrame:
        """Execute a SQL query and return results as a Polars DataFrame."""
        return self._require_con().sql(sql).pl()

    def get_system_prices(
        self,
        start: str | date,
        end: str | date,
    ) -> pl.DataFrame:
        """Get system sell/buy prices for a date range.

        Returns a Polars DataFrame with the live silver_system_prices
        public schema (bitemporal / partitioning columns excluded). The
        column set is what the silver transformer publishes today; new
        columns added to the silver layer surface here automatically.
        """
        sql = (
            "SELECT * EXCLUDE (" + _BITEMPORAL_EXCLUDE_SQL + ") "
            "FROM silver_system_prices "
            "WHERE settlement_date BETWEEN ? AND ? "
            "ORDER BY timestamp_utc"
        )
        return self._require_con().execute(sql, [str(start), str(end)]).pl()

    def get_generation_by_fuel(
        self,
        start: str | date,
        end: str | date,
        country: str = "GB",
    ) -> pl.DataFrame:
        """Get generation by fuel type for a date range.

        .. deprecated::
            ``silver_generation_by_fuel`` was a duplicate of
            ``silver_fuelhh`` and was removed from the silver registry
            (see ``gridflow/silver/elexon/__init__.py``). This method
            now queries ``silver_fuelhh`` and emits a DeprecationWarning.
            Call :meth:`get_fuel_generation` instead, which returns the
            full silver_fuelhh public schema.
        """
        warnings.warn(
            "GridflowClient.get_generation_by_fuel() is deprecated; "
            "the underlying silver_generation_by_fuel view was removed "
            "(it duplicated silver_fuelhh). Call get_fuel_generation() "
            "instead. This shim queries silver_fuelhh under the hood.",
            DeprecationWarning,
            stacklevel=2,
        )
        sql = (
            "SELECT timestamp_utc, fuel_type, generation_mw "
            "FROM silver_fuelhh "
            "WHERE settlement_date BETWEEN ? AND ? "
            "ORDER BY timestamp_utc, fuel_type"
        )
        return self._require_con().execute(sql, [str(start), str(end)]).pl()

    def get_fuel_generation(
        self,
        start: str | date,
        end: str | date,
    ) -> pl.DataFrame:
        """Get half-hourly fuel generation mix for the GB grid.

        Returns a Polars DataFrame with the live silver_fuelhh public
        schema (bitemporal / partitioning columns excluded).
        """
        sql = (
            "SELECT * EXCLUDE (" + _BITEMPORAL_EXCLUDE_SQL + ") "
            "FROM silver_fuelhh "
            "WHERE settlement_date BETWEEN ? AND ? "
            "ORDER BY timestamp_utc, fuel_type"
        )
        return self._require_con().execute(sql, [str(start), str(end)]).pl()

    def get_gas_storage(
        self,
        start: str | date,
        end: str | date,
        country_code: str | None = None,
    ) -> pl.DataFrame:
        """Get EU gas storage levels from GIE AGSI+.

        Returns a Polars DataFrame with the gold_eu_gas_storage public
        schema (bitemporal / partitioning columns excluded).
        """
        params: list[str] = [str(start), str(end)]
        country_filter = ""
        if country_code:
            country_filter = " AND country_code = ?"
            params.append(country_code)
        sql = (
            "SELECT * EXCLUDE (" + _BITEMPORAL_EXCLUDE_SQL + ") "
            "FROM gold_eu_gas_storage "
            "WHERE gas_day BETWEEN ? AND ?" + country_filter + " "
            "ORDER BY gas_day DESC, country_code"
        )
        return self._require_con().execute(sql, params).pl()

    def get_weather(
        self,
        start: str | date,
        end: str | date,
        location: str | None = None,
    ) -> pl.DataFrame:
        """Get historical weather observations from Open-Meteo (demand role).

        Returns a Polars DataFrame with the live silver_itsdo public
        schema (demand-role weather; bitemporal / partitioning columns
        excluded). Renamed from silver_historical at F7.5 during the
        wind/solar role-split; new columns surface here automatically.
        """
        params: list[str] = [str(start), str(end)]
        location_filter = ""
        if location:
            location_filter = " AND location = ?"
            params.append(location)
        sql = (
            "SELECT * EXCLUDE (" + _BITEMPORAL_EXCLUDE_SQL + ") "
            "FROM silver_itsdo "
            "WHERE timestamp_utc::DATE BETWEEN ? AND ?" + location_filter + " "
            "ORDER BY timestamp_utc, location"
        )
        return self._require_con().execute(sql, params).pl()

    def get_imbalance_context(
        self,
        start: str | date,
        end: str | date,
    ) -> pl.DataFrame:
        """Get UK imbalance context combining prices and carbon intensity.

        Returns a Polars DataFrame with the gold_uk_imbalance_context
        public schema (bitemporal / partitioning columns excluded). The
        view joins silver_system_prices and silver_carbon_intensity; new
        columns on either side surface here automatically.
        """
        sql = (
            "SELECT * EXCLUDE (" + _BITEMPORAL_EXCLUDE_SQL + ") "
            "FROM gold_uk_imbalance_context "
            "WHERE settlement_date BETWEEN ? AND ? "
            "ORDER BY timestamp_utc"
        )
        return self._require_con().execute(sql, [str(start), str(end)]).pl()

    def get_tables(self) -> list[str]:
        """List all available tables and views."""
        result = (
            self._require_con()
            .sql(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'main' "
                "ORDER BY table_name"
            )
            .fetchall()
        )
        return [row[0] for row in result]

    def close(self) -> None:
        """Close the underlying DuckDB connection. Idempotent.

        Safe to call repeatedly — second and subsequent calls are no-ops.
        Used by gridflow_models.control.refresh's writeable_pipeline_session
        context manager (D-F11-02): the broker calls close() before a
        write phase and reopen_readonly() after.
        """
        if self._con is not None:
            self._con.close()
            self._con = None

    def reopen_readonly(self) -> None:
        """Reopen the read-only DuckDB handle on the same db_path.

        Used by gridflow_models.control.refresh's writeable_pipeline_session
        context manager (D-F11-02): the broker calls close() before the
        write phase and reopen_readonly() after, so the user's bound
        client variable continues to work after the broker hands control
        back. Idempotent: closes any existing connection first.
        """
        if self._con is not None:
            self._con.close()
        self._con = duckdb.connect(str(self._db_path), read_only=True)

    def __enter__(self) -> GridflowClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return "GridflowClient(db_path='" + str(self._db_path) + "')"
