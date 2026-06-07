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
# columns to every silver parquet view. The user-facing get_* helpers hide
# them via SELECT * EXCLUDE so callers see only the public surface.
#
# Not every relation carries all six, though: the cross-source gold SQL views
# (gold_eu_gas_storage, gold_uk_imbalance_context) are explicit-column SELECTs
# that carry NONE of them. An unconditional EXCLUDE of absent columns raises
# BinderException, so the helpers EXCLUDE only the bitemporal columns ACTUALLY
# present in the queried relation (see _present_bitemporal_exclude_clause). A
# new public column on either layer still flows through automatically.
_BITEMPORAL_EXCLUDE = (
    "event_time",
    "available_at",
    "source_run_id",
    "dataset_version",
    "month",
    "year",
)


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

    def _present_bitemporal_exclude_clause(self, relation: str) -> str:
        """Build a ``SELECT *`` EXCLUDE clause for one relation's bitemporal columns.

        Introspects the relation's columns via ``information_schema.columns`` and
        intersects them with :data:`_BITEMPORAL_EXCLUDE`, so only the bitemporal /
        partitioning columns ACTUALLY present are excluded. Silver parquet views
        carry all six and get the full ``EXCLUDE (...)``; the cross-source gold SQL
        views carry none and get an empty string (a plain ``SELECT *``), avoiding
        the ``BinderException`` an unconditional EXCLUDE of absent columns raises.

        Args:
            relation: The unqualified view/table name the caller SELECTs from.

        Returns:
            ``" EXCLUDE (col, ...)"`` (leading space, identifier-quoted) when one or
            more bitemporal columns are present, else ``""``.
        """
        # WHY: parameterised SQL only — the relation name binds as data against
        # information_schema rather than being interpolated into the query text.
        present = {
            row[0]
            for row in self._require_con()
            .execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
                [relation],
            )
            .fetchall()
        }
        # Preserve _BITEMPORAL_EXCLUDE order for a stable, readable clause.
        to_exclude = [col for col in _BITEMPORAL_EXCLUDE if col in present]
        if not to_exclude:
            return ""
        # WHY: column names come from the curated catalogue, not user input, but
        # quote them defensively so the clause is robust to any future column name.
        quoted = ", ".join('"' + col.replace('"', '""') + '"' for col in to_exclude)
        return " EXCLUDE (" + quoted + ")"

    def get_system_prices(
        self,
        start: str | date,
        end: str | date,
    ) -> pl.DataFrame:
        """Get system sell/buy prices for a date range.

        Returns a Polars DataFrame with the live silver_elexon_system_prices
        public schema (bitemporal / partitioning columns excluded). The
        column set is what the silver transformer publishes today; new
        columns added to the silver layer surface here automatically.
        """
        exclude = self._present_bitemporal_exclude_clause("silver_elexon_system_prices")
        sql = (
            "SELECT *" + exclude + " "
            "FROM silver_elexon_system_prices "
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
            ``silver_elexon_fuelhh`` and was removed from the silver registry
            (see ``gridflow/silver/elexon/__init__.py``). This method
            now queries ``silver_elexon_fuelhh`` and emits a DeprecationWarning.
            Call :meth:`get_fuel_generation` instead, which returns the
            full silver_elexon_fuelhh public schema.
        """
        warnings.warn(
            "GridflowClient.get_generation_by_fuel() is deprecated; "
            "the underlying silver_generation_by_fuel view was removed "
            "(it duplicated silver_elexon_fuelhh). Call get_fuel_generation() "
            "instead. This shim queries silver_elexon_fuelhh under the hood.",
            DeprecationWarning,
            stacklevel=2,
        )
        sql = (
            "SELECT timestamp_utc, fuel_type, generation_mw "
            "FROM silver_elexon_fuelhh "
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

        Returns a Polars DataFrame with the live silver_elexon_fuelhh public
        schema (bitemporal / partitioning columns excluded).
        """
        exclude = self._present_bitemporal_exclude_clause("silver_elexon_fuelhh")
        sql = (
            "SELECT *" + exclude + " "
            "FROM silver_elexon_fuelhh "
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

        Returns a Polars DataFrame with the gold_eu_gas_storage public schema.
        That gold view is an explicit-column cross-source SQL view carrying no
        bitemporal / partitioning columns, so none are excluded here.
        """
        params: list[str] = [str(start), str(end)]
        country_filter = ""
        if country_code:
            country_filter = " AND country_code = ?"
            params.append(country_code)
        exclude = self._present_bitemporal_exclude_clause("gold_eu_gas_storage")
        sql = (
            "SELECT *" + exclude + " "
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
        """Get Elexon ITSDO (Initial Transmission System Demand Outturn).

        Despite the method name, this reads ``silver_elexon_itsdo`` — the GB
        transmission-system DEMAND outturn (MW), not weather. The name is a
        pre-existing misnomer kept for SDK compatibility.

        Returns a Polars DataFrame with the live ``silver_elexon_itsdo`` public
        schema (bitemporal / partitioning columns excluded). The ``location``
        filter and ordering are retained for backward compatibility; new columns
        added to the silver layer surface here automatically.
        """
        params: list[str] = [str(start), str(end)]
        location_filter = ""
        if location:
            location_filter = " AND location = ?"
            params.append(location)
        exclude = self._present_bitemporal_exclude_clause("silver_elexon_itsdo")
        sql = (
            "SELECT *" + exclude + " "
            "FROM silver_elexon_itsdo "
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

        Returns a Polars DataFrame with the gold_uk_imbalance_context public
        schema. That gold view is an explicit-column cross-source SQL view
        (joining silver_elexon_system_prices and silver_neso_carbon_intensity)
        carrying no bitemporal / partitioning columns, so none are excluded
        here; new columns added to the view surface here automatically.
        """
        exclude = self._present_bitemporal_exclude_clause("gold_uk_imbalance_context")
        sql = (
            "SELECT *" + exclude + " "
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
