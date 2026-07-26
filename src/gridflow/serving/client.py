"""GridflowClient — Python SDK for querying gridflow data."""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

from gridflow.silver.schema_manifest import BITEMPORAL_EXCLUDE

if TYPE_CHECKING:
    from collections.abc import Sequence
    from datetime import date

    import polars as pl


# WHY: the F0 silver-layer convention adds these bitemporal / partitioning
# columns to every silver parquet view. The user-facing get_* helpers hide
# them via SELECT * EXCLUDE so callers see only the public surface. The public
# authority now lives in gridflow.silver.schema_manifest because downstream
# schema consumers need the same contract without copying literals.
#
# Not every relation carries all six, though: the cross-source gold SQL views
# (gold_eu_gas_storage, gold_uk_imbalance_context) are explicit-column SELECTs
# that carry NONE of them. An unconditional EXCLUDE of absent columns raises
# BinderException, so the helpers EXCLUDE only the bitemporal columns ACTUALLY
# present in the queried relation (see _present_bitemporal_exclude_clause). A
# new public column on either layer still flows through automatically.
_BITEMPORAL_EXCLUDE = BITEMPORAL_EXCLUDE

# WHY (R1-A, F-01): ADR-025 makes available_at the vintage discriminator for
# APPEND_ONLY datasets. A caller reading a vintage-collapsed relation with no
# available_at column cannot tell which vintage it is holding, so the two
# system_prices-derived read paths retain it against the general bitemporal
# EXCLUDE (see _present_bitemporal_exclude_clause's retain= parameter).
_VINTAGE_VISIBLE: tuple[str, ...] = ("available_at",)


class GridflowClient:
    """Client for querying gridflow data via DuckDB.

    Args:
        db_path: Path to the DuckDB catalogue. When omitted, the path comes
            from gridflow settings: process environment
            ``GRIDFLOW_DUCKDB_PATH`` first, then repo-root ``.env``, then
            ``config/settings.yaml``, then the default.

    Usage:
        gf = GridflowClient()
        prices = gf.get_system_prices("2024-01-01", "2024-01-31")
    """

    def __init__(self, db_path: str | Path | None = None):
        if db_path is None:
            from gridflow.config.settings import load_settings

            db_path = load_settings().pipeline.duckdb_path
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

    def _present_bitemporal_exclude_clause(
        self, relation: str, *, retain: Sequence[str] = ()
    ) -> str:
        """Build a ``SELECT *`` EXCLUDE clause for one relation's bitemporal columns.

        Introspects the relation's columns via ``information_schema.columns`` and
        intersects them with :data:`_BITEMPORAL_EXCLUDE`, so only the bitemporal /
        partitioning columns ACTUALLY present are excluded. Silver parquet views
        carry all six and get the full ``EXCLUDE (...)``; the cross-source gold SQL
        views carry none and get an empty string (a plain ``SELECT *``), avoiding
        the ``BinderException`` an unconditional EXCLUDE of absent columns raises.

        Args:
            relation: The unqualified view/table name the caller SELECTs from.
            retain: Bitemporal column(s) to keep VISIBLE despite being present —
                e.g. ``available_at`` on the vintage-collapsed ``system_prices``
                read paths (ADR-025, R1-A/F-01): a caller reading a collapsed
                relation with no vintage discriminator cannot tell which vintage
                it is holding. Converted to a set internally; the emitted clause
                still preserves :data:`_BITEMPORAL_EXCLUDE` order.

        Returns:
            ``" EXCLUDE (col, ...)"`` (leading space, identifier-quoted) when one or
            more bitemporal columns are present and not retained, else ``""``.
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
        retained = set(retain)
        # Preserve _BITEMPORAL_EXCLUDE order for a stable, readable clause.
        to_exclude = [col for col in _BITEMPORAL_EXCLUDE if col in present and col not in retained]
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

        Reads ``silver_elexon_system_prices_latest`` (R1-A/F-01), so this
        returns exactly one row per ``(settlement_date, settlement_period)`` —
        the winning vintage by ``available_at`` then settlement-run rank
        (ADR-025 §2), even with 2+ vintages on disk.

        ``available_at`` is returned as the winning vintage's provenance
        stamp. Filtering it (``available_at <= as_of``) is a **fail-closed
        cutoff, not historical point-in-time selection**: an ``as_of`` that
        falls between vintages returns nothing for that key rather than the
        value that was current then (ADR-025:117-120). A consumer needing
        genuine historical PIT must query the **all-vintage** surface —
        ``silver_elexon_system_prices``, or the deprecated
        ``silver_system_prices`` alias, both of which still return every
        vintage by design — and apply ``available_at <= as_of`` **then
        latest-of-survivors**. That primitive is consumer-side
        (gridflow_models) and is not built here.

        Fails loud, deliberately: if ``silver_elexon_system_prices_latest`` is
        absent (a stale catalogue, or the view dropped on key-column drift)
        this raises ``duckdb.CatalogException`` rather than silently falling
        back to the all-vintage base view and serving stacked vintages. Run
        ``gridflow init`` / refresh the catalogue if this occurs.
        """
        relation = "silver_elexon_system_prices_latest"
        exclude = self._present_bitemporal_exclude_clause(relation, retain=_VINTAGE_VISIBLE)
        sql = (
            "SELECT *" + exclude + " "
            f"FROM {relation} "
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
        (joining silver_elexon_system_prices_latest and
        silver_neso_carbon_intensity) carrying no bitemporal / partitioning
        columns other than the deliberately-projected ``available_at``
        (R1-A/F-01); new columns added to the view surface here
        automatically.

        ``available_at`` is the winning PRICE vintage's provenance stamp — it
        does not gate the carbon-intensity columns (see the view's leakage
        comment for the carbon-intensity realised/forecast distinction).
        Filtering it (``available_at <= as_of``) is a **fail-closed cutoff,
        not historical point-in-time selection**: an ``as_of`` between
        vintages returns nothing for that key rather than the value current
        then (ADR-025:117-120). Genuine historical PIT needs the all-vintage
        silver surfaces followed by ``available_at <= as_of`` then
        latest-of-survivors — consumer-side, not built here.
        """
        exclude = self._present_bitemporal_exclude_clause(
            "gold_uk_imbalance_context", retain=_VINTAGE_VISIBLE
        )
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
