"""GridflowClient — Python SDK for querying gridflow data."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import polars as pl


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
                f"DuckDB catalogue not found at {self._db_path}. "
                f"Run 'gridflow init' to create it."
            )
        self._con = duckdb.connect(str(self._db_path), read_only=True)

    def query(self, sql: str) -> pl.DataFrame:
        """Execute a SQL query and return results as a Polars DataFrame."""
        return self._con.sql(sql).pl()

    def get_system_prices(
        self,
        start: str | date,
        end: str | date,
    ) -> pl.DataFrame:
        """Get system sell/buy prices for a date range.

        Returns DataFrame with columns:
            timestamp_utc, system_sell_price, system_buy_price,
            net_imbalance_volume, run_type
        """
        return self.query(f"""
            SELECT timestamp_utc, system_sell_price, system_buy_price,
                   net_imbalance_volume, run_type
            FROM silver_system_prices
            WHERE settlement_date BETWEEN '{start}' AND '{end}'
            ORDER BY timestamp_utc
        """)

    def get_generation_by_fuel(
        self,
        start: str | date,
        end: str | date,
        country: str = "GB",
    ) -> pl.DataFrame:
        """Get generation by fuel type for a date range.

        Returns DataFrame with columns:
            timestamp_utc, fuel_type, generation_mw
        """
        return self.query(f"""
            SELECT timestamp_utc, fuel_type, generation_mw
            FROM silver_generation_by_fuel
            WHERE settlement_date BETWEEN '{start}' AND '{end}'
            ORDER BY timestamp_utc, fuel_type
        """)

    def get_fuel_generation(
        self,
        start: str | date,
        end: str | date,
    ) -> pl.DataFrame:
        """Get half-hourly fuel generation mix for the GB grid.

        Returns DataFrame with columns:
            timestamp_utc, settlement_date, settlement_period,
            fuel_type, generation_mw, data_provider
        """
        return self.query(f"""
            SELECT timestamp_utc, settlement_date, settlement_period,
                   fuel_type, generation_mw, data_provider
            FROM silver_fuelhh
            WHERE settlement_date BETWEEN '{start}' AND '{end}'
            ORDER BY timestamp_utc, fuel_type
        """)

    def get_gas_storage(
        self,
        start: str | date,
        end: str | date,
        country_code: str | None = None,
    ) -> pl.DataFrame:
        """Get EU gas storage levels from GIE AGSI+.

        Returns DataFrame with columns:
            gas_day, country_code, country_name, gas_in_storage_gwh,
            withdrawal_gwh, injection_gwh, working_gas_volume_gwh,
            storage_pct_full, trend
        """
        where_clauses = [f"gas_day BETWEEN '{start}' AND '{end}'"]
        if country_code:
            where_clauses.append(f"country_code = '{country_code}'")
        where = " AND ".join(where_clauses)
        return self.query(f"""
            SELECT gas_day, country_code, country_name,
                   gas_in_storage_gwh, withdrawal_gwh, injection_gwh,
                   working_gas_volume_gwh, storage_pct_full, trend
            FROM gold_eu_gas_storage
            WHERE {where}
            ORDER BY gas_day DESC, country_code
        """)

    def get_weather(
        self,
        start: str | date,
        end: str | date,
        location: str | None = None,
    ) -> pl.DataFrame:
        """Get historical weather observations from Open-Meteo.

        Returns DataFrame with columns:
            timestamp_utc, location, latitude, longitude,
            temperature_2m, wind_speed_10m, precipitation, hdd, cdd
        """
        where_clauses = [f"timestamp_utc::DATE BETWEEN '{start}' AND '{end}'"]
        if location:
            where_clauses.append(f"location = '{location}'")
        where = " AND ".join(where_clauses)
        return self.query(f"""
            SELECT timestamp_utc, location, latitude, longitude,
                   temperature_2m, wind_speed_10m, precipitation, hdd, cdd
            FROM silver_historical
            WHERE {where}
            ORDER BY timestamp_utc, location
        """)

    def get_imbalance_context(
        self,
        start: str | date,
        end: str | date,
    ) -> pl.DataFrame:
        """Get UK imbalance context combining prices and carbon intensity.

        Returns DataFrame with columns:
            timestamp_utc, settlement_date, settlement_period,
            system_sell_price, system_buy_price, net_imbalance_volume,
            run_type, carbon_intensity_forecast_gco2_kwh,
            carbon_intensity_actual_gco2_kwh, intensity_index
        """
        return self.query(f"""
            SELECT timestamp_utc, settlement_date, settlement_period,
                   system_sell_price, system_buy_price, net_imbalance_volume,
                   run_type,
                   carbon_intensity_forecast_gco2_kwh,
                   carbon_intensity_actual_gco2_kwh,
                   intensity_index
            FROM gold_uk_imbalance_context
            WHERE settlement_date BETWEEN '{start}' AND '{end}'
            ORDER BY timestamp_utc, run_type
        """)

    def get_tables(self) -> list[str]:
        """List all available tables and views."""
        result = self._con.sql(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' "
            "ORDER BY table_name"
        ).fetchall()
        return [row[0] for row in result]

    def close(self) -> None:
        """Close the DuckDB connection."""
        self._con.close()

    def __enter__(self) -> GridflowClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"GridflowClient(db_path='{self._db_path}')"
