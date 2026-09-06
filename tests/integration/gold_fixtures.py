"""Shared silver seeds for strict registration of all gold SQL views."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from pathlib import Path


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def seed_silver(data_dir: Path, *, include_vintage_policy: bool = False) -> None:
    """Seed all silver inputs the real gold SQL views bind against.

    - ``gie_agsi/storage`` supplies ``gold_eu_gas_storage``.
    - Empty ``elexon/mid`` supplies ``gold_gb_day_ahead_benchmark``; callers
      choose explicitly whether its schema includes ``vintage_policy``.
    - ``elexon/system_prices`` + ``neso/carbon_intensity`` are required only so
      the co-registered ``uk_imbalance_context.sql`` binds cleanly under strict
      mode (they are otherwise irrelevant to this assertion).
    """
    mid = pl.DataFrame(
        schema={
            "timestamp_utc": pl.Datetime("us", "UTC"),
            "settlement_date": pl.Date,
            "settlement_period": pl.Int32,
            "market_index_price": pl.Float64,
            "market_index_volume": pl.Float64,
            "data_provider_id": pl.String,
            "available_at": pl.Datetime("us", "UTC"),
        }
    )
    if include_vintage_policy:
        mid = mid.with_columns(pl.lit(None, dtype=pl.String).alias("vintage_policy"))
    _write_parquet(
        mid,
        PathBuilder(data_dir).silver_file("elexon", "mid", date(2024, 1, 15)),
    )
    storage = pl.DataFrame(
        {
            "gas_day": [date(2024, 1, 15), date(2024, 1, 15)],
            "country_code": ["DE", "FR"],
            "country_name": ["Germany", "France"],
            "gas_in_storage_gwh": [1000.0, 800.0],
            "withdrawal_gwh": [10.0, 8.0],
            "injection_gwh": [0.0, 0.0],
            "working_gas_volume_gwh": [2000.0, 1600.0],
            "storage_pct_full": [50.0, 50.0],
            "trend": [-0.5, -0.5],
            "data_provider": ["GIE", "GIE"],
            "ingested_at": ["2024-01-16T06:00:00Z", "2024-01-16T06:00:00Z"],
        }
    )
    _write_parquet(
        storage,
        PathBuilder(data_dir).silver_file("gie_agsi", "storage", date(2024, 1, 15)),
    )

    # Minimal seeds so uk_imbalance_context.sql binds under strict mode.
    # available_at is required here too (R1-A): the gold view now reads
    # silver_elexon_system_prices_latest, which _register_views only builds
    # when the seed carries a key + at least one order/rank column.
    _write_parquet(
        pl.DataFrame(
            {
                "timestamp_utc": ["2024-01-15T00:00:00Z"],
                "settlement_date": [date(2024, 1, 15)],
                "settlement_period": [1],
                "system_sell_price": [45.5],
                "system_buy_price": [55.0],
                "net_imbalance_volume": [-120.5],
                "price_derivation_code": ["A"],
                "available_at": ["2024-01-16T06:00:00Z"],
            }
        ),
        PathBuilder(data_dir).silver_file("elexon", "system_prices", date(2024, 1, 15)),
    )
    _write_parquet(
        pl.DataFrame(
            {
                "timestamp_utc": ["2024-01-15T00:00:00Z"],
                "forecast_gco2_kwh": [200.0],
                "actual_gco2_kwh": [195.0],
                "intensity_index": ["moderate"],
            }
        ),
        PathBuilder(data_dir).silver_file("neso", "carbon_intensity", date(2024, 1, 15)),
    )
