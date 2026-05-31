"""Gold builder for system marginal price dataset.

Combines system prices with contextual data for modelling.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import polars as pl

from gridflow.gold.base import BaseGoldBuilder
from gridflow.storage.parquet import read_parquet_dir

if TYPE_CHECKING:
    from datetime import date

logger = logging.getLogger(__name__)


class SystemMarginalPriceBuilder(BaseGoldBuilder):
    """Build system marginal price gold dataset from silver system prices."""

    gold_dataset = "system_marginal_price"

    def build(self, start_date: date, end_date: date) -> pl.DataFrame:
        """Build enriched system price dataset."""
        silver_dir = self.data_dir / "silver" / "elexon" / "system_prices"

        df = read_parquet_dir(silver_dir)
        if df.is_empty():
            return df

        # Filter to date range
        if "settlement_date" in df.columns:
            df = df.filter(
                (pl.col("settlement_date") >= start_date) & (pl.col("settlement_date") <= end_date)
            )

        if df.is_empty():
            return df

        # Enrich with derived features
        df = df.with_columns(
            [
                # Spread between buy and sell price
                (pl.col("system_buy_price") - pl.col("system_sell_price")).alias("spread"),
                # Absolute imbalance
                pl.col("net_imbalance_volume").abs().alias("abs_imbalance"),
            ]
        )

        # Add time-based features if timestamp_utc exists.
        # `day_of_week` convention: Polars `dt.weekday()` is ISO, 1=Mon..7=Sun.
        # NOTE: the gridflow_models calendar feature uses Python `weekday()`
        # (0=Mon..6=Sun); these two indices differ by one and must be
        # reconciled at the cross-repo seam (tracked against the calendar-
        # feature remediation). Pinned here by test_day_of_week_convention_iso.
        if "timestamp_utc" in df.columns:
            df = df.with_columns(
                [
                    pl.col("timestamp_utc").dt.hour().alias("hour_of_day"),
                    pl.col("timestamp_utc").dt.weekday().alias("day_of_week"),
                ]
            )

        return df.sort("timestamp_utc") if "timestamp_utc" in df.columns else df
