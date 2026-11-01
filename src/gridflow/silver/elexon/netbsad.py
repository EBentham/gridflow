"""Silver transformer for Elexon Net Balancing Services Adjustment Data (NETBSAD)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import polars as pl

from gridflow.schemas.elexon import ElexonNETBSAD
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc_expr

logger = logging.getLogger(__name__)


class NETBSADTransformer(ElexonTransformer):
    """Transform Elexon NETBSAD data from bronze to silver."""

    source = "elexon"
    dataset = "netbsad"
    schema_cls = ElexonNETBSAD
    ENTITY_KEY_COLUMNS = (
        "settlement_date",
        "settlement_period",
    )  # D-8

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        # G5-W1.1 (2026-05): the NETBSAD live API (verified 2026-05-08)
        # replaced 4 coarse adjustment fields with 8 finer-grained ones —
        # cost vs volume × energy vs system × buy vs sell. These are NOT
        # aggregations of the old 4; they're a different decomposition.
        # Legacy bronze (pre-2026) populates the 4 old columns; current
        # bronze populates the 8 new columns. We emit both column sets
        # when present and rely on bronze era to determine which is
        # populated.
        column_mapping = {
            "settlementDate": "settlement_date",
            "settlementPeriod": "settlement_period",
            # Legacy 4 (pre-2026) — historical bronze only
            "netBuyPriceAdjustment": "net_buy_price_adjustment",
            "netSellPriceAdjustment": "net_sell_price_adjustment",
            "netBuyVolumeAdjustment": "net_buy_volume_adjustment",
            "netSellVolumeAdjustment": "net_sell_volume_adjustment",
            # Current 8 (2026+) — buy side
            "netBuyPriceCostAdjustmentEnergy": "net_buy_price_cost_adjustment_energy",
            "netBuyPriceVolumeAdjustmentEnergy": "net_buy_price_volume_adjustment_energy",
            "netBuyPriceVolumeAdjustmentSystem": "net_buy_price_volume_adjustment_system",
            "buyPricePriceAdjustment": "buy_price_price_adjustment",
            # Current 8 (2026+) — sell side
            "netSellPriceCostAdjustmentEnergy": "net_sell_price_cost_adjustment_energy",
            "netSellPriceVolumeAdjustmentEnergy": "net_sell_price_volume_adjustment_energy",
            "netSellPriceVolumeAdjustmentSystem": "net_sell_price_volume_adjustment_system",
            "sellPricePriceAdjustment": "sell_price_price_adjustment",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["settlement_date", "settlement_period"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in NETBSAD: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns(
            [
                pl.col("settlement_date").cast(pl.Date),
                pl.col("settlement_period").cast(pl.Int32),
            ]
        )

        for col in [
            # Legacy 4
            "net_buy_price_adjustment",
            "net_sell_price_adjustment",
            "net_buy_volume_adjustment",
            "net_sell_volume_adjustment",
            # Current 8 — buy side
            "net_buy_price_cost_adjustment_energy",
            "net_buy_price_volume_adjustment_energy",
            "net_buy_price_volume_adjustment_system",
            "buy_price_price_adjustment",
            # Current 8 — sell side
            "net_sell_price_cost_adjustment_energy",
            "net_sell_price_volume_adjustment_energy",
            "net_sell_price_volume_adjustment_system",
            "sell_price_price_adjustment",
        ]:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))

        df = df.with_columns(
            settlement_period_to_utc_expr("settlement_date", "settlement_period").alias(
                "timestamp_utc"
            )
        )

        df = self.dedup_to_grain(df)

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("elexon").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        output_cols = [
            "settlement_date",
            "settlement_period",
            "timestamp_utc",
            # Legacy 4 — present in pre-2026 bronze
            "net_buy_price_adjustment",
            "net_sell_price_adjustment",
            "net_buy_volume_adjustment",
            "net_sell_volume_adjustment",
            # Current 8 — present in 2026+ bronze
            "net_buy_price_cost_adjustment_energy",
            "net_buy_price_volume_adjustment_energy",
            "net_buy_price_volume_adjustment_system",
            "buy_price_price_adjustment",
            "net_sell_price_cost_adjustment_energy",
            "net_sell_price_volume_adjustment_energy",
            "net_sell_price_volume_adjustment_system",
            "sell_price_price_adjustment",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "netbsad", NETBSADTransformer)
