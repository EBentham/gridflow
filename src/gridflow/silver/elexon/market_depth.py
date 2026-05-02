"""Silver transformer for Elexon Settlement Market Depth."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc

logger = logging.getLogger(__name__)


class MarketDepthTransformer(BaseSilverTransformer):
    """Transform Elexon market depth data from bronze to silver."""

    source = "elexon"
    dataset = "market_depth"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        bronze_path = (
            self.bronze_dir
            / str(target_date.year)
            / f"{target_date.month:02d}"
            / f"{target_date.day:02d}"
        )
        if not bronze_path.exists():
            return pl.DataFrame()

        rows: list[dict[str, Any]] = []
        for f in sorted(bronze_path.glob("raw_*.json")):
            if f.name.endswith(".meta.json"):
                continue
            try:
                data = json.loads(f.read_text())
                records = data.get("data", []) if isinstance(data, dict) else data
                rows.extend(records)
            except (json.JSONDecodeError, AttributeError) as e:
                logger.warning(f"Failed to parse bronze file {f}: {e}")
                continue

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        column_mapping = {
            "settlementDate": "settlement_date",
            "settlementPeriod": "settlement_period",
            "indicatedImbalance": "indicated_imbalance_mwh",
            "offerVolume": "offer_volume_mwh",
            "bidVolume": "bid_volume_mwh",
            "totalAcceptedOfferVolume": "total_accepted_offer_volume_mwh",
            "totalAcceptedBidVolume": "total_accepted_bid_volume_mwh",
            "totalAdjustmentSellVolume": "total_adjustment_sell_volume_mwh",
            "totalAdjustmentBuyVolume": "total_adjustment_buy_volume_mwh",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["settlement_date", "settlement_period"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in market_depth: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns([
            pl.col("settlement_date").cast(pl.Date),
            pl.col("settlement_period").cast(pl.Int32),
        ])

        numeric_cols = [
            "indicated_imbalance_mwh", "offer_volume_mwh", "bid_volume_mwh",
            "total_accepted_offer_volume_mwh", "total_accepted_bid_volume_mwh",
            "total_adjustment_sell_volume_mwh", "total_adjustment_buy_volume_mwh",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))

        df = df.with_columns(
            pl.struct(["settlement_date", "settlement_period"])
            .map_elements(
                lambda row: settlement_period_to_utc(
                    row["settlement_date"], row["settlement_period"]
                ),
                return_dtype=pl.Datetime("us", "UTC"),
            )
            .alias("timestamp_utc")
        )

        df = df.unique(subset=["settlement_date", "settlement_period"], keep="last")

        now = datetime.now(UTC)
        df = df.with_columns([
            pl.lit("elexon").alias("data_provider"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        ])

        output_cols = [
            "settlement_date", "settlement_period", "timestamp_utc",
            "indicated_imbalance_mwh", "offer_volume_mwh", "bid_volume_mwh",
            "total_accepted_offer_volume_mwh", "total_accepted_bid_volume_mwh",
            "total_adjustment_sell_volume_mwh", "total_adjustment_buy_volume_mwh",
            "data_provider", "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "market_depth", MarketDepthTransformer)
