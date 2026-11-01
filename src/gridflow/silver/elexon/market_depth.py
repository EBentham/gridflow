"""Silver transformer for Elexon Settlement Market Depth."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import polars as pl

from gridflow.schemas.elexon import ElexonMarketDepth
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc_expr

logger = logging.getLogger(__name__)


class MarketDepthTransformer(ElexonTransformer):
    """Transform Elexon market depth data from bronze to silver."""

    source = "elexon"
    dataset = "market_depth"
    schema_cls = ElexonMarketDepth
    ENTITY_KEY_COLUMNS = (
        "settlement_date",
        "settlement_period",
    )  # D-8

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
            "pricedAcceptedOffersVolume": "priced_accepted_offers_volume_mwh",
            "pricedAcceptedBidsVolume": "priced_accepted_bids_volume_mwh",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["settlement_date", "settlement_period"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in market_depth: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns(
            [
                pl.col("settlement_date").cast(pl.Date),
                pl.col("settlement_period").cast(pl.Int32),
            ]
        )

        numeric_cols = [
            "indicated_imbalance_mwh",
            "offer_volume_mwh",
            "bid_volume_mwh",
            "total_accepted_offer_volume_mwh",
            "total_accepted_bid_volume_mwh",
            "priced_accepted_offers_volume_mwh",
            "priced_accepted_bids_volume_mwh",
        ]
        for col in numeric_cols:
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
            "indicated_imbalance_mwh",
            "offer_volume_mwh",
            "bid_volume_mwh",
            "total_accepted_offer_volume_mwh",
            "total_accepted_bid_volume_mwh",
            "priced_accepted_offers_volume_mwh",
            "priced_accepted_bids_volume_mwh",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "market_depth", MarketDepthTransformer)
