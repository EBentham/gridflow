"""Silver transformer for ENTSO-E water reservoirs and hydro storage plants."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import polars as pl

from gridflow.schemas.entsoe import EntsoeWaterReservoirs
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.published_at import with_published_at
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class WaterReservoirsTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E water reservoir XML to silver."""

    source = "entsoe"
    dataset = "water_reservoirs"
    schema_cls = EntsoeWaterReservoirs
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code")

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        required = ["timestamp_utc", "value", "in_domain"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error("Missing required columns in water_reservoirs: %s", missing)
            return pl.DataFrame()

        df = raw_df.rename({"value": "reservoir_mwh", "in_domain": "area_code"})

        if df["timestamp_utc"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC")))

        df = df.with_columns(pl.col("reservoir_mwh").cast(pl.Float64))
        df = self.dedup_to_grain(df)

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("entsoe").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        # ADR-025 P1.1: carry the document publication vintage (createdDateTime) as published_at.
        df = with_published_at(df, dataset=self.dataset)

        output_cols = [
            "timestamp_utc",
            "area_code",
            "reservoir_mwh",
            "resolution",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        return df.select(output_cols).sort("timestamp_utc", "area_code")


register_transformer("entsoe", "water_reservoirs", WaterReservoirsTransformer)
