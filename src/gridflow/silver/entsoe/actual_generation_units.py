"""Silver transformer for ENTSO-E actual generation per generation unit."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import ClassVar

import polars as pl

from gridflow.schemas.entsoe import EntsoeActualGenerationUnits
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.published_at import with_published_at
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class ActualGenerationUnitsTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E actual generation per generation unit XML to silver."""

    source = "entsoe"
    dataset = "actual_generation_units"
    schema_cls = EntsoeActualGenerationUnits
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code", "unit_mrid")
    EVENT_WINDOW_FILTER: ClassVar[bool] = True
    """R2-A Task 4 / F-10 opt-in — see ``silver/entsoe/_event_window.py``."""

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        required = ["timestamp_utc", "value", "in_domain", "unit_mrid"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error("Missing required columns in actual_generation_units: %s", missing)
            return pl.DataFrame()

        df = raw_df.rename({"value": "generation_mw", "in_domain": "area_code"})

        if df["timestamp_utc"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC")))

        df = df.with_columns(
            [
                pl.col("generation_mw").cast(pl.Float64),
                pl.col("unit_mrid").fill_null("").alias("unit_mrid"),
                pl.col("unit_name").fill_null("").alias("unit_name"),
                pl.col("production_type").fill_null("unknown").alias("production_type"),
            ]
        )
        df = df.filter(pl.col("unit_mrid") != "")
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
            "production_type",
            "unit_mrid",
            "unit_name",
            "generation_mw",
            "resolution",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        return df.select(output_cols).sort("timestamp_utc", "area_code", "unit_mrid")


register_transformer("entsoe", "actual_generation_units", ActualGenerationUnitsTransformer)
