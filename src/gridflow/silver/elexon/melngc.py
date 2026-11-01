"""Silver transformer for Elexon Indicated Margin (MELNGC)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import polars as pl

from gridflow.schemas.elexon import ElexonMelNGC
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.published_at import with_published_at
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc_expr

logger = logging.getLogger(__name__)


class MelNGCTransformer(ElexonTransformer):
    """Transform Elexon MELNGC data from bronze to silver."""

    source = "elexon"
    dataset = "melngc"
    schema_cls = ElexonMelNGC
    APPEND_ONLY = True
    DATASET_VERSION = "2.0.0"
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period", "published_at")  # D-8
    OPTIONAL_ENTITY_KEY_COLUMNS = ("boundary",)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        column_mapping = {
            "settlementDate": "settlement_date",
            "settlementPeriod": "settlement_period",
            "publishDateTime": "published_at",
            "publishTime": "published_at",
            "indicatedMargin": "indicated_margin",
            "margin": "indicated_margin",
            "boundary": "boundary",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["settlement_date", "settlement_period", "indicated_margin"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in MELNGC: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns(
            [
                pl.col("settlement_date").cast(pl.Date),
                pl.col("settlement_period").cast(pl.Int32),
                pl.col("indicated_margin").cast(pl.Float64),
            ]
        )

        df = df.with_columns(
            settlement_period_to_utc_expr("settlement_date", "settlement_period").alias(
                "timestamp_utc"
            )
        )

        # G6 (W2.2 pattern): cast `published_at` to UTC datetime so the
        # column survives to silver. ElexonMelNGC declares
        # `published_at: datetime | None`; without this cast and the
        # output_cols entry it was being silently dropped.
        df = with_published_at(df, "published_at", dataset=self.dataset)

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
            "indicated_margin",
            "boundary",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "melngc", MelNGCTransformer)
