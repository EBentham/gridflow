"""Silver transformer for Elexon Actual Aggregated Generation Per Type (AGPT / B1620)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import polars as pl

from gridflow.schemas.elexon import ElexonAGPT
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.published_at import with_published_at
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc_expr

logger = logging.getLogger(__name__)


class AGPTTransformer(ElexonTransformer):
    """Transform Elexon AGPT data from bronze to silver."""

    source = "elexon"
    dataset = "agpt"
    schema_cls = ElexonAGPT
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period", "psr_type")  # D-8

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        column_mapping = {
            "settlementDate": "settlement_date",
            "settlementPeriod": "settlement_period",
            "publishTime": "published_at",
            "psrType": "psr_type",
            "quantity": "generation_mw",
            "businessType": "business_type",
            "documentId": "document_id",
            "documentRevisionNumber": "document_revision",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["settlement_date", "settlement_period", "psr_type", "generation_mw"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in AGPT: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns(
            [
                pl.col("settlement_date").cast(pl.Date),
                pl.col("settlement_period").cast(pl.Int32),
                pl.col("generation_mw").cast(pl.Float64),
                pl.col("psr_type").cast(pl.Utf8),
            ]
        )

        df = df.with_columns(
            settlement_period_to_utc_expr("settlement_date", "settlement_period").alias(
                "timestamp_utc"
            )
        )

        # G6 (W2.2 pattern): cast `published_at` to UTC datetime so the
        # column survives to silver well-typed.
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
            "psr_type",
            "generation_mw",
            "business_type",
            "document_id",
            "document_revision",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc", "psr_type")


register_transformer("elexon", "agpt", AGPTTransformer)
