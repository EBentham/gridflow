"""Silver transformer for Elexon Physical Notifications (PN)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import polars as pl

from gridflow.schemas.elexon import ElexonPN
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc

logger = logging.getLogger(__name__)


class PNTransformer(ElexonTransformer):
    """Transform Elexon PN data from bronze to silver."""

    source = "elexon"
    dataset = "pn"
    schema_cls = ElexonPN
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period", "bm_unit_id")  # D-8

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        column_mapping = {
            "settlementDate": "settlement_date",
            "settlementPeriod": "settlement_period",
            "bmUnit": "bm_unit_id",
            "levelFrom": "level_from",
            "levelTo": "level_to",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["settlement_date", "settlement_period", "bm_unit_id"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in PN: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns(
            [
                pl.col("settlement_date").cast(pl.Date),
                pl.col("settlement_period").cast(pl.Int32),
                pl.col("bm_unit_id").cast(pl.Utf8),
            ]
        )

        for col in ["level_from", "level_to"]:
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

        df = df.unique(
            subset=["settlement_date", "settlement_period", "bm_unit_id"],
            keep="last",
        )

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
            "bm_unit_id",
            "level_from",
            "level_to",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc", "bm_unit_id")


register_transformer("elexon", "pn", PNTransformer)
