"""Silver transformer for Elexon Initial National Demand Outturn Daily (INDOD)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import polars as pl

from gridflow.schemas.elexon import ElexonINDOD
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc

logger = logging.getLogger(__name__)


class INDODTransformer(ElexonTransformer):
    """Transform Elexon INDOD data from bronze to silver."""

    source = "elexon"
    dataset = "indod"
    schema_cls = ElexonINDOD
    ENTITY_KEY_COLUMNS = ("settlement_date",)  # D-8

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        column_mapping = {
            "settlementDate": "settlement_date",
            "publishTime": "published_at",
            "demand": "initial_demand_outturn_mw",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["settlement_date", "initial_demand_outturn_mw"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in INDOD: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns(
            [
                pl.col("settlement_date").cast(pl.Date),
                pl.col("initial_demand_outturn_mw").cast(pl.Float64),
            ]
        )

        # Daily dataset — timestamp_utc is the settlement-day START (SP1, i.e.
        # 00:00 UK local), so the daily roll-up aligns with its own half-hourly
        # INDO series. A plain UTC-midnight cast would sit 1h off during BST.
        df = df.with_columns(
            pl.col("settlement_date")
            .map_elements(
                lambda d: settlement_period_to_utc(d, 1),
                return_dtype=pl.Datetime("us", "UTC"),
            )
            .alias("timestamp_utc")
        )

        # F-08: INDOD carries its own independent vendor vintage (publishTime,
        # distinct from timestamp_utc). Cast it to UTC datetime (indo.py W2.2
        # pattern) rather than dropping it at select — there is no
        # "event_time<=available_at" invariant to protect (X2-F03: 802/1022
        # windfor rows legitimately invert it), so emit unconditionally.
        if "published_at" in df.columns:
            df = df.with_columns(
                pl.col("published_at")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                .dt.replace_time_zone("UTC")
            )
        else:
            df = df.with_columns(pl.lit(None).cast(pl.Datetime("us", "UTC")).alias("published_at"))

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
            "timestamp_utc",
            "initial_demand_outturn_mw",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "indod", INDODTransformer)
