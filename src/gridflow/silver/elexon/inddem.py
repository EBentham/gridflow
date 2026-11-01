"""Silver transformer for Elexon Day and Day-Ahead Indicated Demand (INDDEM)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import polars as pl

from gridflow.schemas.elexon import ElexonIndDem
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc

logger = logging.getLogger(__name__)


class INDDEMTransformer(ElexonTransformer):
    """Transform Elexon INDDEM data from bronze to silver."""

    source = "elexon"
    dataset = "inddem"
    schema_cls = ElexonIndDem
    ENTITY_KEY_COLUMNS = (
        "settlement_date",
        "settlement_period",
    )  # D-8: verbatim from unique() below
    OPTIONAL_ENTITY_KEY_COLUMNS = ("boundary",)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        column_mapping = {
            "settlementDate": "settlement_date",
            "settlementPeriod": "settlement_period",
            "publishTime": "published_at",
            "demand": "indicated_demand_mw",
            "boundary": "boundary",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["settlement_date", "settlement_period", "indicated_demand_mw"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in INDDEM: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns(
            [
                pl.col("settlement_date").cast(pl.Date),
                pl.col("settlement_period").cast(pl.Int32),
                pl.col("indicated_demand_mw").cast(pl.Float64),
            ]
        )

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

        # G6 (W2.2 pattern): cast `published_at` to UTC datetime so the
        # column survives to silver well-typed.
        if "published_at" in df.columns:
            df = df.with_columns(
                pl.col("published_at")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                .dt.replace_time_zone("UTC")
            )
        else:
            # WHY: the silver schema declares published_at as a nullable contract
            # column. Emit it as typed-null when bronze lacks the publish field so the
            # silver schema is deterministic and partition globs don't drift across
            # history (a missing column breaks SELECT * reads spanning files that do
            # carry it).
            df = df.with_columns(pl.lit(None).cast(pl.Datetime("us", "UTC")).alias("published_at"))

        dedup_cols = ["settlement_date", "settlement_period"]
        if "boundary" in df.columns:
            dedup_cols.append("boundary")
        df = df.unique(subset=dedup_cols, keep="last")

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
            "indicated_demand_mw",
            "boundary",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "inddem", INDDEMTransformer)
