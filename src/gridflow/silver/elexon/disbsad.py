"""Silver transformer for Elexon Disaggregated Balancing Services Adjustment Data (DISBSAD)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import polars as pl

from gridflow.schemas.elexon import ElexonDISBSAD
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc

logger = logging.getLogger(__name__)


class DISBSADTransformer(ElexonTransformer):
    """Transform Elexon DISBSAD data from bronze to silver."""

    source = "elexon"
    dataset = "disbsad"
    schema_cls = ElexonDISBSAD
    ENTITY_KEY_COLUMNS = (
        "settlement_date",
        "settlement_period",
    )  # D-8: verbatim from unique() below
    OPTIONAL_ENTITY_KEY_COLUMNS = ("adjustment_action_id", "component")

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        # G5-W1.3 (2026-05): live API renamed `component` to `service`. The
        # silver column stays `component` (downstream contract); we map both
        # source keys to it so historical bronze still ingests cleanly.
        # ELEXA-06 (2026-06-04): live API also emits `storFlag` (not the older
        # `storProviderFlag`) — map both so stor_flag is not silently always-False
        # on current bronze. Only one is present per record, so no rename clash.
        column_mapping = {
            "settlementDate": "settlement_date",
            "settlementPeriod": "settlement_period",
            "id": "adjustment_action_id",
            "soFlag": "so_flag",
            "storProviderFlag": "stor_flag",  # legacy (pre-2026)
            "storFlag": "stor_flag",  # current API (ELEXA-06)
            "component": "component",  # legacy (pre-2026)
            "service": "component",  # current API
            "cost": "cost",
            "volume": "volume",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["settlement_date", "settlement_period"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in DISBSAD: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns(
            [
                pl.col("settlement_date").cast(pl.Date),
                pl.col("settlement_period").cast(pl.Int32),
            ]
        )

        for col in ["cost", "volume"]:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))

        # ELEXA-06: the current API returns an integer `id`; the silver contract
        # (ElexonDISBSAD.adjustment_action_id) is a string. Cast so live int ids
        # conform without a fail-soft schema warning (legacy str ids pass through).
        if "adjustment_action_id" in df.columns:
            df = df.with_columns(pl.col("adjustment_action_id").cast(pl.Utf8))

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

        dedup_cols = ["settlement_date", "settlement_period"]
        if "adjustment_action_id" in df.columns:
            dedup_cols.append("adjustment_action_id")
        if "component" in df.columns:
            dedup_cols.append("component")
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
            "adjustment_action_id",
            "so_flag",
            "stor_flag",
            "component",
            "cost",
            "volume",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "disbsad", DISBSADTransformer)
