"""Silver transformer for Elexon SO-SO Prices (Cross-Border Interconnector Trading)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import polars as pl

from gridflow.schemas.elexon import ElexonSOSO
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc_expr

logger = logging.getLogger(__name__)


class SOSOTransformer(ElexonTransformer):
    """Transform Elexon SOSO data from bronze to silver."""

    source = "elexon"
    dataset = "soso"
    schema_cls = ElexonSOSO
    ENTITY_KEY_COLUMNS = ("settlement_date", "contract_identification")  # D-8
    DATASET_VERSION = "2.1.0"
    OPTIONAL_ENTITY_KEY_COLUMNS = ("trade_direction", "trader_unit")

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        column_mapping = {
            "settlementDate": "settlement_date",
            "settlementPeriod": "settlement_period",
            "publishTime": "published_at",
            "senderIdentification": "sender_identification",
            "receiverIdentification": "receiver_identification",
            "contractIdentification": "contract_identification",
            "resourceProvider": "resource_provider",
            "tradeDirection": "trade_direction",
            "tradeQuantity": "trade_quantity_mw",
            "tradePrice": "trade_price",
            "traderUnit": "trader_unit",
            "startTime": "start_time",
            "endTime": "end_time",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["settlement_date", "contract_identification"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in SOSO: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns(
            [
                pl.col("settlement_date").cast(pl.Date),
            ]
        )

        if "settlement_period" in df.columns:
            df = df.with_columns(pl.col("settlement_period").cast(pl.Int32))
            df = df.with_columns(
                settlement_period_to_utc_expr("settlement_date", "settlement_period").alias(
                    "timestamp_utc"
                )
            )
        elif "start_time" in df.columns:
            df = df.with_columns(
                pl.col("start_time")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                .dt.replace_time_zone("UTC")
                .alias("timestamp_utc")
            )
        else:
            df = df.with_columns(
                settlement_period_to_utc_expr("settlement_date").alias("timestamp_utc")
            )

        for col in ["trade_quantity_mw", "trade_price"]:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))

        # endTime arrives without a zone marker and the vendor documents none, so
        # the text is kept as sent. end_time below is filled only when a value
        # carries its zone; an unzoned value stays null rather than guessed.
        if "end_time" in df.columns:
            df = df.with_columns(pl.col("end_time").cast(pl.Utf8).alias("end_time_raw"))

        for col in ["end_time", "start_time"]:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                    .dt.replace_time_zone("UTC")
                    .alias(col)
                )

        # F-08: SOSO carries its own independent vendor vintage (publishTime,
        # distinct from timestamp_utc/start_time/end_time). Cast it to UTC
        # datetime (indo.py W2.2 pattern) rather than dropping it at select —
        # there is no "event_time<=available_at" invariant to protect (X2-F03:
        # 802/1022 windfor rows legitimately invert it), so emit unconditionally.
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
            "settlement_period",
            "timestamp_utc",
            "contract_identification",
            "sender_identification",
            "receiver_identification",
            "resource_provider",
            "trade_direction",
            "trade_quantity_mw",
            "trade_price",
            "trader_unit",
            "start_time",
            "end_time",
            "end_time_raw",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "soso", SOSOTransformer)
