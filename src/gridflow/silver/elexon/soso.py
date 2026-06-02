"""Silver transformer for Elexon SO-SO Prices (Cross-Border Interconnector Trading)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.schemas.elexon import ElexonSOSO
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc

logger = logging.getLogger(__name__)


class SOSOTransformer(BaseSilverTransformer):
    """Transform Elexon SOSO data from bronze to silver."""

    source = "elexon"
    dataset = "soso"
    schema_cls = ElexonSOSO

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        bronze_path = (
            self.bronze_dir
            / str(target_date.year)
            / f"{target_date.month:02d}"
            / f"{target_date.day:02d}"
        )
        if not bronze_path.exists():
            return pl.DataFrame()

        rows: list[dict[str, Any]] = []
        for f in sorted(bronze_path.glob("raw_*.json")):
            if f.name.endswith(".meta.json"):
                continue
            try:
                data = json.loads(f.read_text())
                records = data.get("data", []) if isinstance(data, dict) else data
                rows.extend(records)
            except (json.JSONDecodeError, AttributeError) as e:
                logger.warning(f"Failed to parse bronze file {f}: {e}")
                continue

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

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
                pl.struct(["settlement_date", "settlement_period"])
                .map_elements(
                    lambda row: settlement_period_to_utc(
                        row["settlement_date"], row["settlement_period"]
                    ),
                    return_dtype=pl.Datetime("us", "UTC"),
                )
                .alias("timestamp_utc")
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
                pl.col("settlement_date").cast(pl.Datetime("us", "UTC")).alias("timestamp_utc")
            )

        for col in ["trade_quantity_mw", "trade_price"]:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))

        for col in ["end_time", "start_time"]:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                    .dt.replace_time_zone("UTC")
                    .alias(col)
                )

        dedup_cols = ["settlement_date", "contract_identification"]
        if "trade_direction" in df.columns:
            dedup_cols.append("trade_direction")
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
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "soso", SOSOTransformer)
