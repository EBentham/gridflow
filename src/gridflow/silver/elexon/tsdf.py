"""Silver transformer for Elexon Transmission System Demand Forecast (TSDF)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.schemas.elexon import ElexonTSDF
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc

logger = logging.getLogger(__name__)


class TSDFTransformer(BaseSilverTransformer):
    """Transform Elexon TSDF data from bronze to silver."""

    source = "elexon"
    dataset = "tsdf"
    schema_cls = ElexonTSDF
    ENTITY_KEY_COLUMNS = (
        "settlement_date",
        "settlement_period",
    )  # D-8: verbatim from unique() below
    OPTIONAL_ENTITY_KEY_COLUMNS = ("boundary",)

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
            "demand": "forecast_demand_mw",
            "boundary": "boundary",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["settlement_date", "settlement_period", "forecast_demand_mw"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in TSDF: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns(
            [
                pl.col("settlement_date").cast(pl.Date),
                pl.col("settlement_period").cast(pl.Int32),
                pl.col("forecast_demand_mw").cast(pl.Float64),
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

        # F-08: TSDF carries its own independent vendor vintage (publishTime,
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
            "forecast_demand_mw",
            "boundary",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "tsdf", TSDFTransformer)
