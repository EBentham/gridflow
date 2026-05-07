"""Silver transformer for Elexon Wind Generation Forecast (WINDFOR)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any, ClassVar

import polars as pl

from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc

logger = logging.getLogger(__name__)


class WindForecastTransformer(BaseSilverTransformer):
    """Transform Elexon WINDFOR data from bronze to silver."""

    source = "elexon"
    dataset = "windfor"
    DATASET_VERSION: ClassVar[str] = "1.0.0"

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
            "initialForecast": "initial_forecast_mw",
            "latestForecast": "latest_forecast_mw",
            "generation": "latest_forecast_mw",
            "publishDateTime": "published_at",
            "publishTime": "published_at",
            "startTimeOfHalfHrPeriod": "start_time",
            "startTime": "start_time",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        # Try to derive timestamp from settlement date/period or start_time
        has_sp = "settlement_date" in raw_df.columns and "settlement_period" in raw_df.columns

        if not has_sp and "start_time" not in raw_df.columns:
            logger.error(
                "Missing required columns in WINDFOR: need settlement_date/period or start_time"
            )
            return pl.DataFrame()

        df = raw_df

        if has_sp:
            df = df.with_columns([
                pl.col("settlement_date").cast(pl.Date),
                pl.col("settlement_period").cast(pl.Int32),
            ])
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
            # Polars ≥1.x requires explicit format when string contains tz info.
            df = df.with_columns(
                pl.col("start_time")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                .dt.replace_time_zone("UTC")
                .alias("timestamp_utc")
            )

        for col in ["initial_forecast_mw", "latest_forecast_mw"]:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))

        if "published_at" in df.columns:
            df = df.with_columns(
                pl.col("published_at")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                .dt.replace_time_zone("UTC")
                .alias("issue_time")
            )

        dedup_cols = ["timestamp_utc"]
        if "settlement_date" in df.columns and "settlement_period" in df.columns:
            dedup_cols = ["settlement_date", "settlement_period"]
        if "issue_time" in df.columns:
            dedup_cols.append("issue_time")
        df = df.unique(subset=dedup_cols, keep="last")

        now = datetime.now(UTC)
        df = df.with_columns([
            pl.lit("elexon").alias("data_provider"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        ])

        output_cols = [
            "settlement_date", "settlement_period", "timestamp_utc",
            "initial_forecast_mw", "latest_forecast_mw",
            "issue_time", "data_provider", "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "windfor", WindForecastTransformer)
