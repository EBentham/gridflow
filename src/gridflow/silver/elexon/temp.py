"""Silver transformer for Elexon Temperature Data (TEMP)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class TempTransformer(BaseSilverTransformer):
    """Transform Elexon TEMP data from bronze to silver."""

    source = "elexon"
    dataset = "temp"

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
            "publishDateTime": "timestamp_utc",
            "publishTime": "timestamp_utc",
            "measurementDate": "measurement_date",
            "temperature": "temperature",
            "normal": "normal_temperature",
            "low": "low_temperature",
            "high": "high_temperature",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        # Prefer timestamp_utc from publishTime; fall back to measurement_date
        if "timestamp_utc" not in raw_df.columns and "measurement_date" in raw_df.columns:
            raw_df = raw_df.with_columns(
                (pl.col("measurement_date") + "T00:00:00Z").alias("timestamp_utc")
            )
        missing = [c for c in ["timestamp_utc", "temperature"] if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in TEMP: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns(
            pl.col("timestamp_utc")
            .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
            .dt.replace_time_zone("UTC")
        )

        for col in ["temperature", "normal_temperature", "low_temperature", "high_temperature"]:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))

        # G5-W1.4 (2026-05): when `measurementDate` is present in bronze we
        # rename to `measurement_date` above, but it was previously dropped
        # before write. Cast to Date so it survives the select() below.
        if "measurement_date" in df.columns:
            df = df.with_columns(pl.col("measurement_date").cast(pl.Date, strict=False))

        df = df.unique(subset=["timestamp_utc"], keep="last")

        now = datetime.now(UTC)
        df = df.with_columns([
            pl.lit("elexon").alias("data_provider"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        ])

        output_cols = [
            "timestamp_utc", "measurement_date",
            "temperature", "normal_temperature",
            "low_temperature", "high_temperature",
            "data_provider", "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "temp", TempTransformer)
