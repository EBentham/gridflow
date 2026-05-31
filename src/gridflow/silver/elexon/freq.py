"""Silver transformer for Elexon System Frequency (FREQ)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class FreqTransformer(BaseSilverTransformer):
    """Transform Elexon FREQ data from bronze to silver."""

    source = "elexon"
    dataset = "freq"

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
            "reportDateTime": "timestamp_utc",
            "measurementTime": "timestamp_utc",
            "frequency": "frequency_hz",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["timestamp_utc", "frequency_hz"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in FREQ: {missing}")
            return pl.DataFrame()

        # Polars ≥1.x requires an explicit format when the string contains tz info.
        # Elexon FREQ timestamps are ISO-8601 UTC (e.g. "2024-01-15T00:00:00Z").
        df = raw_df.with_columns(
            [
                pl.col("timestamp_utc")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                .dt.replace_time_zone("UTC"),
                pl.col("frequency_hz").cast(pl.Float64),
            ]
        )

        df = df.unique(subset=["timestamp_utc"], keep="last")

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("elexon").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        output_cols = ["timestamp_utc", "frequency_hz", "data_provider", "ingested_at"]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "freq", FreqTransformer)
