"""Silver transformer for Elexon Instantaneous Generation by Fuel Type (FUELINST)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.schemas.elexon import ElexonFuelInst
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class FuelInstTransformer(BaseSilverTransformer):
    """Transform Elexon FUELINST data from bronze to silver."""

    source = "elexon"
    dataset = "fuelinst"
    # ELEXB-05 (VT4): FUELINST output is instantaneous (timestamp_utc only, no
    # settlement coordinates), so its contract is ElexonFuelInst — not
    # ElexonGenerationByFuel, which requires settlement_date + settlement_period
    # the transformer never emits.
    schema_cls = ElexonFuelInst

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
            "publishDateTime": "published_at",
            "publishTime": "published_at",
            "fuelType": "fuel_type",
            "generation": "generation_mw",
            "startTimeOfHalfHrPeriod": "period_start",
            "startTime": "period_start",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["fuel_type", "generation_mw"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in FUELINST: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns(
            [
                pl.col("fuel_type").cast(pl.Utf8),
                pl.col("generation_mw").cast(pl.Float64),
            ]
        )

        # Derive timestamp from published_at or period_start
        if "published_at" in df.columns:
            df = df.with_columns(
                pl.col("published_at")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                .dt.replace_time_zone("UTC")
                .alias("timestamp_utc")
            )
        elif "period_start" in df.columns:
            df = df.with_columns(
                pl.col("period_start")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                .dt.replace_time_zone("UTC")
                .alias("timestamp_utc")
            )
        else:
            logger.error("No timestamp column found in FUELINST data")
            return pl.DataFrame()

        df = df.unique(subset=["timestamp_utc", "fuel_type"], keep="last")

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("elexon").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        output_cols = [
            "timestamp_utc",
            "fuel_type",
            "generation_mw",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc", "fuel_type")


register_transformer("elexon", "fuelinst", FuelInstTransformer)
