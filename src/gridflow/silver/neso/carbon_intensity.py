"""Silver transformer for NESO / National Grid Carbon Intensity data."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class CarbonIntensityTransformer(BaseSilverTransformer):
    """Transform NESO carbon intensity JSON from bronze to silver."""

    source = "neso"
    dataset = "carbon_intensity"

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
                payload = json.loads(f.read_text())
                # Carbon Intensity API wraps in "data" array
                raw_records = (
                    payload.get("data", []) if isinstance(payload, dict) else []
                )
                for record in raw_records:
                    row: dict[str, Any] = {
                        "from": record.get("from"),
                        "to": record.get("to"),
                    }
                    intensity = record.get("intensity", {}) or {}
                    row["forecast"] = intensity.get("forecast")
                    row["actual"] = intensity.get("actual")
                    row["index"] = intensity.get("index", "")
                    rows.append(row)
            except (json.JSONDecodeError, AttributeError, TypeError) as exc:
                logger.warning(
                    "Failed to parse carbon intensity bronze file %s: %s", f, exc
                )

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        required = ["from"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(
                "Missing required columns in carbon intensity: %s", missing
            )
            return pl.DataFrame()

        df = raw_df

        # Parse the half-period start time as UTC
        df = df.with_columns(
            pl.col("from")
            .str.to_datetime(
                format="%Y-%m-%dT%H:%MZ",
                time_unit="us",
                strict=False,
            )
            .dt.replace_time_zone("UTC")
            .alias("timestamp_utc")
        )

        # Rename intensity columns
        rename_map: dict[str, str] = {}
        if "forecast" in df.columns:
            rename_map["forecast"] = "forecast_gco2_kwh"
        if "actual" in df.columns:
            rename_map["actual"] = "actual_gco2_kwh"
        if "index" in df.columns:
            rename_map["index"] = "intensity_index"
        if rename_map:
            df = df.rename(rename_map)

        for col in ["forecast_gco2_kwh", "actual_gco2_kwh"]:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))

        df = df.unique(subset=["timestamp_utc"], keep="last")

        now = datetime.now(UTC)
        df = df.with_columns([
            pl.lit("neso").alias("data_provider"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        ])

        output_cols = [
            "timestamp_utc",
            "forecast_gco2_kwh", "actual_gco2_kwh",
            "intensity_index",
            "data_provider", "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("neso", "carbon_intensity", CarbonIntensityTransformer)
