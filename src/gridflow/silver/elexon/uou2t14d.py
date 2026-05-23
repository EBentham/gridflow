"""Silver transformer for Elexon 2-14 Day Generation Availability by BM Unit (UOU2T14D)."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc

logger = logging.getLogger(__name__)


class UOU2T14DTransformer(BaseSilverTransformer):
    """Transform Elexon UOU2T14D data from bronze to silver."""

    source = "elexon"
    dataset = "uou2t14d"

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
            "forecastDate": "settlement_date",
            "publishDateTime": "published_at",
            "publishTime": "published_at",
            "bmUnit": "bm_unit_id",
            "outputUsable": "output_usable_mw",
            "nationalGridBmUnit": "national_grid_bm_unit",
            "fuelType": "fuel_type",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["settlement_date", "bm_unit_id", "output_usable_mw"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in UOU2T14D: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns([
            pl.col("settlement_date").cast(pl.Date),
            pl.col("bm_unit_id").cast(pl.Utf8),
            pl.col("output_usable_mw").cast(pl.Float64),
        ])

        # G5-W2.3 (2026-05): the rename map renames fuelType / nationalGridBmUnit
        # to snake_case, but output_cols previously dropped both, forcing
        # downstream consumers to join against bmunits_reference for fuel
        # context. Restore both so UOU2T14D is self-describing.
        for col in ("fuel_type", "national_grid_bm_unit"):
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Utf8))

        # Cast published_at (already produced by rename map from
        # publishDateTime / publishTime) to UTC-aware datetime.
        if "published_at" in df.columns:
            df = df.with_columns(
                pl.col("published_at")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                .dt.replace_time_zone("UTC")
            )

        # settlement_period may not exist for forecast data (forecastDate only)
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
        else:
            df = df.with_columns(
                pl.col("settlement_date")
                .cast(pl.Datetime("us"))
                .dt.replace_time_zone("UTC")
                .alias("timestamp_utc")
            )

        dedup_cols = ["settlement_date", "bm_unit_id"]
        if "settlement_period" in df.columns:
            dedup_cols.insert(1, "settlement_period")
        df = df.unique(subset=dedup_cols, keep="last")

        now = datetime.now(UTC)
        df = df.with_columns([
            pl.lit("elexon").alias("data_provider"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        ])

        output_cols = [
            "settlement_date", "settlement_period", "timestamp_utc",
            "bm_unit_id", "national_grid_bm_unit", "fuel_type",
            "output_usable_mw", "published_at",
            "data_provider", "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc", "bm_unit_id")


register_transformer("elexon", "uou2t14d", UOU2T14DTransformer)
