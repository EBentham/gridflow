"""Silver transformer for Elexon National Demand Forecast (NDF/NDFD)."""

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


class DemandForecastTransformer(BaseSilverTransformer):
    """Transform Elexon NDF/NDFD data from bronze to silver.

    Registered for both 'ndf' and 'ndfd' datasets with a forecast_type
    column to distinguish day-ahead from 2-14 day.
    """

    source = "elexon"
    dataset = "ndf"
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
            "nationalDemand": "national_demand_mw",
            "demand": "national_demand_mw",
            "transmissionSystemDemand": "transmission_demand_mw",
            "publishDateTime": "published_at",
            "publishTime": "published_at",
            "forecastDate": "forecast_date",
            "startTime": "start_time",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        # national_demand_mw is required
        if "national_demand_mw" not in raw_df.columns:
            logger.error("Missing required columns in NDF: ['national_demand_mw']")
            return pl.DataFrame()

        has_sp = "settlement_date" in raw_df.columns and "settlement_period" in raw_df.columns

        # For NDFD: use forecast_date as settlement_date if no settlement columns
        if not has_sp and "forecast_date" in raw_df.columns:
            raw_df = raw_df.with_columns(
                pl.col("forecast_date").alias("settlement_date")
            )
            # NDFD has no settlement period — set to 1 as placeholder
            raw_df = raw_df.with_columns(pl.lit(1).alias("settlement_period"))
            has_sp = True

        if not has_sp:
            missing = []
            if "settlement_date" not in raw_df.columns:
                missing.append("settlement_date")
            if "settlement_period" not in raw_df.columns:
                missing.append("settlement_period")
            logger.error(f"Missing required columns in NDF: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns([
            pl.col("settlement_date").cast(pl.Date),
            pl.col("settlement_period").cast(pl.Int32),
            pl.col("national_demand_mw").cast(pl.Float64),
        ])

        if "transmission_demand_mw" in df.columns:
            df = df.with_columns(pl.col("transmission_demand_mw").cast(pl.Float64))

        if "published_at" in df.columns:
            df = df.with_columns(
                pl.col("published_at")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                .dt.replace_time_zone("UTC")
            )
        else:
            # WHY: the silver schema declares published_at as a nullable contract
            # column. Emit it as typed-null when bronze lacks the publish field so the
            # silver schema is deterministic and partition globs don't drift across
            # history (a missing column breaks SELECT * reads spanning files that do
            # carry it).
            df = df.with_columns(
                pl.lit(None).cast(pl.Datetime("us", "UTC")).alias("published_at")
            )

        # Derive timestamp from settlement date/period or start_time
        if has_sp:
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

        forecast_type = "day_ahead" if self.dataset == "ndf" else "2_14_day"
        df = df.with_columns(pl.lit(forecast_type).alias("forecast_type"))

        dedup_cols = ["settlement_date", "settlement_period", "forecast_type"]
        if "published_at" in df.columns:
            dedup_cols.append("published_at")
        df = df.unique(subset=dedup_cols, keep="last")

        now = datetime.now(UTC)
        df = df.with_columns([
            pl.lit("elexon").alias("data_provider"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        ])

        output_cols = [
            "settlement_date", "settlement_period", "timestamp_utc",
            "forecast_type", "national_demand_mw", "transmission_demand_mw",
            "published_at", "data_provider", "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


class NDFDTransformer(DemandForecastTransformer):
    """Transform Elexon NDFD (2-14 day ahead demand forecast)."""

    dataset = "ndfd"


register_transformer("elexon", "ndf", DemandForecastTransformer)
register_transformer("elexon", "ndfd", NDFDTransformer)
