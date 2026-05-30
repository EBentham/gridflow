"""Silver transformer for ENTSO-E day-ahead generation forecast aggregated (A71/A01)."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.entsoe._published_at import with_published_at
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class GenerationForecastTransformer(BaseSilverTransformer):
    """Transform ENTSO-E day-ahead generation forecast XML from bronze to silver.

    A71/A01 (day-ahead generation forecast aggregated by technology).
    Each TimeSeries carries ``MktPSRType / psrType`` for the generation type.
    The ``quantity`` value is the forecast generation in MW.
    """

    source = "entsoe"
    dataset = "generation_forecast"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        bronze_path = self._bronze_path_for_date(target_date)
        if bronze_path is None:
            return pl.DataFrame()

        rows: list[dict[str, Any]] = []
        for f in sorted(bronze_path.glob("raw_*.xml")):
            if f.name.endswith(".meta.json"):
                continue
            try:
                records = parse_timeseries_xml(f.read_bytes(), value_tag="quantity")
                rows.extend(records)
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to parse ENTSO-E XML %s: %s", f, exc)

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        required = ["timestamp_utc", "value", "in_domain"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error("Missing required columns in generation_forecast: %s", missing)
            return pl.DataFrame()

        df = raw_df.rename(
            {"value": "generation_forecast_mw", "in_domain": "area_code"}
        )

        if df["timestamp_utc"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(
                pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC"))
            )

        df = df.with_columns(pl.col("generation_forecast_mw").cast(pl.Float64))

        if "production_type" in df.columns:
            df = df.with_columns(
                pl.col("production_type")
                .fill_null("unknown")
                .alias("production_type")
            )

        df = df.unique(
            subset=["timestamp_utc", "area_code", "production_type"], keep="last"
        )

        # Issue 04: carry the document publication vintage (createdDateTime)
        # as published_at; typed-null when the source lacks it.
        df = with_published_at(df)

        now = datetime.now(UTC)
        df = df.with_columns([
            pl.lit("entsoe").alias("data_provider"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        ])

        output_cols = [
            "timestamp_utc", "area_code", "production_type",
            "generation_forecast_mw", "resolution", "published_at",
            "data_provider", "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc", "area_code", "production_type")


register_transformer("entsoe", "generation_forecast", GenerationForecastTransformer)
