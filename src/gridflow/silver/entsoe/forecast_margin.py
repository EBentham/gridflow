"""Silver transformer for ENTSO-E year-ahead forecast margin (A70/A33)."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.schemas.entsoe import EntsoeForecastMargin
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.entsoe._published_at import with_published_at
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class ForecastMarginTransformer(BaseSilverTransformer):
    """Transform ENTSO-E year-ahead forecast margin XML from bronze to silver."""

    source = "entsoe"
    dataset = "forecast_margin"
    schema_cls = EntsoeForecastMargin

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
            logger.error("Missing required columns in forecast_margin: %s", missing)
            return pl.DataFrame()

        df = raw_df.rename({"value": "forecast_margin_mw", "in_domain": "area_code"})

        if df["timestamp_utc"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC")))

        df = df.with_columns(pl.col("forecast_margin_mw").cast(pl.Float64))
        df = df.unique(subset=["timestamp_utc", "area_code"], keep="last")

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("entsoe").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        # ADR-025 P1.1: carry the document publication vintage (createdDateTime) as published_at.
        df = with_published_at(df)

        output_cols = [
            "timestamp_utc",
            "area_code",
            "forecast_margin_mw",
            "resolution",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        df = df.select(available).sort("timestamp_utc", "area_code")

        return df


register_transformer("entsoe", "forecast_margin", ForecastMarginTransformer)
