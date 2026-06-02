"""Silver transformer for ENTSO-E actual generation per production type."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.connectors.entsoe.area_codes import area_name_for
from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.schemas.entsoe import EntsoeActualGeneration
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class ActualGenerationTransformer(BaseSilverTransformer):
    """Transform ENTSO-E actual generation XML from bronze to silver."""

    source = "entsoe"
    dataset = "actual_generation"
    schema_cls = EntsoeActualGeneration

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
            logger.error("Missing required columns in actual_generation: %s", missing)
            return pl.DataFrame()

        df = raw_df.rename({"value": "generation_mw", "in_domain": "area_code"})

        if df["timestamp_utc"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC")))

        df = df.with_columns(pl.col("generation_mw").cast(pl.Float64))

        # G9 ENTSOE-03: populate `area_name` from the EIC mRID via the
        # canonical lookup in connectors/entsoe/area_codes.py. Codes
        # outside the canonical set resolve to an empty string so the
        # column type stays consistent (`area_name: str = ""` per schema).
        df = df.with_columns(
            pl.col("area_code").map_elements(area_name_for, return_dtype=pl.Utf8).alias("area_name")
        )

        # production_type comes from parser; fill empty with "unknown"
        if "production_type" in df.columns:
            df = df.with_columns(
                pl.col("production_type").fill_null("unknown").alias("production_type")
            )

        df = df.unique(subset=["timestamp_utc", "area_code", "production_type"], keep="last")

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("entsoe").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        output_cols = [
            "timestamp_utc",
            "area_code",
            "area_name",
            "production_type",
            "generation_mw",
            "resolution",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc", "area_code", "production_type")


register_transformer("entsoe", "actual_generation", ActualGenerationTransformer)
