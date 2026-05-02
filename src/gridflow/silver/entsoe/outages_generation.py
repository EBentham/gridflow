"""Silver transformer for ENTSO-E unavailability of generation units (A80)."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class OutagesGenerationTransformer(BaseSilverTransformer):
    """Transform ENTSO-E generation unavailability XML (A80) from bronze to silver.

    Each silver row represents one generation unit's unavailable MW for one
    timestamp. unit_mrid (RegisteredResource.mRID) is the unit identity;
    outage_type is mapped from ENTSO-E businessType (A53 -> "planned",
    A54 -> "unplanned"). The XML <quantity> value is the unavailable MW
    during the interval (renamed to unavailable_mw on output).
    """

    source = "entsoe"
    dataset = "outages_generation"

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

        required = ["timestamp_utc", "value", "in_domain", "business_type", "unit_mrid"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error("Missing required columns in outages_generation: %s", missing)
            return pl.DataFrame()

        df = raw_df.rename(
            {"value": "unavailable_mw", "in_domain": "area_code"}
        )

        if df["timestamp_utc"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(
                pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC"))
            )

        df = df.with_columns(pl.col("unavailable_mw").cast(pl.Float64))

        # Map businessType A53 -> "planned", A54 -> "unplanned"
        df = df.with_columns(
            pl.col("business_type").replace_strict(
                {"A53": "planned", "A54": "unplanned"}
            ).alias("outage_type")
        )

        # unit_name may be absent; ensure it is a string column with empty default
        if "unit_name" in df.columns:
            df = df.with_columns(
                pl.col("unit_name").fill_null("").alias("unit_name")
            )
        else:
            df = df.with_columns(pl.lit("").alias("unit_name"))

        # Dedup on (timestamp_utc, unit_mrid) — one row per unit per timestamp
        df = df.unique(
            subset=["timestamp_utc", "unit_mrid"], keep="last"
        )

        now = datetime.now(UTC)
        df = df.with_columns([
            pl.lit("entsoe").alias("data_provider"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        ])

        output_cols = [
            "timestamp_utc", "area_code", "unit_mrid", "unit_name",
            "outage_type", "unavailable_mw", "resolution",
            "data_provider", "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc", "unit_mrid")


register_transformer("entsoe", "outages_generation", OutagesGenerationTransformer)
