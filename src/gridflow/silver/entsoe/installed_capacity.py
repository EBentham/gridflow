"""Silver transformer for ENTSO-E installed generation capacity aggregated (A68/A33)."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class InstalledCapacityTransformer(BaseSilverTransformer):
    """Transform ENTSO-E installed generation capacity XML from bronze to silver.

    A68/A33 (installed capacity aggregated by technology).  Resolution is
    typically P1Y (annual) or P1M.  Each TimeSeries carries ``MktPSRType /
    psrType`` to identify the generation technology.  The ``quantity`` value is
    the capacity_mw: Total installed capacity in MW.
    """

    source = "entsoe"
    dataset = "installed_capacity"

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
            logger.error("Missing required columns in installed_capacity: %s", missing)
            return pl.DataFrame()

        df = raw_df.rename(
            {"value": "capacity_mw", "in_domain": "area_code"}
        )

        if df["timestamp_utc"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(
                pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC"))
            )

        df = df.with_columns(pl.col("capacity_mw").cast(pl.Float64))

        if "production_type" in df.columns:
            df = df.with_columns(
                pl.col("production_type")
                .fill_null("unknown")
                .alias("production_type")
            )

        df = df.unique(
            subset=["timestamp_utc", "area_code", "production_type"], keep="last"
        )

        now = datetime.now(UTC)
        df = df.with_columns([
            pl.lit("entsoe").alias("data_provider"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        ])

        output_cols = [
            "timestamp_utc", "area_code", "production_type",
            "capacity_mw", "resolution", "data_provider", "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc", "area_code", "production_type")


register_transformer("entsoe", "installed_capacity", InstalledCapacityTransformer)
