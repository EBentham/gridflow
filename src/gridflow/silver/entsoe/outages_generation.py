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
    """Transform ENTSO-E generation unavailability XML from bronze to silver.

    The A80 document is an ``Unavailability_MarketDocument``.  Each TimeSeries
    represents one outage interval: the ``quantity`` value is the *available*
    capacity in MW during that interval.  ``MktPSRType / psrType`` gives the
    generation technology code.

    Note: ENTSO-E A80 XML uses the same generic ``TimeSeries + Period + Point``
    structure as price/load documents, so ``parse_timeseries_xml`` handles it
    without modification.
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

        required = ["timestamp_utc", "value", "in_domain"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error("Missing required columns in outages_generation: %s", missing)
            return pl.DataFrame()

        df = raw_df.rename(
            {"value": "available_capacity_mw", "in_domain": "area_code"}
        )

        if df["timestamp_utc"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(
                pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC"))
            )

        df = df.with_columns(pl.col("available_capacity_mw").cast(pl.Float64))

        if "production_type" in df.columns:
            df = df.with_columns(
                pl.col("production_type")
                .fill_null("")
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
            "available_capacity_mw", "resolution", "data_provider", "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc", "area_code", "production_type")


register_transformer("entsoe", "outages_generation", OutagesGenerationTransformer)
