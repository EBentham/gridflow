"""Silver transformer for ENTSO-E installed capacity per production unit."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any, ClassVar

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class InstalledCapacityUnitsTransformer(BaseSilverTransformer):
    """Transform ENTSO-E installed capacity per production unit XML to silver.

    Note on timestamps: ``available_at`` is the authoritative bitemporal
    publication timestamp added by ``BaseSilverTransformer``; ``ingested_at``
    is retained for backward compatibility as the local processing
    timestamp. Under ``--reingest`` the two diverge.
    """

    source = "entsoe"
    dataset = "installed_capacity_units"
    DATASET_VERSION: ClassVar[str] = "1.0.0"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        bronze_path = self._bronze_path_for_date(target_date)
        if bronze_path is None:
            return pl.DataFrame()

        rows: list[dict[str, Any]] = []
        for f in sorted(bronze_path.glob("raw_*.xml")):
            if f.name.endswith(".meta.json"):
                continue
            try:
                rows.extend(parse_timeseries_xml(f.read_bytes(), value_tag="quantity"))
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to parse ENTSO-E XML %s: %s", f, exc)

        return pl.DataFrame(rows) if rows else pl.DataFrame()

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        required = ["timestamp_utc", "value", "in_domain", "unit_mrid"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error("Missing required columns in installed_capacity_units: %s", missing)
            return pl.DataFrame()

        df = raw_df.rename({"value": "capacity_mw", "in_domain": "area_code"})

        if df["timestamp_utc"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC")))

        df = df.with_columns(
            [
                pl.col("capacity_mw").cast(pl.Float64),
                pl.col("unit_mrid").fill_null("").alias("unit_mrid"),
                pl.col("unit_name").fill_null("").alias("unit_name"),
                pl.col("production_type").fill_null("unknown").alias("production_type"),
            ]
        )
        df = df.filter(pl.col("unit_mrid") != "")
        df = df.unique(
            subset=["timestamp_utc", "area_code", "unit_mrid"],
            keep="last",
        )

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
            "production_type",
            "unit_mrid",
            "unit_name",
            "capacity_mw",
            "resolution",
            "data_provider",
            "ingested_at",
        ]
        return df.select(output_cols).sort("timestamp_utc", "area_code", "unit_mrid")


register_transformer("entsoe", "installed_capacity_units", InstalledCapacityUnitsTransformer)
