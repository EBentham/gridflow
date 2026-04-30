"""Silver transformer for ENTSO-E activated balancing energy quantity (A83/A16)."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.schemas.entsoe import EntsoeActivatedBalancingQty
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class ActivatedBalancingQtyTransformer(BaseSilverTransformer):
    """Transform ENTSO-E activated balancing quantity (A83/A16) bronze XML → silver Parquet.

    Distinguishes upward (A95) from downward (A96) via business_type.
    Deduplicates on (timestamp_utc, area_code, business_type).
    """

    source = "entsoe"
    dataset = "activated_balancing_qty"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        bronze_path = (
            self.bronze_dir
            / str(target_date.year)
            / f"{target_date.month:02d}"
            / f"{target_date.day:02d}"
        )
        if not bronze_path.exists():
            logger.warning("No bronze directory: %s", bronze_path)
            return pl.DataFrame()

        records: list[dict] = []
        for xml_file in sorted(bronze_path.glob("raw_*.xml")):
            records.extend(
                parse_timeseries_xml(xml_file.read_bytes(), value_tag="quantity")
            )

        if not records:
            return pl.DataFrame()

        return pl.DataFrame(records).with_columns(
            pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC"))
        )

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        available = set(raw_df.columns)
        required = {"timestamp_utc", "value", "control_area_domain", "business_type", "resolution"}
        if not required.issubset(available):
            missing = required - available
            logger.error("Missing columns in bronze data: %s", missing)
            return pl.DataFrame()

        df = (
            raw_df.rename({
                "value": "quantity_mwh",
                "control_area_domain": "area_code",
            })
            .select([
                "timestamp_utc",
                "area_code",
                "business_type",
                "quantity_mwh",
                "resolution",
            ])
            .unique(subset=["timestamp_utc", "area_code", "business_type"], keep="last")
            .sort(["timestamp_utc", "area_code", "business_type"])
            .with_columns([
                pl.lit("entsoe").alias("data_provider"),
                pl.col("timestamp_utc").dt.replace_time_zone("UTC"),
            ])
        )

        if not df.is_empty():
            sample = df.row(0, named=True)
            EntsoeActivatedBalancingQty(**sample)

        return df


register_transformer("entsoe", "activated_balancing_qty", ActivatedBalancingQtyTransformer)
