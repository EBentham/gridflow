"""Silver transformer for ENTSO-E imbalance prices (A85)."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.schemas.entsoe import EntsoeImbalancePrices
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class ImbalancePricesTransformer(BaseSilverTransformer):
    """Transform ENTSO-E imbalance prices (A85) bronze XML → silver Parquet.

    Distinguishes excess (A19) from shortage (A20) via business_type.
    Deduplicates on (timestamp_utc, area_code, business_type).
    """

    source = "entsoe"
    dataset = "imbalance_prices"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        date_str = target_date.strftime("%Y-%m-%d")
        bronze_dir = Path(self.bronze_dir) / "entsoe" / "imbalance_prices" / date_str
        if not bronze_dir.exists():
            logger.warning("No bronze directory: %s", bronze_dir)
            return pl.DataFrame()

        records: list[dict] = []
        for xml_file in sorted(bronze_dir.glob("raw_*.xml")):
            records.extend(
                parse_timeseries_xml(xml_file.read_bytes(), value_tag="price.amount")
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
                "value": "price_gbp_mwh",
                "control_area_domain": "area_code",
            })
            .select([
                "timestamp_utc",
                "area_code",
                "business_type",
                "price_gbp_mwh",
                "resolution",
            ])
            .unique(subset=["timestamp_utc", "area_code", "business_type"], keep="last")
            .sort(["timestamp_utc", "area_code", "business_type"])
            .with_columns([
                pl.lit("entsoe").alias("data_provider"),
                pl.col("timestamp_utc").dt.replace_time_zone("UTC"),
            ])
        )

        # Contract validation on first row
        if not df.is_empty():
            sample = df.row(0, named=True)
            EntsoeImbalancePrices(**sample)

        return df


register_transformer("entsoe", "imbalance_prices", ImbalancePricesTransformer)
