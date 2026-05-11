"""Silver transformer for ENTSO-E contracted reserves (A81)."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.schemas.entsoe import EntsoeContractedReserves
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class ContractedReservesTransformer(BaseSilverTransformer):
    """Transform ENTSO-E contracted reserves (A81) bronze XML → silver Parquet.

    Maps businessType A95→"fcr", A96→"afrr", A97→"mfrr", A98→"rr" via replace_strict.
    Deduplicates on (timestamp_utc, area_code, reserve_type).
    """

    source = "entsoe"
    dataset = "contracted_reserves"

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        bronze_path = self._bronze_path_for_date(target_date)
        if bronze_path is None:
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

        now = datetime.now(UTC)
        df = (
            raw_df.rename({
                "value": "quantity_mw",
                "control_area_domain": "area_code",
            })
            .with_columns(
                pl.col("business_type").replace_strict(
                    {"A95": "fcr", "A96": "afrr", "A97": "mfrr", "A98": "rr"}
                ).alias("reserve_type")
            )
            .select([
                "timestamp_utc",
                "area_code",
                "reserve_type",
                "quantity_mw",
                "resolution",
            ])
            .unique(subset=["timestamp_utc", "area_code", "reserve_type"], keep="last")
            .sort(["timestamp_utc", "area_code", "reserve_type"])
            .with_columns([
                pl.lit("entsoe").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
                pl.col("timestamp_utc").dt.replace_time_zone("UTC"),
            ])
        )

        output_cols = [
            "timestamp_utc", "area_code", "reserve_type",
            "quantity_mw", "resolution", "data_provider", "ingested_at",
        ]
        available_cols = [c for c in output_cols if c in df.columns]
        df = df.select(available_cols)

        if not df.is_empty():
            sample = df.row(0, named=True)
            EntsoeContractedReserves(**sample)

        return df


register_transformer("entsoe", "contracted_reserves", ContractedReservesTransformer)
