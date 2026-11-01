"""Silver transformer for ENTSO-E production/generation units master data."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_generation_units_master_data_xml
from gridflow.schemas.entsoe import EntsoeGenerationUnitsMasterData
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.registry import register_transformer

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)


class GenerationUnitsMasterDataTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E generation-unit reference XML to silver."""

    source = "entsoe"
    dataset = "generation_units_master_data"
    schema_cls = EntsoeGenerationUnitsMasterData
    ENTITY_KEY_COLUMNS = ("area_code", "unit_mrid")

    def _parse_bronze_body(self, path: Path) -> list[dict[str, Any]]:
        return parse_generation_units_master_data_xml(path.read_bytes())

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        required = ["area_code", "unit_mrid"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(
                "Missing required columns in generation_units_master_data: %s",
                missing,
            )
            return pl.DataFrame()

        df = raw_df.with_columns(
            [
                pl.col("area_code").fill_null("").alias("area_code"),
                pl.col("unit_mrid").fill_null("").alias("unit_mrid"),
                pl.col("unit_name").fill_null("").alias("unit_name"),
                pl.col("production_type").fill_null("").alias("production_type"),
            ]
        )
        if "implementation_datetime_utc" in df.columns:
            df = df.with_columns(
                pl.col("implementation_datetime_utc").cast(pl.Datetime("us", "UTC"))
            )

        df = df.filter(pl.col("unit_mrid") != "")
        df = self.dedup_to_grain(df)

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("entsoe").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        output_cols = [
            "area_code",
            "unit_mrid",
            "unit_name",
            "production_type",
            "implementation_datetime_utc",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("area_code", "unit_mrid")


register_transformer(
    "entsoe",
    "generation_units_master_data",
    GenerationUnitsMasterDataTransformer,
)
