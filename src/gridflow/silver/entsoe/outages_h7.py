"""Silver transformers for H7 ENTSO-E outage extension datasets."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.schemas.entsoe import (
    EntsoeOutagesConsumption,
    EntsoeOutagesOffshoreGrid,
    EntsoeOutagesProduction,
    EntsoeOutagesTransmission,
)
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class _H7OutageTransformer(BaseSilverTransformer):
    """Shared outage transformer preserving document and asset metadata."""

    value_tag = "quantity"
    schema_cls = EntsoeOutagesConsumption
    output_cols: list[str] = []
    required_domain_cols: set[str] = {"in_domain"}
    rename_cols: dict[str, str] = {"in_domain": "area_code"}
    dedup_subset: list[str] = []

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        bronze_path = self._bronze_path_for_date(target_date)
        if bronze_path is None:
            return pl.DataFrame()

        rows: list[dict[str, Any]] = []
        for xml_file in sorted(bronze_path.glob("raw_*.xml")):
            if xml_file.name.endswith(".meta.json"):
                continue
            try:
                rows.extend(
                    parse_timeseries_xml(
                        xml_file.read_bytes(),
                        value_tag=self.value_tag,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to parse ENTSO-E XML %s: %s", xml_file, exc)

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        required = {"timestamp_utc", "value"} | self.required_domain_cols
        missing = required - set(raw_df.columns)
        if missing:
            logger.error("Missing required columns in %s: %s", self.dataset, missing)
            return pl.DataFrame()

        df = raw_df.rename({"value": "unavailable_mw", **self.rename_cols})
        for column in (
            "business_type",
            "document_mrid",
            "document_status",
            "timeseries_mrid",
            "resolution",
            "unit_mrid",
            "unit_name",
            "production_type",
            "asset_mrid",
            "asset_name",
        ):
            if column not in df.columns:
                df = df.with_columns(pl.lit("").alias(column))

        if df["timestamp_utc"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC")))

        df = df.with_columns(
            [
                pl.col("unavailable_mw").cast(pl.Float64),
                pl.when(pl.col("business_type") == "A53")
                .then(pl.lit("planned"))
                .when(pl.col("business_type") == "A54")
                .then(pl.lit("unplanned"))
                .otherwise(pl.col("business_type"))
                .alias("outage_type"),
                pl.lit("entsoe").alias("data_provider"),
                pl.lit(datetime.now(UTC)).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        df = df.unique(subset=self.dedup_subset, keep="last").sort(self.dedup_subset)
        df = df.select(self.output_cols)

        return df


class OutagesConsumptionTransformer(_H7OutageTransformer):
    source = "entsoe"
    dataset = "outages_consumption"
    schema_cls = EntsoeOutagesConsumption
    output_cols = [
        "timestamp_utc",
        "area_code",
        "outage_type",
        "unavailable_mw",
        "business_type",
        "document_mrid",
        "document_status",
        "timeseries_mrid",
        "resolution",
        "data_provider",
        "ingested_at",
    ]
    dedup_subset = ["timestamp_utc", "area_code", "business_type", "timeseries_mrid"]


class OutagesTransmissionTransformer(_H7OutageTransformer):
    source = "entsoe"
    dataset = "outages_transmission"
    schema_cls = EntsoeOutagesTransmission
    required_domain_cols = {"in_domain", "out_domain"}
    rename_cols = {"in_domain": "in_area_code", "out_domain": "out_area_code"}
    output_cols = [
        "timestamp_utc",
        "in_area_code",
        "out_area_code",
        "asset_mrid",
        "asset_name",
        "outage_type",
        "unavailable_mw",
        "business_type",
        "document_mrid",
        "document_status",
        "timeseries_mrid",
        "resolution",
        "data_provider",
        "ingested_at",
    ]
    dedup_subset = [
        "timestamp_utc",
        "in_area_code",
        "out_area_code",
        "asset_mrid",
        "timeseries_mrid",
    ]


class OutagesOffshoreGridTransformer(_H7OutageTransformer):
    source = "entsoe"
    dataset = "outages_offshore_grid"
    schema_cls = EntsoeOutagesOffshoreGrid
    output_cols = [
        "timestamp_utc",
        "area_code",
        "asset_mrid",
        "asset_name",
        "outage_type",
        "unavailable_mw",
        "business_type",
        "document_mrid",
        "document_status",
        "timeseries_mrid",
        "resolution",
        "data_provider",
        "ingested_at",
    ]
    dedup_subset = ["timestamp_utc", "area_code", "asset_mrid", "timeseries_mrid"]


class OutagesProductionTransformer(_H7OutageTransformer):
    source = "entsoe"
    dataset = "outages_production"
    schema_cls = EntsoeOutagesProduction
    output_cols = [
        "timestamp_utc",
        "area_code",
        "unit_mrid",
        "unit_name",
        "production_type",
        "outage_type",
        "unavailable_mw",
        "business_type",
        "document_mrid",
        "document_status",
        "timeseries_mrid",
        "resolution",
        "data_provider",
        "ingested_at",
    ]
    dedup_subset = ["timestamp_utc", "area_code", "unit_mrid", "timeseries_mrid"]


_TRANSFORMERS = [
    OutagesConsumptionTransformer,
    OutagesTransmissionTransformer,
    OutagesOffshoreGridTransformer,
    OutagesProductionTransformer,
]

for transformer_cls in _TRANSFORMERS:
    register_transformer("entsoe", transformer_cls.dataset, transformer_cls)
