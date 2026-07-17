"""Silver transformer for ENTSO-E contracted reserves (A81)."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.schemas.entsoe import EntsoeContractedReserves
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.entsoe._enum_maps import UNMAPPED_SENTINEL
from gridflow.silver.entsoe._published_at import with_published_at
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class ContractedReservesTransformer(BaseSilverTransformer):
    """Transform ENTSO-E contracted reserves (A81) bronze XML → silver Parquet.

    Maps businessType A95→"fcr", A96→"afrr", A97→"mfrr", A98→"rr" via replace_strict.
    Deduplicates on (timestamp_utc, area_code, reserve_type).
    """

    source = "entsoe"
    dataset = "contracted_reserves"
    schema_cls = EntsoeContractedReserves

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        bronze_path = self._bronze_path_for_date(target_date)
        if bronze_path is None:
            return pl.DataFrame()

        records: list[dict[str, Any]] = []
        for xml_file in sorted(bronze_path.glob("raw_*.xml")):
            records.extend(parse_timeseries_xml(xml_file.read_bytes(), value_tag="quantity"))

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
            raw_df.rename(
                {
                    "value": "quantity_mw",
                    "control_area_domain": "area_code",
                }
            )
            .with_columns(
                pl.col("business_type")
                .replace_strict(
                    {"A95": "fcr", "A96": "afrr", "A97": "mfrr", "A98": "rr"},
                    default=UNMAPPED_SENTINEL,
                    return_dtype=pl.Utf8,
                )
                .alias("reserve_type")
            )
            .select(
                [
                    "timestamp_utc",
                    "area_code",
                    "reserve_type",
                    "quantity_mw",
                    "resolution",
                ]
            )
            .unique(subset=["timestamp_utc", "area_code", "reserve_type"], keep="last")
            .sort(["timestamp_utc", "area_code", "reserve_type"])
            .with_columns(
                [
                    pl.lit("entsoe").alias("data_provider"),
                    pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
                    pl.col("timestamp_utc").dt.replace_time_zone("UTC"),
                ]
            )
        )

        # ADR-025 P1.1: carry the document publication vintage (createdDateTime) as published_at.
        df = with_published_at(df)

        output_cols = [
            "timestamp_utc",
            "area_code",
            "reserve_type",
            "quantity_mw",
            "resolution",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        available_cols = [c for c in output_cols if c in df.columns]
        df = df.select(available_cols)

        self.last_unmapped_count = int(
            df.filter(pl.col("reserve_type") == UNMAPPED_SENTINEL).height
        )
        if self.last_unmapped_count > 0:
            raw_codes = raw_df.get_column("business_type").unique().to_list()
            unmapped_codes = sorted(c for c in raw_codes if c not in {"A95", "A96", "A97", "A98"})
            logger.warning(
                "%s/%s: %d unmapped business_type row(s) labelled %r; unmapped raw codes: %s",
                self.source,
                self.dataset,
                self.last_unmapped_count,
                UNMAPPED_SENTINEL,
                unmapped_codes,
            )

        return df


register_transformer("entsoe", "contracted_reserves", ContractedReservesTransformer)
