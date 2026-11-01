"""Silver transformer for ENTSO-E contracted reserves (A81)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import polars as pl

from gridflow.schemas.entsoe import EntsoeContractedReserves
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.entsoe._enum_maps import UNMAPPED_SENTINEL
from gridflow.silver.published_at import with_published_at
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class ContractedReservesTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E contracted reserves (A81) bronze XML → silver Parquet.

    Maps businessType A95→"fcr", A96→"afrr", A97→"mfrr", A98→"rr" via replace_strict.
    Deduplicates on (timestamp_utc, area_code, business_type).
    """

    source = "entsoe"
    dataset = "contracted_reserves"
    schema_cls = EntsoeContractedReserves
    DATASET_VERSION = "2.0.0"
    # The raw codes, not their labels: two unknown codes stay two rows (ADR-022).
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code", "business_type")

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
        df = raw_df.rename(
            {
                "value": "quantity_mw",
                "control_area_domain": "area_code",
            }
        ).with_columns(
            pl.col("business_type")
            .replace_strict(
                {"A95": "fcr", "A96": "afrr", "A97": "mfrr", "A98": "rr"},
                default=UNMAPPED_SENTINEL,
                return_dtype=pl.Utf8,
            )
            .alias("reserve_type")
        )

        # F-02 fix: derive published_at from document_created_at BEFORE the
        # column-dropping select below, which otherwise discards
        # document_created_at and forces the typed-null branch permanently.
        df = with_published_at(df, dataset=self.dataset)

        df = (
            df.select(
                [
                    "timestamp_utc",
                    "area_code",
                    "business_type",
                    "reserve_type",
                    "quantity_mw",
                    "resolution",
                    "published_at",
                ]
            )
            .pipe(self.dedup_to_grain)
            .sort(["timestamp_utc", "area_code", "reserve_type"])
            .with_columns(
                [
                    pl.lit("entsoe").alias("data_provider"),
                    pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
                    pl.col("timestamp_utc").dt.replace_time_zone("UTC"),
                ]
            )
        )

        output_cols = [
            "timestamp_utc",
            "area_code",
            "business_type",
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
            unmapped_codes = sorted(
                str(c) for c in raw_codes if c not in {"A95", "A96", "A97", "A98"}
            )
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
