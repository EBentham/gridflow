"""Silver transformer for ENTSO-E activated balancing energy prices (A84/A16)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import ClassVar

import polars as pl

from gridflow.schemas.entsoe import EntsoeActivatedBalancingPrices
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.entsoe._enum_maps import (
    RESERVE_TYPE,
    count_unmapped,
    currency_expr,
    map_enums,
)
from gridflow.silver.published_at import with_published_at
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)

_ENUMS = {
    "reserve_type": ("business_type", RESERVE_TYPE),
    "direction": ("flow_direction", {"A01": "up", "A02": "down"}),
}


class ActivatedBalancingPricesTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E activated balancing prices (A84/A16) bronze XML → silver Parquet.

    Maps businessType A95→"fcr", A96→"afrr", A97→"mfrr", A98→"rr" via replace_strict.
    Maps flowDirection A01→"up", A02→"down" via replace_strict.
    Deduplicates on (timestamp_utc, area_code, business_type, flow_direction).
    """

    source = "entsoe"
    dataset = "activated_balancing_prices"
    value_tag = "price.amount"
    schema_cls = EntsoeActivatedBalancingPrices
    DATASET_VERSION = "2.0.0"
    # The raw codes, not their labels: two unknown codes stay two rows (ADR-022).
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code", "business_type", "flow_direction")
    EVENT_WINDOW_FILTER: ClassVar[bool] = True
    """B4 (N-9): FILTER_SAFE, vault-recorded live NL 2025-04-01: real
    populated Balancing_MarketDocument, processType=A16 Realised
    (R3-RESEARCH.md:66); window-bound comparison not shown in the abridged
    vault text, see
    ADR-026 Appendix A."""

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        available = set(raw_df.columns)
        required = {
            "timestamp_utc",
            "value",
            "control_area_domain",
            "business_type",
            "flow_direction",
            "resolution",
        }
        if not required.issubset(available):
            missing = required - available
            logger.error("Missing columns in bronze data: %s", missing)
            return pl.DataFrame()

        now = datetime.now(UTC)
        df = raw_df.rename(
            {
                "value": "price_eur_mwh",
                "control_area_domain": "area_code",
            }
        ).with_columns(
            [
                *map_enums(_ENUMS),
                # ENPRICE-04 (VT4): carry the explicit source currency
                # (parsed from <currency_Unit.name>) so a GBP price is never
                # silently trusted as EUR on the strength of the
                # `price_eur_mwh` column name alone. Mirrors day_ahead_prices.
                # Derived here, while `currency_unit` is still in scope before
                # the select() below.
                currency_expr(raw_df).alias("currency"),
            ]
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
                    "flow_direction",
                    "reserve_type",
                    "direction",
                    "price_eur_mwh",
                    "currency",
                    "resolution",
                    "published_at",
                ]
            )
            .pipe(self.dedup_to_grain)
            .sort(["timestamp_utc", "area_code", "reserve_type", "direction"])
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
            "flow_direction",
            "reserve_type",
            "direction",
            "price_eur_mwh",
            "currency",
            "resolution",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        available_cols = [c for c in output_cols if c in df.columns]
        df = df.select(available_cols)

        self.last_unmapped_count = count_unmapped(df, raw_df, _ENUMS, dataset=self.dataset)
        return df


register_transformer("entsoe", "activated_balancing_prices", ActivatedBalancingPricesTransformer)
