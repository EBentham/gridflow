"""Silver transformer for ENTSO-E day-ahead prices."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import ClassVar

import polars as pl

from gridflow.schemas.entsoe import EntsoeDayAheadPrice
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.entsoe._published_at import with_published_at
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class DayAheadPricesTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E day-ahead price XML from bronze to silver."""

    source = "entsoe"
    dataset = "day_ahead_prices"
    value_tag = "price.amount"
    schema_cls = EntsoeDayAheadPrice
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code")
    EVENT_WINDOW_FILTER: ClassVar[bool] = True
    """R2-A Task 4 / F-10: the probe-proven over-span case (plan S1.4) --
    opted in first, see ``silver/entsoe/_event_window.py``."""

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        required = ["timestamp_utc", "value", "in_domain"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error("Missing required columns in day_ahead_prices: %s", missing)
            return pl.DataFrame()

        df = raw_df.rename({"value": "price_eur_mwh", "in_domain": "area_code"})

        # Cast timestamp_utc if it came in as Python datetime objects
        if df["timestamp_utc"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC")))

        df = df.with_columns(pl.col("price_eur_mwh").cast(pl.Float64))

        # Issue 05 #1: carry the explicit source currency (parsed from
        # <currency_Unit.name>) so a GBP price is never silently trusted as
        # EUR on the strength of the `price_eur_mwh` column name alone. The
        # value column name is retained for backward compatibility; `currency`
        # is the authoritative denomination. Empty -> "EUR" fallback only when
        # the source omitted currency_Unit (continental default).
        if "currency_unit" in df.columns:
            df = df.with_columns(
                pl.when(pl.col("currency_unit").cast(pl.Utf8).str.strip_chars() != "")
                .then(pl.col("currency_unit").cast(pl.Utf8).str.strip_chars())
                .otherwise(pl.lit("EUR"))
                .alias("currency")
            )
        else:
            df = df.with_columns(pl.lit("EUR").alias("currency"))

        df = self.dedup_to_grain(df)

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("entsoe").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        # ADR-025 P1.1: carry the document publication vintage (createdDateTime) as published_at.
        df = with_published_at(df, dataset=self.dataset)

        output_cols = [
            "timestamp_utc",
            "area_code",
            "price_eur_mwh",
            "currency",
            "resolution",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc", "area_code")


register_transformer("entsoe", "day_ahead_prices", DayAheadPricesTransformer)
