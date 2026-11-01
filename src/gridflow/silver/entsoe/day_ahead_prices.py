"""Silver transformer for ENTSO-E day-ahead prices."""

from __future__ import annotations

from typing import ClassVar

import polars as pl

from gridflow.schemas.entsoe import EntsoeDayAheadPrice
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.entsoe._enum_maps import currency_expr
from gridflow.silver.registry import register_transformer


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
    COLUMN_MAP = {"value": "price_eur_mwh", "in_domain": "area_code"}
    REQUIRED = ("timestamp_utc", "price_eur_mwh", "area_code")
    CASTS = {"price_eur_mwh": pl.Float64()}
    OUTPUT_COLS = (
        "timestamp_utc",
        "area_code",
        "price_eur_mwh",
        "currency",
        "resolution",
        "published_at",
    )

    def _derive(self, df: pl.DataFrame) -> pl.DataFrame:
        # Issue 05 #1: `currency` is the authoritative denomination, so a GBP
        # price is never trusted as EUR on the strength of the column name.
        return df.with_columns(currency_expr(df).alias("currency"))


register_transformer("entsoe", "day_ahead_prices", DayAheadPricesTransformer)
