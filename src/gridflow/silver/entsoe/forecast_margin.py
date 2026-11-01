"""Silver transformer for ENTSO-E year-ahead forecast margin (A70/A33)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.entsoe import EntsoeForecastMargin
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.registry import register_transformer


class ForecastMarginTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E year-ahead forecast margin XML from bronze to silver."""

    source = "entsoe"
    dataset = "forecast_margin"
    schema_cls = EntsoeForecastMargin
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code")
    COLUMN_MAP = {"value": "forecast_margin_mw", "in_domain": "area_code"}
    REQUIRED = ("timestamp_utc", "forecast_margin_mw", "area_code")
    CASTS = {"forecast_margin_mw": pl.Float64()}
    OUTPUT_COLS = (
        "timestamp_utc",
        "area_code",
        "forecast_margin_mw",
        "resolution",
        "published_at",
    )


register_transformer("entsoe", "forecast_margin", ForecastMarginTransformer)
