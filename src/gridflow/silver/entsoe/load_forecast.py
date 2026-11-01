"""Silver transformer for ENTSO-E day-ahead load forecast (A65/A01)."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

import polars as pl

from gridflow.schemas.entsoe import EntsoeLoadForecast
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.registry import register_transformer

if TYPE_CHECKING:
    from pydantic import BaseModel


class LoadForecastTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E day-ahead load forecast XML from bronze to silver.

    ``published_at`` is in the grain: each fetch is an observation an as-of
    read must find. The week-, month- and year-ahead forecasts subclass this
    with their own ``forecast_horizon`` label.
    """

    source = "entsoe"
    dataset = "load_forecast"
    schema_cls: ClassVar[type[BaseModel]] = EntsoeLoadForecast
    APPEND_ONLY = True
    DATASET_VERSION = "2.0.0"
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code", "published_at")
    forecast_horizon = "day_ahead"
    EVENT_WINDOW_FILTER: ClassVar[bool] = True
    """R2-A Task 4 / F-10 opt-in — day-ahead only (A01), NOT the week/month/
    year-ahead siblings (A31/A32/A33 — those stay exempt, see
    ``silver/entsoe/_event_window.py``)."""
    COLUMN_MAP = {"value": "load_forecast_mw", "in_domain": "area_code"}
    REQUIRED = ("timestamp_utc", "load_forecast_mw", "area_code")
    CASTS = {"load_forecast_mw": pl.Float64()}
    OUTPUT_COLS = (
        "timestamp_utc",
        "area_code",
        "load_forecast_mw",
        "resolution",
        "forecast_horizon",
        "published_at",
    )

    def _derive(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns(pl.lit(self.forecast_horizon).alias("forecast_horizon"))


register_transformer("entsoe", "load_forecast", LoadForecastTransformer)
