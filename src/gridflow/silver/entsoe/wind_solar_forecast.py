"""Silver transformer for ENTSO-E wind and solar generation forecast (A69/A01)."""

from __future__ import annotations

from typing import ClassVar

import polars as pl

from gridflow.schemas.entsoe import EntsoeWindSolarForecast
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.registry import register_transformer


class WindSolarForecastTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E wind and solar forecast XML from bronze to silver.

    Each TimeSeries carries a MktPSRType / psrType code:
      B16 = Solar, B18 = Wind Offshore, B19 = Wind Onshore.
    ``published_at`` is in the grain: each fetch is an observation an as-of
    read must find.
    """

    source = "entsoe"
    dataset = "wind_solar_forecast"
    schema_cls = EntsoeWindSolarForecast
    APPEND_ONLY = True
    DATASET_VERSION = "2.0.0"
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code", "production_type", "published_at")
    EVENT_WINDOW_FILTER: ClassVar[bool] = True
    """R2-A Task 4 / F-10 opt-in — see ``silver/entsoe/_event_window.py``."""
    COLUMN_MAP = {"value": "generation_forecast_mw", "in_domain": "area_code"}
    REQUIRED = ("timestamp_utc", "generation_forecast_mw", "area_code")
    CASTS = {"generation_forecast_mw": pl.Float64()}
    FILL_NULL = {"production_type": "unknown"}
    OUTPUT_COLS = (
        "timestamp_utc",
        "area_code",
        "production_type",
        "generation_forecast_mw",
        "resolution",
        "published_at",
    )
    SORT = ("timestamp_utc", "area_code", "production_type")


register_transformer("entsoe", "wind_solar_forecast", WindSolarForecastTransformer)
