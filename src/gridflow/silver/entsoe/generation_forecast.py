"""Silver transformer for ENTSO-E day-ahead generation forecast aggregated (A71/A01)."""

from __future__ import annotations

from typing import ClassVar

import polars as pl

from gridflow.schemas.entsoe import EntsoeGenerationForecast
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.registry import register_transformer


class GenerationForecastTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E day-ahead generation forecast XML from bronze to silver.

    A71/A01 (day-ahead generation forecast aggregated by technology).
    Each TimeSeries carries ``MktPSRType / psrType`` for the generation type.
    The ``quantity`` value is the forecast generation in MW. ``published_at``
    is in the grain: each fetch is an observation an as-of read must find.
    """

    source = "entsoe"
    dataset = "generation_forecast"
    schema_cls = EntsoeGenerationForecast
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


register_transformer("entsoe", "generation_forecast", GenerationForecastTransformer)
