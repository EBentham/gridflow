"""Silver transformer for ENTSO-E year-ahead load forecast (A65/A33)."""

from __future__ import annotations

from typing import ClassVar

from gridflow.silver.entsoe.load_forecast import LoadForecastTransformer
from gridflow.silver.registry import register_transformer


class LoadForecastYearlyTransformer(LoadForecastTransformer):
    """Transform ENTSO-E year-ahead load forecast XML from bronze to silver."""

    dataset = "load_forecast_yearly"
    forecast_horizon = "year_ahead"
    EVENT_WINDOW_FILTER: ClassVar[bool] = False
    """R2-A Task 4 / F-10: un-inherit the day-ahead parent's opt-in -- A33
    year-ahead is a horizon dataset, exempt (see
    ``silver/entsoe/_event_window.py``)."""


register_transformer(
    "entsoe",
    "load_forecast_yearly",
    LoadForecastYearlyTransformer,
)
