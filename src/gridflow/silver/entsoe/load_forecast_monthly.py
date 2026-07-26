"""Silver transformer for ENTSO-E month-ahead load forecast (A65/A32)."""

from __future__ import annotations

from typing import ClassVar

from gridflow.silver.entsoe.load_forecast import LoadForecastTransformer
from gridflow.silver.registry import register_transformer


class LoadForecastMonthlyTransformer(LoadForecastTransformer):
    """Transform ENTSO-E month-ahead load forecast XML from bronze to silver."""

    dataset = "load_forecast_monthly"
    forecast_horizon = "month_ahead"
    EVENT_WINDOW_FILTER: ClassVar[bool] = False
    """R2-A Task 4 / F-10: un-inherit the day-ahead parent's opt-in -- A32
    month-ahead is a horizon dataset, exempt (see
    ``silver/entsoe/_event_window.py``)."""


register_transformer(
    "entsoe",
    "load_forecast_monthly",
    LoadForecastMonthlyTransformer,
)
