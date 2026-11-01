"""Silver transformer for ENTSO-E week-ahead load forecast (A65/A31)."""

from __future__ import annotations

from typing import ClassVar

from gridflow.schemas.entsoe import EntsoeLoadForecastWeekly
from gridflow.silver.entsoe.load_forecast import LoadForecastTransformer
from gridflow.silver.registry import register_transformer


class LoadForecastWeeklyTransformer(LoadForecastTransformer):
    """Transform ENTSO-E week-ahead load forecast XML from bronze to silver.

    A65/A31, typically at P7D resolution: the day-ahead transform with its
    own schema and horizon label.
    """

    dataset = "load_forecast_weekly"
    schema_cls = EntsoeLoadForecastWeekly
    forecast_horizon = "week_ahead"
    EVENT_WINDOW_FILTER: ClassVar[bool] = False
    """Un-inherit the day-ahead parent's opt-in: A31 week-ahead is a horizon
    dataset, exempt (see ``silver/entsoe/_event_window.py``)."""


register_transformer("entsoe", "load_forecast_weekly", LoadForecastWeeklyTransformer)
