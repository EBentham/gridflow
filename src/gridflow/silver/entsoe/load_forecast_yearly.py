"""Silver transformer for ENTSO-E year-ahead load forecast (A65/A33)."""

from __future__ import annotations

from gridflow.silver.entsoe.load_forecast import LoadForecastTransformer
from gridflow.silver.registry import register_transformer


class LoadForecastYearlyTransformer(LoadForecastTransformer):
    """Transform ENTSO-E year-ahead load forecast XML from bronze to silver."""

    dataset = "load_forecast_yearly"
    forecast_horizon = "year_ahead"


register_transformer(
    "entsoe",
    "load_forecast_yearly",
    LoadForecastYearlyTransformer,
)
