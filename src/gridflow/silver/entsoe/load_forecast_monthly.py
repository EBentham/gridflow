"""Silver transformer for ENTSO-E month-ahead load forecast (A65/A32)."""

from __future__ import annotations

from gridflow.silver.entsoe.load_forecast import LoadForecastTransformer
from gridflow.silver.registry import register_transformer


class LoadForecastMonthlyTransformer(LoadForecastTransformer):
    """Transform ENTSO-E month-ahead load forecast XML from bronze to silver."""

    dataset = "load_forecast_monthly"
    forecast_horizon = "month_ahead"


register_transformer(
    "entsoe",
    "load_forecast_monthly",
    LoadForecastMonthlyTransformer,
)

