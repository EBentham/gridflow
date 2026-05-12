"""Open-Meteo silver transformers — import all modules to trigger registration."""

from gridflow.silver.openmeteo.forecast import (
    ForecastDemandWeather,
    ForecastSolarWeather,
    ForecastWindWeather,
)
from gridflow.silver.openmeteo.historical import (
    BaseOpenMeteoTransformer,
    HistoricalDemandWeather,
    HistoricalSolarWeather,
    HistoricalWindWeather,
)

__all__ = [
    "BaseOpenMeteoTransformer",
    "ForecastDemandWeather",
    "ForecastSolarWeather",
    "ForecastWindWeather",
    "HistoricalDemandWeather",
    "HistoricalSolarWeather",
    "HistoricalWindWeather",
]
