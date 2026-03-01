"""Open-Meteo silver transformers — import all modules to trigger registration."""

from gridflow.silver.openmeteo.forecast import ForecastWeatherTransformer
from gridflow.silver.openmeteo.historical import HistoricalWeatherTransformer

__all__ = [
    "HistoricalWeatherTransformer",
    "ForecastWeatherTransformer",
]
