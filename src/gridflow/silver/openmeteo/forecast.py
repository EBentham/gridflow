"""Silver transformers for Open-Meteo forecast weather data.

F7.5 split: three role-specific subclasses of the corresponding historical
transformers — same locations, same schemas, same derived columns; only
the dataset name and (for wind) the variable list differ.

Wind forecast carries the wider hub-height set ``WIND_FORECAST_VARS``
because UKMO/ECMWF forecast models do publish 80m/120m/180m for areas
where the underlying model supports them. Open-Meteo nulls those fields
elsewhere; the ``WindWeather`` schema is permissive (``float | None``)
to accept the null-degradation cleanly.
"""

from __future__ import annotations

from gridflow.connectors.openmeteo.endpoints import (
    DATASET_SPECS,
    WIND_FORECAST_VARS,
)
from gridflow.silver.openmeteo.historical import (
    HistoricalDemandWeather,
    HistoricalSolarWeather,
    HistoricalWindWeather,
)
from gridflow.silver.registry import register_transformer


class ForecastDemandWeather(HistoricalDemandWeather):
    """Forecast counterpart of ``HistoricalDemandWeather``."""

    dataset = "forecast_demand"
    VINTAGE_POLICY = None
    BRONZE_DATASET_PREFIX = "forecast_demand"


class ForecastWindWeather(HistoricalWindWeather):
    """Forecast counterpart of ``HistoricalWindWeather``.

    Uses the wider ``WIND_FORECAST_VARS`` (includes 80m/120m/180m heights
    and directions). Forecast models that don't carry those heights null
    them; the Pydantic schema is permissive.
    """

    dataset = "forecast_wind"
    VINTAGE_POLICY = None
    BRONZE_DATASET_PREFIX = "forecast_wind"
    HOURLY_VARS = WIND_FORECAST_VARS


class ForecastSolarWeather(HistoricalSolarWeather):
    """Forecast counterpart of ``HistoricalSolarWeather``."""

    dataset = "forecast_solar"
    VINTAGE_POLICY = None
    BRONZE_DATASET_PREFIX = "forecast_solar"


# Sanity-check the variable list / dataset spec wiring at import time.
assert DATASET_SPECS["forecast_demand"].hourly == ForecastDemandWeather.HOURLY_VARS
assert DATASET_SPECS["forecast_wind"].hourly == ForecastWindWeather.HOURLY_VARS
assert DATASET_SPECS["forecast_solar"].hourly == ForecastSolarWeather.HOURLY_VARS


register_transformer("open_meteo", "forecast_demand", ForecastDemandWeather)
register_transformer("open_meteo", "forecast_wind", ForecastWindWeather)
register_transformer("open_meteo", "forecast_solar", ForecastSolarWeather)
