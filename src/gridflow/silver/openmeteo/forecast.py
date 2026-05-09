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

from typing import ClassVar

from gridflow.connectors.openmeteo.endpoints import (
    DATASET_SPECS,
    DEMAND_LOCATIONS,
    SOLAR_LOCATIONS,
    WIND_FORECAST_VARS,
    WIND_LOCATIONS,
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
    BRONZE_DATASET_PREFIX = "forecast_demand"
    BRONZE_SIBLING_DATASETS: ClassVar[tuple[str, ...]] = tuple(
        f"forecast_demand__{loc.name}" for loc in DEMAND_LOCATIONS
    )


class ForecastWindWeather(HistoricalWindWeather):
    """Forecast counterpart of ``HistoricalWindWeather``.

    Uses the wider ``WIND_FORECAST_VARS`` (includes 80m/120m/180m heights
    and directions). Forecast models that don't carry those heights null
    them; the Pydantic schema is permissive.
    """

    dataset = "forecast_wind"
    BRONZE_DATASET_PREFIX = "forecast_wind"
    HOURLY_VARS = WIND_FORECAST_VARS
    BRONZE_SIBLING_DATASETS: ClassVar[tuple[str, ...]] = tuple(
        f"forecast_wind__{loc.name}" for loc in WIND_LOCATIONS
    )


class ForecastSolarWeather(HistoricalSolarWeather):
    """Forecast counterpart of ``HistoricalSolarWeather``."""

    dataset = "forecast_solar"
    BRONZE_DATASET_PREFIX = "forecast_solar"
    BRONZE_SIBLING_DATASETS: ClassVar[tuple[str, ...]] = tuple(
        f"forecast_solar__{loc.name}" for loc in SOLAR_LOCATIONS
    )


# Sanity-check the variable list / dataset spec wiring at import time.
assert ForecastDemandWeather.HOURLY_VARS == DATASET_SPECS["forecast_demand"].hourly
assert ForecastWindWeather.HOURLY_VARS == DATASET_SPECS["forecast_wind"].hourly
assert ForecastSolarWeather.HOURLY_VARS == DATASET_SPECS["forecast_solar"].hourly


register_transformer("open_meteo", "forecast_demand", ForecastDemandWeather)
register_transformer("open_meteo", "forecast_wind", ForecastWindWeather)
register_transformer("open_meteo", "forecast_solar", ForecastSolarWeather)
