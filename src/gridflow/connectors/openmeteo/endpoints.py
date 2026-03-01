"""Open-Meteo API location and variable definitions."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class WeatherLocation:
    """A named geographic location to fetch weather data for."""

    name: str  # Used in dataset names: "historical_london"
    latitude: float
    longitude: float
    timezone: str = "UTC"


# Major UK population centres + energy demand hotspots
LOCATIONS: list[WeatherLocation] = [
    WeatherLocation("london", 51.5074, -0.1278),
    WeatherLocation("birmingham", 52.4862, -1.8904),
    WeatherLocation("manchester", 53.4808, -2.2426),
    WeatherLocation("leeds", 53.8008, -1.5491),
    WeatherLocation("glasgow", 55.8642, -4.2518),
    WeatherLocation("cardiff", 51.4816, -3.1791),
    WeatherLocation("belfast", 54.5973, -5.9301),
]

# Hourly variables fetched for every location
HOURLY_VARIABLES: list[str] = [
    "temperature_2m",
    "wind_speed_10m",
    "wind_direction_10m",
    "relative_humidity_2m",
    "precipitation",
    "shortwave_radiation",
    "surface_pressure",
]

# Open-Meteo base URLs (archive uses a different host)
ARCHIVE_BASE_URL = "https://archive-api.open-meteo.com/v1"
FORECAST_BASE_URL = "https://api.open-meteo.com/v1"
