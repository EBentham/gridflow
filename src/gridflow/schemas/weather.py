"""Pydantic v2 schemas for Open-Meteo weather silver-layer data contracts."""

from __future__ import annotations

from datetime import datetime

from pydantic import Field, field_validator

from gridflow.schemas.common import BaseSchema


class WeatherObservation(BaseSchema):
    """Silver-layer schema for a single hourly weather observation at one location."""

    timestamp_utc: datetime
    location: str  # e.g. "london"
    latitude: float
    longitude: float

    # Meteorological variables (all optional — API may return NaN for some)
    temperature_2m: float | None = None  # °C at 2m
    wind_speed_10m: float | None = None  # km/h at 10m
    wind_direction_10m: float | None = None  # degrees (0–360)
    relative_humidity_2m: float | None = None  # %
    precipitation: float | None = None  # mm
    shortwave_radiation: float | None = None  # W/m²
    surface_pressure: float | None = None  # hPa

    # Derived energy-demand indicators
    hdd: float | None = None  # Heating Degree Day (base 15.5 °C): max(0, 15.5 - T)
    cdd: float | None = None  # Cooling Degree Day (base 22.0 °C): max(0, T - 22.0)

    data_provider: str = Field(default="open_meteo")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v
