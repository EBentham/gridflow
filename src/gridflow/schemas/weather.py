"""Pydantic v2 schemas for Open-Meteo weather silver-layer data contracts.

F7.5 split: ``WeatherObservation`` is replaced by three role-specific
schemas (``DemandWeather``, ``WindWeather``, ``SolarWeather``) so that
each silver dataset validates against its own field set without leaving
unrelated nullable columns.

Bitemporal columns (``event_time``, ``available_at``, ``source_run_id``,
``dataset_version``) are NOT declared on these schemas; they are stamped
onto the silver Polars DataFrame at write time by ``BaseSilverTransformer``
per the F0 pattern.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import Field, field_validator

from gridflow.schemas.common import BaseSchema


class _BaseWeather(BaseSchema):
    """Shared base — timestamp, location, provider, validator."""

    timestamp_utc: datetime
    location: str  # e.g. "london", "hornsea", "kent"
    latitude: float
    longitude: float

    data_provider: str = Field(default="open_meteo")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v


class DemandWeather(_BaseWeather):
    """Silver-layer schema for the ``open_meteo/historical_demand`` and
    ``open_meteo/forecast_demand`` datasets.

    Same field set as the F0-era ``WeatherObservation`` plus winter-peak
    snow variables and derived air density.
    """

    temperature_2m: float | None = None  # °C at 2m
    wind_speed_10m: float | None = None  # km/h at 10m
    wind_direction_10m: float | None = None  # degrees (0–360)
    relative_humidity_2m: float | None = None  # %
    precipitation: float | None = None  # mm
    shortwave_radiation: float | None = None  # W/m²
    surface_pressure: float | None = None  # hPa
    snowfall: float | None = None  # cm/h
    snow_depth: float | None = None  # m

    # Derived energy-demand indicators
    hdd: float | None = None  # max(0, 15.5 - T)
    cdd: float | None = None  # max(0, T - 22.0)
    air_density_kg_m3: float | None = None  # P / (287.05 * T_K)


class WindWeather(_BaseWeather):
    """Silver-layer schema for the ``open_meteo/historical_wind`` and
    ``open_meteo/forecast_wind`` datasets.

    Hub-height fields (80m / 120m / 180m) are typed permissively because
    the ERA5 archive endpoint exposes only 10m + 100m reliably (verified
    2026-05-09 against Hornsea and Whitelee — the others return null).
    The forecast endpoint (UKMO/ECMWF) carries the wider set.
    """

    temperature_2m: float | None = None
    surface_pressure: float | None = None
    precipitation: float | None = None

    wind_speed_10m: float | None = None
    wind_speed_80m: float | None = None
    wind_speed_100m: float | None = None
    wind_speed_120m: float | None = None
    wind_speed_180m: float | None = None

    wind_direction_10m: float | None = None
    wind_direction_80m: float | None = None
    wind_direction_100m: float | None = None
    wind_direction_120m: float | None = None
    wind_direction_180m: float | None = None

    wind_gusts_10m: float | None = None

    cloud_cover: float | None = None
    cloud_cover_low: float | None = None
    cloud_cover_mid: float | None = None
    cloud_cover_high: float | None = None

    dew_point_2m: float | None = None  # °C — icing risk

    air_density_kg_m3: float | None = None


class SolarWeather(_BaseWeather):
    """Silver-layer schema for the ``open_meteo/historical_solar`` and
    ``open_meteo/forecast_solar`` datasets.

    GHI is the existing ``shortwave_radiation``; DNI / DHI / GTI are added.
    Tilt/azimuth on the GTI request are documented in vault README.
    """

    temperature_2m: float | None = None

    shortwave_radiation: float | None = None  # GHI (W/m²)
    direct_radiation: float | None = None  # beam on horizontal (W/m²)
    direct_normal_irradiance: float | None = None  # DNI (W/m²)
    diffuse_radiation: float | None = None  # DHI (W/m²)
    global_tilted_irradiance: float | None = None  # GTI on UK fixed tilt (W/m²)

    cloud_cover: float | None = None
    cloud_cover_low: float | None = None
    cloud_cover_mid: float | None = None
    cloud_cover_high: float | None = None

    snowfall: float | None = None
    snow_depth: float | None = None
