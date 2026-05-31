"""Property-style tests for the air-density derivation in openmeteo silver.

F7.5-VARS-06: ``air_density_kg_m3 = surface_pressure_Pa / (287.05 * T_K)``.
For ``T ∈ [-30, 45] °C`` and ``P ∈ [950, 1050] hPa`` the output band is
[0.95, 1.55] kg/m³ — wider than originally specified because the joint
extreme T=-30°C, P=1050 hPa lands at ~1.504 kg/m³.
"""

from __future__ import annotations

import itertools
from pathlib import Path

import polars as pl
import pytest

from gridflow.silver.openmeteo.historical import (
    HistoricalDemandWeather,
    HistoricalSolarWeather,
    HistoricalWindWeather,
)

_R = 287.05


def _formula(t_c: float, p_hpa: float) -> float:
    return (p_hpa * 100.0) / (_R * (t_c + 273.15))


@pytest.mark.parametrize(
    "t_c,p_hpa",
    list(itertools.product([-30, -15, 0, 15, 30, 45], [950, 1000, 1050])),
)
def test_air_density_in_atmospheric_band(t_c: float, p_hpa: float) -> None:
    df = pl.DataFrame(
        [
            {
                "time": "2024-01-15T00:00",
                "location": "test",
                "latitude": 50.0,
                "longitude": 0.0,
                "temperature_2m": float(t_c),
                "surface_pressure": float(p_hpa),
                # other variables present for completeness
                "wind_speed_10m": 0.0,
                "wind_direction_10m": 0.0,
                "relative_humidity_2m": 50.0,
                "precipitation": 0.0,
                "shortwave_radiation": 0.0,
                "snowfall": 0.0,
                "snow_depth": 0.0,
            }
        ]
    )

    t = HistoricalDemandWeather.__new__(HistoricalDemandWeather)
    t.data_dir = Path("/tmp/test")
    t.bronze_dir = Path("/tmp/test/bronze/open_meteo/historical_demand")
    t.silver_dir = Path("/tmp/test/silver/open_meteo/historical_demand")

    out = t.transform(df)

    rho = out["air_density_kg_m3"][0]
    assert rho is not None
    # Joint extreme T=-30°C, P=1050 hPa lands at ~1.504; widen band accordingly.
    assert 0.95 <= rho <= 1.55, (t_c, p_hpa, rho)
    # Numeric formula match within 1e-9.
    assert abs(rho - _formula(t_c, p_hpa)) < 1e-9, (t_c, p_hpa, rho)


def test_air_density_omitted_when_pressure_missing() -> None:
    # WindWeather wraps surface_pressure in its variable list, but if a
    # row's surface_pressure is null the derived column should be null too.
    df = pl.DataFrame(
        [
            {
                "time": "2024-01-15T00:00",
                "location": "hornsea",
                "latitude": 53.88,
                "longitude": 1.79,
                "temperature_2m": 5.0,
                "surface_pressure": None,
                "wind_speed_10m": 8.0,
                "wind_speed_100m": 14.0,
                "wind_direction_10m": 200.0,
                "wind_direction_100m": 200.0,
                "wind_gusts_10m": 12.0,
                "cloud_cover": 50.0,
                "cloud_cover_low": 30.0,
                "cloud_cover_mid": 10.0,
                "cloud_cover_high": 10.0,
                "dew_point_2m": 1.0,
                "precipitation": 0.0,
            }
        ]
    )
    t = HistoricalWindWeather.__new__(HistoricalWindWeather)
    t.data_dir = Path("/tmp/test")
    t.bronze_dir = Path("/tmp/test/bronze/open_meteo/historical_wind")
    t.silver_dir = Path("/tmp/test/silver/open_meteo/historical_wind")
    out = t.transform(df)
    assert out["air_density_kg_m3"][0] is None


def test_air_density_not_derived_for_solar() -> None:
    # SolarWeather does not include surface_pressure in its variable list,
    # so the transformer's DERIVE_AIR_DENSITY is False — silver carries no
    # air_density_kg_m3 column.
    assert HistoricalSolarWeather.DERIVE_AIR_DENSITY is False

    t = HistoricalSolarWeather.__new__(HistoricalSolarWeather)
    t.data_dir = Path("/tmp/test")
    t.bronze_dir = Path("/tmp/test/bronze/open_meteo/historical_solar")
    t.silver_dir = Path("/tmp/test/silver/open_meteo/historical_solar")
    assert "air_density_kg_m3" not in t._output_columns()
    df = pl.DataFrame(
        [
            {
                "time": "2024-06-15T12:00",
                "location": "kent",
                "latitude": 51.2,
                "longitude": 0.7,
                "temperature_2m": 22.0,
                "shortwave_radiation": 600.0,
                "direct_radiation": 450.0,
                "direct_normal_irradiance": 750.0,
                "diffuse_radiation": 150.0,
                "global_tilted_irradiance": 720.0,
                "cloud_cover": 20.0,
                "cloud_cover_low": 10.0,
                "cloud_cover_mid": 5.0,
                "cloud_cover_high": 5.0,
                "snowfall": 0.0,
                "snow_depth": 0.0,
            }
        ]
    )
    out = t.transform(df)
    assert "air_density_kg_m3" not in out.columns
