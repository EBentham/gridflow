"""Property-style tests for the irradiance-component invariants in solar silver.

F7.5-VARS-03 contract: ``direct_radiation + diffuse_radiation`` is within
~5% of ``shortwave_radiation`` (GHI) for daylight rows. The 5% tolerance
accommodates Open-Meteo's separation-model rounding when the underlying
weather model does not natively carry DNI/DHI.

For night rows (zero GHI) all three components are zero.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from gridflow.silver.openmeteo.historical import HistoricalSolarWeather


def _solar_transformer() -> HistoricalSolarWeather:
    t = HistoricalSolarWeather.__new__(HistoricalSolarWeather)
    t.data_dir = Path("/tmp/test")
    t.bronze_dir = Path("/tmp/test/bronze/open_meteo/historical_solar")
    t.silver_dir = Path("/tmp/test/silver/open_meteo/historical_solar")
    return t


def _solar_row(
    *,
    ghi: float,
    direct: float,
    diffuse: float,
    hour: int,
) -> dict:
    return {
        "time": f"2024-06-15T{hour:02d}:00",
        "location": "kent",
        "latitude": 51.2,
        "longitude": 0.7,
        "temperature_2m": 20.0,
        "shortwave_radiation": ghi,
        "direct_radiation": direct,
        "direct_normal_irradiance": direct * 1.5,  # rough geometric scaling
        "diffuse_radiation": diffuse,
        "global_tilted_irradiance": ghi * 1.05,
        "cloud_cover": 20.0,
        "cloud_cover_low": 10.0,
        "cloud_cover_mid": 5.0,
        "cloud_cover_high": 5.0,
        "snowfall": 0.0,
        "snow_depth": 0.0,
    }


@pytest.mark.parametrize(
    "ghi,direct,diffuse",
    [
        (600.0, 450.0, 150.0),  # 600 vs 600 — exact match
        (600.0, 470.0, 140.0),  # 610 vs 600 — within ~2%
        (300.0, 200.0, 110.0),  # 310 vs 300 — within ~3%
        (800.0, 700.0, 120.0),  # 820 vs 800 — within ~3%
    ],
)
def test_direct_plus_diffuse_within_5pct_of_ghi(
    ghi: float, direct: float, diffuse: float
) -> None:
    df = pl.DataFrame([_solar_row(ghi=ghi, direct=direct, diffuse=diffuse, hour=12)])
    out = _solar_transformer().transform(df)

    row = out.row(0, named=True)
    assert row["shortwave_radiation"] is not None
    assert row["direct_radiation"] is not None
    assert row["diffuse_radiation"] is not None
    sum_components = row["direct_radiation"] + row["diffuse_radiation"]
    rel_err = abs(sum_components - row["shortwave_radiation"]) / row["shortwave_radiation"]
    assert rel_err <= 0.05, (sum_components, row["shortwave_radiation"], rel_err)


def test_night_rows_all_zero() -> None:
    df = pl.DataFrame([_solar_row(ghi=0.0, direct=0.0, diffuse=0.0, hour=2)])
    out = _solar_transformer().transform(df)
    row = out.row(0, named=True)
    assert row["shortwave_radiation"] == 0.0
    assert row["direct_radiation"] == 0.0
    assert row["diffuse_radiation"] == 0.0
    assert row["direct_normal_irradiance"] == 0.0


def test_solar_silver_carries_all_irradiance_columns() -> None:
    df = pl.DataFrame([_solar_row(ghi=500.0, direct=400.0, diffuse=110.0, hour=12)])
    out = _solar_transformer().transform(df)
    for col in (
        "shortwave_radiation",
        "direct_radiation",
        "direct_normal_irradiance",
        "diffuse_radiation",
        "global_tilted_irradiance",
    ):
        assert col in out.columns, col
