"""Unit tests for canonical Open-Meteo silver column names (F15-B).

RED state: all 5 tests fail before BaseOpenMeteoTransformer._UNIT_CONVERSIONS /
_PURE_RENAMES are added (Task 2 of F15-B makes them GREEN).

Pitfall 6 guard (Test 3): rename MUST run AFTER _add_derived so HDD/CDD derivation
reads connector-native `temperature_2m` before it becomes `temperature_2m_c`.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from gridflow.connectors.openmeteo.endpoints import (
    DEMAND_HOURLY_VARS,
    SOLAR_HOURLY_VARS,
    WIND_ARCHIVE_VARS,
)
from gridflow.silver.openmeteo.historical import (
    HistoricalDemandWeather,
    HistoricalSolarWeather,
    HistoricalWindWeather,
)
from gridflow.storage.parquet import read_parquet


def _make_open_meteo_json(
    hourly_vars: tuple[str, ...],
    values: dict[str, float] | None = None,
    lat: float = 51.5074,
    lon: float = -0.1278,
) -> dict:
    """Build a minimal Open-Meteo API response JSON with one hourly time point."""
    default_val = 10.0
    hourly: dict = {"time": ["2026-05-01T12:00"]}
    for var in hourly_vars:
        hourly[var] = [values.get(var, default_val) if values else default_val]
    return {
        "latitude": lat,
        "longitude": lon,
        "hourly": hourly,
    }


def _write_om_bronze(
    data_dir: Path,
    target_date: date,
    bronze_prefix: str,
    location: str,
    hourly_vars: tuple[str, ...],
    values: dict[str, float] | None = None,
) -> None:
    bronze_dir = (
        data_dir
        / "bronze"
        / "open_meteo"
        / f"{bronze_prefix}__{location}"
        / str(target_date.year)
        / f"{target_date.month:02d}"
        / f"{target_date.day:02d}"
    )
    bronze_dir.mkdir(parents=True, exist_ok=True)
    payload = _make_open_meteo_json(hourly_vars, values=values)
    (bronze_dir / "raw_test.json").write_text(json.dumps(payload))


def _read_silver(data_dir: Path, source: str, dataset: str) -> pl.DataFrame:
    pattern = data_dir / "silver" / source / dataset / "**" / "*.parquet"
    return read_parquet(pattern)


TARGET_DATE = date(2026, 5, 1)


def test_historical_demand_emits_canonical_names(tmp_path: Path) -> None:
    """Silver must have wind_speed_10m_mps with value 10.0 (= 36 km/h / 3.6)."""
    _write_om_bronze(
        tmp_path, TARGET_DATE, "historical_demand", "london",
        DEMAND_HOURLY_VARS,
        values={"wind_speed_10m": 36.0, "temperature_2m": 15.0},
    )
    HistoricalDemandWeather(tmp_path).run(TARGET_DATE, run_id="t1")
    df = _read_silver(tmp_path, "open_meteo", "historical_demand")

    assert "wind_speed_10m_mps" in df.columns, "canonical wind speed column missing"
    assert "temperature_2m_c" in df.columns, "canonical temperature column missing"
    assert abs(df["wind_speed_10m_mps"].to_list()[0] - 10.0) < 1e-6, (
        f"36 km/h should convert to 10 m/s, got {df['wind_speed_10m_mps'].to_list()[0]}"
    )
    assert df["temperature_2m_c"].to_list()[0] == pytest.approx(15.0)


def test_historical_demand_has_no_connector_native_names(tmp_path: Path) -> None:
    """Connector-native column names must be absent from silver after F15-B."""
    _write_om_bronze(
        tmp_path, TARGET_DATE, "historical_demand", "london",
        DEMAND_HOURLY_VARS,
    )
    HistoricalDemandWeather(tmp_path).run(TARGET_DATE, run_id="t2")
    df = _read_silver(tmp_path, "open_meteo", "historical_demand")

    for native in ("wind_speed_10m", "temperature_2m", "shortwave_radiation", "surface_pressure"):
        assert native not in df.columns, f"connector-native '{native}' leaked into silver"


def test_historical_demand_derived_columns_populated_with_rename(tmp_path: Path) -> None:
    """Pitfall 6 guard: hdd/cdd derivation must read temperature_2m BEFORE rename."""
    _write_om_bronze(
        tmp_path, TARGET_DATE, "historical_demand", "london",
        DEMAND_HOURLY_VARS,
        values={"temperature_2m": 8.0},  # below HDD_BASE 15.5°C => hdd = 7.5 > 0
    )
    HistoricalDemandWeather(tmp_path).run(TARGET_DATE, run_id="t3")
    df = _read_silver(tmp_path, "open_meteo", "historical_demand")

    assert "hdd_k" in df.columns, "derived hdd_k missing (rename or derivation broke)"
    assert df["hdd_k"].to_list()[0] == pytest.approx(7.5, abs=1e-6), (
        f"hdd_k should be 15.5 - 8.0 = 7.5, got {df['hdd_k'].to_list()[0]}"
    )
    assert "temperature_2m_c" in df.columns
    assert df["temperature_2m_c"].to_list()[0] == pytest.approx(8.0)


def test_historical_wind_emits_mps_for_hub_heights(tmp_path: Path) -> None:
    """HistoricalWindWeather: wind speeds in m/s for both 10m and 100m hub heights."""
    _write_om_bronze(
        tmp_path, TARGET_DATE, "historical_wind", "dogger_bank",
        WIND_ARCHIVE_VARS,
        values={"wind_speed_10m": 36.0, "wind_speed_100m": 72.0},
    )
    HistoricalWindWeather(tmp_path).run(TARGET_DATE, run_id="t4")
    df = _read_silver(tmp_path, "open_meteo", "historical_wind")

    assert "wind_speed_10m_mps" in df.columns
    assert "wind_speed_100m_mps" in df.columns
    assert abs(df["wind_speed_10m_mps"].to_list()[0] - 10.0) < 1e-6
    assert abs(df["wind_speed_100m_mps"].to_list()[0] - 20.0) < 1e-6


def test_historical_solar_emits_canonical_radiation(tmp_path: Path) -> None:
    """HistoricalSolarWeather: radiation columns get _wm2 suffix, cloud_cover gets _pct."""
    _write_om_bronze(
        tmp_path, TARGET_DATE, "historical_solar", "east_anglia_norfolk",
        SOLAR_HOURLY_VARS,
        values={"shortwave_radiation": 200.0, "cloud_cover": 50.0},
    )
    HistoricalSolarWeather(tmp_path).run(TARGET_DATE, run_id="t5")
    df = _read_silver(tmp_path, "open_meteo", "historical_solar")

    assert "shortwave_radiation_wm2" in df.columns
    assert df["shortwave_radiation_wm2"].to_list()[0] == pytest.approx(200.0)
    assert "cloud_cover_pct" in df.columns
    assert df["cloud_cover_pct"].to_list()[0] == pytest.approx(50.0)
