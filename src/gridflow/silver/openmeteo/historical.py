"""Silver transformers for Open-Meteo historical weather data.

F7.5 split: ``HistoricalWeatherTransformer`` (single ``historical`` dataset)
is replaced by three role-specific subclasses on a shared
``BaseOpenMeteoTransformer``:

- ``HistoricalDemandWeather`` — population-centre weather; HDD/CDD + air
  density derivations preserved from F0.
- ``HistoricalWindWeather`` — capacity-weighted GB wind sites; archive
  variable list excludes 80/120/180m heights (verified all-null on ERA5).
- ``HistoricalSolarWeather`` — capacity-weighted GB solar sites; GTI
  request adds tilt/azimuth (handled by connector spec table).

Forecast counterparts live in ``forecast.py``.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any, ClassVar

import polars as pl

from gridflow.connectors.openmeteo.endpoints import (
    DATASET_SPECS,
    DEMAND_HOURLY_VARS,
    DEMAND_LOCATIONS,
    SOLAR_HOURLY_VARS,
    SOLAR_LOCATIONS,
    WIND_ARCHIVE_VARS,
    WIND_LOCATIONS,
    WeatherLocation,
)
from gridflow.schemas.common import BaseSchema
from gridflow.schemas.weather import DemandWeather, SolarWeather, WindWeather
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)

# Degree-day base temperatures (°C) — preserved from F0.
_HDD_BASE = 15.5
_CDD_BASE = 22.0

# Specific gas constant for dry air (J / (kg·K)).
_R_SPECIFIC_DRY_AIR = 287.05


def _pivot_openmeteo_json(
    data: dict[str, Any],
    location_name: str,
    hourly_vars: tuple[str, ...] | list[str],
) -> list[dict[str, Any]]:
    """Pivot Open-Meteo columnar JSON into a list of row dicts.

    ``hourly_vars`` is the role-specific variable list — wind archive vs
    wind forecast vs demand vs solar all differ.
    """
    hourly = data.get("hourly", {})
    times: list[str] = hourly.get("time", [])
    if not times:
        return []

    lat = data.get("latitude")
    lon = data.get("longitude")
    rows: list[dict[str, Any]] = []
    for i, time_str in enumerate(times):
        row: dict[str, Any] = {
            "time": time_str,
            "location": location_name,
            "latitude": lat,
            "longitude": lon,
        }
        for var in hourly_vars:
            values: list[Any] = hourly.get(var, [])
            row[var] = values[i] if i < len(values) else None
        rows.append(row)
    return rows


class BaseOpenMeteoTransformer(BaseSilverTransformer):
    """Shared logic for all six role-split openmeteo transformers."""

    source = "open_meteo"
    DATASET_VERSION: ClassVar[str] = "2.0.0"

    HOURLY_VARS: ClassVar[tuple[str, ...]] = ()
    SCHEMA: ClassVar[type[BaseSchema]]
    LOCATIONS: ClassVar[tuple[WeatherLocation, ...]] = ()
    DERIVE_HDD_CDD: ClassVar[bool] = False
    DERIVE_AIR_DENSITY: ClassVar[bool] = False

    # Subclasses set the per-role bronze prefix (e.g. ``historical_demand``).
    BRONZE_DATASET_PREFIX: ClassVar[str] = ""

    # F15-B: canonical-schema rename + unit conversion map.
    # Wind speeds: km/h -> m/s (Open-Meteo default when no windspeed_unit param sent).
    # Applied AFTER _add_derived() so HDD/CDD/air_density derivation reads pre-rename names.
    _UNIT_CONVERSIONS: ClassVar[dict[str, tuple[str, float]]] = {
        "wind_speed_10m": ("wind_speed_10m_mps", 1.0 / 3.6),
        "wind_speed_100m": ("wind_speed_100m_mps", 1.0 / 3.6),
        "wind_speed_80m": ("wind_speed_80m_mps", 1.0 / 3.6),
        "wind_speed_120m": ("wind_speed_120m_mps", 1.0 / 3.6),
        "wind_speed_180m": ("wind_speed_180m_mps", 1.0 / 3.6),
        "wind_gusts_10m": ("wind_gusts_10m_mps", 1.0 / 3.6),
    }
    # Pure renames: unit already correct; column gets an explicit unit suffix.
    _PURE_RENAMES: ClassVar[dict[str, str]] = {
        "temperature_2m": "temperature_2m_c",
        "dew_point_2m": "dew_point_2m_c",
        "surface_pressure": "surface_pressure_hpa",
        "relative_humidity_2m": "relative_humidity_2m_pct",
        "cloud_cover": "cloud_cover_pct",
        "cloud_cover_low": "cloud_cover_low_pct",
        "cloud_cover_mid": "cloud_cover_mid_pct",
        "cloud_cover_high": "cloud_cover_high_pct",
        "shortwave_radiation": "shortwave_radiation_wm2",
        "direct_radiation": "direct_radiation_wm2",
        "direct_normal_irradiance": "direct_normal_irradiance_wm2",
        "diffuse_radiation": "diffuse_radiation_wm2",
        "global_tilted_irradiance": "global_tilted_irradiance_wm2",
        "precipitation": "precipitation_mm",
        "snowfall": "snowfall_cm",
        "snow_depth": "snow_depth_m",
        "wind_direction_10m": "wind_direction_10m_deg",
        "wind_direction_100m": "wind_direction_100m_deg",
        "wind_direction_80m": "wind_direction_80m_deg",
        "wind_direction_120m": "wind_direction_120m_deg",
        "wind_direction_180m": "wind_direction_180m_deg",
        "hdd": "hdd_k",
        "cdd": "cdd_k",
    }

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        # Bronze lives under: bronze/open_meteo/{prefix}__{loc}/YYYY/MM/DD/
        parent_dir = self.bronze_dir.parent  # bronze/open_meteo/
        rows: list[dict[str, Any]] = []

        for loc in self.LOCATIONS:
            loc_dir = parent_dir / f"{self.BRONZE_DATASET_PREFIX}__{loc.name}"
            exact_path = (
                loc_dir
                / str(target_date.year)
                / f"{target_date.month:02d}"
                / f"{target_date.day:02d}"
            )
            if exact_path.exists() and any(exact_path.glob("raw_*")):
                date_path = exact_path
            else:
                date_path = self._find_covering_bronze_partition(target_date, bronze_dir=loc_dir)
            if date_path is None:
                continue

            for f in sorted(date_path.glob("raw_*.json")):
                if f.name.endswith(".meta.json"):
                    continue
                try:
                    data = json.loads(f.read_text())
                    rows.extend(_pivot_openmeteo_json(data, loc.name, self.HOURLY_VARS))
                except (json.JSONDecodeError, AttributeError, KeyError) as exc:
                    logger.warning("Failed to parse weather bronze file %s: %s", f, exc)

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        required = ["time", "location"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(
                "Missing required columns in %s/%s: %s",
                self.source,
                self.dataset,
                missing,
            )
            return pl.DataFrame()

        df = raw_df.with_columns(
            pl.col("time")
            .str.to_datetime(format="%Y-%m-%dT%H:%M", time_unit="us", strict=False)
            .dt.replace_time_zone("UTC")
            .alias("timestamp_utc")
        )

        # Cast numeric columns
        for col in ["latitude", "longitude", *self.HOURLY_VARS]:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))

        df = self._add_derived(df)

        # F15-B: apply unit conversions then pure renames (AFTER _add_derived so
        # HDD/CDD/air_density derivations read connector-native column names).
        conversion_exprs = [
            (pl.col(src_col) * factor).alias(target_col)
            for src_col, (target_col, factor) in self._UNIT_CONVERSIONS.items()
            if src_col in df.columns
        ]
        if conversion_exprs:
            df = df.with_columns(conversion_exprs).drop(
                [c for c in self._UNIT_CONVERSIONS if c in df.columns]
            )
        rename_map = {k: v for k, v in self._PURE_RENAMES.items() if k in df.columns}
        if rename_map:
            df = df.rename(rename_map)

        df = df.unique(subset=["timestamp_utc", "location"], keep="last")

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("open_meteo").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        output_cols = self._output_columns()
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc", "location")

    def _add_derived(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply per-role derived columns (HDD/CDD, air density)."""
        if self.DERIVE_HDD_CDD and "temperature_2m" in df.columns:
            df = df.with_columns(
                [
                    (pl.lit(_HDD_BASE) - pl.col("temperature_2m")).clip(lower_bound=0).alias("hdd"),
                    (pl.col("temperature_2m") - pl.lit(_CDD_BASE)).clip(lower_bound=0).alias("cdd"),
                ]
            )

        if (
            self.DERIVE_AIR_DENSITY
            and "surface_pressure" in df.columns
            and "temperature_2m" in df.columns
        ):
            df = df.with_columns(
                (
                    (pl.col("surface_pressure") * 100.0)
                    / (_R_SPECIFIC_DRY_AIR * (pl.col("temperature_2m") + 273.15))
                ).alias("air_density_kg_m3")
            )

        return df

    def _output_columns(self) -> list[str]:
        """The columns to keep, in stable order, for this role's silver."""

        def _canonical(col: str) -> str:
            if col in self._UNIT_CONVERSIONS:
                return self._UNIT_CONVERSIONS[col][0]
            return self._PURE_RENAMES.get(col, col)

        base = ["timestamp_utc", "location", "latitude", "longitude"]
        canonical_vars = [_canonical(c) for c in self.HOURLY_VARS]
        derived: list[str] = []
        if self.DERIVE_HDD_CDD:
            # hdd/cdd are renamed to hdd_k/cdd_k by _PURE_RENAMES
            derived.extend([_canonical("hdd"), _canonical("cdd")])
        if self.DERIVE_AIR_DENSITY:
            derived.append("air_density_kg_m3")
        tail = ["data_provider", "ingested_at"]
        return base + canonical_vars + derived + tail


class HistoricalDemandWeather(BaseOpenMeteoTransformer):
    """Historical (ERA5 archive) weather at the 7 UK demand population centres."""

    dataset = "historical_demand"
    BRONZE_DATASET_PREFIX = "historical_demand"
    LOCATIONS = DEMAND_LOCATIONS
    HOURLY_VARS = DEMAND_HOURLY_VARS
    SCHEMA = DemandWeather
    DERIVE_HDD_CDD = True
    DERIVE_AIR_DENSITY = True
    BRONZE_SIBLING_DATASETS: ClassVar[tuple[str, ...]] = tuple(
        f"historical_demand__{loc.name}" for loc in DEMAND_LOCATIONS
    )


class HistoricalWindWeather(BaseOpenMeteoTransformer):
    """Historical (ERA5 archive) weather at the 12 GB capacity-weighted wind sites.

    Variable list deliberately excludes ``wind_speed_{80,120,180}m`` and the
    matching directions because ERA5 archive returns ``units: "undefined"``
    and all-null at those heights (verified 2026-05-09 against Hornsea and
    Whitelee). The forecast counterpart in ``forecast.py`` requests the
    wider set.
    """

    dataset = "historical_wind"
    BRONZE_DATASET_PREFIX = "historical_wind"
    LOCATIONS = WIND_LOCATIONS
    HOURLY_VARS = WIND_ARCHIVE_VARS
    SCHEMA = WindWeather
    DERIVE_AIR_DENSITY = True
    BRONZE_SIBLING_DATASETS: ClassVar[tuple[str, ...]] = tuple(
        f"historical_wind__{loc.name}" for loc in WIND_LOCATIONS
    )


class HistoricalSolarWeather(BaseOpenMeteoTransformer):
    """Historical (ERA5 archive) weather at the 6 GB capacity-weighted solar sites.

    GHI/DNI/DHI/GTI components plus cloud-cover decomposition. The GTI
    request is keyed by the connector ``DATASET_SPECS`` entry's
    ``extra_params`` (tilt=35, azimuth=180 for UK fixed-tilt).
    """

    dataset = "historical_solar"
    BRONZE_DATASET_PREFIX = "historical_solar"
    LOCATIONS = SOLAR_LOCATIONS
    HOURLY_VARS = SOLAR_HOURLY_VARS
    SCHEMA = SolarWeather
    BRONZE_SIBLING_DATASETS: ClassVar[tuple[str, ...]] = tuple(
        f"historical_solar__{loc.name}" for loc in SOLAR_LOCATIONS
    )


# Sanity-check the variable list / dataset spec wiring at import time so the
# refactor catches drift between endpoints.py and the transformers.
assert HistoricalDemandWeather.HOURLY_VARS == DATASET_SPECS["historical_demand"].hourly
assert HistoricalWindWeather.HOURLY_VARS == DATASET_SPECS["historical_wind"].hourly
assert HistoricalSolarWeather.HOURLY_VARS == DATASET_SPECS["historical_solar"].hourly


register_transformer("open_meteo", "historical_demand", HistoricalDemandWeather)
register_transformer("open_meteo", "historical_wind", HistoricalWindWeather)
register_transformer("open_meteo", "historical_solar", HistoricalSolarWeather)
