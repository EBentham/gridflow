"""Silver transformer for Open-Meteo historical weather data."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any, ClassVar

import polars as pl

from gridflow.connectors.openmeteo.endpoints import HOURLY_VARIABLES, LOCATIONS
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)

# Degree-day base temperatures (°C)
_HDD_BASE = 15.5
_CDD_BASE = 22.0


def _pivot_openmeteo_json(data: dict[str, Any], location_name: str) -> list[dict[str, Any]]:
    """Pivot Open-Meteo columnar JSON to a list of row dicts."""
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
        for var in HOURLY_VARIABLES:
            values: list[Any] = hourly.get(var, [])
            row[var] = values[i] if i < len(values) else None
        rows.append(row)
    return rows


class HistoricalWeatherTransformer(BaseSilverTransformer):
    """Transform Open-Meteo historical weather data from bronze to silver.

    Reads from all ``historical_{location}`` bronze subdirectories for a given
    date, pivots columnar JSON to row format, then derives HDD / CDD columns.
    """

    source = "open_meteo"
    dataset = "historical"
    DATASET_VERSION: ClassVar[str] = "1.0.0"
    BRONZE_SIBLING_DATASETS: ClassVar[tuple[str, ...]] = tuple(
        f"historical_{loc.name}" for loc in LOCATIONS
    )

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        # Bronze lives under: bronze/open_meteo/historical_{loc}/YYYY/MM/DD/
        parent_dir = self.bronze_dir.parent  # bronze/open_meteo/
        rows: list[dict[str, Any]] = []

        for loc in LOCATIONS:
            loc_dir = parent_dir / f"historical_{loc.name}"
            date_path = (
                loc_dir
                / str(target_date.year)
                / f"{target_date.month:02d}"
                / f"{target_date.day:02d}"
            )
            if not date_path.exists():
                continue

            for f in sorted(date_path.glob("raw_*.json")):
                if f.name.endswith(".meta.json"):
                    continue
                try:
                    data = json.loads(f.read_text())
                    rows.extend(_pivot_openmeteo_json(data, loc.name))
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
            logger.error("Missing required columns in historical weather: %s", missing)
            return pl.DataFrame()

        df = raw_df.with_columns(
            pl.col("time")
            .str.to_datetime(format="%Y-%m-%dT%H:%M", time_unit="us", strict=False)
            .dt.replace_time_zone("UTC")
            .alias("timestamp_utc")
        )

        # Cast numeric columns
        for col in ["latitude", "longitude", *HOURLY_VARIABLES]:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))

        # Derive HDD and CDD from temperature
        if "temperature_2m" in df.columns:
            df = df.with_columns([
                (pl.lit(_HDD_BASE) - pl.col("temperature_2m"))
                .clip(lower_bound=0)
                .alias("hdd"),
                (pl.col("temperature_2m") - pl.lit(_CDD_BASE))
                .clip(lower_bound=0)
                .alias("cdd"),
            ])

        df = df.unique(subset=["timestamp_utc", "location"], keep="last")

        now = datetime.now(UTC)
        df = df.with_columns([
            pl.lit("open_meteo").alias("data_provider"),
            pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
        ])

        output_cols = [
            "timestamp_utc", "location", "latitude", "longitude",
            "temperature_2m", "wind_speed_10m", "wind_direction_10m",
            "relative_humidity_2m", "precipitation",
            "shortwave_radiation", "surface_pressure",
            "hdd", "cdd",
            "data_provider", "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc", "location")


register_transformer("open_meteo", "historical", HistoricalWeatherTransformer)
