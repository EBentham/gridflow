"""Silver transformer for Open-Meteo forecast weather data."""

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, ClassVar

import polars as pl

from gridflow.connectors.openmeteo.endpoints import LOCATIONS
from gridflow.silver.openmeteo.historical import (
    HistoricalWeatherTransformer,
    _pivot_openmeteo_json,
)
from gridflow.silver.registry import register_transformer

if TYPE_CHECKING:
    from datetime import date

logger = logging.getLogger(__name__)


class ForecastWeatherTransformer(HistoricalWeatherTransformer):
    """Transform Open-Meteo forecast weather data from bronze to silver.

    Reads from all ``forecast_{location}`` bronze subdirectories.
    The transform logic is identical to historical (same JSON response structure).
    """

    dataset = "forecast"
    BRONZE_SIBLING_DATASETS: ClassVar[tuple[str, ...]] = tuple(
        f"forecast_{loc.name}" for loc in LOCATIONS
    )

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        parent_dir = self.bronze_dir.parent  # bronze/open_meteo/
        rows: list[dict[str, Any]] = []

        for loc in LOCATIONS:
            loc_dir = parent_dir / f"forecast_{loc.name}"
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
                    logger.warning(
                        "Failed to parse weather forecast bronze file %s: %s", f, exc
                    )

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)


register_transformer("open_meteo", "forecast", ForecastWeatherTransformer)
