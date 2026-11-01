"""Silver transformer for ENTSO-E actual generation per generation unit."""

from __future__ import annotations

from typing import ClassVar

import polars as pl

from gridflow.schemas.entsoe import EntsoeActualGenerationUnits
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.registry import register_transformer


class ActualGenerationUnitsTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E actual generation per generation unit XML to silver."""

    source = "entsoe"
    dataset = "actual_generation_units"
    schema_cls = EntsoeActualGenerationUnits
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code", "unit_mrid")
    EVENT_WINDOW_FILTER: ClassVar[bool] = True
    """R2-A Task 4 / F-10 opt-in — see ``silver/entsoe/_event_window.py``."""
    COLUMN_MAP = {"value": "generation_mw", "in_domain": "area_code"}
    REQUIRED = ("timestamp_utc", "generation_mw", "area_code", "unit_mrid")
    CASTS = {"generation_mw": pl.Float64()}
    FILL_NULL = {"unit_mrid": "", "unit_name": "", "production_type": "unknown"}
    OUTPUT_COLS = (
        "timestamp_utc",
        "area_code",
        "production_type",
        "unit_mrid",
        "unit_name",
        "generation_mw",
        "resolution",
        "published_at",
    )
    SORT = ("timestamp_utc", "area_code", "unit_mrid")

    def _derive(self, df: pl.DataFrame) -> pl.DataFrame:
        # A series without a unit mRID has no unit to key on.
        return df.filter(pl.col("unit_mrid") != "")


register_transformer("entsoe", "actual_generation_units", ActualGenerationUnitsTransformer)
