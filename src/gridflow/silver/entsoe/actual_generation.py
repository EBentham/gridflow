"""Silver transformer for ENTSO-E actual generation per production type."""

from __future__ import annotations

from typing import ClassVar

import polars as pl

from gridflow.connectors.entsoe.area_codes import area_name_for
from gridflow.schemas.entsoe import EntsoeActualGeneration
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.registry import register_transformer


class ActualGenerationTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E actual generation XML from bronze to silver."""

    source = "entsoe"
    dataset = "actual_generation"
    schema_cls = EntsoeActualGeneration
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code", "production_type")
    EVENT_WINDOW_FILTER: ClassVar[bool] = True
    """R2-A Task 4 / F-10 opt-in — see ``silver/entsoe/_event_window.py``."""
    COLUMN_MAP = {"value": "generation_mw", "in_domain": "area_code"}
    REQUIRED = ("timestamp_utc", "generation_mw", "area_code")
    CASTS = {"generation_mw": pl.Float64()}
    FILL_NULL = {"production_type": "unknown"}
    OUTPUT_COLS = (
        "timestamp_utc",
        "area_code",
        "area_name",
        "production_type",
        "generation_mw",
        "resolution",
        "published_at",
    )
    SORT = ("timestamp_utc", "area_code", "production_type")

    def _derive(self, df: pl.DataFrame) -> pl.DataFrame:
        # G9 ENTSOE-03: name the area from its EIC code; a code outside the
        # canonical set gets "" so the column stays text (schema default "").
        return df.with_columns(
            pl.col("area_code").map_elements(area_name_for, return_dtype=pl.Utf8).alias("area_name")
        )


register_transformer("entsoe", "actual_generation", ActualGenerationTransformer)
