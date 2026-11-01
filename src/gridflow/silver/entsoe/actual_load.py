"""Silver transformer for ENTSO-E actual total load."""

from __future__ import annotations

from typing import ClassVar

import polars as pl

from gridflow.schemas.entsoe import EntsoeActualLoad
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.registry import register_transformer


class ActualLoadTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E actual load XML from bronze to silver."""

    source = "entsoe"
    dataset = "actual_load"
    schema_cls = EntsoeActualLoad
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code")
    EVENT_WINDOW_FILTER: ClassVar[bool] = True
    """R2-A Task 4 / F-10 opt-in — see ``silver/entsoe/_event_window.py``."""
    COLUMN_MAP = {"value": "load_mw", "in_domain": "area_code"}
    REQUIRED = ("timestamp_utc", "load_mw", "area_code")
    CASTS = {"load_mw": pl.Float64()}
    OUTPUT_COLS = ("timestamp_utc", "area_code", "load_mw", "resolution", "published_at")


register_transformer("entsoe", "actual_load", ActualLoadTransformer)
