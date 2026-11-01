"""Silver transformer for Elexon Indicated Margin (MELNGC)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.elexon import ElexonMelNGC
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


class MelNGCTransformer(ElexonTransformer):
    """Transform Elexon MELNGC data from bronze to silver."""

    source = "elexon"
    dataset = "melngc"
    schema_cls = ElexonMelNGC
    APPEND_ONLY = True
    DATASET_VERSION = "2.0.0"
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period", "published_at")  # D-8
    OPTIONAL_ENTITY_KEY_COLUMNS = ("boundary",)
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "publishDateTime": "published_at",
        "publishTime": "published_at",
        "indicatedMargin": "indicated_margin",
        "margin": "indicated_margin",
        "boundary": "boundary",
    }
    REQUIRED = ("settlement_date", "settlement_period", "indicated_margin")
    CASTS = {"indicated_margin": pl.Float64()}
    OUTPUT_COLS = (
        "settlement_date",
        "settlement_period",
        "timestamp_utc",
        "indicated_margin",
        "boundary",
        "published_at",
    )


register_transformer("elexon", "melngc", MelNGCTransformer)
