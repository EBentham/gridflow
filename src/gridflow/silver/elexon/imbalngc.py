"""Silver transformer for Elexon Indicated Imbalance (IMBALNGC)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.elexon import ElexonImbalNGC
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


class ImbalNGCTransformer(ElexonTransformer):
    """Transform Elexon IMBALNGC data from bronze to silver."""

    source = "elexon"
    dataset = "imbalngc"
    schema_cls = ElexonImbalNGC
    APPEND_ONLY = True
    DATASET_VERSION = "2.0.0"
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period", "published_at")  # D-8
    OPTIONAL_ENTITY_KEY_COLUMNS = ("boundary",)
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "publishDateTime": "published_at",
        "publishTime": "published_at",
        "indicatedImbalance": "indicated_imbalance",
        "imbalance": "indicated_imbalance",
        "boundary": "boundary",
    }
    REQUIRED = ("settlement_date", "settlement_period", "indicated_imbalance")
    CASTS = {"indicated_imbalance": pl.Float64()}
    OUTPUT_COLS = (
        "settlement_date",
        "settlement_period",
        "timestamp_utc",
        "indicated_imbalance",
        "boundary",
        "published_at",
    )


register_transformer("elexon", "imbalngc", ImbalNGCTransformer)
