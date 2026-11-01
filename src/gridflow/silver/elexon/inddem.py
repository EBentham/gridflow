"""Silver transformer for Elexon Day and Day-Ahead Indicated Demand (INDDEM)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.elexon import ElexonIndDem
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


class INDDEMTransformer(ElexonTransformer):
    """Transform Elexon INDDEM data from bronze to silver."""

    source = "elexon"
    dataset = "inddem"
    schema_cls = ElexonIndDem
    APPEND_ONLY = True
    DATASET_VERSION = "2.0.0"
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period", "published_at")  # D-8
    OPTIONAL_ENTITY_KEY_COLUMNS = ("boundary",)
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "publishTime": "published_at",
        "demand": "indicated_demand_mw",
        "boundary": "boundary",
    }
    REQUIRED = ("settlement_date", "settlement_period", "indicated_demand_mw")
    CASTS = {"indicated_demand_mw": pl.Float64()}
    OUTPUT_COLS = (
        "settlement_date",
        "settlement_period",
        "timestamp_utc",
        "indicated_demand_mw",
        "boundary",
        "published_at",
    )


register_transformer("elexon", "inddem", INDDEMTransformer)
