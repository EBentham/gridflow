"""Silver transformer for Elexon Initial National Demand Outturn (INDO)."""

from __future__ import annotations

from typing import ClassVar

import polars as pl

from gridflow.schemas.elexon import ElexonINDO
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


class INDOTransformer(ElexonTransformer):
    """Transform Elexon INDO data from bronze to silver."""

    source = "elexon"
    dataset = "indo"
    schema_cls = ElexonINDO
    DATASET_VERSION: ClassVar[str] = "1.0.0"
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period")  # D-8
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "publishTime": "published_at",
        "demand": "initial_demand_outturn_mw",
    }
    REQUIRED = ("settlement_date", "settlement_period", "initial_demand_outturn_mw")
    CASTS = {"initial_demand_outturn_mw": pl.Float64()}
    OUTPUT_COLS = (
        "settlement_date",
        "settlement_period",
        "timestamp_utc",
        "initial_demand_outturn_mw",
        "published_at",
    )


register_transformer("elexon", "indo", INDOTransformer)
