"""Silver transformer for Elexon Initial National Demand Outturn Daily (INDOD)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.elexon import ElexonINDOD
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


class INDODTransformer(ElexonTransformer):
    """Transform Elexon INDOD data from bronze to silver.

    A daily dataset: ``timestamp_utc`` is the start of the settlement day
    (period 1, 00:00 UK local), so the roll-up lines up with half-hourly INDO.
    """

    source = "elexon"
    dataset = "indod"
    schema_cls = ElexonINDOD
    ENTITY_KEY_COLUMNS = ("settlement_date",)  # D-8
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "publishTime": "published_at",
        "demand": "initial_demand_outturn_mw",
    }
    REQUIRED = ("settlement_date", "initial_demand_outturn_mw")
    CASTS = {"initial_demand_outturn_mw": pl.Float64()}
    OUTPUT_COLS = (
        "settlement_date",
        "timestamp_utc",
        "initial_demand_outturn_mw",
        "published_at",
    )


register_transformer("elexon", "indod", INDODTransformer)
