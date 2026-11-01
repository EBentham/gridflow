"""Silver transformer for Elexon Transmission System Demand Forecast (TSDF)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.elexon import ElexonTSDF
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


class TSDFTransformer(ElexonTransformer):
    """Transform Elexon TSDF data from bronze to silver."""

    source = "elexon"
    dataset = "tsdf"
    schema_cls = ElexonTSDF
    APPEND_ONLY = True
    DATASET_VERSION = "2.0.0"
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period", "published_at")  # D-8
    OPTIONAL_ENTITY_KEY_COLUMNS = ("boundary",)
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "publishTime": "published_at",
        "demand": "forecast_demand_mw",
        "boundary": "boundary",
    }
    REQUIRED = ("settlement_date", "settlement_period", "forecast_demand_mw")
    CASTS = {"forecast_demand_mw": pl.Float64()}
    # F-08: publishTime is TSDF's own vendor vintage, distinct from timestamp_utc.
    OUTPUT_COLS = (
        "settlement_date",
        "settlement_period",
        "timestamp_utc",
        "forecast_demand_mw",
        "boundary",
        "published_at",
    )


register_transformer("elexon", "tsdf", TSDFTransformer)
