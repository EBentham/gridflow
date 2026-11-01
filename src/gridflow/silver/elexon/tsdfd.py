"""Silver transformer for Elexon 2-14 Day Ahead Transmission System Demand Forecast (TSDFD)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.elexon import ElexonTSDFD
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


class TSDFDTransformer(ElexonTransformer):
    """Transform Elexon TSDFD data from bronze to silver."""

    source = "elexon"
    dataset = "tsdfd"
    schema_cls = ElexonTSDFD
    APPEND_ONLY = True
    DATASET_VERSION = "3.0.0"
    ENTITY_KEY_COLUMNS = ("forecast_date", "published_at")  # D-8
    COLUMN_MAP = {
        "forecastDate": "forecast_date",
        "publishTime": "published_at",
        "demand": "forecast_demand_mw",
    }
    REQUIRED = ("forecast_date", "forecast_demand_mw")
    CASTS = {"forecast_demand_mw": pl.Float64()}
    # The forecast date is a UK settlement day: stamped at the start of period 1.
    SETTLEMENT_DATE = "forecast_date"
    OUTPUT_COLS = ("forecast_date", "timestamp_utc", "forecast_demand_mw", "published_at")
    SORT = ("forecast_date",)


register_transformer("elexon", "tsdfd", TSDFDTransformer)
