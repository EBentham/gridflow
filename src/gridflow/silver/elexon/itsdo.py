"""Silver transformer for Elexon Initial Transmission System Demand Outturn (ITSDO)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.elexon import ElexonITSDO
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


class ITSDOTransformer(ElexonTransformer):
    """Transform Elexon ITSDO data from bronze to silver."""

    source = "elexon"
    dataset = "itsdo"
    schema_cls = ElexonITSDO
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period")  # D-8
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "publishTime": "published_at",
        "demand": "initial_transmission_system_demand_outturn_mw",
    }
    REQUIRED = (
        "settlement_date",
        "settlement_period",
        "initial_transmission_system_demand_outturn_mw",
    )
    CASTS = {"initial_transmission_system_demand_outturn_mw": pl.Float64()}
    OUTPUT_COLS = (
        "settlement_date",
        "settlement_period",
        "timestamp_utc",
        "initial_transmission_system_demand_outturn_mw",
        "published_at",
    )


register_transformer("elexon", "itsdo", ITSDOTransformer)
