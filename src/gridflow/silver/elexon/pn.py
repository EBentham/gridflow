"""Silver transformer for Elexon Physical Notifications (PN)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.elexon import ElexonPN
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


class PNTransformer(ElexonTransformer):
    """Transform Elexon PN data from bronze to silver.

    A unit-period carries one row per ramp segment; ``time_from`` tells them apart.
    """

    source = "elexon"
    dataset = "pn"
    schema_cls = ElexonPN
    DATASET_VERSION = "2.0.0"
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period", "bm_unit_id")  # D-8
    OPTIONAL_ENTITY_KEY_COLUMNS = ("time_from",)
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "bmUnit": "bm_unit_id",
        "levelFrom": "level_from",
        "levelTo": "level_to",
        "timeFrom": "time_from",
        "timeTo": "time_to",
    }
    REQUIRED = ("settlement_date", "settlement_period", "bm_unit_id")
    CASTS = {
        "bm_unit_id": pl.Utf8(),
        "level_from": pl.Float64(),
        "level_to": pl.Float64(),
        "time_from": pl.Datetime("us", "UTC"),
        "time_to": pl.Datetime("us", "UTC"),
    }
    OUTPUT_COLS = (
        "settlement_date",
        "settlement_period",
        "timestamp_utc",
        "bm_unit_id",
        "time_from",
        "time_to",
        "level_from",
        "level_to",
    )
    SORT = ("timestamp_utc", "bm_unit_id")


register_transformer("elexon", "pn", PNTransformer)
