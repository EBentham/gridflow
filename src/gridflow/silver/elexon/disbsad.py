"""Silver transformer for Elexon Disaggregated Balancing Services Adjustment Data (DISBSAD)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.elexon import ElexonDISBSAD
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


class DISBSADTransformer(ElexonTransformer):
    """Transform Elexon DISBSAD data from bronze to silver."""

    source = "elexon"
    dataset = "disbsad"
    schema_cls = ElexonDISBSAD
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period")  # D-8
    OPTIONAL_ENTITY_KEY_COLUMNS = ("adjustment_action_id", "component")
    # The live API renamed `component` to `service` (G5-W1.3) and
    # `storProviderFlag` to `storFlag` (ELEXA-06); both spellings map to one
    # silver column, and a record carries only one of each pair.
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "id": "adjustment_action_id",
        "soFlag": "so_flag",
        "storProviderFlag": "stor_flag",  # legacy (pre-2026)
        "storFlag": "stor_flag",  # current API (ELEXA-06)
        "component": "component",  # legacy (pre-2026)
        "service": "component",  # current API
        "cost": "cost",
        "volume": "volume",
    }
    REQUIRED = ("settlement_date", "settlement_period")
    # ELEXA-06: the current API's `id` is an integer; the silver contract is text.
    CASTS = {"cost": pl.Float64(), "volume": pl.Float64(), "adjustment_action_id": pl.Utf8()}
    OUTPUT_COLS = (
        "settlement_date",
        "settlement_period",
        "timestamp_utc",
        "adjustment_action_id",
        "so_flag",
        "stor_flag",
        "component",
        "cost",
        "volume",
    )


register_transformer("elexon", "disbsad", DISBSADTransformer)
