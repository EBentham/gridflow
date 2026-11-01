"""Silver transformer for Elexon Bid/Offer Acceptance Levels (BOAL)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.elexon import ElexonBOAL
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


class BOALTransformer(ElexonTransformer):
    """Transform Elexon BOAL data from bronze to silver.

    An acceptance carries one row per step; ``time_from`` tells them apart.
    """

    source = "elexon"
    dataset = "boal"
    schema_cls = ElexonBOAL
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period", "bm_unit_id")  # D-8
    DATASET_VERSION = "2.0.0"
    OPTIONAL_ENTITY_KEY_COLUMNS = ("acceptance_number", "time_from")
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        # API may return settlementPeriod or settlementPeriodFrom
        "settlementPeriod": "settlement_period",
        "settlementPeriodFrom": "settlement_period",
        "bmUnit": "bm_unit_id",
        "acceptanceNumber": "acceptance_number",
        "acceptanceTime": "acceptance_time",
        "deemedBoFlag": "deem_flag",
        "soFlag": "so_flag",
        "storProviderFlag": "stor_flag",
        "storFlag": "stor_flag",
        "rrFlag": "rr_flag",
        "levelFrom": "bid_offer_level_from",
        "levelTo": "bid_offer_level_to",
        "timeFrom": "time_from",
        "timeTo": "time_to",
        "nationalGridBmUnit": "national_grid_bm_unit",
        "amendmentFlag": "amendment_flag",
    }
    REQUIRED = ("settlement_date", "settlement_period", "bm_unit_id")
    CASTS = {
        "bm_unit_id": pl.Utf8(),
        "acceptance_time": pl.Datetime("us", "UTC"),
        "time_from": pl.Datetime("us", "UTC"),
        "time_to": pl.Datetime("us", "UTC"),
    }
    OUTPUT_COLS = (
        "settlement_date",
        "settlement_period",
        "timestamp_utc",
        "bm_unit_id",
        "acceptance_number",
        "acceptance_time",
        "time_from",
        "time_to",
        "deem_flag",
        "so_flag",
        "stor_flag",
        "rr_flag",
        "bid_offer_level_from",
        "bid_offer_level_to",
    )
    SORT = ("timestamp_utc", "bm_unit_id")


register_transformer("elexon", "boal", BOALTransformer)
