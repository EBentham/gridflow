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
    # Some units carry only a National Grid id (bmUnit null), as in UOU2T14D.
    OPTIONAL_ENTITY_KEY_COLUMNS = ("time_from", "national_grid_bm_unit")
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "bmUnit": "bm_unit_id",
        "nationalGridBmUnit": "national_grid_bm_unit",
        "levelFrom": "level_from",
        "levelTo": "level_to",
        "timeFrom": "time_from",
        "timeTo": "time_to",
    }
    REQUIRED = ("settlement_date", "settlement_period", "bm_unit_id")
    CASTS = {
        "bm_unit_id": pl.Utf8(),
        "national_grid_bm_unit": pl.Utf8(),
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
        "national_grid_bm_unit",
        "time_from",
        "time_to",
        "level_from",
        "level_to",
    )
    SORT = ("timestamp_utc", "bm_unit_id")

    def dedup_to_grain(self, df: pl.DataFrame) -> pl.DataFrame:
        # A fetch can send a unit with bmUnit null that a later fetch fills in
        # (AG-DSTK08 on 2026-09-21). Give the null row the id its National Grid
        # id carries in the same period, so the grain keeps one row, the last
        # read, instead of both. A unit no fetch names keeps its null.
        if "national_grid_bm_unit" in df.columns:
            same_unit = ["settlement_date", "settlement_period", "national_grid_bm_unit"]
            named = pl.col("bm_unit_id").max().over(same_unit)
            df = df.with_columns(
                pl.col("bm_unit_id").fill_null(
                    pl.when(pl.col("national_grid_bm_unit").is_not_null()).then(named)
                )
            )
        return super().dedup_to_grain(df)


register_transformer("elexon", "pn", PNTransformer)
