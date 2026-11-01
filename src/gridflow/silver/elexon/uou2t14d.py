"""Silver transformer for Elexon 2-14 Day Generation Availability by BM Unit (UOU2T14D)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.elexon import ElexonUOU2T14D
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


class UOU2T14DTransformer(ElexonTransformer):
    """Transform Elexon UOU2T14D data from bronze to silver.

    A row without a settlement period (``forecastDate`` only) is a UK
    settlement day, stamped at the start of period 1. ``fuel_type`` and
    ``national_grid_bm_unit`` are kept so the dataset is self-describing
    (G5-W2.3).
    """

    source = "elexon"
    dataset = "uou2t14d"
    schema_cls = ElexonUOU2T14D
    APPEND_ONLY = True
    DATASET_VERSION = "3.0.0"
    ENTITY_KEY_COLUMNS = ("settlement_date", "bm_unit_id", "published_at")  # D-8
    # bm_unit_id is null on about 26,000 rows a day; the National Grid unit,
    # one-to-one with it where both exist, keeps those rows apart.
    OPTIONAL_ENTITY_KEY_COLUMNS = ("national_grid_bm_unit", "settlement_period")
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "forecastDate": "settlement_date",
        "publishDateTime": "published_at",
        "publishTime": "published_at",
        "bmUnit": "bm_unit_id",
        "outputUsable": "output_usable_mw",
        "nationalGridBmUnit": "national_grid_bm_unit",
        "fuelType": "fuel_type",
    }
    REQUIRED = ("settlement_date", "bm_unit_id", "output_usable_mw")
    CASTS = {
        "bm_unit_id": pl.Utf8(),
        "output_usable_mw": pl.Float64(),
        "fuel_type": pl.Utf8(),
        "national_grid_bm_unit": pl.Utf8(),
    }
    OUTPUT_COLS = (
        "settlement_date",
        "settlement_period",
        "timestamp_utc",
        "bm_unit_id",
        "national_grid_bm_unit",
        "fuel_type",
        "output_usable_mw",
        "published_at",
    )
    SORT = ("timestamp_utc", "bm_unit_id")


register_transformer("elexon", "uou2t14d", UOU2T14DTransformer)
