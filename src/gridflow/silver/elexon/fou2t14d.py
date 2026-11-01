"""Silver transformer for Elexon 2-14 Day Generation Availability by Fuel Type (FOU2T14D)."""

from __future__ import annotations

from typing import ClassVar

import polars as pl

from gridflow.schemas.elexon import ElexonFOU2T14D
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


class FOU2T14DTransformer(ElexonTransformer):
    """Transform Elexon FOU2T14D data from bronze to silver.

    Append-only: silver keeps every vendor publication bronze carries for a
    key (F-06), so ``published_at`` is in the grain unconditionally, and if it
    ever drifted out of the output the dedup would raise rather than collapse
    vintages. Picking the latest publication is a read-time concern, served
    by ``silver_elexon_fou2t14d_latest`` (ADR-025) and gridflow_models'
    ``latest_only`` fetch. ``forecastDate`` is a UK local day, so a row
    without a settlement period is stamped at the start of period 1.
    """

    source = "elexon"
    dataset = "fou2t14d"
    schema_cls = ElexonFOU2T14D
    APPEND_ONLY: ClassVar[bool] = True
    DATASET_VERSION: ClassVar[str] = "2.0.0"
    ENTITY_KEY_COLUMNS = ("settlement_date", "fuel_type", "published_at")  # D-8
    OPTIONAL_ENTITY_KEY_COLUMNS = ("settlement_period",)
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "forecastDate": "settlement_date",
        "publishDateTime": "published_at",
        "publishTime": "published_at",
        "fuelType": "fuel_type",
        "outputUsable": "output_usable_mw",
    }
    REQUIRED = ("settlement_date", "fuel_type", "output_usable_mw")
    CASTS = {"fuel_type": pl.Utf8(), "output_usable_mw": pl.Float64()}
    OUTPUT_COLS = (
        "settlement_date",
        "settlement_period",
        "timestamp_utc",
        "fuel_type",
        "output_usable_mw",
        "published_at",
    )
    SORT = ("timestamp_utc", "fuel_type")


register_transformer("elexon", "fou2t14d", FOU2T14DTransformer)
