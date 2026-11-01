"""Silver transformer for Elexon Loss of Load Probability and De-rated Margin (LOLPDRM)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.elexon import ElexonLOLPDRM
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


class LOLPDRMTransformer(ElexonTransformer):
    """Transform Elexon LOLPDRM data from bronze to silver."""

    source = "elexon"
    dataset = "lolpdrm"
    schema_cls = ElexonLOLPDRM
    APPEND_ONLY = True
    DATASET_VERSION = "2.0.0"
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period", "published_at")  # D-8
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "publishTime": "published_at",
        "publishingPeriodCommencingTime": "publishing_period_commencing_time",
        "lossOfLoadProbability": "loss_of_load_probability",
        "deratedMargin": "derated_margin_mw",
    }
    REQUIRED = (
        "settlement_date",
        "settlement_period",
        "loss_of_load_probability",
        "derated_margin_mw",
    )
    CASTS = {"loss_of_load_probability": pl.Float64(), "derated_margin_mw": pl.Float64()}
    OUTPUT_COLS = (
        "settlement_date",
        "settlement_period",
        "timestamp_utc",
        "loss_of_load_probability",
        "derated_margin_mw",
        "published_at",
    )


register_transformer("elexon", "lolpdrm", LOLPDRMTransformer)
