"""Silver transformer for Elexon System Frequency (FREQ)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.elexon import ElexonFrequency
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


class FreqTransformer(ElexonTransformer):
    """Transform Elexon FREQ data from bronze to silver."""

    source = "elexon"
    dataset = "freq"
    schema_cls = ElexonFrequency
    ENTITY_KEY_COLUMNS = ("timestamp_utc",)  # D-8
    COLUMN_MAP = {
        "reportDateTime": "timestamp_utc",
        "measurementTime": "timestamp_utc",
        "frequency": "frequency_hz",
    }
    REQUIRED = ("timestamp_utc", "frequency_hz")
    CASTS = {"timestamp_utc": pl.Datetime("us", "UTC"), "frequency_hz": pl.Float64()}
    SETTLEMENT_DATE = None
    OUTPUT_COLS = ("timestamp_utc", "frequency_hz")


register_transformer("elexon", "freq", FreqTransformer)
