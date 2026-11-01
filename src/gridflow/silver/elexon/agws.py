"""Silver transformer for Elexon Actual/Estimated Wind and Solar Power Generation (AGWS / B1630)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.elexon import ElexonAGWS
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


class AGWSTransformer(ElexonTransformer):
    """Transform Elexon AGWS data from bronze to silver."""

    source = "elexon"
    dataset = "agws"
    schema_cls = ElexonAGWS
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period", "psr_type")  # D-8
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "publishTime": "published_at",
        "psrType": "psr_type",
        "quantity": "generation_mw",
        "businessType": "business_type",
        "documentId": "document_id",
        "documentRevisionNumber": "document_revision",
    }
    REQUIRED = ("settlement_date", "settlement_period", "psr_type", "generation_mw")
    CASTS = {"generation_mw": pl.Float64(), "psr_type": pl.Utf8()}
    OUTPUT_COLS = (
        "settlement_date",
        "settlement_period",
        "timestamp_utc",
        "psr_type",
        "generation_mw",
        "business_type",
        "document_id",
        "document_revision",
        "published_at",
    )
    SORT = ("timestamp_utc", "psr_type")


register_transformer("elexon", "agws", AGWSTransformer)
