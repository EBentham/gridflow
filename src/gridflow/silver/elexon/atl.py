"""Silver transformer for Elexon Actual Total Load Per Bidding Zone (ATL / B0610)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.elexon import ElexonATL
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


class ATLTransformer(ElexonTransformer):
    """Transform Elexon ATL data from bronze to silver."""

    source = "elexon"
    dataset = "atl"
    schema_cls = ElexonATL
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period")  # D-8
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "publishTime": "published_at",
        "quantity": "total_load_mw",
        "documentId": "document_id",
        "documentRevisionNumber": "document_revision",
        "businessType": "business_type",
    }
    REQUIRED = ("settlement_date", "settlement_period", "total_load_mw")
    CASTS = {"total_load_mw": pl.Float64()}
    OUTPUT_COLS = (
        "settlement_date",
        "settlement_period",
        "timestamp_utc",
        "total_load_mw",
        "business_type",
        "document_id",
        "document_revision",
        "published_at",
    )


register_transformer("elexon", "atl", ATLTransformer)
