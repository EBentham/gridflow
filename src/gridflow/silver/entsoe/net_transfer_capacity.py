"""Silver transformer for ENTSO-E net transfer capacity day-ahead (A61/A01)."""

from __future__ import annotations

from typing import ClassVar

import polars as pl

from gridflow.schemas.entsoe import EntsoeNetTransferCapacity
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.registry import register_transformer


class NetTransferCapacityTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E net transfer capacity XML from bronze to silver.

    A61/A01 (NTC day-ahead), fetched per zone pair like cross_border_flows,
    so each bronze file holds one in_domain / out_domain pair. The
    ``quantity`` value is the NTC in MW.
    """

    source = "entsoe"
    dataset = "net_transfer_capacity"
    schema_cls = EntsoeNetTransferCapacity
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "in_area_code", "out_area_code")
    EVENT_WINDOW_FILTER: ClassVar[bool] = True
    """B4 (N-9): FILTER_SAFE, vault-recorded live GB-FR: window-bounded
    single Point, curveType A03 (R3-RESEARCH.md:53); see
    ADR-026 Appendix A."""
    COLUMN_MAP = {"value": "ntc_mw", "in_domain": "in_area_code", "out_domain": "out_area_code"}
    REQUIRED = ("timestamp_utc", "ntc_mw", "in_area_code", "out_area_code")
    CASTS = {"ntc_mw": pl.Float64()}
    OUTPUT_COLS = (
        "timestamp_utc",
        "in_area_code",
        "out_area_code",
        "ntc_mw",
        "resolution",
        "published_at",
    )
    SORT = ("timestamp_utc", "in_area_code", "out_area_code")


register_transformer("entsoe", "net_transfer_capacity", NetTransferCapacityTransformer)
