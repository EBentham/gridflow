"""Silver transformer for ENTSO-E physical cross-border flows."""

from __future__ import annotations

from typing import ClassVar

import polars as pl

from gridflow.schemas.entsoe import EntsoeCrossborderFlow
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.registry import register_transformer


class CrossBorderFlowsTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E cross-border flow XML from bronze to silver."""

    source = "entsoe"
    dataset = "cross_border_flows"
    schema_cls = EntsoeCrossborderFlow
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "in_area_code", "out_area_code")
    EVENT_WINDOW_FILTER: ClassVar[bool] = True
    """B4 (N-9): FILTER_SAFE, live-probed FR-BE 2026-08-01 -- period.time
    Interval matches the request window exactly (R3-RESEARCH.md:50); see
    ADR-026 Appendix A."""
    COLUMN_MAP = {"value": "flow_mw", "in_domain": "in_area_code", "out_domain": "out_area_code"}
    REQUIRED = ("timestamp_utc", "flow_mw", "in_area_code", "out_area_code")
    CASTS = {"flow_mw": pl.Float64()}
    OUTPUT_COLS = (
        "timestamp_utc",
        "in_area_code",
        "out_area_code",
        "flow_mw",
        "resolution",
        "published_at",
    )
    SORT = ("timestamp_utc", "in_area_code", "out_area_code")


register_transformer("entsoe", "cross_border_flows", CrossBorderFlowsTransformer)
