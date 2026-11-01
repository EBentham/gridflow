"""Silver transformer for ENTSO-E installed generation capacity aggregated (A68/A33)."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.entsoe import EntsoeInstalledCapacity
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.registry import register_transformer


class InstalledCapacityTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E installed generation capacity XML from bronze to silver.

    A68/A33 (installed capacity aggregated by technology), typically at P1Y
    or P1M resolution. Each TimeSeries carries ``MktPSRType / psrType`` for
    the technology; the ``quantity`` value is the installed capacity in MW.
    """

    source = "entsoe"
    dataset = "installed_capacity"
    schema_cls = EntsoeInstalledCapacity
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code", "production_type")
    COLUMN_MAP = {"value": "capacity_mw", "in_domain": "area_code"}
    REQUIRED = ("timestamp_utc", "capacity_mw", "area_code")
    CASTS = {"capacity_mw": pl.Float64()}
    FILL_NULL = {"production_type": "unknown"}
    OUTPUT_COLS = (
        "timestamp_utc",
        "area_code",
        "production_type",
        "capacity_mw",
        "resolution",
        "published_at",
    )
    SORT = ("timestamp_utc", "area_code", "production_type")


register_transformer("entsoe", "installed_capacity", InstalledCapacityTransformer)
