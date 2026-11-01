"""Silver transformer for ENTSO-E installed capacity per production unit."""

from __future__ import annotations

from typing import ClassVar

import polars as pl

from gridflow.schemas.entsoe import EntsoeInstalledCapacityUnits
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.registry import register_transformer


class InstalledCapacityUnitsTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E installed capacity per production unit XML to silver.

    Note on timestamps: ``available_at`` is the authoritative bitemporal
    publication timestamp added by ``BaseSilverTransformer``; ``ingested_at``
    is retained for backward compatibility as the local processing
    timestamp. Under ``--reingest`` the two diverge.
    """

    source = "entsoe"
    dataset = "installed_capacity_units"
    schema_cls = EntsoeInstalledCapacityUnits
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code", "unit_mrid")
    DATASET_VERSION: ClassVar[str] = "1.0.0"
    COLUMN_MAP = {"value": "capacity_mw", "in_domain": "area_code"}
    REQUIRED = ("timestamp_utc", "capacity_mw", "area_code", "unit_mrid")
    CASTS = {"capacity_mw": pl.Float64()}
    FILL_NULL = {"unit_mrid": "", "unit_name": "", "production_type": "unknown"}
    OUTPUT_COLS = (
        "timestamp_utc",
        "area_code",
        "production_type",
        "unit_mrid",
        "unit_name",
        "capacity_mw",
        "resolution",
        "published_at",
    )
    SORT = ("timestamp_utc", "area_code", "unit_mrid")

    def _derive(self, df: pl.DataFrame) -> pl.DataFrame:
        # A series without a unit mRID has no unit to key on.
        return df.filter(pl.col("unit_mrid") != "")


register_transformer("entsoe", "installed_capacity_units", InstalledCapacityUnitsTransformer)
