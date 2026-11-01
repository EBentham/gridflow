"""Silver transformer for ENTSO-E water reservoirs and hydro storage plants."""

from __future__ import annotations

import polars as pl

from gridflow.schemas.entsoe import EntsoeWaterReservoirs
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.registry import register_transformer


class WaterReservoirsTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E water reservoir XML to silver."""

    source = "entsoe"
    dataset = "water_reservoirs"
    schema_cls = EntsoeWaterReservoirs
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code")
    COLUMN_MAP = {"value": "reservoir_mwh", "in_domain": "area_code"}
    REQUIRED = ("timestamp_utc", "reservoir_mwh", "area_code")
    CASTS = {"reservoir_mwh": pl.Float64()}
    OUTPUT_COLS = ("timestamp_utc", "area_code", "reservoir_mwh", "resolution", "published_at")


register_transformer("entsoe", "water_reservoirs", WaterReservoirsTransformer)
