"""Silver transformers for H7 ENTSO-E outage extension datasets."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar

import polars as pl

from gridflow.schemas.entsoe import (
    EntsoeOutagesConsumption,
    EntsoeOutagesOffshoreGrid,
    EntsoeOutagesProduction,
    EntsoeOutagesTransmission,
)
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.entsoe._outages import (
    OUTAGE_COMMON_COLUMNS,
    latest_revision_steps,
    prepare_outage_frame,
    read_outage_partition,
)
from gridflow.silver.entsoe._published_at import with_published_at
from gridflow.silver.registry import register_transformer

if TYPE_CHECKING:
    from datetime import date

    from pydantic import BaseModel

logger = logging.getLogger(__name__)


class _H7OutageTransformer(BaseSilverTransformer):
    """Shared outage transformer: one row per step of each document's latest revision.

    Rows are clipped to the partition's UTC day (see ``_outages.py``).
    ``identity`` names one step of one document together with
    ``timestamp_utc``; ``document_status`` ``A09`` marks a cancelled outage.
    """

    schema_cls: ClassVar[type[BaseModel]] = EntsoeOutagesConsumption
    DATASET_VERSION: ClassVar[str] = "2.0.0"
    identity_cols: ClassVar[tuple[str, ...]] = ()
    required_domain_cols: ClassVar[frozenset[str]] = frozenset({"in_domain"})
    rename_cols: ClassVar[dict[str, str]] = {"in_domain": "area_code"}
    lead_cols: ClassVar[tuple[str, ...]] = ()

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        bronze_path = self._bronze_path_for_date(target_date)
        if bronze_path is None:
            return pl.DataFrame()
        return read_outage_partition(bronze_path, target_date)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        required = {"timestamp_utc", "step_end_utc", "value"} | self.required_domain_cols
        missing = required - set(raw_df.columns)
        if missing:
            logger.error("Missing required columns in %s: %s", self.dataset, missing)
            return pl.DataFrame()

        df = prepare_outage_frame(raw_df.rename(self.rename_cols))
        df = latest_revision_steps(df, ("document_mrid", "timeseries_mrid", *self.identity_cols))
        # ADR-025 P1.1: carry the document publication vintage (createdDateTime)
        # as published_at before the hard select; typed-null when absent.
        df = with_published_at(df, dataset=self.dataset)
        return df.select(["timestamp_utc", "step_end_utc", *self.lead_cols, *OUTAGE_COMMON_COLUMNS])


class OutagesConsumptionTransformer(_H7OutageTransformer):
    source = "entsoe"
    dataset = "outages_consumption"
    schema_cls = EntsoeOutagesConsumption
    lead_cols = ("area_code",)


class OutagesTransmissionTransformer(_H7OutageTransformer):
    source = "entsoe"
    dataset = "outages_transmission"
    schema_cls = EntsoeOutagesTransmission
    required_domain_cols = frozenset({"in_domain", "out_domain"})
    rename_cols = {"in_domain": "in_area_code", "out_domain": "out_area_code"}
    identity_cols = ("asset_mrid",)
    lead_cols = ("in_area_code", "out_area_code", "asset_mrid", "asset_name")


class OutagesOffshoreGridTransformer(_H7OutageTransformer):
    source = "entsoe"
    dataset = "outages_offshore_grid"
    schema_cls = EntsoeOutagesOffshoreGrid
    identity_cols = ("asset_mrid",)
    lead_cols = ("area_code", "asset_mrid", "asset_name", "nominal_mw")


class OutagesProductionTransformer(_H7OutageTransformer):
    source = "entsoe"
    dataset = "outages_production"
    schema_cls = EntsoeOutagesProduction
    identity_cols = ("unit_mrid",)
    lead_cols = ("area_code", "unit_mrid", "unit_name", "production_type", "nominal_mw")


_TRANSFORMERS = [
    OutagesConsumptionTransformer,
    OutagesTransmissionTransformer,
    OutagesOffshoreGridTransformer,
    OutagesProductionTransformer,
]

for transformer_cls in _TRANSFORMERS:
    register_transformer("entsoe", transformer_cls.dataset, transformer_cls)
