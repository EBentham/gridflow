"""Silver transformer for ENTSO-E unavailability of generation units (A80)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar

import polars as pl

from gridflow.schemas.entsoe import EntsoeOutagesGeneration
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.entsoe._enum_maps import UNMAPPED_SENTINEL
from gridflow.silver.entsoe._outages import (
    OUTAGE_COMMON_COLUMNS,
    latest_revision_steps,
    outage_steps_for_day,
    parse_outage_xml,
    prepare_outage_frame,
)
from gridflow.silver.published_at import with_published_at
from gridflow.silver.registry import register_transformer

if TYPE_CHECKING:
    from datetime import date
    from pathlib import Path

logger = logging.getLogger(__name__)


class OutagesGenerationTransformer(EntsoeXmlTransformer):
    """Transform ENTSO-E generation unavailability XML (A80) from bronze to silver.

    Each silver row is one step of one outage document's latest revision,
    clipped to the partition's UTC day: ``timestamp_utc`` to ``step_end_utc``
    at ``unavailable_mw``. ``unit_mrid`` is the production unit
    (``production_RegisteredResource``) and ``generation_unit_mrid`` the
    generation unit inside it; ``nominal_mw`` is that unit's nominal power.
    ``document_status`` ``A09`` marks a cancelled outage. The XML ``<quantity>``
    is renamed ``unavailable_mw``; whether it is the available or the
    unavailable power is still unverified (backlog 13am).
    """

    source = "entsoe"
    dataset = "outages_generation"
    schema_cls = EntsoeOutagesGeneration
    DATASET_VERSION: ClassVar[str] = "2.0.0"

    ENTITY_KEY_COLUMNS = (
        "document_mrid",
        "timeseries_mrid",
        "generation_unit_mrid",
        "timestamp_utc",
    )

    def _parse_bronze_body(self, path: Path) -> list[dict[str, Any]]:
        return parse_outage_xml(path.read_bytes())

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        pairs = self._read_bronze_records(self._bronze_read_set(target_date), target_date)
        return outage_steps_for_day([r for _, records in pairs for r in records], target_date)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        required = [
            "timestamp_utc",
            "step_end_utc",
            "value",
            "in_domain",
            "business_type",
            "unit_mrid",
        ]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error("Missing required columns in outages_generation: %s", missing)
            return pl.DataFrame()

        df = prepare_outage_frame(raw_df.rename({"in_domain": "area_code"}))
        df = df.with_columns(
            pl.col("business_type")
            .replace_strict(
                {"A53": "planned", "A54": "unplanned"},
                default=UNMAPPED_SENTINEL,
                return_dtype=pl.Utf8,
            )
            .alias("outage_type")
        )
        df = self.dedup_to_grain(latest_revision_steps(df))
        # ADR-025 P1.1: carry the document publication vintage (createdDateTime) as published_at.
        df = with_published_at(df, dataset=self.dataset)

        output_cols = [
            "timestamp_utc",
            "step_end_utc",
            "area_code",
            "unit_mrid",
            "unit_name",
            "generation_unit_mrid",
            "generation_unit_name",
            "production_type",
            "nominal_mw",
            *OUTAGE_COMMON_COLUMNS,
        ]
        df = df.select(output_cols).sort("timestamp_utc", "unit_mrid", "document_mrid")

        self.last_unmapped_count = int(df.filter(pl.col("outage_type") == UNMAPPED_SENTINEL).height)
        if self.last_unmapped_count > 0:
            raw_codes = raw_df.get_column("business_type").unique().to_list()
            unmapped_codes = sorted(c for c in raw_codes if c not in {"A53", "A54"})
            logger.warning(
                "%s/%s: %d unmapped business_type row(s) labelled %r; unmapped raw codes: %s",
                self.source,
                self.dataset,
                self.last_unmapped_count,
                UNMAPPED_SENTINEL,
                unmapped_codes,
            )

        return df


register_transformer("entsoe", "outages_generation", OutagesGenerationTransformer)
