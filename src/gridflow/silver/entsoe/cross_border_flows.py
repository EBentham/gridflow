"""Silver transformer for ENTSO-E physical cross-border flows."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import ClassVar

import polars as pl

from gridflow.schemas.entsoe import EntsoeCrossborderFlow
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.entsoe._published_at import with_published_at
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


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

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        required = ["timestamp_utc", "value", "in_domain", "out_domain"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error("Missing required columns in cross_border_flows: %s", missing)
            return pl.DataFrame()

        df = raw_df.rename(
            {
                "value": "flow_mw",
                "in_domain": "in_area_code",
                "out_domain": "out_area_code",
            }
        )

        if df["timestamp_utc"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC")))

        df = df.with_columns(pl.col("flow_mw").cast(pl.Float64))

        df = self.dedup_to_grain(df)

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("entsoe").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        # ADR-025 P1.1: carry the document publication vintage (createdDateTime) as published_at.
        df = with_published_at(df, dataset=self.dataset)

        output_cols = [
            "timestamp_utc",
            "in_area_code",
            "out_area_code",
            "flow_mw",
            "resolution",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc", "in_area_code", "out_area_code")


register_transformer("entsoe", "cross_border_flows", CrossBorderFlowsTransformer)
