"""Silver transformer for Elexon Wind Generation Forecast (WINDFOR)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, ClassVar

import polars as pl

if TYPE_CHECKING:
    from collections.abc import Iterable

from gridflow.schemas.elexon import ElexonWindForecast
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc_expr

logger = logging.getLogger(__name__)


class WindForecastTransformer(ElexonTransformer):
    """Transform Elexon WINDFOR data from bronze to silver."""

    source = "elexon"
    dataset = "windfor"
    schema_cls = ElexonWindForecast
    DATASET_VERSION: ClassVar[str] = "1.0.0"
    # D-8: the static base key -- the PRIMARY (settlement-coordinate) shape.
    # Bronze without settlement_date and settlement_period falls back to a
    # timestamp_utc grain. That swaps the REQUIRED columns, which no additive
    # OPTIONAL_ENTITY_KEY_COLUMNS refinement can express, so
    # resolve_entity_key() is overridden below. These two class attributes
    # stay declared for the golden-map / completeness contract (A-4, A-13)
    # and remain correct for the primary shape.
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period")
    OPTIONAL_ENTITY_KEY_COLUMNS = ("published_at",)

    @classmethod
    def resolve_entity_key(cls, available_columns: Iterable[str]) -> tuple[str, ...]:
        """The grain ``dedup_to_grain`` enforces: settlement_date +
        settlement_period when both are present, else the timestamp_utc-only
        fallback; published_at appended when present (transform() always
        emits it -- typed-null when bronze lacks the publish field)."""
        available = set(available_columns)
        key: tuple[str, ...]
        if "settlement_date" in available and "settlement_period" in available:
            key = ("settlement_date", "settlement_period")
        else:
            key = ("timestamp_utc",)
        if "published_at" in available:
            key = (*key, "published_at")
        return key

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        column_mapping = {
            "settlementDate": "settlement_date",
            "settlementPeriod": "settlement_period",
            "initialForecast": "initial_forecast_mw",
            "latestForecast": "latest_forecast_mw",
            "generation": "latest_forecast_mw",
            "publishDateTime": "published_at",
            "publishTime": "published_at",
            "startTimeOfHalfHrPeriod": "start_time",
            "startTime": "start_time",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        # Try to derive timestamp from settlement date/period or start_time
        has_sp = "settlement_date" in raw_df.columns and "settlement_period" in raw_df.columns

        if not has_sp and "start_time" not in raw_df.columns:
            logger.error(
                "Missing required columns in WINDFOR: need settlement_date/period or start_time"
            )
            return pl.DataFrame()

        df = raw_df

        if has_sp:
            df = df.with_columns(
                [
                    pl.col("settlement_date").cast(pl.Date),
                    pl.col("settlement_period").cast(pl.Int32),
                ]
            )
            df = df.with_columns(
                settlement_period_to_utc_expr("settlement_date", "settlement_period").alias(
                    "timestamp_utc"
                )
            )
        elif "start_time" in df.columns:
            # Polars ≥1.x requires explicit format when string contains tz info.
            df = df.with_columns(
                pl.col("start_time")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                .dt.replace_time_zone("UTC")
                .alias("timestamp_utc")
            )

        for col in ["initial_forecast_mw", "latest_forecast_mw"]:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Float64))

        if "published_at" in df.columns:
            df = df.with_columns(
                pl.col("published_at")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                .dt.replace_time_zone("UTC")
            )
        else:
            # WHY: the silver schema declares published_at as a nullable contract
            # column. Emit it as typed-null when bronze lacks the publish field so the
            # silver schema is deterministic and partition globs don't drift across
            # history (a missing column breaks SELECT * reads spanning files that do
            # carry it).
            df = df.with_columns(pl.lit(None).cast(pl.Datetime("us", "UTC")).alias("published_at"))

        df = self.dedup_to_grain(df)

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("elexon").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        output_cols = [
            "settlement_date",
            "settlement_period",
            "timestamp_utc",
            "initial_forecast_mw",
            "latest_forecast_mw",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "windfor", WindForecastTransformer)
