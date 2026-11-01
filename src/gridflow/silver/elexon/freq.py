"""Silver transformer for Elexon System Frequency (FREQ)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import polars as pl

from gridflow.schemas.elexon import ElexonFrequency
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class FreqTransformer(ElexonTransformer):
    """Transform Elexon FREQ data from bronze to silver."""

    source = "elexon"
    dataset = "freq"
    schema_cls = ElexonFrequency
    ENTITY_KEY_COLUMNS = ("timestamp_utc",)  # D-8: verbatim from unique() below

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        column_mapping = {
            "reportDateTime": "timestamp_utc",
            "measurementTime": "timestamp_utc",
            "frequency": "frequency_hz",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["timestamp_utc", "frequency_hz"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in FREQ: {missing}")
            return pl.DataFrame()

        # Polars ≥1.x requires an explicit format when the string contains tz info.
        # Elexon FREQ timestamps are ISO-8601 UTC (e.g. "2024-01-15T00:00:00Z").
        df = raw_df.with_columns(
            [
                pl.col("timestamp_utc")
                .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
                .dt.replace_time_zone("UTC"),
                pl.col("frequency_hz").cast(pl.Float64),
            ]
        )

        df = df.unique(subset=["timestamp_utc"], keep="last")

        now = datetime.now(UTC)
        df = df.with_columns(
            [
                pl.lit("elexon").alias("data_provider"),
                pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
            ]
        )

        output_cols = ["timestamp_utc", "frequency_hz", "data_provider", "ingested_at"]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "freq", FreqTransformer)
