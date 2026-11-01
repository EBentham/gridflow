"""Silver transformer for Elexon Market Index Data (MID)."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from typing import ClassVar

import polars as pl

from gridflow.schemas.elexon import ElexonMID
from gridflow.silver.base import VintagePolicy
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer
from gridflow.utils.time import settlement_period_to_utc

logger = logging.getLogger(__name__)


class MIDTransformer(ElexonTransformer):
    """Transform Elexon MID data from bronze to silver."""

    source = "elexon"
    dataset = "mid"
    schema_cls = ElexonMID
    PARTITION_DATE_COLUMN: ClassVar[str | None] = "settlement_date"
    PARTITION_SOURCE_OFFSETS: ClassVar[tuple[int, ...]] = (-1, 0)
    VINTAGE_POLICY = VintagePolicy(
        name="elexon-mid/vp-2026-09b",
        lag=timedelta(minutes=35),
        dated=date(2026, 9, 7),
        rule=(
            "ASSUMPTION: 35 minutes from period start, equivalent to period end plus "
            "a five-minute margin. An approximately one-minute current observation "
            "motivates this assumption but does not prove a historical upper bound. "
            "TODO: verify MID publication cadence. "
            "ASSUMPTION cutover: 2026-08-01T00:00Z."
        ),
        applies_before=datetime(2026, 8, 1, tzinfo=UTC),
    )
    ENTITY_KEY_COLUMNS = (
        "settlement_date",
        "settlement_period",
    )  # D-8: verbatim from unique() below
    OPTIONAL_ENTITY_KEY_COLUMNS = ("data_provider_id",)
    DATASET_VERSION = "1.1.0"

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        # G5-W1.2 (2026-05): live API renamed two fields. Keep legacy keys
        # so historical bronze still ingests cleanly.
        # - dataProviderId  → dataProvider  (current)
        # - midPrice        → price         (current)
        column_mapping = {
            "settlementDate": "settlement_date",
            "settlementPeriod": "settlement_period",
            "dataProviderId": "data_provider_id",  # legacy (pre-2026)
            "dataProvider": "data_provider_id",  # current API
            "midPrice": "market_index_price",  # legacy (pre-2026)
            "price": "market_index_price",  # current API
            "volume": "market_index_volume",
        }
        rename_map = {k: v for k, v in column_mapping.items() if k in raw_df.columns}
        if rename_map:
            raw_df = raw_df.rename(rename_map)

        required = ["settlement_date", "settlement_period"]
        missing = [c for c in required if c not in raw_df.columns]
        if missing:
            logger.error(f"Missing required columns in MID: {missing}")
            return pl.DataFrame()

        df = raw_df.with_columns(
            [
                pl.col("settlement_date").cast(pl.Date),
                pl.col("settlement_period").cast(pl.Int32),
            ]
        )

        if "market_index_price" in df.columns:
            df = df.with_columns(pl.col("market_index_price").cast(pl.Float64))
        if "market_index_volume" in df.columns:
            df = df.with_columns(pl.col("market_index_volume").cast(pl.Float64))

        df = df.with_columns(
            pl.struct(["settlement_date", "settlement_period"])
            .map_elements(
                lambda row: settlement_period_to_utc(
                    row["settlement_date"], row["settlement_period"]
                ),
                return_dtype=pl.Datetime("us", "UTC"),
            )
            .alias("timestamp_utc")
        )

        dedup_cols = ["settlement_date", "settlement_period"]
        if "data_provider_id" in df.columns:
            dedup_cols.append("data_provider_id")
        df = df.unique(subset=dedup_cols, keep="last")

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
            "data_provider_id",
            "market_index_price",
            "market_index_volume",
            "data_provider",
            "ingested_at",
        ]
        available = [c for c in output_cols if c in df.columns]
        return df.select(available).sort("timestamp_utc")


register_transformer("elexon", "mid", MIDTransformer)
