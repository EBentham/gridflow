"""Silver transformer for Elexon Market Index Data (MID)."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import ClassVar

import polars as pl

from gridflow.schemas.elexon import ElexonMID
from gridflow.silver.base import VintagePolicy
from gridflow.silver.elexon._base import ElexonTransformer
from gridflow.silver.registry import register_transformer


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
    ENTITY_KEY_COLUMNS = ("settlement_date", "settlement_period")  # D-8
    OPTIONAL_ENTITY_KEY_COLUMNS = ("data_provider_id",)
    DATASET_VERSION = "1.1.0"
    # G5-W1.2 (2026-05): the live API renamed two fields; the legacy keys stay
    # so historical bronze still ingests.
    COLUMN_MAP = {
        "settlementDate": "settlement_date",
        "settlementPeriod": "settlement_period",
        "dataProviderId": "data_provider_id",  # legacy (pre-2026)
        "dataProvider": "data_provider_id",  # current API
        "midPrice": "market_index_price",  # legacy (pre-2026)
        "price": "market_index_price",  # current API
        "volume": "market_index_volume",
    }
    REQUIRED = ("settlement_date", "settlement_period")
    CASTS = {"market_index_price": pl.Float64(), "market_index_volume": pl.Float64()}
    OUTPUT_COLS = (
        "settlement_date",
        "settlement_period",
        "timestamp_utc",
        "data_provider_id",
        "market_index_price",
        "market_index_volume",
    )


register_transformer("elexon", "mid", MIDTransformer)
