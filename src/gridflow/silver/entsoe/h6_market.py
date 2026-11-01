"""Silver transformers for H6 ENTSO-E transmission and market time series."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, ClassVar

import polars as pl

from gridflow.schemas.entsoe import (
    EntsoeTransmissionMarketAmount,
    EntsoeTransmissionMarketQuantity,
)
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.entsoe._published_at import with_published_at
from gridflow.silver.registry import register_transformer

if TYPE_CHECKING:
    from pydantic import BaseModel

logger = logging.getLogger(__name__)


class _H6ZonePairTransformer(EntsoeXmlTransformer):
    """Shared H6 transformer for zone-pair time-series payloads."""

    value_tag = "quantity"
    value_column = "quantity_mw"
    schema_cls: ClassVar[type[BaseModel]] = EntsoeTransmissionMarketQuantity
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "in_area_code", "out_area_code", "business_type")

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        required = {"timestamp_utc", "value", "in_domain", "out_domain"}
        missing = required - set(raw_df.columns)
        if missing:
            logger.error("Missing required columns in %s: %s", self.dataset, missing)
            return pl.DataFrame()

        df = raw_df.rename(
            {
                "value": self.value_column,
                "in_domain": "in_area_code",
                "out_domain": "out_area_code",
            }
        )

        if "business_type" not in df.columns:
            df = df.with_columns(pl.lit("").alias("business_type"))
        if "resolution" not in df.columns:
            df = df.with_columns(pl.lit("").alias("resolution"))

        if df["timestamp_utc"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC")))

        now = datetime.now(UTC)
        df = (
            df.with_columns(
                [
                    pl.col(self.value_column).cast(pl.Float64),
                    pl.lit("entsoe").alias("data_provider"),
                    pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
                ]
            )
            .pipe(self.dedup_to_grain)
            .sort(["timestamp_utc", "in_area_code", "out_area_code"])
        )

        # ADR-025 P1.1: carry the document publication vintage (createdDateTime)
        # as published_at; typed-null when the source lacks it.
        df = with_published_at(df, dataset=self.dataset)

        output_cols = [
            "timestamp_utc",
            "in_area_code",
            "out_area_code",
            self.value_column,
            "business_type",
            "resolution",
            "published_at",
            "data_provider",
            "ingested_at",
        ]
        df = df.select(output_cols)

        return df


class _H6QuantityTransformer(_H6ZonePairTransformer):
    value_tag = "quantity"
    value_column = "quantity_mw"
    schema_cls = EntsoeTransmissionMarketQuantity


class _H6AmountTransformer(_H6ZonePairTransformer):
    value_tag = "price.amount"
    value_column = "amount_eur"
    schema_cls = EntsoeTransmissionMarketAmount


class DcLinkIntradayTransferLimitsTransformer(_H6QuantityTransformer):
    dataset = "dc_link_intraday_transfer_limits"
    source = "entsoe"
    # B4 (N-9): FILTER_SAFE, vault-recorded real EMPTY + structural analogy
    # to the directly-verified quantity family (R3-RESEARCH.md:57); see
    # ADR-026 Appendix A.
    EVENT_WINDOW_FILTER: ClassVar[bool] = True


class CommercialSchedulesTransformer(_H6QuantityTransformer):
    dataset = "commercial_schedules"
    source = "entsoe"
    # B4 (N-9): FILTER_SAFE, vault-recorded live GB-FR 2026-05-06, window-
    # bounded (R3-RESEARCH.md:51); see ADR-026 Appendix A.
    EVENT_WINDOW_FILTER: ClassVar[bool] = True


# CommercialSchedulesNetPositionsTransformer removed in V2 (ADR-019).


class RedispatchingCrossBorderTransformer(_H6QuantityTransformer):
    dataset = "redispatching_cross_border"
    source = "entsoe"
    # B4 (N-9): FILTER_SAFE, vault-recorded real EMPTY + structural analogy
    # (R3-RESEARCH.md:58); see ADR-026 Appendix A.
    EVENT_WINDOW_FILTER: ClassVar[bool] = True


class RedispatchingInternalTransformer(_H6QuantityTransformer):
    dataset = "redispatching_internal"
    source = "entsoe"
    # B4 (N-9): FILTER_SAFE, vault-recorded real EMPTY + structural analogy
    # (R3-RESEARCH.md:59); see ADR-026 Appendix A.
    EVENT_WINDOW_FILTER: ClassVar[bool] = True


class CountertradingTransformer(_H6QuantityTransformer):
    dataset = "countertrading"
    source = "entsoe"
    # B4 (N-9): FILTER_SAFE, vault-recorded real EMPTY + structural analogy
    # (R3-RESEARCH.md:60); see ADR-026 Appendix A.
    EVENT_WINDOW_FILTER: ClassVar[bool] = True


class OfferedTransferCapacityContinuousTransformer(_H6QuantityTransformer):
    dataset = "offered_transfer_capacity_continuous"
    source = "entsoe"
    # B4 (N-9): FILTER_SAFE, vault-recorded real EMPTY + structural analogy
    # (R3-RESEARCH.md:61); see ADR-026 Appendix A.
    EVENT_WINDOW_FILTER: ClassVar[bool] = True


class OfferedTransferCapacityImplicitTransformer(_H6QuantityTransformer):
    dataset = "offered_transfer_capacity_implicit"
    source = "entsoe"
    # B4 (N-9): FILTER_SAFE, vault + live probe real EMPTY across 3
    # independent probes (R3-RESEARCH.md:62); see
    # ADR-026 Appendix A.
    EVENT_WINDOW_FILTER: ClassVar[bool] = True


class OfferedTransferCapacityExplicitTransformer(_H6QuantityTransformer):
    dataset = "offered_transfer_capacity_explicit"
    source = "entsoe"
    # B4 (N-9): FILTER_SAFE, vault-recorded real EMPTY + structural analogy
    # (R3-RESEARCH.md:63); see ADR-026 Appendix A.
    EVENT_WINDOW_FILTER: ClassVar[bool] = True


class TransferCapacityUseTransformer(_H6QuantityTransformer):
    dataset = "transfer_capacity_use"
    source = "entsoe"
    # B4 (N-9): FILTER_SAFE, vault + 2 live probes real EMPTY, quantity
    # schema (R3-RESEARCH.md:64); see
    # ADR-026 Appendix A.
    EVENT_WINDOW_FILTER: ClassVar[bool] = True


class TotalNominatedCapacityTransformer(_H6QuantityTransformer):
    dataset = "total_nominated_capacity"
    source = "entsoe"
    # B4 (N-9): FILTER_SAFE, vault-recorded live GB-FR 2026-05-06, window
    # matches exactly (R3-RESEARCH.md:52); see
    # ADR-026 Appendix A.
    EVENT_WINDOW_FILTER: ClassVar[bool] = True


class TotalCapacityAllocatedTransformer(_H6QuantityTransformer):
    dataset = "total_capacity_allocated"
    source = "entsoe"
    # B4 (N-9): FILTER_SAFE, vault-recorded real EMPTY, sister to
    # total_nominated_capacity (R3-RESEARCH.md:65); see
    # ADR-026 Appendix A.
    EVENT_WINDOW_FILTER: ClassVar[bool] = True


class NetPositionsTransformer(_H6QuantityTransformer):
    dataset = "net_positions"
    source = "entsoe"
    # B4 (N-9): FILTER_SAFE, live probe FR 2026-06-01, window matches
    # exactly (R3-RESEARCH.md:54); see
    # ADR-026 Appendix A.
    EVENT_WINDOW_FILTER: ClassVar[bool] = True


class CongestionManagementCostsTransformer(_H6AmountTransformer):
    dataset = "congestion_management_costs"
    source = "entsoe"


class AuctionRevenueTransformer(_H6AmountTransformer):
    dataset = "auction_revenue"
    source = "entsoe"


class CongestionIncomeTransformer(_H6AmountTransformer):
    dataset = "congestion_income"
    source = "entsoe"


_TRANSFORMERS = [
    DcLinkIntradayTransferLimitsTransformer,
    CommercialSchedulesTransformer,
    RedispatchingCrossBorderTransformer,
    RedispatchingInternalTransformer,
    CountertradingTransformer,
    OfferedTransferCapacityContinuousTransformer,
    OfferedTransferCapacityImplicitTransformer,
    OfferedTransferCapacityExplicitTransformer,
    TransferCapacityUseTransformer,
    TotalNominatedCapacityTransformer,
    TotalCapacityAllocatedTransformer,
    NetPositionsTransformer,
    CongestionManagementCostsTransformer,
    AuctionRevenueTransformer,
    CongestionIncomeTransformer,
]

for transformer_cls in _TRANSFORMERS:
    register_transformer("entsoe", transformer_cls.dataset, transformer_cls)
