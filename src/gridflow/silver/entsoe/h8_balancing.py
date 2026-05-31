"""Silver transformers for H8 ENTSO-E balancing extension datasets."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime
from typing import Any

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.schemas.entsoe import (
    EntsoeBalancingCapacity,
    EntsoeBalancingEnergyBid,
    EntsoeBalancingFinancial,
    EntsoeBalancingState,
    EntsoeCrossZonalBalancingCapacity,
)
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.registry import register_transformer

logger = logging.getLogger(__name__)


class _H8BalancingTransformer(BaseSilverTransformer):
    """Shared transformer for H8 balancing time-series payloads."""

    value_tag = "quantity"
    value_column = "quantity_mw"
    schema_cls = EntsoeBalancingState
    area_columns = ("area_domain",)
    output_cols = (
        "timestamp_utc",
        "area_code",
        "quantity_mw",
        "business_type",
        "resolution",
        "data_provider",
        "ingested_at",
    )
    unique_subset = ("timestamp_utc", "area_code", "business_type")

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        bronze_path = self._bronze_path_for_date(target_date)
        if bronze_path is None:
            return pl.DataFrame()

        rows: list[dict[str, Any]] = []
        for xml_file in sorted(bronze_path.glob("raw_*.xml")):
            if xml_file.name.endswith(".meta.json"):
                continue
            try:
                rows.extend(
                    parse_timeseries_xml(
                        xml_file.read_bytes(),
                        value_tag=self.value_tag,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed to parse ENTSO-E XML %s: %s", xml_file, exc)

        if not rows:
            return pl.DataFrame()
        return pl.DataFrame(rows)

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        available = set(raw_df.columns)
        required = {"timestamp_utc", "value", "resolution", *self.area_columns}
        missing = required - available
        if missing:
            logger.error("Missing required columns in %s: %s", self.dataset, missing)
            return pl.DataFrame()

        df = _with_optional_columns(raw_df)
        df = self._rename_domain_columns(df)

        if "timestamp_utc" in df.columns and df["timestamp_utc"].dtype != pl.Datetime("us", "UTC"):
            df = df.with_columns(pl.col("timestamp_utc").cast(pl.Datetime("us", "UTC")))

        now = datetime.now(UTC)
        df = (
            df.with_columns(
                [
                    pl.col("value").cast(pl.Float64).alias(self.value_column),
                    pl.lit("entsoe").alias("data_provider"),
                    pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
                    pl.col("timestamp_utc").dt.replace_time_zone("UTC"),
                ]
            )
            .select(list(self.output_cols))
            .unique(subset=list(self.unique_subset), keep="last")
            .sort(list(self.unique_subset))
        )

        if not df.is_empty():
            self.schema_cls(**df.row(0, named=True))

        return df

    def _rename_domain_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        area_column = self.area_columns[0]
        return df.rename({"value": "_value"}).rename(
            {
                "_value": "value",
                area_column: "area_code",
                "flow_direction": "direction",
                "timeseries_mrid": "bid_mrid",
            }
        )


class CurrentBalancingStateTransformer(_H8BalancingTransformer):
    dataset = "current_balancing_state"
    source = "entsoe"
    schema_cls = EntsoeBalancingState
    area_columns = ("area_domain",)


class BalancingEnergyBidsTransformer(_H8BalancingTransformer):
    dataset = "balancing_energy_bids"
    source = "entsoe"
    schema_cls = EntsoeBalancingEnergyBid
    area_columns = ("connecting_domain",)
    output_cols = (
        "timestamp_utc",
        "area_code",
        "quantity_mw",
        "business_type",
        "bid_mrid",
        "direction",
        "original_market_product",
        "standard_market_product",
        "resolution",
        "data_provider",
        "ingested_at",
    )
    unique_subset = ("timestamp_utc", "area_code", "bid_mrid", "direction")


class AggregatedBalancingEnergyBidsTransformer(BalancingEnergyBidsTransformer):
    dataset = "aggregated_balancing_energy_bids"
    area_columns = ("area_domain",)


class ProcuredBalancingCapacityTransformer(_H8BalancingTransformer):
    dataset = "procured_balancing_capacity"
    source = "entsoe"
    schema_cls = EntsoeBalancingCapacity
    area_columns = ("area_domain",)
    output_cols = (
        "timestamp_utc",
        "area_code",
        "quantity_mw",
        "market_agreement_type",
        "business_type",
        "resolution",
        "data_provider",
        "ingested_at",
    )
    unique_subset = ("timestamp_utc", "area_code", "market_agreement_type")


class CrossZonalBalancingCapacityTransformer(_H8BalancingTransformer):
    dataset = "cross_zonal_balancing_capacity"
    source = "entsoe"
    schema_cls = EntsoeCrossZonalBalancingCapacity
    area_columns = ("acquiring_domain", "connecting_domain")
    output_cols = (
        "timestamp_utc",
        "acquiring_area_code",
        "connecting_area_code",
        "quantity_mw",
        "market_agreement_type",
        "business_type",
        "resolution",
        "data_provider",
        "ingested_at",
    )
    unique_subset = (
        "timestamp_utc",
        "acquiring_area_code",
        "connecting_area_code",
        "market_agreement_type",
    )

    def _rename_domain_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.rename(
            {
                "acquiring_domain": "acquiring_area_code",
                "connecting_domain": "connecting_area_code",
            }
        )


class BalancingFinancialExpensesIncomeTransformer(_H8BalancingTransformer):
    dataset = "balancing_financial_expenses_income"
    source = "entsoe"
    value_tag = "price.amount"
    value_column = "amount_eur"
    schema_cls = EntsoeBalancingFinancial
    area_columns = ("control_area_domain",)
    output_cols = (
        "timestamp_utc",
        "area_code",
        "amount_eur",
        "business_type",
        # G9 ENTSOE-02: surface per-series Reason.code (extracted by
        # parse_timeseries_xml). Empty string when source has no Reason.
        "reason_code",
        "resolution",
        "data_provider",
        "ingested_at",
    )
    unique_subset = ("timestamp_utc", "area_code", "business_type")


def _with_optional_columns(df: pl.DataFrame) -> pl.DataFrame:
    optional_cols = {
        "business_type",
        "market_agreement_type",
        "original_market_product",
        "standard_market_product",
        "timeseries_mrid",
        # G9 ENTSOE-02: ensures `reason_code` always exists as a column
        # in transformer output even when the source XML omits Reason.
        "reason_code",
    }
    missing_exprs = [pl.lit("").alias(column) for column in sorted(optional_cols - set(df.columns))]
    if missing_exprs:
        df = df.with_columns(missing_exprs)
    return df


_TRANSFORMERS = [
    CurrentBalancingStateTransformer,
    BalancingEnergyBidsTransformer,
    AggregatedBalancingEnergyBidsTransformer,
    ProcuredBalancingCapacityTransformer,
    CrossZonalBalancingCapacityTransformer,
    BalancingFinancialExpensesIncomeTransformer,
]

for transformer_cls in _TRANSFORMERS:
    register_transformer("entsoe", transformer_cls.dataset, transformer_cls)
