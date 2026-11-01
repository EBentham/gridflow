"""Silver transformers for H8 ENTSO-E balancing extension datasets."""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime, timedelta
from typing import TYPE_CHECKING, ClassVar

import polars as pl

from gridflow.schemas.entsoe import (
    EntsoeBalancingCapacity,
    EntsoeBalancingEnergyBid,
    EntsoeBalancingFinancial,
    EntsoeBalancingState,
    EntsoeCrossZonalBalancingCapacity,
)
from gridflow.silver.entsoe._base import EntsoeXmlTransformer
from gridflow.silver.published_at import with_published_at
from gridflow.silver.registry import register_transformer

if TYPE_CHECKING:
    from pathlib import Path

    from pydantic import BaseModel

logger = logging.getLogger(__name__)


class _H8BalancingTransformer(EntsoeXmlTransformer):
    """Shared transformer for H8 balancing time-series payloads."""

    value_tag = "quantity"
    value_column = "quantity_mw"
    #: Where the price that rides beside a quantity in each Point lands.
    price_column = "price_amount"
    #: Name of the series mRID column (the bid identity in A37).
    series_column = "series_mrid"
    schema_cls: ClassVar[type[BaseModel]] = EntsoeBalancingState
    area_columns: ClassVar[tuple[str, ...]] = ("area_domain",)
    output_cols: ClassVar[tuple[str, ...]] = (
        "timestamp_utc",
        "area_code",
        "quantity_mw",
        "business_type",
        "resolution",
        "published_at",
        "data_provider",
        "ingested_at",
    )
    ENTITY_KEY_COLUMNS: ClassVar[tuple[str, ...]] = ("timestamp_utc", "area_code", "business_type")
    #: Read only the latest fetch of each distinct request. ENTSO-E stamps a
    #: fresh document mRID on every reply and A15 restarts its series mRIDs at
    #: 1 on every page, so neither identifies a row across pages or re-fetches;
    #: the request (zone, offset, ...) does.
    LATEST_FETCH_PER_REQUEST: ClassVar[bool] = False

    def _bronze_candidates(self, target_date: date) -> list[Path]:
        candidates = super()._bronze_candidates(target_date)
        return _latest_fetch_files(candidates) if self.LATEST_FETCH_PER_REQUEST else candidates

    def read_bronze(self, target_date: date) -> pl.DataFrame:
        # Full-length schema inference, unlike the base default: a field can
        # first appear past the first 100 rows.
        pairs = self._read_bronze_records(self._bronze_read_set(target_date), target_date)
        rows = [record for _, records in pairs for record in records]
        return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()

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

        # ADR-025 P1.1: carry the document publication vintage (createdDateTime) as
        # published_at BEFORE the output_cols select (this family selects then dedups);
        # typed-null when the source lacks it.
        df = with_published_at(df, dataset=self.dataset)

        now = datetime.now(UTC)
        df = (
            df.with_columns(
                [
                    pl.col("value").cast(pl.Float64).alias(self.value_column),
                    pl.col("price_amount").cast(pl.Float64).alias(self.price_column),
                    pl.lit("entsoe").alias("data_provider"),
                    pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
                    pl.col("timestamp_utc").dt.replace_time_zone("UTC"),
                ]
            )
            .select(list(self.output_cols))
            .pipe(self.dedup_to_grain)
            .sort(list(self.ENTITY_KEY_COLUMNS))
        )

        return df

    def _rename_domain_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        area_column = self.area_columns[0]
        return df.rename({"value": "_value"}).rename(
            {
                "_value": "value",
                area_column: "area_code",
                "flow_direction": "direction",
                "timeseries_mrid": self.series_column,
                "production_type": "psr_type",
            }
        )


class CurrentBalancingStateTransformer(_H8BalancingTransformer):
    """A86/B33 current balancing state: one row per minute per area and direction.

    The documents are A03 PT1M curves; each series covers a block with one
    direction (A01 up, A02 down), so the forward-filled minutes keep it.
    """

    dataset = "current_balancing_state"
    source = "entsoe"
    schema_cls = EntsoeBalancingState
    area_columns = ("area_domain",)
    DATASET_VERSION: ClassVar[str] = "2.0.0"
    LATEST_FETCH_PER_REQUEST: ClassVar[bool] = True
    output_cols = (
        "timestamp_utc",
        "area_code",
        "quantity_mw",
        "direction",
        "business_type",
        "resolution",
        "published_at",
        "data_provider",
        "ingested_at",
    )
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code", "business_type", "direction")


class BalancingEnergyBidsTransformer(_H8BalancingTransformer):
    dataset = "balancing_energy_bids"
    source = "entsoe"
    schema_cls = EntsoeBalancingEnergyBid
    area_columns = ("connecting_domain",)
    price_column = "energy_price"
    series_column = "bid_mrid"
    DATASET_VERSION: ClassVar[str] = "2.0.0"
    LATEST_FETCH_PER_REQUEST: ClassVar[bool] = True
    output_cols: ClassVar[tuple[str, ...]] = (
        "timestamp_utc",
        "area_code",
        "quantity_mw",
        "energy_price",
        "currency_unit",
        "business_type",
        "bid_mrid",
        "direction",
        "original_market_product",
        "standard_market_product",
        "resolution",
        "published_at",
        "data_provider",
        "ingested_at",
    )
    # One bid mRID can carry several standard products at different prices
    # (BE, 2026-08-01: 326 such groups), so the product is part of the key.
    ENTITY_KEY_COLUMNS: ClassVar[tuple[str, ...]] = (
        "timestamp_utc",
        "area_code",
        "bid_mrid",
        "direction",
        "standard_market_product",
        "original_market_product",
    )
    # B4 (N-9): FILTER_SAFE, live probe DE-LU 2026-06-01, lower edge
    # coincides exactly with the request start (R3-RESEARCH.md:56, B-7);
    # see ADR-026 Appendix A.
    EVENT_WINDOW_FILTER: ClassVar[bool] = True


class AggregatedBalancingEnergyBidsTransformer(BalancingEnergyBidsTransformer):
    dataset = "aggregated_balancing_energy_bids"
    area_columns = ("area_domain",)
    series_column = "bid_mrid"
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
        "published_at",
        "data_provider",
        "ingested_at",
    )
    # C-5/I-6: this class subclasses BalancingEnergyBidsTransformer (now
    # EVENT_WINDOW_FILTER=True) and would silently inherit that True
    # without this explicit override. aggregated_balancing_energy_bids is
    # UNKNOWN (B4/N-9, R3-RESEARCH.md:75) -- never observed populated,
    # fixture-only prior evidence not trusted alone. Un-inherit the
    # parent's opt-in explicitly (M-6).
    EVENT_WINDOW_FILTER: ClassVar[bool] = False


class ProcuredBalancingCapacityTransformer(_H8BalancingTransformer):
    dataset = "procured_balancing_capacity"
    source = "entsoe"
    schema_cls = EntsoeBalancingCapacity
    area_columns = ("area_domain",)
    price_column = "procurement_price"
    DATASET_VERSION: ClassVar[str] = "2.0.0"
    LATEST_FETCH_PER_REQUEST: ClassVar[bool] = True
    output_cols = (
        "timestamp_utc",
        "area_code",
        "quantity_mw",
        "procurement_price",
        "currency_unit",
        "market_agreement_type",
        "direction",
        "standard_market_product",
        "original_market_product",
        "psr_type",
        "business_type",
        "document_mrid",
        "series_mrid",
        "resolution",
        "published_at",
        "data_provider",
        "ingested_at",
    )
    # Each series is one procured offer: several share every descriptive
    # attribute at the same time with different quantities and prices
    # (2026-08-01: 496 such groups), so a row is one series of one reply.
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code", "document_mrid", "series_mrid")
    # B4 (N-9): FILTER_SAFE, live probe DE-LU 2026-06-01, CET-boundary
    # over-span, same shape as the already-proven A44 case (R3-RESEARCH.md:55,
    # B-7); see ADR-026 Appendix A.
    EVENT_WINDOW_FILTER: ClassVar[bool] = True


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
        "published_at",
        "data_provider",
        "ingested_at",
    )
    ENTITY_KEY_COLUMNS = (
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
        "published_at",
        "data_provider",
        "ingested_at",
    )
    ENTITY_KEY_COLUMNS = ("timestamp_utc", "area_code", "business_type")


def _with_optional_columns(df: pl.DataFrame) -> pl.DataFrame:
    optional_cols = {
        "business_type",
        "market_agreement_type",
        "original_market_product",
        "standard_market_product",
        "timeseries_mrid",
        "flow_direction",
        "production_type",
        "currency_unit",
        "document_mrid",
        # G9 ENTSOE-02: ensures `reason_code` always exists as a column
        # in transformer output even when the source XML omits Reason.
        "reason_code",
    }
    missing_exprs = [pl.lit("").alias(column) for column in sorted(optional_cols - set(df.columns))]
    if "price_amount" not in df.columns:
        missing_exprs.append(pl.lit(None).cast(pl.Float64).alias("price_amount"))
    if missing_exprs:
        df = df.with_columns(missing_exprs)
    return df


#: Pages of one paged request are fetched seconds apart (a few minutes for
#: the hundreds of FR A37 pages); re-fetches of a request are hours or days
#: apart. Fetches of one request closer than this belong to one run.
_SAME_RUN_GAP = timedelta(minutes=10)

#: Sidecar request params that do not identify the request: the token, the
#: ZIP entry (one reply, several bodies) and the page offset (one paged fetch).
_NOT_REQUEST_IDENTITY = frozenset({"securityToken", "zip_entry", "offset"})


def _latest_fetch_files(xml_files: list[Path]) -> list[Path]:
    """Keep, for each distinct request, only the bodies of its latest fetch run.

    The request is the sidecar's ``request_params`` without
    ``_NOT_REQUEST_IDENTITY``, so every page of a paged request is one fetch.
    A request's fetch times split into runs wherever consecutive fetches are
    more than ``_SAME_RUN_GAP`` apart, and only the latest run is kept: a
    re-fetch that returns fewer pages drops all of the older fetch, trailing
    pages included. Two runs closer than the gap would both be kept. A body
    whose sidecar is missing, unreadable, has no request params or a naive
    ``fetched_at`` is kept and logged.

    Args:
        xml_files: Raw bodies of one bronze partition.

    Returns:
        The kept bodies, in their original order.
    """
    fetch_of: dict[Path, tuple[str, datetime]] = {}
    times_by_request: dict[str, list[datetime]] = {}
    for xml_file in xml_files:
        meta_path = xml_file.with_name(xml_file.name.removesuffix(".xml") + ".meta.json")
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            params = meta["request_params"]
            fetched_at = datetime.fromisoformat(str(meta["fetched_at"]))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            logger.warning("Keeping %s without a usable sidecar: %s", xml_file.name, exc)
            continue
        if not isinstance(params, dict) or not params or fetched_at.tzinfo is None:
            logger.warning(
                "Keeping %s: sidecar has no request params or a naive fetched_at",
                xml_file.name,
            )
            continue
        request = json.dumps(
            {k: v for k, v in params.items() if k not in _NOT_REQUEST_IDENTITY},
            sort_keys=True,
            default=str,
        )
        fetch_of[xml_file] = (request, fetched_at)
        times_by_request.setdefault(request, []).append(fetched_at)

    run_start: dict[str, datetime] = {}
    for request, times in times_by_request.items():
        times.sort()
        start = times[-1]
        for earlier in reversed(times[:-1]):
            if start - earlier > _SAME_RUN_GAP:
                break
            start = earlier
        run_start[request] = start
    return [
        f for f in xml_files if f not in fetch_of or fetch_of[f][1] >= run_start[fetch_of[f][0]]
    ]


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
