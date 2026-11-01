"""The bronze body parser and the declarative transform Elexon Insights transformers share."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar

import polars as pl

from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.published_at import with_published_at
from gridflow.utils.time import settlement_period_to_utc_expr

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

_UTC_INSTANT = pl.Datetime("us", "UTC")


def _cast(column: str, dtype: pl.DataType) -> pl.Expr:
    if dtype == _UTC_INSTANT:
        # Elexon sends instants as "2024-01-15T00:30:00Z" text.
        return (
            pl.col(column)
            .str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us", strict=False)
            .dt.replace_time_zone("UTC")
        )
    return pl.col(column).cast(dtype)


class ElexonTransformer(BaseSilverTransformer):
    """An Elexon transformer: each body is ``{"data": [...]}`` or a bare list.

    A dataset whose silver rows are its bronze rows renamed, typed and keyed
    declares the spec below and inherits :meth:`transform`. One that reshapes
    its rows overrides ``transform`` instead.
    """

    COLUMN_MAP: ClassVar[dict[str, str]] = {}
    """Vendor field to silver column; fields a frame lacks are skipped."""
    REQUIRED: ClassVar[tuple[str, ...]] = ()
    """Silver columns a frame must have after the rename, or it transforms to nothing."""
    CASTS: ClassVar[dict[str, pl.DataType]] = {}
    """Silver column to dtype, cast when present. A ``Datetime("us", "UTC")``
    target parses the vendor's ``...Z`` text. The settlement columns are cast
    to ``Date`` and ``Int32`` without being listed."""
    SETTLEMENT_DATE: ClassVar[str | None] = "settlement_date"
    """The UK settlement day ``timestamp_utc`` is stamped from: the start of the
    row's ``settlement_period`` when ``COLUMN_MAP`` maps one and the frame has
    it, else of period 1. ``None`` when bronze carries the instant itself."""
    OUTPUT_COLS: ClassVar[tuple[str, ...]] = ()
    """The silver columns in order, before ``data_provider`` and ``ingested_at``.
    Those outside ``REQUIRED`` and ``timestamp_utc`` appear only when bronze
    carried them. Listing ``published_at`` parses it from the renamed
    ``publishTime``, or emits a typed null when bronze has none."""
    SORT: ClassVar[tuple[str, ...]] = ("timestamp_utc",)

    def _parse_bronze_body(self, path: Path) -> list[dict[str, Any]]:
        # Bytes, not read_text(): json decodes UTF-8 itself, so silver no longer
        # depends on the machine's default encoding (cp1252 turned ’ into â€™).
        data = json.loads(path.read_bytes())
        records = data.get("data") if isinstance(data, dict) else data
        if not isinstance(records, list):
            # A dict without a "data" list is an error reply, not empty data.
            keys = f" with keys {sorted(data)}" if isinstance(data, dict) else ""
            raise ValueError(
                f"expected a list or {{'data': [...]}}, got {type(data).__name__}{keys}"
            )
        return records

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        df = raw_df.rename({k: v for k, v in self.COLUMN_MAP.items() if k in raw_df.columns})
        missing = [c for c in self.REQUIRED if c not in df.columns]
        if missing:
            logger.error("Missing required columns in %s: %s", self.dataset, missing)
            return pl.DataFrame()

        # Only a period the spec maps counts: a daily dataset stays on period 1
        # whatever columns its frame happens to carry.
        has_period = "settlement_period" in self.COLUMN_MAP.values()
        casts = dict(self.CASTS)
        if self.SETTLEMENT_DATE is not None:
            casts[self.SETTLEMENT_DATE] = pl.Date()
            if has_period:
                casts["settlement_period"] = pl.Int32()
        df = df.with_columns([_cast(c, t) for c, t in casts.items() if c in df.columns])

        if self.SETTLEMENT_DATE is not None:
            period = "settlement_period" if has_period and "settlement_period" in df.columns else 1
            df = df.with_columns(
                settlement_period_to_utc_expr(self.SETTLEMENT_DATE, period).alias("timestamp_utc")
            )
        if "published_at" in self.OUTPUT_COLS:
            df = with_published_at(df, "published_at", dataset=self.dataset)

        df = self.dedup_to_grain(df).with_columns(
            pl.lit(self.source).alias("data_provider"),
            pl.lit(datetime.now(UTC)).cast(_UTC_INSTANT).alias("ingested_at"),
        )
        columns = [c for c in self.OUTPUT_COLS if c in df.columns]
        return df.select(*columns, "data_provider", "ingested_at").sort(list(self.SORT))
