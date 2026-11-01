"""The bronze body parser and the declarative transform ENTSO-E XML transformers share."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar

import polars as pl

from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from gridflow.silver.base import BaseSilverTransformer
from gridflow.silver.published_at import with_published_at

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

_UTC_INSTANT = pl.Datetime("us", "UTC")


class EntsoeXmlTransformer(BaseSilverTransformer):
    """An ENTSO-E transformer: each body is one XML market document.

    A dataset whose silver rows are the parser's points renamed, typed and
    keyed declares the spec below and inherits :meth:`transform`, adding any
    derived column in :meth:`_derive`. One that reshapes its rows overrides
    ``transform`` instead.
    """

    BRONZE_BODY_GLOB: ClassVar[str] = "raw_*.xml"
    value_tag: ClassVar[str] = "quantity"
    """The ``<Point>`` child holding the value (``"quantity"`` or ``"price.amount"``)."""

    COLUMN_MAP: ClassVar[dict[str, str]] = {}
    """Parser column to silver column, e.g. ``{"value": "load_mw", "in_domain": "area_code"}``."""
    REQUIRED: ClassVar[tuple[str, ...]] = ()
    """Silver columns a frame must have after the rename, or it transforms to nothing."""
    CASTS: ClassVar[dict[str, pl.DataType]] = {}
    """Silver column to dtype, cast when present. ``timestamp_utc`` is always UTC."""
    FILL_NULL: ClassVar[dict[str, str]] = {}
    """Silver column to the text a null becomes, when present."""
    OUTPUT_COLS: ClassVar[tuple[str, ...]] = ()
    """The silver columns in order, before ``data_provider`` and ``ingested_at``.
    Those outside ``REQUIRED`` appear only when present. Listing ``published_at``
    parses it from the document's ``createdDateTime``, or emits a typed null."""
    SORT: ClassVar[tuple[str, ...]] = ("timestamp_utc", "area_code")

    def _parse_bronze_body(self, path: Path) -> list[dict[str, Any]]:
        return parse_timeseries_xml(path.read_bytes(), value_tag=self.value_tag)

    def _derive(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add a dataset's derived columns, or drop its unusable rows, before the dedup."""
        return df

    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        if raw_df.is_empty():
            return pl.DataFrame()

        df = raw_df.rename({k: v for k, v in self.COLUMN_MAP.items() if k in raw_df.columns})
        missing = [c for c in self.REQUIRED if c not in df.columns]
        if missing:
            logger.error("Missing required columns in %s: %s", self.dataset, missing)
            return pl.DataFrame()

        df = df.with_columns(
            pl.col("timestamp_utc").cast(_UTC_INSTANT),
            *[pl.col(c).cast(t) for c, t in self.CASTS.items() if c in df.columns],
            *[pl.col(c).fill_null(v) for c, v in self.FILL_NULL.items() if c in df.columns],
        )
        df = self._derive(df)
        if "published_at" in self.OUTPUT_COLS:
            df = with_published_at(df, dataset=self.dataset)

        df = self.dedup_to_grain(df).with_columns(
            pl.lit(self.source).alias("data_provider"),
            pl.lit(datetime.now(UTC)).cast(_UTC_INSTANT).alias("ingested_at"),
        )
        columns = [c for c in self.OUTPUT_COLS if c in df.columns]
        return df.select(*columns, "data_provider", "ingested_at").sort(list(self.SORT))
