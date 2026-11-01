"""Parse a vendor publication time into the silver ``published_at`` column.

Shared by the Elexon and ENTSO-E transformers. The column is the vendor's own
vintage (Elexon ``publishTime``, ENTSO-E ``createdDateTime``), and
``BaseSilverTransformer`` turns it into ``available_at`` row by row (ADR-025),
so a value that silently fails to parse would move that row's availability to
the ingest clock. Hence the contract below: three unambiguous UTC forms are
accepted, anything else that is present but unparseable is counted and logged
once per frame, and a missing source column becomes a typed-null
``published_at`` so partition globs stay schema-stable.
"""

from __future__ import annotations

import logging

import polars as pl

logger = logging.getLogger(__name__)

_PUBLISHED_AT_DTYPE = pl.Datetime("us", "UTC")

# Nominal form: "2024-01-14T12:00:00Z".
_FORMAT_BASE = "%Y-%m-%dT%H:%M:%SZ"
# Fractional-second variant: "2024-01-14T12:00:00.000Z" — unambiguously UTC.
_FORMAT_FRACTIONAL = "%Y-%m-%dT%H:%M:%S%.fZ"
# Explicit-offset variant: "2024-01-14T12:00:00+00:00" — unambiguously UTC
# (Polars normalises any parsed offset to a true UTC instant).
_FORMAT_OFFSET = "%Y-%m-%dT%H:%M:%S%:z"

_SAMPLE_LIMIT = 5


def with_published_at(
    df: pl.DataFrame,
    source_col: str = "document_created_at",
    *,
    dataset: str,
) -> pl.DataFrame:
    """Return ``df`` with a tz-aware UTC ``published_at`` parsed from ``source_col``.

    Accepts ``...Z``, fractional seconds (``...000Z``) and an explicit
    ``+00:00`` offset, after stripping surrounding whitespace. Empty strings
    count as absent. Every other present value is nulled, counted and
    reported in one WARNING naming ``dataset``. The default ``source_col`` is
    the ENTSO-E parser's column.
    """
    if source_col not in df.columns:
        return df.with_columns(pl.lit(None).cast(_PUBLISHED_AT_DTYPE).alias("published_at"))

    raw = pl.col(source_col).cast(pl.Utf8).str.strip_chars().replace("", None)

    base = raw.str.to_datetime(format=_FORMAT_BASE, time_unit="us", strict=False)
    fractional = raw.str.to_datetime(format=_FORMAT_FRACTIONAL, time_unit="us", strict=False)
    # Offset-form parse yields a tz-aware (UTC-normalised) result; strip the
    # tz label so it coalesces cleanly against the naive base/fractional
    # candidates, then re-attach UTC uniformly at the end.
    offset = raw.str.to_datetime(
        format=_FORMAT_OFFSET, time_unit="us", strict=False
    ).dt.replace_time_zone(None)

    parsed = pl.coalesce([base, fractional, offset])

    result = df.with_columns(
        [
            raw.alias("_raw_published_at"),
            parsed.alias("published_at"),
        ]
    )

    unparseable_mask = pl.col("_raw_published_at").is_not_null() & pl.col("published_at").is_null()
    failure_count = int(result.select(unparseable_mask.sum()).item())
    if failure_count:
        sample = (
            result.filter(unparseable_mask).get_column("_raw_published_at").unique().to_list()
        )[:_SAMPLE_LIMIT]
        logger.warning(
            "%s: %d row(s) had an unparseable %s; published_at nulled for those "
            "rows (not silently dropped) — sample raw value(s): %s",
            dataset,
            failure_count,
            source_col,
            sample,
        )

    return result.drop("_raw_published_at").with_columns(
        pl.col("published_at").dt.replace_time_zone("UTC")
    )
