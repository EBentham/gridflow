"""Shared helper: derive `published_at` from the ENTSO-E document vintage.

Issue 04 (code-review-2026-05): the ENTSO-E XML parser now carries the
document-level ``<createdDateTime>`` (the vendor's genuine, leak-proof
forecast issue time / publication vintage) as the raw string column
``document_created_at``. Forecast silver transformers consume that as a
``published_at`` column so point-in-time / as-of joins in ``gridflow_models``
key on the true publication time rather than the ingest clock.

This mirrors the established Elexon ``published_at`` pattern
(``silver/elexon/indo.py``), including the typed-null contract: when the source
document lacks ``createdDateTime`` the column is emitted as a typed-null
``pl.Datetime("us", "UTC")`` rather than dropped, so a ``SELECT *`` partition
glob spanning vintage-present and vintage-absent files stays schema-stable.

``available_at`` / ``ingested_at`` are NOT touched here — they remain the
ingest-side clocks stamped by ``BaseSilverTransformer``.
"""

from __future__ import annotations

import polars as pl

_PUBLISHED_AT_DTYPE = pl.Datetime("us", "UTC")


def with_published_at(
    df: pl.DataFrame,
    source_col: str = "document_created_at",
) -> pl.DataFrame:
    """Return ``df`` with a tz-aware UTC ``published_at`` column.

    Parses ``source_col`` (the raw ``createdDateTime`` string) into
    ``published_at`` as ``pl.Datetime("us", "UTC")``. When ``source_col`` is
    absent, or every value is empty/unparseable, ``published_at`` is a
    typed-null column of the correct dtype (never dropped, never object-null).

    Args:
        df: The transform-stage DataFrame, before column selection.
        source_col: Name of the raw vintage-string column from the parser.

    Returns:
        ``df`` with a ``published_at`` column added (or overwritten).
    """
    if source_col not in df.columns:
        return df.with_columns(
            pl.lit(None).cast(_PUBLISHED_AT_DTYPE).alias("published_at")
        )

    # ENTSO-E createdDateTime is ISO-8601 with a trailing 'Z'
    # (e.g. "2024-01-14T12:00:00Z"). Empty strings -> typed null via strict=False.
    return df.with_columns(
        pl.col(source_col)
        .cast(pl.Utf8)
        .str.strip_chars()
        .replace("", None)
        .str.to_datetime(
            format="%Y-%m-%dT%H:%M:%SZ",
            time_unit="us",
            strict=False,
        )
        .dt.replace_time_zone("UTC")
        .alias("published_at")
    )
