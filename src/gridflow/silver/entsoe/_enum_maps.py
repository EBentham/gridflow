"""Shared enum mapping for ENTSO-E silver transformers.

ENTSO-E enum codes (``flow_direction``, ``business_type``) cross the API ->
silver boundary untrusted-by-value. The parser's empty-string default and
legitimate-but-unlisted codes (e.g. ``A03`` "up and down") are expected, not
garbage. Mapping such a code with ``replace_strict`` and no ``default=`` raises
``InvalidOperationError`` and zeroes the whole date (ADR-022 finding H2).

So :func:`map_enums` maps an unlisted code to ``UNMAPPED_SENTINEL``, an
explicit, recoverable label that :func:`count_unmapped` counts and reports.
See ``docs/DECISION_LOG/ADR-022-unmapped-enum-code-policy.md``.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping

import polars as pl

logger = logging.getLogger(__name__)

UNMAPPED_SENTINEL = "unmapped"
"""Sentinel label for an ENTSO-E enum code absent from a transformer's map."""

RESERVE_TYPE = {"A95": "fcr", "A96": "afrr", "A97": "mfrr", "A98": "rr"}
"""Balancing reserve product by ``businessType`` code."""

EnumMaps = Mapping[str, tuple[str, Mapping[str, str]]]
"""Label column to the raw code column it is read from and that column's code-to-label map."""


def map_enums(enums: EnumMaps) -> list[pl.Expr]:
    """Return one label column per entry; an unlisted or null code becomes the sentinel."""
    return [
        pl.col(raw)
        .replace_strict(mapping, default=UNMAPPED_SENTINEL, return_dtype=pl.Utf8)
        .alias(label)
        for label, (raw, mapping) in enums.items()
    ]


def count_unmapped(df: pl.DataFrame, raw_df: pl.DataFrame, enums: EnumMaps, *, dataset: str) -> int:
    """Count the rows of ``df`` with any sentinel label and log the raw codes behind them.

    Call it on the deduplicated output: rows are keyed on their raw codes, so
    two unknown codes stay two rows and each is counted once. Codes are listed
    as text, so a null beside a string sorts instead of raising.
    """
    unmapped = pl.any_horizontal([pl.col(label) == UNMAPPED_SENTINEL for label in enums])
    count = df.filter(unmapped).height
    if count:
        codes = {
            raw: sorted(str(c) for c in raw_df.get_column(raw).unique().to_list() if c not in m)
            for raw, m in enums.values()
        }
        logger.warning(
            "entsoe/%s: %d unmapped row(s) labelled %r; unmapped raw codes: %s",
            dataset,
            count,
            UNMAPPED_SENTINEL,
            codes,
        )
    return count


def currency_expr(df: pl.DataFrame) -> pl.Expr:
    """Return a Polars expression yielding the explicit source currency.

    ENPRICE-04 (VT4): ENTSO-E carries the price denomination in
    ``<currency_Unit.name>`` (e.g. "EUR" for continental zones, "GBP" for GB),
    parsed to a ``currency_unit`` bronze column. Price transformers store the
    value in a ``price_eur_mwh`` column whose name must not be trusted as the
    denomination; this expression surfaces the authoritative currency label.

    A non-empty ``currency_unit`` is carried through verbatim (stripped);
    otherwise the expression falls back to the literal ``"EUR"`` continental
    default. When the bronze frame has no ``currency_unit`` column at all (older
    fixtures), a constant ``"EUR"`` literal is returned. Mirrors the inline
    treatment in ``silver/entsoe/day_ahead_prices.py``.

    Args:
        df: The bronze (or partially-renamed) frame the expression will run
            against, inspected for the presence of a ``currency_unit`` column.

    Returns:
        A Polars expression evaluating to the currency string per row.
    """
    if "currency_unit" not in df.columns:
        return pl.lit("EUR")
    stripped = pl.col("currency_unit").cast(pl.Utf8).str.strip_chars()
    return pl.when(stripped != "").then(stripped).otherwise(pl.lit("EUR"))
