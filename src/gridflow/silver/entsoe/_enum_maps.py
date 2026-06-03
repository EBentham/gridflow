"""Shared enum-mapping constants for ENTSO-E silver transformers.

ENTSO-E enum codes (``flow_direction``, ``business_type``) cross the API ->
silver boundary untrusted-by-value. The parser's empty-string default and
legitimate-but-unlisted codes (e.g. ``A03`` "up and down") are expected, not
garbage. Mapping such a code with ``replace_strict`` and no ``default=`` raises
``InvalidOperationError`` and zeroes the whole date (ADR-022 finding H2).

Every ENTSO-E ``replace_strict`` enum site supplies ``default=UNMAPPED_SENTINEL``
so an unmapped code maps to an explicit, recoverable, counted label instead of
crashing the transform. See ``docs/DECISION_LOG/ADR-022-unmapped-enum-code-policy.md``.
"""

from __future__ import annotations

import polars as pl

UNMAPPED_SENTINEL = "unmapped"
"""Sentinel label for an ENTSO-E enum code absent from a transformer's map."""


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
