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

This module does not set ``available_at`` / ``ingested_at`` directly, but its
output feeds ``available_at`` indirectly: per ADR-025 Sec 3,
``BaseSilverTransformer`` row-wise coalesces
``available_at = coalesce(published_at, ingest_time)``
(``silver/base.py:1447-1471``), so once ``published_at`` is emitted here it
silently becomes ``available_at`` for every row with a non-null vintage.
Only rows with a null ``published_at`` fall back to the ingest/reingest
scalar clock. (X1-F09: corrects prior wording that claimed ``available_at``
was untouched by this helper's output.)

F-07 (2026-07-26): the nominal ``createdDateTime`` format is
``"%Y-%m-%dT%H:%M:%SZ"``, but ENTSO-E documents have carried a fractional-second
(``".000Z"``) and an explicit-offset (``"+00:00"``) variant — both unambiguously
UTC. Previously ANY variant, plus genuinely malformed values (space-separated,
missing the ``Z`` marker entirely), nulled silently with zero warnings. Now:
the two unambiguous variants are accepted via additional format attempts, and
whatever remains unparseable (raw value present, still null after all attempts)
is counted and surfaced as a single WARNING naming the dataset — never a
silent null (CLAUDE.md: validation failures are logged, counted, and
surfaced, never silently dropped; mirrors the GIE/P0.8 counted-warning
pattern in ``silver/gie/agsi.py``).
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
    dataset: str | None = None,
) -> pl.DataFrame:
    """Return ``df`` with a tz-aware UTC ``published_at`` column.

    Parses ``source_col`` (the raw ``createdDateTime`` string) into
    ``published_at`` as ``pl.Datetime("us", "UTC")``. When ``source_col`` is
    absent, or every value is empty, ``published_at`` is a typed-null column
    of the correct dtype (never dropped, never object-null).

    Accepts the nominal ``"...Z"`` form plus two unambiguously-UTC variants
    (fractional seconds, explicit ``+00:00`` offset). Any value that still
    fails to parse (space-separated, missing the timezone marker entirely,
    or otherwise malformed) is counted and surfaced via a single WARNING
    naming ``dataset`` — never silently nulled without a trace.

    Args:
        df: The transform-stage DataFrame, before column selection.
        source_col: Name of the raw vintage-string column from the parser.
        dataset: Dataset name for the parse-failure warning. Falls back to
            ``"entsoe"`` when not supplied.

    Returns:
        ``df`` with a ``published_at`` column added (or overwritten).
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
            raw.alias("_f07_raw_created_at"),
            parsed.alias("published_at"),
        ]
    )

    unparseable_mask = (
        pl.col("_f07_raw_created_at").is_not_null() & pl.col("published_at").is_null()
    )
    failure_count = int(result.select(unparseable_mask.sum()).item())
    if failure_count:
        sample = (
            result.filter(unparseable_mask).get_column("_f07_raw_created_at").unique().to_list()
        )[:_SAMPLE_LIMIT]
        logger.warning(
            "%s: %d row(s) had an unparseable createdDateTime; published_at "
            "nulled for those rows (not silently dropped) — sample raw "
            "value(s): %s",
            dataset or "entsoe",
            failure_count,
            sample,
        )

    return result.drop("_f07_raw_created_at").with_columns(
        pl.col("published_at").dt.replace_time_zone("UTC")
    )
