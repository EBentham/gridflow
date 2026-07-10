"""Latest-vintage selection for APPEND_ONLY silver datasets (ADR-025 P0.3).

APPEND_ONLY datasets store one run-suffixed parquet file per vintage, so their
base ``silver_{source}_{dataset}`` views return one row per vintage. This module
is the single home for the "current best value" selection, rendered two ways
that MUST stay semantically identical (guarded by a parity test):

- :func:`latest_view_sql` — a DuckDB ``QUALIFY ROW_NUMBER()`` view for catalogue
  consumers (``silver_{source}_{dataset}_latest``).
- :func:`select_latest_vintage` — the same selection as a Polars transform for
  Polars-native readers (the quality CLI), which must not depend on the DuckDB
  catalogue file.

Ordering is ``available_at``-primary (ADR-025: the live system_prices feed has
no run label; publication order is the only universal vintage axis), with an
optional categorical rank as the secondary tie-break. Both renderers adapt to
the columns actually present: silver written from the live DISEBSP feed has no
``run_type`` column at all, and pre-F0 legacy files union in null
``available_at`` (sorted last).

Kept dependency-light (polars + stdlib only) so ``storage.duckdb`` can import
it without dragging in the transformer stack.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import polars as pl

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LatestViewSpec:
    """Business key and vintage precedence for a latest-vintage projection.

    Attributes:
        key_columns: Entity key — the projection returns one row per key.
        order_columns: Vintage ordering, strongest first; each sorts DESC with
            nulls last. Columns missing from the relation are skipped.
        rank_column: Optional categorical column ranked via ``rank_map`` as the
            final DESC tie-break; skipped when absent from the relation.
        rank_map: Category -> rank (higher wins); unmapped/null rank as 0.
    """

    key_columns: tuple[str, ...]
    order_columns: tuple[str, ...] = ("available_at",)
    rank_column: str | None = None
    rank_map: tuple[tuple[str, int], ...] | None = None


# BSC settlement-run precedence (II < SF < R1 < R2 < R3 < RF < DF). Secondary
# tie-break only: the live feed carries no run_type, so available_at leads.
_SETTLEMENT_RUN_RANK: tuple[tuple[str, int], ...] = (
    ("II", 1),
    ("SF", 2),
    ("R1", 3),
    ("R2", 4),
    ("R3", 5),
    ("RF", 6),
    ("DF", 7),
)

LATEST_VIEW_SPECS: dict[tuple[str, str], LatestViewSpec] = {
    ("elexon", "system_prices"): LatestViewSpec(
        key_columns=("settlement_date", "settlement_period"),
        rank_column="run_type",
        rank_map=_SETTLEMENT_RUN_RANK,
    ),
    ("elexon", "remit"): LatestViewSpec(
        key_columns=("mrid",),
        order_columns=("available_at", "revision_number"),
    ),
    ("elexon", "fou2t14d"): LatestViewSpec(
        key_columns=("settlement_date", "settlement_period", "fuel_type"),
    ),
}


def _quote_identifier(name: str) -> str:
    """Quote a DuckDB identifier, doubling embedded double-quotes."""
    return '"' + name.replace('"', '""') + '"'


def _rank_case_sql(spec: LatestViewSpec) -> str:
    assert spec.rank_column is not None and spec.rank_map is not None
    whens = " ".join(
        f"WHEN '{value}' THEN {rank}" for value, rank in spec.rank_map if "'" not in value
    )
    return f"CASE {_quote_identifier(spec.rank_column)} {whens} ELSE 0 END"


def latest_view_sql(
    base_view: str,
    latest_view: str,
    spec: LatestViewSpec,
    available_columns: set[str],
) -> str | None:
    """Render the ``CREATE OR REPLACE VIEW`` SQL for a latest-vintage view.

    Args:
        base_view: Existing source-qualified silver view name.
        latest_view: Name for the latest-vintage projection.
        spec: Key and precedence definition.
        available_columns: Columns of ``base_view`` — order/rank terms are
            adapted to what exists (live-feed silver has no ``run_type``).

    Returns:
        The DDL string, or ``None`` when a key column is missing (a projection
        keyed on absent columns would be meaningless — caller logs and skips).
    """
    missing_keys = [c for c in spec.key_columns if c not in available_columns]
    if missing_keys:
        logger.warning(
            "Skipping %s: key column(s) %s absent from %s",
            latest_view,
            missing_keys,
            base_view,
        )
        return None

    order_terms = [
        f"{_quote_identifier(c)} DESC NULLS LAST"
        for c in spec.order_columns
        if c in available_columns
    ]
    if spec.rank_column is not None and spec.rank_column in available_columns:
        order_terms.append(f"{_rank_case_sql(spec)} DESC")
    if not order_terms:
        logger.warning("Skipping %s: no vintage-order column present on %s", latest_view, base_view)
        return None

    keys = ", ".join(_quote_identifier(c) for c in spec.key_columns)
    return (
        f"CREATE OR REPLACE VIEW {_quote_identifier(latest_view)} AS "
        f"SELECT * FROM {_quote_identifier(base_view)} "
        f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {', '.join(order_terms)}) = 1"
    )


def select_latest_vintage(lf: pl.LazyFrame, spec: LatestViewSpec) -> pl.LazyFrame:
    """Apply the latest-vintage selection to a Polars frame (SQL-view mirror).

    Semantically identical to the view produced by :func:`latest_view_sql`
    (parity-tested). When a key column is missing the frame is returned
    unchanged with a warning — downstream checks then see the raw vintages and
    surface the drift loudly rather than crashing the whole quality run.

    Args:
        lf: Frame carrying all vintages of one dataset.
        spec: Key and precedence definition.

    Returns:
        One row per ``spec.key_columns``, the winning vintage first by
        ``order_columns`` (DESC, nulls last) then by the optional rank.
    """
    schema_columns = set(lf.collect_schema().names())
    missing_keys = [c for c in spec.key_columns if c not in schema_columns]
    if missing_keys:
        logger.warning("Latest-vintage selection skipped: key column(s) %s absent", missing_keys)
        return lf

    sort_columns = [c for c in spec.order_columns if c in schema_columns]
    rank_alias = "_vintage_rank"
    drop_rank = False
    if spec.rank_column is not None and spec.rank_column in schema_columns:
        mapping = dict(spec.rank_map or ())
        lf = lf.with_columns(
            pl.col(spec.rank_column)
            .replace_strict(mapping, default=0, return_dtype=pl.Int32)
            .fill_null(0)
            .alias(rank_alias)
        )
        sort_columns.append(rank_alias)
        drop_rank = True
    if not sort_columns:
        logger.warning("Latest-vintage selection skipped: no vintage-order column present")
        return lf

    out = lf.sort(sort_columns, descending=True, nulls_last=True).unique(
        subset=list(spec.key_columns), keep="first", maintain_order=True
    )
    return out.drop(rank_alias) if drop_rank else out
