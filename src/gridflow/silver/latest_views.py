"""Latest-vintage selection for APPEND_ONLY silver datasets (ADR-025 P0.3).

APPEND_ONLY datasets store one run-suffixed parquet file per vintage, so their
base ``silver_{source}_{dataset}`` views return one row per vintage. This module
is the single home for the "current best value" selection, rendered two ways
(guarded by a parity test):

- :func:`latest_view_sql` — a DuckDB ``QUALIFY ROW_NUMBER()`` view for catalogue
  consumers (``silver_{source}_{dataset}_latest``).
- :func:`select_latest_vintage` — the same selection as a Polars transform for
  Polars-native readers (the quality CLI), which must not depend on the DuckDB
  catalogue file.

R1-A/F-18 correction: the two renderers make the SAME selection and the SAME
skip decision (both delegate to :func:`_resolve_selection`, so the decision is
identical by construction — not merely asserted), and diverge ONLY in how they
REACT to a skip. This is a deliberate, documented asymmetry, not a bug:

- :func:`latest_view_sql` returns ``None`` on a skip, so ``storage.duckdb``
  DROPs the ``_latest`` projection entirely — fail-closed: the DuckDB
  consumer gets no surface at all, and any gold view reading it fails to
  register (raise under strict/pytest mode, WARNING in production).
- :func:`select_latest_vintage` returns the frame UNCHANGED (all vintages) on
  a skip, with a warning — so the quality CLI's checks then see the raw
  vintages and surface the drift loudly (e.g. a false duplicate-key failure)
  rather than crashing the whole run.

A present rank column counts as a usable ordering term ON ITS OWN: a
rank-only schema (key columns + a rank column, no ``available_at`` at all)
stays a SELECTION on both renderers, not a skip — see :func:`_resolve_selection`.

Ordering is ``available_at``-primary (ADR-025: the live system_prices feed has
no run label; publication order is the only universal vintage axis), with an
optional categorical rank as the secondary tie-break. Both renderers adapt to
the columns actually present: silver written BEFORE v0.18 R1-A from the live
DISEBSP feed has no ``run_type`` column at all (the transformer now always
emits it, typed-null when the raw field is absent — F-13), so the
rank-column-absent path is legacy-file compatibility, and pre-F0 legacy files
union in null ``available_at`` (sorted last).

Kept dependency-light (polars + stdlib only) so ``storage.duckdb`` can import
it without dragging in the transformer stack.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import polars as pl

if TYPE_CHECKING:
    from datetime import datetime

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
    optional_key_columns: tuple[str, ...] = ()
    """Key refinements included only when present on the relation.

    fou2t14d's live forecastDate-only shape has no settlement_period; making it
    a required key would silently skip the whole projection (review finding,
    v0.17 PR-A). Optional keys tighten the grain when the column exists and are
    dropped when it doesn't.
    """
    mode: Literal["key_latest", "whole_capture"] = "key_latest"
    """``key_latest`` returns one row per key; ``whole_capture`` returns every row
    of the newest COMPLETE capture (ADR-034 P-10), so a valid-empty newest
    capture yields zero rows."""
    tiebreak_columns: tuple[str, ...] = ()
    """Final ``DESC NULLS LAST`` ordering terms after the rank (ADR-034 P-10).

    Unlike ``order_columns`` these are REQUIRED: a relation missing any of them
    skips the selection (the view fails closed), because a tie-break that
    silently disappears makes the winner depend on scan order. Empty for every
    pre-existing spec, whose SQL is byte-identical (T-B8-3)."""
    completion_relation: str | None = None
    """``whole_capture`` only: the relation of completion records to select from."""
    completion_family: str | None = None
    """``whole_capture`` only: the completion records' ``family`` value."""


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
        key_columns=("settlement_date", "fuel_type"),
        optional_key_columns=("settlement_period",),
    ),
    # D-21/D-24. Every NESO Data Portal capture is a whole-file snapshot, so
    # successive captures COEXIST in the base view by design (APPEND_ONLY) and
    # this projection is what returns one current row per BMU-day. The key is
    # deliberately COARSER than the transformer's ENTITY_KEY_COLUMNS, which
    # carries `published_at`: the entity key preserves every vintage, this key
    # picks the winner among them (ordered by `available_at`, which D-22 makes
    # NESO's own publication instant).
    ("neso_data_portal", "daily_wind_availability"): LatestViewSpec(
        key_columns=("bmu_id", "availability_date"),
    ),
    # D-21/D-24. The resource's own CKAN `notes` says the data "is subject to
    # change due to a data cleansing process", so two captures legitimately
    # disagree about one instant and both are retained. This projection returns
    # the most recently PUBLISHED value per instant, and it is the default
    # consumer surface for this dataset: the base view holds one full copy of
    # 2009-present per capture (D-30).
    ("neso_data_portal", "historic_generation_mix"): LatestViewSpec(
        key_columns=("timestamp_utc",),
    ),
    # D-21/D-24. A rolling forecast (NESO's package notes: within day up to 14
    # days ahead, updated hourly), so the base view holds every issued vintage
    # for a settlement period and this projection returns the current one. The
    # key is the entity key MINUS `issue_time`, which is precisely the vintage
    # axis it selects over.
    ("neso_data_portal", "embedded_wind_solar_forecast"): LatestViewSpec(
        key_columns=("settlement_date", "settlement_period"),
    ),
}


def _quote_identifier(name: str) -> str:
    """Quote a DuckDB identifier, doubling embedded double-quotes."""
    return '"' + name.replace('"', '""') + '"'


def _quote_string_literal(value: str) -> str:
    """Quote a SQL string literal, doubling embedded single-quotes."""
    return "'" + value.replace("'", "''") + "'"


def _rank_case_sql(spec: LatestViewSpec) -> str:
    assert spec.rank_column is not None and spec.rank_map is not None
    # Escape rather than drop: silently dropping an entry would diverge from the
    # Polars mirror (and an empty WHEN list is a syntax error).
    whens = " ".join(
        f"WHEN {_quote_string_literal(value)} THEN {rank}" for value, rank in spec.rank_map
    )
    return f"CASE {_quote_identifier(spec.rank_column)} {whens} ELSE 0 END"


@dataclass(frozen=True)
class _Selection:
    """The shared skip decision's resolved output — one row per renderer call.

    Carries enough render-agnostic information for EACH renderer to build its
    own concrete ordering (SQL ``CASE...WHEN`` vs Polars ``replace_strict``);
    the renderers still differ in HOW they render, only the decision of WHAT
    to select is now made once, by :func:`_resolve_selection`.
    """

    key_columns: tuple[str, ...]
    order_columns: tuple[str, ...]
    has_rank: bool
    tiebreak_columns: tuple[str, ...] = ()


_WHOLE_CAPTURE_ORDER: tuple[str, ...] = ("available_at", "capture_written_at", "bronze_capture_id")
"""The completion records' winner order for ``whole_capture`` (all DESC NULLS LAST)."""

_AS_OF_SQL = "CAST($as_of AS TIMESTAMPTZ)"
"""The as-of bound: an ISO string parameter cast in SQL (no ``pytz`` needed, C-7)."""


def _resolve_selection(spec: LatestViewSpec, available_columns: set[str]) -> _Selection | None:
    """Resolve the shared latest-vintage skip decision (R1-A/F-18).

    Both :func:`latest_view_sql` and :func:`select_latest_vintage` call this,
    so the *decision* (skip vs. select, and on what) is identical by
    construction; only the *reaction* to a skip differs between them (see the
    module docstring). This function does no logging — callers log their own
    skip warning, since their messages carry different context (view names
    vs. bare frame semantics) and existing tests pin those exact messages.

    Skip (return ``None``) only when:
      (a) a required key column (``spec.key_columns``) is missing, or
      (b) NEITHER any ``order_columns`` member NOR ``rank_column`` is present, or
      (c) any ``tiebreak_columns`` member is missing (ADR-034 P-10), or
      (d) a ``whole_capture`` spec's relation has no ``bronze_capture_id`` or the
          spec names no completion relation/family.

    A present rank column counts as a usable ordering term ON ITS OWN (Sol
    finding 6): a rank-only schema — e.g.
    ``{settlement_date, settlement_period, run_type}`` with no
    ``available_at`` at all — must stay a SELECTION (ordered by rank alone),
    not a skip/drop. Both renderers already behaved this way before this
    refactor (the rank term was appended before either renderer's own
    "no order column" check); this function is what makes that behaviour
    provably shared rather than independently-duplicated.

    Args:
        spec: Key and precedence definition.
        available_columns: Columns actually present on the relation/frame.

    Returns:
        A :class:`_Selection` with the resolved key columns, the
        ``order_columns`` members present (in spec order), and whether the
        rank column is present and usable — or ``None`` when the selection is
        impossible.
    """
    if spec.mode == "whole_capture":
        if (
            "bronze_capture_id" not in available_columns
            or spec.completion_relation is None
            or spec.completion_family is None
        ):
            return None
        return _Selection(key_columns=(), order_columns=(), has_rank=False)

    missing_keys = [c for c in spec.key_columns if c not in available_columns]
    if missing_keys:
        return None

    order_columns = tuple(c for c in spec.order_columns if c in available_columns)
    has_rank = spec.rank_column is not None and spec.rank_column in available_columns
    if not order_columns and not has_rank:
        return None
    if any(c not in available_columns for c in spec.tiebreak_columns):
        return None

    key_columns = tuple(spec.key_columns) + tuple(
        c for c in spec.optional_key_columns if c in available_columns
    )
    return _Selection(
        key_columns=key_columns,
        order_columns=order_columns,
        has_rank=has_rank,
        tiebreak_columns=spec.tiebreak_columns,
    )


def latest_select_sql(
    base_view: str,
    spec: LatestViewSpec,
    available_columns: set[str],
    *,
    as_of_param: bool,
) -> str | None:
    """Render the latest-vintage ``SELECT`` over ``base_view`` (ADR-034 P-10).

    With ``as_of_param=True`` the statement takes one named parameter,
    ``$as_of`` (an ISO-8601 string), and applies ``available_at <= as_of``
    BEFORE selection: a ``WHERE`` ahead of ``QUALIFY`` for ``key_latest``,
    inside the eligible-completions filter for ``whole_capture``. No
    registered view carries the parameter (:func:`latest_view_sql` renders
    with ``as_of_param=False``).

    Args:
        base_view: Existing source-qualified silver view name.
        spec: Key and precedence definition.
        available_columns: Columns of ``base_view``.
        as_of_param: Emit the ``$as_of`` bound.

    Returns:
        The ``SELECT`` text, or ``None`` on the shared skip decision.
    """
    selection = _resolve_selection(spec, available_columns)
    if selection is None:
        return None
    if as_of_param and spec.mode == "key_latest" and "available_at" not in available_columns:
        return None
    base = _quote_identifier(base_view)
    if spec.mode == "whole_capture":
        assert spec.completion_relation is not None and spec.completion_family is not None
        capture = _quote_identifier("bronze_capture_id")
        bound = f" AND c.{_quote_identifier('available_at')} <= {_AS_OF_SQL}" if as_of_param else ""
        order = ", ".join(f"c.{_quote_identifier(c)} DESC NULLS LAST" for c in _WHOLE_CAPTURE_ORDER)
        return (
            f"SELECT * FROM {base} WHERE {capture} IN ("
            f"SELECT c.{capture} FROM {_quote_identifier(spec.completion_relation)} AS c "
            f"LEFT JOIN (SELECT {capture}, COUNT(*) AS n FROM {base} GROUP BY {capture}) AS b "
            f"ON b.{capture} = c.{capture} "
            f"WHERE c.{_quote_identifier('family')} = "
            f"{_quote_string_literal(spec.completion_family)} AND ("
            f"(c.{_quote_identifier('outcome')} = 'valid_empty' "
            f"AND c.{_quote_identifier('row_count')} = 0) OR "
            f"(c.{_quote_identifier('outcome')} = 'populated' "
            f"AND b.n = c.{_quote_identifier('row_count')})){bound} "
            f"ORDER BY {order} LIMIT 1)"
        )

    order_terms = [f"{_quote_identifier(c)} DESC NULLS LAST" for c in selection.order_columns]
    if selection.has_rank:
        order_terms.append(f"{_rank_case_sql(spec)} DESC")
    order_terms.extend(
        f"{_quote_identifier(c)} DESC NULLS LAST" for c in selection.tiebreak_columns
    )
    keys = ", ".join(_quote_identifier(c) for c in selection.key_columns)
    where = f" WHERE {_quote_identifier('available_at')} <= {_AS_OF_SQL}" if as_of_param else ""
    return (
        f"SELECT * FROM {base}{where} "
        f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {keys} ORDER BY {', '.join(order_terms)}) = 1"
    )


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
        The DDL string, or ``None`` when the shared skip decision
        (:func:`_resolve_selection`) says the projection cannot be built (a
        missing key column, or no usable order/rank column at all) — caller
        logs and skips; ``storage.duckdb`` DROPs the view fail-closed.
    """
    select = latest_select_sql(base_view, spec, available_columns, as_of_param=False)
    if select is None:
        # The decision itself came from _resolve_selection; this recomputation
        # is ONLY to pick which warning message to log — the missing-key and
        # no-order-column messages carry different context and existing tests
        # pin their exact text.
        missing_keys = [c for c in spec.key_columns if c not in available_columns]
        missing_ties = [c for c in spec.tiebreak_columns if c not in available_columns]
        if spec.mode == "whole_capture":
            logger.warning(
                "Skipping %s: whole-capture selection needs bronze_capture_id on %s and a "
                "completion relation",
                latest_view,
                base_view,
            )
        elif missing_keys:
            logger.warning(
                "Skipping %s: key column(s) %s absent from %s",
                latest_view,
                missing_keys,
                base_view,
            )
        elif missing_ties:
            logger.warning(
                "Skipping %s: tie-break column(s) %s absent from %s",
                latest_view,
                missing_ties,
                base_view,
            )
        else:
            logger.warning(
                "Skipping %s: no vintage-order column present on %s", latest_view, base_view
            )
        return None
    return f"CREATE OR REPLACE VIEW {_quote_identifier(latest_view)} AS {select}"


def select_latest_vintage(
    lf: pl.LazyFrame,
    spec: LatestViewSpec,
    as_of: datetime | None = None,
    *,
    completions: pl.LazyFrame | None = None,
) -> pl.LazyFrame:
    """Apply the latest-vintage selection to a Polars frame (SQL-view mirror).

    Makes the SAME selection and the SAME skip decision as
    :func:`latest_view_sql` / :func:`latest_select_sql` (all delegate to
    :func:`_resolve_selection`, parity-tested) — they diverge only in how they
    REACT to a skip: this function returns the frame unchanged (all vintages)
    with a warning, rather than dropping anything, so downstream checks (the
    quality CLI) then see the raw vintages and surface the drift loudly rather
    than crashing the whole run.

    Args:
        lf: Frame carrying all vintages of one dataset.
        spec: Key and precedence definition.
        as_of: When set, only rows (and, for ``whole_capture``, completion
            records) with ``available_at <= as_of`` take part, applied BEFORE
            selection (ADR-034 P-10, RULINGS 466).
        completions: The completion records (``whole_capture`` only).

    Returns:
        ``key_latest``: one row per ``spec.key_columns``, the winning vintage
        first by ``order_columns`` (DESC, nulls last), then the optional rank,
        then ``tiebreak_columns``. ``whole_capture``: every row of the newest
        complete capture, or none.

    Raises:
        ValueError: A ``whole_capture`` spec without ``completions``.
    """
    if spec.mode == "whole_capture" and completions is None:
        raise ValueError("a whole_capture selection needs the completion records")
    schema_columns = set(lf.collect_schema().names())
    selection = _resolve_selection(spec, schema_columns)
    if (
        selection is not None
        and as_of is not None
        and spec.mode == "key_latest"
        and "available_at" not in schema_columns
    ):
        selection = None
    if selection is None:
        # The decision itself came from _resolve_selection (called once,
        # above); this recomputation is ONLY to pick which warning message to
        # log — existing tests pin the missing-key message's exact text.
        missing_keys = [c for c in spec.key_columns if c not in schema_columns]
        if missing_keys:
            logger.warning(
                "Latest-vintage selection skipped: key column(s) %s absent", missing_keys
            )
        else:
            logger.warning("Latest-vintage selection skipped: no vintage-order column present")
        return lf

    if spec.mode == "whole_capture":
        assert completions is not None
        return _whole_capture(lf, spec, as_of, completions)

    if as_of is not None:
        lf = lf.filter(pl.col("available_at") <= as_of)
    sort_columns = list(selection.order_columns)
    rank_alias = "_vintage_rank"
    drop_rank = False
    if selection.has_rank:
        assert spec.rank_column is not None  # has_rank implies this (_resolve_selection)
        mapping = dict(spec.rank_map or ())
        lf = lf.with_columns(
            pl.col(spec.rank_column)
            .replace_strict(mapping, default=0, return_dtype=pl.Int32)
            .fill_null(0)
            .alias(rank_alias)
        )
        sort_columns.append(rank_alias)
        drop_rank = True
    sort_columns.extend(selection.tiebreak_columns)

    key_columns = list(selection.key_columns)
    out = lf.sort(sort_columns, descending=True, nulls_last=True).unique(
        subset=key_columns, keep="first", maintain_order=True
    )
    return out.drop(rank_alias) if drop_rank else out


def _whole_capture(
    lf: pl.LazyFrame,
    spec: LatestViewSpec,
    as_of: datetime | None,
    completions: pl.LazyFrame,
) -> pl.LazyFrame:
    """The Polars mirror of :func:`latest_select_sql`'s whole-capture branch."""
    counts = lf.group_by("bronze_capture_id").agg(pl.len().alias("__n"))
    eligible = (
        completions.filter(pl.col("family") == spec.completion_family)
        .join(counts, on="bronze_capture_id", how="left")
        .filter(
            ((pl.col("outcome") == "valid_empty") & (pl.col("row_count") == 0))
            | ((pl.col("outcome") == "populated") & (pl.col("__n") == pl.col("row_count")))
        )
    )
    if as_of is not None:
        eligible = eligible.filter(pl.col("available_at") <= as_of)
    winner = (
        eligible.sort(list(_WHOLE_CAPTURE_ORDER), descending=True, nulls_last=True)
        .head(1)
        .select("bronze_capture_id")
    )
    return lf.join(winner, on="bronze_capture_id", how="semi")
