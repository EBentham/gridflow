"""Rows gridflow observed before their target start: per-row known-at (ADR-044).

Silver keeps every capture of a family, each row stamped with its capture's
``bronze_capture_id`` and ``capture_written_at``. This module derives, as pure
Polars over that all-vintage base and the completion ledger, when gridflow first
saw each row version and whether that was before the row's target started:

- a **row version** is the capture's resource (from the completion ledger) plus
  every non-lineage column, lineage being the record
  :data:`~gridflow.connectors.neso_data_portal.registry.record.RESERVED` names.
  Nulls compare equal;
- ``first_seen_at`` is the earliest ``capture_written_at`` among the
  **complete** captures holding the version
  (:func:`~gridflow.silver.latest_views.complete_captures`, the ``_latest``
  completeness rule);
- a version is **observed before its target** when ``timestamp_utc >
  first_seen_at``, strictly.

The guarantee is that the record content was observed by gridflow before its
target start, so it cannot be a hindsight value; it is not a claim that the
content is a forecast, and the family's own semantics (tariff status, issued
limits, plan snapshots) stand. It assumes ``capture_written_at`` is stamped
after the body was fetched and that gridflow's clock is not behind real time.

:func:`with_first_seen` only annotates. The two selectors are the only reads
that may serve rows of a held family, and each refuses a family absent from the
registry's ``_observed_before_target.json``:

- :func:`observed_snapshot` -- what gridflow held at ``as_of``: the newest
  complete capture written at or before ``as_of`` (per resource where the
  record selects per resource), then its rows observed before their target;
- :func:`observed_archive` -- every distinct row version observed before its
  target, once, with the lineage of its first complete capture. This is
  content observed, not content current at ``as_of``.

Neither selector reads anything written after ``as_of``, so a later capture
never changes an earlier result. As-of is a parameter, never baked into a view
(RULINGS 466). Completion records come from
:func:`~gridflow.silver.neso_data_portal.completion.scan_completions`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import polars as pl

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.registry.record import RESERVED
from gridflow.silver.latest_views import complete_captures

if TYPE_CHECKING:
    from collections.abc import Iterable

    from gridflow.connectors.neso_data_portal.registry import Registry, SchemaRecord

__all__ = [
    "FIRST_SEEN_AT",
    "OBSERVED_BEFORE_TARGET",
    "NotObservedBeforeTargetError",
    "observed_archive",
    "observed_snapshot",
    "version_columns",
    "with_first_seen",
]

FIRST_SEEN_AT = "first_seen_at"
"""The added column: the row version's first complete capture (tz-aware UTC)."""

OBSERVED_BEFORE_TARGET = "observed_before_target"
"""The added annotation: whether the row's target starts after ``first_seen_at``."""

_RESOURCE = "__first_seen_resource"
_COMPLETE = "__first_seen_complete"
_TARGET = "__first_seen_target"
_UTC_COLUMNS = ("capture_written_at", "timestamp_utc")
_CAPTURE_ORDER = ("capture_written_at", "bronze_capture_id")


class NotObservedBeforeTargetError(ValueError):
    """A selector was asked for a family the observed-before-target policy does not list."""


def version_columns(columns: Iterable[str]) -> tuple[str, ...]:
    """The columns that identify a row version within one family and resource.

    Args:
        columns: The base frame's column names, in order.

    Returns:
        Every name not in the record ``RESERVED`` set, in the given order.
    """
    return tuple(name for name in columns if name not in RESERVED)


def with_first_seen(base: pl.LazyFrame, completions: pl.LazyFrame, *, family: str) -> pl.LazyFrame:
    """Annotate every base row with ``first_seen_at`` and ``observed_before_target``.

    An annotation only: it makes no eligibility claim and applies no as-of
    bound. Serve rows through :func:`observed_snapshot` or
    :func:`observed_archive`.

    Args:
        base: One family's all-vintage silver rows, carrying
            ``bronze_capture_id``, ``capture_written_at`` and ``timestamp_utc``
            (both tz-aware UTC).
        completions: Completion records; only ``family``'s take part.
        family: The family key (the completion records' ``family`` value).

    Returns:
        ``base``, row order kept, plus the two columns. Only complete captures
        count towards ``first_seen_at``; a row of an incomplete capture is
        annotated by its version's complete sightings, and a version no
        complete capture holds has a null ``first_seen_at`` and is not
        observed before its target.

    Raises:
        ValueError: A required column is missing or not UTC, or ``base``
            already carries one of the output names.
    """
    return _annotate(base, completions, family).drop(_RESOURCE, _COMPLETE)


def observed_snapshot(
    family: str,
    base: pl.LazyFrame,
    completions: pl.LazyFrame,
    as_of: datetime,
    *,
    registry: Registry | None = None,
) -> pl.LazyFrame:
    """The rows of the capture gridflow held at ``as_of`` that it observed before their target.

    The capture is chosen first: the newest complete capture with
    ``capture_written_at <= as_of`` (capture time, not ``available_at``), one per
    ``resource_id`` when the record sets ``latest_partition``. A deletion, a
    replacement or a valid-empty newest capture therefore clears older rows.
    Then only its rows whose version was first seen strictly before its target
    start are kept, ``first_seen_at`` taken over the complete captures written
    at or before ``as_of``. Lineage columns are the chosen capture's.

    Args:
        family: The family key; it must be on the observed-before-target policy.
        base: As :func:`with_first_seen`.
        completions: As :func:`with_first_seen`.
        as_of: The tz-aware instant of the read.
        registry: The registry; the committed one when ``None``.

    Returns:
        The selected rows plus ``first_seen_at``.

    Raises:
        NotObservedBeforeTargetError: ``family`` is not on the policy.
        ValueError: ``as_of`` is naive, or as :func:`with_first_seen`.
    """
    record = _admitted(family, registry)
    bound = _utc(as_of)
    bounded = base.filter(pl.col("capture_written_at") <= bound)
    ranked = (
        complete_captures(bounded, completions, family)
        .filter(pl.col("capture_written_at") <= bound)
        .sort(list(_CAPTURE_ORDER), descending=True, nulls_last=True)
    )
    if record.latest_partition is not None:
        winners = ranked.unique(subset=[record.latest_partition], keep="first", maintain_order=True)
    else:
        winners = ranked.head(1)
    return (
        _annotate(bounded, completions, family)
        .join(winners.select("bronze_capture_id"), on="bronze_capture_id", how="semi")
        .filter(pl.col(OBSERVED_BEFORE_TARGET))
        .drop(_RESOURCE, _COMPLETE, OBSERVED_BEFORE_TARGET)
    )


def observed_archive(
    family: str,
    base: pl.LazyFrame,
    completions: pl.LazyFrame,
    as_of: datetime | None = None,
    *,
    registry: Registry | None = None,
) -> pl.LazyFrame:
    """Every distinct row version gridflow observed before its target, once.

    Each version is represented by its row in its first complete capture, so
    every lineage column is that capture's and nothing of a later capture
    leaks. This is content observed, not content current at ``as_of``: a version
    a later capture revised or deleted is still returned.

    Args:
        family: The family key; it must be on the observed-before-target policy.
        base: As :func:`with_first_seen`.
        completions: As :func:`with_first_seen`.
        as_of: When set (tz-aware), only captures written at or before it take
            part, so every returned version has ``first_seen_at <= as_of``.
        registry: The registry; the committed one when ``None``.

    Returns:
        One row per version plus ``first_seen_at``.

    Raises:
        NotObservedBeforeTargetError: ``family`` is not on the policy.
        ValueError: ``as_of`` is naive, or as :func:`with_first_seen`.
    """
    _admitted(family, registry)
    if as_of is not None:
        base = base.filter(pl.col("capture_written_at") <= _utc(as_of))
    annotated = _annotate(base, completions, family)
    version = [_RESOURCE, *version_columns(base.collect_schema().names())]
    return (
        annotated.filter(
            pl.col(_COMPLETE)
            & pl.col(OBSERVED_BEFORE_TARGET)
            & (pl.col("capture_written_at") == pl.col(FIRST_SEEN_AT))
        )
        .sort("bronze_capture_id")
        .unique(subset=version, keep="first", maintain_order=True)
        .drop(_RESOURCE, _COMPLETE, OBSERVED_BEFORE_TARGET)
    )


def _admitted(family: str, registry: Registry | None) -> SchemaRecord:
    loaded = registry_module.load_registry() if registry is None else registry
    entries = registry_module.load_observed_before_target(loaded.root, registry=loaded)
    if family not in {entry.family for entry in entries}:
        raise NotObservedBeforeTargetError(
            f"{family!r} is not on {registry_module.OBSERVED_BEFORE_TARGET_FILE} (ADR-044)"
        )
    record = loaded.families[family][1].record
    assert record is not None  # the policy loader admits only recorded families
    return record


def _utc(as_of: datetime) -> datetime:
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError(f"as_of must be timezone-aware, got {as_of!r}")
    return as_of.astimezone(UTC)


def _annotate(base: pl.LazyFrame, completions: pl.LazyFrame, family: str) -> pl.LazyFrame:
    schema = base.collect_schema()
    names = schema.names()
    clash = sorted({FIRST_SEEN_AT, OBSERVED_BEFORE_TARGET} & set(names))
    if clash:
        raise ValueError(f"base already carries {clash}")
    for name in ("bronze_capture_id", *_UTC_COLUMNS):
        if name not in schema:
            raise ValueError(f"base has no {name!r} column")
    for name in _UTC_COLUMNS:
        dtype = schema[name]
        if not isinstance(dtype, pl.Datetime) or dtype.time_zone != "UTC":
            raise ValueError(f"{name!r} must be a UTC datetime, got {dtype}")

    complete = complete_captures(base, completions, family).select(
        "bronze_capture_id", pl.lit(True).alias(_COMPLETE)
    )
    ledger = (
        completions.filter(pl.col("family") == family)
        .select("bronze_capture_id", pl.col("resource_id").alias(_RESOURCE))
        .join(complete, on="bronze_capture_id", how="left")
    )
    version = [_RESOURCE, *version_columns(names)]
    in_complete = pl.col(_COMPLETE)
    return (
        base.join(ledger, on="bronze_capture_id", how="left", maintain_order="left")
        .with_columns(in_complete.fill_null(False))
        .with_columns(
            pl.when(in_complete)
            .then(pl.col("capture_written_at"))
            .min()
            .over(version)
            .alias(FIRST_SEEN_AT),
            pl.when(in_complete).then(pl.col("timestamp_utc")).min().over(version).alias(_TARGET),
        )
        .with_columns(
            (pl.col(_TARGET) > pl.col(FIRST_SEEN_AT)).fill_null(False).alias(OBSERVED_BEFORE_TARGET)
        )
        .drop(_TARGET)
    )
