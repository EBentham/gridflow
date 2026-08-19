"""Abstract base class for silver-layer transformers."""

from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, assert_never

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

import polars as pl
from pydantic import BaseModel, ValidationError

from gridflow.silver.partition_window import (
    IntervalSemantics,
    OwnershipVerdict,
    RequestWindow,
    WindowReason,
    exclude_out_of_window,
    filter_frame_to_window,
    neighbour_owns,
    partition_request_window,
)
from gridflow.storage.parquet import write_parquet
from gridflow.utils.time import settlement_period_to_utc

logger = logging.getLogger(__name__)

_VALIDATION_SAMPLE_LIMIT = 5
"""Max distinct validation-error strings logged per ``run()`` (fail-soft; bounded)."""

_BRONZE_VINTAGE_COLUMN = "gf_bronze_vintage"
"""Transient per-row bronze vintage carrier for ``LOCKSTEP_BRONZE_READ`` reads.

Added by ``run()`` to the RAW frame, consumed by ``_add_bitemporal_columns``
as the ingest-time source, and DROPPED inside ``_process_frame`` before
``_write_silver`` -- it must never reach silver.

No frame-level ``available_at`` can satisfy the invariant once two bronze
files with different stamps both contribute rows: ``max`` over-stamps the
earlier file's rows (hiding them from a point-in-time query positioned between
the two) and ``min`` leaks the later file's (lookahead bias). So the stamp
travels WITH the row, from the same per-file structure the rows come from, and
``available_at``'s existing row-wise ``coalesce(published_at, ingest_time)``
(ADR-025 §3) simply gets a per-row fallback arm for opted-in transformers.

The name is deliberately (a) lower-snake with no leading underscore, so
ENTSO-G's ``_camel_to_snake`` is the identity on it, and (b) ``gf_``-prefixed,
to make a vendor-field clash implausible.

WARNING: ``gf_``-prefixing does NOT make collision impossible.
``_normalise_column_names`` maps ``gfBronzeVintage``, ``gf-bronze-vintage`` and
``GF_Bronze_Vintage`` all onto this exact name and coalesces them, so the
collision guard tests NORMALISED names (``_normalise_raw_column_name``), never
the literal string. "Vendor fields are camelCase" is not a defence here --
camelCase is precisely the colliding spelling.
"""

_SIDECAR_TIMESTAMP_KEYS: tuple[str, ...] = (
    "available_at",
    "written_at",
    "response_received_at",
    "fetched_at",
)
"""Bronze sidecar timestamp keys, most-to-least authoritative as the historical
availability anchor.

``written_at`` (durable bronze write completion) is preferred over
``fetched_at`` (stamped at ``RawResponse`` construction, before any
paging/retries) so reingest reconstructs availability from the true write time
rather than a pre-write proxy. The direction is conservative
(``written_at >= fetched_at``), so this never makes a row look available
earlier than it truly was. ``response_received_at`` is an as-yet-unwritten
reserved key kept for forward compatibility.

**The search FALLS THROUGH on a parse failure** (R2-g D-3): a key that is
present but unparseable does not end the search, so a sidecar with an invalid
``written_at`` and a valid ``fetched_at`` still vouches, on ``fetched_at``.
Only a successful parse returns.

N-16 (accepted residual, NOT fixed in R2-g): ``bronze/writer.py`` captures
``written_at`` *before* the body write it marks the completion of, so a
RECORDED stamp can be earlier than the true durable-write instant. Every
availability claim in this module is stated over the recorded stamp.
"""


class BronzeVouchReason(StrEnum):
    """Why a bronze body could not be vouched for by its own sidecar (ADR-028).

    ``UNPARSEABLE_TIMESTAMP`` means EVERY present-and-truthy key in
    :data:`_SIDECAR_TIMESTAMP_KEYS` failed to parse, never merely the first
    one -- see that constant's fall-through note (R2-g D-3).
    """

    NO_SIDECAR = "NO_SIDECAR"
    """No ``<body stem>.meta.json`` beside the body (the literal orphan: a
    crash between ``bronze/writer.py``'s body write and its sidecar write)."""
    UNREADABLE_SIDECAR = "UNREADABLE_SIDECAR"
    """``OSError``/``JSONDecodeError`` on the sidecar, or syntactically valid
    but non-object JSON (``[1, 2]``, ``"text"``, ``3``, ``null``)."""
    NO_TIMESTAMP_KEY = "NO_TIMESTAMP_KEY"
    """Valid sidecar object, but no key from :data:`_SIDECAR_TIMESTAMP_KEYS`
    was present and truthy."""
    UNPARSEABLE_TIMESTAMP = "UNPARSEABLE_TIMESTAMP"
    """Every present-and-truthy key failed to parse into a datetime."""


class BronzeReadSelection(StrEnum):
    """Which vouched bronze bodies a lockstep read consumes (R2-g D-4).

    The truncation happens INSIDE the resolver, on the vouched list -- never
    before it. Truncating first would let one orphan newest body empty the
    frame for a dataset with dozens of perfectly good older files.
    """

    ALL = "ALL"
    """Examine every candidate; read every one that vouches."""
    NEWEST_VOUCHED = "NEWEST_VOUCHED"
    """Walk candidates in order and stop at the FIRST that vouches. The
    stepped-over bodies are counted and named; the selected file's rows are
    read together with its OWN stamp -- never a borrowed sibling's."""


@dataclass(frozen=True)
class VouchedBronzeSet:
    """The single value threaded to BOTH the frame read and the vintage (I-1).

    Resolved by exactly ONE filesystem scan and ONE sidecar read per examined
    candidate, so there is no second scan for the two derivations to disagree
    about -- R2-C's ``IncrementalWindow.snapshot`` shape (ADR-027) applied to
    the filesystem.

    Deliberately has NO ``available_at()`` aggregate: per D-5 the vintage is
    PER ROW, taken from ``dict(entries)`` as a path -> stamp lookup at
    frame-build time. No frame-level scalar can satisfy the invariant once two
    files with different stamps both contribute rows, so none is offered.

    Attributes:
        entries: ``(body path, that body's OWN recorded stamp)`` pairs, in
            candidate order. The pairing is structural: a stamp exists only as
            the second element of a tuple whose first element IS in the read
            set, so a sibling's stamp can never be borrowed.
        unvouched: ``(body path, reason)`` for every examined candidate that
            could not vouch. Excluded from the frame AND from the vintage --
            never deleted, never repaired, never written to.
        examined: How many candidates were actually probed. Under
            ``NEWEST_VOUCHED`` the walk stops early, so this is less than
            ``len(candidates)`` whenever a vouched file was found.
    """

    entries: tuple[tuple[Path, datetime], ...]
    unvouched: tuple[tuple[Path, BronzeVouchReason], ...]
    examined: int

    @property
    def paths(self) -> tuple[Path, ...]:
        """The body paths whose rows may enter the frame, in candidate order."""
        return tuple(path for path, _ in self.entries)


@dataclass(frozen=True)
class SidecarDiagnostic:
    """One failure seen while classifying a sidecar, in the order it occurred.

    Attributes:
        key: The :data:`_SIDECAR_TIMESTAMP_KEYS` entry that failed, or ``None``
            for a file-level failure (unreadable / undecodable sidecar).
        reason: The classification this individual failure carries.
        detail: Pre-narrowed text for the WARNING the logging wrapper replays,
            or ``None`` when master emits no warning for this failure -- a
            present-but-non-string, non-datetime value fails *silently* on
            master (:meth:`BaseSilverTransformer._parse_timestamp` returns
            ``None`` without logging for those).
    """

    key: str | None
    reason: BronzeVouchReason
    detail: str | None = None


@dataclass(frozen=True)
class SidecarRead:
    """Pure classification of one bronze sidecar (R2-g D-7).

    Attributes:
        timestamp: The vouched tz-aware UTC stamp, or ``None``.
        reason: ``None`` iff ``timestamp`` is not ``None``.
        diagnostics: An ORDERED log of every failure seen on the way,
            INCLUDING ones that preceded a later success. Master logs a
            warning per failed key and then keeps going (the fall-through at
            :data:`_SIDECAR_TIMESTAMP_KEYS`), so a single ``reason``/``detail``
            pair could not let the logging wrapper replay master's output.
        non_object_json: ``True`` iff the sidecar held syntactically valid
            JSON that is not an object. Master reaches ``meta.get(key)`` on it
            and raises an uncaught ``AttributeError``; the wrapper preserves
            that exactly (N-18) while this classifier excludes the file.
        payload: The parsed non-object payload, kept solely so the wrapper can
            reproduce master's ``AttributeError`` verbatim. Meaningless unless
            ``non_object_json`` is ``True``.
    """

    timestamp: datetime | None
    reason: BronzeVouchReason | None
    diagnostics: tuple[SidecarDiagnostic, ...] = ()
    non_object_json: bool = False
    payload: Any = None


_EXACT_PARTITION_ONLY_SOURCES: frozenset[str] = frozenset({"entsoe", "entsog"})
"""Sources whose connectors write day-exact bronze partitions (P0.8 / R2-F08,
plus ENTSO-G via R2-g / F-05).

As of P0.8, ``EntsoeConnector.fetch`` chunks every multi-day window into one
request per covered UTC calendar day, so a correctly-fetched ENTSO-E date
either has its own exact bronze partition or has no bronze at all. For these
sources, any covering-fallback hit (``_find_covering_bronze_partition``) would
fabricate wrong-day rows: it would silently relabel a neighbouring day's rows
under the requested date's silver file, reproducing the R2-F08 duplication bug
chunking exists to fix. This mirrors the project's own precedent for exactly
this failure class — ``VINTAGE_PER_BRONZE_FILE`` / ADR-025 (class docstring
above, "Only the EXACT date partition is read — never the multi-day
covering-partition fallback") and the ENTSO-G generic family's exact-only
``_bronze_files`` (``silver/entsog/generic.py``).

**ENTSO-G (R2-g, closing F-05's open half).** ``EntsogConnector.fetch`` chunks
every multi-day window into one request per covered UTC calendar day
(``connectors/entsog/client.py``), so the same exact-or-nothing guarantee
holds. Before this change, ``PhysicalFlowsTransformer.read_bronze`` could
resolve a covering partition up to **35 days** before ``target_date`` and
relabel those rows under it -- precisely the fabrication the per-day chunking
exists to prevent.

The two gated call sites, and what entsog's membership changes at each:

- ``_bronze_path_for_date`` (the READ path, below): the covering fallback is
  removed. This IS F-05's open half, and the only production effect the flip
  now has.
- ``_bronze_date_dirs`` (the VINTAGE path, below): **dead with respect to
  entsog.** R2-g's ``LOCKSTEP_BRONZE_READ`` branch resolves the vintage from
  the same vouched set as the read, so neither ENTSO-G family calls this
  method at all any more (pinned by a spy in
  ``tests/silver/test_entsog_exact_partition.py``). The gate is left in place
  because it remains correct for ``entsoe``.

That ordering is deliberate and load-bearing. Flipping the frozenset while the
vintage path still ran through ``_bronze_date_dirs`` would make that walk
return ``[]`` for any date without an exact partition and fall through to
``datetime.now(UTC)`` -- a FABRICATED vintage, measured 26 days off on an
earlier attempt. Removing entsog from that method's caller set first leaves
the flip with no path to fire down.

Source-scoped (not a per-transformer ``ClassVar`` flag) because the exact-only
guarantee is a property of the *connector's* write layout established by this
unit: every current and future ENTSO-E transformer is covered automatically,
and a new ENTSO-E dataset cannot silently reintroduce the fallback by
forgetting to set a flag. ``_find_covering_bronze_partition`` itself is not
modified — NESO and Open-Meteo/ALSI resolution (the other callers) are
unaffected; ``tests/silver/test_partition_fallback.py``'s ``test_source``
stub pins that the fallback stays intact for non-ENTSO-E sources.
"""

_PUBLICATION_WINDOW_FILTER_SOURCES: frozenset[str] = frozenset({"elexon"})
"""Sources whose bronze partitions are PUBLICATION windows, not settlement
days (R2-A / F-04, F-10, F-16).

Elexon's ``PUBLISH_DATETIME`` chunks are closed at both ends and share their
boundary instant with the neighbouring chunk (``R2-A-PLAN.md`` S1.1), so the
same vendor record is written into BOTH partitions unless trimmed under a
proven neighbour-durability gate (D-3b). Source-scoped like
``_EXACT_PARTITION_ONLY_SOURCES`` above, for the same reason: the property
belongs to the connector's write layout, not to any one transformer, so every
current and future in-scope Elexon dataset is covered automatically (scope
and exemptions are resolved per-dataset by
``silver.elexon._publication_window``, imported lazily inside
``_resolve_publication_window_plan`` to avoid a module-load-time cycle
through ``silver.elexon.__init__`` — see that method's docstring).
"""


@dataclass(frozen=True)
class _PublicationWindowPlan:
    """Per-``run()`` inputs for the request-window filter, shared by both
    Elexon's publication-window filter
    (:meth:`BaseSilverTransformer._apply_publication_window_filter`, Task 2)
    and ENTSO-E's event-window filter
    (:meth:`BaseSilverTransformer._apply_event_window_filter`, Task 4 / F-10).

    Resolved once per ``run()`` call from the CURRENT partition's own
    sidecars (D-7e, all-or-nothing) — cheap and IO-bounded to this one
    partition. Any neighbour-durability proof needed under CLOSED interval
    semantics (D-3b, extra IO against a neighbour partition) is deliberately
    NOT resolved here: it is deferred until the transformed frame is known
    to actually have a row sitting at a boundary (R-10's short-circuit),
    which needs the frame, not just the partition directory.
    """

    column: str
    window: RequestWindow
    from_param: str
    to_param: str
    interval_semantics: IntervalSemantics = IntervalSemantics.CLOSED
    """Which of the two request-application paths applies (Sol ruling,
    2026-07-26) — a property of the VENDOR's OWN interval semantics, never
    of a source name, so a future connector inherits the correct behaviour
    by declaring which one its vendor uses:

    - ``CLOSED`` (default — Elexon): the vendor's request window is closed
      at both ends, so a boundary row is genuinely REQUESTED by two
      adjacent chunks and its removal needs D-3b's neighbour-durability
      proof (:meth:`_apply_publication_window_filter`). D-3: Elexon never
      enforces its lower bound at all (only the trailing/upper boundary is
      genuinely shared in practice).
    - ``HALF_OPEN`` (set only by
      :meth:`BaseSilverTransformer._resolve_event_window_plan`, ENTSO-E):
      the vendor's request is ``[start, end)`` but it may return rows
      beyond it that were never part of THIS partition's request at all —
      no ownership question, unconditionally excluded
      (:meth:`_apply_event_window_filter`)."""


def gas_day_event_time_expr(column: str = "gas_day") -> pl.Expr:
    """Build the fixed-06:00 UTC event-time expression for a gas day.

    Fixed 06:00 UTC is Gridflow's project labelling convention required by the
    project ``CLAUDE.md``, the GIE vendor README, P0.6, and R1-F06. It
    deliberately differs from the broader DST-aware vault page while tracked
    follow-up P0.6-DOC-1 is unresolved. Any future convention change requires
    another major dataset-version bump.

    Args:
        column: Name of the ``pl.Date`` gas-day column.

    Returns:
        A UTC-aware Polars expression aliased to ``event_time``.
    """
    return (pl.col(column).cast(pl.Datetime("us", "UTC")) + pl.duration(hours=6)).alias(
        "event_time"
    )


class BaseSilverTransformer(ABC):
    """Base class for bronze -> silver transformations.

    Subclasses implement:
    - source: the data source name
    - dataset: the dataset name
    - read_bronze(): read and parse raw bronze files
    - transform(): apply normalisation, validation, deduplication
    """

    source: str
    dataset: str
    schema_cls: ClassVar[type[BaseModel] | None] = None
    """Opt-in Pydantic schema for full-frame silver validation (VTA-SCHEMA-01).

    When set, ``run()`` validates every row of the ``transform()`` output against
    this model, fail-soft: failures are counted into
    ``last_validation_failure_count`` and surfaced by the CLI as
    ``completed_with_warnings`` — never raised, never dropped. ``None`` (the
    default) skips validation, which cleanly excludes generic/dynamic transformers
    that have no fixed Pydantic contract (the ENTSO-G generic family incl. CMP, the
    GIE generic JSON family). Subclasses that serve one dataset set this as a class
    attribute; one-class-many-schemas transformers (NESO) set it per instance.
    """
    write_silver_csv: bool = False
    """Opt-in for the per-date silver CSV sidecar (CH3-02 / CH-PERF-02 / C4-1).

    Default ``False``: ``run()`` writes only the canonical Parquet partition. The
    legacy always-on ``_write_csv`` emitted an unpartitioned ``.csv`` alongside
    every Parquet write on every run, doubling the silver write surface for a
    sidecar no read/gold/quality consumer reads (on-demand CSV is served by the
    ``export_csv`` CLI command). Set per-instance at the call boundary from
    ``PipelineSettings.write_silver_csv`` — a plain instance attribute (not
    ``ClassVar``), mirroring ``last_unmapped_count``, so the boundary assignment
    is clean under ``mypy --strict`` and never leaks class state across runs.
    """
    last_unmapped_count: int = 0
    """Count of rows whose enum code was unmapped in the most recent ``run()``.

    Reset to 0 at the start of every ``run()`` and set by ``transform()`` when a
    transformer maps an enum with an unmapped-code sentinel (ADR-022). The CLI
    reads it after each per-date ``run()`` to thread the unmapped total into
    ``PipelineRunTracker.complete_with_warnings``. Resetting in ``run()`` (not
    only on ``transform()``'s happy path) keeps a date with no bronze or missing
    columns from being charged the previous date's count.
    """
    last_validation_failure_count: int = 0
    """Count of rows that failed ``schema_cls`` validation in the most recent ``run()``.

    Reset to 0 at the start of every ``run()`` (before any early return) and set by
    the central ``_validate_against_schema`` step on the ``transform()`` output. The
    CLI accumulates it across dates and threads the total into
    ``PipelineRunTracker.complete_with_warnings`` (parallel to
    ``last_unmapped_count``; VTA-SCHEMA-01). Rows that fail validation are still
    written by ``_validate_against_schema`` — for the rows THIS counter counts,
    the count is the only signal (fail-soft). That sentence describes the base
    validator, not the whole of ``rows_invalid``: a transformer that EXCLUDES a
    row it declares invalid counts it in ``last_excluded_row_count`` instead,
    and ``run_transform`` folds both into one reported total (D-40).
    """
    last_excluded_row_count: int = 0
    """Rows a transformer DECLARED INVALID AND REMOVED in the most recent ``run()``.

    A plain class-level attribute carrying a per-instance value -- exactly the
    form ``last_unmapped_count`` uses, and deliberately NOT ``typing.ClassVar``:
    this is per-run instance state, so a ``ClassVar`` annotation would both
    misdescribe it and invite a class-level mutation leaking across every
    transformer in the process.

    Reset to 0 at the start of every ``run()`` and ACCUMULATED with ``+=``
    inside ``transform()`` -- never assigned. The ``+=`` is not a style
    preference: on the ``VINTAGE_PER_BRONZE_FILE`` branch ``transform()`` is
    called once per bronze file against a single reset in ``run()``, so an
    assignment would silently discard every earlier file's exclusions.

    Exists because an excluded row is invisible to the counter that would
    otherwise see it: the exclusion happens inside ``transform()`` and
    ``_validate_against_schema`` runs on ``transform()``'s OUTPUT, so the
    removed row is never seen by the thing that counts. Without this counter a
    run can drop rows loudly in the log and still report plain ``success``.

    ``run_transform`` folds it into the same per-dataset total as
    ``last_validation_failure_count``, so an exclusion reaches
    ``completed_with_warnings``, ``rows_skipped`` and ``rows_invalid`` through
    the path that already exists -- no new result field, no new precedence rung.

    THE CONFLATION IS DELIBERATE AND IS STATED HERE RATHER THAN LEFT TO BE
    DISCOVERED: ``rows_invalid`` consequently carries TWO dispositions -- rows
    that failed validation and were still WRITTEN (the base class's documented
    fail-soft) and rows a transformer declared invalid and EXCLUDED. The
    discriminator is the per-row WARNING the excluding transformer emits. One
    number is the point: the operator's question is "did this run produce
    anything the contract calls wrong", and that question has one answer.
    """
    last_partition_filter_dropped_count: int = 0
    """Rows dropped by the request-window filter in the most recent ``run()``
    (R2-A / F-04, F-10). Reset to 0 at the start of every ``run()``. Nonzero
    for sources in ``_PUBLICATION_WINDOW_FILTER_SOURCES`` with an in-scope,
    resolvable dataset (Elexon, CLOSED interval semantics —
    ``_apply_publication_window_filter``, a proven-durable boundary row) OR
    a transformer with ``EVENT_WINDOW_FILTER = True`` (ENTSO-E, HALF_OPEN
    interval semantics — ``_apply_event_window_filter``, an unconditionally
    out-of-scope row, no proof needed) — the two filters share this counter
    since they are the same underlying primitive family
    (``partition_window.py``) applied under two different vendor interval
    semantics (Sol ruling 2026-07-26; see ``IntervalSemantics``).
    """
    last_partition_filter_unclassified_count: int = 0
    """Rows whose filter-dimension value was null (unclassifiable) in the most
    recent ``run()`` — always KEPT, never dropped (D-7d). Reset to 0 at the
    start of every ``run()``.
    """
    last_partition_filter_unresolved_count: int = 0
    """Count of ``run()`` calls where the partition-window filter could not
    resolve the CURRENT partition's own window at all (D-7e: an unpaired raw
    body, a missing/invalid sidecar, or an unparseable bound) and was
    therefore disabled for the whole partition. Reset to 0 at the start of
    every ``run()``. This is the aggregate A-15's R2-exit gate checks for
    ``ORPHAN_BODY``/unresolved events (R-12: not otherwise self-detecting).
    """
    last_partition_filter_boundary_retained_count: int = 0
    """Rows retained at a window boundary because neighbour ownership was
    attempted but could not be proven (D-3b/D-3e) — the failing
    ``WindowReason`` is always logged alongside this count, never just the
    number (A-3/A-12). Reset to 0 at the start of every ``run()``. Only ever
    incremented on the CLOSED-interval (Elexon) path — HALF_OPEN (ENTSO-E)
    never retains an out-of-scope row, so this stays 0 for
    ``EVENT_WINDOW_FILTER`` transformers (Sol ruling 2026-07-26).
    """
    last_partition_filter_all_dropped_count: int = 0
    """Rows excluded by a 100%-out-of-window event-window filter drop in the
    most recent ``run()`` (Sol re-review, 2026-07-26 — the ``all_dropped``
    gap: performing the HALF_OPEN drop instead of D-5's CLOSED-path refusal
    is correct, but logging it as ``ERROR`` is not a status, and a 100% drop
    on an in-scope dataset is never a normal outcome). Reset to 0 at the
    start of every ``run()``; incremented ONLY on the HALF_OPEN
    (``EVENT_WINDOW_FILTER``) path when :func:`exclude_out_of_window` reports
    ``all_dropped=True`` — never on the CLOSED (Elexon) path, which keeps
    D-5's refusal and its ``success`` status completely unchanged. The CLI
    (``pipeline/runner.py::run_transform``) treats a nonzero accumulated
    total as a HARD FAILURE for the whole date range, distinct from and
    taking priority over the ``completed_with_warnings`` path that
    ``last_unmapped_count``/``last_validation_failure_count`` drive — a
    misclassified opt-in must stop the run, not blend into a routine
    warning. Deliberately NOT threaded together with the other
    ``last_partition_filter_*`` counters (those stay deferred to N-10):
    this one counter is the sole exceptional-outcome signal propagated by
    this fix.
    """
    last_unvouched_bronze: frozenset[tuple[Path, BronzeVouchReason]] = frozenset()
    """Bronze bodies EXCLUDED from the most recent ``run()`` because their own
    sidecar could not vouch for them (R2-g / ADR-028). Reset at the top of
    every ``run()``; only the ``LOCKSTEP_BRONZE_READ`` branch ever populates it.

    Carries the ``(path, reason)`` ASSOCIATION deliberately, rather than a
    running integer or a path set beside a detached reason counter. Both of
    those misreport:

    - the reference ENTSO-G family rescans the whole tree on every target
      date, so one orphan body over a 30-day range would be counted 30 times —
      a 30x overstatement of the remediation scope in the very record whose
      job is to size it. ``run_transform`` unions the SETS instead;
    - once paths are deduplicated across dates, a detached ``Counter`` cannot
      say which reason belongs to a newly-seen path, so exact per-reason
      totals become impossible.

    A path appears at most once because the classifier assigns exactly one
    reason per file per read.
    """
    last_unvouched_total_exclusion: bool = False
    """``True`` when the most recent ``run()`` examined at least one bronze
    candidate and NONE of them vouched — bronze demonstrably exists and zero
    rows could be read from it (R2-g D-9 rung 2).

    Distinct from "no bronze at all" (``examined == 0``), which is not a
    failure. ``run_transform`` turns a nonzero accumulated total into a HARD
    FAILURE for the whole dataset, mirroring
    ``last_partition_filter_all_dropped_count``: under exclusion the frame is
    empty, and only a failed dataset-level status stops a stale pre-existing
    Parquet being treated as current.
    """
    LOCKSTEP_BRONZE_READ: ClassVar[bool] = False
    """Opt in to resolving the bronze read set and the vintage from ONE scan.

    When ``True``, ``run()`` calls :meth:`_bronze_candidates` exactly once and
    threads the resulting :class:`VouchedBronzeSet` into BOTH the frame read
    and ``available_at`` — so the set of files whose rows enter the frame and
    the set of files whose sidecars determine the vintage are the SAME set, by
    construction rather than by argument (R2-g I-1). Opt-ins must implement
    :meth:`_bronze_candidates` AND :meth:`_read_bronze_records`.

    Per-transformer (like :attr:`VINTAGE_PER_BRONZE_FILE`), NOT source-scoped
    like ``_EXACT_PARTITION_ONLY_SOURCES``. The honest reason, so the next
    reader need not reconstruct it: vouching is a property of the BRONZE
    WRITER's body-then-sidecar ordering (``bronze/writer.py``), which is
    universal across sources — so the control is universally correct, not
    entsog-specific. It is rolled out narrowly because every other source has
    bronze on disk and changing their read sets is a data-semantics change on
    live data. Generalising the rollout is N-15, which must audit every
    ``_bronze_files`` override rather than point-fix known cases.

    Mutually exclusive with :attr:`VINTAGE_PER_BRONZE_FILE`: they are different
    vintage strategies over the same partition, and ``run()`` dispatches over
    three mutually exclusive branches. A transformer setting both is a bug.
    """
    BRONZE_READ_SELECTION: ClassVar[BronzeReadSelection] = BronzeReadSelection.ALL
    """Default selection policy for :attr:`LOCKSTEP_BRONZE_READ` reads.

    Transformers whose policy depends on instance state override
    :meth:`_bronze_read_selection` instead of reassigning this — a ClassVar
    mutated at import time cannot express "reference datasets only".
    """
    EVENT_WINDOW_FILTER: ClassVar[bool] = False
    """Opt-in, PER TRANSFORMER (D-6 — contrast with Elexon's source-scoped
    ``_PUBLICATION_WINDOW_FILTER_SOURCES``), to the ENTSO-E event-window
    filter (R2-A Task 4 / F-10). Default ``False``. When ``True``, ``run()``
    resolves the CURRENT partition's own ``[periodStart, periodEnd)`` request
    window (same D-7e all-or-nothing pairing rule as the publication-window
    filter) and UNCONDITIONALLY excludes ``timestamp_utc`` rows outside it
    (HALF_OPEN interval semantics, Sol ruling 2026-07-26 — see
    ``partition_window.IntervalSemantics``). Unlike Elexon's CLOSED-interval
    boundary row, an out-of-window ENTSO-E row was never part of THIS
    partition's own request at all (the vendor's measured CET/CEST
    delivery-day over-span, S1.4), so there is no ownership question and no
    neighbour-durability proof to make before excluding it — it is always
    dropped, counted, and logged. See ``silver/entsoe/_event_window.py``
    (``EVENT_WINDOW_CLASSIFICATION``) for the full dataset -> verdict ->
    citation -> transformer-family map and the exemption table with reasons.
    **B4 (N-9): the opt-in set is R2-A's original 7 plus B4's 19
    evidence-classified FILTER_SAFE datasets (R3-RESEARCH.md Sec 1.1). A
    `TODO`-marked minority remains genuinely unclassified, so F-10 and the
    N-9 gate stay OPEN until that residual is resolved or accepted at a
    milestone close (a Bobbo decision).**
    """
    DATASET_VERSION: ClassVar[str] = "1.0.0"
    BRONZE_SIBLING_DATASETS: ClassVar[tuple[str, ...]] = ()
    APPEND_ONLY: ClassVar[bool] = False
    """Per-dataset opt-in for revision-preserving silver writes.

    Default ``False`` keeps the F0 atomic-replace behaviour: each
    ``(dataset, target_date)`` pair maps to a single Parquet file that is
    overwritten on each run. When ``True`` the writer emits a run-suffixed
    filename derived from ``available_at`` so successive runs coexist in the
    partition directory and downstream readers apply ``QUALIFY``-style
    selection at read time. See ``docs/DECISION_LOG/ADR-018-append-only-
    run-suffixed-files.md`` for the trade-off discussion. Only datasets that
    publish meaningful revisions (REMIT, FOU2T14D) should opt in.
    """
    VINTAGE_PER_BRONZE_FILE: ClassVar[bool] = False
    """Opt in to assigning one availability timestamp per bronze raw file.

    The default keeps the established whole-date read and single availability
    timestamp. Revision feeds whose bronze fetch time is their only vintage
    marker set this to ``True`` and implement ``read_bronze_file``.

    Contract notes for opt-ins (ADR-025):
    - Only the EXACT date partition is read — never the multi-day
      covering-partition fallback, which would stamp a prior day's rows into a
      wrong-dated vintage file (review finding, v0.17 PR-A).
    - A raw file without a parseable sidecar timestamp is SKIPPED loudly: a
      ``now()`` fallback would mint a new non-idempotent vintage filename on
      every re-transform and poison point-in-time reads.
    - Transformers that ASSIGN ``last_unmapped_count`` inside ``transform()``
      (the ADR-022 enum-mapping pattern) must convert to accumulation before
      opting in, or per-frame counts overwrite each other.
    """
    BRONZE_BODY_GLOB: ClassVar[str] = "raw_*.json"
    """Which bronze bodies the ``VINTAGE_PER_BRONZE_FILE`` per-file loop sees.

    Overridden by transformers whose bronze bodies are not JSON (the NESO
    Data Portal family captures CSV: ``"raw_*.csv"``). The ``.meta.json``
    guard in the loop stays correct under any glob -- a sidecar is never a
    body.

    Caller enumeration (required before touching a shared symbol): the single
    read site is the per-file glob inside ``if self.VINTAGE_PER_BRONZE_FILE:``,
    and the sole opt-in in the repo is
    ``silver/elexon/system_prices.py:32`` (grep-verified), which inherits this
    unchanged default -- so its behaviour is byte-identical. Every other
    ``raw_*.json`` literal in the repo is a per-transformer local glob in that
    transformer's own ``read_bronze`` and is untouched.

    Blast-radius classification: this is a READ-path symbol with WRITE-path
    consequences, and it must not be described as "read only". The glob
    selects which bodies are seen; for each selected body the loop reads that
    body's OWN sidecar for ``available_at``, and ``_write_silver`` turns that
    scalar into the run-suffixed silver FILENAME. So the symbol determines
    which vintages are assigned and which silver files are written. What makes
    the change safe is not its reach but its default: unchanged, with the sole
    inheritor enumerated above, so no existing dataset's vintage assignment or
    filename can move -- a claim pinned by tests on BOTH branches (JSON and
    CSV) asserting written silver FILENAMES, not row counts.
    """
    ENTITY_KEY_COLUMNS: ClassVar[tuple[str, ...]] = ()
    """The dataset's REQUIRED entity/business key — the columns that, taken
    together, grain the ``transform()`` output to one row per real-world
    entity (R2-A / F-16, D-8). Consumed by the F-16 duplicate-quality-check
    (``cli.py``) so it keys ``check_duplicates`` on the dataset's actual
    grain instead of a hardcoded ``(settlement_date, settlement_period)`` —
    the hardcoded pair falsely flagged every genuinely-distinct row of a
    dataset with a finer grain (e.g. ``fuelhh``'s ``fuel_type``) as a
    duplicate.

    Declared verbatim from each transformer's own ``.unique(subset=...)``
    dedup key where one exists; sourced from ``LATEST_VIEW_SPECS`` (cited
    in-comment) for the two APPEND_ONLY datasets with no in-transform
    ``unique()`` call at all (``system_prices``, ``remit`` — no
    ``unique(subset=...)`` to hoist). Empty (``()``) is a bug for any
    REGISTERED dataset — ``test_every_elexon_transformer_declares_an_entity_key``
    (A-4) pins 33/33.

    **D-8 order-insensitivity contract**: this is SET semantics
    (``unique(subset=...)`` behaves as a set of columns, not an ordered
    tuple) — ``test_unique_subset_is_order_insensitive`` pins that the golden
    map (``tests/fixtures/entity_keys_golden.json``) and every consumer
    compare declared keys as sets, never caring about declaration order.
    """
    OPTIONAL_ENTITY_KEY_COLUMNS: ClassVar[tuple[str, ...]] = ()
    """Key refinements included in the entity grain only when the column is
    actually present on the frame being checked (mirrors
    ``LatestViewSpec.optional_key_columns``, ``latest_views.py``) — e.g.
    ``fou2t14d``'s live forecastDate-only shape has no ``settlement_period``.
    Consumers resolve the effective key as ``ENTITY_KEY_COLUMNS + tuple(c for
    c in OPTIONAL_ENTITY_KEY_COLUMNS if c in <the frame's columns>)`` via
    :meth:`resolve_entity_key`.
    """

    @classmethod
    def resolve_entity_key(cls, available_columns: Iterable[str]) -> tuple[str, ...] | None:
        """Resolve the effective F-16 entity/business key for a frame with
        the given columns.

        Default (this implementation): ``ENTITY_KEY_COLUMNS`` plus whichever
        ``OPTIONAL_ENTITY_KEY_COLUMNS`` are actually present -- the additive
        contract every dataset with a single required shape needs. Returns
        ``None`` when ``ENTITY_KEY_COLUMNS`` is undeclared (empty).

        A transformer whose ``transform()`` picks between mutually EXCLUSIVE
        required shapes (not merely an additive optional refinement --
        e.g. ``WindForecastTransformer``'s settlement-coordinate vs
        ``timestamp_utc``-only fallback) overrides this classmethod to
        mirror that exact conditional. Class-level only (no instantiation,
        T-R2A-04) -- called by the CLI's ``_entity_key_for``.
        """
        if not cls.ENTITY_KEY_COLUMNS:
            return None
        available = set(available_columns)
        return tuple(cls.ENTITY_KEY_COLUMNS) + tuple(
            c for c in cls.OPTIONAL_ENTITY_KEY_COLUMNS if c in available
        )

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.bronze_dir = data_dir / "bronze" / self.source / self.dataset
        self.silver_dir = data_dir / "silver" / self.source / self.dataset

    @abstractmethod
    def read_bronze(self, target_date: date) -> pl.DataFrame:
        """Read and parse all bronze files for a given date.
        Returns a raw DataFrame before validation."""
        ...

    @abstractmethod
    def transform(self, raw_df: pl.DataFrame) -> pl.DataFrame:
        """Apply source-specific normalisation, validation, and deduplication.
        Returns a clean DataFrame matching the silver schema."""
        ...

    def read_bronze_file(self, raw_path: Path) -> pl.DataFrame:
        """Read one raw bronze file for per-file vintage capture.

        Only transformers opting into ``VINTAGE_PER_BRONZE_FILE`` need to
        implement this method; the ordinary ``read_bronze`` path is unchanged.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement read_bronze_file for per-file vintages"
        )

    def _bronze_read_selection(self) -> BronzeReadSelection:
        """Resolve this run's selection policy (R2-g D-6).

        Defaults to :attr:`BRONZE_READ_SELECTION`. Families whose policy
        depends on instance state (the ENTSO-G generic family's
        ``reference_dataset``) override this method rather than mutating the
        ClassVar at import time.
        """
        return self.BRONZE_READ_SELECTION

    def _bronze_candidates(self, target_date: date) -> list[Path]:
        """Resolve the ordered bronze BODY paths to consider for ``target_date``.

        THE one filesystem scan of a ``LOCKSTEP_BRONZE_READ`` run. Must return
        the FULL ordered candidate list -- any "newest only" truncation belongs
        to :class:`BronzeReadSelection`, applied after vouching, never before
        it. Must exclude ``*.meta.json``: the writer puts bodies and sidecars in
        the same directory under names that a ``raw_*.json`` glob both match,
        and a sidecar admitted as a body has no sidecar of its own, so it would
        classify ``NO_SIDECAR`` and fire the hard-fail rung on every healthy run.

        Args:
            target_date: The date being transformed.

        Returns:
            Body paths in the family's own selection order.
        """
        raise NotImplementedError(
            f"{type(self).__name__} sets LOCKSTEP_BRONZE_READ but does not implement "
            "_bronze_candidates + _read_bronze_records"
        )

    def _read_bronze_records(
        self,
        paths: Sequence[Path],
        target_date: date,
    ) -> tuple[tuple[Path, list[dict[str, Any]]], ...]:
        """Parse each vouched bronze body and apply this family's date filter.

        Returns PER-FILE record lists and constructs NO DataFrame, so the base
        keeps ownership of frame construction and the merged frame stays
        byte-identical to the non-lockstep path's. The per-file structure is
        what lets ``run()`` build the row list and the stamp list by parallel
        comprehensions over ONE object, so rows and stamps cannot desync.

        ``target_date`` is required, not optional: both ENTSO-G families apply
        a row-level date filter INSIDE the read, and a hook without it would
        strand that filter in ``read_bronze()``, which the lockstep branch
        bypasses -- writing parseable OFF-DATE rows into the target-date silver
        partition, the exact fabrication class this unit exists to stop.

        Implementations emit at most ONE record above ``DEBUG`` per call (the
        aggregated undated-record warning), never one per file.

        Args:
            paths: Vouched body paths, in candidate order.
            target_date: The date being transformed.

        Returns:
            ``(path, records)`` pairs in the same order as ``paths``.
        """
        raise NotImplementedError(
            f"{type(self).__name__} sets LOCKSTEP_BRONZE_READ but does not implement "
            "_bronze_candidates + _read_bronze_records"
        )

    def _normalise_raw_column_name(self, column: str) -> str:
        """Normalisation this family applies to RAW bronze column names.

        Used only by the lockstep branch's reserved-name collision guard.
        Identity by default; families that fold vendor spellings together
        (ENTSO-G's ``_camel_to_snake``) override it, because a guard that
        tested the literal name would pass a vendor field spelled
        ``gfBronzeVintage`` and let a datetime-castable vendor value silently
        replace the true bronze stamp.
        """
        return column

    def run(
        self,
        target_date: date,
        run_id: str | None = None,
        reingest: bool = False,
    ) -> int:
        """Execute the full bronze -> silver pipeline for one date.

        Returns the number of rows written.
        """
        # Reset the per-run warning counters before either early-return path so a
        # date with no bronze / missing columns is never charged a prior date's
        # count (ADR-022 unmapped + VTA-SCHEMA-01 validation; the CLI accumulates
        # both after each per-date run).
        self.last_unmapped_count = 0
        self.last_validation_failure_count = 0
        # D-40: the SINGLE reset the transformer's `+=` accumulates against.
        self.last_excluded_row_count = 0
        self.last_partition_filter_dropped_count = 0
        self.last_partition_filter_unclassified_count = 0
        self.last_partition_filter_unresolved_count = 0
        self.last_partition_filter_boundary_retained_count = 0
        self.last_partition_filter_all_dropped_count = 0
        # Not optional: run_transform reads these PER DATE inside its loop, so
        # a run() returning early without resetting would charge this date the
        # previous date's exclusions.
        self.last_unvouched_bronze = frozenset()
        self.last_unvouched_total_exclusion = False

        resolved_run_id = run_id or f"adhoc-{datetime.now(UTC).isoformat()}"
        frames: list[pl.DataFrame] = []
        saw_bronze = False

        # Mutually exclusive: a transformer is either an Elexon in-scope
        # dataset (source-scoped, D-6) or an ENTSO-E opt-in
        # (EVENT_WINDOW_FILTER, per-transformer, D-6) — never both, since
        # _PUBLICATION_WINDOW_FILTER_SOURCES is elexon-only and
        # EVENT_WINDOW_FILTER defaults False for every Elexon transformer.
        window_plan = self._resolve_publication_window_plan(
            target_date
        ) or self._resolve_event_window_plan(target_date)

        if self.VINTAGE_PER_BRONZE_FILE:
            # EXACT date partition only — the covering-partition fallback in
            # _bronze_date_dirs would stamp a prior day's rows into a file named
            # for target_date (wrong-dated vintage; see class docstring).
            exact_dir = (
                self.bronze_dir
                / str(target_date.year)
                / f"{target_date.month:02d}"
                / f"{target_date.day:02d}"
            )
            date_dirs = [exact_dir] if exact_dir.exists() else []
            for date_dir in date_dirs:
                for raw_path in sorted(date_dir.glob(self.BRONZE_BODY_GLOB)):
                    # The data glob also matches sidecars (raw_*.meta.json) — skip them.
                    if raw_path.name.endswith(".meta.json"):
                        continue
                    available_at = self._timestamp_from_sidecar(raw_path.with_suffix(".meta.json"))
                    if available_at is None:
                        # Skip loudly: a now() fallback would mint a fresh
                        # non-idempotent vintage file on every re-transform.
                        logger.warning(
                            "Skipping bronze file with no usable sidecar timestamp "
                            "(cannot assign an honest vintage): %s",
                            raw_path,
                        )
                        continue
                    raw_df = self.read_bronze_file(raw_path)
                    if raw_df.is_empty():
                        continue
                    saw_bronze = True
                    clean_df = self._process_frame(
                        raw_df, target_date, resolved_run_id, available_at, window_plan
                    )
                    if clean_df is not None:
                        frames.append(clean_df)
        elif self.LOCKSTEP_BRONZE_READ:
            # ONE scan, ONE sidecar read per examined candidate, threaded as a
            # single value into BOTH the frame read and available_at. There is
            # no second scan for the two derivations to disagree about, so the
            # wrong-partition, borrowed-sibling-stamp, mixed-sidecar and TOCTOU
            # failure modes are closed structurally rather than case by case.
            candidates = self._bronze_candidates(target_date)
            vouched = self._resolve_vouched_bronze_set(candidates, self._bronze_read_selection())
            self.last_unvouched_bronze = frozenset(vouched.unvouched)
            self.last_unvouched_total_exclusion = vouched.examined > 0 and not vouched.entries
            # Suppress master's generic "No bronze data" warning exactly when
            # this unit's own machinery will emit a record for this date -- i.e.
            # whenever any unvouched file was seen. Keyed on `unvouched`, never
            # an unconditional True: `entries` is empty in TWO shapes, and only
            # total exclusion (unvouched non-empty) should suppress it. With no
            # candidates at all, `unvouched` is empty and the warning correctly
            # still fires. Evaluated BEFORE the `not entries` arm below.
            saw_bronze = saw_bronze or bool(vouched.unvouched)
            if vouched.entries:
                stamp_by_path = dict(vouched.entries)
                pairs = self._read_bronze_records(vouched.paths, target_date)
                rows = [record for _, records in pairs for record in records]
                # Master's rule exactly: set only on a NON-EMPTY frame. Vouched
                # bronze holding only off-date rows must keep emitting master's
                # warning, or this becomes a new silent zero-row path.
                saw_bronze = saw_bronze or bool(rows)
                if rows:
                    # The only REACHABLE way rows and stamps could desync: a
                    # reader returning records for a path the resolver never
                    # vouched. Fail loud rather than mis-stamp those rows with
                    # a sibling's timestamp.
                    smuggled = [path for path, _ in pairs if path not in stamp_by_path]
                    if smuggled:
                        raise ValueError(
                            f"{self.source}/{self.dataset}: the bronze reader returned "
                            f"records for {smuggled}, which are not in the vouched read "
                            "set. The read set and the stamp set must be ONE set."
                        )
                    live_now = None if reingest else datetime.now(UTC)
                    stamps = [
                        stamp_by_path[path] if live_now is None else live_now
                        for path, records in pairs
                        for _ in records
                    ]
                    raw_df = pl.DataFrame(rows, infer_schema_length=None)
                    collisions = [
                        column
                        for column in raw_df.columns
                        if self._normalise_raw_column_name(column) == _BRONZE_VINTAGE_COLUMN
                    ]
                    if collisions:
                        raise ValueError(
                            f"{self.source}/{self.dataset}: bronze column(s) {collisions} "
                            f"normalise onto the reserved bronze-vintage carrier "
                            f"{_BRONZE_VINTAGE_COLUMN!r}. A vendor value there would "
                            "silently replace the true bronze stamp and corrupt "
                            "available_at. Rename the carrier before proceeding."
                        )
                    if len(stamps) != raw_df.height:
                        raise ValueError(
                            f"{self.source}/{self.dataset}: {len(stamps)} bronze vintage "
                            f"stamp(s) for {raw_df.height} raw row(s) -- the read hook's "
                            "per-file record counts disagree with the rows it returned."
                        )
                    raw_df = raw_df.with_columns(
                        pl.Series(_BRONZE_VINTAGE_COLUMN, stamps, dtype=pl.Datetime("us", "UTC"))
                    )
                    clean_df = self._process_frame(
                        raw_df,
                        target_date,
                        resolved_run_id,
                        live_now if live_now is not None else stamps[0],
                        window_plan,
                        vintage_column=_BRONZE_VINTAGE_COLUMN,
                    )
                    if clean_df is not None:
                        frames.append(clean_df)
            # Neither empty arm returns early: both fall through to the common
            # tail below, so master's "nothing to read" vs "read but transformed
            # to zero rows" distinction is preserved rather than bypassed.
        else:
            available_at = (
                self._available_at_from_bronze(target_date) if reingest else datetime.now(UTC)
            )
            raw_df = self.read_bronze(target_date)
            if not raw_df.is_empty():
                saw_bronze = True
                clean_df = self._process_frame(
                    raw_df, target_date, resolved_run_id, available_at, window_plan
                )
                if clean_df is not None:
                    frames.append(clean_df)

        if not frames:
            # Distinguish "nothing to read" from "read but transformed to zero
            # rows" (the latter already logged per frame by _process_frame).
            if not saw_bronze:
                logger.warning(f"No bronze data for {self.source}/{self.dataset} on {target_date}")
            return 0

        if self.write_silver_csv:
            # Frames can differ in optional columns (e.g. run_type present in one
            # vintage only) — diagonal concat null-fills instead of raising (CL-1).
            self._write_csv(pl.concat(frames, how="diagonal"), target_date)

        total_rows = sum(len(frame) for frame in frames)
        logger.info(
            f"Silver write: {self.source}/{self.dataset} {target_date} -> {total_rows} rows"
        )
        return total_rows

    def _process_frame(
        self,
        raw_df: pl.DataFrame,
        target_date: date,
        run_id: str,
        available_at: datetime,
        window_plan: _PublicationWindowPlan | None = None,
        *,
        vintage_column: str | None = None,
    ) -> pl.DataFrame | None:
        """Transform, filter, validate, stamp, and write one bronze-vintage frame.

        Args:
            raw_df: The raw bronze frame.
            target_date: The date being transformed.
            run_id: The pipeline run id stamped into ``source_run_id``.
            available_at: The frame-level ingest vintage. Ignored as the
                ingest-time source when ``vintage_column`` is set (each row
                carries its own), but still passed to ``_write_silver``.
            window_plan: The resolved request-window filter plan, if any.
            vintage_column: Name of the transient per-row bronze-vintage
                carrier on ``raw_df``, for ``LOCKSTEP_BRONZE_READ`` reads.
                ``None`` (the default) leaves both existing branches
                character-for-character unchanged. When set, the column is the
                ingest-time source and is DROPPED before ``_write_silver`` --
                it must never reach silver.

        Raises:
            ValueError: ``vintage_column`` was requested but ``transform()``
                dropped it (per-row ``available_at`` could not be derived), or
                it survived to the write boundary.
        """
        if raw_df.is_empty():
            return None

        clean_df = self.transform(raw_df)
        if clean_df.is_empty():
            logger.warning(f"Transform produced 0 rows for {target_date}")
            return None

        if window_plan is not None:
            clean_df = (
                self._apply_event_window_filter(clean_df, window_plan, target_date)
                if window_plan.interval_semantics is IntervalSemantics.HALF_OPEN
                else self._apply_publication_window_filter(clean_df, window_plan, target_date)
            )
            if clean_df.is_empty():
                # Two routes land here, not one. CLOSED (Elexon, D-5): the
                # trim is refused rather than emptying an otherwise
                # non-empty frame, so an empty frame here means the
                # ORIGINAL transform() output was already empty of the
                # filter dimension's rows. HALF_OPEN (ENTSO-E): D-5's
                # refusal rule does NOT apply on this path
                # (exclude_out_of_window, TRIM ruling) -- a 100%-out-of-
                # window drop empties a non-empty frame by design
                # (all_dropped=True, already logged ERROR above). Either
                # way, nothing further to write.
                return None

        # Enforce the declared Pydantic schema on the FULL frame, fail-soft
        # (VTA-SCHEMA-01): failures are counted + logged here and surfaced by the
        # CLI as completed_with_warnings — never raised, never dropped (CLAUDE.md
        # hard rule). Validated on the transform() output, before bitemporal
        # columns are stamped (schemas do not declare those). No-op when
        # schema_cls is None (generic/dynamic transformers, incl. ENTSO-G CMP).
        # Accumulates across vintage frames (reset once at run() start).
        self.last_validation_failure_count += self._validate_against_schema(clean_df)
        if vintage_column is not None and vintage_column not in clean_df.columns:
            raise ValueError(
                f"{self.source}/{self.dataset}: transform() dropped the transient "
                f"bronze-vintage column {vintage_column!r}; per-row available_at "
                "cannot be derived. Carry it through the output projection."
            )
        clean_df = self._add_bitemporal_columns(
            clean_df,
            target_date=target_date,
            run_id=run_id,
            available_at=available_at,
            vintage_column=vintage_column,
        )
        if vintage_column is not None:
            clean_df = clean_df.drop(vintage_column)
        if vintage_column is not None and vintage_column in clean_df.columns:
            raise ValueError(
                f"{self.source}/{self.dataset}: the transient bronze-vintage column "
                f"{vintage_column!r} reached the silver write boundary."
            )
        self._write_silver(clean_df, target_date, available_at=available_at)
        return clean_df

    def _resolve_publication_window_plan(self, target_date: date) -> _PublicationWindowPlan | None:
        """Resolve the CURRENT partition's request window for the publication-window filter.

        ``None`` (filtering disabled, no-op) for: any source not in
        ``_PUBLICATION_WINDOW_FILTER_SOURCES``; any out-of-scope/exempt
        dataset (logged ``DEBUG`` — D-7b); or an in-scope dataset whose own
        partition could not be resolved (D-7e: an orphan raw body, an invalid
        sidecar, or an unparseable bound) — counted into
        ``last_partition_filter_unresolved_count`` and logged ``WARNING``
        with the failing reason (D-7b/D-3e), never silently narrowed to
        "filter what we can".

        The ``silver.elexon._publication_window`` import is deliberately
        LOCAL (not module-level): ``silver.elexon.__init__`` eagerly imports
        every Elexon transformer module, each of which does
        ``from gridflow.silver.base import BaseSilverTransformer`` — a
        module-level import here would cycle back into this half-initialised
        module. Deferring the import to call time (after both modules have
        fully loaded) avoids the cycle; ``partition_window`` itself carries
        no such risk and is imported at module level above (R-8's import
        boundary is about ``connectors.elexon``, confined to
        ``_publication_window.py``, not about ``partition_window``, which is
        source-agnostic).
        """
        if self.source not in _PUBLICATION_WINDOW_FILTER_SOURCES:
            return None

        from gridflow.silver.elexon._publication_window import (
            publication_window_column,
            publication_window_params,
        )

        params = publication_window_params(self.dataset)
        if params is None:
            logger.debug(
                "Partition-window filter: %s/%s is out of scope or exempt",
                self.source,
                self.dataset,
            )
            return None
        from_param, to_param = params

        partition_dir = (
            self.bronze_dir
            / str(target_date.year)
            / f"{target_date.month:02d}"
            / f"{target_date.day:02d}"
        )
        window, reason = partition_request_window(
            partition_dir,
            from_param,
            to_param,
            expect_source=self.source,
            expect_dataset=self.dataset,
        )
        if window is None:
            self.last_partition_filter_unresolved_count += 1
            logger.warning(
                "Partition-window filter unresolved for %s/%s on %s (%s); "
                "filtering disabled for this partition (all-or-nothing, D-7e)",
                self.source,
                self.dataset,
                target_date,
                reason,
            )
            return None

        return _PublicationWindowPlan(
            column=publication_window_column(self.dataset),
            window=window,
            from_param=from_param,
            to_param=to_param,
        )

    def _apply_publication_window_filter(
        self,
        df: pl.DataFrame,
        plan: _PublicationWindowPlan,
        target_date: date,
    ) -> pl.DataFrame:
        """Trim ``df`` to ``plan.window`` under a per-instant durability proof (D-3b/D-3d).

        D-7(c): an absent filter column is FAIL-LOUD — a ``ValueError``
        naming the dataset, the column, and where to declare an exemption,
        never a silent no-op filter.

        The upper-bound ownership check (extra IO against the successor
        partition) is short-circuited entirely when no row sits at or after
        ``plan.window.end`` (R-10) — the common case costs nothing. Elexon
        never enforces its lower bound (D-3): ``lower_bound_ownership=None``.
        """
        if plan.column not in df.columns:
            raise ValueError(
                f"{self.source}/{self.dataset}: publication-window filter column "
                f"{plan.column!r} is absent from transform() output. Declare an "
                "override in PUBLICATION_WINDOW_COLUMN or an exemption in "
                "PUBLICATION_WINDOW_EXEMPT (silver/elexon/_publication_window.py)."
            )

        has_boundary_rows = df.filter(pl.col(plan.column) >= plan.window.end).height > 0
        if has_boundary_rows:
            upper_bound_ownership = neighbour_owns(
                self.bronze_dir,
                plan.window.end,
                plan.from_param,
                plan.to_param,
                expect_source=self.source,
                expect_dataset=self.dataset,
            )
        else:
            upper_bound_ownership = OwnershipVerdict(False, WindowReason.NOT_RESOLVED)

        result = filter_frame_to_window(
            df,
            plan.column,
            plan.window,
            upper_bound_ownership=upper_bound_ownership,
            lower_bound_ownership=None,
        )

        self.last_partition_filter_dropped_count += result.dropped
        self.last_partition_filter_unclassified_count += result.unclassified
        self.last_partition_filter_boundary_retained_count += result.boundary_retained

        if result.refused:
            logger.error(
                "Partition-window filter would drop the entire %s/%s frame for "
                "%s; keeping every row instead (D-5 refusal)",
                self.source,
                self.dataset,
                target_date,
            )
        if result.dropped:
            logger.warning(
                "Partition-window filter dropped %d row(s) for %s/%s on %s: a "
                "durable covering chunk in the successor partition owns the "
                "boundary instant",
                result.dropped,
                self.source,
                self.dataset,
                target_date,
            )
        if result.unclassified:
            logger.warning(
                "Partition-window filter: %d row(s) with an unparseable/null %s "
                "could not be classified and were kept for %s/%s on %s",
                result.unclassified,
                plan.column,
                self.source,
                self.dataset,
                target_date,
            )
        if result.below_window:
            logger.warning(
                "Partition-window filter: %d row(s) below the window start were "
                "kept for %s/%s on %s (the lower bound is not enforced for this "
                "source, D-3)",
                result.below_window,
                self.source,
                self.dataset,
                target_date,
            )
        if result.retained_reasons:
            logger.warning(
                "Partition-window filter: %d row(s) retained at an unproven "
                "boundary for %s/%s on %s: %s",
                result.boundary_retained,
                self.source,
                self.dataset,
                target_date,
                result.retained_reasons,
            )

        return result.frame

    def _resolve_event_window_plan(self, target_date: date) -> _PublicationWindowPlan | None:
        """Resolve the CURRENT partition's request window for the ENTSO-E
        event-window filter (Task 4 / F-10; HALF_OPEN semantics, Sol
        ruling 2026-07-26).

        ``None`` (filtering disabled, no-op) when ``EVENT_WINDOW_FILTER`` is
        ``False`` (the default — D-6, opt-in PER TRANSFORMER, unlike
        Elexon's source-scoped constant) or when the CURRENT partition's own
        window could not be resolved (D-7e: an orphan raw body, an invalid
        sidecar, or an unparseable bound) — counted into
        ``last_partition_filter_unresolved_count`` and logged ``WARNING``
        with the failing reason, never silently narrowed to "filter what we
        can". Params are always ``("periodStart", "periodEnd")`` and the
        dimension is always ``timestamp_utc`` (D-1) — no per-dataset
        overrides, unlike Elexon's ``remit`` column override.
        """
        if not self.EVENT_WINDOW_FILTER:
            return None

        from_param, to_param = "periodStart", "periodEnd"
        partition_dir = (
            self.bronze_dir
            / str(target_date.year)
            / f"{target_date.month:02d}"
            / f"{target_date.day:02d}"
        )
        window, reason = partition_request_window(
            partition_dir,
            from_param,
            to_param,
            expect_source=self.source,
            expect_dataset=self.dataset,
        )
        if window is None:
            self.last_partition_filter_unresolved_count += 1
            logger.warning(
                "Event-window filter unresolved for %s/%s on %s (%s); "
                "filtering disabled for this partition (all-or-nothing, D-7e)",
                self.source,
                self.dataset,
                target_date,
                reason,
            )
            return None

        return _PublicationWindowPlan(
            column="timestamp_utc",
            window=window,
            from_param=from_param,
            to_param=to_param,
            interval_semantics=IntervalSemantics.HALF_OPEN,
        )

    def _apply_event_window_filter(
        self,
        df: pl.DataFrame,
        plan: _PublicationWindowPlan,
        target_date: date,
    ) -> pl.DataFrame:
        """Unconditionally exclude rows outside ``plan.window`` — the
        ENTSO-E event-window filter (Task 4 / F-10), HALF_OPEN interval
        semantics (:class:`~gridflow.silver.partition_window.IntervalSemantics`,
        Sol ruling 2026-07-26, amending the R2-A plan's original D-3d).

        ENTSO-E's OWN request is ``[periodStart, periodEnd)``, but the
        vendor may return whole CET/CEST delivery days beyond it (measured,
        ``R2-A-PLAN.md`` S1.4). Those rows were never part of THIS
        partition's request at all — there is no ownership question to
        settle and therefore no neighbour-durability proof to make (unlike
        Elexon's CLOSED-interval boundary row, which genuinely IS requested
        by two adjacent chunks — see :meth:`_apply_publication_window_filter`
        and D-3b, untouched by this ruling). Keeping an out-of-scope row
        would both violate this partition's own declared window and plant a
        duplicate for whenever the owning date is properly ingested; bronze
        remains the immutable source of truth for that row under its own
        correctly-scoped partition, so excluding it here is "do not
        materialise an out-of-scope row in silver", not a deletion of
        anything durable. Always dropped, counted, and logged — never
        silent.

        D-7(c): an absent filter column is FAIL-LOUD, the same contract as
        :meth:`_apply_publication_window_filter`.
        """
        if plan.column not in df.columns:
            raise ValueError(
                f"{self.source}/{self.dataset}: event-window filter column "
                f"{plan.column!r} is absent from transform() output. Declare an "
                "exemption in EVENT_WINDOW_FILTER_EXEMPT "
                "(silver/entsoe/_event_window.py) or unset EVENT_WINDOW_FILTER."
            )

        result = exclude_out_of_window(df, plan.column, plan.window)

        self.last_partition_filter_dropped_count += result.dropped
        self.last_partition_filter_unclassified_count += result.unclassified

        if result.refused:
            logger.error(
                "Event-window filter would drop the entire %s/%s frame for "
                "%s; keeping every row instead (D-5 refusal)",
                self.source,
                self.dataset,
                target_date,
            )
        if result.all_dropped:
            # 100% of a non-empty frame fell outside this partition's own
            # [periodStart, periodEnd) window -- the drop is PERFORMED (TRIM
            # ruling: out-of-scope ENTSO-E rows are always excluded,
            # unconditionally), but this is also the exact signature of a
            # horizon/annual dataset opted into EVENT_WINDOW_FILTER by
            # mistake (D-5's original misclassification concern) -- logged
            # ERROR, loud rather than silent, never retained. A log line is
            # not a safety net (Sol re-review, 2026-07-26): this counter is
            # what turns it into a HARD FAILURE of the whole date-range run
            # (pipeline/runner.py::run_transform), not merely a log a nobody
            # reads.
            self.last_partition_filter_all_dropped_count += result.dropped
            logger.error(
                "Event-window filter excluded 100%% of the %s/%s frame for %s "
                "(%d row(s)): none fall inside the partition's own recorded "
                "[%s, %s) request window (%s). Rows are correctly dropped, not "
                "retained -- but a 100%% drop may indicate this dataset was "
                "opted into EVENT_WINDOW_FILTER by mistake; verify its window "
                "semantics",
                self.source,
                self.dataset,
                target_date,
                result.dropped,
                plan.window.start,
                plan.window.end,
                WindowReason.OUT_OF_REQUEST_SCOPE,
            )
        elif result.dropped:
            logger.warning(
                "Event-window filter excluded %d out-of-scope row(s) for %s/%s "
                "on %s: outside the partition's own recorded [%s, %s) request "
                "window (%s — never requested by this partition; no neighbour "
                "proof needed)",
                result.dropped,
                self.source,
                self.dataset,
                target_date,
                plan.window.start,
                plan.window.end,
                WindowReason.OUT_OF_REQUEST_SCOPE,
            )
        if result.unclassified:
            logger.warning(
                "Event-window filter: %d row(s) with an unparseable/null %s "
                "could not be classified and were kept for %s/%s on %s",
                result.unclassified,
                plan.column,
                self.source,
                self.dataset,
                target_date,
            )

        return result.frame

    def _validate_against_schema(self, df: pl.DataFrame) -> int:
        """Validate every row of the transform output against ``schema_cls``, fail-soft.

        Returns the number of rows that failed Pydantic validation. **Never raises
        and never drops a row**: invalid rows are still written to silver; the
        returned count is the only signal, threaded by the CLI into
        ``PipelineRunTracker.complete_with_warnings`` (the CLAUDE.md hard rule —
        validation failures are logged, counted, and surfaced, never silently
        dropped). A no-op returning ``0`` when ``schema_cls`` is ``None`` (generic/
        dynamic transformers with no fixed contract) or the frame is empty.

        Validates the ``transform()`` output (pre-bitemporal): the schema describes
        exactly those columns, and ``BaseSchema``'s ``extra="ignore"`` means any
        additional columns are tolerated. ``strict=True`` schemas will surface real
        ``Field(ge/le)`` / tz breaches on later (non-first) rows as warnings — that
        is the intended fail-soft behaviour, not an error.
        """
        schema = self.schema_cls
        if schema is None or df.is_empty():
            return 0

        failures = 0
        sample: list[str] = []
        for row in df.iter_rows(named=True):
            try:
                schema.model_validate(row)
            except ValidationError as exc:
                failures += 1
                if len(sample) < _VALIDATION_SAMPLE_LIMIT:
                    sample.append(str(exc).replace("\n", " ")[:300])
            except Exception as exc:  # noqa: BLE001
                # Fail-soft is an ABSOLUTE guarantee — one row must never crash the
                # whole date's transform. Pydantic v2 only wraps ValueError/
                # AssertionError into ValidationError, so a custom field_validator
                # raising e.g. TypeError/KeyError would otherwise escape this method
                # and propagate out of run(). Count it like any other invalid row,
                # but log it LOUDLY with its type so a genuine code bug stays
                # visible (surfaced, never silently swallowed).
                failures += 1
                if len(sample) < _VALIDATION_SAMPLE_LIMIT:
                    sample.append(f"{type(exc).__name__}: {str(exc)[:280]}")
                logger.warning(
                    "Unexpected %s validating %s/%s row against %s: %s",
                    type(exc).__name__,
                    self.source,
                    self.dataset,
                    schema.__name__,
                    exc,
                )

        if failures:
            logger.warning(
                "Schema validation: %d/%d row(s) failed %s for %s/%s "
                "(completed_with_warnings; rows still written). Sample: %s",
                failures,
                len(df),
                schema.__name__,
                self.source,
                self.dataset,
                sample,
            )
        return failures

    def _add_bitemporal_columns(
        self,
        df: pl.DataFrame,
        target_date: date,
        run_id: str,
        available_at: datetime,
        *,
        vintage_column: str | None = None,
    ) -> pl.DataFrame:
        """Add modelling lineage columns before silver output is persisted.

        ``available_at = coalesce(published_at, ingest_time)`` (ADR-025 §3): when
        the transformer emitted a ``published_at`` column (the vendor publication
        vintage), it becomes ``available_at`` per row; rows with a null
        ``published_at`` fall back to the ingest/reingest scalar. Datasets that
        emit no ``published_at`` column keep byte-identical ``available_at``.

        ``vintage_column`` (keyword-only, default ``None``) makes only the
        FALLBACK arm of that coalesce per-row, for ``LOCKSTEP_BRONZE_READ``
        reads: the ingest stamp becomes each row's own bronze file's recorded
        sidecar timestamp instead of a frame-level literal. With the default
        ``None`` the emitted expression is character-for-character today's, so
        every existing caller and every non-opted-in transformer is untouched.
        A non-null vendor ``published_at`` still wins the coalesce either way.

        Raises:
            TypeError: F-19 — ``published_at`` present but not a ``pl.Datetime``
                dtype (e.g. String or Int64, a transformer bug: the rename map
                produced the column but never cast it). ``pl.coalesce`` already
                raises loudly on a tz-naive ``Datetime`` (supertype mismatch
                against the tz-aware ingest scalar); String/Int previously
                passed through *silently*, mistyping ``available_at`` instead
                of raising — same failure class, same loudness, now enforced
                explicitly rather than left to chance.
        """
        if available_at.tzinfo is None:
            available_at = available_at.replace(tzinfo=UTC)
        else:
            available_at = available_at.astimezone(UTC)

        ingest_stamp = (
            pl.col(vintage_column).cast(pl.Datetime("us", "UTC"))
            if vintage_column is not None
            else pl.lit(available_at).cast(pl.Datetime("us", "UTC"))
        )
        # ADR-025 §3: available_at = coalesce(published_at, ingest_time), ROW-WISE.
        # pl.coalesce is per-row, so a mixed-null frame (Elexon publishTime is
        # per-record; a date's ENTSO-E bronze can mix files with and without
        # createdDateTime) falls back to the ingest scalar on null rows rather
        # than writing a null available_at — which gridflow_models' fail-closed
        # availability barrier rejects wholesale. A frame-level column swap would not.
        if "published_at" in df.columns:
            published_dtype = df.schema["published_at"]
            if not isinstance(published_dtype, pl.Datetime):
                # F-19: a String/Int published_at silently passes through
                # pl.coalesce, mistyping available_at instead of failing. Raise
                # explicitly — the tz-naive Datetime case already raises via
                # Polars' own supertype check below; this closes the latent
                # non-Datetime half of the same failure class.
                raise TypeError(
                    f"{self.source}/{self.dataset}: published_at must be a "
                    f"pl.Datetime dtype (tz-aware UTC), got {published_dtype!r}. "
                    "Fix the transformer's published_at emission — do not rely "
                    "on pl.coalesce to catch a mistyped column."
                )
            available_at_expr = pl.coalesce(pl.col("published_at"), ingest_stamp).alias(
                "available_at"
            )
        else:
            available_at_expr = ingest_stamp.alias("available_at")

        return df.with_columns(
            [
                self._event_time_expr(df, target_date),
                available_at_expr,
                pl.lit(run_id).alias("source_run_id"),
                pl.lit(self.DATASET_VERSION).alias("dataset_version"),
            ]
        )

    def _event_time_expr(self, df: pl.DataFrame, target_date: date) -> pl.Expr:
        """Return the expression used for the row's semantic event time."""
        column = self._event_time_column()
        if column in df.columns:
            return pl.col(column).cast(pl.Datetime("us", "UTC")).alias("event_time")

        if {"settlement_date", "settlement_period"}.issubset(df.columns):
            return (
                pl.struct(["settlement_date", "settlement_period"])
                .map_elements(
                    lambda row: settlement_period_to_utc(
                        row["settlement_date"],
                        row["settlement_period"],
                    ),
                    return_dtype=pl.Datetime("us", "UTC"),
                )
                .alias("event_time")
            )

        logger.debug(
            "Falling back to target-date event_time for %s/%s on %s",
            self.source,
            self.dataset,
            target_date,
        )
        return (
            pl.lit(datetime(target_date.year, target_date.month, target_date.day, tzinfo=UTC))
            .cast(pl.Datetime("us", "UTC"))
            .alias("event_time")
        )

    def _event_time_column(self) -> str:
        """Name of the transform output column that represents event time."""
        return "timestamp_utc"

    def _available_at_from_bronze(self, target_date: date) -> datetime:
        """Reconstruct historical availability from bronze sidecar metadata."""
        timestamps: list[datetime] = []
        for date_dir in self._bronze_date_dirs(target_date):
            for meta_path in sorted(date_dir.glob("raw_*.meta.json")):
                timestamp = self._timestamp_from_sidecar(meta_path)
                if timestamp is not None:
                    timestamps.append(timestamp)

        if timestamps:
            return max(timestamps)

        fallback = datetime.now(UTC)
        logger.warning(
            "No bronze sidecar timestamp found for %s/%s on %s; using %s",
            self.source,
            self.dataset,
            target_date,
            fallback.isoformat(),
        )
        return fallback

    def _bronze_date_dirs(self, target_date: date) -> list[Path]:
        """Candidate bronze date directories for this dataset/date."""
        suffix = Path(str(target_date.year)) / f"{target_date.month:02d}" / f"{target_date.day:02d}"
        candidates = [self.bronze_dir / suffix]

        # Some aggregate transformers read from explicit sibling partitions, e.g.
        # open_meteo/historical reads bronze/open_meteo/historical_london.
        parent = self.bronze_dir.parent
        if parent.exists():
            for sibling_dataset in self.BRONZE_SIBLING_DATASETS:
                candidates.append(parent / sibling_dataset / suffix)

        existing = [p for p in candidates if p.exists()]
        if not existing and self.source not in _EXACT_PARTITION_ONLY_SOURCES:
            # No exact-date partition found; fall back to the nearest covering
            # partition so that transformers that iterate _bronze_date_dirs()
            # also benefit from the Variant A/B fix. Skipped for exact-only
            # sources (P0.8) — see _EXACT_PARTITION_ONLY_SOURCES docstring.
            fallback = self._find_covering_bronze_partition(target_date)
            if fallback is not None:
                return [fallback]
        return [path for path in candidates if path.exists()]

    def _find_covering_bronze_partition(
        self,
        target_date: date,
        max_lookback_days: int = 35,
        *,
        bronze_dir: Path | None = None,
    ) -> Path | None:
        """Return the nearest prior bronze partition likely to contain data for target_date.

        Used when the connector batched multiple days into one fetch and stored
        all files under the window-start date rather than the target date.
        Scans back up to max_lookback_days looking for any partition that has raw files.
        Returns None if nothing found within the window.

        Pass ``bronze_dir`` to search a location-specific directory instead of
        ``self.bronze_dir`` (used by multi-location transformers such as openmeteo).
        """
        base = bronze_dir if bronze_dir is not None else self.bronze_dir
        for delta in range(1, max_lookback_days + 1):
            candidate_date = target_date - timedelta(days=delta)
            candidate_path = (
                base
                / str(candidate_date.year)
                / f"{candidate_date.month:02d}"
                / f"{candidate_date.day:02d}"
            )
            if candidate_path.exists() and any(candidate_path.glob("raw_*")):
                return candidate_path
        return None

    def _bronze_path_for_date(
        self,
        target_date: date,
        max_lookback_days: int = 35,
    ) -> Path | None:
        """Return the bronze partition path to read for target_date.

        Returns exact partition if it exists and has raw files, falls back to
        the nearest covering partition, or None if nothing found. For sources
        in ``_EXACT_PARTITION_ONLY_SOURCES`` (P0.8), never falls back — the
        exact partition or nothing, since a covering-fallback hit for those
        sources would fabricate wrong-day rows (see that constant's docstring).
        """
        exact = (
            self.bronze_dir
            / str(target_date.year)
            / f"{target_date.month:02d}"
            / f"{target_date.day:02d}"
        )
        if exact.exists() and any(exact.glob("raw_*")):
            return exact
        if self.source in _EXACT_PARTITION_ONLY_SOURCES:
            return None
        return self._find_covering_bronze_partition(target_date, max_lookback_days)

    def _resolve_vouched_bronze_set(
        self,
        candidates: Sequence[Path],
        selection: BronzeReadSelection,
    ) -> VouchedBronzeSet:
        """Classify an already-scanned candidate list into vouched/unvouched.

        Every clause of this contract is load-bearing (R2-g D-2):

        1. ``candidates`` is an already-ordered list produced by exactly ONE
           filesystem scan, supplied by the caller. This method performs **no
           globbing at all** -- a second scanning surface here would reopen
           precisely the read-path/vintage-path disagreement it exists to close.
        2. For each candidate examined, the sidecar is read **exactly once**
           via the pure classifier, and the stamp is stored IN the returned
           value. Nothing downstream re-reads a sidecar.
        3. ``ALL`` examines every candidate. ``NEWEST_VOUCHED`` walks in order
           and stops at the first vouched one, so ``unvouched`` then holds
           exactly those STEPPED OVER, and ``examined`` records how many were
           probed.
        4. Emits nothing above ``DEBUG``. All aggregation is the caller's job.
        5. Pure with respect to its inputs and the on-disk sidecar bytes -- no
           clock, no ``now()``, no filesystem write.

        Args:
            candidates: Body paths, in the caller's own selection order.
            selection: Which vouched bodies to consume.

        Returns:
            The :class:`VouchedBronzeSet` threaded to both derivations.
        """
        match selection:
            case BronzeReadSelection.ALL:
                stop_at_first_vouched = False
            case BronzeReadSelection.NEWEST_VOUCHED:
                stop_at_first_vouched = True
            case _:  # pragma: no cover - exhaustiveness guard (mypy does not check enum matches)
                assert_never(selection)

        entries: list[tuple[Path, datetime]] = []
        unvouched: list[tuple[Path, BronzeVouchReason]] = []
        examined = 0
        for candidate in candidates:
            examined += 1
            read = self._read_sidecar_timestamp(candidate.with_suffix(".meta.json"))
            if read.timestamp is not None:
                entries.append((candidate, read.timestamp))
                if stop_at_first_vouched:
                    break
                continue
            if read.reason is None:  # pragma: no cover - SidecarRead's own invariant
                raise ValueError(
                    f"SidecarRead for {candidate} has neither a timestamp nor a reason"
                )
            unvouched.append((candidate, read.reason))

        logger.debug(
            "Vouched bronze for %s/%s: %d of %d examined candidate(s) usable",
            self.source,
            self.dataset,
            len(entries),
            examined,
        )
        return VouchedBronzeSet(
            entries=tuple(entries),
            unvouched=tuple(unvouched),
            examined=examined,
        )

    @staticmethod
    def _read_sidecar_timestamp(meta_path: Path) -> SidecarRead:
        """Classify one bronze sidecar. PURE (R2-g D-7).

        No logging at any level, no clock, no globbing, no filesystem write --
        every emission decision belongs to the caller (D-8's layering), so the
        lockstep read path can aggregate one bounded record per
        ``run_transform`` invocation instead of one WARNING per file.

        Hardened beyond :meth:`_timestamp_from_sidecar` in exactly one named
        place: syntactically valid NON-OBJECT JSON is classified
        ``UNREADABLE_SIDECAR`` here, where master raises ``AttributeError``
        (N-18). That divergence is deliberate and confined to this
        lockstep-only classifier -- the wrapper keeps master's crash, because
        softening it would let the non-lockstep callers stamp a frame from a
        SIBLING file's timestamp or fall through to ``now()``.

        Args:
            meta_path: The sidecar path (``<body stem>.meta.json``).

        Returns:
            A :class:`SidecarRead` whose ``reason`` is ``None`` iff the file
            vouched.
        """
        try:
            meta = json.loads(meta_path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            # An ABSENT sidecar is the literal orphan (a crash between the
            # writer's body write and its sidecar write) and is reported as
            # such; a sidecar that exists but cannot be read is a different
            # remediation story, so the aggregate must be able to say which.
            file_level_reason = (
                BronzeVouchReason.NO_SIDECAR
                if isinstance(exc, FileNotFoundError)
                else BronzeVouchReason.UNREADABLE_SIDECAR
            )
            return SidecarRead(
                timestamp=None,
                reason=file_level_reason,
                diagnostics=(
                    SidecarDiagnostic(key=None, reason=file_level_reason, detail=str(exc)),
                ),
            )

        if not isinstance(meta, dict):
            return SidecarRead(
                timestamp=None,
                reason=BronzeVouchReason.UNREADABLE_SIDECAR,
                non_object_json=True,
                payload=meta,
            )

        diagnostics: list[SidecarDiagnostic] = []
        reason = BronzeVouchReason.NO_TIMESTAMP_KEY
        for key in _SIDECAR_TIMESTAMP_KEYS:
            raw_value = meta.get(key)
            if not raw_value:
                continue
            timestamp, loggable_failure = BaseSilverTransformer._coerce_sidecar_timestamp(raw_value)
            if timestamp is not None:
                return SidecarRead(timestamp=timestamp, reason=None, diagnostics=tuple(diagnostics))
            # Fall through to the next key, exactly as master does: a present
            # but unparseable key must not exclude a file master would read.
            reason = BronzeVouchReason.UNPARSEABLE_TIMESTAMP
            diagnostics.append(
                SidecarDiagnostic(
                    key=key,
                    reason=BronzeVouchReason.UNPARSEABLE_TIMESTAMP,
                    detail=str(raw_value) if loggable_failure else None,
                )
            )
        return SidecarRead(timestamp=None, reason=reason, diagnostics=tuple(diagnostics))

    @staticmethod
    def _timestamp_from_sidecar(meta_path: Path) -> datetime | None:
        """Extract the most useful timestamp field from a bronze sidecar.

        UNCHANGED CONTRACT (R2-g D-7): a thin logging wrapper over
        :meth:`_read_sidecar_timestamp`. Same return value, same WARNING text
        at the same two conditions, in the same order -- and, deliberately,
        master's uncaught ``AttributeError`` on non-object JSON (N-18).
        ``VINTAGE_PER_BRONZE_FILE`` and :meth:`_available_at_from_bronze` call
        this for EVERY source, none of which opts into lockstep reads, so any
        softening here would convert a loud fail-closed crash into fabricated
        provenance or missing rows outside ENTSO-G.
        """
        read = BaseSilverTransformer._read_sidecar_timestamp(meta_path)
        for diagnostic in read.diagnostics:
            if diagnostic.detail is None:
                # Master's `_parse_timestamp` is silent for a present-but-
                # non-string, non-datetime value; replaying nothing is correct.
                continue
            if diagnostic.key is None:
                logger.warning(
                    "Failed to parse bronze sidecar %s: %s", meta_path, diagnostic.detail
                )
            else:
                logger.warning("Could not parse bronze sidecar timestamp: %s", diagnostic.detail)
        if read.non_object_json:
            # N-18: master lands on `meta.get(key)` with a non-dict `meta` and
            # raises. Reproduced through the payload itself so the exception
            # type and message stay master's, byte for byte.
            payload: Any = read.payload
            payload.get(_SIDECAR_TIMESTAMP_KEYS[0])
        return read.timestamp

    @staticmethod
    def _coerce_sidecar_timestamp(raw_value: object) -> tuple[datetime | None, bool]:
        """Pure core of :meth:`_parse_timestamp` -- identical logic, no logging.

        Args:
            raw_value: The sidecar value to coerce.

        Returns:
            ``(timestamp, loggable_failure)``. ``loggable_failure`` is ``True``
            only for the single case master warns about: a STRING that
            :meth:`datetime.fromisoformat` rejects. A non-string,
            non-datetime value fails silently on master, so it yields
            ``(None, False)`` and the wrapper replays no warning for it.
        """
        if isinstance(raw_value, datetime):
            value = raw_value
        elif isinstance(raw_value, str):
            normalized = raw_value.replace("Z", "+00:00")
            try:
                value = datetime.fromisoformat(normalized)
            except ValueError:
                return None, True
        else:
            return None, False

        if value.tzinfo is None:
            return value.replace(tzinfo=UTC), False
        return value.astimezone(UTC), False

    @staticmethod
    def _parse_timestamp(raw_value: object) -> datetime | None:
        value, loggable_failure = BaseSilverTransformer._coerce_sidecar_timestamp(raw_value)
        if value is None and loggable_failure:
            logger.warning("Could not parse bronze sidecar timestamp: %s", raw_value)
        return value

    def _write_silver(
        self,
        df: pl.DataFrame,
        target_date: date,
        available_at: datetime,
    ) -> None:
        """Write DataFrame to partitioned Parquet.

        Default branch (``APPEND_ONLY = False``) writes a single file per
        ``(dataset, target_date)`` and overwrites it on each run via
        :func:`write_parquet`'s atomic temp-then-rename. The ``APPEND_ONLY =
        True`` branch suffixes the filename with the ISO ``available_at``
        timestamp so re-ingest with a sidecar-derived ``available_at`` is
        idempotent (two reingest passes produce the same path and the second
        cleanly replaces the first), while distinct live runs produce
        distinct files. See ``docs/DECISION_LOG/ADR-018``.
        """
        out_dir = self.silver_dir / f"year={target_date.year}" / f"month={target_date.month:02d}"
        if self.APPEND_ONLY:
            run_stamp = available_at.isoformat().replace(":", "-").replace("+", "-")
            filename = f"{self.dataset}_{target_date.strftime('%Y%m%d')}_run{run_stamp}.parquet"
        else:
            filename = f"{self.dataset}_{target_date.strftime('%Y%m%d')}.parquet"
        final_path = out_dir / filename
        write_parquet(df, final_path)

    def _write_csv(self, df: pl.DataFrame, target_date: date) -> None:
        """Write DataFrame to CSV at data/silver/{source}/{dataset}/{dataset}_{YYYYMMDD}.csv."""
        csv_dir = self.silver_dir
        csv_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{self.dataset}_{target_date.strftime('%Y%m%d')}.csv"
        final_path = csv_dir / filename
        tmp_path = csv_dir / f".tmp_{filename}"
        df.write_csv(tmp_path)
        os.replace(tmp_path, final_path)
        logger.debug(f"Wrote CSV: {len(df)} rows to {final_path}")
