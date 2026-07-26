"""Sidecar request-window filter primitive (R2-A: F-04 / F-10 remediation).

An Elexon ``PUBLISH_DATETIME`` bronze partition is a **publication** window,
not a settlement day: consecutive request chunks share their boundary
instant and the vendor treats each chunk as CLOSED at both ends, so the
boundary instant is written into BOTH the current and the successor
partition (F-04). A naive settlement-date-keyed filter would delete SP1 of
every settlement day (see the plan's ``1.2`` measurement) — the fix here is
narrower and safer: trim a row at a window bound ONLY when a neighbour
partition is PROVEN to hold that row's own instant durably.

Design decisions this module implements (``R2-A-PLAN.md`` `S2`):

- **D-3b — neighbour-ownership gate, with a durability proof.** A row is
  trimmed at a window bound only when a neighbour partition is proven to
  hold that row's own instant durably — never on the promise of a future
  ingest, never on the inference that a completed fetch implies a durable
  partition. :func:`covering_chunk_is_durable` proves EXISTENCE of one
  durable covering response (page-complete, fully paired) — not
  whole-partition completeness, which would need an ingest-layer change
  (T3). Chunk-scoped by design: reducing the proof from "the whole
  partition materialised" to "one covering chunk materialised" is what
  keeps this a T2 change (see the plan's D-3b for the full argument).
- **D-3d — per-instant ownership for the lower bound.** ``day_subwindows``
  (``utils/time.py``) clamps sub-windows at the overall range edges, so a
  predecessor partition can hold only part of a UTC day. Two below-bound
  instants on the SAME UTC date can therefore have different ownership
  results — ownership is resolved per distinct instant, memoized by
  covering-chunk identity, never collapsed to a per-date set.
- **D-3e — ownership carries its reason.** :class:`OwnershipVerdict` always
  carries the failing :class:`WindowReason`, threaded by the caller into
  :attr:`WindowFilterResult.retained_reasons` and logged — a bare boolean
  cannot diagnose a torn ingest (``INCOMPLETE_PAGE_SET`` must be visible).
- **D-7(e) — all-or-nothing partition aggregation + raw<->sidecar pairing.**
  For the partition being FILTERED, every raw body must have a validated
  sidecar or filtering is disabled for the entire partition (counted,
  logged, never silently narrowed). This is a scope contrast with the
  neighbour's chunk-scoped proof, deliberate: the partition being filtered
  contributes every raw body to the output frame regardless of sidecar
  presence, so an orphan body there means the true window cannot be known;
  the covering chunk in a neighbour only needs an EXISTENCE proof, so an
  orphan body in a non-covering chunk of the neighbour must not block
  ownership.
- **D-5 — a filter may never empty a frame.** Dropping 100% of a non-empty
  frame refuses the trim entirely (kept, logged ``ERROR``).

This module is IO-free at its lowest level (:func:`filter_frame_to_window`)
and does no logging anywhere — callers own logging and the D-7(c) fail-loud
check for an absent filter column, since only they know the full context
(dataset, source, target date) worth logging.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

logger = logging.getLogger(__name__)


class WindowReason(StrEnum):
    """Why a row's window bound was or was not enforced."""

    OK = "OK"
    NO_SIDECAR = "NO_SIDECAR"
    NO_REQUEST_PARAMS = "NO_REQUEST_PARAMS"
    MISSING_PARAM = "MISSING_PARAM"
    UNPARSEABLE_BOUND = "UNPARSEABLE_BOUND"
    NON_POSITIVE_RANGE = "NON_POSITIVE_RANGE"
    IDENTITY_MISMATCH = "IDENTITY_MISMATCH"
    ORPHAN_BODY = "ORPHAN_BODY"
    INCOMPLETE_PAGE_SET = "INCOMPLETE_PAGE_SET"
    NO_COVERING_CHUNK = "NO_COVERING_CHUNK"
    NOT_RESOLVED = "NOT_RESOLVED"
    OUT_OF_REQUEST_SCOPE = "OUT_OF_REQUEST_SCOPE"
    """A row outside a HALF_OPEN vendor interval (Sol ruling, 2026-07-26) --
    never requested by this partition at all, so there is no ownership
    question to resolve; it is unconditionally excluded. Contrast with the
    CLOSED-interval reasons above, which all describe why a genuinely
    REQUESTED boundary row's removal could or could not be proven safe."""


class IntervalSemantics(StrEnum):
    """The vendor's OWN request-interval semantics for a dataset family --
    the property that decides whether a boundary/out-of-window row needs a
    neighbour-durability proof at all (Sol ruling, 2026-07-26, amending the
    R2-A plan's D-3d).

    This is a property of how the VENDOR interprets ``[from, to]`` /
    ``[from, to)``, never of a source name -- so a future connector inherits
    the correct behaviour automatically by declaring which interval its
    vendor uses, without anyone re-deriving this argument.

    - ``CLOSED``: the vendor treats the request window as closed at BOTH
      ends (Elexon's ``publishDateTimeFrom``/``To`` -- ``R2-A-PLAN.md``
      S1.1). The row AT the boundary is genuinely PART of the request and
      is legitimately returned by two adjacent chunks -- ownership is
      shared, so removing it from one side requires D-3b's
      neighbour-durability proof (:func:`filter_frame_to_window`). Retained,
      never dropped, when that proof cannot be made -- duplication is
      preferred to loss.
    - ``HALF_OPEN``: the vendor's OWN request is ``[from, to)`` (ENTSO-E's
      ``periodStart``/``periodEnd``), but the vendor may return rows beyond
      it (ENTSO-E's measured CET/CEST delivery-day over-span, S1.4). Those
      rows were never part of THIS partition's request in the first place --
      there is no ownership to resolve, no neighbour proof needed, and
      keeping one in silver both violates the partition's own declared scope
      and plants a duplicate for whenever the owning date is properly
      ingested. Unconditionally excluded (:func:`exclude_out_of_window`),
      counted, and logged -- never silent.
    """

    CLOSED = "closed"
    HALF_OPEN = "half_open"


@dataclass(frozen=True)
class OwnershipVerdict:
    """Outcome of a durability proof (D-3b) for one instant.

    ``reason`` is ``WindowReason.OK`` when ``owned`` is True, and the
    specific failing reason otherwise (D-3e) — never a bare boolean, so a
    torn ingest (``INCOMPLETE_PAGE_SET``) is diagnosable from the logs.
    """

    owned: bool
    reason: WindowReason


@dataclass(frozen=True)
class RequestWindow:
    """A resolved ``[start, end)`` request window, tz-aware UTC."""

    start: datetime
    end: datetime
    param_names: tuple[str, str]


@dataclass(frozen=True)
class WindowFilterResult:
    """Outcome of applying :func:`filter_frame_to_window` to one frame."""

    frame: pl.DataFrame
    dropped: int
    unclassified: int
    below_window: int
    boundary_retained: int
    """Rows for which enforcement was ATTEMPTED (an ownership check was
    made against a neighbour partition) but the row was kept because
    ownership could not be proven. Equal to ``sum(n for _, n in
    retained_reasons)`` by construction. Does NOT include below-window rows
    when the caller passes ``lower_bound_ownership=None`` (D-3: Elexon
    deliberately never attempts to enforce its lower bound at all) — those
    are counted only in ``below_window``.
    """
    retained_reasons: tuple[tuple[WindowReason, int], ...]
    refused: bool
    all_dropped: bool = False
    """``True`` only on the HALF_OPEN path (:func:`exclude_out_of_window`)
    when every row in a non-empty frame fell outside the partition's own
    recorded window. Unlike ``refused`` (CLOSED/D-5: the drop is refused and
    the frame kept unchanged), the HALF_OPEN drop-all case still PERFORMS
    the drop -- ``frame`` is empty, ``dropped == `` the original row count --
    because a wholly out-of-scope response was never part of this
    partition's request at all (no ownership to preserve by retaining it).
    The flag exists purely so the caller can log this loudly (ERROR, not the
    usual per-row WARNING): a 100% drop is the exact signature of a horizon
    dataset opted into the filter by mistake (D-5's original misclassification
    concern), so it must be visible even though the rows are correctly
    excluded. Always ``False`` on the CLOSED path
    (:func:`filter_frame_to_window`)."""


def _parse_bound(raw_value: object) -> datetime | None:
    """Parse a request-param timestamp value into a tz-aware UTC datetime.

    Accepts both Elexon's ``publishDateTimeFrom/To`` shape
    (``2026-07-11T00:00:00Z``, via :func:`connectors.elexon.endpoints._to_utc_z`)
    and ENTSO-E's compact ``periodStart/End`` shape (``202401150600``,
    ``ENTSOE_DT_FORMAT``). Returns ``None`` — never raises — on anything else,
    so a corrupt or unexpected sidecar value becomes ``UNPARSEABLE_BOUND``
    (D-7a), not a crash.
    """
    if isinstance(raw_value, datetime):
        return raw_value if raw_value.tzinfo else raw_value.replace(tzinfo=None)
    if not isinstance(raw_value, str):
        return None

    normalized = raw_value.replace("Z", "+00:00")
    try:
        value = datetime.fromisoformat(normalized)
    except ValueError:
        pass
    else:
        return value if value.tzinfo else value.replace(tzinfo=None)

    # ENTSO-E's ENTSOE_DT_FORMAT = "%Y%m%d%H%M" (no separators, naive).
    try:
        value = datetime.strptime(raw_value, "%Y%m%d%H%M")
    except ValueError:
        return None
    from datetime import UTC

    return value.replace(tzinfo=UTC)


def _load_sidecar(meta_path: Path) -> dict[str, object] | None:
    try:
        data = json.loads(meta_path.read_text())
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    return data if isinstance(data, dict) else None


def request_window_from_sidecar(
    meta_path: Path,
    from_param: str,
    to_param: str,
    *,
    expect_source: str,
    expect_dataset: str,
) -> tuple[RequestWindow | None, WindowReason]:
    """Resolve and validate one sidecar's recorded request window (D-7a).

    Validates: the sidecar parses as JSON, ``source``/``dataset`` match the
    caller's expectation, ``request_params`` is present and non-empty, both
    ``from_param``/``to_param`` are present and parseable, and
    ``start < end`` (UTC-normalised). Failure returns ``(None,
    <WindowReason>)`` — never raises.
    """
    meta = _load_sidecar(meta_path)
    if meta is None:
        return None, WindowReason.NO_SIDECAR

    if meta.get("source") != expect_source or meta.get("dataset") != expect_dataset:
        return None, WindowReason.IDENTITY_MISMATCH

    params = meta.get("request_params")
    if not isinstance(params, dict) or not params:
        return None, WindowReason.NO_REQUEST_PARAMS

    if from_param not in params or to_param not in params:
        return None, WindowReason.MISSING_PARAM

    start = _parse_bound(params[from_param])
    end = _parse_bound(params[to_param])
    if start is None or end is None:
        return None, WindowReason.UNPARSEABLE_BOUND

    if start.tzinfo is None or end.tzinfo is None:
        return None, WindowReason.UNPARSEABLE_BOUND

    if not start < end:
        return None, WindowReason.NON_POSITIVE_RANGE

    return RequestWindow(start=start, end=end, param_names=(from_param, to_param)), WindowReason.OK


def _raw_bodies(partition_dir: Path) -> list[Path]:
    if not partition_dir.exists():
        return []
    return sorted(p for p in partition_dir.glob("raw_*") if not p.name.endswith(".meta.json"))


def partition_request_window(
    partition_dir: Path,
    from_param: str,
    to_param: str,
    *,
    expect_source: str,
    expect_dataset: str,
) -> tuple[RequestWindow | None, WindowReason]:
    """Resolve the request window for the partition being FILTERED (D-7e).

    ALL-OR-NOTHING: every raw body (``raw_*``, excluding ``*.meta.json``)
    must have a sidecar that passes :func:`request_window_from_sidecar`.
    Any missing or invalid member disables filtering for the ENTIRE
    partition — ``(None, ORPHAN_BODY)`` for a body with no sidecar at all,
    or ``(None, <the member's own failing reason>)`` otherwise. This is a
    partition-wide rule (contrast with the neighbour's chunk-scoped proof in
    :func:`covering_chunk_is_durable`) because every raw body here
    contributes rows to the output frame regardless of sidecar presence.

    On success, the window is ``(min(start), max(end))`` across every valid
    sidecar (D-4) — ``max`` on the upper bound is the loss-avoiding choice.
    """
    raw_bodies = _raw_bodies(partition_dir)
    if not raw_bodies:
        return None, WindowReason.NO_SIDECAR

    windows: list[RequestWindow] = []
    for raw_path in raw_bodies:
        meta_path = raw_path.with_suffix(".meta.json")
        if not meta_path.exists():
            return None, WindowReason.ORPHAN_BODY
        window, reason = request_window_from_sidecar(
            meta_path,
            from_param,
            to_param,
            expect_source=expect_source,
            expect_dataset=expect_dataset,
        )
        if window is None:
            return None, reason
        windows.append(window)

    start = min(w.start for w in windows)
    end = max(w.end for w in windows)
    return RequestWindow(start=start, end=end, param_names=(from_param, to_param)), WindowReason.OK


def covering_chunk_is_durable(
    partition_dir: Path,
    instant: datetime,
    from_param: str,
    to_param: str,
    *,
    expect_source: str,
    expect_dataset: str,
) -> OwnershipVerdict:
    """D-3b's durability proof: does a durable chunk in ``partition_dir`` cover ``instant``?

    Groups every VALID sidecar in ``partition_dir`` by its recorded
    ``(start, end)`` window, takes the group whose window covers ``instant``
    (``start <= instant < end``), and returns ``owned=True`` iff that group
    is BOTH:

    1. fully paired — every member's raw body is present on disk, and
    2. page-complete — the ``page`` values present are exactly
       ``1..max(total_pages)`` declared within that group.

    CHUNK-SCOPED by design (D-3b): this is an EXISTENCE proof — some durable
    response set covering the instant exists — not a whole-partition
    completeness claim. An orphan body in a NON-covering chunk of this
    partition must not block ownership: sidecars are discovered by scanning
    ``raw_*.meta.json`` (not raw bodies), so a body with no sidecar in a
    different window group never enters any group and cannot affect this
    verdict. Any of: no covering chunk, a missing page, an unpaired member,
    or an absent/non-int ``page``/``total_pages`` (legacy bronze) returns
    ``owned=False`` with the failing reason — never raises.
    """
    if not partition_dir.exists():
        return OwnershipVerdict(False, WindowReason.NO_COVERING_CHUNK)

    groups: dict[tuple[datetime, datetime], list[tuple[object, object, bool]]] = {}
    for meta_path in sorted(partition_dir.glob("raw_*.meta.json")):
        window, reason = request_window_from_sidecar(
            meta_path,
            from_param,
            to_param,
            expect_source=expect_source,
            expect_dataset=expect_dataset,
        )
        if window is None:
            continue  # An invalid sidecar contributes to no group (safe: it
            # can only ever cause an otherwise-covering group to look
            # incomplete, never fabricate a false covering match).

        meta = _load_sidecar(meta_path)
        if meta is None:
            continue
        page = meta.get("page")
        total_pages = meta.get("total_pages")

        stem = meta_path.name[: -len(".meta.json")]
        has_body = any(
            candidate.name != meta_path.name for candidate in meta_path.parent.glob(f"{stem}.*")
        )

        key = (window.start, window.end)
        groups.setdefault(key, []).append((page, total_pages, has_body))

    covering_key = next((key for key in groups if key[0] <= instant < key[1]), None)
    if covering_key is None:
        return OwnershipVerdict(False, WindowReason.NO_COVERING_CHUNK)

    members = groups[covering_key]

    if any(not has_body for _, _, has_body in members):
        return OwnershipVerdict(False, WindowReason.ORPHAN_BODY)

    validated_members: list[tuple[int, int, bool]] = []
    for page, total_pages, has_body in members:
        if not isinstance(page, int) or not isinstance(total_pages, int):
            # Legacy bronze with no usable pagination metadata cannot be
            # proven complete — the safe direction is to withhold the proof.
            return OwnershipVerdict(False, WindowReason.INCOMPLETE_PAGE_SET)
        validated_members.append((page, total_pages, has_body))

    max_total = max(total_pages for _, total_pages, _ in validated_members)
    pages_present = {page for page, _, _ in validated_members}
    if pages_present != set(range(1, max_total + 1)):
        return OwnershipVerdict(False, WindowReason.INCOMPLETE_PAGE_SET)

    return OwnershipVerdict(True, WindowReason.OK)


def neighbour_owns(
    bronze_dir: Path,
    instant: datetime,
    from_param: str,
    to_param: str,
    *,
    expect_source: str,
    expect_dataset: str,
) -> OwnershipVerdict:
    """``covering_chunk_is_durable()`` against the partition dir for ``instant.date()``.

    Resolves the bronze partition directory for the calendar date the
    instant's UTC date falls on and delegates the proof. Used symmetrically
    for both call sites (D-3c): Elexon's upper-bound trim (``instant ==
    window.end``, neighbour = the successor partition) and ENTSO-E's
    lower-bound trim (``instant`` = a below-window row's own timestamp,
    neighbour = the predecessor partition).
    """
    partition_dir = bronze_dir / str(instant.year) / f"{instant.month:02d}" / f"{instant.day:02d}"
    return covering_chunk_is_durable(
        partition_dir,
        instant,
        from_param,
        to_param,
        expect_source=expect_source,
        expect_dataset=expect_dataset,
    )


def filter_frame_to_window(
    df: pl.DataFrame,
    column: str,
    window: RequestWindow,
    *,
    upper_bound_ownership: OwnershipVerdict,
    lower_bound_ownership: Mapping[datetime, OwnershipVerdict] | None = None,
) -> WindowFilterResult:
    """Trim ``df`` to ``window`` under a per-instant durability proof.

    IO-free and does no logging (callers own that). Absent ``column`` -> the
    frame is returned unchanged (the CALLER fails loud per D-7c; this
    primitive does not raise).

    - **Upper bound** (``column >= window.end``): a single verdict, since
      only one instant (``window.end``) can sit there. Enforced iff
      ``upper_bound_ownership.owned``; otherwise every such row is retained
      and counted under its failing reason.
    - **Lower bound** (``column < window.start``): resolved PER ROW's own
      instant (D-3d) — never collapsed to a per-date set. When
      ``lower_bound_ownership`` is ``None`` (Elexon, D-3: the lower bound is
      never enforced at all), every below-window row is counted into
      ``below_window`` and kept, but NOT attributed as a proof failure
      (nothing was attempted). When a mapping is supplied, a row whose own
      instant is absent from it is treated as ``NOT_RESOLVED`` and retained.
    - **D-5**: if trimming would empty an otherwise non-empty frame, the
      trim is refused entirely (``refused=True``, frame unchanged) — the
      caller logs this as an ERROR.
    """
    if column not in df.columns or df.is_empty():
        return WindowFilterResult(
            frame=df,
            dropped=0,
            unclassified=0,
            below_window=0,
            boundary_retained=0,
            retained_reasons=(),
            refused=False,
        )

    total = len(df)
    values = df[column]
    is_null = values.is_null()
    unclassified = int(is_null.sum())

    below_start = (values < window.start).fill_null(False)
    at_or_after_end = (values >= window.end).fill_null(False)

    reason_totals: dict[WindowReason, int] = {}

    n_at_end = int(at_or_after_end.sum())
    if n_at_end > 0 and upper_bound_ownership.owned:
        drop_upper = at_or_after_end
    else:
        drop_upper = pl.Series([False] * total)
        if n_at_end > 0:
            reason_totals[upper_bound_ownership.reason] = (
                reason_totals.get(upper_bound_ownership.reason, 0) + n_at_end
            )

    n_below = int(below_start.sum())
    drop_lower_flags = [False] * total
    if n_below > 0 and lower_bound_ownership is not None:
        column_values = values.to_list()
        below_flags = below_start.to_list()
        for idx in range(total):
            if not below_flags[idx]:
                continue
            verdict = lower_bound_ownership.get(column_values[idx])
            if verdict is None:
                verdict = OwnershipVerdict(False, WindowReason.NOT_RESOLVED)
            if verdict.owned:
                drop_lower_flags[idx] = True
            else:
                reason_totals[verdict.reason] = reason_totals.get(verdict.reason, 0) + 1
    drop_lower = pl.Series(drop_lower_flags)

    drop_mask = drop_upper | drop_lower
    dropped = int(drop_mask.sum())
    boundary_retained = sum(reason_totals.values())
    retained_reasons = tuple(sorted(reason_totals.items(), key=lambda kv: kv[0].value))

    if dropped > 0 and dropped == total:
        # D-5: never empty an otherwise non-empty frame.
        return WindowFilterResult(
            frame=df,
            dropped=0,
            unclassified=unclassified,
            below_window=n_below,
            boundary_retained=boundary_retained,
            retained_reasons=retained_reasons,
            refused=True,
        )

    kept_frame = df.filter(~drop_mask)
    return WindowFilterResult(
        frame=kept_frame,
        dropped=dropped,
        unclassified=unclassified,
        below_window=n_below,
        boundary_retained=boundary_retained,
        retained_reasons=retained_reasons,
        refused=False,
    )


def exclude_out_of_window(
    df: pl.DataFrame,
    column: str,
    window: RequestWindow,
) -> WindowFilterResult:
    """Unconditionally exclude rows outside ``window`` -- HALF_OPEN interval
    semantics (:class:`IntervalSemantics`, Sol ruling 2026-07-26).

    Contrast with :func:`filter_frame_to_window` (CLOSED interval, Elexon):
    there, a boundary row genuinely belongs to two adjacent chunks and its
    removal must be proven safe against a neighbour first (D-3b), because
    dropping an unproven row would be a real loss. Here, a row outside
    ``[window.start, window.end)`` was never part of THIS partition's own
    request in the first place (ENTSO-E's measured CET/CEST delivery-day
    over-span, ``R2-A-PLAN.md`` S1.4) -- there is no ownership question to
    resolve. Bronze is immutable and remains the source of truth for that
    row under its OWN correctly-scoped partition; keeping the over-returned
    copy in THIS partition's silver output would both violate this
    partition's declared window and plant a duplicate for whenever the
    owning date is properly ingested. So the row is always excluded here,
    never gated on any neighbour's durability -- but always counted and
    logged (never a silent drop).

    IO-free, like :func:`filter_frame_to_window`; does no logging itself
    (the caller owns that, D-7c's absent-column check included). Absent
    ``column`` -> the frame is returned unchanged, same contract as
    :func:`filter_frame_to_window`.

    D-5's REFUSAL does NOT apply here (contrast with :func:`filter_frame_to_window`,
    CLOSED interval, where D-5 keeps the frame unchanged and sets
    ``refused=True``). A wholly out-of-window frame is still performed, not
    refused: TRIM ruling — an out-of-scope ENTSO-E row is ALWAYS excluded,
    unconditionally, even when every row qualifies. ``refused`` stays
    ``False`` on this path; ``all_dropped=True`` signals the 100% case
    instead, so the caller can log it as ``ERROR`` (loud, not silent) --
    the exact signature of a horizon dataset opted into the filter by
    mistake, D-5's original misclassification concern.
    """
    if column not in df.columns or df.is_empty():
        return WindowFilterResult(
            frame=df,
            dropped=0,
            unclassified=0,
            below_window=0,
            boundary_retained=0,
            retained_reasons=(),
            refused=False,
        )

    total = len(df)
    values = df[column]
    unclassified = int(values.is_null().sum())

    below_start = (values < window.start).fill_null(False)
    at_or_after_end = (values >= window.end).fill_null(False)
    drop_mask = below_start | at_or_after_end
    dropped = int(drop_mask.sum())
    n_below = int(below_start.sum())

    kept_frame = df.filter(~drop_mask)

    if dropped > 0 and dropped == total:
        # D-5's refusal ("never empty an otherwise non-empty frame") does
        # NOT apply here, unlike filter_frame_to_window's CLOSED path: a
        # 100%-out-of-window frame is not an ownership question this
        # partition could ever answer "yes" to -- every row genuinely was
        # never part of THIS partition's own request. Refusing the drop
        # would write out-of-scope vendor over-span into silver, which is
        # exactly the F-10/TRIM violation this function exists to prevent.
        # The drop PROCEEDS; the caller logs ERROR (not the usual per-row
        # WARNING) so a wrongly opted-in horizon dataset -- where ~100% of
        # rows would legitimately fall outside a delivery-day window -- is
        # loud, not silently retained (D-5's original misclassification
        # concern, preserved as a signal rather than a refusal).
        return WindowFilterResult(
            frame=kept_frame,
            dropped=dropped,
            unclassified=unclassified,
            below_window=n_below,
            boundary_retained=0,
            retained_reasons=(),
            refused=False,
            all_dropped=True,
        )

    return WindowFilterResult(
        frame=kept_frame,
        dropped=dropped,
        unclassified=unclassified,
        below_window=n_below,
        boundary_retained=0,
        # retained_reasons is meaningless here -- nothing is ever RETAINED
        # under HALF_OPEN semantics (an unproven boundary), only ever
        # dropped as OUT_OF_REQUEST_SCOPE or kept because it is genuinely
        # in-window. The caller logs `dropped` directly with that reason.
        retained_reasons=(),
        refused=False,
    )
