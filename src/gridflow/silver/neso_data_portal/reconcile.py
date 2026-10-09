"""Reconcile NESO bronze captures against silver completion, and drain the gaps (ADR-034 P-14).

**Scope.** Families with a frozen record plus the three legacy keys (all, or
the named ones), over their own and sibling bronze directories, at bronze
partitions on or before the cutoff. Ingest-only and ``files`` families are
reported as skipped, never as gaps. The **expected pairs** are P-2's families
of every committed sidecar, read with ``require_provenance=False``.

**Categories.** ``missing`` (no completion, no failure record); ``failed`` (a
failure record without a valid completion, or an unusable sidecar);
``missing_or_invalid_output`` (a completion that fails the one validity
predicate); ``orphaned`` ((a) a completion whose capture is not expected, (b) a
generic output whose capture has no completion); ``duplicated`` (two outputs
carry one capture, or two bespoke captures resolve to one output path);
``stale_covered`` (a ``COVERED`` grant whose proof inputs, re-derived at the
cutoff from the grant's registry position, no longer digest to its evidence's
components, or whose scope is empty; ADR-037 P-13); ``overlap`` (a
resource-partitioned family whose `_latest` serves one entity key from two
resources' captures, ADR-039).

**Adjudication (ADR-040).** After the raw gaps are built, each is matched
against the registry's ``_reconcile_adjudications.json``: an entry in scope
(its family checked, every capture at or before the cutoff) covers a gap of
its family and category on one of its named captures, a ``failed`` gap only
with the entry's cause, an ``overlap`` gap only when every capture serving its
shared keys is named too. Covered gaps move to :attr:`ReconcileReport.adjudicated`;
they are listed (``ADJUDICATED`` lines) but do not fail the run. A named
capture with no covered gap is a ``stale_adjudication`` gap, which is never
drainable and never adjudicable. A ledger that fails to load or names what the
registry does not back raises :class:`RegistryError`.

**Drain.** Recovers ``missing``, attempt-``failed``,
``missing_or_invalid_output`` and orphaned (b), grouped by (family, partition
date): a generic group is one ``run_captures`` call restricted to its capture
ids; a bespoke group is one :func:`completion.run_bespoke_capture` per
capture. A failing capture or group never stops the next. After the last
group the catalogue is refreshed once and the drain reconciles again; that
second result is what the drain returns. It never touches orphaned (a),
``duplicated``, unusable sidecars, ``stale_covered``, ``overlap``,
``stale_adjudication`` or any adjudicated gap.
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import polars as pl

from gridflow.connectors.neso_data_portal.captures import scan_dataset
from gridflow.connectors.neso_data_portal.registry import (
    LEGACY_KEYS,
    CoveredDisposition,
    ReconcileAdjudication,
    RegistryError,
    capture_partition,
    load_reconcile_adjudications,
    reconcile_adjudication_problems,
)
from gridflow.silver.latest_views import select_latest_vintage
from gridflow.silver.neso_data_portal.completion import (
    NesoCaptureFailedError,
    Versions,
    bespoke_targets,
    bespoke_versions,
    capture_id_for,
    is_valid,
    partition_date_of,
    run_bespoke_capture,
    scan_completions,
)
from gridflow.silver.neso_data_portal.equivalence import (
    ProofInputError,
    Site,
    comparison_components,
    fingerprint_of,
    gather_inputs,
    scope_index,
)
from gridflow.silver.neso_data_portal.generic import (
    FILES_REASON,
    INGEST_ONLY_REASON,
    GenericNesoTransformer,
    families_of,
    latest_spec_for_record,
)
from gridflow.silver.registry import get_transformer_class
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from datetime import date

    from gridflow.connectors.neso_data_portal.captures import Capture
    from gridflow.connectors.neso_data_portal.registry import Registry, ResourceEntry
    from gridflow.connectors.neso_data_portal.registry.record import SchemaRecord
    from gridflow.silver.base import BaseSilverTransformer

logger = logging.getLogger(__name__)

__all__ = [
    "CATEGORIES",
    "DRAINABLE",
    "STALE_ADJUDICATION",
    "AdjudicatedGap",
    "Gap",
    "ReconcileReport",
    "UnknownFamilyError",
    "drain",
    "reconcile",
]

SOURCE = "neso_data_portal"
CATEGORIES: tuple[str, ...] = (
    "missing",
    "failed",
    "missing_or_invalid_output",
    "orphaned",
    "duplicated",
    "stale_covered",
    "overlap",
)
DRAINABLE: frozenset[str] = frozenset({"missing", "failed", "missing_or_invalid_output"})
"""Plus orphaned (b), told apart from (a) by :attr:`Gap.drainable`."""
STALE_ADJUDICATION = "stale_adjudication"
"""A ledger capture no live gap matches (ADR-040). Not in :data:`CATEGORIES`, so the
per-category ``SUMMARY`` lines stay byte-identical for a run without entries."""


class UnknownFamilyError(ValueError):
    """A named key is not a registry family (a usage error, exit 2)."""


@dataclass(frozen=True)
class Gap:
    """One reconcile gap.

    Attributes:
        category: One of :data:`CATEGORIES`, or :data:`STALE_ADJUDICATION`.
        family: The family key the gap belongs to.
        partition_date: The capture's bronze partition date, when known.
        capture_id: The capture id, or ``-``.
        detail: Free text; for ``orphaned`` it starts ``a:`` or ``b:``.
        drainable: Whether ``--drain`` attempts it.
        cause: For a ``failed`` gap with a failure record, its ``error_class``.
        peers: For an ``overlap`` gap, the other selected captures serving any
            of its shared entity keys, sorted.
    """

    category: str
    family: str
    partition_date: date | None
    capture_id: str
    detail: str
    drainable: bool = False
    cause: str | None = None
    peers: tuple[str, ...] = ()

    def line(self) -> str:
        """The ``GAP`` output line."""
        return f"GAP {self.category} {self.family} {_day(self)} {self.capture_id} {self.detail}"


def _day(gap: Gap) -> str:
    return gap.partition_date.isoformat() if gap.partition_date is not None else "-"


@dataclass(frozen=True)
class AdjudicatedGap:
    """A gap a ledger entry covers: reported, but it does not fail the run (ADR-040).

    Attributes:
        gap: The raw gap.
        entry: The covering ``_reconcile_adjudications.json`` entry.
    """

    gap: Gap
    entry: ReconcileAdjudication

    def line(self) -> str:
        """The ``ADJUDICATED`` output line: the gap, then its ruling, reason and question."""
        g, e = self.gap, self.entry
        return (
            f"ADJUDICATED {g.category} {g.family} {_day(g)} {g.capture_id} {g.detail} "
            f"[ruling {e.ruling}; reason: {e.reason}; question: {e.question}]"
        )


@dataclass(frozen=True)
class ReconcileReport:
    """Everything one reconcile pass found.

    Attributes:
        families: The families checked.
        skipped: ``(family, reason)`` for every named or listed family not
            checked (ingest-only, ``files``).
        gaps: Every open gap (including ``stale_adjudication``), in a
            deterministic order.
        drained: ``(family, partition date, capture count)`` per drained group.
        adjudicated: Every gap a ledger entry covers, in the same order.
    """

    families: tuple[str, ...]
    skipped: tuple[tuple[str, str], ...]
    gaps: tuple[Gap, ...]
    drained: tuple[tuple[str, date, int], ...] = field(default=())
    adjudicated: tuple[AdjudicatedGap, ...] = field(default=())

    @property
    def passed(self) -> bool:
        """Whether no open gap was found (the CLI's exit-0 predicate)."""
        return not self.gaps

    @property
    def clean(self) -> bool:
        """Whether no gap was found at all: adjudicated gaps are not clean (H2)."""
        return self.passed and not self.adjudicated

    def lines(self) -> list[str]:
        """``GAP`` lines, ``ADJUDICATED`` lines, then the ``SUMMARY`` lines.

        Per-category counts are over open gaps; the ``adjudicated`` and
        ``stale_adjudication`` counts follow only when either is non-zero, so a
        run without ledger entries prints exactly what it did before ADR-040.
        """
        out = [gap.line() for gap in self.gaps]
        out.extend(item.line() for item in self.adjudicated)
        counts = Counter(gap.category for gap in self.gaps)
        out.append(
            "SUMMARY families="
            f"{len(self.families)} skipped={len(self.skipped)} gaps={len(self.gaps)}"
        )
        out.extend(f"SUMMARY {category} {counts.get(category, 0)}" for category in CATEGORIES)
        out.extend(f"SUMMARY skipped {key} ({reason})" for key, reason in self.skipped)
        out.extend(
            f"SUMMARY drained {key} {day.isoformat()} {count}" for key, day, count in self.drained
        )
        if self.adjudicated or counts.get(STALE_ADJUDICATION, 0):
            out.append(f"SUMMARY adjudicated {len(self.adjudicated)}")
            out.append(f"SUMMARY {STALE_ADJUDICATION} {counts.get(STALE_ADJUDICATION, 0)}")
        return out


@dataclass(frozen=True)
class _Pair:
    capture: Capture
    capture_id: str
    partition_date: date


def _partition_or_none(path: Path) -> date | None:
    try:
        return partition_date_of(path)
    except ValueError:
        return None


def _resolve_scope(
    registry: Registry, keys: Sequence[str] | None
) -> tuple[list[str], list[tuple[str, str]]]:
    names = sorted(registry.families) if keys is None else list(dict.fromkeys(keys))
    unknown = [key for key in names if key not in registry.families]
    if unknown:
        raise UnknownFamilyError(f"not registry families: {unknown}")
    families: list[str] = []
    skipped: list[tuple[str, str]] = []
    for key in names:
        _package, family = registry.families[key]
        if key in LEGACY_KEYS or family.record is not None:
            families.append(key)
        else:
            reason = FILES_REASON if family.kind == "files" else INGEST_ONLY_REASON
            skipped.append((key, reason))
    return families, skipped


def _transformer_class(key: str) -> type[BaseSilverTransformer]:
    cls = get_transformer_class(SOURCE, key)
    if cls is None:
        raise UnknownFamilyError(f"{SOURCE}/{key} has no registered transformer")
    return cls


def _versions(cls: type[BaseSilverTransformer]) -> Versions:
    if issubclass(cls, GenericNesoTransformer):
        return cls.versions()
    return bespoke_versions(cls)


def _directories(key: str, registry: Registry) -> tuple[str, ...]:
    _package, family = registry.families[key]
    siblings = family.record.siblings if family.record is not None else ()
    return (key, *siblings)


def _expected(
    key: str, registry: Registry, data_dir: Path, cutoff: date, gaps: list[Gap]
) -> dict[str, _Pair]:
    """The expected pairs of ``key``; unusable sidecars become ``failed`` gaps."""
    paths = PathBuilder(data_dir)
    expected: dict[str, _Pair] = {}
    for dir_key in _directories(key, registry):
        scan = scan_dataset(paths.bronze_dir(SOURCE, dir_key), registry, require_provenance=False)
        for item in scan.unusable:
            day = _partition_or_none(item.sidecar)
            if day is None or day <= cutoff:
                gaps.append(
                    Gap(
                        "failed",
                        key,
                        day,
                        capture_id_for(item.sidecar, data_dir),
                        f"unusable sidecar: {item.reason}",
                    )
                )
        for capture in scan.captures:
            day = partition_date_of(capture.body)
            if day > cutoff or key not in families_of(capture, dir_key, registry):
                continue
            capture_id = capture_id_for(capture.body, data_dir)
            expected[capture_id] = _Pair(capture, capture_id, day)
    return expected


def _failures(data_dir: Path, key: str) -> dict[str, dict[str, Any]]:
    directory = PathBuilder(data_dir).completion_failure_dir(SOURCE, key)
    out: dict[str, dict[str, Any]] = {}
    if not directory.is_dir():
        return out
    for path in sorted(directory.glob("[!.]*.json")):
        try:
            document: Any = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            logger.warning("%s: unreadable failure record %s: %s", SOURCE, path, exc)
            continue
        if isinstance(document, dict) and isinstance(document.get("bronze_capture_id"), str):
            out[document["bronze_capture_id"]] = document
    return out


def _output_ids(data_dir: Path, key: str) -> dict[str, list[str]]:
    """Each generic output's capture ids -> the outputs (relative paths) holding them."""
    root = PathBuilder(data_dir).silver_dir(SOURCE, key)
    holders: dict[str, list[str]] = {}
    if not root.is_dir():
        return holders
    for path in sorted(root.rglob("[!.]*.parquet")):
        relative = path.relative_to(data_dir).as_posix()
        try:
            ids = (
                pl.scan_parquet(path, hive_partitioning=False)
                .select(pl.col("bronze_capture_id").unique())
                .collect()
                .to_series()
                .to_list()
            )
        except (OSError, pl.exceptions.PolarsError) as exc:
            logger.warning("%s/%s: unreadable output %s: %s", SOURCE, key, relative, exc)
            continue
        for capture_id in ids:
            holders.setdefault(str(capture_id), []).append(relative)
    return holders


def _family_gaps(
    key: str, registry: Registry, data_dir: Path, cutoff: date
) -> tuple[list[Gap], dict[str, _Pair]]:
    cls = _transformer_class(key)
    generic = issubclass(cls, GenericNesoTransformer)
    versions = _versions(cls)
    gaps: list[Gap] = []
    expected = _expected(key, registry, data_dir, cutoff, gaps)
    ledger = {
        row["bronze_capture_id"]: row
        for row in scan_completions(data_dir, key).collect().to_dicts()
        if row["partition_date"] <= cutoff
    }
    failures = _failures(data_dir, key)

    # ``duplicated`` is found first because it vetoes the drain for its capture:
    # the drain never touches a duplicated pair, so the capture's other gaps
    # (``missing``, orphaned (b), ...) are reported but not drainable.
    duplicated: set[str] = set()
    outputs: list[tuple[str, date | None, list[str]]] = []
    if generic:
        for capture_id, holders in sorted(_output_ids(data_dir, key).items()):
            day = _partition_or_none(Path(capture_id))
            if day is not None and day > cutoff:
                continue
            outputs.append((capture_id, day, holders))
            if len(holders) > 1:
                duplicated.add(capture_id)
                gaps.append(
                    Gap("duplicated", key, day, capture_id, f"outputs {', '.join(holders)}")
                )
    else:
        transformer = cls(data_dir)
        for day in sorted({pair.partition_date for pair in expected.values()}):
            _unique, collisions = bespoke_targets(transformer, day)
            for path, group in collisions:
                for capture in group:
                    capture_id = capture_id_for(capture.body, data_dir)
                    duplicated.add(capture_id)
                    gaps.append(
                        Gap(
                            "duplicated",
                            key,
                            day,
                            capture_id,
                            f"shares output path {path.name} with {len(group) - 1} other(s)",
                        )
                    )

    for capture_id in sorted(expected):
        pair = expected[capture_id]
        row = ledger.get(capture_id)
        if row is not None and is_valid(row, data_dir, versions):
            continue
        drainable = capture_id not in duplicated
        failure = failures.get(capture_id)
        if failure is not None:
            detail = f"{failure.get('error_class', '?')}: {failure.get('message', '')}"
            error_class = failure.get("error_class")
            cause = error_class if isinstance(error_class, str) else None
            gaps.append(
                Gap("failed", key, pair.partition_date, capture_id, detail, drainable, cause)
            )
        elif row is not None:
            detail = f"completion fails the validity predicate ({row['outcome']})"
            gaps.append(
                Gap(
                    "missing_or_invalid_output",
                    key,
                    pair.partition_date,
                    capture_id,
                    detail,
                    drainable,
                )
            )
        else:
            gaps.append(
                Gap("missing", key, pair.partition_date, capture_id, "no completion", drainable)
            )

    for capture_id in sorted(set(ledger) - set(expected)):
        gaps.append(
            Gap(
                "orphaned",
                key,
                ledger[capture_id]["partition_date"],
                capture_id,
                "a: completion without an expected capture",
            )
        )

    for capture_id, day, holders in outputs:
        if capture_id not in ledger:
            gaps.append(
                Gap(
                    "orphaned",
                    key,
                    day,
                    capture_id,
                    f"b: output without a completion ({holders[0]})",
                    capture_id in expected and capture_id not in duplicated,
                )
            )
    return gaps, expected


def _overlaps(key: str, record: SchemaRecord, data_dir: Path, cutoff: date) -> list[Gap]:
    """``overlap``: entity keys the family's ``_latest`` serves from two resources (ADR-039).

    The selection is :func:`select_latest_vintage` over the family's outputs and
    its completion records up to the cutoff, the one Polars renderer of
    ``_latest``, so the report and the view agree by construction. One gap per
    selected capture holding any such key; never drainable. A check that cannot
    read an output is one ``overlap check failed`` gap, never a pass.
    """
    root = PathBuilder(data_dir).silver_dir(SOURCE, key)
    grain = [column for column in record.entity_key if column != "resource_id"]
    try:
        files = sorted(root.rglob("[!.]*.parquet")) if root.is_dir() else []
        if not files:
            return []
        completions = scan_completions(data_dir, key).filter(pl.col("partition_date") <= cutoff)
        selected = select_latest_vintage(
            pl.scan_parquet(files, hive_partitioning=False),
            latest_spec_for_record(record, key),
            completions=completions,
        )
        shared = (
            selected.select(*grain, "resource_id", "bronze_capture_id")
            .filter(pl.col("resource_id").n_unique().over(grain) > 1)
            .collect()
        )
    except (OSError, pl.exceptions.PolarsError) as exc:
        return [Gap("overlap", key, None, "-", f"overlap check failed: {exc}")]

    resources_by_key: dict[tuple[Any, ...], set[str]] = {}
    captures_by_key: dict[tuple[Any, ...], set[str]] = {}
    keys_by_capture: dict[str, list[tuple[Any, ...]]] = {}
    resource_of_capture: dict[str, str] = {}
    for row in shared.iter_rows(named=True):
        entity = tuple(row[column] for column in grain)
        resources_by_key.setdefault(entity, set()).add(row["resource_id"])
        captures_by_key.setdefault(entity, set()).add(row["bronze_capture_id"])
        keys_by_capture.setdefault(row["bronze_capture_id"], []).append(entity)
        resource_of_capture[row["bronze_capture_id"]] = row["resource_id"]
    gaps: list[Gap] = []
    for capture_id, entities in sorted(keys_by_capture.items()):
        own = resource_of_capture[capture_id]
        others = sorted(set().union(*(resources_by_key[e] for e in entities)) - {own})
        peers = set().union(*(captures_by_key[e] for e in entities)) - {capture_id}
        first = min(entities, key=lambda e: tuple((v is None, v) for v in e))
        rendered = ", ".join(f"{c}={v}" for c, v in zip(grain, first, strict=True))
        gaps.append(
            Gap(
                "overlap",
                key,
                _partition_or_none(Path(capture_id)),
                capture_id,
                f"{len(entities)} key(s) also served by resource(s) {others}; first: {rendered}",
                peers=tuple(sorted(peers)),
            )
        )
    return gaps


def _stale_covered(
    registry: Registry, data_dir: Path, cutoff: date, families: Sequence[str]
) -> list[Gap]:
    """``stale_covered``: every COVERED grant of the checked families' packages (P-13).

    Each grant's proof inputs are gathered from its registry position (the
    resource and child it sits on, its ``key`` and ``by``) at the cutoff and
    digested; any component that differs from the evidence, an empty scope or
    an evidence fingerprint that does not match its components is reported.
    The directory scan runs once and is shared by every grant.
    """
    packages = {registry.families[key][0].package for key in families}
    grants: list[tuple[ResourceEntry, str | None, CoveredDisposition]] = []
    for package in registry.packages:
        if package.package not in packages:
            continue
        for resource in package.resources:
            if isinstance(resource.disposition, CoveredDisposition):
                grants.append((resource, None, resource.disposition))
            for child in resource.children:
                if isinstance(child.disposition, CoveredDisposition):
                    grants.append((resource, child.child, child.disposition))
    if not grants:
        return []
    index = scope_index(registry, data_dir)

    gaps: list[Gap] = []
    for resource, child_id, grant in grants:
        reasons: list[str] = []
        newest: tuple[str, date] | None = None
        evidence = grant.evidence
        if evidence is None or grant.key is None:
            reasons.append("no evidence or no key")
        else:
            try:
                inputs = gather_inputs(
                    registry,
                    data_dir,
                    Site(resource.id, child_id),
                    grant.by,
                    grant.key,
                    cutoff,
                    index=index,
                )
            except ProofInputError as exc:
                reasons.append(f"proof inputs unresolvable: {exc}")
            else:
                if inputs.covered_scope:
                    latest = max(
                        inputs.covered_scope,
                        key=lambda item: (item.capture.written_at, str(item.capture.body)),
                    )
                    newest = (
                        capture_id_for(latest.capture.body, data_dir),
                        partition_date_of(latest.capture.body),
                    )
                else:
                    reasons.append("no capture of covered")
                if not inputs.covering_scope:
                    reasons.append("no capture of covering")
                now = comparison_components(inputs)
                names = sorted(set(now) | set(evidence.components))
                changed = [n for n in names if now.get(n) != evidence.components.get(n)]
                if changed:
                    reasons.append(f"components changed: {', '.join(changed)}")
                if fingerprint_of(evidence.components) != evidence.fingerprint:
                    reasons.append("evidence fingerprint does not match its components")
        if reasons:
            gaps.append(
                Gap(
                    "stale_covered",
                    resource.family,
                    newest[1] if newest else None,
                    newest[0] if newest else "-",
                    f"resource {resource.id} child {child_id or '-'}: {'; '.join(reasons)}",
                )
            )
    return gaps


def _sort_key(gap: Gap) -> tuple[str, str, str, str]:
    day = gap.partition_date.isoformat() if gap.partition_date is not None else ""
    return (gap.family, day, gap.category, gap.capture_id)


def _covers(entry: ReconcileAdjudication, gap: Gap) -> bool:
    if gap.family != entry.family or gap.category != entry.category:
        return False
    if gap.capture_id not in entry.captures:
        return False
    if entry.category == "failed":
        return gap.cause == entry.cause
    # An overlap gap always has a peer; an empty set would cover vacuously.
    return bool(gap.peers) and set(gap.peers) <= set(entry.captures)


def _adjudicate(
    gaps: Sequence[Gap],
    entries: Sequence[ReconcileAdjudication],
    families: Sequence[str],
    cutoff: date,
) -> tuple[list[Gap], list[AdjudicatedGap], list[Gap]]:
    """Split raw gaps into (open, adjudicated, stale) per ADR-040.

    An entry is in scope when its family is checked and every capture is filed
    at or before the cutoff; an out-of-scope entry neither covers nor goes
    stale, so any gap it names stays open. Each in-scope capture that no
    covered gap names is one ``stale_adjudication`` gap.
    """
    scoped = [
        entry
        for entry in entries
        if entry.family in families
        and max(capture_partition(capture) for capture in entry.captures) <= cutoff
    ]
    open_gaps: list[Gap] = []
    adjudicated: list[AdjudicatedGap] = []
    covered: set[tuple[int, str]] = set()
    for gap in gaps:
        match = next(
            ((index, entry) for index, entry in enumerate(scoped) if _covers(entry, gap)), None
        )
        if match is None:
            open_gaps.append(gap)
            continue
        index, entry = match
        adjudicated.append(AdjudicatedGap(gap, entry))
        covered.add((index, gap.capture_id))
    stale = [
        Gap(
            STALE_ADJUDICATION,
            entry.family,
            capture_partition(capture),
            capture,
            f"ruling {entry.ruling}: no live {entry.category} gap on this capture matches "
            "the entry",
        )
        for index, entry in enumerate(scoped)
        for capture in entry.captures
        if (index, capture) not in covered
    ]
    return open_gaps, adjudicated, stale


def reconcile(
    data_dir: Path, registry: Registry, keys: Sequence[str] | None, cutoff: date
) -> ReconcileReport:
    """Report every gap between the scoped families' bronze and silver.

    Args:
        data_dir: The data root.
        registry: The loaded registry.
        keys: Family keys, or ``None`` for every registry family.
        cutoff: The last bronze partition date considered (inclusive).

    Returns:
        The report: open gaps (including ``stale_adjudication``) and, apart,
        the gaps the registry's reconcile adjudication ledger covers.

    Raises:
        UnknownFamilyError: A named key is not a registry family, or a
            checked family has no registered transformer.
        RegistryError: The adjudication ledger is missing or malformed, or
            names a family, directory or resource the registry does not back.
    """
    families, skipped = _resolve_scope(registry, keys)
    entries = load_reconcile_adjudications(registry.root)
    problems = reconcile_adjudication_problems(registry, entries)
    if problems:
        raise RegistryError("; ".join(problems))
    gaps: list[Gap] = []
    for key in families:
        family_gaps, _expected_pairs = _family_gaps(key, registry, data_dir, cutoff)
        gaps.extend(family_gaps)
        record = registry.families[key][1].record
        if record is not None and record.latest_partition is not None:
            gaps.extend(_overlaps(key, record, data_dir, cutoff))
    gaps.extend(_stale_covered(registry, data_dir, cutoff, families))
    open_gaps, adjudicated, stale = _adjudicate(gaps, entries, families, cutoff)
    return ReconcileReport(
        tuple(families),
        tuple(skipped),
        tuple(sorted([*open_gaps, *stale], key=_sort_key)),
        (),
        tuple(sorted(adjudicated, key=lambda item: _sort_key(item.gap))),
    )


def drain(
    data_dir: Path,
    registry: Registry,
    keys: Sequence[str] | None,
    cutoff: date,
    refresh: Callable[[], None],
) -> ReconcileReport:
    """Recover every drainable gap, refresh the catalogue once, reconcile again.

    Args:
        data_dir: The data root.
        registry: The loaded registry.
        keys: Family keys, or ``None`` for every registry family.
        cutoff: The last bronze partition date considered (inclusive).
        refresh: Re-registers the catalogue's views (called once, if any
            group ran).

    Returns:
        The reconcile report AFTER the drain, carrying the drained groups.
        Only open gaps are drained: an adjudicated ``failed`` capture is never
        re-run, so its failure record is never rewritten.
    """
    before = reconcile(data_dir, registry, keys, cutoff)
    groups: dict[tuple[str, date], set[str]] = {}
    for gap in before.gaps:
        if gap.drainable and gap.partition_date is not None:
            groups.setdefault((gap.family, gap.partition_date), set()).add(gap.capture_id)

    drained: list[tuple[str, date, int]] = []
    for (key, day), capture_ids in sorted(groups.items()):
        cls = _transformer_class(key)
        run_id = f"drain-{uuid4()}"
        drained.append((key, day, len(capture_ids)))
        if issubclass(cls, GenericNesoTransformer):
            try:
                cls(data_dir).run_captures(day, frozenset(capture_ids), run_id)
            except NesoCaptureFailedError as exc:
                logger.error("%s/%s %s: drain group failed: %s", SOURCE, key, day, exc)
            continue
        for capture_id in sorted(capture_ids):
            try:
                run_bespoke_capture(cls(data_dir), data_dir / capture_id, run_id)
            except NesoCaptureFailedError as exc:
                logger.error("%s/%s %s: drain capture failed: %s", SOURCE, key, day, exc)

    if drained:
        refresh()
    after = reconcile(data_dir, registry, keys, cutoff)
    return ReconcileReport(
        after.families, after.skipped, after.gaps, tuple(drained), after.adjudicated
    )
