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
``stale_covered`` (a ``COVERED`` grant whose evidence no longer matches).

**Drain.** Recovers ``missing``, attempt-``failed``,
``missing_or_invalid_output`` and orphaned (b), grouped by (family, partition
date): a generic group is one ``run_captures`` call restricted to its capture
ids; a bespoke group is one :func:`completion.run_bespoke_capture` per
capture. A failing capture or group never stops the next. After the last
group the catalogue is refreshed once and the drain reconciles again; that
second result is what the drain returns. It never touches orphaned (a),
``duplicated``, unusable sidecars or ``stale_covered``.
"""

from __future__ import annotations

import hashlib
import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import polars as pl

from gridflow.connectors.neso_data_portal.captures import newest_by_resource, scan_dataset
from gridflow.connectors.neso_data_portal.registry import (
    LEGACY_KEYS,
    CoveredDisposition,
    SilverDisposition,
)
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
from gridflow.silver.neso_data_portal.generic import (
    FILES_REASON,
    INGEST_ONLY_REASON,
    GenericNesoTransformer,
    families_of,
)
from gridflow.silver.registry import get_transformer_class
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence
    from datetime import date

    from gridflow.connectors.neso_data_portal.captures import Capture
    from gridflow.connectors.neso_data_portal.registry import (
        Disposition,
        Registry,
        ResourceEntry,
    )
    from gridflow.silver.base import BaseSilverTransformer

logger = logging.getLogger(__name__)

__all__ = [
    "CATEGORIES",
    "DRAINABLE",
    "Gap",
    "ReconcileReport",
    "UnknownFamilyError",
    "drain",
    "inventory_sha256",
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
)
DRAINABLE: frozenset[str] = frozenset({"missing", "failed", "missing_or_invalid_output"})
"""Plus orphaned (b), told apart from (a) by :attr:`Gap.drainable`."""


class UnknownFamilyError(ValueError):
    """A named key is not a registry family (a usage error, exit 2)."""


@dataclass(frozen=True)
class Gap:
    """One reconcile gap.

    Attributes:
        category: One of :data:`CATEGORIES`.
        family: The family key the gap belongs to.
        partition_date: The capture's bronze partition date, when known.
        capture_id: The capture id, or ``-``.
        detail: Free text; for ``orphaned`` it starts ``a:`` or ``b:``.
        drainable: Whether ``--drain`` attempts it.
    """

    category: str
    family: str
    partition_date: date | None
    capture_id: str
    detail: str
    drainable: bool = False

    def line(self) -> str:
        """The ``GAP`` output line."""
        day = self.partition_date.isoformat() if self.partition_date is not None else "-"
        return f"GAP {self.category} {self.family} {day} {self.capture_id} {self.detail}"


@dataclass(frozen=True)
class ReconcileReport:
    """Everything one reconcile pass found.

    Attributes:
        families: The families checked.
        skipped: ``(family, reason)`` for every named or listed family not
            checked (ingest-only, ``files``).
        gaps: Every gap, in a deterministic order.
        drained: ``(family, partition date, capture count)`` per drained group.
    """

    families: tuple[str, ...]
    skipped: tuple[tuple[str, str], ...]
    gaps: tuple[Gap, ...]
    drained: tuple[tuple[str, date, int], ...] = field(default=())

    @property
    def clean(self) -> bool:
        """Whether no gap was found."""
        return not self.gaps

    def lines(self) -> list[str]:
        """Every ``GAP`` line, then the ``SUMMARY`` lines."""
        out = [gap.line() for gap in self.gaps]
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
            gaps.append(Gap("failed", key, pair.partition_date, capture_id, detail, drainable))
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


def inventory_sha256(resource: ResourceEntry) -> str:
    """The digest of a resource's registry child inventory (``COVERED`` evidence).

    Ordered ``(child, disposition)`` pairs as canonical JSON; a resource with
    no children digests the empty list.
    """
    document = [
        [child.child, child.disposition.model_dump(mode="json")] for child in resource.children
    ]
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _record_version(registry: Registry, resource: ResourceEntry) -> str:
    target = (
        resource.disposition.key
        if isinstance(resource.disposition, SilverDisposition)
        else resource.family
    )
    entry = registry.families.get(target)
    record = entry[1].record if entry is not None else None
    return record.version if record is not None else ""


def _newest_capture_ids(
    registry: Registry, data_dir: Path, cutoff: date, directories: Iterable[str]
) -> dict[str, tuple[str, date]]:
    paths = PathBuilder(data_dir)
    captures: list[Capture] = []
    for dir_key in sorted(set(directories)):
        scan = scan_dataset(paths.bronze_dir(SOURCE, dir_key), registry, require_provenance=False)
        captures.extend(c for c in scan.captures if partition_date_of(c.body) <= cutoff)
    return {
        resource_id: (capture_id_for(capture.body, data_dir), partition_date_of(capture.body))
        for resource_id, capture in newest_by_resource(captures).items()
    }


def _stale_covered(
    registry: Registry, data_dir: Path, cutoff: date, families: Sequence[str]
) -> list[Gap]:
    """``stale_covered``: every COVERED grant of the checked families' packages."""
    packages = {registry.families[key][0].package for key in families}
    grants: list[tuple[ResourceEntry, str, Disposition]] = []
    for package in registry.packages:
        if package.package not in packages:
            continue
        for resource in package.resources:
            if isinstance(resource.disposition, CoveredDisposition):
                grants.append((resource, "-", resource.disposition))
            for child in resource.children:
                if isinstance(child.disposition, CoveredDisposition):
                    grants.append((resource, child.child, child.disposition))
    if not grants:
        return []
    directories = [resource.family for resource, _child, _grant in grants]
    for _resource, _child, grant in grants:
        assert isinstance(grant, CoveredDisposition)
        covering = registry.resources.get(grant.by)
        if covering is not None:
            directories.append(covering[1].family)
    newest = _newest_capture_ids(registry, data_dir, cutoff, directories)

    gaps: list[Gap] = []
    for resource, child_id, grant in grants:
        assert isinstance(grant, CoveredDisposition)
        covered_now = newest.get(resource.id)
        covering_entry = registry.resources.get(grant.by)
        covering_now = newest.get(grant.by) if covering_entry is not None else None
        evidence = grant.evidence
        reasons: list[str] = []
        if evidence is None:
            reasons.append("no evidence")
        else:
            if covering_entry is None:
                reasons.append(f"covering resource {grant.by} is not in the registry")
            if (covered_now[0] if covered_now else "") != evidence.covered_capture:
                reasons.append("covered resource has a newer capture")
            if (covering_now[0] if covering_now else "") != evidence.covering_capture:
                reasons.append("covering resource has a newer capture")
            if _record_version(registry, resource) != evidence.covered_record_version:
                reasons.append("covered record version changed")
            if (
                covering_entry is not None
                and _record_version(registry, covering_entry[1]) != evidence.covering_record_version
            ):
                reasons.append("covering record version changed")
            if inventory_sha256(resource) != evidence.inventory_sha256:
                reasons.append("child inventory changed")
        if reasons:
            gaps.append(
                Gap(
                    "stale_covered",
                    resource.family,
                    covered_now[1] if covered_now else None,
                    covered_now[0] if covered_now else "-",
                    f"resource {resource.id} child {child_id}: {'; '.join(reasons)}",
                )
            )
    return gaps


def _sort_key(gap: Gap) -> tuple[str, str, str, str]:
    day = gap.partition_date.isoformat() if gap.partition_date is not None else ""
    return (gap.family, day, gap.category, gap.capture_id)


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
        The report.

    Raises:
        UnknownFamilyError: A named key is not a registry family, or a
            checked family has no registered transformer.
    """
    families, skipped = _resolve_scope(registry, keys)
    gaps: list[Gap] = []
    for key in families:
        family_gaps, _expected_pairs = _family_gaps(key, registry, data_dir, cutoff)
        gaps.extend(family_gaps)
    gaps.extend(_stale_covered(registry, data_dir, cutoff, families))
    return ReconcileReport(tuple(families), tuple(skipped), tuple(sorted(gaps, key=_sort_key)))


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
    return ReconcileReport(after.families, after.skipped, after.gaps, tuple(drained))
