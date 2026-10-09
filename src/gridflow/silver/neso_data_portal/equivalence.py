"""The COVERED equivalence harness: proof inputs, fingerprint and proof (ADR-037 P-13, P-14).

A ``COVERED`` grant exempts **every** capture of its site from transform
(``families_of`` resolves each capture to its resource and gives a COVERED
resource or child no family). So the proof covers exactly that set (I-SCOPE):
a leg's scope is every usable capture, in every registry family's bronze
directory, that :func:`~gridflow.silver.neso_data_portal.generic.resource_of`
resolves to the leg's resource, at or before the cutoff.

**One proof-input value (I-PROOF).** :func:`gather_inputs` builds one
:class:`ProofInputs`; :func:`prove` reads that value and the bodies its scope
entries name, and nothing else. :func:`comparison_components` digests every
field of it by iterating ``dataclasses.fields(ProofInputs)``, so no proof input
can sit outside the fingerprint. Evidence is never an input: installing a
proved grant changes no component; editing its ``key``/``by`` or copying its
evidence to another site does.

**Legs.** A covered capture ``C`` is matched by a covering capture ``D`` when
their projections have equal ``{name: dtype}`` and equal multisets of rows
(i), and ``max`` vintage over ``D``'s rows is at or before ``min`` vintage over
``C``'s (iii). Every capture of both scopes must type cleanly (ii). A grant
needs both scopes non-empty, (ii), and every covered capture matched.

**CLI**::

    python -m gridflow.silver.neso_data_portal.equivalence --covered RID[::CHILD] \\
        --as KEY --by RID --cutoff YYYY-MM-DD [--data-dir D]

prints the JSON proof; exit 0 on a grant, 1 on a refusal, 2 on a usage error.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import sys
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.captures import scan_dataset
from gridflow.connectors.neso_data_portal.registry import (
    CoveredEvidence,
    SilverDisposition,
)
from gridflow.connectors.neso_data_portal.registry.record import CHILD_SEPARATOR, SchemaRecord
from gridflow.silver.neso_data_portal.casting import (
    ExclusionTally,
    finish_capture,
    record_dtypes,
    type_child,
)
from gridflow.silver.neso_data_portal.completion import (
    capture_context,
    capture_id_for,
    partition_date_of,
)
from gridflow.silver.neso_data_portal.generic import ENGINE_VERSION, resource_of
from gridflow.silver.neso_data_portal.readers import read_children
from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from collections.abc import Sequence

    from gridflow.connectors.neso_data_portal.captures import Capture
    from gridflow.connectors.neso_data_portal.registry import Registry

__all__ = [
    "COMPONENTS",
    "EXCLUDED_COLUMNS",
    "HARNESS_VERSION",
    "ProofInputError",
    "ProofInputs",
    "ProofResult",
    "ScopeEntry",
    "ScopeIndex",
    "Site",
    "comparison_components",
    "fingerprint_of",
    "gather_inputs",
    "main",
    "metadata_dependencies",
    "prove",
    "scope_index",
]

SOURCE = "neso_data_portal"

HARNESS_VERSION = "1"
"""Bumped on any change to how the harness reads, projects or compares; a bump
voids every grant (field ``harness``)."""

EXCLUDED_COLUMNS: tuple[str, ...] = (
    "child_id",
    "child_crc32",
    "bronze_capture_id",
    "capture_written_at",
    "published_at",
)
"""Capture identity and clocks: never compared (the vintage leg reads the clocks)."""

_COUNT = "__equivalence_rows"


class ProofInputError(Exception):
    """The proof's site, key or covering resource cannot be resolved consistently."""


@dataclass(frozen=True)
class Site:
    """Where a grant sits: a resource, or one child of it."""

    resource_id: str
    child: str | None


@dataclass(frozen=True)
class ScopeEntry:
    """One capture in a leg's scope.

    Attributes:
        capture: The capture (for reading).
        data_dir: The data root its capture id is relative to (not digested).
        identity: ``{body_sha256, metadata}``: what the fingerprint digests.
    """

    capture: Capture
    data_dir: Path
    identity: dict[str, Any]


@dataclass(frozen=True)
class ProofInputs:
    """Every input of one proof; each field is one fingerprint component (P-13)."""

    harness: dict[str, Any]
    covered_leg: dict[str, Any]
    covering_leg: dict[str, Any]
    covered_children: tuple[str, ...]
    covered_record: dict[str, Any]
    covering_record: dict[str, Any]
    covered_scope: tuple[ScopeEntry, ...]
    covering_scope: tuple[ScopeEntry, ...]


COMPONENTS: tuple[str, ...] = tuple(item.name for item in dataclasses.fields(ProofInputs))
"""The fingerprint components, in field order."""

_SCOPE_FIELDS = frozenset({"covered_scope", "covering_scope"})


@dataclass(frozen=True)
class ProofResult:
    """A proof's outcome.

    Attributes:
        granted: Whether every leg holds for every covered capture.
        components: The digest of every proof input.
        evidence: The evidence to install, on a grant.
        disposition: The installable ``COVERED`` disposition, on a grant.
        refusals: Per unmatched covered capture (or per scope failure), why.
    """

    granted: bool
    components: dict[str, str]
    evidence: CoveredEvidence | None = None
    disposition: dict[str, Any] | None = None
    refusals: list[dict[str, Any]] = field(default_factory=list)

    def as_json(self) -> dict[str, Any]:
        """The printed proof."""
        return {
            "granted": self.granted,
            "components": self.components,
            "evidence": self.evidence.model_dump(mode="json") if self.evidence else None,
            "disposition": self.disposition,
            "refusals": self.refusals,
        }


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def metadata_dependencies(record: SchemaRecord) -> tuple[str, ...]:
    """The sidecar fields a record's read depends on (``capture_context`` and issue times).

    Always ``url_type`` and ``empty_capture``; plus ``ckan_last_modified`` under
    that vintage, ``resource_filename`` when an epoch takes a filename-token
    issue time, and ``written_at`` under ``capture_fallback``.
    """
    fields = {"url_type", "empty_capture"}
    if record.vintage == "ckan_last_modified":
        fields.add("ckan_last_modified")
    if any(epoch.issue.kind == "filename_token" for epoch in record.epochs):
        fields.add("resource_filename")
    if record.vintage == "capture_fallback":
        fields.add("written_at")
    return tuple(sorted(fields))


def _identity(capture: Capture, record: SchemaRecord) -> dict[str, Any]:
    meta: Any = json.loads(capture.sidecar.read_text(encoding="utf-8"))
    params: dict[str, Any] = meta.get("request_params") or {}
    metadata: dict[str, Any] = {}
    for name in metadata_dependencies(record):
        metadata[name] = meta.get(name) if name == "written_at" else params.get(name)
    return {"body_sha256": capture.body_sha256, "metadata": metadata}


ScopeIndex = tuple[tuple["Capture", str], ...]
"""Every usable capture of every registry family directory, with its resource id."""


def scope_index(registry: Registry, data_dir: Path) -> ScopeIndex:
    """Scan every registry family's bronze directory once (I-SCOPE's superset walk).

    Args:
        registry: The loaded registry.
        data_dir: The data root.

    Returns:
        ``(capture, resource id)`` for every usable capture that
        :func:`resource_of` resolves.
    """
    paths = PathBuilder(data_dir)
    out: list[tuple[Capture, str]] = []
    for dir_key in sorted(registry.families):
        scan = scan_dataset(paths.bronze_dir(SOURCE, dir_key), registry, require_provenance=False)
        for capture in scan.captures:
            resource = resource_of(capture, dir_key, registry)
            if resource is not None:
                out.append((capture, resource.id))
    return tuple(out)


def _record_of(registry: Registry, key: str, what: str) -> SchemaRecord:
    entry = registry.families.get(key)
    record = entry[1].record if entry is not None else None
    if record is None:
        raise ProofInputError(f"{what} family {key!r} has no frozen schema record")
    return record


def gather_inputs(
    registry: Registry,
    data_dir: Path,
    site: Site,
    by: str,
    key: str,
    cutoff: date,
    *,
    index: ScopeIndex | None = None,
) -> ProofInputs:
    """Build the one :class:`ProofInputs` of a proof (P-13).

    Args:
        registry: The registry the grant is read from.
        data_dir: The data root.
        site: Where the grant sits.
        by: The covering resource id.
        key: The recorded family that types the covered rows.
        cutoff: The last bronze partition date in scope (inclusive).
        index: A precomputed :func:`scope_index` (shared across grants).

    Returns:
        The proof inputs.

    Raises:
        ProofInputError: The site, ``key`` or ``by`` does not resolve, or the
            reader seam's registry gives the site different child ids.
    """
    entry = registry.resources.get(site.resource_id)
    if entry is None:
        raise ProofInputError(f"covered resource {site.resource_id} is not in the registry")
    covered = entry[1]
    children = tuple(sorted(child.child for child in covered.children))
    if site.child is not None and site.child not in children:
        raise ProofInputError(
            f"covered resource {site.resource_id} has no child {site.child!r} in its inventory"
        )
    covered_record = _record_of(registry, key, "covered")
    covering_entry = registry.resources.get(by)
    if covering_entry is None:
        raise ProofInputError(f"covering resource {by} is not in the registry")
    covering = covering_entry[1]
    if not isinstance(covering.disposition, SilverDisposition) or covering.children:
        raise ProofInputError(f"covering resource {by} is not a childless SILVER resource")
    covering_key = covering.disposition.key
    covering_record = _record_of(registry, covering_key, "covering")

    seam = registry_module.load_registry().resources.get(site.resource_id)
    seam_children = tuple(sorted(child.child for child in seam[1].children)) if seam else None
    if seam_children != children:
        raise ProofInputError(
            f"the reader's registry gives resource {site.resource_id} children "
            f"{list(seam_children) if seam_children is not None else None}, the proof's "
            f"registry {list(children)}"
        )

    captures = index if index is not None else scope_index(registry, data_dir)

    def scope(resource_id: str, record: SchemaRecord) -> tuple[ScopeEntry, ...]:
        return tuple(
            ScopeEntry(capture, data_dir, _identity(capture, record))
            for capture, owner in captures
            if owner == resource_id and partition_date_of(capture.body) <= cutoff
        )

    return ProofInputs(
        harness={
            "HARNESS_VERSION": HARNESS_VERSION,
            "ENGINE_VERSION": ENGINE_VERSION,
            "EXCLUDED_COLUMNS": list(EXCLUDED_COLUMNS),
            "temporal_none_drops": "timestamp_utc",
        },
        covered_leg={"resource_id": site.resource_id, "child": site.child, "family": key},
        covering_leg={"resource_id": by, "child": None, "family": covering_key},
        covered_children=children,
        covered_record=covered_record.model_dump(mode="json", exclude_none=True),
        covering_record=covering_record.model_dump(mode="json", exclude_none=True),
        covered_scope=scope(site.resource_id, covered_record),
        covering_scope=scope(by, covering_record),
    )


def _component_value(name: str, value: Any) -> Any:
    if name in _SCOPE_FIELDS:
        return sorted({_canonical(item.identity) for item in value})
    if isinstance(value, tuple):
        return list(value)
    return value


def comparison_components(inputs: ProofInputs) -> dict[str, str]:
    """One SHA-256 of canonical JSON per :class:`ProofInputs` field, in field order.

    Scope fields digest their sorted distinct identities only.
    """
    return {
        item.name: _sha256(_component_value(item.name, getattr(inputs, item.name)))
        for item in dataclasses.fields(ProofInputs)
    }


def fingerprint_of(components: dict[str, str]) -> str:
    """The SHA-256 of the canonical JSON of a components dict."""
    return _sha256(components)


@dataclass
class _Read:
    """One scope capture read and typed through B's pure path."""

    capture_id: str
    body_sha256: str
    frame: pl.DataFrame | None
    problem: str | None


def _projection(record: SchemaRecord) -> dict[str, str]:
    dtypes = record_dtypes(record)
    drop = set(EXCLUDED_COLUMNS)
    if record.temporal.kind == "none":
        drop.add("timestamp_utc")
    return {name: dtype for name, dtype in dtypes.items() if name not in drop}


def _read(entry: ScopeEntry, record: SchemaRecord, children: tuple[str, ...], key: str) -> _Read:
    capture = entry.capture
    capture_id = capture_id_for(capture.body, entry.data_dir)
    try:
        ctx = capture_context(capture, record, entry.data_dir)
        tables = list(read_children(ctx.body, record, children))
        if all(table.frame.height == 0 for table in tables):
            return _Read(capture_id, capture.body_sha256, None, "empty capture")
        typed = [type_child(table, record, ctx) for table in tables]
        tally = ExclusionTally()
        for child in typed:
            tally.merge(child.tally)
        if tally.total:
            return _Read(
                capture_id,
                capture.body_sha256,
                None,
                f"excluded {tally.total} row(s) {dict(sorted(tally.counts.items()))}",
            )
        frame = finish_capture([child.frame for child in typed], tally, record, ctx, key)
    except Exception as exc:  # noqa: BLE001 - any failure to read is leg (ii), reported
        return _Read(capture_id, capture.body_sha256, None, f"raised {type(exc).__name__}: {exc}")
    return _Read(capture_id, capture.body_sha256, frame, None)


def _vintage(frame: pl.DataFrame) -> pl.Series:
    return frame.select(pl.coalesce("published_at", "capture_written_at")).to_series()


def _multiset(frame: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    return frame.select(columns).group_by(columns).len(name=_COUNT)


def _anti(left: pl.DataFrame, right: pl.DataFrame, on: list[str]) -> int:
    return left.join(right, on=on, how="anti", nulls_equal=True).height


def prove(inputs: ProofInputs) -> ProofResult:
    """Run legs (i)-(iii) over both scopes (P-14).

    Args:
        inputs: The proof inputs from :func:`gather_inputs`.

    Returns:
        The grant (with evidence and the installable disposition), or the
        refusal naming each unmatched covered capture and its failing legs.
    """
    components = comparison_components(inputs)
    # I-PROOF: the records are rebuilt from the digested dumps, never re-read.
    covered_record = SchemaRecord.model_validate(inputs.covered_record)
    covering_record = SchemaRecord.model_validate(inputs.covering_record)
    covered_key = inputs.covered_leg["family"]
    covering_key = inputs.covering_leg["family"]
    covered_children = (inputs.covered_leg["child"],) if inputs.covered_leg["child"] else ()
    refusals: list[dict[str, Any]] = []
    if not inputs.covered_scope:
        refusals.append({"scope": "covered", "reason": "no capture of covered"})
    if not inputs.covering_scope:
        refusals.append({"scope": "covering", "reason": "no capture of covering"})
    if refusals:
        return ProofResult(False, components, refusals=refusals)

    covered = [
        _read(e, covered_record, covered_children, covered_key) for e in inputs.covered_scope
    ]
    covering = [_read(e, covering_record, (), covering_key) for e in inputs.covering_scope]
    for item in [*covered, *covering]:
        if item.problem is not None:
            refusals.append(
                {
                    "capture_id": item.capture_id,
                    "body_sha256": item.body_sha256,
                    "legs": {"ii": item.problem},
                }
            )
    if refusals:
        return ProofResult(False, components, refusals=refusals)

    left_schema = _projection(covered_record)
    right_schema = _projection(covering_record)
    columns = list(left_schema)
    for c_item in covered:
        assert c_item.frame is not None
        c_min = _vintage(c_item.frame).min()
        failures: list[dict[str, Any]] = []
        matched = False
        for d_item in covering:
            assert d_item.frame is not None
            legs: dict[str, Any] = {}
            if left_schema != right_schema:
                legs["i"] = {"schema": {"covered": left_schema, "covering": right_schema}}
            else:
                left = _multiset(c_item.frame, columns)
                right = _multiset(d_item.frame, columns)
                on = [*columns, _COUNT]
                only_left, only_right = _anti(left, right, on), _anti(right, left, on)
                if only_left or only_right:
                    legs["i"] = {
                        "rows": [c_item.frame.height, d_item.frame.height],
                        "anti_join": [only_left, only_right],
                    }
            d_max = _vintage(d_item.frame).max()
            if d_max > c_min:  # type: ignore[operator]
                legs["iii"] = {"covering_max": str(d_max), "covered_min": str(c_min)}
            if not legs:
                matched = True
                break
            failures.append({"covering_capture_id": d_item.capture_id, "legs": legs})
        if not matched:
            refusals.append(
                {
                    "capture_id": c_item.capture_id,
                    "body_sha256": c_item.body_sha256,
                    "against": failures,
                }
            )
    if refusals:
        return ProofResult(False, components, refusals=refusals)
    evidence = CoveredEvidence(fingerprint=fingerprint_of(components), components=components)
    disposition = {
        "kind": "COVERED",
        "by": inputs.covering_leg["resource_id"],
        "key": covered_key,
        "evidence": evidence.model_dump(mode="json"),
    }
    return ProofResult(True, components, evidence=evidence, disposition=disposition)


def _site(value: str) -> Site:
    resource_id, separator, child = value.partition(CHILD_SEPARATOR)
    return Site(resource_id, child if separator else None)


def main(argv: Sequence[str] | None = None) -> int:
    """The proof CLI; exit 0 grant, 1 refusal, 2 usage error."""
    parser = argparse.ArgumentParser(prog="python -m gridflow.silver.neso_data_portal.equivalence")
    parser.add_argument("--covered", required=True, help="RID or RID::CHILD")
    parser.add_argument("--as", dest="key", required=True, help="the recorded family key")
    parser.add_argument("--by", required=True, help="the covering resource id")
    parser.add_argument("--cutoff", required=True, help="YYYY-MM-DD")
    parser.add_argument("--data-dir", type=Path, default=None)
    try:
        args = parser.parse_args(argv)
        cutoff = date.fromisoformat(args.cutoff)
    except SystemExit as exc:
        return int(exc.code) if isinstance(exc.code, int) else 2
    except ValueError as exc:
        print(f"--cutoff must be YYYY-MM-DD ({exc})", file=sys.stderr)
        return 2
    data_dir = args.data_dir
    if data_dir is None:
        from gridflow.config.settings import load_settings

        data_dir = load_settings().pipeline.data_dir
    registry = registry_module.load_registry()
    try:
        inputs = gather_inputs(registry, data_dir, _site(args.covered), args.by, args.key, cutoff)
    except ProofInputError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    result = prove(inputs)
    print(json.dumps(result.as_json(), indent=2, sort_keys=True))
    return 0 if result.granted else 1


if __name__ == "__main__":
    sys.exit(main())
