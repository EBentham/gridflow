"""Deterministic vault page skeletons, one per NESO package (ADR-036 P-9).

Usage::

    python -m gridflow.connectors.neso_data_portal.skeleton --snapshot <dir> --out <dir> \
        [--package SLUG ...] [--field-info <dir>]

Writes ``<out>/<slug>.md`` for each named package (every registry package when
``--package`` is absent) and nothing else. The page is a **skeleton**: the
registry's facts (file inventory by disposition, schema tables, keys, both
clocks, cadence, licence and attribution, holds) with a ``TODO`` where the
docs wave writes prose. The seat places the files in the vault after merge.

**Clobber guard (I-3).** Every target is checked before anything is written:
if any existing target's front matter lacks ``skeleton: true`` the run exits 1
and writes nothing, so a hand-written vault page can never be overwritten.
Every write goes through ``files.replace_atomically``.

Exit codes: 0 written, 1 a refused target or a failed evidence check, 2 usage.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.eligibility import (
    describe_eligibility,
    effective_eligibility,
)
from gridflow.connectors.neso_data_portal.evidence import (
    EvidenceError,
    field_entries,
    load_field_info,
    load_snapshot,
    vendor_unit,
)
from gridflow.connectors.neso_data_portal.files import replace_atomically
from gridflow.connectors.neso_data_portal.registry import (
    CoveredDisposition,
    Held,
    HoldDisposition,
)

if TYPE_CHECKING:
    from collections.abc import Mapping

    from gridflow.connectors.neso_data_portal.registry import (
        PackageEntry,
        Registry,
        ResourceEntry,
    )
    from gridflow.connectors.neso_data_portal.registry.record import ColumnSpec, SchemaRecord

__all__ = [
    "ATTRIBUTION",
    "CLOCKS",
    "DISPOSITION_ORDER",
    "SkeletonRefusedError",
    "main",
    "render_package",
]

ATTRIBUTION = "Supported by National Energy SO Open Data"
"""The attribution text every NESO page carries (RULINGS 477)."""

VENDOR = "NESO Open Data Portal"
DISPOSITION_ORDER: tuple[str, ...] = ("SILVER", "COVERED", "DOC", "GIS", "HOLD")
SKELETON_MARKER = "skeleton: true"

CLOCKS: dict[str, tuple[str, str]] = {
    "ckan_last_modified": (
        "CKAN `last_modified` of the captured file (ADR-030)",
        "= `published_at`",
    ),
    "capture_fallback": ("null", "gridflow capture time (sidecar `written_at`), labelled so"),
    "issue_time_evidenced": ("per-row `issue_time`", "= `published_at`"),
}
"""Record vintage -> (``published_at``, ``available_at``) as a page states them."""

_DASH = "—"


class SkeletonRefusedError(Exception):
    """A target exists and is not a generated skeleton (I-3)."""


def _q(value: str) -> str:
    """A YAML-safe scalar: every front-matter string is a JSON string."""
    return json.dumps(value, ensure_ascii=False)


def _cell(text: str) -> str:
    return " ".join(str(text).split()).replace("|", "\\|")


def _tabular_keys(package: PackageEntry) -> list[str]:
    return sorted(f.key for f in package.families if f.kind == "tabular")


def _front_matter(package: PackageEntry) -> list[str]:
    keys = _tabular_keys(package) or sorted(f.key for f in package.families)
    lines = ["---", f"source: {_q('neso_data_portal')}", f"package: {_q(package.package)}"]
    if len(_tabular_keys(package)) == 1:
        lines.append(f"dataset_key: {_q(keys[0])}")
    else:
        lines.append("dataset_keys: [" + ", ".join(_q(key) for key in keys) + "]")
    silver = any(f.legacy or f.record is not None for f in package.families)
    lines += [
        f"vendor: {_q(VENDOR)}",
        SKELETON_MARKER,
        f"layer_coverage: {_q('bronze, silver' if silver else 'bronze')}",
        f"eligibility: {_q(package.eligibility.status)}",
        "---",
    ]
    return lines


def _resource_cells(resource: ResourceEntry, kind: str) -> list[str]:
    capture = "dump" if resource.url_type == "datastore" else "upload"
    cells = [_cell(resource.name), resource.format, capture, f"`{resource.id}`"]
    disposition = resource.disposition
    if kind == "COVERED":
        assert isinstance(disposition, CoveredDisposition)
        cells.append(f"`{disposition.by}`")
    elif kind == "HOLD":
        assert isinstance(disposition, HoldDisposition)
        cells.append(_cell(f"{disposition.reason} ({disposition.unit})"))
    return cells


def _child_detail(child_disposition: Any) -> str:
    kind = child_disposition.kind
    if isinstance(child_disposition, HoldDisposition):
        return f"HOLD: {child_disposition.reason} ({child_disposition.unit})"
    if isinstance(child_disposition, CoveredDisposition):
        return f"COVERED by `{child_disposition.by}`"
    key = getattr(child_disposition, "key", None)
    return f"SILVER `{key}`" if key else str(kind)


def _files(package: PackageEntry) -> list[str]:
    lines = ["## Files by disposition", ""]
    order = {kind: index for index, kind in enumerate(DISPOSITION_ORDER)}
    resources = sorted(
        package.resources,
        key=lambda r: (order.get(r.disposition.kind, len(order)), r.name, r.id),
    )
    for kind in DISPOSITION_ORDER:
        group = [r for r in resources if r.disposition.kind == kind]
        if not group:
            continue
        header = ["Resource", "Format", "Capture", "Id"]
        if kind == "COVERED":
            header.append("Covered by")
        elif kind == "HOLD":
            header.append("Reason (unit)")
        lines += [
            f"### {kind}",
            "",
            "| " + " | ".join(header) + " |",
            "|" + "---|" * len(header),
        ]
        for resource in group:
            lines.append("| " + " | ".join(_resource_cells(resource, kind)) + " |")
            for child in resource.children:
                cells = [
                    _cell(f"↳ {child.child}"),
                    _DASH,
                    _DASH,
                    _cell(_child_detail(child.disposition)),
                ]
                cells += [_DASH] * (len(header) - len(cells))
                lines.append("| " + " | ".join(cells) + " |")
        lines.append("")
    return lines


def _format_cell(column: ColumnSpec) -> str:
    """A column's format, or its per-filename formats (ADR-039), or a dash."""
    if column.formats_by_filename is not None:
        return "; ".join(f"`{name}` → `{fmt}`" for name, fmt in column.formats_by_filename)
    return f"`{column.format}`" if column.format else _DASH


def _schema(record: SchemaRecord, fields: Mapping[str, dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for index, epoch in enumerate(record.epochs, start=1):
        lines += [
            f"Schema, epoch {index}:",
            "",
            "| Vendor column | Silver column | Dtype | Format | Nullable | Zone | Vendor unit |",
            "|---|---|---|---|---|---|---|",
        ]
        for column in epoch.columns:
            unit = vendor_unit(fields.get(column.source))
            cells = [
                _cell(column.source),
                f"`{column.name}`",
                column.dtype,
                _format_cell(column),
                "yes" if column.nullable else "no",
                column.zone or _DASH,
                _cell(unit) if unit else _DASH,
            ]
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")
    published, available = CLOCKS[record.vintage]
    if record.vintage == "issue_time_evidenced" and record.vintage_evidence:
        published = f"{published}: {record.vintage_evidence}"
    temporal = record.temporal
    recipe: str = temporal.kind
    if temporal.inputs:
        recipe += " (" + ", ".join(f"`{name}`" for name in temporal.inputs) + ")"
    if temporal.kind == "none":
        recipe += ": `timestamp_utc` is the capture time"
    lines += [
        "- Entity key: " + ", ".join(f"`{name}`" for name in record.entity_key),
        f"- Latest: `{record.latest}`"
        + (f" per `{record.latest_partition}`" if record.latest_partition is not None else ""),
        f"- Temporal recipe: {recipe}",
        "",
        "| Clock | Meaning |",
        "|---|---|",
        f"| `published_at` | {_cell(published)} |",
        f"| `available_at` | {_cell(available)} |",
        "",
    ]
    return lines


def _families(
    package: PackageEntry, field_info: Mapping[str, Mapping[str, Any]] | None
) -> list[str]:
    lines = ["## Families", ""]
    for family in sorted(package.families, key=lambda f: f.key):
        lines += [
            f"### `{family.key}`",
            "",
            f"- Kind: {family.kind} · archetype: {family.archetype} · refresh: {family.refresh}"
            f" · empty allowed: {'yes' if family.empty_allowed else 'no'}",
        ]
        if family.legacy:
            lines += ["- bespoke transformer: see the existing page", ""]
        elif family.record is not None:
            document = dict(field_info.get(family.key, {})) if field_info else None
            lines += ["", *_schema(family.record, field_entries(document))]
        elif family.kind == "tabular":
            lines += ["- ingest-only: no silver yet", ""]
        else:
            lines += ["- files: catalogue only", ""]
    return lines


def _extras(snapshot_package: Mapping[str, Any]) -> list[str]:
    extras = snapshot_package.get("extras") or []
    out: list[str] = []
    for item in extras:
        if isinstance(item, dict) and "key" in item:
            out.append(f"- {_cell(str(item['key']))}: {_cell(str(item.get('value', '')))}")
    return out


def _cadence(package: PackageEntry, snapshot_package: Mapping[str, Any]) -> list[str]:
    lines = ["## Cadence", "", f"- Registry refresh: {package.refresh}"]
    for family in sorted(package.families, key=lambda f: f.key):
        if family.refresh != package.refresh:
            lines.append(f"- `{family.key}` refresh: {family.refresh}")
    extras = _extras(snapshot_package)
    if extras:
        lines += ["- Vendor metadata (snapshot extras, verbatim):", *(f"  {e}" for e in extras)]
    if package.refresh == "intraday" or any(f.refresh == "intraday" for f in package.families):
        lines.append("- Intraday: captured once per refresh run — sampled, not complete")
    lines.append("")
    return lines


def _holds(package: PackageEntry) -> list[str]:
    lines = ["## Holds", ""]
    held: list[str] = []
    if isinstance(package.eligibility, Held):
        held.append(f"- package: {_cell(describe_eligibility(package.eligibility))}")
    for family in sorted(package.families, key=lambda f: f.key):
        if not (family.legacy or family.record is not None):
            continue
        eligibility = effective_eligibility(package, family)
        if isinstance(eligibility, Held):
            held.append(f"- `{family.key}`: {_cell(describe_eligibility(eligibility))}")
    lines += held or ["- none"]
    lines.append("")
    return lines


def render_package(
    registry: Registry,
    snapshot_package: Mapping[str, Any],
    field_info: Mapping[str, Mapping[str, Any]] | None = None,
) -> str:
    """Render one package's skeleton page (pure, deterministic).

    Args:
        registry: The loaded registry.
        snapshot_package: The package's catalogue snapshot entry (``name``,
            ``title``, ``organization``, ``license_title``, ``extras``).
        field_info: Family key -> field-info document, for vendor units.

    Returns:
        The page text, LF line endings, ending in one newline.

    Raises:
        KeyError: The snapshot package is not a registry package.
    """
    slug = str(snapshot_package["name"])
    package = next((p for p in registry.packages if p.package == slug), None)
    if package is None:
        raise KeyError(f"package {slug!r} is not in the registry")
    title = str(snapshot_package.get("title") or slug)
    organization = snapshot_package.get("organization") or {}
    org_title = organization.get("title") if isinstance(organization, dict) else None
    licence = snapshot_package.get("license_title")
    lines = [
        *_front_matter(package),
        "",
        f"# {title}",
        "",
        "> TODO: overview prose (docs wave, gridflow-dataset-spec).",
        "",
        f"- Publisher group: {_cell(str(org_title)) if org_title else _DASH}",
        f"- Registry group: {package.group} · archetype: {package.archetype}",
        "",
        *_files(package),
        *_families(package, field_info),
        *_cadence(package, snapshot_package),
        "## Licence and attribution",
        "",
        f"- Licence: {_cell(str(licence)) if licence else _DASH}",
        f"- Attribution: {ATTRIBUTION}",
        "",
        *_holds(package),
    ]
    while lines and lines[-1] == "":
        lines.pop()
    return "\n".join(lines) + "\n"


def _is_skeleton(path: Path) -> bool:
    text = path.read_bytes().decode("utf-8", errors="replace").replace("\r\n", "\n")
    if not text.startswith("---\n"):
        return False
    end = text.find("\n---\n", 4)
    front = text[4:end] if end >= 0 else ""
    return SKELETON_MARKER in front.split("\n")


def write_skeletons(out: Path, pages: Mapping[str, str]) -> list[Path]:
    """Write ``<out>/<slug>.md`` for every page, all or nothing (I-3).

    Args:
        out: The output directory (created if absent).
        pages: Slug -> page text.

    Returns:
        The written paths, sorted.

    Raises:
        SkeletonRefusedError: A target exists without ``skeleton: true``;
            nothing was written.
    """
    targets = {slug: out / f"{slug}.md" for slug in sorted(pages)}
    refused = sorted(
        str(path) for path in targets.values() if path.exists() and not _is_skeleton(path)
    )
    if refused:
        raise SkeletonRefusedError(
            f"refusing to overwrite pages that are not generated skeletons: {refused}"
        )
    out.mkdir(parents=True, exist_ok=True)
    for slug, path in targets.items():
        replace_atomically(path, pages[slug].encode("utf-8"))
    return sorted(targets.values())


def main(argv: list[str] | None = None) -> int:
    """Entry point. Exit 0 written, 1 refused or failed evidence, 2 usage."""
    parser = argparse.ArgumentParser(prog="python -m gridflow.connectors.neso_data_portal.skeleton")
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--package", action="append", default=None)
    parser.add_argument("--field-info", type=Path, default=None)
    args = parser.parse_args(argv)

    try:
        snapshot = load_snapshot(args.snapshot)
        field_info = (
            load_field_info(args.field_info, str(snapshot.get("snapshot_id")))
            if args.field_info is not None
            else None
        )
    except EvidenceError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    registry = registry_module.load_registry()
    by_slug = {str(p.get("name")): p for p in snapshot["packages"] if isinstance(p, dict)}
    registered = [p.package for p in registry.packages]
    wanted = sorted(set(args.package)) if args.package else registered
    unknown = [slug for slug in wanted if slug not in by_slug or slug not in registered]
    if unknown:
        print(f"packages not in both the snapshot and the registry: {unknown}", file=sys.stderr)
        return 2
    pages = {slug: render_package(registry, by_slug[slug], field_info) for slug in wanted}
    try:
        written = write_skeletons(args.out, pages)
    except SkeletonRefusedError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    for path in written:
        print(path.name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
