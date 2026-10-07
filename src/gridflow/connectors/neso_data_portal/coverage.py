"""Snapshot coverage check for NESO Data Portal bronze (ADR-033 P-11, A4).

Usage::

    python -m gridflow.connectors.neso_data_portal.coverage --snapshot <path> \
        [--data-dir <dir>] [--verify-sha] [--json <out>]

The **expected set comes from the snapshot only**, never from bronze: every
snapshot resource must be ``captured`` (some usable capture carries its id) or
``adjudicated`` (a seat ruling in ``_adjudications.json``). A bronze-derived
check is clean whenever every capture present is valid, so it cannot see a
resource that was never captured at all; this one can.

Exit codes: 0 clean, 1 gap (a ``missing`` or ``unusable`` resource, an
unregistered or unfrozen key, or a hash failure), 2 usage.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from gridflow.connectors.neso_data_portal import captures as captures_module
from gridflow.connectors.neso_data_portal import registry as registry_module
from gridflow.connectors.neso_data_portal.files import replace_atomically

if TYPE_CHECKING:
    from gridflow.connectors.neso_data_portal.captures import Capture, UnusableCapture

__all__ = ["CoverageReport", "ResourceRef", "build_report", "main"]

_HASH_CHUNK = 1024 * 1024


@dataclass(frozen=True)
class ResourceRef:
    """One snapshot resource, as the report names it."""

    package: str
    resource_id: str
    name: str


@dataclass
class CoverageReport:
    """The coverage verdict for one snapshot against one bronze tree."""

    counts: dict[str, int] = field(default_factory=dict)
    missing: list[ResourceRef] = field(default_factory=list)
    unusable: list[tuple[ResourceRef, list[str]]] = field(default_factory=list)
    unregistered_keys: list[str] = field(default_factory=list)
    unfrozen_keys: list[str] = field(default_factory=list)
    orphans: list[str] = field(default_factory=list)
    temps: list[str] = field(default_factory=list)
    hash_failures: list[str] = field(default_factory=list)
    unattributed: list[str] = field(default_factory=list)

    @property
    def clean(self) -> bool:
        """No gap of any kind."""
        return not (
            self.missing
            or self.unusable
            or self.unregistered_keys
            or self.unfrozen_keys
            or self.hash_failures
        )

    def as_dict(self) -> dict[str, Any]:
        """A JSON-serialisable rendering."""
        return {
            "clean": self.clean,
            "counts": self.counts,
            "missing": [vars(ref) for ref in self.missing],
            "unusable": [{**vars(ref), "reasons": reasons} for ref, reasons in self.unusable],
            "unregistered_keys": self.unregistered_keys,
            "unfrozen_keys": self.unfrozen_keys,
            "orphans": self.orphans,
            "temps": self.temps,
            "hash_failures": self.hash_failures,
            "unattributed": self.unattributed,
        }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_HASH_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def build_report(
    snapshot: dict[str, Any], data_dir: Path, *, verify_sha: bool = False
) -> CoverageReport:
    """Classify every snapshot resource against the bronze tree under ``data_dir``.

    Args:
        snapshot: A catalogue snapshot (``packages[].resources[]``), either the
            snapshot of record or its trimmed fixture.
        data_dir: The pipeline data root.
        verify_sha: Re-hash every usable capture's body (the only body read).

    Returns:
        The :class:`CoverageReport`.
    """
    registry = registry_module.load_registry()
    frozen = {row.key for row in registry_module.load_frozen_keys(registry.root)}
    adjudicated = {row.resource_id for row in registry_module.load_adjudications(registry.root)}
    report = CoverageReport()

    report.unregistered_keys = captures_module.unregistered_bronze_dirs(data_dir, registry)
    root = captures_module.bronze_source_dir(data_dir)
    usable: list[Capture] = []
    unusable: list[UnusableCapture] = []
    if root.is_dir():
        for dataset_dir in sorted(p for p in root.iterdir() if p.is_dir()):
            scan = captures_module.scan_dataset(dataset_dir, registry)
            usable.extend(scan.captures)
            unusable.extend(scan.unusable)
            report.orphans.extend(str(p) for p in scan.orphans)
            report.temps.extend(str(p) for p in scan.temps)
            has_sidecar = bool(scan.captures or scan.unusable)
            if has_sidecar and dataset_dir.name not in frozen:
                report.unfrozen_keys.append(dataset_dir.name)

    if verify_sha:
        for capture in usable:
            if _sha256(capture.body) != capture.body_sha256:
                report.hash_failures.append(str(capture.body))

    captured_ids = {capture.resource_id for capture in usable}
    unusable_reasons: dict[str, list[str]] = {}
    for item in unusable:
        if item.resource_id is None:
            # Unreadable enough to name no resource; the resource it belonged to
            # is reported missing unless another capture covers it.
            report.unattributed.append(f"{item.sidecar}: {item.reason}")
        else:
            unusable_reasons.setdefault(item.resource_id, []).append(
                f"{item.sidecar.name}: {item.reason}"
            )

    counts = {"captured": 0, "adjudicated": 0, "unusable": 0, "missing": 0}
    snapshot_ids: set[str] = set()
    for package in snapshot.get("packages", []):
        for resource in package.get("resources", []):
            ref = ResourceRef(
                package=str(package.get("name", "")),
                resource_id=str(resource.get("id", "")),
                name=str(resource.get("name", "")),
            )
            snapshot_ids.add(ref.resource_id)
            if ref.resource_id in captured_ids:
                counts["captured"] += 1
            elif ref.resource_id in adjudicated:
                counts["adjudicated"] += 1
            elif ref.resource_id in unusable_reasons:
                counts["unusable"] += 1
                report.unusable.append((ref, unusable_reasons[ref.resource_id]))
            else:
                counts["missing"] += 1
                report.missing.append(ref)
    counts["captures_outside_snapshot"] = len(captured_ids - snapshot_ids)
    counts["orphans"] = len(report.orphans)
    counts["temps"] = len(report.temps)
    report.counts = counts
    return report


def _print_report(report: CoverageReport) -> None:
    counts = report.counts
    print(
        "coverage: "
        + ", ".join(
            f"{name} {counts[name]}" for name in ("captured", "adjudicated", "unusable", "missing")
        )
    )
    print(
        f"unregistered keys {len(report.unregistered_keys)}, unfrozen keys "
        f"{len(report.unfrozen_keys)}, orphans {counts['orphans']}, temps {counts['temps']}, "
        f"hash failures {len(report.hash_failures)}, captures outside the snapshot "
        f"{counts['captures_outside_snapshot']}"
    )
    for key in report.unregistered_keys:
        print(f"  unregistered: {key}")
    for key in report.unfrozen_keys:
        print(f"  unfrozen: {key}")
    for ref in report.missing:
        print(f"  missing: {ref.package} {ref.resource_id} {ref.name!r}")
    for ref, reasons in report.unusable:
        print(f"  unusable: {ref.package} {ref.resource_id} {ref.name!r}")
        for reason in reasons:
            print(f"    {reason}")
    for path in report.hash_failures:
        print(f"  hash failure: {path}")
    for line in report.unattributed:
        print(f"  unattributed unusable sidecar: {line}")
    print("CLEAN" if report.clean else "GAP")


def main(argv: list[str] | None = None) -> int:
    """Entry point. Exit 0 clean, 1 gap, 2 usage."""
    parser = argparse.ArgumentParser(prog="python -m gridflow.connectors.neso_data_portal.coverage")
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--verify-sha", action="store_true")
    parser.add_argument("--json", type=Path, default=None)
    args = parser.parse_args(argv)

    try:
        snapshot = json.loads(Path(args.snapshot).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        print(f"cannot read snapshot {args.snapshot}: {exc}", file=sys.stderr)
        return 2
    if not isinstance(snapshot, dict) or not isinstance(snapshot.get("packages"), list):
        print(f"{args.snapshot} is not a catalogue snapshot (no packages list)", file=sys.stderr)
        return 2

    data_dir: Path
    if args.data_dir is not None:
        data_dir = args.data_dir
    else:
        from gridflow.config.settings import load_settings

        data_dir = load_settings().pipeline.data_dir

    report = build_report(snapshot, data_dir, verify_sha=args.verify_sha)
    _print_report(report)
    if args.json is not None:
        replace_atomically(
            args.json, (json.dumps(report.as_dict(), indent=2) + "\n").encode("utf-8")
        )
    return 0 if report.clean else 1


if __name__ == "__main__":
    raise SystemExit(main())
