"""Read-only scanner of NESO Data Portal bronze captures (ADR-033 P-4, P-10).

One scanner, three consumers: the bind-time key-freeze pin (P-4), the
connector's unchanged-member skip (P-10, A5) and the coverage command (P-11).
It never writes, never deletes and never reads a body's bytes.

**Visibility contract (P-9).** A capture exists iff its sidecar exists; the
sidecar is the commit marker. A body without a sidecar is an *orphan*, a
``.tmp_*`` file is an interrupted publication, and both are reported, never
treated as captures.

**The usable rule** is stated once, in :func:`_usable_reason`. A sidecar that
fails it is never a skip basis, so the member is fetched again (fail-open
toward capture), and coverage reports it.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any

from gridflow.storage.paths import PathBuilder

if TYPE_CHECKING:
    from collections.abc import Iterable
    from pathlib import Path

    from gridflow.connectors.neso_data_portal.registry import Registry

logger = logging.getLogger(__name__)

__all__ = [
    "SOURCE",
    "Capture",
    "RegistryFreezeError",
    "ScanResult",
    "UnusableCapture",
    "assert_bronze_dirs_registered",
    "newest_by_resource",
    "scan_dataset",
    "unregistered_bronze_dirs",
]

SOURCE = "neso_data_portal"
SIDECAR_SUFFIX = ".meta.json"
TEMP_PREFIX = ".tmp_"


class RegistryFreezeError(Exception):
    """Bronze exists under a directory the registry no longer declares (I-F).

    Raised before any send: a key with bronze was renamed or removed, so
    every capture filed under it would become unreachable.
    """


@dataclass(frozen=True)
class Capture:
    """A usable capture: a committed sidecar whose body and provenance check out.

    Attributes:
        sidecar: The ``raw_*.meta.json`` path.
        body: Its one sibling body.
        resource_id: The CKAN resource UUID — capture identity, not a selector.
        package: The CKAN package slug from the sidecar.
        resource_name: The resource name the capture was selected by.
        ckan_last_modified: CKAN's ``last_modified`` string at capture time.
        written_at: When the capture became durable, tz-aware.
        body_sha256: The recorded body digest.
        body_size_bytes: The recorded body size.
    """

    sidecar: Path
    body: Path
    resource_id: str
    package: str
    resource_name: str
    ckan_last_modified: str
    written_at: datetime
    body_sha256: str
    body_size_bytes: int


@dataclass(frozen=True)
class UnusableCapture:
    """A sidecar that fails the usable rule, with the first failing clause."""

    sidecar: Path
    reason: str
    resource_id: str | None


@dataclass(frozen=True)
class ScanResult:
    """Everything one dataset directory holds, classified."""

    captures: tuple[Capture, ...]
    unusable: tuple[UnusableCapture, ...]
    orphans: tuple[Path, ...]
    temps: tuple[Path, ...]


def bronze_source_dir(data_dir: Path) -> Path:
    """Return ``<data_dir>/bronze/neso_data_portal`` (via :class:`PathBuilder`)."""
    return PathBuilder(data_dir).bronze_source_dir(SOURCE)


def unregistered_bronze_dirs(data_dir: Path, registry: Registry) -> list[str]:
    """Return the bronze dataset directories that are not registry keys.

    Reads directory names only — never the freeze ledger — so captures a sweep
    makes under new keys cannot block that sweep's later binds (P-4).
    """
    root = bronze_source_dir(data_dir)
    if not root.is_dir():
        return []
    return sorted(
        child.name
        for child in root.iterdir()
        if child.is_dir() and child.name not in registry.families
    )


def assert_bronze_dirs_registered(data_dir: Path, registry: Registry) -> None:
    """Raise :class:`RegistryFreezeError` if any bronze directory is unregistered.

    Args:
        data_dir: The pipeline data root.
        registry: The loaded registry.
    """
    unregistered = unregistered_bronze_dirs(data_dir, registry)
    if unregistered:
        raise RegistryFreezeError(
            f"bronze/{SOURCE} holds captures under keys the registry does not declare: "
            f"{unregistered}. A key with bronze is frozen (ADR-033 I-F); restore it in the "
            "registry before any NESO ingest."
        )


def _body_stem(name: str) -> str:
    return name.rsplit(".", 1)[0]


def _parse_written_at(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def _usable_reason(
    meta: Any,
    bodies: list[Path],
    dataset_key: str,
    registry: Registry,
) -> str | None:
    """The usable rule (P-10, amended by REVIEW-PLAN-3 M1). ``None`` = usable.

    Clauses in order; the first failure is the recorded reason:

    1. the sidecar is a JSON object whose ``written_at`` parses tz-aware;
    2. exactly one sibling body exists and its size equals ``body_size_bytes``;
    3. ``provenance_for(body)`` accepts it — the D-23 rule at its single site,
       called, not copied;
    4. registry identity: the directory is a registry family, the provenance
       ``package`` is that family's package, and the family's selector accepts
       the recorded ``(resource_name, ckan_format)``. The resource UUID is
       deliberately NOT required to be a seeded id: NESO may recreate a
       resource under a new UUID (ADR-030 D-03), and selection follows the
       name, so the capture it produced must remain a skip basis.
    """
    if not isinstance(meta, dict):
        return "sidecar is not a JSON object"
    if _parse_written_at(meta.get("written_at")) is None:
        return "written_at is missing, unparseable or naive"

    if len(bodies) != 1:
        return f"expected exactly one sibling body, found {len(bodies)}"
    declared = meta.get("body_size_bytes")
    if not isinstance(declared, int) or isinstance(declared, bool):
        return "body_size_bytes is missing or not an integer"
    actual = bodies[0].stat().st_size
    if actual != declared:
        return f"body is {actual} B but the sidecar records {declared} B"

    # Imported lazily: the silver package import loads the three transformers.
    from gridflow.silver.neso_data_portal._bronze import provenance_for

    provenance = provenance_for(bodies[0])
    if provenance is None:
        return "provenance_for rejected the sidecar (D-23; see its WARNING)"

    entry = registry.families.get(dataset_key)
    if entry is None:
        return f"directory {dataset_key!r} is not a registry family"
    package, _family = entry
    if provenance.package != package.package:
        return (
            f"package {provenance.package!r} is not family {dataset_key!r}'s package "
            f"{package.package!r}"
        )
    params = meta.get("request_params")
    ckan_format = params.get("ckan_format") if isinstance(params, dict) else None
    if not isinstance(ckan_format, str) or not ckan_format:
        return "request_params.ckan_format is missing or empty"
    if not registry.family_selects(dataset_key, provenance.resource_name, ckan_format):
        return (
            f"family {dataset_key!r} does not select resource "
            f"({provenance.resource_name!r}, {ckan_format!r})"
        )
    return None


def _raw_resource_id(meta: Any) -> str | None:
    if not isinstance(meta, dict):
        return None
    params = meta.get("request_params")
    if not isinstance(params, dict):
        return None
    value = params.get("resource_id")
    return value if isinstance(value, str) and value else None


def scan_dataset(dataset_dir: Path, registry: Registry) -> ScanResult:
    """Classify every file under one ``bronze/neso_data_portal/<key>/`` tree.

    Args:
        dataset_dir: The dataset directory; its name is the family key.
        registry: The loaded registry.

    Returns:
        Usable captures, unusable sidecars with reasons, orphans and temps.
    """
    captures: list[Capture] = []
    unusable: list[UnusableCapture] = []
    orphans: list[Path] = []
    temps: list[Path] = []
    if not dataset_dir.is_dir():
        return ScanResult((), (), (), ())

    by_dir: dict[Path, list[Path]] = {}
    for path in sorted(dataset_dir.rglob("*")):
        if path.is_file():
            by_dir.setdefault(path.parent, []).append(path)

    for files in by_dir.values():
        sidecars = [
            p for p in files if p.name.startswith("raw_") and p.name.endswith(SIDECAR_SUFFIX)
        ]
        bodies = [
            p for p in files if p.name.startswith("raw_") and not p.name.endswith(SIDECAR_SUFFIX)
        ]
        temps.extend(p for p in files if p.name.startswith(TEMP_PREFIX))
        sidecar_stems = {p.name[: -len(SIDECAR_SUFFIX)] for p in sidecars}
        orphans.extend(p for p in bodies if _body_stem(p.name) not in sidecar_stems)

        for sidecar in sidecars:
            stem = sidecar.name[: -len(SIDECAR_SUFFIX)]
            siblings = [p for p in bodies if _body_stem(p.name) == stem]
            try:
                meta: Any = json.loads(sidecar.read_text(encoding="utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
                unusable.append(UnusableCapture(sidecar, f"sidecar unreadable ({exc})", None))
                continue
            reason = _usable_reason(meta, siblings, dataset_dir.name, registry)
            if reason is not None:
                unusable.append(UnusableCapture(sidecar, reason, _raw_resource_id(meta)))
                continue
            params = meta["request_params"]
            written_at = _parse_written_at(meta["written_at"])
            assert written_at is not None  # clause 1 established it
            captures.append(
                Capture(
                    sidecar=sidecar,
                    body=siblings[0],
                    resource_id=str(params["resource_id"]),
                    package=str(params["package"]),
                    resource_name=str(params["resource_name"]),
                    ckan_last_modified=str(params["ckan_last_modified"]),
                    written_at=written_at,
                    body_sha256=str(meta.get("body_sha256", "")),
                    body_size_bytes=int(meta["body_size_bytes"]),
                )
            )

    for item in unusable:
        logger.warning(
            "%s/%s: capture %s is not usable: %s",
            SOURCE,
            dataset_dir.name,
            item.sidecar.name,
            item.reason,
        )
    return ScanResult(tuple(captures), tuple(unusable), tuple(orphans), tuple(temps))


def newest_by_resource(captures: Iterable[Capture]) -> dict[str, Capture]:
    """Return each resource id's newest capture: max of ``(written_at, str(body))``."""
    newest: dict[str, Capture] = {}
    for capture in captures:
        current = newest.get(capture.resource_id)
        if current is None or (capture.written_at, str(capture.body)) > (
            current.written_at,
            str(current.body),
        ):
            newest[capture.resource_id] = capture
    return newest
