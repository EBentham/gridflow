"""The NESO Data Portal registry: one JSON file per CKAN package (ADR-033).

The registry is the single source of truth for which gridflow dataset key
(a *family*) owns which CKAN resource, how each resource is dispositioned, and
the per-family download contract. It was seeded once from the ratified dataset
matrix by ``scripts/seed_neso_registry.py``; the JSON files beside this module
are the artifact, edited by registry commits from then on.

Layout. Every ``*.json`` here whose name does not start with ``_`` is one
package. The two ``_``-prefixed ledgers are read by their own loaders and are
never parsed as packages:

- ``_frozen_keys.json`` — keys that have bronze and therefore cannot be renamed
  or removed (P-4's CI pin; the runtime pin reads bronze directory names).
- ``_adjudications.json`` — snapshot resources the coverage check accepts
  without a capture, each with a recorded seat ruling (P-11).

**Test seam.** Runtime consumers call :func:`load_registry` through this module
attribute at call time and never keep a module-level copy, so a test can
monkeypatch ``gridflow.connectors.neso_data_portal.registry.load_registry`` to
return a registry loaded from a temporary directory.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from functools import cache
from importlib import resources as importlib_resources
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

if TYPE_CHECKING:
    from collections.abc import Iterable
    from importlib.resources.abc import Traversable

__all__ = [
    "KEY_PATTERN",
    "LEGACY_KEYS",
    "Adjudication",
    "FamilyEntry",
    "FrozenKey",
    "PackageEntry",
    "Registry",
    "RegistryError",
    "ResourceEntry",
    "dump_json",
    "frozen_key_violations",
    "key_collisions",
    "load_adjudications",
    "load_frozen_keys",
    "load_registry",
]

KEY_PATTERN = re.compile(r"^[a-z][a-z0-9_]{2,39}$")

# The three keys onboarded before the registry existed. They keep their bespoke
# transformers and their in-code header contracts (endpoints.DATASETS).
LEGACY_KEYS: frozenset[str] = frozenset(
    {"daily_wind_availability", "historic_generation_mix", "embedded_wind_solar_forecast"}
)

FROZEN_KEYS_FILE = "_frozen_keys.json"
ADJUDICATIONS_FILE = "_adjudications.json"

Archetype = Literal["SER", "FC", "REG", "EVT", "SCN", "TAR", "FILE"]
Refresh = Literal["daily", "adhoc", "frozen", "monthly", "weekly", "intraday"]


class RegistryError(Exception):
    """The registry on disk is malformed or internally inconsistent."""


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Eligible(_Frozen):
    """The package may be published."""

    status: Literal["eligible"]


class Held(_Frozen):
    """The package is held from publication pending a research unit."""

    status: Literal["held"]
    question: str
    unit: str


Eligibility = Annotated[Eligible | Held, Field(discriminator="status")]


class SilverDisposition(_Frozen):
    """The resource feeds the silver dataset ``key`` (its own family)."""

    kind: Literal["SILVER"]
    key: str


class DocDisposition(_Frozen):
    """Documentation: captured, never transformed."""

    kind: Literal["DOC"]


class GisDisposition(_Frozen):
    """Geospatial file: captured, never transformed by gridflow."""

    kind: Literal["GIS"]


class HoldDisposition(_Frozen):
    """Classification pending the named research unit."""

    kind: Literal["HOLD"]
    reason: str
    unit: str


class CoveredDisposition(_Frozen):
    """Proven value-equivalent to another resource (unused by unit A)."""

    kind: Literal["COVERED"]
    by: str


Disposition = Annotated[
    SilverDisposition | DocDisposition | GisDisposition | HoldDisposition | CoveredDisposition,
    Field(discriminator="kind"),
]


class FamilyEntry(_Frozen):
    """One gridflow dataset key: a set of CKAN resources of one package."""

    key: str
    kind: Literal["tabular", "files"]
    legacy: bool
    archetype: Archetype
    refresh: Refresh
    empty_allowed: bool
    max_download_bytes: int = Field(gt=0)
    name_regex: str | None = None
    transformer: Literal["bespoke"] | None = None


class ResourceEntry(_Frozen):
    """One CKAN resource, its family and its disposition."""

    id: str
    name: str
    format: str
    url_type: Literal["upload", "datastore"]
    family: str
    disposition: Disposition


class PackageEntry(_Frozen):
    """One CKAN package and everything the registry says about it."""

    package: str
    package_id: str
    group: str
    archetype: Archetype
    refresh: Refresh
    eligibility: Eligibility
    families: tuple[FamilyEntry, ...]
    resources: tuple[ResourceEntry, ...]


class FrozenKey(_Frozen):
    """A ledger row: ``key`` has bronze and belongs to ``package``."""

    key: str
    package: str


class Adjudication(_Frozen):
    """A snapshot resource the coverage check accepts without a capture."""

    resource_id: str
    package: str
    reason: str
    evidence: str
    ruling: str


@dataclass(frozen=True)
class Registry:
    """The loaded, validated registry.

    Attributes:
        root: The directory it was loaded from, or ``None`` for package data.
        packages: Every package, sorted by slug.
        families: Key -> ``(package, family)``.
        resources: Resource id -> ``(package, resource)``.
    """

    root: Path | None
    packages: tuple[PackageEntry, ...]
    families: dict[str, tuple[PackageEntry, FamilyEntry]] = field(repr=False)
    resources: dict[str, tuple[PackageEntry, ResourceEntry]] = field(repr=False)
    _names: dict[str, frozenset[tuple[str, str]]] = field(repr=False)

    def family_names(self, key: str) -> frozenset[tuple[str, str]]:
        """Return the exact ``(name, FORMAT)`` pairs seeded under ``key``."""
        return self._names[key]

    def family_formats(self, key: str) -> frozenset[str]:
        """Return the upper-case CKAN formats of ``key``'s seeded resources."""
        return frozenset(fmt for _name, fmt in self._names[key])

    def family_selects(self, key: str, name: str, fmt: str) -> bool:
        """Whether a resource ``(name, fmt)`` satisfies family ``key``'s selector.

        Exact name set first; then the anchored ``name_regex`` with a format
        among the family's seeded formats. Names are compared as stored: no
        strip, no case-fold, no normalisation (P-6).
        """
        upper = fmt.upper()
        if (name, upper) in self._names[key]:
            return True
        _package, family = self.families[key]
        if family.name_regex is None:
            return False
        return upper in self.family_formats(key) and (
            re.fullmatch(family.name_regex, name) is not None
        )


def dump_json(obj: Any) -> str:
    """Serialise ``obj`` the one way every registry file is written."""
    return json.dumps(obj, ensure_ascii=False, indent=2) + "\n"


def _entries(path: Path | None) -> list[tuple[str, Traversable | Path]]:
    if path is None:
        root: Traversable | Path = importlib_resources.files(__name__)
    else:
        if not path.is_dir():
            raise RegistryError(f"registry directory {path} does not exist")
        root = path
    return sorted(
        ((item.name, item) for item in root.iterdir() if item.name.endswith(".json")),
        key=lambda pair: pair[0],
    )


def _read_json(name: str, item: Traversable | Path) -> Any:
    try:
        return json.loads(item.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RegistryError(f"registry file {name}: unreadable ({exc})") from exc


def _validate_package(name: str, entry: PackageEntry) -> dict[str, frozenset[tuple[str, str]]]:
    """Check one package file's internal consistency; return its name sets."""
    family_keys: dict[str, FamilyEntry] = {}
    for family in entry.families:
        if not KEY_PATTERN.fullmatch(family.key):
            raise RegistryError(
                f"registry file {name}: family key {family.key!r} does not match "
                f"{KEY_PATTERN.pattern}"
            )
        if family.key in family_keys:
            raise RegistryError(f"registry file {name}: family key {family.key!r} repeats")
        if family.legacy != (family.key in LEGACY_KEYS):
            raise RegistryError(
                f"registry file {name}: family {family.key!r} has legacy={family.legacy}, "
                f"but the legacy keys are exactly {sorted(LEGACY_KEYS)}"
            )
        if family.legacy != (family.transformer == "bespoke"):
            raise RegistryError(
                f"registry file {name}: family {family.key!r} legacy={family.legacy} "
                f"disagrees with transformer={family.transformer!r}"
            )
        if family.name_regex is not None:
            try:
                re.compile(family.name_regex)
            except re.error as exc:
                raise RegistryError(
                    f"registry file {name}: family {family.key!r} name_regex does not "
                    f"compile ({exc})"
                ) from exc
        family_keys[family.key] = family

    names: dict[str, set[tuple[str, str]]] = {key: set() for key in family_keys}
    seen_pairs: set[tuple[str, str]] = set()
    for resource in entry.resources:
        owner = family_keys.get(resource.family)
        if owner is None:
            raise RegistryError(
                f"registry file {name}: resource {resource.id} names family "
                f"{resource.family!r}, which this file does not declare"
            )
        pair = (resource.name, resource.format.upper())
        if resource.format != resource.format.upper():
            raise RegistryError(
                f"registry file {name}: resource {resource.id} format {resource.format!r} "
                "is not upper-case"
            )
        if pair in seen_pairs:
            raise RegistryError(
                f"registry file {name}: (name, format) {pair!r} repeats within the package"
            )
        seen_pairs.add(pair)
        disposition = resource.disposition
        if isinstance(disposition, SilverDisposition):
            if disposition.key != resource.family:
                raise RegistryError(
                    f"registry file {name}: resource {resource.id} is SILVER under key "
                    f"{disposition.key!r} but belongs to family {resource.family!r}"
                )
            if owner.kind != "tabular":
                raise RegistryError(
                    f"registry file {name}: resource {resource.id} is SILVER in the "
                    f"non-tabular family {resource.family!r}"
                )
        names[resource.family].add(pair)
    return {key: frozenset(pairs) for key, pairs in names.items()}


@cache
def _load(path: Path | None) -> Registry:
    packages: list[PackageEntry] = []
    families: dict[str, tuple[PackageEntry, FamilyEntry]] = {}
    resources: dict[str, tuple[PackageEntry, ResourceEntry]] = {}
    names: dict[str, frozenset[tuple[str, str]]] = {}
    slugs: set[str] = set()

    for name, item in _entries(path):
        if name.startswith("_"):
            continue
        raw = _read_json(name, item)
        try:
            entry = PackageEntry.model_validate(raw)
        except ValidationError as exc:
            raise RegistryError(f"registry file {name}: {exc}") from exc
        if entry.package in slugs:
            raise RegistryError(f"registry file {name}: package {entry.package!r} repeats")
        slugs.add(entry.package)
        package_names = _validate_package(name, entry)
        for family in entry.families:
            if family.key in families:
                raise RegistryError(
                    f"registry file {name}: family key {family.key!r} is also declared by "
                    f"package {families[family.key][0].package!r}"
                )
            families[family.key] = (entry, family)
        for resource in entry.resources:
            if resource.id in resources:
                raise RegistryError(
                    f"registry file {name}: resource id {resource.id} is also declared by "
                    f"package {resources[resource.id][0].package!r}"
                )
            resources[resource.id] = (entry, resource)
        names.update(package_names)
        packages.append(entry)

    packages.sort(key=lambda entry: entry.package)
    return Registry(
        root=path,
        packages=tuple(packages),
        families=families,
        resources=resources,
        _names=names,
    )


def load_registry(path: Path | None = None) -> Registry:
    """Load and validate the registry, cached per ``path``.

    Args:
        path: A directory of package files, or ``None`` for the package data.

    Returns:
        The validated :class:`Registry`.

    Raises:
        RegistryError: A file is unreadable, fails the schema, or the files
            disagree with each other (a key or resource id declared twice, a
            resource naming an undeclared family, a SILVER resource outside its
            own tabular family, an invalid key, a non-compiling ``name_regex``).
    """
    return _load(None if path is None else Path(path))


def _ledger(path: Path | None, filename: str) -> Any:
    if path is None:
        item: Traversable | Path = importlib_resources.files(__name__).joinpath(filename)
    else:
        item = Path(path) / filename
    raw = _read_json(filename, item)
    if not isinstance(raw, list):
        raise RegistryError(f"registry file {filename}: expected a JSON list")
    return raw


def load_frozen_keys(path: Path | None = None) -> tuple[FrozenKey, ...]:
    """Load ``_frozen_keys.json`` from ``path`` (``None`` = package data)."""
    try:
        return tuple(FrozenKey.model_validate(row) for row in _ledger(path, FROZEN_KEYS_FILE))
    except ValidationError as exc:
        raise RegistryError(f"registry file {FROZEN_KEYS_FILE}: {exc}") from exc


def load_adjudications(path: Path | None = None) -> tuple[Adjudication, ...]:
    """Load ``_adjudications.json`` from ``path`` (``None`` = package data)."""
    try:
        return tuple(Adjudication.model_validate(row) for row in _ledger(path, ADJUDICATIONS_FILE))
    except ValidationError as exc:
        raise RegistryError(f"registry file {ADJUDICATIONS_FILE}: {exc}") from exc


def frozen_key_violations(registry: Registry, frozen: Iterable[FrozenKey]) -> list[str]:
    """Return every ledger row the registry no longer honours (P-4's CI pin).

    A frozen key has bronze somewhere, so it must still exist and still belong
    to the same package; a rename or a removal breaks every capture filed
    under it.

    Args:
        registry: The loaded registry.
        frozen: The ``_frozen_keys.json`` rows.

    Returns:
        One message per violated row; empty when the ledger is honoured.
    """
    problems: list[str] = []
    for row in frozen:
        entry = registry.families.get(row.key)
        if entry is None:
            problems.append(f"frozen key {row.key!r} (package {row.package!r}) is not registered")
        elif entry[0].package != row.package:
            problems.append(
                f"frozen key {row.key!r} moved from package {row.package!r} to {entry[0].package!r}"
            )
    return problems


def key_collisions(registry: Registry, registered: Iterable[tuple[str, str]]) -> list[str]:
    """Return registry keys that collide with registered silver datasets (P-3).

    Every non-legacy key must be absent from every source's registered dataset
    names; a legacy key may appear only under ``neso_data_portal``.

    Args:
        registry: The loaded registry.
        registered: ``(source, dataset)`` pairs, e.g. from ``list_transformers()``.

    Returns:
        One message per collision; empty when the key space is disjoint.
    """
    owners: dict[str, set[str]] = {}
    for source, dataset in registered:
        owners.setdefault(dataset, set()).add(source)
    problems: list[str] = []
    for key in sorted(registry.families):
        sources = owners.get(key, set())
        foreign = sources - {"neso_data_portal"} if key in LEGACY_KEYS else sources
        if foreign:
            problems.append(f"registry key {key!r} collides with datasets of {sorted(foreign)}")
    return problems
