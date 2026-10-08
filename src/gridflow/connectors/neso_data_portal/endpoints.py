"""CKAN dataset tables for the NESO Open Data Portal (D-28, D-03, D-24; ADR-033).

Since ADR-033 both tables are **generated from the registry**
(``registry/<package>.json``): :data:`FAMILIES` holds every gridflow dataset
key, and :data:`DATASETS` is the legacy view of the three keys onboarded before
the registry, which keep their bespoke transformers and the in-code header
contracts below. Nothing here lists a key by hand any more.

CKAN identity lives **in code, not in YAML** (D-28). ``DatasetConfig`` and
``SourceConfig`` are declared with ``extra="ignore"``, so an unrecognised YAML
key is dropped in silence — a package slug or a resource name that silently
vanished would turn into a fetch against the wrong resource with no error
anywhere. Keeping the table here also avoids a shared-model change for one
source, exactly as every other vendor keeps its endpoint table in code.

Resource selection is by **exact ``resources[].name`` string match** (D-04) and
never by UUID or by a hardcoded download URL: the raw filenames are date-stamped
and change on every refresh (``embedded-register-14-august-2026.csv``), and the
``url`` field is a 302 redirector to a presigned URL with a 7-day expiry. The
registry records resource UUIDs as capture identity and coverage evidence;
they are never a selector (D-03, D-12).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from gridflow.connectors.neso_data_portal import registry as registry_module

if TYPE_CHECKING:
    from gridflow.connectors.neso_data_portal.registry import Registry

__all__ = [
    "CKAN_ACTION_PREFIX",
    "DATASETS",
    "DATASTORE_DUMP_PREFIX",
    "FAMILIES",
    "CkanDataset",
    "FamilySpec",
    "build_action_url",
    "build_datasets",
    "build_dump_path",
    "build_families",
    "is_canonical_resource_id",
]

CKAN_ACTION_PREFIX = "/api/3/action"

DATASTORE_DUMP_PREFIX = "/datastore/dump"
"""CKAN's datastore dump route; the resource id is its only path segment (ADR-035)."""

# The canonical lowercase UUID ``BronzeWriter.publish_capture`` requires
# (``bronze/writer.py``), copied rather than imported: this table must not
# import the bronze writer.
_RESOURCE_ID_PATTERN = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


@dataclass(frozen=True)
class CkanDataset:
    """One gridflow dataset's CKAN identity and download contract.

    Attributes:
        package: The CKAN package slug, passed as ``package_show?id=``.
        resource_name: The exact ``resources[].name`` to select (D-04). Zero
            matches or more than one is a hard error — no fuzzy match, no
            "Archive"-substring fallback, no ``last_modified`` tie-break.
        expected_format: The CKAN ``format`` the selected resource must
            declare. ``RawResponse.content_type`` is stamped from this, never
            from the response header (D-10), because the presigned host serves
            ``application/octet-stream`` and a ``.bin`` bronze body would be
            invisible to the transformer's ``raw_*.csv`` glob.
        expected_columns: The CSV header contract, enforced at fetch time by
            D-36's admission parse and again at transform time. Exact and
            ordered; drift fails loud.
        max_download_bytes: The streaming size cap (T-NDP-02). An order of
            magnitude of headroom over the observed size, so a legitimate
            publication growth does not false-refuse while a vendor-controlled
            unbounded body still cannot exhaust memory.
    """

    package: str
    resource_name: str
    expected_format: str
    expected_columns: tuple[str, ...]
    max_download_bytes: int


# The 34-column ``historic-generation-mix`` header, read verbatim from the
# Stage-A capture ``_probe/sample_historic-generation-mix.csv``, which D-24
# names as the authority. (D-24's prose said 37 through plan revision 13; the
# count was corrected to 34 in revision 14 after being counted from the file.)
_HISTORIC_GENERATION_MIX_COLUMNS: tuple[str, ...] = (
    "DATETIME",
    "GAS",
    "COAL",
    "NUCLEAR",
    "WIND",
    "WIND_EMB",
    "HYDRO",
    "IMPORTS",
    "BIOMASS",
    "OTHER",
    "SOLAR",
    "STORAGE",
    "GENERATION",
    "CARBON_INTENSITY",
    "LOW_CARBON",
    "ZERO_CARBON",
    "RENEWABLE",
    "FOSSIL",
    "GAS_perc",
    "COAL_perc",
    "NUCLEAR_perc",
    "WIND_perc",
    "WIND_EMB_perc",
    "HYDRO_perc",
    "IMPORTS_perc",
    "BIOMASS_perc",
    "OTHER_perc",
    "SOLAR_perc",
    "STORAGE_perc",
    "GENERATION_perc",
    "LOW_CARBON_perc",
    "ZERO_CARBON_perc",
    "RENEWABLE_perc",
    "FOSSIL_perc",
)


# The in-code header contracts of the three legacy keys, enforced at fetch
# time by D-36's admission parse and again at transform time. The registry
# carries membership and caps; it does not carry header contracts.
_LEGACY_EXPECTED_COLUMNS: dict[str, tuple[str, ...]] = {
    "daily_wind_availability": ("BMU_ID", "Date", "MW"),
    "historic_generation_mix": _HISTORIC_GENERATION_MIX_COLUMNS,
    "embedded_wind_solar_forecast": (
        "DATE_GMT",
        "TIME_GMT",
        "SETTLEMENT_DATE",
        "SETTLEMENT_PERIOD",
        "EMBEDDED_WIND_FORECAST",
        "EMBEDDED_WIND_CAPACITY",
        "EMBEDDED_SOLAR_FORECAST",
        "EMBEDDED_SOLAR_CAPACITY",
    ),
}

# Master's DATASETS order, kept so the generated dict iterates identically.
_LEGACY_ORDER: tuple[str, ...] = (
    "daily_wind_availability",
    "historic_generation_mix",
    "embedded_wind_solar_forecast",
)


@dataclass(frozen=True)
class FamilySpec:
    """One registry family, flattened for the connector (P-3).

    Attributes:
        package: The CKAN package slug the family's resources live in.
        key: The gridflow dataset key.
        kind: ``tabular`` (CSV resources) or ``files`` (everything else).
        legacy: One of the three keys onboarded before the registry.
        empty_allowed: A header-only CSV member is captured, marked empty (P-7).
        max_download_bytes: The streaming size cap per member (A9).
        names: The exact ``(name, FORMAT)`` pairs seeded under the key.
        name_regex: An optional anchored selector for future members; matched
            with ``re.fullmatch`` only, and only for a format already seeded.
        refresh: The registry's refresh class. Read only by the dump leg's
            frozen-class cadence gate (ADR-035 P-7).
    """

    package: str
    key: str
    kind: Literal["tabular", "files"]
    legacy: bool
    empty_allowed: bool
    max_download_bytes: int
    names: frozenset[tuple[str, str]]
    name_regex: str | None
    refresh: str

    @property
    def formats(self) -> frozenset[str]:
        """The upper-case CKAN formats of the family's seeded resources."""
        return frozenset(fmt for _name, fmt in self.names)

    def selects(self, name: str, fmt: str) -> bool:
        """Whether a live resource ``(name, fmt)`` is a member (P-6).

        Names are compared as stored: no strip, no case-fold, no
        normalisation, no fuzzy match.
        """
        upper = fmt.upper()
        if (name, upper) in self.names:
            return True
        if self.name_regex is None or upper not in self.formats:
            return False
        return re.fullmatch(self.name_regex, name) is not None


def build_families(registry: Registry) -> dict[str, FamilySpec]:
    """Flatten every registry family into a :class:`FamilySpec`, keyed by key."""
    return {
        key: FamilySpec(
            package=package.package,
            key=key,
            kind=family.kind,
            legacy=family.legacy,
            empty_allowed=family.empty_allowed,
            max_download_bytes=family.max_download_bytes,
            names=registry.family_names(key),
            name_regex=family.name_regex,
            refresh=family.refresh,
        )
        for key, (package, family) in registry.families.items()
    }


def build_datasets(families: dict[str, FamilySpec]) -> dict[str, CkanDataset]:
    """Generate the legacy :class:`CkanDataset` view from the legacy families.

    Raises:
        RuntimeError: A legacy family is missing or does not hold exactly one
            seeded member; its single exact name is its selector (D-04).
    """
    datasets: dict[str, CkanDataset] = {}
    for key in _LEGACY_ORDER:
        family = families.get(key)
        if family is None or not family.legacy:
            raise RuntimeError(f"registry has no legacy family {key!r}")
        if len(family.names) != 1:
            raise RuntimeError(
                f"legacy family {key!r} must hold exactly one member, has {sorted(family.names)}"
            )
        ((resource_name, expected_format),) = family.names
        datasets[key] = CkanDataset(
            package=family.package,
            resource_name=resource_name,
            expected_format=expected_format,
            expected_columns=_LEGACY_EXPECTED_COLUMNS[key],
            max_download_bytes=family.max_download_bytes,
        )
    return datasets


FAMILIES: dict[str, FamilySpec] = build_families(registry_module.load_registry())

DATASETS: dict[str, CkanDataset] = build_datasets(FAMILIES)


def build_action_url(action: str, **params: str) -> tuple[str, dict[str, str]]:
    """Build the path and query dict for one CKAN action call.

    Args:
        action: The CKAN action name, e.g. ``package_show``.
        **params: Query parameters, e.g. ``id="daily-wind-availability"``.

    Returns:
        A ``(path, params)`` pair. The path is relative, so httpx resolves it
        against the client's ``base_url`` — no URL is ever hand-built, and no
        URL taken from a response body is ever fetched (D-39 §1a).
    """
    return f"{CKAN_ACTION_PREFIX}/{action}", dict(params)


def is_canonical_resource_id(resource_id: str) -> bool:
    """Whether ``resource_id`` is a canonical lowercase CKAN resource UUID."""
    return _RESOURCE_ID_PATTERN.fullmatch(resource_id) is not None


def build_dump_path(resource_id: str) -> str:
    """Build the datastore dump path for one registry-seeded resource id (ADR-035).

    The path is built from the id alone and never read from ``resources[].url``
    (D-39): the dump URL is a vendor-supplied field, the id is the registry's.
    The path is relative, so httpx resolves it against ``base_url``.

    Args:
        resource_id: A canonical lowercase UUID.

    Returns:
        ``/datastore/dump/<resource_id>``.

    Raises:
        ValueError: ``resource_id`` is not a canonical lowercase UUID, so no
            path segment other than the id can be smuggled in.
    """
    if not is_canonical_resource_id(resource_id):
        raise ValueError(f"datastore resource id {resource_id!r} is not a canonical lowercase UUID")
    return f"{DATASTORE_DUMP_PREFIX}/{resource_id}"
