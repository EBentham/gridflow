"""Manifest-side invariants I-1..I-7 for N-5 (APPEND_ONLY preferred relation).

Registry population is mandatory and comes first (Sol pass-2 finding 4): the
transformer registry is lazy, populated only by importing the six silver
subpackages. Mirrors tests/silver/test_schema_manifest_contract.py's top-level
imports, plus a session-scoped autouse fixture explicitly calling
``_ensure_silver_transformers_registered()`` so every test here -- in
particular I-6 -- is correct even when run alone (belt and braces,
deliberately).

This suite must not read the DuckDB catalogue (ADR-024): the manifest is a
registry-derived, import-time-only Python API surface.
"""

from __future__ import annotations

import pytest

# Registry side effects: importing subpackages registers their transformers.
import gridflow.silver.elexon  # noqa: F401
import gridflow.silver.entsoe  # noqa: F401
import gridflow.silver.entsog  # noqa: F401
import gridflow.silver.gie  # noqa: F401
import gridflow.silver.neso  # noqa: F401
import gridflow.silver.openmeteo  # noqa: F401
from gridflow.silver import schema_manifest
from gridflow.silver.latest_views import LATEST_VIEW_SPECS
from gridflow.silver.registry import get_transformer, list_transformers
from gridflow.silver.schema_manifest import (
    DECOMMISSIONED_DATASETS,
    SilverSchemaEntry,
    get_silver_schema_manifest,
)

# A HARDCODED census, deliberately: this module is a change-detector, and a
# census derived from the registry would agree with the registry by
# construction and detect nothing. Maintaining it by hand is the designed
# cost. Grew to four when the neso-data-portal phase registered the first
# non-elexon APPEND_ONLY dataset (D-21; PHASE.md ruling 12), then to five with
# that phase's `historic_generation_mix` (B3a/T-16) and to six with its
# `embedded_wind_solar_forecast` (B3a/T-17).
_APPEND_ONLY_DATASETS: tuple[tuple[str, str], ...] = (
    ("elexon", "system_prices"),
    ("elexon", "remit"),
    ("elexon", "fou2t14d"),
    ("neso_data_portal", "daily_wind_availability"),
    ("neso_data_portal", "historic_generation_mix"),
    ("neso_data_portal", "embedded_wind_solar_forecast"),
)


@pytest.fixture(autouse=True, scope="session")
def _registered_transformers() -> None:
    """Populate the lazy transformer registry once for this whole module.

    Belt and braces alongside the top-level subpackage imports above: this is
    what makes I-6 correct when the module -- or a single test in it -- is run
    in isolation, matching how the module actually resolves the registry at
    manifest-build time (``_ensure_silver_transformers_registered``).
    """
    schema_manifest._ensure_silver_transformers_registered()


def _silver_entries() -> tuple[SilverSchemaEntry, ...]:
    return tuple(
        entry
        for entry in get_silver_schema_manifest(include_serving_aliases=False)
        if entry.relation_kind == "silver"
    )


def _entry(source: str, dataset: str) -> SilverSchemaEntry:
    matches = [
        entry for entry in _silver_entries() if entry.source == source and entry.dataset == dataset
    ]
    assert len(matches) == 1, f"expected exactly one silver entry for {source}/{dataset}"
    return matches[0]


def test_append_only_datasets_advertise_latest() -> None:
    """I-1: an APPEND_ONLY dataset with a _latest spec advertises the _latest name."""
    for source, dataset in _APPEND_ONLY_DATASETS:
        entry = _entry(source, dataset)
        assert entry.relation_name == f"silver_{source}_{dataset}_latest"


@pytest.mark.parametrize(
    ("source", "dataset", "expected_base"),
    [
        # I-2a: silver rows -- the dataset-derived formula coincides with the
        # suffix-stripping rule here, and only here, because a silver row's
        # relation IS built from its own source/dataset.
        ("elexon", "system_prices", "silver_elexon_system_prices"),
        ("elexon", "remit", "silver_elexon_remit"),
        ("elexon", "fou2t14d", "silver_elexon_fou2t14d"),
        # A non-APPEND_ONLY silver row: relation_name == qualified_view == base.
        ("elexon", "windfor", "silver_elexon_windfor"),
    ],
)
def test_qualified_view_is_the_base_of_its_own_relation_silver(
    source: str, dataset: str, expected_base: str
) -> None:
    """I-2 (I-2a arm): qualified_view names the all-vintage base of ITS OWN relation."""
    entry = _entry(source, dataset)
    assert entry.qualified_view == expected_base
    assert entry.relation_name.removesuffix("_latest") == entry.qualified_view


@pytest.mark.parametrize(
    ("dataset", "expected_qualified_view"),
    [
        # I-2b: these are the two rows that falsified revision 2's
        # dataset-derived formula -- the public handle name does NOT appear in
        # the underlying relation.
        ("fuel_generation", "silver_elexon_fuelhh"),
        ("weather", "silver_elexon_itsdo"),
        # system_prices' alias row: relation_name is now the _latest name
        # (D-4), so qualified_view must still strip it correctly.
        ("system_prices", "silver_elexon_system_prices"),
    ],
)
def test_qualified_view_is_the_base_of_its_own_relation_silver_backed_alias(
    dataset: str, expected_qualified_view: str
) -> None:
    """I-2 (I-2b arm): silver-backed serving-alias rows, never dataset-derived."""
    entries = get_silver_schema_manifest(include_serving_aliases=True)
    matches = [
        entry
        for entry in entries
        if entry.relation_kind == "serving_alias" and entry.dataset == dataset
    ]
    assert len(matches) == 1
    entry = matches[0]
    assert entry.qualified_view == expected_qualified_view
    assert entry.qualified_view == entry.relation_name.removesuffix("_latest")
    # Never derived from `dataset` -- the public handle name is absent from it.
    assert dataset not in entry.qualified_view or dataset in ("system_prices",)


@pytest.mark.parametrize("dataset", ["gas_storage", "imbalance_context"])
def test_qualified_view_is_the_base_of_its_own_relation_gold(dataset: str) -> None:
    """I-2 (I-2c arm): gold-backed rows keep qualified_view=None, unchanged."""
    entries = get_silver_schema_manifest(include_serving_aliases=True)
    matches = [
        entry for entry in entries if entry.relation_kind == "gold" and entry.dataset == dataset
    ]
    assert len(matches) == 1
    assert matches[0].qualified_view is None


def test_non_append_only_relations_unchanged() -> None:
    """I-3: every non-APPEND_ONLY silver row still matches the pre-N-5 formula.

    Pins `relation_name` and `qualified_view` independently, per field,
    against `f"silver_{source}_{dataset}"` -- an independent baseline that
    does NOT route through `_silver_entry`/`_preferred_relation`. This is
    deliberately NOT a rebuild-and-compare against a monkeypatched manifest:
    an earlier revision reconstructed the manifest with the APPEND_ONLY gate
    neutralised and asserted full dataclass equality against the live one,
    but both builds shared the same `_silver_entry` code path for every
    non-APPEND_ONLY row, so corrupting e.g. `qualified_view` for an
    unrelated dataset would change both sides identically and still pass
    (Sol diff-review pass 2, 2026-08-03).

    This test pins only the standing I-3 invariant above. The one-time
    full-field migration proof (158 non-APPEND_ONLY rows byte-identical on
    every `SilverSchemaEntry` field vs master@cf90abe) is a cross-version
    diff artifact, not a live reconstruction -- see
    `.planning/phases/R2-partition-integrity/N-5-VERIFICATION.md`.
    """
    entries = _silver_entries()
    non_append_only = [
        entry for entry in entries if (entry.source, entry.dataset) not in _APPEND_ONLY_DATASETS
    ]
    assert len(non_append_only) == len(entries) - len(_APPEND_ONLY_DATASETS)
    for entry in non_append_only:
        base = f"silver_{entry.source}_{entry.dataset}"
        assert entry.relation_name == base
        assert entry.qualified_view == base

    # Companion assertion, pinned against the same independent formula (no
    # rebuild): APPEND_ONLY rows differ from it ONLY by the `_latest` suffix
    # on relation_name -- qualified_view still names the all-vintage base.
    for entry in entries:
        if (entry.source, entry.dataset) not in _APPEND_ONLY_DATASETS:
            continue
        base = f"silver_{entry.source}_{entry.dataset}"
        assert entry.qualified_view == base
        assert entry.relation_name == f"{base}_latest"


def test_latest_relations_have_a_view_spec() -> None:
    """I-4: every `_latest`-suffixed relation_name has a LATEST_VIEW_SPECS key.

    Mutation coverage: M3a (dropping the APPEND_ONLY test) only -- M3b (dropping
    the spec-membership test) is invisible here while the APPEND_ONLY and
    LATEST_VIEW_SPECS sets coincide (see I-5/I-6, and N-5-PLAN.md Section 3.5).
    """
    for entry in _silver_entries():
        if entry.relation_name.endswith("_latest"):
            assert (entry.source, entry.dataset) in LATEST_VIEW_SPECS, (
                f"{entry.source}/{entry.dataset} advertises a _latest relation "
                f"with no LATEST_VIEW_SPECS entry"
            )


def test_append_only_without_spec_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """I-5: an APPEND_ONLY transformer with no LATEST_VIEW_SPECS entry raises.

    This is the invariant that detects M3b (dropping the spec-membership test
    from _preferred_relation) -- I-4 cannot see that mutant while the
    APPEND_ONLY and LATEST_VIEW_SPECS sets are equal (Section 3.5). Patches the
    name AS IMPORTED INTO schema_manifest, not the source dict, so the test
    pins the actual call site.
    """
    patched = dict(LATEST_VIEW_SPECS)
    del patched[("elexon", "fou2t14d")]
    monkeypatch.setattr(schema_manifest, "LATEST_VIEW_SPECS", patched)

    with pytest.raises(ValueError, match="fou2t14d"):
        get_silver_schema_manifest(include_serving_aliases=False)


def test_append_only_set_equals_latest_spec_set() -> None:
    """I-6: {d : APPEND_ONLY} == set(LATEST_VIEW_SPECS), both directions.

    Derived from the EXPLICITLY POPULATED registry (the `_registered_transformers`
    autouse fixture + this module's top-level subpackage imports), so this holds
    regardless of test invocation order (F16) -- run this test alone via
    ``pytest ...::test_append_only_set_equals_latest_spec_set`` as the
    order-independence proof.
    """
    registered = sorted(key for key in list_transformers() if key not in DECOMMISSIONED_DATASETS)
    append_only = {
        (source, dataset)
        for source, dataset in registered
        if get_transformer(source, dataset, __import__("pathlib").Path("__test__")).APPEND_ONLY
    }
    spec_set = set(LATEST_VIEW_SPECS)

    # Non-vacuity guard: this invariant must never pass because both sides are
    # empty (F16) -- six APPEND_ONLY datasets are registered today.
    assert len(append_only) > 0
    assert len(append_only) == 6

    missing_specs = append_only - spec_set
    extra_specs = spec_set - append_only
    assert append_only == spec_set, (
        f"APPEND_ONLY datasets with no LATEST_VIEW_SPECS entry: {missing_specs}; "
        f"LATEST_VIEW_SPECS entries for non-APPEND_ONLY datasets: {extra_specs}"
    )


# Keyed by _APPEND_ONLY_DATASETS above, so this table is part of the SAME
# hand-maintained census and grows with it (PHASE.md ruling 12, extended). Each
# value is `_deprecated_aliases`'s documented rule -- `silver_{dataset}` when
# the dataset name is unique across sources, else None -- and is verified
# against the live manifest at edit time rather than copied from a failure
# message.
_EXPECTED_APPEND_ONLY_DEPRECATED_ALIASES: dict[tuple[str, str], str | None] = {
    ("elexon", "system_prices"): "silver_system_prices",
    ("elexon", "remit"): "silver_remit",
    ("elexon", "fou2t14d"): "silver_fou2t14d",
    ("neso_data_portal", "daily_wind_availability"): "silver_daily_wind_availability",
    ("neso_data_portal", "historic_generation_mix"): "silver_historic_generation_mix",
    (
        "neso_data_portal",
        "embedded_wind_solar_forecast",
    ): "silver_embedded_wind_solar_forecast",
}


def test_deprecated_alias_still_all_vintage() -> None:
    """I-7: deprecated_alias continues to name the base (all-vintage) view.

    Pinned against an explicit expected mapping rather than a conditional
    skip (Sol diff-review finding, 2026-08-03): a regression that made all
    three APPEND_ONLY entries return `deprecated_alias=None` -- silently
    dropping the promised all-vintage legacy names -- passed a
    ``if entry.deprecated_alias is not None`` guard silently. It fails here.
    """
    for source, dataset in _APPEND_ONLY_DATASETS:
        entry = _entry(source, dataset)
        expected = _EXPECTED_APPEND_ONLY_DEPRECATED_ALIASES[(source, dataset)]
        assert entry.deprecated_alias == expected
