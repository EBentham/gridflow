"""Contract tests for the exported silver schema manifest."""

from __future__ import annotations

from pathlib import Path

# Registry side effects: importing subpackages registers their transformers.
import gridflow.silver.elexon  # noqa: F401
import gridflow.silver.entsoe  # noqa: F401
import gridflow.silver.entsog  # noqa: F401
import gridflow.silver.gie  # noqa: F401
import gridflow.silver.neso  # noqa: F401
import gridflow.silver.openmeteo  # noqa: F401
from gridflow.serving.client import _BITEMPORAL_EXCLUDE
from gridflow.silver.registry import get_transformer, list_transformers
from gridflow.silver.schema_manifest import (
    BITEMPORAL_EXCLUDE,
    DECOMMISSIONED_DATASETS,
    DESIGNATED_DATE_COLS,
    SilverSchemaEntry,
    get_silver_schema_manifest,
)


def _silver_entries() -> tuple[SilverSchemaEntry, ...]:
    return tuple(
        entry
        for entry in get_silver_schema_manifest(include_serving_aliases=False)
        if entry.relation_kind == "silver"
    )


def _silver_entry(source: str, dataset: str) -> SilverSchemaEntry:
    matches = [
        entry
        for entry in get_silver_schema_manifest()
        if entry.relation_kind == "silver" and entry.source == source and entry.dataset == dataset
    ]
    assert len(matches) == 1
    return matches[0]


def test_manifest_covers_registered_transformers() -> None:
    manifest_keys = {(entry.source, entry.dataset) for entry in _silver_entries()}
    registry_keys = set(list_transformers()) - DECOMMISSIONED_DATASETS

    assert manifest_keys == registry_keys


def test_manifest_excludes_decommissioned_even_when_registered() -> None:
    import gridflow.silver.elexon.bod  # noqa: F401

    entries = get_silver_schema_manifest()

    assert ("elexon", "bod") in set(list_transformers())
    assert not any(entry.source == "elexon" and entry.dataset == "bod" for entry in entries)


def test_manifest_has_ratified_date_columns() -> None:
    assert DESIGNATED_DATE_COLS[("elexon", "windfor")] == "timestamp_utc"
    assert DESIGNATED_DATE_COLS[("gie_agsi", "about_listing")] == "ingested_at"
    assert DESIGNATED_DATE_COLS[("gie_agsi", "about_summary")] == "ingested_at"
    assert DESIGNATED_DATE_COLS[("gie_agsi", "news")] == "ingested_at"
    assert DESIGNATED_DATE_COLS[("gie_agsi", "unavailability")] == "ingested_at"
    assert DESIGNATED_DATE_COLS[("entsog", "aggregate_interconnections")] == "ingested_at"
    assert DESIGNATED_DATE_COLS[("entsog", "balancing_zones")] == "ingested_at"
    assert DESIGNATED_DATE_COLS[("entsog", "connection_points")] == "ingested_at"
    assert ("elexon", "bod") not in DESIGNATED_DATE_COLS
    assert _silver_entry("elexon", "system_prices").relation_name == ("silver_elexon_system_prices")


def test_manifest_designated_date_col_resolvable() -> None:
    dynamic_date_cols = {"gas_day", "ingested_at", "timestamp_utc"}

    for entry in get_silver_schema_manifest():
        if entry.columns is not None:
            if (
                entry.source == "neso"
                and entry.dataset == "intensity_factors"
                and entry.designated_date_col == "ingested_at"
            ):
                continue
            assert entry.designated_date_col in entry.columns
            continue

        allowed = set(entry.bitemporal_columns)
        if entry.columns_source == "declared_dynamic":
            allowed.update(dynamic_date_cols)
        if entry.columns_source == "serving_alias" and entry.qualified_view is not None:
            target = next(
                candidate
                for candidate in _silver_entries()
                if candidate.relation_name == entry.qualified_view
            )
            allowed.add(target.designated_date_col)

        assert entry.designated_date_col in allowed, entry


def test_manifest_columns_match_schema_cls() -> None:
    for source, dataset in sorted(list_transformers()):
        if (source, dataset) in DECOMMISSIONED_DATASETS:
            continue
        transformer = get_transformer(source, dataset, Path("__schema_manifest_test__"))
        schema_cls = transformer.schema_cls
        if schema_cls is None:
            continue

        assert _silver_entry(source, dataset).columns == tuple(schema_cls.model_fields)


def test_manifest_partition_columns_match_storage_layout() -> None:
    expected = {
        ("elexon", "agpt"): ("year", "month"),
        ("elexon", "bmunits_reference"): (),
        ("entsog", "balancing_zones"): (),
        ("entsog", "physical_flows"): ("year", "month"),
        ("gie_agsi", "about_listing"): ("year", "month"),
        ("neso", "intensity_factors"): (),
        ("neso", "regional_intensity"): ("year", "month"),
    }
    entries = {(entry.source, entry.dataset): entry for entry in _silver_entries()}

    for key, partition_columns in expected.items():
        assert entries[key].partition_columns == partition_columns

    for entry in _silver_entries():
        assert entry.partition_columns in {(), ("year", "month")}


def test_bitemporal_exclude_is_public_authority() -> None:
    assert _BITEMPORAL_EXCLUDE is BITEMPORAL_EXCLUDE
