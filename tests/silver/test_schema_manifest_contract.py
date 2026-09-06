"""Contract tests for the exported silver schema manifest."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import polars as pl

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
    silver_schema_manifest_frame,
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
    entry = _silver_entry("elexon", "system_prices")
    # system_prices is APPEND_ONLY with a registered _latest spec (N-5, D-1/D-2):
    # relation_name is the _latest projection; qualified_view keeps naming the
    # all-vintage base (D-3/I-2a) so the full history stays reachable.
    assert entry.relation_name == "silver_elexon_system_prices_latest"
    assert entry.qualified_view == "silver_elexon_system_prices"


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
            # candidate.qualified_view, not candidate.relation_name: an
            # APPEND_ONLY silver row's relation_name is its `_latest` name
            # (N-5), but qualified_view always names the all-vintage base --
            # the correct join key for a serving alias's own qualified_view.
            target = next(
                candidate
                for candidate in _silver_entries()
                if candidate.qualified_view == entry.qualified_view
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


def test_vintage_policy_manifest_membership_and_alias() -> None:
    expected = {
        ("elexon", "mid"): ("elexon-mid/vp-2026-09", 3600, datetime(2026, 8, 1, tzinfo=UTC)),
        ("elexon", "system_prices"): (
            "elexon-system_prices/vp-2026-09",
            5400,
            datetime(2026, 7, 31, tzinfo=UTC),
        ),
        ("open_meteo", "historical_demand"): (
            "open_meteo-historical_demand/vp-2026-09",
            432000,
            datetime(2026, 8, 1, tzinfo=UTC),
        ),
        ("open_meteo", "historical_wind"): (
            "open_meteo-historical_wind/vp-2026-09",
            432000,
            datetime(2026, 8, 1, tzinfo=UTC),
        ),
        ("open_meteo", "historical_solar"): (
            "open_meteo-historical_solar/vp-2026-09",
            432000,
            datetime(2026, 8, 1, tzinfo=UTC),
        ),
    }
    for entry in _silver_entries():
        key = (entry.source, entry.dataset)
        assert ("vintage_policy" in entry.bitemporal_columns) == (key in expected)
        assert (entry.vintage_policy is not None) == (key in expected)
        if entry.vintage_policy is not None:
            transformer = get_transformer(entry.source, entry.dataset, Path("__manifest_test__"))
            policy = transformer.VINTAGE_POLICY
            assert policy is not None
            name, lag_seconds, cutover = expected[key]
            assert (policy.name, policy.lag, policy.applies_before) == (
                name,
                timedelta(seconds=lag_seconds),
                cutover,
            )
            assert policy.dated == date(2026, 9, 6)
            assert entry.vintage_policy.name == name
            assert entry.vintage_policy.lag_seconds == lag_seconds
            assert entry.vintage_policy.dated == date(2026, 9, 6)
            assert entry.vintage_policy.rule == policy.rule
            assert entry.vintage_policy.applies_before == cutover.isoformat()
            assert "null as unknown" in entry.vintage_policy.legacy_rows
            assert "vintage_policy" not in (entry.columns or ())
    alias = next(
        entry
        for entry in get_silver_schema_manifest()
        if entry.relation_kind == "serving_alias" and entry.dataset == "system_prices"
    )
    base = _silver_entry("elexon", "system_prices")
    assert alias.vintage_policy == base.vintage_policy
    assert all(
        entry.bitemporal_columns == ()
        for entry in get_silver_schema_manifest()
        if entry.relation_kind == "serving_alias"
    )
    assert "vintage_policy" in BITEMPORAL_EXCLUDE


def test_vintage_policy_manifest_frame_is_serializable() -> None:
    frame = silver_schema_manifest_frame()
    assert isinstance(frame.schema["vintage_policy"], pl.Struct)
    mid = frame.filter((pl.col("dataset") == "mid") & (pl.col("relation_kind") == "silver"))
    assert mid["vintage_policy"][0]["name"] == "elexon-mid/vp-2026-09"
    assert '"applies_before":"2026-08-01T00:00:00+00:00"' in mid.write_json()
    expected = {
        ("elexon", "mid"): ("elexon-mid/vp-2026-09", 3600, "2026-08-01T00:00:00+00:00"),
        ("elexon", "system_prices"): (
            "elexon-system_prices/vp-2026-09",
            5400,
            "2026-07-31T00:00:00+00:00",
        ),
        ("open_meteo", "historical_demand"): (
            "open_meteo-historical_demand/vp-2026-09",
            432000,
            "2026-08-01T00:00:00+00:00",
        ),
        ("open_meteo", "historical_wind"): (
            "open_meteo-historical_wind/vp-2026-09",
            432000,
            "2026-08-01T00:00:00+00:00",
        ),
        ("open_meteo", "historical_solar"): (
            "open_meteo-historical_solar/vp-2026-09",
            432000,
            "2026-08-01T00:00:00+00:00",
        ),
    }
    rows = frame.filter(pl.col("relation_kind") == "silver")
    for row in [*rows.to_dicts(), *json.loads(rows.write_json())]:
        policy = row["vintage_policy"]
        if policy is not None:
            assert (policy["name"], policy["lag_seconds"], policy["applies_before"]) == expected[
                (row["source"], row["dataset"])
            ]
            assert type(policy["lag_seconds"]) is int
            assert policy["dated"] == "2026-09-06"
