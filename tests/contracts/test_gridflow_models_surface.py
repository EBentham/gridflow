"""The gridflow surface gridflow_models uses, pinned so a rename fails here first.

gridflow_models, a sibling repo, imports these names and reads these fields
directly, so removing or renaming one breaks it. A change to this list is a
cross-repo change and lands in both repos together; a name gridflow_models
starts using is added here in the same change.
"""

from __future__ import annotations

import dataclasses
import importlib

import pytest

from gridflow.serving.client import GridflowClient
from gridflow.silver.schema_manifest import SilverSchemaEntry, get_silver_schema_manifest

_IMPORTED_NAMES: dict[str, tuple[str, ...]] = {
    "gridflow.config.settings": (
        "GridflowConfig",
        "PipelineSettings",
        "SourceConfig",
        "load_settings",
    ),
    "gridflow.pipeline.runner": (
        "BackfillUnsupportedError",
        "DatasetResult",
        "assert_backfillable",
        "build_context",
        "import_connectors",
        "import_transformers",
        "refresh_views",
        "resolve_datasets",
        "resolve_dates",
        "run_backfill",
        "run_ingest",
        "run_transform",
    ),
    "gridflow.serving.client": ("GridflowClient",),
    "gridflow.silver.registry": ("list_transformers",),
    "gridflow.silver.schema_manifest": (
        "BITEMPORAL_EXCLUDE",
        "SilverSchemaEntry",
        "get_silver_schema_manifest",
    ),
    # _register_gold_views is stubbed by gridflow_models' catalogue fixture.
    "gridflow.storage.duckdb": ("_register_gold_views", "init_catalogue", "refresh_views"),
    "gridflow.storage.parquet": ("is_reserved_temp_path", "write_parquet"),
    # Stubbed by gridflow_models' tests.
    "gridflow.utils.logging": ("setup_logging",),
    "gridflow.utils.time": (
        "parse_lookback",
        "settlement_period_to_utc",
        "settlement_periods_in_day",
        "utc_to_settlement_period",
    ),
}

_CLIENT_METHODS: tuple[str, ...] = (
    "close",
    "get_fuel_generation",
    "get_gas_storage",
    "get_gb_day_ahead_benchmark",
    "get_generation_by_fuel",
    "get_imbalance_context",
    "get_system_prices",
    "get_tables",
    "get_weather",
    "query",
    "reopen_readonly",
)

# deprecated_alias stays although its views are retired: gridflow_models checks
# it never queries the old single-token name.
_MANIFEST_FIELDS: tuple[str, ...] = (
    "bitemporal_columns",
    "columns",
    "columns_source",
    "dataset",
    "date_col_sql_type",
    "deprecated_alias",
    "designated_date_col",
    "relation_kind",
    "relation_name",
    "source",
)

# The source each GridflowClient handle is bound to in gridflow_models'
# get-method registry; None is a cross-source handle.
_HANDLE_SOURCES: dict[str, str | None] = {
    "fuel_generation": "elexon",
    "gas_storage": "gie_agsi",
    "gb_day_ahead_benchmark": None,
    "imbalance_context": None,
    "system_prices": "elexon",
    "weather": "open_meteo",
}


@pytest.mark.parametrize("module_name", sorted(_IMPORTED_NAMES))
def test_every_imported_name_exists(module_name: str) -> None:
    module = importlib.import_module(module_name)
    missing = [name for name in _IMPORTED_NAMES[module_name] if not hasattr(module, name)]
    assert not missing, f"{module_name} no longer exports {missing}"


def test_client_methods_exist() -> None:
    missing = [
        name for name in _CLIENT_METHODS if not callable(getattr(GridflowClient, name, None))
    ]
    assert not missing, f"GridflowClient lost {missing}"


def test_manifest_entry_fields_exist() -> None:
    fields = {field.name for field in dataclasses.fields(SilverSchemaEntry)}
    assert set(_MANIFEST_FIELDS) <= fields, sorted(set(_MANIFEST_FIELDS) - fields)


def test_each_client_handle_has_its_manifest_row_and_source() -> None:
    handles = {
        entry.dataset: entry.source
        for entry in get_silver_schema_manifest(include_serving_aliases=True)
        if entry.relation_kind != "silver"
    }
    assert {name: handles.get(name, "missing") for name in _HANDLE_SOURCES} == _HANDLE_SOURCES
