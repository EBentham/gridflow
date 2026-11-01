"""Every gridflow name gridflow_models uses, installed with the package.

gridflow_models imports these names and reads these fields directly, so
removing or renaming one breaks it. gridflow's contract test checks that each
listed name still resolves, and gridflow_models can check that everything it
uses is listed here. A name gridflow_models starts using is added here in the
same change. While gridflow is on 0.x, any change to this module is a minor
version bump.
"""

from __future__ import annotations

IMPORTED_NAMES: dict[str, tuple[str, ...]] = {
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

CLIENT_METHODS: tuple[str, ...] = (
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
    "require_con",
)

# deprecated_alias stays although its views are retired: gridflow_models checks
# it never queries the old single-token name.
MANIFEST_FIELDS: tuple[str, ...] = (
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
HANDLE_SOURCES: dict[str, str | None] = {
    "fuel_generation": "elexon",
    "gas_storage": "gie_agsi",
    "gb_day_ahead_benchmark": None,
    "imbalance_context": None,
    "system_prices": "elexon",
    "weather": "open_meteo",
}
