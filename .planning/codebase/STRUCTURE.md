# Codebase Structure

**Analysis Date:** 2026-05-02

## Directory Layout

```text
gridflow/
+-- .github/                 # CI workflow configuration
+-- .planning/               # GSD planning and generated codebase maps
+-- config/                  # Runtime YAML configuration
+-- data/                    # Local generated lakehouse data and DuckDB catalogue
+-- docs/                    # Project documentation, endpoint notes, specs, logs
+-- logs/                    # Runtime log output
+-- scripts/                 # Direct Python operational/debug scripts
+-- src/
|   +-- gridflow/            # Installable Python package
|       +-- bronze/          # Raw response persistence
|       +-- config/          # Typed settings loading
|       +-- connectors/      # External API clients, endpoint definitions, parsers
|       +-- gold/            # Analytics/model-ready builders and SQL views
|       +-- quality/         # Data quality checks and reports
|       +-- schemas/         # Pydantic data contracts
|       +-- serving/         # Read-only Python query client
|       +-- silver/          # Bronze-to-silver transformers
|       +-- storage/         # Path, Parquet, and DuckDB utilities
|       +-- utils/           # Logging, retry, and time helpers
|       +-- cli.py           # Typer CLI
|       +-- __main__.py      # `python -m gridflow` entrypoint
+-- tests/                   # Unit, integration, contract tests, and fixtures
+-- Makefile                 # Common developer commands
+-- pyproject.toml           # Packaging, dependencies, lint, type, test config
+-- .env                     # Present; environment configuration, never read into docs
+-- .env.example             # Example environment configuration
```

## Directory Purposes

**`config/`:**
- Purpose: Store non-secret YAML runtime configuration.
- Contains: `config/settings.yaml` for pipeline paths and quality defaults; `config/sources.yaml` for source URLs, auth env var names, rate limits, dataset metadata, schedules, and API document types.
- Key files: `config/settings.yaml`, `config/sources.yaml`

**`src/gridflow/`:**
- Purpose: Main Python package installed from the `src` layout configured in `pyproject.toml`.
- Contains: Pipeline layers, CLI, storage utilities, schemas, quality checks, and serving client.
- Key files: `src/gridflow/cli.py`, `src/gridflow/__main__.py`, `src/gridflow/__init__.py`

**`src/gridflow/config/`:**
- Purpose: Turn YAML and environment variables into typed configuration objects.
- Contains: `DatasetConfig`, `SourceConfig`, `QualityConfig`, `PipelineSettings`, `GridflowConfig`, `load_settings()`.
- Key files: `src/gridflow/config/settings.py`

**`src/gridflow/connectors/`:**
- Purpose: Own all API-specific fetching logic and connector registration.
- Contains: `base.py`, `registry.py`, provider subdirectories, endpoint definitions, parsers, and client classes.
- Key files: `src/gridflow/connectors/base.py`, `src/gridflow/connectors/registry.py`, `src/gridflow/connectors/elexon/client.py`, `src/gridflow/connectors/entsoe/client.py`

**`src/gridflow/connectors/{provider}/`:**
- Purpose: Keep each external source isolated by provider.
- Contains: `client.py` for `BaseConnector` subclasses, `endpoints.py` for endpoint/document definitions, optional `parsers.py`, and `__init__.py` registration imports.
- Key files: `src/gridflow/connectors/elexon/endpoints.py`, `src/gridflow/connectors/entsoe/parsers.py`, `src/gridflow/connectors/openmeteo/endpoints.py`

**`src/gridflow/bronze/`:**
- Purpose: Persist raw API bodies and sidecar metadata.
- Contains: `BronzeWriter`.
- Key files: `src/gridflow/bronze/writer.py`

**`src/gridflow/silver/`:**
- Purpose: Transform raw bronze files into normalized silver datasets.
- Contains: `BaseSilverTransformer`, transformer registry, and provider-specific transformer packages.
- Key files: `src/gridflow/silver/base.py`, `src/gridflow/silver/registry.py`

**`src/gridflow/silver/{provider}/`:**
- Purpose: Store one transformer module per source/dataset or closely related dataset family.
- Contains: modules such as `system_prices.py`, `day_ahead_prices.py`, `physical_flows.py`, `historical.py`, plus `__init__.py` registration imports.
- Key files: `src/gridflow/silver/elexon/system_prices.py`, `src/gridflow/silver/entsoe/day_ahead_prices.py`, `src/gridflow/silver/openmeteo/historical.py`, `src/gridflow/silver/gie/agsi.py`

**`src/gridflow/gold/`:**
- Purpose: Build modelling-ready datasets and SQL-defined cross-source views from silver data.
- Contains: `BaseGoldBuilder`, concrete builder modules, and `views/`.
- Key files: `src/gridflow/gold/base.py`, `src/gridflow/gold/system_marginal_price.py`, `src/gridflow/gold/demand_features.py`, `src/gridflow/gold/merit_order.py`

**`src/gridflow/gold/views/`:**
- Purpose: Store SQL files executed by DuckDB catalogue initialization.
- Contains: SQL view definitions.
- Key files: `src/gridflow/gold/views/uk_imbalance_context.sql`, `src/gridflow/gold/views/eu_gas_storage.sql`

**`src/gridflow/storage/`:**
- Purpose: Centralize filesystem path rules, atomic Parquet IO, and DuckDB catalogue management.
- Contains: `PathBuilder`, `write_parquet()`, `read_parquet_dir()`, `get_connection()`, `init_catalogue()`, view registration.
- Key files: `src/gridflow/storage/paths.py`, `src/gridflow/storage/parquet.py`, `src/gridflow/storage/duckdb.py`

**`src/gridflow/schemas/`:**
- Purpose: Define Pydantic data contracts and reusable validators.
- Contains: `BaseSchema`, mixins, and source-specific model files.
- Key files: `src/gridflow/schemas/common.py`, `src/gridflow/schemas/elexon.py`, `src/gridflow/schemas/entsoe.py`, `src/gridflow/schemas/weather.py`

**`src/gridflow/quality/`:**
- Purpose: Run and report data quality checks over generated datasets.
- Contains: check functions and `QualityReporter`.
- Key files: `src/gridflow/quality/checks.py`, `src/gridflow/quality/reporter.py`

**`src/gridflow/serving/`:**
- Purpose: Provide downstream read-only query helpers over DuckDB.
- Contains: `GridflowClient`.
- Key files: `src/gridflow/serving/client.py`

**`src/gridflow/utils/`:**
- Purpose: Shared operational helpers.
- Contains: logging setup, retry policy/circuit breaker, settlement-period and date utilities.
- Key files: `src/gridflow/utils/logging.py`, `src/gridflow/utils/retry.py`, `src/gridflow/utils/time.py`

**`scripts/`:**
- Purpose: Direct Python operational scripts outside the package CLI.
- Contains: pipeline runner, DuckDB init, backfill, CSV export.
- Key files: `scripts/run_pipeline.py`, `scripts/init_duckdb.py`, `scripts/backfill.py`, `scripts/export_to_csv.py`

**`tests/`:**
- Purpose: Verify contracts, unit behavior, integration behavior, and fixture parsing.
- Contains: `unit/`, `integration/`, `contracts/`, `fixtures/`, empty `endpoints/` directory, and `conftest.py`.
- Key files: `tests/unit/test_silver_transforms.py`, `tests/unit/test_entsoe.py`, `tests/integration/test_bronze_to_silver.py`, `tests/contracts/test_bronze_silver_contract.py`

**`data/`:**
- Purpose: Generated local lakehouse and export output.
- Contains: `data/bronze/`, `data/silver/`, `data/exports/`, and configured DuckDB path `data/gridflow.duckdb`.
- Key files: Generated files only; write through `src/gridflow/bronze/writer.py`, `src/gridflow/silver/base.py`, `src/gridflow/gold/base.py`, and `src/gridflow/storage/duckdb.py`.

## Key File Locations

**Entry Points:**
- `src/gridflow/cli.py`: Main Typer command surface for installed `gridflow` CLI.
- `src/gridflow/__main__.py`: Entrypoint for `python -m gridflow`.
- `scripts/run_pipeline.py`: Debug/IDE pipeline runner with argparse.
- `scripts/init_duckdb.py`: Direct DuckDB catalogue initialization script.
- `scripts/backfill.py`: Historical backfill helper.
- `scripts/export_to_csv.py`: CSV export helper.

**Configuration:**
- `pyproject.toml`: Package metadata, dependencies, console script, Ruff, mypy, pytest config.
- `config/settings.yaml`: Local paths and quality defaults.
- `config/sources.yaml`: Source and dataset metadata.
- `.env`: Present; contains runtime environment configuration and must not be read or quoted.
- `.env.example`: Example environment configuration.
- `.pre-commit-config.yaml`: Pre-commit hook configuration.
- `.github/workflows/ci.yml`: CI workflow configuration.

**Core Logic:**
- `src/gridflow/config/settings.py`: Settings model and loader.
- `src/gridflow/connectors/base.py`: Connector and raw response contract.
- `src/gridflow/connectors/registry.py`: Source connector registry.
- `src/gridflow/bronze/writer.py`: Bronze persistence.
- `src/gridflow/silver/base.py`: Silver transformer template method.
- `src/gridflow/silver/registry.py`: Silver transformer registry.
- `src/gridflow/gold/base.py`: Gold builder template method.
- `src/gridflow/storage/duckdb.py`: DuckDB metadata tables and view registration.
- `src/gridflow/storage/parquet.py`: Atomic Parquet read/write helpers.
- `src/gridflow/observability.py`: Pipeline run tracking and watermarks.

**Source Integrations:**
- `src/gridflow/connectors/elexon/client.py`: Elexon connector.
- `src/gridflow/connectors/elexon/endpoints.py`: Elexon endpoint registry and parameter builder.
- `src/gridflow/connectors/entsoe/client.py`: ENTSO-E connector.
- `src/gridflow/connectors/entsoe/endpoints.py`: ENTSO-E document, zone, and format definitions.
- `src/gridflow/connectors/entsoe/parsers.py`: ENTSO-E XML parser.
- `src/gridflow/connectors/gie/client.py`: GIE AGSI/ALSI connectors.
- `src/gridflow/connectors/entsog/client.py`: ENTSO-G connector.
- `src/gridflow/connectors/neso/carbon_intensity.py`: NESO carbon intensity connector.
- `src/gridflow/connectors/openmeteo/client.py`: Open-Meteo connector.

**Transformers:**
- `src/gridflow/silver/elexon/system_prices.py`: Elexon system prices transformer.
- `src/gridflow/silver/elexon/fuelhh.py`: Elexon half-hourly fuel generation transformer.
- `src/gridflow/silver/elexon/demand_forecast.py`: Elexon NDF/NDFD transformer classes.
- `src/gridflow/silver/entsoe/day_ahead_prices.py`: ENTSO-E day-ahead prices transformer.
- `src/gridflow/silver/entsoe/actual_load.py`: ENTSO-E actual load transformer.
- `src/gridflow/silver/gie/agsi.py`: GIE gas storage transformer.
- `src/gridflow/silver/openmeteo/historical.py`: Open-Meteo historical weather transformer.
- `src/gridflow/silver/entsog/physical_flows.py`: ENTSO-G physical flows transformer.
- `src/gridflow/silver/neso/carbon_intensity.py`: NESO carbon intensity transformer.

**Gold:**
- `src/gridflow/gold/system_marginal_price.py`: Current CLI-registered gold builder.
- `src/gridflow/gold/demand_features.py`: Additional gold builder module.
- `src/gridflow/gold/merit_order.py`: Additional gold builder module.
- `src/gridflow/gold/views/uk_imbalance_context.sql`: DuckDB SQL view for UK imbalance context.
- `src/gridflow/gold/views/eu_gas_storage.sql`: DuckDB SQL view for EU gas storage.

**Testing:**
- `tests/conftest.py`: Shared fixtures and sample config/response data.
- `tests/unit/`: Unit tests for endpoints, schemas, utilities, and transformers.
- `tests/integration/`: Integration tests for connectors and bronze-to-silver flow.
- `tests/contracts/`: Contract tests for bronze/silver and silver/gold boundaries.
- `tests/fixtures/`: JSON/XML source fixtures organized by provider.

## Naming Conventions

**Files:**
- Use snake_case for modules: `src/gridflow/silver/elexon/system_prices.py`, `src/gridflow/storage/duckdb.py`.
- Use provider subpackages for integration-specific code: `src/gridflow/connectors/elexon/`, `src/gridflow/silver/entsoe/`.
- Use `client.py` for provider API clients: `src/gridflow/connectors/gie/client.py`.
- Use `endpoints.py` for provider endpoint metadata: `src/gridflow/connectors/elexon/endpoints.py`.
- Use `parsers.py` for provider parsing helpers when raw formats need shared parsing: `src/gridflow/connectors/entsoe/parsers.py`.
- Use one transformer module per dataset or small dataset family: `src/gridflow/silver/elexon/demand_forecast.py` contains both `DemandForecastTransformer` and `NDFDTransformer`.
- Use `test_*.py` for tests: `tests/unit/test_silver_transforms.py`.

**Directories:**
- Provider directories use source names matching configuration keys where possible: `elexon`, `entsoe`, `entsog`, `openmeteo`, `gie`, `neso`.
- Data directories use layer/source/dataset/date partitioning: `data/bronze/elexon/system_prices/2026/04/24/`.
- Silver data uses Hive-style year/month partitions: `data/silver/elexon/system_prices/year=2026/month=04/`.
- Gold data uses dataset-first partitioning: `data/gold/system_marginal_price/year=2026/`.

**Classes:**
- Connector classes end in `Connector`: `ElexonConnector`, `EntsoeConnector`, `CarbonIntensityConnector`.
- Silver transformer classes end in `Transformer`: `SystemPriceTransformer`, `DayAheadPricesTransformer`, `GasStorageTransformer`.
- Gold builder classes end in `Builder`: `SystemMarginalPriceBuilder`, `DemandFeaturesBuilder`, `MeritOrderBuilder`.
- Pydantic schema classes are source/domain nouns: `ElexonSystemPrice`, `EntsoeDayAheadPrice`, `WeatherObservation`.

**Functions:**
- Registry functions use `register_*`, `get_*`, and `list_*`: `register_connector()`, `get_transformer()`, `list_sources()`.
- CLI helper functions are private with leading underscore: `_resolve_dates()`, `_import_connectors()`.
- Pipeline script functions use verb-stage names: `run_bronze()`, `run_silver()`, `run_gold()`.

## Where to Add New Code

**New API Source:**
- Configuration: add a source block in `config/sources.yaml`.
- Connector implementation: create `src/gridflow/connectors/{source}/client.py`.
- Endpoint metadata: create `src/gridflow/connectors/{source}/endpoints.py` if the source has multiple endpoints or document types.
- Parsers: create `src/gridflow/connectors/{source}/parsers.py` for shared raw parsing helpers.
- Registration import: create/update `src/gridflow/connectors/{source}/__init__.py`.
- CLI import hook: add `gridflow.connectors.{source}` to `_import_connectors()` in `src/gridflow/cli.py` and `scripts/run_pipeline.py`.
- Tests: add unit tests under `tests/unit/` and connector integration tests under `tests/integration/`.

**New Dataset for an Existing Source:**
- Configuration: add the dataset under the provider in `config/sources.yaml`.
- Endpoint metadata: update `src/gridflow/connectors/{source}/endpoints.py` if dataset-specific request metadata is needed.
- Connector routing: update `src/gridflow/connectors/{source}/client.py` when fetch behavior differs from existing patterns.
- Bronze support: no new bronze module is required if the connector returns correct `RawResponse` metadata.
- Silver transformer: create `src/gridflow/silver/{source}/{dataset}.py` or extend a small related family module.
- Transformer registration: call `register_transformer("{source}", "{dataset}", TransformerClass)` at the bottom of the module.
- Provider import list: update `src/gridflow/silver/{source}/__init__.py`, but only import modules that exist in the root tree.
- Tests: add transformer coverage in `tests/unit/`, fixtures in `tests/fixtures/{source}/`, and contract coverage where schema boundaries change.

**New Elexon Dataset:**
- Endpoint definitions: update `src/gridflow/connectors/elexon/endpoints.py`.
- Transformer: add `src/gridflow/silver/elexon/{dataset}.py`.
- Registration import: update `src/gridflow/silver/elexon/__init__.py`.
- Tests: add fixture JSON under `tests/fixtures/elexon/` and assertions in `tests/unit/test_silver_transforms.py` or a new focused test file.

**New ENTSO-E Dataset:**
- Endpoint/document mapping: update `src/gridflow/connectors/entsoe/endpoints.py`.
- XML parsing: reuse or extend `src/gridflow/connectors/entsoe/parsers.py`.
- Transformer: add `src/gridflow/silver/entsoe/{dataset}.py`.
- Registration import: update `src/gridflow/silver/entsoe/__init__.py`.
- Tests: add fixture XML under `tests/fixtures/entsoe/` and assertions in `tests/unit/test_entsoe.py` or a new focused test file.

**New Silver Transformer:**
- Implementation: subclass `BaseSilverTransformer` from `src/gridflow/silver/base.py`.
- Required methods: implement `read_bronze(self, target_date)` and `transform(self, raw_df)`.
- Output: return a Polars `DataFrame` with stable snake_case column names and `timestamp_utc` where applicable.
- Registration: call `register_transformer(source, dataset, ClassName)` at module bottom.
- Tests: use fixture files from `tests/fixtures/` and add direct transformer tests under `tests/unit/`.

**New Gold Dataset:**
- Implementation: subclass `BaseGoldBuilder` in a new module under `src/gridflow/gold/`.
- Output: set `gold_dataset` and implement `build(start_date, end_date)`.
- CLI wiring: add the builder to the `gold_builders` dictionary in `src/gridflow/cli.py`.
- Script wiring: add the builder to the `gold_builders` dictionary in `scripts/run_pipeline.py`.
- Query surface: add SQL under `src/gridflow/gold/views/` or methods in `src/gridflow/serving/client.py` if downstream consumers need named access.
- Tests: add contract tests under `tests/contracts/` and builder tests under `tests/unit/` or `tests/integration/`.

**New DuckDB View:**
- SQL view: add a `.sql` file under `src/gridflow/gold/views/`.
- Registration: no Python change is required if the SQL file can run through `_register_gold_views()` in `src/gridflow/storage/duckdb.py`.
- Query client: add a method in `src/gridflow/serving/client.py` when the view becomes part of the public SDK.
- Tests: add query/view validation under `tests/contracts/` or `tests/integration/`.

**New Quality Check:**
- Check logic: add a function to `src/gridflow/quality/checks.py` returning `QualityResult`.
- Report integration: call the check from `quality()` in `src/gridflow/cli.py` or from `QualityReporter` flow in `src/gridflow/quality/reporter.py`.
- Tests: add unit tests under `tests/unit/`.

**New Storage Path Rule:**
- Implementation: add methods to `PathBuilder` in `src/gridflow/storage/paths.py`.
- IO behavior: update `src/gridflow/storage/parquet.py` only when read/write semantics change.
- Tests: add or extend `tests/unit/test_path_builder.py`.

**New CLI Command:**
- Implementation: add an `@app.command()` function in `src/gridflow/cli.py`.
- Shared logic: put reusable behavior in package modules under `src/gridflow/`, not only inside CLI functions.
- Debug parity: update `scripts/run_pipeline.py` only if the command is part of the bronze/silver/gold pipeline path.
- Tests: add command-level tests when a CLI runner fixture exists, otherwise test the package-level logic directly.

**Utilities:**
- Time/date helpers: add to `src/gridflow/utils/time.py`.
- Retry or resilience helpers: add to `src/gridflow/utils/retry.py`.
- Logging setup changes: add to `src/gridflow/utils/logging.py`.

## Special Directories

**`.planning/`:**
- Purpose: GSD planning artifacts and generated codebase maps.
- Generated: Yes
- Committed: Project-dependent; this mapping writes `.planning/codebase/ARCHITECTURE.md` and `.planning/codebase/STRUCTURE.md`.

**`.claude/`:**
- Purpose: Claude/GSD worktree and agent-related local workspace state.
- Generated: Yes
- Committed: Not part of main package architecture; exclude from source mapping.

**`.venv/`:**
- Purpose: Local Python virtual environment.
- Generated: Yes
- Committed: No

**`.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`:**
- Purpose: Tool caches for pytest, mypy, and Ruff.
- Generated: Yes
- Committed: No

**`data/`:**
- Purpose: Generated bronze, silver, gold, DuckDB, and CSV export artifacts.
- Generated: Yes
- Committed: Usually no for generated data; use pipeline code to regenerate.

**`logs/`:**
- Purpose: Runtime logs produced by `src/gridflow/utils/logging.py`.
- Generated: Yes
- Committed: No

**`tests/fixtures/`:**
- Purpose: Committed sample JSON/XML inputs used by unit and integration tests.
- Generated: No
- Committed: Yes

**`docs/`:**
- Purpose: Human documentation, endpoint notes, specs, and logs.
- Generated: Mixed
- Committed: Yes when documentation is source-controlled.

---

*Structure analysis: 2026-05-02*
