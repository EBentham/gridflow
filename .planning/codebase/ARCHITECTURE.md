<!-- refreshed: 2026-05-02 -->
# Architecture

**Analysis Date:** 2026-05-02

## System Overview

```text
+-------------------------------------------------------------------+
|                         Entry Points                              |
| `src/gridflow/cli.py`  `src/gridflow/__main__.py`  `scripts/*.py` |
+-----------+-----------------------+-------------------------------+
            |
            v
+-------------------------------------------------------------------+
|                      Orchestration Layer                          |
| CLI commands and script runners resolve config, dates, datasets,  |
| import registries, initialise DuckDB, and track pipeline runs.     |
| `src/gridflow/cli.py`  `scripts/run_pipeline.py`                  |
+-----------+-----------------------+-------------------------------+
            |
            v
+-------------------------------------------------------------------+
|                         Pipeline Layers                           |
| Connectors -> BronzeWriter -> SilverTransformer -> GoldBuilder     |
| `src/gridflow/connectors/`  `src/gridflow/bronze/`                 |
| `src/gridflow/silver/`      `src/gridflow/gold/`                   |
+-----------+-----------------------+-------------------------------+
            |
            v
+-------------------------------------------------------------------+
|                   Storage, Catalogue, Query Surface                |
| Raw files, Parquet/CSV partitions, DuckDB views, SDK client.       |
| `data/`  `src/gridflow/storage/`  `src/gridflow/serving/client.py` |
+-------------------------------------------------------------------+
```

## Component Responsibilities

| Component | Responsibility | File |
|-----------|----------------|------|
| CLI app | User-facing Typer commands for ingest, transform, build, backfill, export, pipeline, status, quality, reset, and init workflows. | `src/gridflow/cli.py` |
| Python module entrypoint | Runs the Typer CLI when invoked with `python -m gridflow`. | `src/gridflow/__main__.py` |
| Debug pipeline runner | Argparse-based runner for IDE/debug use with bronze, silver, gold, and all-step modes. | `scripts/run_pipeline.py` |
| Configuration loader | Merges YAML config, environment variables, and `.env` settings into typed Pydantic models. | `src/gridflow/config/settings.py` |
| Source configuration | Defines source base URLs, auth env var names, rate limits, timeouts, schedules, and datasets. | `config/sources.yaml` |
| Pipeline configuration | Defines local data path, log path, DuckDB path, lookback, concurrency, and quality settings. | `config/settings.yaml` |
| Connector base | Defines the async source connector contract and immutable `RawResponse` provenance container. | `src/gridflow/connectors/base.py` |
| Connector registry | Maps source names to connector classes through explicit registration calls. | `src/gridflow/connectors/registry.py` |
| Source connectors | Fetch raw API responses for Elexon, ENTSO-E, ENTSO-G, GIE, NESO, and Open-Meteo. | `src/gridflow/connectors/*/client.py` |
| Bronze writer | Persists raw API bodies plus `.meta.json` sidecars under partitioned source/dataset/date paths. | `src/gridflow/bronze/writer.py` |
| Silver base transformer | Defines the bronze-to-silver contract and common partitioned Parquet/CSV write behavior. | `src/gridflow/silver/base.py` |
| Silver registry | Maps `(source, dataset)` pairs to transformer classes through registration calls. | `src/gridflow/silver/registry.py` |
| Silver transformers | Parse source-specific raw JSON/XML, normalize columns, deduplicate, and write silver data. | `src/gridflow/silver/elexon/system_prices.py`, `src/gridflow/silver/entsoe/day_ahead_prices.py` |
| Schemas | Pydantic data contracts and shared timestamp/settlement-period validation helpers. | `src/gridflow/schemas/common.py`, `src/gridflow/schemas/elexon.py`, `src/gridflow/schemas/entsoe.py` |
| Gold base builder | Defines the silver-to-gold contract and common gold partition write behavior. | `src/gridflow/gold/base.py` |
| Gold builders | Build modelling-ready datasets from silver inputs. | `src/gridflow/gold/system_marginal_price.py`, `src/gridflow/gold/demand_features.py`, `src/gridflow/gold/merit_order.py` |
| Gold SQL views | Define DuckDB SQL views that combine silver/gold tables into query surfaces. | `src/gridflow/gold/views/uk_imbalance_context.sql`, `src/gridflow/gold/views/eu_gas_storage.sql` |
| Storage utilities | Centralize filesystem path generation, atomic Parquet writes, Parquet reads, and DuckDB catalogue setup. | `src/gridflow/storage/paths.py`, `src/gridflow/storage/parquet.py`, `src/gridflow/storage/duckdb.py` |
| Observability | Writes pipeline run status, durations, row counts, failures, and watermarks into DuckDB metadata tables. | `src/gridflow/observability.py` |
| Quality checks | Computes null-rate, time-gap, range, row-count, and duplicate checks and writes quality reports. | `src/gridflow/quality/checks.py`, `src/gridflow/quality/reporter.py` |
| Query client | Read-only DuckDB SDK for downstream analysis of silver/gold views. | `src/gridflow/serving/client.py` |

## Pattern Overview

**Overall:** Local lakehouse ETL pipeline with plugin-style source/dataset registries.

**Key Characteristics:**
- API integrations are implemented as async `BaseConnector` subclasses that return `RawResponse` objects with provenance metadata.
- Data flows through bronze raw files, silver normalized Parquet/CSV partitions, and gold modelling datasets or DuckDB SQL views.
- Connectors and silver transformers register themselves at import time; orchestration code imports package modules before resolving registry entries.
- DuckDB is a local catalogue over filesystem Parquet, not the primary storage format.
- Polars is the transformation engine for silver and gold dataframes.

## Layers

**Configuration:**
- Purpose: Provide typed runtime configuration and source/dataset metadata.
- Location: `src/gridflow/config/settings.py`, `config/settings.yaml`, `config/sources.yaml`
- Contains: Pydantic settings models, YAML loading, API key resolution from environment variables.
- Depends on: `pydantic`, `pydantic-settings`, `yaml`, `os`, `pathlib`.
- Used by: `src/gridflow/cli.py`, `scripts/run_pipeline.py`, `scripts/init_duckdb.py`, connectors, storage setup.

**Connector Layer:**
- Purpose: Fetch raw API data from external energy/weather services.
- Location: `src/gridflow/connectors/`
- Contains: `BaseConnector`, `RawResponse`, per-provider clients, endpoint registries, parsers, and registration calls.
- Depends on: `httpx`, `gridflow.config.settings.SourceConfig`, provider endpoint modules, retry policy in `src/gridflow/utils/retry.py`.
- Used by: `gridflow ingest` in `src/gridflow/cli.py` and `run_bronze()` in `scripts/run_pipeline.py`.

**Bronze Layer:**
- Purpose: Preserve raw API response bodies and request metadata before parsing or normalization.
- Location: `src/gridflow/bronze/writer.py`, `data/bronze/`
- Contains: `BronzeWriter.write()` and partitioned raw data layout.
- Depends on: `RawResponse`, `hashlib`, `json`, `pathlib`.
- Used by: Ingest orchestration in `src/gridflow/cli.py` and `scripts/run_pipeline.py`.

**Silver Layer:**
- Purpose: Convert bronze JSON/XML into normalized, deduplicated, typed analytical datasets.
- Location: `src/gridflow/silver/`
- Contains: `BaseSilverTransformer`, transformer registry, provider subpackages, one transformer module per source/dataset.
- Depends on: `polars`, source parsers, time utilities, `src/gridflow/storage/parquet.py`.
- Used by: `gridflow transform`, `gridflow pipeline`, `scripts/run_pipeline.py`, tests under `tests/unit/` and `tests/contracts/`.

**Schemas Layer:**
- Purpose: Define validation contracts for normalized records.
- Location: `src/gridflow/schemas/`
- Contains: Pydantic base schema, UTC timestamp mixin, settlement-period mixin, source-specific models.
- Depends on: `pydantic`, `datetime`.
- Used by: Transformer tests and selected transformers for validation boundaries.

**Gold Layer:**
- Purpose: Build modelling-ready datasets and cross-source views from silver data.
- Location: `src/gridflow/gold/`, `src/gridflow/gold/views/`
- Contains: `BaseGoldBuilder`, Python builders, SQL view definitions.
- Depends on: `polars`, `src/gridflow/storage/parquet.py`, DuckDB view registration.
- Used by: `gridflow build`, `gridflow pipeline`, `scripts/run_pipeline.py`, `src/gridflow/storage/duckdb.py`.

**Storage and Catalogue:**
- Purpose: Standardize local paths, atomically write/read Parquet, and expose data through DuckDB.
- Location: `src/gridflow/storage/`, `data/`
- Contains: `PathBuilder`, `write_parquet()`, `read_parquet_dir()`, `get_connection()`, `init_catalogue()`, DuckDB metadata tables.
- Depends on: `polars`, `duckdb`, `pathlib`, `os.replace`.
- Used by: All pipeline stages, quality reporting, serving client, scripts.

**Observability and Quality:**
- Purpose: Track pipeline runs and produce data quality reports.
- Location: `src/gridflow/observability.py`, `src/gridflow/quality/`
- Contains: `PipelineRunTracker`, watermarks, quality check functions, `QualityReporter`.
- Depends on: `duckdb`, `polars`, `datetime`, local storage config.
- Used by: CLI commands and pipeline scripts.

**Serving:**
- Purpose: Provide a Python client for read-only DuckDB queries over generated views.
- Location: `src/gridflow/serving/client.py`
- Contains: `GridflowClient` methods such as `get_system_prices()`, `get_gas_storage()`, and `get_imbalance_context()`.
- Depends on: `duckdb`, `polars`.
- Used by: Downstream Python analysis code.

## Data Flow

### Primary Request Path

1. CLI parses command arguments in `ingest()`, `transform()`, `build()`, or `pipeline()` (`src/gridflow/cli.py:18`, `src/gridflow/cli.py:79`, `src/gridflow/cli.py:133`, `src/gridflow/cli.py:292`).
2. Configuration is loaded from YAML and environment via `load_settings()` (`src/gridflow/config/settings.py:100`).
3. `init_catalogue()` creates DuckDB metadata tables and registers available Parquet views (`src/gridflow/storage/duckdb.py:47`).
4. `_import_connectors()` imports provider packages so source connectors register themselves (`src/gridflow/cli.py:629`).
5. `get_connector()` instantiates the registered provider connector (`src/gridflow/connectors/registry.py:21`).
6. The connector fetches API responses as `RawResponse` objects (`src/gridflow/connectors/base.py:16`, `src/gridflow/connectors/elexon/client.py:35`, `src/gridflow/connectors/entsoe/client.py:84`).
7. `BronzeWriter.write()` persists raw bodies and `.meta.json` sidecars under `data/bronze/{source}/{dataset}/{YYYY}/{MM}/{DD}/` (`src/gridflow/bronze/writer.py:17`).
8. `_import_transformers()` imports provider transformer packages so `(source, dataset)` transformers register themselves (`src/gridflow/cli.py:645`).
9. `get_transformer()` resolves the transformer and `BaseSilverTransformer.run()` reads bronze, transforms data, writes Parquet, and writes CSV (`src/gridflow/silver/registry.py:21`, `src/gridflow/silver/base.py:44`).
10. Gold builders read silver Parquet and write gold Parquet through `BaseGoldBuilder.run()` (`src/gridflow/gold/base.py:31`, `src/gridflow/gold/system_marginal_price.py:20`).
11. DuckDB views are refreshed from filesystem Parquet and SQL view files (`src/gridflow/storage/duckdb.py:107`, `src/gridflow/storage/duckdb.py:148`).

### Debug Script Path

1. `scripts/run_pipeline.py` inserts `src` into `sys.path` for IDE execution (`scripts/run_pipeline.py:93`).
2. `_setup()` loads settings, creates `data/`, configures logging, initializes DuckDB, and returns a connection (`scripts/run_pipeline.py:98`).
3. `main()` resolves dates and routes to `run_bronze()`, `run_silver()`, and `run_gold()` depending on `--step` (`scripts/run_pipeline.py:278`).
4. `run_bronze()`, `run_silver()`, and `run_gold()` use the same production modules as the Typer CLI (`scripts/run_pipeline.py:183`, `scripts/run_pipeline.py:217`, `scripts/run_pipeline.py:244`).

### Query Path

1. `GridflowClient.__init__()` opens `data/gridflow.duckdb` read-only (`src/gridflow/serving/client.py:21`).
2. Methods build SQL against views such as `silver_system_prices`, `gold_eu_gas_storage`, and `gold_uk_imbalance_context` (`src/gridflow/serving/client.py:34`, `src/gridflow/serving/client.py:83`, `src/gridflow/serving/client.py:132`).
3. `query()` returns DuckDB results as a Polars `DataFrame` (`src/gridflow/serving/client.py:30`).

**State Management:**
- Runtime settings are loaded per command through `load_settings()`; no long-lived application container exists.
- Registries are module-level dictionaries: `_REGISTRY` in `src/gridflow/connectors/registry.py` and `_REGISTRY` in `src/gridflow/silver/registry.py`.
- Pipeline run state, quality reports, and watermarks are persisted in DuckDB tables created by `src/gridflow/storage/duckdb.py`.
- Data state is filesystem-backed under `data/bronze/`, `data/silver/`, `data/gold/`, and `data/exports/`.

## Key Abstractions

**RawResponse:**
- Purpose: Immutable response body plus provenance metadata for bronze persistence.
- Examples: `src/gridflow/connectors/base.py:16`, `src/gridflow/connectors/elexon/client.py:100`, `src/gridflow/connectors/entsoe/client.py:190`
- Pattern: Dataclass value object returned by connector `fetch()` methods and consumed by `BronzeWriter.write()`.

**BaseConnector:**
- Purpose: Shared async connector contract and `httpx.AsyncClient` lifecycle.
- Examples: `src/gridflow/connectors/base.py:35`, `src/gridflow/connectors/elexon/client.py:22`, `src/gridflow/connectors/entsoe/client.py:46`
- Pattern: Abstract base class with `fetch()` and `list_datasets()` methods plus async context manager setup.

**Connector Registry:**
- Purpose: Resolve source names such as `elexon`, `entsoe`, `gie_agsi`, and `open_meteo` to connector classes.
- Examples: `src/gridflow/connectors/registry.py:16`, `src/gridflow/connectors/elexon/client.py:313`, `src/gridflow/connectors/gie/client.py:161`
- Pattern: Module-level dictionary populated by import-time `register_connector()` calls.

**BronzeWriter:**
- Purpose: Persist raw bodies and metadata sidecars into a deterministic partition layout.
- Examples: `src/gridflow/bronze/writer.py:15`
- Pattern: Stateless writer object initialized with `data_dir`.

**BaseSilverTransformer:**
- Purpose: Common template method for `read_bronze() -> transform() -> write silver`.
- Examples: `src/gridflow/silver/base.py:18`, `src/gridflow/silver/elexon/system_prices.py:16`, `src/gridflow/silver/entsoe/day_ahead_prices.py:18`
- Pattern: Abstract base class with subclass-specific parsing/normalization and common output writes.

**Silver Transformer Registry:**
- Purpose: Resolve `(source, dataset)` to a transformer class.
- Examples: `src/gridflow/silver/registry.py:14`, `src/gridflow/silver/elexon/system_prices.py:149`, `src/gridflow/silver/entsoe/day_ahead_prices.py:85`
- Pattern: Module-level dictionary populated by import-time `register_transformer()` calls.

**BaseGoldBuilder:**
- Purpose: Common template for building gold datasets from silver data and writing date-partitioned outputs.
- Examples: `src/gridflow/gold/base.py:17`, `src/gridflow/gold/system_marginal_price.py:20`
- Pattern: Abstract base class with subclass-specific `build()` and common output writes.

**PathBuilder:**
- Purpose: Canonical path generation for bronze, silver, gold, and DuckDB locations.
- Examples: `src/gridflow/storage/paths.py:12`
- Pattern: Small utility object around `pathlib.Path`.

**PipelineRunTracker:**
- Purpose: Record pipeline operation lifecycle and row counts in DuckDB.
- Examples: `src/gridflow/observability.py:15`
- Pattern: Stateful tracker object created per source/dataset/operation.

## Entry Points

**Installed CLI:**
- Location: `src/gridflow/cli.py`
- Triggers: `gridflow` console script declared in `pyproject.toml`.
- Responsibilities: Main user command surface for pipeline operations.

**Module CLI:**
- Location: `src/gridflow/__main__.py`
- Triggers: `python -m gridflow`.
- Responsibilities: Calls the Typer app from `src/gridflow/cli.py`.

**IDE/Debug Runner:**
- Location: `scripts/run_pipeline.py`
- Triggers: `python scripts/run_pipeline.py --step ...`.
- Responsibilities: Runs bronze/silver/gold steps with argparse without installing the package.

**Initialization Script:**
- Location: `scripts/init_duckdb.py`
- Triggers: `python scripts/init_duckdb.py`.
- Responsibilities: Loads settings and initializes DuckDB catalogue.

**Backfill Script:**
- Location: `scripts/backfill.py`
- Triggers: `python scripts/backfill.py` or direct function import.
- Responsibilities: Historical chunk processing via subprocess/CLI orchestration.

**CSV Export Script:**
- Location: `scripts/export_to_csv.py`
- Triggers: `python scripts/export_to_csv.py`.
- Responsibilities: Exports DuckDB views or generated datasets to CSV.

**Python Query SDK:**
- Location: `src/gridflow/serving/client.py`
- Triggers: `from gridflow.serving.client import GridflowClient`.
- Responsibilities: Read-only queries against generated DuckDB views.

## Architectural Constraints

- **Threading:** The ingest layer uses Python `asyncio` and `httpx.AsyncClient`; per-connector request concurrency is limited by an `asyncio.Semaphore` created in `BaseConnector.__aenter__()` (`src/gridflow/connectors/base.py:62`). Silver, gold, DuckDB, and CLI orchestration are synchronous.
- **Global state:** Connector and transformer registries are module-level dictionaries in `src/gridflow/connectors/registry.py` and `src/gridflow/silver/registry.py`. Registration depends on importing provider modules before calling `get_connector()` or `get_transformer()`.
- **Storage locality:** The system assumes writable local filesystem paths from `config/settings.yaml`, especially `data/`, `logs/`, and `data/gridflow.duckdb`.
- **DuckDB locking:** `get_connection()` retries transient DuckDB file-lock failures, including cloud-sync file locks, before raising a runtime error (`src/gridflow/storage/duckdb.py:14`).
- **View naming:** DuckDB silver views are named as `silver_{dataset}` from dataset directory names only (`src/gridflow/storage/duckdb.py:118`). Add new cross-source datasets with unique dataset names or adjust view naming to include source.
- **Gold registry:** Gold builders are manually listed in dictionaries inside `src/gridflow/cli.py:151` and `scripts/run_pipeline.py:250`; adding a gold builder requires updating both entry points.
- **Import side effects:** `_import_connectors()` and `_import_transformers()` silently ignore `ImportError` (`src/gridflow/cli.py:629`, `src/gridflow/cli.py:645`). Missing imports can hide registration failures.
- **Circular imports:** No circular import chain is detected from the scanned source files. Keep registries independent of provider modules except through import-time registration calls.

## Anti-Patterns

### Silent Import-Time Registration Failure

**What happens:** Orchestration imports provider packages to trigger registration and catches `ImportError` with `pass` (`src/gridflow/cli.py:629`, `src/gridflow/cli.py:645`). `src/gridflow/silver/elexon/__init__.py` imports modules such as `gridflow.silver.elexon.agpt`, `gridflow.silver.elexon.fuelinst`, and `gridflow.silver.elexon.market_depth` that are not present in the root `src/gridflow/silver/elexon/` tree.

**Why it's wrong:** A failed package import can prevent all later registration calls in that package from running while the CLI continues until `get_transformer()` raises a later "not registered" error.

**Do this instead:** Add new transformer modules under `src/gridflow/silver/{source}/`, register each transformer at the bottom of its own module, and keep `src/gridflow/silver/{source}/__init__.py` synchronized with files that actually exist. Prefer logging failed imports in `_import_transformers()` in `src/gridflow/cli.py`.

### Duplicated Gold Builder Registry

**What happens:** Gold builders are listed separately in `src/gridflow/cli.py:151` and `scripts/run_pipeline.py:250`.

**Why it's wrong:** A new gold builder can work through one entry point and be unavailable through the other.

**Do this instead:** Add a shared gold registry module such as `src/gridflow/gold/registry.py`, then have both `src/gridflow/cli.py` and `scripts/run_pipeline.py` use the same registration path.

### Dataset-Only DuckDB View Names

**What happens:** Silver views are registered as `silver_{dataset}` without source names in `src/gridflow/storage/duckdb.py:118`.

**Why it's wrong:** Two sources with the same dataset directory name can overwrite each other's view names.

**Do this instead:** Keep dataset names globally unique or change view registration to `silver_{source}_{dataset}` in `src/gridflow/storage/duckdb.py` and update `src/gridflow/serving/client.py` queries accordingly.

## Error Handling

**Strategy:** Pipeline commands catch per-dataset exceptions, record failure metadata, log stack traces, print concise CLI errors, and continue to the next dataset.

**Patterns:**
- Connector HTTP calls use `resp.raise_for_status()` with a shared retry decorator in provider clients such as `src/gridflow/connectors/elexon/client.py:300` and `src/gridflow/connectors/entsoe/client.py:250`.
- Bronze and silver parsing errors are logged and skipped at file/record boundaries, as in `src/gridflow/silver/elexon/system_prices.py:35` and `src/gridflow/silver/entsoe/day_ahead_prices.py:35`.
- Missing required columns in transforms return an empty dataframe and log errors, as in `src/gridflow/silver/elexon/system_prices.py:67` and `src/gridflow/silver/entsoe/day_ahead_prices.py:48`.
- Pipeline run failures call `PipelineRunTracker.fail()` before continuing (`src/gridflow/cli.py:64`, `src/gridflow/cli.py:119`, `src/gridflow/cli.py:180`).
- DuckDB view registration catches missing-file or unavailable-source errors and logs at debug level (`src/gridflow/storage/duckdb.py:134`, `src/gridflow/storage/duckdb.py:148`).

## Cross-Cutting Concerns

**Logging:** Logging is standard library `logging`; initialize it with `setup_logging()` in `src/gridflow/utils/logging.py` before pipeline work. Use module loggers via `logging.getLogger(__name__)`.

**Validation:** Runtime config validation uses Pydantic models in `src/gridflow/config/settings.py`. Data contract validation is represented by Pydantic schemas in `src/gridflow/schemas/`, while transformation output is mostly enforced by Polars casting and column selection inside `src/gridflow/silver/`.

**Authentication:** Source config defines auth env var names and header names in `config/sources.yaml`; secrets are loaded through `PipelineSettings` with `env_file=".env"` in `src/gridflow/config/settings.py:44`. ENTSO-E injects `securityToken` query params in `src/gridflow/connectors/entsoe/client.py`; GIE uses an `x-key` header through base auth header handling.

**Observability:** Use `PipelineRunTracker` from `src/gridflow/observability.py` for every source/dataset/operation. Metadata tables live in DuckDB and are created in `src/gridflow/storage/duckdb.py`.

**Atomic Writes:** Parquet and silver CSV outputs use temp files and `os.replace()` in `src/gridflow/storage/parquet.py:14` and `src/gridflow/silver/base.py:74`.

**Time Handling:** GB settlement-period logic and date ranges live in `src/gridflow/utils/time.py`. Transformers that use settlement periods should derive `timestamp_utc` with `settlement_period_to_utc()`.

---

*Architecture analysis: 2026-05-02*
