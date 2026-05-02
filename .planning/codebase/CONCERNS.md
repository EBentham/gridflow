# Codebase Concerns

**Analysis Date:** 2026-05-02

## Tech Debt

**Serving SDK SQL is hand-built with f-strings:**
- Issue: Query helpers interpolate caller-provided values directly into SQL strings instead of using DuckDB parameters.
- Files: `src/gridflow/serving/client.py`
- Impact: `GridflowClient.get_system_prices()`, `GridflowClient.get_gas_storage()`, `GridflowClient.get_weather()`, and `GridflowClient.get_imbalance_context()` can fail on quoted input and expose SQL injection risk for any application that passes user-controlled dates, `country_code`, or `location`.
- Fix approach: Route all user values through DuckDB parameter binding. Keep `GridflowClient.query()` as an explicit raw-SQL escape hatch, but make convenience methods construct parameterized queries.

**Pipeline entry points duplicate orchestration logic:**
- Issue: `src/gridflow/cli.py` and `scripts/run_pipeline.py` each maintain connector imports, transformer imports, date resolution, dataset resolution, and bronze/silver/gold step loops.
- Files: `src/gridflow/cli.py`, `scripts/run_pipeline.py`
- Impact: Behavior can drift between the CLI and IDE/debug script. Error handling, registration imports, and future pipeline stages must be updated in multiple places.
- Fix approach: Move orchestration into a shared module such as `src/gridflow/pipeline/runner.py`. Keep Typer and argparse modules as thin adapters that parse arguments and call the shared runner.

**Connector registration depends on manual import lists:**
- Issue: Connector and transformer registration happens through import side effects, and the import lists suppress `ImportError`.
- Files: `src/gridflow/cli.py`, `scripts/run_pipeline.py`, `src/gridflow/connectors/registry.py`, `src/gridflow/silver/registry.py`
- Impact: A broken module import can be hidden as a missing registration, making failures show up later as "unknown connector" or "unknown transformer" rather than as the original import error.
- Fix approach: Replace broad `except ImportError: pass` with logging that includes the module name, and only suppress the exact optional-module case. Prefer explicit registries generated in package `__init__` files or config-driven discovery.

**Watermark helpers are disconnected from ingestion:**
- Issue: `update_watermark()` and `get_watermark()` exist, and `pipeline_watermarks` is created, but ingest/backfill paths do not use them to choose ranges or record completed ranges.
- Files: `src/gridflow/observability.py`, `src/gridflow/cli.py`, `scripts/run_pipeline.py`, `src/gridflow/storage/duckdb.py`
- Impact: Incremental runs rely on caller-provided dates and default lookbacks. Duplicate ingestion and gaps are easy to create during reruns or scheduled execution.
- Fix approach: Have successful ingest update `(source, dataset)` watermarks and add an option to resolve missing `start` from `get_watermark()`. Treat manual `--start` as an override.

**Quality configuration is not wired into quality checks:**
- Issue: `QualityConfig.null_rate_threshold` is loaded but `gridflow quality` calls `check_null_rate()` with its default `max_rate=0.05`; time-series checks hardcode 30-minute frequency.
- Files: `src/gridflow/config/settings.py`, `src/gridflow/cli.py`, `src/gridflow/quality/checks.py`
- Impact: Changing `config/settings.yaml` does not affect null-rate quality checks, and hourly datasets such as weather can be flagged with 30-minute gap assumptions.
- Fix approach: Pass `settings.quality.null_rate_threshold` into `check_null_rate()` and add per-dataset expected frequency metadata in `config/sources.yaml`.

**Silver writes duplicate Parquet and CSV outputs by default:**
- Issue: every silver transform writes partitioned Parquet and an unpartitioned CSV copy for each date.
- Files: `src/gridflow/silver/base.py`
- Impact: Storage and write time grow unnecessarily for production runs. CSV writes also bypass the shared Parquet storage abstraction and double the write surface that must be kept atomic.
- Fix approach: Make CSV output opt-in via configuration or a CLI export command. Keep silver-layer canonical storage as Parquet.

## Known Bugs

**`GridflowClient.get_generation_by_fuel()` targets a view that is not registered:**
- Symptoms: Calling `GridflowClient.get_generation_by_fuel()` queries `silver_generation_by_fuel`, while current config and transformer registration use `fuelhh` and comments identify `generation_by_fuel` as absent.
- Files: `src/gridflow/serving/client.py`, `config/sources.yaml`, `src/gridflow/connectors/elexon/endpoints.py`, `src/gridflow/silver/elexon/__init__.py`, `tests/unit/test_silver_transforms.py`
- Trigger: Build/init the DuckDB catalogue and call `GridflowClient().get_generation_by_fuel(start, end)`.
- Workaround: Use `GridflowClient.get_fuel_generation()` against `silver_fuelhh`.

**GIE API keys from `.env` are not modeled in `PipelineSettings`:**
- Symptoms: `config/sources.yaml` expects `GIE_API_KEY` for `gie_agsi` and `gie_alsi`, but `PipelineSettings` only defines `elexon_api_key`, `entsoe_api_key`, and `entsog_api_key`. `get_source_config()` falls back to `os.environ`, which does not guarantee values loaded from `.env` are present in process environment.
- Files: `src/gridflow/config/settings.py`, `config/sources.yaml`
- Trigger: Put a GIE key only in `.env` and run `gridflow ingest gie_agsi storage ...`.
- Workaround: Export `GIE_API_KEY` in the shell environment before running the command.

**Open-Meteo API requests do not use the shared retry policy:**
- Symptoms: `OpenMeteoConnector._fetch_location()` calls `response.raise_for_status()` directly and logs failures per location, while other HTTP connectors decorate `_request()` with `RETRY_POLICY`.
- Files: `src/gridflow/connectors/openmeteo/client.py`, `src/gridflow/utils/retry.py`
- Trigger: Any transient Open-Meteo timeout or 5xx response during weather ingest.
- Workaround: Rerun the affected date/location; failed locations are skipped for that run.

**Partial GIE and Open-Meteo ingest can be recorded as success:**
- Symptoms: GIE country failures and Open-Meteo location failures are logged and omitted, but fetch returns the remaining responses. The pipeline tracker records the dataset as successful if at least some responses are written.
- Files: `src/gridflow/connectors/gie/client.py`, `src/gridflow/connectors/openmeteo/client.py`, `src/gridflow/cli.py`, `scripts/run_pipeline.py`
- Trigger: One country/location fails while another succeeds in the same dataset run.
- Workaround: Inspect logs for per-country/per-location warnings after runs; there is no structured incomplete-run status.

## Security Considerations

**ENTSO-E tokens are persisted in bronze metadata:**
- Risk: ENTSO-E auth is passed as `securityToken` in query params. The connector stores both `request_url` and `request_params` on `RawResponse`, and `BronzeWriter` writes both fields to `.meta.json` sidecars.
- Files: `src/gridflow/connectors/entsoe/client.py`, `src/gridflow/bronze/writer.py`, `tests/integration/test_entsoe_connector.py`
- Current mitigation: `data/` is ignored by `.gitignore`; `.env` is also ignored.
- Recommendations: Redact known secret keys such as `securityToken`, `api_key`, `x-key`, and configured `api_key_header` values before creating `RawResponse` metadata. Store auth presence, not auth values.

**Raw SQL execution is exposed through the SDK:**
- Risk: `GridflowClient.query(sql)` executes arbitrary SQL against the DuckDB file. This is expected for local analytics, but unsafe for any API service wrapper that forwards user input.
- Files: `src/gridflow/serving/client.py`
- Current mitigation: The client opens DuckDB in read-only mode.
- Recommendations: Keep `query()` documented as trusted-only. Add safe higher-level query methods for external callers and avoid exposing `query()` in web/API contexts.

**XML parser uses default lxml parser settings for external API bodies:**
- Risk: `etree.fromstring(xml_bytes)` parses ENTSO-E response bodies without an explicit parser disabling entity resolution and network access.
- Files: `src/gridflow/connectors/entsoe/parsers.py`
- Current mitigation: Input comes from the ENTSO-E API and parse errors return an empty record set.
- Recommendations: Use `etree.XMLParser(resolve_entities=False, no_network=True, huge_tree=False)` and add parser tests for external entity payloads.

**Reset command trusts configurable data paths:**
- Risk: `gridflow reset --yes` recursively deletes files under `settings.pipeline.data_dir` and deletes `settings.pipeline.duckdb_path` without checking that paths stay inside the project workspace.
- Files: `src/gridflow/cli.py`, `src/gridflow/config/settings.py`
- Current mitigation: Confirmation is required unless `--yes` is passed.
- Recommendations: Resolve target paths and refuse resets outside the configured project data root unless an additional explicit override is supplied. Add a dry-run mode that prints exact targets.

**Export script interpolates filter values into SQL and COPY paths:**
- Risk: `scripts/export_to_csv.py` validates view names against the catalogue but interpolates `--start`, `--end`, `--limit`, and output path into SQL/COPY strings.
- Files: `scripts/export_to_csv.py`
- Current mitigation: This is a local utility script, and view names are checked against `information_schema`.
- Recommendations: Validate dates with `date.fromisoformat()`, reject negative limits, escape or parameterize values where DuckDB supports it, and restrict COPY output to the requested output directory.

## Performance Bottlenecks

**Whole-directory eager Parquet reads do not scale with local data volume:**
- Problem: `read_parquet_dir()` uses eager `pl.read_parquet()` over `**/*.parquet`, and callers filter after loading.
- Files: `src/gridflow/storage/parquet.py`, `src/gridflow/gold/system_marginal_price.py`, `src/gridflow/cli.py`
- Cause: Date filtering happens after all matching files are read into memory.
- Improvement path: Add lazy scan helpers using `pl.scan_parquet()` with predicate pushdown, or build exact partition paths from date ranges before reading.

**Gold system marginal price builds scan all silver system prices:**
- Problem: `SystemMarginalPriceBuilder.build()` reads the entire `data/silver/elexon/system_prices` tree, then filters `settlement_date`.
- Files: `src/gridflow/gold/system_marginal_price.py`, `src/gridflow/storage/parquet.py`
- Cause: The storage helper has no date-partition-aware read API.
- Improvement path: Read only `year=` and `month=` partitions that overlap the requested range, then filter in Polars.

**Quality command loads every silver dataset eagerly:**
- Problem: `gridflow quality` iterates every silver dataset and calls `read_parquet_dir()` before running simple row/null/duplicate/gap checks.
- Files: `src/gridflow/cli.py`, `src/gridflow/quality/checks.py`, `src/gridflow/storage/parquet.py`
- Cause: Quality checks operate on in-memory DataFrames rather than lazy scans or DuckDB aggregate queries.
- Improvement path: Implement aggregate checks in DuckDB views or Polars lazy scans. Run duplicate/gap checks only on configured key/timestamp columns per dataset.

**Local ignored data directory is already large:**
- Problem: The workspace contains 2,533 files under `data/` totaling about 1.7 GB.
- Files: `data/`, `.gitignore`
- Cause: Bronze, silver, gold, and DuckDB outputs accumulate locally.
- Improvement path: Add retention/prune commands, document expected disk usage, and keep `data/` outside synced folders when possible.

## Fragile Areas

**Bronze metadata schema can leak and drift:**
- Files: `src/gridflow/bronze/writer.py`, `src/gridflow/connectors/base.py`
- Why fragile: All connectors can attach arbitrary `request_params`, and the writer persists them without redaction or schema validation.
- Safe modification: Add a metadata sanitizer in `BronzeWriter.write()` and test it with ENTSO-E query-token responses, GIE header-key responses, and header-auth responses.
- Test coverage: `tests/integration/test_bronze_to_silver.py` checks metadata existence and simple fields, but does not test redaction or secret-bearing connectors.

**Exception handling can hide root causes:**
- Files: `src/gridflow/cli.py`, `scripts/run_pipeline.py`, `src/gridflow/observability.py`, `src/gridflow/storage/parquet.py`, `src/gridflow/quality/reporter.py`
- Why fragile: Several broad `except Exception` blocks continue execution or return empty data. This is useful for batch resilience but makes missing files, parse errors, and database write failures look like empty successful work.
- Safe modification: Return structured step results with `success`, `partial`, `failed`, and `skipped` statuses. Keep warnings for expected missing data, but raise or mark failures for parse/database errors.
- Test coverage: Existing unit tests cover many transformers and schemas, but there are no tests for partial pipeline status semantics.

**DuckDB view registration uses source-agnostic names:**
- Files: `src/gridflow/storage/duckdb.py`, `src/gridflow/gold/views/eu_gas_storage.sql`, `src/gridflow/gold/views/uk_imbalance_context.sql`
- Why fragile: Silver view names are `silver_{dataset}` rather than `silver_{source}_{dataset}`. This works while dataset names are unique, but adding another source with a dataset named `storage`, `forecast`, or `physical_flows` would replace or conflict with an existing view.
- Safe modification: Register source-qualified views and maintain backwards-compatible aliases only where a dataset name is intentionally canonical.
- Test coverage: There are no tests asserting view registration behavior with duplicate dataset names.

**Operational scripts bypass the package CLI behavior:**
- Files: `scripts/backfill.py`, `scripts/run_pipeline.py`, `src/gridflow/cli.py`
- Why fragile: `scripts/backfill.py` shells out to `python -m gridflow`, while `src/gridflow/cli.py` has an in-process `backfill` command. Script behavior can differ in environment, logging, and error handling.
- Safe modification: Use the shared pipeline runner for both script and CLI backfills. Keep subprocess use only for explicit process-isolation needs.
- Test coverage: There are no script-level tests for `scripts/backfill.py` or `scripts/run_pipeline.py`.

**Manual gold builder registry is narrow:**
- Files: `src/gridflow/cli.py`, `scripts/run_pipeline.py`, `src/gridflow/gold/system_marginal_price.py`, `src/gridflow/gold/demand_features.py`, `src/gridflow/gold/merit_order.py`
- Why fragile: Gold build commands register only `system_marginal_price`, while additional gold modules exist in `src/gridflow/gold/`.
- Safe modification: Add a gold registry mirroring connector/silver registries, or remove unused gold modules if they are not part of the product surface.
- Test coverage: Contract tests focus on bronze-to-silver and silver-to-gold for system prices; gold module discovery is untested.

## Scaling Limits

**HTTP concurrency is per connector instance and mostly sequential by loop shape:**
- Current capacity: `SourceConfig.rate_limit_per_second` defaults to 5 and is used as a semaphore size.
- Limit: Elexon, ENTSO-E, GIE, and Open-Meteo fetch loops await each country/date/location request one at a time, so the semaphore rarely provides meaningful parallelism.
- Scaling path: Batch independent date, country, zone, or location requests with bounded `asyncio.gather()`. Record partial failures explicitly before increasing concurrency.

**DuckDB writes assume one local writer:**
- Current capacity: `get_connection()` retries local file-lock errors up to 8 times with exponential delay.
- Limit: Multiple pipeline processes can contend for the same DuckDB file and metadata tables; retries hide contention but do not coordinate writes.
- Scaling path: Keep DuckDB metadata writes single-process, move run metadata to an external transactional store, or add process-level locking around catalogue updates.

**Bronze and silver storage has no retention policy:**
- Current capacity: local `data/` contains about 1.7 GB during this audit.
- Limit: Long backfills and repeated reruns grow bronze metadata, raw payloads, CSV copies, Parquet partitions, and DuckDB files without cleanup.
- Scaling path: Add retention policies by layer/source/dataset/date and a dry-run prune command.

## Dependencies at Risk

**No dependency lockfile:**
- Risk: `pyproject.toml` uses lower-bound-only dependency ranges and no `uv.lock`, `poetry.lock`, or requirements lockfile is present.
- Impact: Fresh installs can pick newer `httpx`, `polars`, `duckdb`, `pydantic`, or `lxml` versions with behavior changes not covered by CI.
- Migration plan: Add a lockfile for the chosen package manager and run CI against the lock. Keep a scheduled dependency update job for deliberate upgrades.

**lxml XML parsing needs explicit hardening:**
- Risk: `lxml` is required for ENTSO-E parsing, but parser hardening is not encoded in code or tests.
- Impact: Future XML fixture or upstream API changes can interact badly with default parser behavior.
- Migration plan: Instantiate a safe parser in `src/gridflow/connectors/entsoe/parsers.py` and add security-focused parser fixtures.

## Missing Critical Features

**No structured partial-run status:**
- Problem: Batch runs can skip failed countries, locations, modules, quality writes, or parse files while still completing with success-like command output.
- Blocks: Reliable scheduling, alerting, and automated backfill verification.
- Files: `src/gridflow/cli.py`, `scripts/run_pipeline.py`, `src/gridflow/connectors/gie/client.py`, `src/gridflow/connectors/openmeteo/client.py`, `src/gridflow/observability.py`

**No dry-run for destructive reset:**
- Problem: Users cannot preview exact files and directories before `gridflow reset` deletes them.
- Blocks: Safe operational use on large or synced data directories.
- Files: `src/gridflow/cli.py`

**No scheduled-run interface:**
- Problem: Watermarks, retries, and run tracking exist as pieces, but no scheduler-safe command uses them as a coherent incremental pipeline.
- Blocks: Production automation without external wrappers.
- Files: `src/gridflow/observability.py`, `src/gridflow/cli.py`, `scripts/run_pipeline.py`

## Test Coverage Gaps

**Serving SDK queries:**
- What's not tested: `GridflowClient` methods, missing-view behavior, SQL escaping, and parameter handling.
- Files: `src/gridflow/serving/client.py`, `tests/`
- Risk: Broken convenience methods and SQL injection regressions can ship unnoticed.
- Priority: High

**Operational CLI commands:**
- What's not tested: `gridflow reset`, `gridflow quality`, `gridflow export-csv`, `gridflow status`, and backfill orchestration.
- Files: `src/gridflow/cli.py`, `scripts/backfill.py`, `scripts/run_pipeline.py`, `tests/`
- Risk: Data deletion, empty quality reports, and pipeline status regressions can ship unnoticed.
- Priority: High

**Secret redaction and metadata safety:**
- What's not tested: `BronzeWriter` redaction for query-param and header-based API keys.
- Files: `src/gridflow/bronze/writer.py`, `src/gridflow/connectors/entsoe/client.py`, `src/gridflow/connectors/gie/client.py`, `tests/`
- Risk: API credentials can be written into ignored local data and later copied or uploaded.
- Priority: High

**Connector partial failure semantics:**
- What's not tested: GIE country failures, Open-Meteo location failures, and how the pipeline tracker records partial success.
- Files: `src/gridflow/connectors/gie/client.py`, `src/gridflow/connectors/openmeteo/client.py`, `src/gridflow/observability.py`, `tests/`
- Risk: Incomplete datasets look successful.
- Priority: Medium

**DuckDB catalogue registration:**
- What's not tested: view creation for duplicate dataset names, missing Parquet files, SQL-defined gold views, and source-qualified aliases.
- Files: `src/gridflow/storage/duckdb.py`, `src/gridflow/gold/views/eu_gas_storage.sql`, `src/gridflow/gold/views/uk_imbalance_context.sql`, `tests/`
- Risk: SDK methods fail at runtime when views are absent or overwritten.
- Priority: Medium

---

*Concerns audit: 2026-05-02*
