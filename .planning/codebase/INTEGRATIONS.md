# External Integrations

**Analysis Date:** 2026-05-02

## APIs & External Services

**Energy Market Data APIs:**
- Elexon BMRS / Insights API - UK electricity market datasets including system prices, balancing data, fuel mix, demand, REMIT, reference BM units, and forecasts.
  - Configuration: `config/sources.yaml`
  - SDK/Client: `httpx` via `src/gridflow/connectors/elexon/client.py`
  - Endpoint metadata: `src/gridflow/connectors/elexon/endpoints.py`
  - Parser helpers: `src/gridflow/connectors/elexon/parsers.py`
  - Base URL: `https://data.elexon.co.uk/bmrs/api/v1`
  - Auth: no auth configured in `config/sources.yaml`; `GRIDFLOW_ELEXON_API_KEY` exists in `PipelineSettings` but the source config has no `api_key_header`.
- ENTSO-E Transparency Platform API - EU electricity market XML datasets including prices, load, generation, flows, forecasts, outages, capacity, and balancing datasets.
  - Configuration: `config/sources.yaml`
  - SDK/Client: `httpx` via `src/gridflow/connectors/entsoe/client.py`
  - Endpoint metadata: `src/gridflow/connectors/entsoe/endpoints.py`
  - Parser helpers: `src/gridflow/connectors/entsoe/parsers.py`
  - Base URL: `https://web-api.tp.entsoe.eu`
  - Auth: `ENTSOE_API_KEY` from `config/sources.yaml` and `GRIDFLOW_ENTSOE_API_KEY` from `src/gridflow/config/settings.py`; connector sends the token as query parameter `securityToken`.
- ENTSO-G Transparency Platform API - European gas physical flow data.
  - Configuration: `config/sources.yaml`
  - SDK/Client: `httpx` via `src/gridflow/connectors/entsog/client.py`
  - Endpoint metadata: `src/gridflow/connectors/entsog/endpoints.py`
  - Base URL: `https://transparency.entsog.eu/api/v1`
  - Auth: no auth configured in `config/sources.yaml`; `GRIDFLOW_ENTSOG_API_KEY` exists in `PipelineSettings` but the connector is implemented as a public API client.
- GIE AGSI+ API - European gas storage data.
  - Configuration: `config/sources.yaml`
  - SDK/Client: `httpx` via `src/gridflow/connectors/gie/client.py`
  - Endpoint metadata: `src/gridflow/connectors/gie/endpoints.py`
  - Base URL: `https://agsi.gie.eu`
  - Auth: `GIE_API_KEY`, sent as header `x-key` by `src/gridflow/connectors/base.py`.
- GIE ALSI API - European LNG terminal data.
  - Configuration: `config/sources.yaml`
  - SDK/Client: `httpx` via `src/gridflow/connectors/gie/client.py`
  - Endpoint metadata: `src/gridflow/connectors/gie/endpoints.py`
  - Base URL: `https://alsi.gie.eu`
  - Auth: `GIE_API_KEY`, sent as header `x-key` by `src/gridflow/connectors/base.py`.

**Weather & Carbon APIs:**
- Open-Meteo API - historical and forecast weather data used by silver weather transforms and serving queries.
  - Configuration: `config/sources.yaml`
  - SDK/Client: `httpx` via `src/gridflow/connectors/openmeteo/client.py`
  - Endpoint metadata: `src/gridflow/connectors/openmeteo/endpoints.py`
  - Base URLs: `https://api.open-meteo.com/v1` in `config/sources.yaml`, plus archive/forecast absolute URLs from `src/gridflow/connectors/openmeteo/endpoints.py`
  - Auth: none.
- NESO / National Grid Carbon Intensity API - GB carbon intensity forecasts and actuals.
  - Configuration: `config/sources.yaml`
  - SDK/Client: `httpx` via `src/gridflow/connectors/neso/carbon_intensity.py`
  - Base URL: `https://api.carbonintensity.org.uk`
  - Auth: none.

**Developer Services:**
- GitHub Actions - CI for linting, formatting, type checking, and tests.
  - Configuration: `.github/workflows/ci.yml`
  - Auth: GitHub repository token managed by GitHub Actions; no project-defined secret use detected in the workflow.
- GitHub-hosted pre-commit hook source - Ruff pre-commit hooks are pulled from `https://github.com/astral-sh/ruff-pre-commit`.
  - Configuration: `.pre-commit-config.yaml`
  - Auth: none.

## Data Storage

**Databases:**
- DuckDB local file database.
  - Connection: `config/settings.yaml` sets `duckdb_path` to `./data/gridflow.duckdb`.
  - Client: `duckdb` in `src/gridflow/storage/duckdb.py`, `src/gridflow/observability.py`, `src/gridflow/quality/reporter.py`, and `src/gridflow/serving/client.py`.
  - Metadata tables: `pipeline_runs`, `pipeline_watermarks`, and `quality_reports` are created in `src/gridflow/storage/duckdb.py`.
  - Views: silver and gold Parquet directories are registered with `read_parquet(..., hive_partitioning=true)` in `src/gridflow/storage/duckdb.py`.

**File Storage:**
- Local filesystem only.
  - Root data directory: `config/settings.yaml` sets `data_dir` to `./data`.
  - Bronze raw API response files: written by `src/gridflow/bronze/writer.py`.
  - Silver and gold Parquet files: path conventions in `src/gridflow/storage/paths.py`, writes in `src/gridflow/storage/parquet.py`, transform base code in `src/gridflow/silver/base.py`, and gold builder base code in `src/gridflow/gold/base.py`.
  - CSV exports: `src/gridflow/cli.py` writes exports below `data/exports/` unless `--output` is supplied.

**Caching:**
- None detected.
- Pipeline watermarks in DuckDB (`pipeline_watermarks`) track incremental processing state in `src/gridflow/observability.py`; this is state tracking, not a cache layer.

## Authentication & Identity

**Auth Provider:**
- No user authentication or identity provider detected.
  - Implementation: CLI-only local execution through `src/gridflow/cli.py`.

**API Authentication:**
- Header-based auth is implemented generically in `BaseConnector._auth_headers()` at `src/gridflow/connectors/base.py`; it sends `config.api_key` under `config.api_key_header` when both are set.
- ENTSO-E uses query-parameter auth in `src/gridflow/connectors/entsoe/client.py`; the connector sends `securityToken` when `config.api_key` is set.
- GIE AGSI+/ALSI use `GIE_API_KEY` and header `x-key` as configured in `config/sources.yaml`.
- Elexon, Open-Meteo, ENTSO-G, and NESO are configured as public/no-auth sources in `config/sources.yaml`.

## Monitoring & Observability

**Error Tracking:**
- None detected. No Sentry, OpenTelemetry, Prometheus, hosted logging, or APM integration found.

**Logs:**
- Local structured logging uses `python-json-logger` in `src/gridflow/utils/logging.py`.
- Log directory is configured as `./logs` in `config/settings.yaml`.
- File logs are named `gridflow_YYYY-MM-DD.log` and console logs are emitted to stderr in `src/gridflow/utils/logging.py`.
- Pipeline run status and failures are stored in DuckDB table `pipeline_runs` by `src/gridflow/observability.py`.
- Data quality results are written by `src/gridflow/quality/reporter.py` into local reports and DuckDB `quality_reports`.

## CI/CD & Deployment

**Hosting:**
- Not detected. The codebase is packaged as a Python CLI/data pipeline, not a deployed web service.

**CI Pipeline:**
- GitHub Actions in `.github/workflows/ci.yml`.
- CI runs on `push` and `pull_request`.
- CI steps: checkout, setup Python 3.12, `pip install -e ".[dev]"`, `ruff check src/ tests/`, `ruff format --check src/ tests/`, `mypy src/gridflow/`, and `pytest tests/ -v --tb=short -x`.

## Environment Configuration

**Required env vars:**
- `ENTSOE_API_KEY` - configured in `config/sources.yaml` for ENTSO-E API auth.
- `GIE_API_KEY` - configured in `config/sources.yaml` for GIE AGSI+/ALSI API auth.

**Optional or code-supported env vars:**
- `GRIDFLOW_DATA_DIR` - overrides `PipelineSettings.data_dir` in `src/gridflow/config/settings.py`.
- `GRIDFLOW_LOG_DIR` - overrides `PipelineSettings.log_dir` in `src/gridflow/config/settings.py`.
- `GRIDFLOW_DUCKDB_PATH` - overrides `PipelineSettings.duckdb_path` in `src/gridflow/config/settings.py`.
- `GRIDFLOW_DEFAULT_LOOKBACK_HOURS` - overrides default lookback in `src/gridflow/config/settings.py`.
- `GRIDFLOW_MAX_CONCURRENT_REQUESTS` - overrides concurrency setting in `src/gridflow/config/settings.py`.
- `GRIDFLOW_LOG_LEVEL` - overrides logging level in `src/gridflow/config/settings.py`.
- `GRIDFLOW_ELEXON_API_KEY`, `GRIDFLOW_ENTSOE_API_KEY`, and `GRIDFLOW_ENTSOG_API_KEY` - supported by `PipelineSettings` in `src/gridflow/config/settings.py`.

**Secrets location:**
- `.env` file present - contains environment configuration and was not read.
- `.env.example` file present - contents were not read.
- `config/sources.yaml` contains env var names and header names, not secret values.

## Webhooks & Callbacks

**Incoming:**
- None detected. No web server, route handlers, webhook endpoints, or callback receivers found under `src/gridflow/`.

**Outgoing:**
- None detected beyond HTTP GET requests to configured data APIs.
- API calls are initiated by CLI/pipeline commands in `src/gridflow/cli.py` and `scripts/run_pipeline.py` through connector classes registered in `src/gridflow/connectors/registry.py`.

---

*Integration audit: 2026-05-02*
