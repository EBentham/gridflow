# Technology Stack

**Analysis Date:** 2026-05-02

## Languages

**Primary:**
- Python >=3.11 - application code, CLI, connectors, pipeline orchestration, transforms, quality checks, and tests in `src/gridflow/`, `scripts/`, and `tests/`.

**Secondary:**
- SQL - DuckDB gold-layer views in `src/gridflow/gold/views/eu_gas_storage.sql` and `src/gridflow/gold/views/uk_imbalance_context.sql`.
- YAML - operational configuration in `config/settings.yaml`, source/API configuration in `config/sources.yaml`, CI in `.github/workflows/ci.yml`, and pre-commit hooks in `.pre-commit-config.yaml`.
- Make - developer command shortcuts in `Makefile`.

## Runtime

**Environment:**
- CPython >=3.11 is required by `pyproject.toml`.
- Ruff targets Python 3.11 via `[tool.ruff].target-version = "py311"` in `pyproject.toml`.
- GitHub Actions runs Python 3.12 in `.github/workflows/ci.yml`.

**Package Manager:**
- pip with editable installs (`pip install -e .`, `pip install -e ".[dev]"`) from `Makefile` and `.github/workflows/ci.yml`.
- Build backend: setuptools via `[build-system]` in `pyproject.toml`.
- Lockfile: missing. No `requirements.txt`, `poetry.lock`, `uv.lock`, or `Pipfile.lock` detected.

## Frameworks

**Core:**
- Typer >=0.12 - command-line interface exposed as `gridflow = "gridflow.cli:app"` in `pyproject.toml` and implemented in `src/gridflow/cli.py`.
- Pydantic >=2.5 and pydantic-settings >=2.1 - typed configuration models and `.env`/environment loading in `src/gridflow/config/settings.py`.
- httpx >=0.27 - async HTTP client for all API connectors through `src/gridflow/connectors/base.py` and connector implementations under `src/gridflow/connectors/`.
- Tenacity >=8.2 - retry/backoff policy for HTTP calls in `src/gridflow/utils/retry.py`.
- Polars >=1.0 - DataFrame engine for transforms, quality checks, serving queries, and Parquet reads/writes in `src/gridflow/storage/parquet.py` and `src/gridflow/serving/client.py`.
- PyArrow >=15.0 - Parquet backend dependency configured in `pyproject.toml`.
- DuckDB >=1.0 - local analytical catalogue, metadata tables, views, and serving queries in `src/gridflow/storage/duckdb.py`, `src/gridflow/observability.py`, and `src/gridflow/serving/client.py`.
- lxml >=5.0 - XML parsing dependency for ENTSO-E parser modules such as `src/gridflow/connectors/entsoe/parsers.py`.

**Testing:**
- pytest >=8.0 - unit, integration, and contract tests under `tests/`.
- pytest-asyncio >=0.23 - async test support configured with `asyncio_mode = "auto"` in `pyproject.toml`.
- respx >=0.21 - HTTPX mocking in connector tests such as `tests/integration/test_elexon_connector.py` and `tests/integration/test_entsoe_connector.py`.

**Build/Dev:**
- Ruff >=0.4 - linting and formatting configured in `pyproject.toml`, `.pre-commit-config.yaml`, and `Makefile`.
- mypy >=1.10 - strict type checking configured in `pyproject.toml` and run against `src/gridflow/`.
- pre-commit >=3.7 - local hooks configured in `.pre-commit-config.yaml`.
- GitHub Actions - CI pipeline in `.github/workflows/ci.yml`.

## Key Dependencies

**Critical:**
- `httpx` - all external API connectors use `httpx.AsyncClient` and `httpx.Response` in `src/gridflow/connectors/base.py`, `src/gridflow/connectors/elexon/client.py`, `src/gridflow/connectors/entsoe/client.py`, `src/gridflow/connectors/gie/client.py`, `src/gridflow/connectors/entsog/client.py`, `src/gridflow/connectors/openmeteo/client.py`, and `src/gridflow/connectors/neso/carbon_intensity.py`.
- `tenacity` - shared retry policy for timeout and HTTP status failures in `src/gridflow/utils/retry.py`.
- `pydantic` / `pydantic-settings` - source, dataset, quality, and pipeline settings in `src/gridflow/config/settings.py`.
- `polars` - transform output, quality analysis, CSV export, and serving query return type in `src/gridflow/storage/parquet.py`, `src/gridflow/cli.py`, and `src/gridflow/serving/client.py`.
- `duckdb` - persistent local catalogue at `data/gridflow.duckdb`, metadata tables, and read-only serving client in `src/gridflow/storage/duckdb.py`, `src/gridflow/observability.py`, and `src/gridflow/serving/client.py`.
- `pyarrow` - Parquet support dependency for the data lake layout in `data/bronze/`, `data/silver/`, and `data/gold/`.

**Infrastructure:**
- `typer` - operator-facing CLI commands in `src/gridflow/cli.py` and the console script entry in `pyproject.toml`.
- `python-json-logger` - JSON file logging in `src/gridflow/utils/logging.py`.
- `pyyaml` - YAML settings loading in `src/gridflow/config/settings.py`.
- `lxml` - XML parser dependency for ENTSO-E data handling in `src/gridflow/connectors/entsoe/parsers.py`.
- `pytest`, `pytest-asyncio`, and `respx` - local test harness and mocked HTTP integration tests under `tests/`.

## Configuration

**Environment:**
- `PipelineSettings` in `src/gridflow/config/settings.py` loads `.env` and environment variables with the `GRIDFLOW_` prefix.
- `.env` and `.env.example` files are present; contents were not read.
- Operational defaults come from `config/settings.yaml`: `data_dir`, `log_dir`, `duckdb_path`, `default_lookback_hours`, `max_concurrent_requests`, and quality thresholds.
- Source definitions come from `config/sources.yaml`: base URLs, rate limits, timeouts, retry counts, auth env var names, auth headers, schedules, and dataset endpoint metadata.
- API key fields supported by settings code: `GRIDFLOW_ELEXON_API_KEY`, `GRIDFLOW_ENTSOE_API_KEY`, and `GRIDFLOW_ENTSOG_API_KEY` via `PipelineSettings` in `src/gridflow/config/settings.py`; `ENTSOE_API_KEY` and `GIE_API_KEY` via `api_key_env` entries in `config/sources.yaml`.

**Build:**
- `pyproject.toml` is the package, dependency, setuptools, Ruff, mypy, and pytest configuration file.
- `.pre-commit-config.yaml` runs Ruff checks and Ruff formatting.
- `Makefile` provides `install`, `dev`, `lint`, `format`, `format-check`, `typecheck`, `test`, `test-unit`, `test-integration`, `pipeline-daily`, `pipeline-full`, `backfill-elexon`, `init`, `status`, and `clean` targets.
- `.github/workflows/ci.yml` installs `.[dev]`, runs Ruff, Ruff format check, mypy, and pytest.

## Platform Requirements

**Development:**
- Install with `pip install -e ".[dev]"` from the repository root.
- Run static checks with `ruff check src/ tests/`, `ruff format --check src/ tests/`, and `mypy src/gridflow/`.
- Run tests with `pytest tests/ -v --tb=short`; live API tests are marked `live` in `pyproject.toml`.
- Initialize local storage with `python -m gridflow init` or `python scripts/init_duckdb.py`.

**Production:**
- Deployment target: Not detected. The project is structured as a local/CLI data pipeline rather than a hosted service.
- Runtime state is local filesystem and DuckDB by default: raw bronze files, silver/gold Parquet, logs, and `data/gridflow.duckdb` as configured in `config/settings.yaml`.
- Scheduling/orchestration is not provided by a platform integration; use CLI commands in `src/gridflow/cli.py`, `Makefile` targets, or external schedulers around `python -m gridflow`.

---

*Stack analysis: 2026-05-02*
