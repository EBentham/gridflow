# Coding Conventions

**Analysis Date:** 2026-05-02

## Naming Patterns

**Files:**
- Use lowercase snake_case Python modules under `src/gridflow/`: `src/gridflow/utils/time.py`, `src/gridflow/storage/parquet.py`, `src/gridflow/quality/checks.py`.
- Keep source-specific implementations in source-named package folders: `src/gridflow/connectors/elexon/client.py`, `src/gridflow/silver/entsoe/day_ahead_prices.py`, `src/gridflow/silver/gie/agsi.py`.
- Use `__init__.py` files as package import surfaces and registration triggers: `src/gridflow/connectors/elexon/__init__.py`, `src/gridflow/silver/elexon/__init__.py`.
- Test files use `test_*.py` and mirror test type rather than source tree exactly: `tests/unit/test_time_utils.py`, `tests/integration/test_elexon_connector.py`, `tests/contracts/test_bronze_silver_contract.py`.

**Functions:**
- Use snake_case for functions and methods: `settlement_period_to_utc()` in `src/gridflow/utils/time.py`, `write_parquet()` in `src/gridflow/storage/parquet.py`, `get_transformer()` in `src/gridflow/silver/registry.py`.
- Use private helper functions with a leading underscore when they are module-internal: `_load_yaml()` in `src/gridflow/config/settings.py`, `_parse_utc()` in `src/gridflow/connectors/entsoe/parsers.py`, `_make_transformer()` in `tests/unit/test_silver_transforms.py`.
- Connector fetch methods are async and named for the data access mode: `fetch()`, `_fetch_date()`, `_fetch_date_path()`, and `_request()` in `src/gridflow/connectors/elexon/client.py`.

**Variables:**
- Use snake_case for local variables and attributes: `target_date`, `bronze_path`, `request_params`, `settlement_period`.
- Use uppercase module constants for fixed mappings and fixtures: `UK_TZ` in `src/gridflow/utils/time.py`, `RUN_PRECEDENCE` in `src/gridflow/silver/elexon/system_prices.py`, `FIXTURES` in `tests/unit/test_silver_transforms.py`.
- Prefix intentionally private module state with `_`: `_REGISTRY` in `src/gridflow/connectors/registry.py` and `src/gridflow/silver/registry.py`, `_RESOLUTION_MAP` in `src/gridflow/connectors/entsoe/parsers.py`.

**Types:**
- Use PascalCase for classes: `RawResponse` in `src/gridflow/connectors/base.py`, `BaseSilverTransformer` in `src/gridflow/silver/base.py`, `PipelineSettings` in `src/gridflow/config/settings.py`.
- Use domain suffixes consistently:
  - `*Connector` for external API clients: `ElexonConnector` in `src/gridflow/connectors/elexon/client.py`.
  - `*Transformer` for bronze-to-silver transforms: `SystemPriceTransformer` in `src/gridflow/silver/elexon/system_prices.py`.
  - `*Builder` for gold-layer builders: `SystemMarginalPriceBuilder` in `src/gridflow/gold/system_marginal_price.py`.
  - Pydantic schema classes use domain nouns: `ElexonSystemPrice` in `src/gridflow/schemas/elexon.py`, `EntsoeDayAheadPrice` in `src/gridflow/schemas/entsoe.py`.

## Code Style

**Formatting:**
- Use Ruff format for `src/` and `tests/`; commands are defined in `Makefile`.
- Line length is 100 characters in `[tool.ruff]` in `pyproject.toml`.
- Target Python is 3.11 via `target-version = "py311"` in `pyproject.toml`.
- Use modern Python union syntax (`Path | str`, `date | None`) as seen in `src/gridflow/storage/parquet.py` and `src/gridflow/connectors/base.py`.

**Linting:**
- Use Ruff check with autofix through `.pre-commit-config.yaml` and `Makefile`.
- Enabled Ruff lint groups in `pyproject.toml`: `E`, `F`, `I`, `N`, `UP`, `B`, `A`, `SIM`, `TCH`.
- Keep imports sorted by Ruff's import rules. Do not manually maintain conflicting ordering.
- Type checking uses mypy strict mode for `src/gridflow/` via `[tool.mypy]` in `pyproject.toml` and `make typecheck`.

## Import Organization

**Order:**
1. `from __future__ import annotations` when future annotations are needed, placed immediately after the module docstring. Examples: `src/gridflow/connectors/base.py`, `src/gridflow/silver/base.py`, `tests/conftest.py`.
2. Standard library imports: `datetime`, `pathlib`, `logging`, `json`, `typing`.
3. Third-party imports: `httpx`, `polars as pl`, `pydantic`, `pytest`, `respx`.
4. First-party imports from `gridflow.*`.
5. Type-only imports belong in `if TYPE_CHECKING:` blocks when they would otherwise create runtime dependencies, as in `src/gridflow/connectors/registry.py`.

**Path Aliases:**
- Import first-party code as `gridflow.*`, relying on the `src` package layout from `[tool.setuptools.packages.find]` in `pyproject.toml`.
- No custom import alias is configured beyond the package name. Avoid relative imports between package modules.

## Error Handling

**Patterns:**
- Raise `ValueError` for invalid domain inputs or unknown registry keys: `GridflowConfig.get_source_config()` in `src/gridflow/config/settings.py`, `get_transformer()` in `src/gridflow/silver/registry.py`, `ElexonConnector.fetch()` in `src/gridflow/connectors/elexon/client.py`.
- Raise `RuntimeError` for incorrect connector lifecycle or backend initialization failures: `src/gridflow/connectors/elexon/client.py`, `src/gridflow/storage/duckdb.py`.
- Return an empty `polars.DataFrame` for absent source data or recoverable parse failures in transformers: `SystemPriceTransformer.read_bronze()` and `SystemPriceTransformer.transform()` in `src/gridflow/silver/elexon/system_prices.py`.
- Log and continue on per-file parse failures in ingestion/transform paths. Examples: JSON parse warnings in `src/gridflow/silver/elexon/system_prices.py`, XML parse warnings in `src/gridflow/silver/entsoe/day_ahead_prices.py`.
- Use Pydantic validators for schema-level validation and let callers/tests catch `ValidationError`: `TimestampMixin.must_be_utc()` in `src/gridflow/schemas/common.py`, schema tests in `tests/unit/test_schemas.py`.
- CLI commands translate user-facing failures into Typer exceptions or non-zero exits: `typer.BadParameter` and `typer.Exit` in `src/gridflow/cli.py`.

## Logging

**Framework:** Python `logging` with `python-json-logger` for file output.

**Patterns:**
- Create a module-level logger with `logger = logging.getLogger(__name__)`, as in `src/gridflow/bronze/writer.py`, `src/gridflow/silver/base.py`, and `src/gridflow/connectors/elexon/client.py`.
- Configure project logging only through `setup_logging()` in `src/gridflow/utils/logging.py`; do not configure handlers in feature modules.
- Use `logger.info()` for successful pipeline actions and row counts: `BaseSilverTransformer.run()` in `src/gridflow/silver/base.py`, `BaseGoldBuilder.run()` in `src/gridflow/gold/base.py`.
- Use `logger.warning()` for missing optional data, empty source directories, and recoverable parse failures.
- Use `logger.error()` when required columns or parse dependencies are unavailable, then return an empty result where the pipeline can continue.

## Comments

**When to Comment:**
- Prefer module docstrings and class/method docstrings for public behavior. Most source files begin with a short module docstring such as `src/gridflow/connectors/base.py` and `src/gridflow/quality/checks.py`.
- Use inline comments for domain-specific quirks, API variations, and compatibility decisions. Examples: Elexon run type precedence in `src/gridflow/silver/elexon/system_prices.py`, endpoint parameter details in `src/gridflow/connectors/elexon/client.py`.
- Avoid comments that restate simple code. Keep comments focused on energy-market semantics, external API behavior, filesystem layout, or non-obvious validation rules.

**JSDoc/TSDoc:**
- Not applicable. Use Python docstrings with concise summaries and, for complex functions, short argument/return sections as in `parse_timeseries_xml()` in `src/gridflow/connectors/entsoe/parsers.py`.

## Function Design

**Size:** Keep small utilities as standalone pure functions in `src/gridflow/utils/`, `src/gridflow/storage/`, and parser modules. Larger workflows belong on connector, transformer, builder, or CLI classes.

**Parameters:** Prefer typed parameters and explicit domain names:
- `write_parquet(df: pl.DataFrame, path: Path, compression: str = "zstd")` in `src/gridflow/storage/parquet.py`.
- `fetch(dataset: str, start: datetime, end: datetime, **params: Any)` in `src/gridflow/connectors/base.py`.
- `run(self, target_date: date) -> int` in `src/gridflow/silver/base.py`.

**Return Values:** Prefer concrete return types:
- `list[RawResponse]` for connectors in `src/gridflow/connectors/base.py`.
- `pl.DataFrame` for readers and transformers in `src/gridflow/silver/base.py`.
- `QualityResult` for checks in `src/gridflow/quality/checks.py`.
- `Path` for filesystem writers such as `write_parquet()` in `src/gridflow/storage/parquet.py`.

## Module Design

**Exports:** Use class-per-domain-file modules and register implementations at module load where registries are used:
- `register_connector("elexon", ElexonConnector)` pattern in connector modules under `src/gridflow/connectors/`.
- `register_transformer("elexon", "system_prices", SystemPriceTransformer)` in `src/gridflow/silver/elexon/system_prices.py`.

**Barrel Files:** Package `__init__.py` files import concrete implementations to expose public package surfaces and trigger registration. Follow this for new sources or datasets in `src/gridflow/connectors/{source}/__init__.py` and `src/gridflow/silver/{source}/__init__.py`.

---

*Convention analysis: 2026-05-02*
