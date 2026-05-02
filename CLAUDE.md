# gridflow

## What this is

gridflow is a local-first Python data pipeline for UK/EU power and gas market data.
It feeds quantitative models (power stack, demand forecasting, imbalance price).
Architecture is **medallion**: **bronze** (immutable raw API bytes + metadata)
→ **silver** (validated, normalised Polars/Parquet) → **gold** (modelling-ready
datasets and cross-source DuckDB views). No cloud — DuckDB + Parquet on the
local filesystem; cron for scheduling.

## Repo layout

```
src/gridflow/
├── cli.py                     Typer app: init, ingest, transform, build, backfill,
│                              pipeline, export-csv, status, quality, reset
├── __main__.py                Enables `python -m gridflow`
├── config/settings.py         Pydantic-settings + YAML loader (sources.yaml, settings.yaml)
├── connectors/                Bronze: async API clients
│   ├── base.py                BaseConnector ABC + RawResponse dataclass
│   ├── registry.py            register_connector / get_connector / list_sources
│   ├── elexon/                Elexon BMRS (JSON; SETTLEMENT_DATE / SETTLEMENT_DATE_PERIOD /
│   │                          DATE_PATH / PUBLISH_DATETIME / NO_PARAMS styles)
│   ├── openmeteo/             Open-Meteo weather (JSON, two base URLs: archive + forecast)
│   ├── entsoe/                ENTSO-E Transparency (XML, query-param auth, per-zone fetches)
│   ├── entsog/                ENTSO-G physical gas flows (JSON, public)
│   ├── gie/                   GIE AGSI+ / ALSI (JSON, x-key header)
│   └── neso/                  NESO Carbon Intensity (JSON, public, path-based dates)
├── bronze/writer.py           Atomic write of RawResponse body + sidecar metadata
├── silver/                    Bronze → Silver
│   ├── base.py                BaseSilverTransformer ABC (read_bronze, transform, run)
│   ├── registry.py            register_transformer / get_transformer
│   └── {elexon,openmeteo,entsoe,entsog,gie,neso}/   per-dataset modules
├── gold/                      Silver → Gold
│   ├── base.py                BaseGoldBuilder ABC
│   ├── system_marginal_price.py
│   ├── merit_order.py
│   ├── demand_features.py
│   └── views/*.sql            Cross-source DuckDB views (registered by init_catalogue)
├── schemas/                   Pydantic v2 silver-layer data contracts
│   ├── common.py              BaseSchema, TimestampMixin, SettlementPeriodMixin
│   └── {elexon,entsoe,entsog,gie,neso,weather}.py
├── storage/
│   ├── paths.py               PathBuilder — only place that constructs layer paths
│   ├── parquet.py             Atomic write_parquet + read_parquet (hive partitioning)
│   └── duckdb.py              Catalogue init + view registration
├── quality/
│   ├── checks.py              row count, null rate, range, duplicates, time-series gaps
│   └── reporter.py            QualityReporter — writes JSON report
├── observability.py           PipelineRunTracker — records ingest/transform/build runs in DuckDB
├── serving/client.py          GridflowClient (Python SDK around DuckDB)
└── utils/
    ├── time.py                Settlement period ↔ UTC, parse_lookback, date_range
    ├── retry.py               RETRY_POLICY (tenacity) + CircuitBreaker
    └── logging.py             JSON structured logging
```

## Commands

No project task aliases (no Makefile / nox / poe). Use the tools directly via `uv`.

```bash
uv pip install -e ".[dev]"          # install with dev deps
uv run pytest -x -q                 # tests, fail fast
uv run pytest -m "not live"         # skip live-API tests
uv run ruff check src/ tests/
uv run ruff format src/ tests/
uv run mypy src/gridflow/

uv run gridflow init                # create DuckDB catalogue + register views
uv run gridflow ingest <source> <dataset> --last 24h
uv run gridflow transform <source> <dataset> --last 24h
uv run gridflow build <gold_dataset> --last 7d
uv run gridflow pipeline <source> --all --last 24h --gold system_marginal_price
uv run gridflow status              # last 24h pipeline runs
uv run gridflow quality --all       # write quality report
uv run gridflow reset elexon system_prices --silver --yes   # scoped wipe
```

# VERIFY: confirm `uv` is installed locally. If not, replace `uv run X` with `python -m X`
# or fall back to `pip install -e ".[dev]"` and bare `pytest` / `ruff` / `mypy`.

## Stack (from pyproject.toml)

- **httpx** — async HTTP client, all connectors
- **tenacity** — retry policy in `utils/retry.py`
- **pydantic** + **pydantic-settings** — silver schemas + YAML/env config loading
- **polars ≥1.0** — every DataFrame transform, no pandas
- **pyarrow** — Parquet engine under polars
- **duckdb** — local catalogue, gold views, observability table
- **lxml** — ENTSO-E XML parsing
- **typer** — CLI in `cli.py`
- **python-json-logger** — structured logs
- **pyyaml** — `config/sources.yaml`, `config/settings.yaml`
- **dev:** pytest, pytest-asyncio (`asyncio_mode = "auto"`), respx (HTTP mocking),
  ruff, mypy (`strict = true`), pre-commit

## Data conventions

- **Timestamps:** every silver row has a `timestamp_utc` (`datetime`, tz-aware UTC).
  `TimestampMixin.must_be_utc` rejects naive datetimes. Never store naive timestamps.
- **Settlement periods:** UK convention. Ranges 1–50; **50 not 48** because BST→GMT
  clock-change days have 50 half-hour periods (and spring forward has 46). Use
  `utils/time.settlement_period_to_utc()` — never roll your own. Bounds are enforced
  in source-specific schemas (`schemas/elexon.py`); `SettlementPeriodMixin` itself
  only declares the fields.
- **Gas day:** European gas day runs 06:00 UTC → 06:00 UTC. Preserve GIE's
  `gasDayStart`; do not coerce to calendar days.
- **Column naming:** `snake_case`. Units in column names where ambiguous
  (`price_gbp_per_mwh`, `volume_mwh`, `temp_c`). Camel-case API fields are renamed
  in `transform()`.
- **Provenance:** every silver row carries `data_provider` (e.g. `"elexon"`) and
  `ingested_at` (UTC, tz-aware).
- **Bronze immutability:** bronze is the audit trail of raw API bytes. Never edit
  bronze in place; never re-ingest into the same bronze date dir without going
  through `BronzeWriter`. The writer partitions by `data_date` when set on
  `RawResponse`, otherwise by `fetched_at`.
- **Silver atomic writes:** all writes go through `storage/parquet.write_parquet()`,
  which writes `.tmp_<name>.parquet` then `os.replace()` (atomic on Windows).
  Silver overwrite for a given date is idempotent — safe to re-run.
- **Path construction:** never build paths by hand. Use `PathBuilder` from
  `storage/paths.py`.

## Adding a new data source

Spec in the vault first; wait for approval; then implement in this order
(template inferred from `connectors/elexon/` + `silver/elexon/`):

1. **Spec** — `30-vendors/<vendor>/README.md` and `endpoints.md` in the Obsidian
   vault. Document base URL, auth, rate limit, every endpoint, query params, and
   one real response example. Do **not** invent rate limits or schemas — copy
   from the vendor's docs.
2. **Config** — append the source to `config/sources.yaml` (base_url,
   `api_key_env`, `api_key_header`, `rate_limit_per_second`, `timeout`,
   `max_retries`, `datasets:`).
3. **Schema** — `src/gridflow/schemas/<source>.py`. One Pydantic model per silver
   dataset. Inherit `BaseSchema` + `TimestampMixin`/`SettlementPeriodMixin` as
   relevant. Validators for ranges, units, enum codes.
4. **Connector** — `src/gridflow/connectors/<source>/`:
   - `endpoints.py` — endpoint table / param-style enum (mirror Elexon's pattern).
   - `client.py` — subclass `BaseConnector`, set `source_name`, implement `fetch()`
     and `list_datasets()`. Decorate the HTTP call with `@RETRY_POLICY` and gate
     with `async with self._semaphore:`. Build `RawResponse` per page/zone and
     set `data_date` for date-partitioned bronze.
   - Register at module level: `register_connector("<source>", <Source>Connector)`.
   - `__init__.py` — import `client` so the registration runs at import time.
5. **Transformer** — `src/gridflow/silver/<source>/<dataset>.py`:
   - Subclass `BaseSilverTransformer`. Implement `read_bronze(target_date)` and
     `transform(raw_df) -> pl.DataFrame`.
   - Rename camelCase → snake_case, cast types, derive `timestamp_utc`, dedupe,
     add `data_provider` + `ingested_at`. Validate one sample row against the
     Pydantic schema.
   - Register: `register_transformer("<source>", "<dataset>", <Class>)`.
   - Import the module from the source's `__init__.py` so registration fires.
6. **Wire imports** — add `gridflow.connectors.<source>` to `_import_connectors()`
   and `gridflow.silver.<source>` to `_import_transformers()` in `cli.py`.
7. **Fixtures + tests** — `tests/fixtures/<source>/<dataset>_response.{json,xml}`
   (real sample). Unit tests for the transformer (offline). Integration test for
   the connector using `respx` to mock httpx. Contract test asserting silver
   output validates against the Pydantic schema.
8. **Run it** — `uv run pytest tests/unit/test_<source>.py -x -q`, then
   `uv run gridflow ingest <source> <dataset> --last 24h` against a real key on a
   small window. Confirm bronze + silver files exist on disk.

## Vault context (Obsidian)

The Obsidian vault is the long-term context layer. Read the relevant file
**before** starting any task — don't ask the user to paste context. The vault is
exposed via the `obsidian-vault` MCP server; the MCP tools
(`mcp__obsidian-vault__obsidian_get_file_contents`,
`mcp__obsidian-vault__obsidian_simple_search`, etc.) access it directly.

Read at session start:

- `00-active/now.md` — current focus.
- `10-projects/energy-pipeline/README.md` — project status.   # VERIFY directory name
- `10-projects/energy-pipeline/architecture.md` — design.     # VERIFY exists
- `10-projects/energy-pipeline/data-contracts.md` — schema conventions.  # VERIFY exists
- `30-vendors/<vendor>/README.md` and `endpoints.md` — before any vendor work.
- `20-domain/` — domain/market questions (gas day, settlement, BSAD, etc.).
- `40-techniques/` — reusable code patterns we've already committed to.

The vault root `CLAUDE.md` is the vault's reading-discipline contract — follow
it when operating inside the vault. (It's not auto-loaded by Claude Code — fetch
it via the MCP at session start.)

## Workflow rules

- **Spec before code** for any task touching > 2 files. Write the spec into
  `10-projects/energy-pipeline/specs/<slug>.md` first; wait for the user to
  approve; then implement.
- **Plan mode** (Shift+Tab) for any multi-module task.
- **Commit after each passing test**, not at the end of the session.
- **Never commit to `main`** — feature branches only.
- **Conventional commits**: `feat:`, `fix:`, `test:`, `refactor:`, `chore:`, `docs:`.
- **Tests before done**: `uv run pytest -x -q` must pass before declaring a task
  complete. Live API tests (`-m live`) are opt-in and not part of the default run.

## DO NOT

- No pandas — Polars only.
- No bare `except:`. Catch the narrowest exception that makes sense.
- No invented rate limits, endpoints, or schemas — read
  `30-vendors/<vendor>/README.md`. If it's not there, stop and write a `TODO:`
  with one line on what's missing.
- No writing to the vault during coding sessions — keep vault updates to a
  deliberate end-of-session step (e.g. `/end-session`) so the vault stays a
  curated context store, not a scratchpad.   # VERIFY /end-session command exists
- No live API ingestion (`gridflow ingest` against a real key on a real window)
  without explicit user confirmation.
- No new top-level packages under `src/gridflow/` without discussion.
- No new cloud dependencies (S3, GCS, Snowflake). Local-only is a hard constraint.
- No comments that restate what the code does. WHY only, when non-obvious.
- No manual path construction — use `PathBuilder`.
- No `Path.rename()` for atomic writes on Windows — use `os.replace()` (already
  encapsulated in `storage/parquet.write_parquet`).
- No editing existing tests without explicit permission.
