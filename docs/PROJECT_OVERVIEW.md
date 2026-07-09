# Gridflow - Project Overview

## What Is Gridflow?

Gridflow is a **UK/EU energy market data pipeline** that pulls data from multiple public and authenticated APIs, normalises it into clean analytical tables, and stores everything in Parquet files backed by a DuckDB catalogue. It covers electricity prices, generation, demand, gas storage, weather, and carbon intensity.

---

## Architecture at a Glance

Gridflow uses a **Medallion architecture** with three layers:

```
┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│    BRONZE     │     │    SILVER     │     │     GOLD      │
│  (raw files)  │ --> │ (normalised)  │ --> │ (analytics)   │
│  JSON / XML   │     │   Parquet     │     │   Parquet     │
└───────────────┘     └───────────────┘     └───────────────┘
       ▲                                            │
       │                                            ▼
   API Sources                               DuckDB Catalogue
   (7 sources)                              (SQL-queryable views)
```

| Layer  | What It Stores | Format | Key Action |
|--------|---------------|--------|------------|
| Bronze | Raw API responses exactly as received | JSON, XML, CSV + metadata sidecar | `ingest` |
| Silver | Cleaned, typed, deduplicated, normalised tables | Hive-partitioned Parquet | `transform` |
| Gold   | Analytics-ready feature datasets (joins across sources) | Hive-partitioned Parquet | `build` |

---

## Data Sources (7 Total)

| Source | Name in CLI | Auth | What It Provides |
|--------|------------|------|-----------------|
| Elexon BMRS | `elexon` | None (public) | UK electricity: prices, generation, demand, balancing |
| ENTSO-E | `entsoe` | API key (`ENTSOE_API_KEY`) | EU electricity: prices, load, generation, cross-border flows |
| ENTSO-G | `entsog` | None (public) | EU gas: physical flows at interconnection points |
| GIE AGSI | `gie_agsi` | API key (`GIE_API_KEY`) | EU gas storage levels |
| GIE ALSI | `gie_alsi` | API key (`GIE_API_KEY`) | EU LNG terminal data |
| Open-Meteo | `open_meteo` | None (public) | Weather: temperature, wind, radiation, humidity |
| NESO | `neso` | None (public) | UK carbon intensity |

---

## How the Code Is Organised

```
src/gridflow/
├── cli.py                  # Typer CLI — the main entrypoint (7 commands)
├── __main__.py             # Enables `python -m gridflow`
├── config/
│   └── settings.py         # Pydantic settings loader (YAML + env vars)
│
├── connectors/             # BRONZE LAYER — API clients
│   ├── base.py             # BaseConnector ABC + RawResponse dataclass
│   ├── registry.py         # Connector registry (lookup by source name)
│   ├── elexon/             # Elexon connector (21 datasets)
│   ├── entsoe/             # ENTSO-E connector (8 datasets, XML parsing)
│   ├── entsog/             # ENTSO-G connector (physical flows)
│   ├── gie/                # GIE connector (AGSI storage + ALSI LNG)
│   ├── openmeteo/          # Open-Meteo connector (weather)
│   └── neso/               # NESO connector (carbon intensity)
│
├── bronze/
│   └── writer.py           # BronzeWriter — writes raw files + metadata sidecars
│
├── silver/                 # SILVER LAYER — transformers
│   ├── base.py             # BaseSilverTransformer ABC
│   ├── registry.py         # Transformer registry (lookup by source+dataset)
│   ├── elexon/             # 12 Elexon transformers
│   ├── entsoe/             # 4 ENTSO-E transformers
│   ├── entsog/             # 1 ENTSO-G transformer
│   ├── gie/                # 2 GIE transformers (AGSI + ALSI)
│   ├── openmeteo/          # 2 Open-Meteo transformers
│   └── neso/               # 1 NESO transformer
│
├── gold/                   # GOLD LAYER — feature builders
│   ├── base.py             # BaseGoldBuilder ABC
│   ├── registry.py         # gold dataset name -> builder class
│   ├── system_marginal_price.py
│   └── views/              # SQL files for cross-source DuckDB views
│
├── schemas/                # Pydantic v2 schemas (contract validation)
│   ├── common.py           # BaseSchema, TimestampMixin, SettlementPeriodMixin
│   ├── elexon.py, entsoe.py, entsog.py, gie.py, neso.py, weather.py
│
├── storage/                # Storage utilities
│   ├── duckdb.py           # DuckDB connection, catalogue init, view registration
│   ├── parquet.py          # Atomic Parquet read/write (os.replace for Windows)
│   └── paths.py            # PathBuilder — centralised path logic
│
├── quality/                # Data quality framework
│   ├── checks.py           # 5 check functions (nulls, gaps, range, count, dupes)
│   └── reporter.py         # QualityReporter — writes results to DuckDB
│
├── serving/
│   └── client.py           # GridflowClient — read-only query interface
│
├── observability.py        # PipelineRunTracker, watermarks
└── utils/
    ├── time.py             # Settlement period <-> UTC, date ranges, lookback parsing
    ├── retry.py            # Tenacity retry policy + circuit breaker
    └── logging.py          # JSON file logging + console logging
```

---

## Code Flow: End to End

### Step 1 — Ingest (Bronze)

```
CLI: gridflow ingest elexon system_prices --last 24h
            │
            ▼
    ┌─ _resolve_dates() ──────── parse --start/--end/--last into UTC datetimes
    ├─ _resolve_datasets() ───── single dataset or --all from config
    ├─ _import_connectors() ──── import modules → triggers register_connector()
    │
    ▼
    get_connector("elexon", config)   ← looks up ElexonConnector from registry
            │
            ▼
    async with connector:             ← creates httpx.AsyncClient + rate-limit semaphore
        connector.fetch(dataset, start, end)
            │
            ▼
    Returns list[RawResponse]         ← raw bytes + metadata (URL, params, status, pages)
            │
            ▼
    BronzeWriter.write(response)      ← writes to data/bronze/{source}/{dataset}/YYYY/MM/DD/
                                         two files: raw data + .meta.json sidecar
```

### Step 2 — Transform (Silver)

```
CLI: gridflow transform elexon system_prices --last 24h
            │
            ▼
    _import_transformers()  ← triggers register_transformer() for all sources
            │
            ▼
    get_transformer("elexon", "system_prices", data_dir)
            │
            ▼
    For each date in range:
        transformer.run(target_date)
            │
            ├── read_bronze(date)     ← reads all raw JSON/XML files for that date
            │       ▼
            │   Raw Polars DataFrame
            │
            ├── transform(raw_df)     ← source-specific logic:
            │       │                    - rename columns to snake_case
            │       │                    - cast types (Date, Float64, Datetime)
            │       │                    - derive timestamps from settlement periods
            │       │                    - deduplicate (e.g. keep latest run type)
            │       │                    - add metadata columns
            │       ▼
            │   Clean Polars DataFrame
            │
            └── _write_silver(df)     ← atomic write to:
                                         data/silver/{source}/{dataset}/year=YYYY/month=MM/
                                         filename: {dataset}_YYYYMMDD.parquet
```

### Step 3 — Build (Gold)

```
CLI: gridflow build system_marginal_price --last 30d
            │
            ▼
    GoldBuilder reads from silver Parquet files
            │
            ▼
    Enriches with derived features:
        - price spreads, absolute imbalance
        - hour_of_day, day_of_week
        - cross-source joins
            │
            ▼
    Writes to data/gold/{dataset}/year=YYYY/{dataset}_YYYYMMDD.parquet
```

---

## Key Classes and Their Roles

### Connectors (Bronze Layer)

| Class | Role |
|-------|------|
| `BaseConnector` (ABC) | Defines the interface: `fetch(dataset, start, end)` returns `list[RawResponse]`. Manages httpx client lifecycle and rate limiting via semaphore. |
| `RawResponse` | Frozen dataclass holding raw bytes, content type, request metadata, pagination info. |
| `ElexonConnector` | Handles 3 different parameter styles and page-based pagination. |
| `EntsoeConnector` | Query-param auth, XML responses, per-zone fetching across EU bidding zones. |
| `EntsogConnector` | Fetches physical gas flows for 6 UK/EU interconnection points. |
| `GieConnector` / `AgsiConnector` / `AlsiConnector` | Header-based auth (`x-key`), page-based pagination per country. |
| `OpenMeteoConnector` | Fetches weather for 7 UK locations, two modes (historical/forecast). |
| `CarbonIntensityConnector` | Chunks requests to 14-day max windows, path-based date parameters. |
| `BronzeWriter` | Writes raw response + metadata sidecar. Filenames include timestamp + SHA256 hash. |

### Transformers (Silver Layer)

| Class | Role |
|-------|------|
| `BaseSilverTransformer` (ABC) | Template method pattern: `run()` calls `read_bronze()` → `transform()` → `_write_silver()`. |
| `SystemPriceTransformer` | Deduplicates by run type precedence (II→SF→R1→R2→R3→RF→DF), converts settlement periods to UTC. |
| `DayAheadPricesTransformer` | Parses ENTSO-E XML TimeSeries, renames fields, deduplicates on (timestamp, area_code). |
| Other transformers | Each normalises its source-specific format into a typed, deduplicated Polars DataFrame. |

### Gold Builders

| Class | Role |
|-------|------|
| `BaseGoldBuilder` (ABC) | Reads from silver, enriches, writes partitioned gold Parquet. |
| `SystemMarginalPriceBuilder` | Joins system prices with imbalance data, adds temporal features. |

### Infrastructure

| Class | Role |
|-------|------|
| `GridflowConfig` | Pydantic v2 settings: loads from `config/settings.yaml` + `config/sources.yaml` + env vars. |
| `PathBuilder` | Centralised path construction for bronze/silver/gold directories. |
| `PipelineRunTracker` | Records each run (UUID, source, dataset, operation, status, rows, duration) to DuckDB. |
| `QualityReporter` | Runs 5 check types against silver data, writes results to DuckDB. |
| `GridflowClient` | Read-only DuckDB query interface with helper methods for common queries. |

---

## Auto-Registration Pattern

Connectors and transformers register themselves when their module is imported:

```python
# In connectors/elexon/client.py (at module level):
register_connector("elexon", ElexonConnector)

# In silver/elexon/system_prices.py (at module level):
register_transformer("elexon", "system_prices", SystemPriceTransformer)
```

The CLI calls `_import_connectors()` / `_import_transformers()` which imports each source module, triggering registration. This means **adding a new source only requires writing the connector/transformer and importing it** — no central file needs editing.

---

## Storage Layout on Disk

```
data/
├── bronze/
│   └── elexon/system_prices/2024/01/15/
│       ├── raw_20240115T120000Z_a1b2c3.json       ← raw API response
│       └── raw_20240115T120000Z_a1b2c3.meta.json   ← request metadata
│
├── silver/
│   └── elexon/system_prices/year=2024/month=01/
│       └── system_prices_20240115.parquet           ← clean, typed, deduplicated
│
├── gold/
│   └── system_marginal_price/year=2024/
│       └── system_marginal_price_20240115.parquet   ← enriched features
│
└── gridflow.duckdb                                  ← catalogue + run history
```

---

## Configuration

Two YAML files in `config/`:

- **`settings.yaml`** — Pipeline paths, lookback defaults, quality thresholds
- **`sources.yaml`** — All 7 sources with base URLs, auth, rate limits, and per-dataset endpoint configs

The default data root is repo-local `data/`, with the DuckDB catalogue at
`data/gridflow.duckdb`. Override it with `GRIDFLOW_DATA_DIR` and
`GRIDFLOW_DUCKDB_PATH` in the process environment or an untracked repo-root
`.env`; absolute paths pass through unchanged. Precedence is process
environment, then repo-root `.env`, then `config/settings.yaml`, then built-in
defaults.

API keys are resolved from environment variables (e.g. `ENTSOE_API_KEY`, `GIE_API_KEY`). Sources without auth (Elexon, Open-Meteo, ENTSO-G, NESO) work out of the box.

---

## DuckDB Catalogue

The DuckDB file (`<data-root>/gridflow.duckdb` by default) stores:

| Table/View | Purpose |
|------------|---------|
| `pipeline_runs` | Run history: source, dataset, operation, status, row counts, duration |
| `pipeline_watermarks` | Last successful end timestamp per source/dataset (for incremental runs) |
| `quality_reports` | Data quality check results |
| `silver_{source}_{dataset}` (views) | Auto-registered, source-qualified views pointing at silver Parquet files (a few legacy unqualified aliases remain as deprecation shims) |
| `gold_{dataset}` (views) | Auto-registered views pointing at gold Parquet files |

Views use `read_parquet('path/**/*.parquet', hive_partitioning=true)` so you can query all historical data with plain SQL.
After relocating an existing data root, run the view refresh because Parquet
view globs store absolute paths.

---

## Key Dependencies

| Library | Used For |
|---------|----------|
| `httpx` | Async HTTP client for all API calls |
| `polars` | DataFrame operations in transformers and gold builders |
| `pydantic v2` | Configuration models and schema validation |
| `duckdb` | SQL catalogue, run tracking, quality reports |
| `typer` | CLI framework |
| `tenacity` | Retry logic with exponential backoff |
| `lxml` | XML parsing for ENTSO-E responses |
