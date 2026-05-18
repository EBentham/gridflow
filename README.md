# gridflow | Also see https://ebentham.github.io/gridflow-front-end

**Local-first Python data pipeline for UK/EU power and gas market data.**

Pulls electricity prices, generation, demand, gas storage, weather, and carbon
intensity from 7 public APIs into a clean, queryable analytical store backed by
DuckDB and Parquet. No cloud, no database server — just files on your disk and
SQL.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

---

## What it does

Gridflow uses a **medallion architecture** to take raw API responses and turn
them into modelling-ready tables:

```
┌───────────────┐     ┌───────────────┐     ┌───────────────┐
│    BRONZE     │     │    SILVER     │     │     GOLD      │
│  (raw bytes)  │ --> │ (validated)   │ --> │ (analytics)   │
│  JSON / XML   │     │   Parquet     │     │   Parquet     │
└───────────────┘     └───────────────┘     └───────────────┘
        ▲                                            │
        │                                            ▼
   API sources                                DuckDB catalogue
   (7 vendors)                                (SQL views)
```

| Layer  | Contents                                          | Format                     | CLI verb     |
|--------|---------------------------------------------------|----------------------------|--------------|
| Bronze | Raw API responses, immutable, partitioned by date | JSON / XML / CSV + sidecar | `ingest`     |
| Silver | Typed, deduplicated, UTC-normalised tables        | Hive-partitioned Parquet   | `transform`  |
| Gold   | Cross-source feature datasets, modelling-ready    | Hive-partitioned Parquet   | `build`      |

---

## Data sources

| Source       | CLI name     | Auth                 | Coverage                                          |
|--------------|--------------|----------------------|---------------------------------------------------|
| Elexon BMRS  | `elexon`     | none                 | UK electricity: prices, generation, demand, BM    |
| ENTSO-E      | `entsoe`     | `ENTSOE_API_KEY`     | EU electricity: prices, load, flows, outages      |
| ENTSO-G      | `entsog`     | none                 | EU gas: physical flows, nominations, capacities   |
| GIE AGSI     | `gie_agsi`   | `GIE_API_KEY`        | EU gas storage levels                             |
| GIE ALSI     | `gie_alsi`   | `GIE_API_KEY`        | EU LNG terminal data                              |
| Open-Meteo   | `open_meteo` | none                 | Weather: temperature, wind, solar irradiance      |
| NESO         | `neso`       | none                 | UK carbon intensity (current + regional + fcst)   |

---

## Install

Requires Python 3.11+. Uses [`uv`](https://docs.astral.sh/uv/) for environment
management.

```bash
git clone https://github.com/EBentham/gridflow.git
cd gridflow
uv pip install -e ".[dev]"
uv run gridflow init
```

`init` scaffolds the local `data/` directory and DuckDB catalogue.

### API keys (optional)

Only required for ENTSO-E and GIE. Put them in a `.env` file at the repo root:

```env
ENTSOE_API_KEY=your-token-here
GIE_API_KEY=your-token-here
```

Elexon, Open-Meteo, ENTSO-G, and NESO work out of the box with no key.

---

## Usage

### One-shot pipeline (most common)

Fetch + transform in a single command:

```bash
# Single dataset, last 24 hours
uv run gridflow pipeline elexon system_prices --last 24h

# All datasets for a source
uv run gridflow pipeline elexon --all --last 24h

# Specific date range
uv run gridflow pipeline entsoe day_ahead_prices --start 2024-06-01 --end 2024-06-07

# Pipeline + gold build
uv run gridflow pipeline elexon system_prices --last 7d --gold system_marginal_price
```

### Stages separately

```bash
uv run gridflow ingest elexon system_prices --last 24h        # bronze only
uv run gridflow transform elexon system_prices --last 24h     # silver only
uv run gridflow build system_marginal_price --last 30d        # gold only
```

### Backfill historical data

Chunks the request to respect API rate limits:

```bash
uv run gridflow backfill elexon --all --start 2024-01-01 --end 2024-12-31
uv run gridflow backfill entsoe day_ahead_prices --start 2023-01-01 --end 2024-12-31 --chunk-days 7
```

### Inspect

```bash
uv run gridflow status                  # pipeline run history + watermarks
uv run gridflow quality --all           # run data-quality checks
uv run gridflow export-csv elexon system_prices --last 24h
```

### Querying

The DuckDB catalogue exposes auto-registered views over the Parquet files. From
Python:

```python
from gridflow.serving.client import GridflowClient

client = GridflowClient()
df = client.query("SELECT * FROM silver_elexon_system_prices WHERE start_time > '2024-06-01' LIMIT 100")
```

Or directly from the DuckDB CLI:

```bash
duckdb data/gridflow.duckdb
> SELECT * FROM silver_elexon_system_prices LIMIT 5;
```

---

## On-disk layout

```
data/
├── bronze/
│   └── elexon/system_prices/2024/01/15/
│       ├── raw_20240115T120000Z_a1b2c3.json       # raw response bytes
│       └── raw_20240115T120000Z_a1b2c3.meta.json  # request metadata sidecar
├── silver/
│   └── elexon/system_prices/year=2024/month=01/
│       └── system_prices_20240115.parquet         # typed, deduplicated
├── gold/
│   └── system_marginal_price/year=2024/
│       └── system_marginal_price_20240115.parquet # cross-source features
└── gridflow.duckdb                                # catalogue + run history
```

Bronze is **immutable**. Re-running `ingest` writes new timestamped files
alongside the old ones — point-in-time queries against silver use a `run_type`
precedence to pick the right version.

---

## Project layout

```
src/gridflow/
├── cli.py            # Typer CLI entrypoint
├── config/           # YAML + env-var settings (pydantic v2)
├── connectors/       # Bronze: async API clients per source
├── bronze/           # BronzeWriter (raw bytes + sidecars)
├── silver/           # Per-source transformers (Polars)
├── gold/             # Cross-source feature builders
├── schemas/          # Pydantic v2 contract schemas
├── storage/          # DuckDB, Parquet, PathBuilder
├── quality/          # Data-quality checks + reporter
├── serving/          # Read-only query client
├── observability.py  # Run tracker + watermarks
└── utils/            # Time helpers, retry policy, logging
```

Connectors and transformers self-register on import — adding a new source means
adding files under `connectors/<source>/` and `silver/<source>/`, then wiring
the import in `cli._import_connectors()`. See
[CONTRIBUTING.md](CONTRIBUTING.md) for the pattern.

---

## Development

```bash
uv run pytest -x -q              # fast test run (default)
uv run pytest -m "not live"      # skip tests that hit real APIs
uv run ruff check src/ tests/
uv run ruff format src/ tests/
uv run mypy src/gridflow/
```

Tests cover unit, integration (via `respx` HTTP mocks), contract validation
against Pydantic schemas, and property-based checks.

---

## Design notes

- **Timezones:** All timestamps tz-aware UTC. The `TimestampMixin` rejects naive
  input at the silver boundary, not at query time.
- **Settlement periods:** Range is `1..50`, not `1..48`. DST days: 46 in spring,
  50 in autumn. Deduplication on `(date, period)` alone is a bug — include
  `run_type`.
- **Gas day:** Stored as `date`. Don't synthesise a UTC timestamp without
  applying the 06:00 UTC offset.
- **Validation:** Pydantic failures are logged and surfaced, never silently
  dropped.
- **Atomic writes:** `os.replace()` on all platforms (Windows-safe).
- **No pandas.** Polars only. `.to_pandas()` only at presentation boundaries.

---

## Documentation

- [docs/PROJECT_OVERVIEW.md](docs/PROJECT_OVERVIEW.md) — architecture deep-dive
- [docs/CLI_CHEAT_SHEET.md](docs/CLI_CHEAT_SHEET.md) — every CLI command
- [docs/ENDPOINT_REFERENCE.md](docs/ENDPOINT_REFERENCE.md) — API endpoint catalogue
- [docs/CANONICAL_SCHEMA.yaml](docs/CANONICAL_SCHEMA.yaml) — silver-layer schema contracts
- [docs/DECISION_LOG/](docs/DECISION_LOG/) — ADRs for non-obvious design choices
- [CONTRIBUTING.md](CONTRIBUTING.md) — connector / transformer pattern reference

---

## License

Apache 2.0. See [LICENSE](LICENSE).
