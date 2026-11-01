# gridflow | Also see https://ebentham.github.io/gridflow-front-end and https://github.com/EBentham/gridflow-explorer

**Local-first Python data pipeline for UK/EU power and gas market data.**

Pulls electricity prices, generation, demand, gas storage, weather, and carbon
intensity from 7 public APIs into a clean, queryable analytical store backed by
DuckDB and Parquet. No cloud, no database server — just files on your disk and
SQL.

[![CI](https://github.com/EBentham/gridflow/actions/workflows/ci.yml/badge.svg?branch=master)](https://github.com/EBentham/gridflow/actions/workflows/ci.yml)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

[Changelog](CHANGELOG.md) · [Releases](https://github.com/EBentham/gridflow/releases)

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

Each vendor's terms, the attribution it asks for and which test fixtures hold real
responses are in [DATA-SOURCES.md](DATA-SOURCES.md).

---

## Quickstart

Needs [`uv`](https://docs.astral.sh/uv/getting-started/installation/), which
fetches Python 3.12 if you don't have it (gridflow supports 3.11 and later).

```bash
git clone https://github.com/EBentham/gridflow.git && cd gridflow
uv sync --group notebooks                    # gridflow, its CLI, JupyterLab and plotly
cp .env.example .env                         # optional: API keys, GRIDFLOW_DATA_DIR
uv run gridflow init                         # creates ~/gridflow-data and its catalogue
uv run gridflow backfill --preset tour       # last 7 days of what the notebooks read
uv run --group notebooks python notebooks/01_sources.py   # a notebook, run as a script
uv run --group notebooks jupyter lab notebooks/
```

Without keys, the tour skips ENTSO-E and GIE with one line each that names the
variable to set, and fetches everything else. [docs/QUICKSTART.md](docs/QUICKSTART.md)
explains each step: the data root, keys, presets and their measured cost, and
the notebooks. [docs/CLI_CHEAT_SHEET.md](docs/CLI_CHEAT_SHEET.md) lists every
command.

### What needs a key

Both keys are free. Put them in `.env`, in or above the folder you run gridflow
from, or in your shell environment. Reading data you already stored needs no key.

| Variable | Register | Unlocks |
|---|---|---|
| `ENTSOE_API_KEY` | an account on the [Transparency Platform](https://transparency.entsoe.eu/), then a [token](https://transparencyplatform.zendesk.com/hc/en-us/articles/12845911031188-How-to-get-security-token) | `entsoe`: EU day-ahead prices, load, generation and flows (the ENTSO-E section of `01_sources`) |
| `GIE_API_KEY` | API, on [agsi.gie.eu](https://agsi.gie.eu/account); one key covers ALSI too | `gie_agsi` gas storage and `gie_alsi` LNG terminals (the GIE section of `01_sources`) |

Elexon, NESO, ENTSO-G and Open-Meteo need no key.

### Windows

The commands run as written in PowerShell 7. Windows PowerShell 5.1 has no `&&`,
so clone with `git clone https://github.com/EBentham/gridflow.git; cd gridflow`.
To set a variable for one session, bash's `export GRIDFLOW_DATA_DIR=/srv/gridflow-data`
is `$env:GRIDFLOW_DATA_DIR = "D:\gridflow-data"` in PowerShell.

---

## On-disk layout

```
~/gridflow-data/
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
the import in `runner.import_connectors()`. See
[CONTRIBUTING.md](CONTRIBUTING.md) for the pattern.

---

## Development

```bash
uv sync --extra dev                              # pytest, ruff and mypy
uv run pytest -x -q -m "not live and not slow"   # the fast run, as CI runs it
uv run ruff check src/ tests/ scripts/
uv run ruff format src/ tests/ scripts/
uv run mypy src/gridflow/
```

Tests that call the real APIs run only when selected with `-m live`. The `slow`
memory gates read a real data root and skip unless `GRIDFLOW_REAL_DATA_DIR`
names one.

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
- [src/gridflow/silver/schema_manifest.py](src/gridflow/silver/schema_manifest.py) — silver-layer schema contracts
- [docs/DECISION_LOG/](docs/DECISION_LOG/) — ADRs for non-obvious design choices
- [CONTRIBUTING.md](CONTRIBUTING.md) — connector / transformer pattern reference

---

## License

Apache 2.0. See [LICENSE](LICENSE).
