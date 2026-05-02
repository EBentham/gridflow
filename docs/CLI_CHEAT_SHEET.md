# Gridflow CLI Cheat Sheet

## ⚡ One-Command Bronze + Silver (Most Used)

`gridflow pipeline` fetches raw data **and** transforms it to silver in a single command.

```bash
# Single dataset — ingest + transform
gridflow pipeline elexon system_prices --last 24h

# All datasets for a source — ingest + transform
gridflow pipeline elexon --all --last 24h

# With gold build on top
gridflow pipeline elexon system_prices --last 24h --gold system_marginal_price

# Specific date range
gridflow pipeline elexon system_prices --start 2024-06-01 --end 2024-06-07
```

---

## 🗑️ Reset / Wipe Data

Permanently deletes bronze, silver, gold data and/or the DuckDB catalogue.

```bash
# Wipe EVERYTHING (all sources, all layers) — requires confirmation
gridflow reset

# Skip confirmation prompt
gridflow reset --yes

# Wipe all data for one source
gridflow reset elexon --yes

# Wipe one specific dataset (all layers)
gridflow reset elexon system_prices --yes

# Wipe only the silver layer for a dataset
gridflow reset elexon system_prices --silver --yes

# Wipe only bronze + DuckDB (keep silver/gold intact)
gridflow reset elexon --bronze --duckdb --yes
```

> Layer flags: `--bronze`, `--silver`, `--gold`, `--duckdb`. Omit all to wipe every layer.

---

## Run Pipeline Stages Separately

```bash
# Bronze only
gridflow ingest elexon system_prices --last 24h
gridflow ingest elexon --all --last 7d

# Silver only (re-transform existing bronze)
gridflow transform elexon system_prices --last 24h
gridflow transform elexon --all --last 7d
```

---

## Backfill Historical Data

Ingest + transform in date-chunked batches (avoids API rate limits).

```bash
# Single dataset, 7-day chunks (default)
gridflow backfill elexon system_prices --start 2024-01-01 --end 2024-12-31

# Custom chunk size (3 days)
gridflow backfill elexon system_prices --start 2024-01-01 --end 2024-06-30 --chunk-days 3

# ALL datasets for a source
gridflow backfill elexon --all --start 2024-01-01 --end 2024-12-31

# ENTSO-E full year backfill
gridflow backfill entsoe --all --start 2024-01-01 --end 2024-12-31
```

---

## Export Silver Data to CSV

Export existing silver Parquet files to CSV (no re-ingestion needed).

```bash
# Single dataset
gridflow export-csv elexon system_prices

# All datasets for a source
gridflow export-csv elexon --all

# Custom output directory
gridflow export-csv elexon --all --output ./my_exports
```

Output goes to `data/exports/{source}/{dataset}.csv` by default.

> **Note:** Silver transforms also auto-write per-date CSVs to `data/silver/{source}/{dataset}/` during `transform`.

---

## All Commands

### `gridflow ingest` — Pull Raw Data (Bronze)

```bash
# Single dataset, last 24 hours (default)
gridflow ingest elexon system_prices

# Specific date range
gridflow ingest elexon system_prices --start 2024-06-01 --end 2024-06-07

# Relative lookback
gridflow ingest elexon system_prices --last 7d

# ALL datasets for a source
gridflow ingest elexon --all --last 24h
```

### `gridflow transform` — Clean Data (Silver)

```bash
# Single dataset
gridflow transform elexon system_prices --last 24h

# All datasets for a source
gridflow transform elexon --all --last 7d

# Specific date range
gridflow transform entsoe day_ahead_prices --start 2024-06-01 --end 2024-06-07
```

### `gridflow build` — Build Analytics Datasets (Gold)

```bash
# Build a specific gold dataset
gridflow build system_marginal_price --last 30d

# Build all gold datasets
gridflow build --all --last 30d
```

### `gridflow pipeline` — Full Pipeline (Bronze -> Silver -> Gold)

```bash
gridflow pipeline elexon system_prices --last 24h --gold system_marginal_price
gridflow pipeline elexon --all --last 7d
```

### `gridflow backfill` — Historical Backfill in Chunks

```bash
gridflow backfill elexon system_prices --start 2024-01-01 --end 2024-12-31
gridflow backfill elexon --all --start 2024-01-01 --end 2024-12-31 --chunk-days 3
```

### `gridflow export-csv` — Export Silver to CSV

```bash
gridflow export-csv elexon system_prices
gridflow export-csv elexon --all --output ./exports
```

### `gridflow status` — View Run History

```bash
gridflow status
```

### `gridflow quality` — Run Data Quality Checks

```bash
gridflow quality --all
gridflow quality --source elexon
```

### `gridflow reset` — Wipe Data

```bash
gridflow reset --yes                              # everything
gridflow reset elexon --yes                       # all elexon data
gridflow reset elexon system_prices --yes         # one dataset
gridflow reset elexon system_prices --silver --yes  # silver only
```

### `gridflow init` — Initialise Catalogue

```bash
gridflow init
```

---

## Date Options (Apply to ingest, transform, build, pipeline)

| Option | Example | Description |
|--------|---------|-------------|
| `--last` | `--last 24h`, `--last 7d`, `--last 30d` | Relative lookback from now |
| `--start` | `--start 2024-06-01` | Start date (YYYY-MM-DD) |
| `--end` | `--end 2024-06-07` | End date (YYYY-MM-DD), defaults to now if omitted |
| *(none)* | | Uses default lookback (24 hours) |

---

## Quick Reference: All Sources and Datasets

| Source | CLI Name | Datasets |
|--------|----------|----------|
| Elexon | `elexon` | `system_prices`, `fuelhh`, `fuelinst`, `boal`, `mid`, `freq`, `ndf`, `ndfd`, `pn`, `disbsad`, `netbsad`, `imbalngc`, `melngc`, `windfor`, `temp`, `fou2t14d`, `uou2t14d`, `generation_by_fuel`, `bmunits_reference` |
| ENTSO-E | `entsoe` | `day_ahead_prices`, `actual_load`, `load_forecast`, `actual_generation`, `wind_solar_forecast`, `cross_border_flows`, `outages_generation`, `installed_capacity` |
| ENTSO-G | `entsog` | `physical_flows` |
| GIE AGSI | `gie_agsi` | `storage` |
| GIE ALSI | `gie_alsi` | `lng` |
| Open-Meteo | `open_meteo` | `historical`, `forecast` |
| NESO | `neso` | `carbon_intensity` |
| **Gold** | (see `build`) | `system_marginal_price` |

### Environment Variables (only needed for authenticated sources)

```bash
export ENTSOE_API_KEY="your-key-here"   # ENTSO-E
export GIE_API_KEY="your-key-here"      # GIE (AGSI + ALSI)
```

Public (no key needed): `elexon`, `open_meteo`, `entsog`, `neso`.

---

## Common Workflows

### Daily Refresh (All Sources)

```bash
gridflow pipeline elexon --all --last 24h --gold system_marginal_price
gridflow pipeline entsoe --all --last 24h
gridflow pipeline entsog physical_flows --last 24h
gridflow pipeline gie_agsi storage --last 24h
gridflow pipeline gie_alsi lng --last 24h
gridflow pipeline open_meteo forecast --last 24h
gridflow pipeline neso carbon_intensity --last 24h
```

### Backfill a Full Year

```bash
gridflow backfill elexon --all --start 2024-01-01 --end 2024-12-31
gridflow backfill entsoe --all --start 2024-01-01 --end 2024-12-31
```

### Export All Silver Data to CSV

```bash
gridflow export-csv elexon --all
gridflow export-csv entsoe --all
gridflow export-csv entsog --all
```

---

## Running from Python (IDE Debugging)

### Option 1: `python -m gridflow`

```bash
python -m gridflow ingest elexon system_prices --last 24h
python -m gridflow pipeline elexon --all --last 7d --gold system_marginal_price
python -m gridflow status
```

### Option 2: IDE Debug Script — `scripts/run_pipeline.py`

```bash
python scripts/run_pipeline.py --step bronze --source elexon --dataset system_prices --last 24h
python scripts/run_pipeline.py --step all --source elexon --dataset system_prices --last 24h
```

### Option 3: IDE Debug Script — `scripts/run_all_sources.py`

```bash
python scripts/run_all_sources.py
python scripts/run_all_sources.py --date 2024-06-15
python scripts/run_all_sources.py --public-only
```

---

## File Locations

| What | Path |
|------|------|
| CLI entrypoint | `src/gridflow/cli.py` |
| Config files | `config/settings.yaml`, `config/sources.yaml` |
| Raw data (bronze) | `data/bronze/{source}/{dataset}/YYYY/MM/DD/` |
| Clean data (silver) | `data/silver/{source}/{dataset}/year=YYYY/month=MM/` |
| Analytics data (gold) | `data/gold/{dataset}/year=YYYY/` |
| CSV exports | `data/exports/{source}/{dataset}.csv` |
| DuckDB catalogue | `data/gridflow.duckdb` |
| Logs | `logs/` |
| IDE debug scripts | `scripts/run_pipeline.py`, `scripts/run_all_sources.py` |
