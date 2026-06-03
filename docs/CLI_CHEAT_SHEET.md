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
| Elexon | `elexon` | `system_prices`, `boal`, `disbsad`, `freq`, `fuelhh`, `fuelinst`, `imbalngc`, `mid`, `netbsad`, `ndf`, `ndfd`, `pn`, `melngc`, `fou2t14d`, `uou2t14d`, `windfor`, `temp`, `agpt`, `agws`, `atl`, `indo`, `itsdo`, `indod`, `nonbm`, `inddem`, `indgen`, `tsdf`, `tsdfd`, `lolpdrm`, `remit`, `soso`, `market_depth`, `bmunits_reference` |
| ENTSO-E | `entsoe` | `day_ahead_prices`, `actual_load`, `load_forecast`, `actual_generation`, `wind_solar_forecast`, `cross_border_flows`, `outages_generation`, `outages_consumption`, `outages_transmission`, `outages_offshore_grid`, `outages_production`, `installed_capacity`, `installed_capacity_units`, `generation_forecast`, `actual_generation_units`, `water_reservoirs`, `generation_units_master_data`, `load_forecast_weekly`, `load_forecast_monthly`, `load_forecast_yearly`, `forecast_margin`, `net_transfer_capacity`, `dc_link_intraday_transfer_limits`, `commercial_schedules`, `redispatching_cross_border`, `redispatching_internal`, `countertrading`, `congestion_management_costs`, `offered_transfer_capacity_continuous`, `offered_transfer_capacity_implicit`, `offered_transfer_capacity_explicit`, `auction_revenue`, `transfer_capacity_use`, `total_nominated_capacity`, `total_capacity_allocated`, `congestion_income`, `net_positions`, `imbalance_prices`, `imbalance_volume`, `activated_balancing_prices`, `contracted_reserves`, `current_balancing_state`, `balancing_energy_bids`, `aggregated_balancing_energy_bids`, `procured_balancing_capacity`, `cross_zonal_balancing_capacity`, `balancing_financial_expenses_income` |
| ENTSO-G | `entsog` | `physical_flows`, `nominations`, `allocations`, `renominations`, `firm_available`, `firm_booked`, `firm_technical`, `interruptible_available`, `interruptible_booked`, `interruptible_total`, `gcv`, `wobbe_index`, `methane_content`, `hydrogen_content`, `oxygen_content`, `available_through_oversubscription`, `available_through_surrender`, `available_through_uioli_long_term`, `available_through_uioli_short_term`, `cmp_unsuccessful_requests`, `cmp_unavailable_firm_capacity`, `cmp_auction_premiums`, `interruptions`, `aggregated_physical_flows`, `tariffs`, `tariff_simulations`, `urgent_market_messages`, `connection_points`, `operators`, `balancing_zones`, `operator_point_directions`, `interconnections`, `aggregate_interconnections` |
| GIE AGSI | `gie_agsi` | `storage`, `storage_reports`, `about_summary`, `about_listing`, `news`, `news_item`, `unavailability` |
| GIE ALSI | `gie_alsi` | `lng` |
| Open-Meteo | `open_meteo` | `historical_demand`, `forecast_demand`, `historical_wind`, `forecast_wind`, `historical_solar`, `forecast_solar` |
| NESO | `neso` | `intensity_current`, `intensity_today`, `intensity_date`, `intensity_period`, `intensity_factors`, `intensity_at`, `intensity_fw24h`, `intensity_fw48h`, `intensity_pt24h`, `carbon_intensity`, `intensity_stats`, `intensity_stats_block`, `generation_current`, `generation_pt24h`, `generation`, `regional_current`, `regional_england`, `regional_scotland`, `regional_wales`, `regional_postcode`, `regional_regionid`, `regional_intensity_fw24h`, `regional_intensity_fw24h_postcode`, `regional_intensity_fw24h_regionid`, `regional_intensity_fw48h`, `regional_intensity_fw48h_postcode`, `regional_intensity_fw48h_regionid`, `regional_intensity_pt24h`, `regional_intensity_pt24h_postcode`, `regional_intensity_pt24h_regionid`, `regional_intensity`, `regional_intensity_postcode`, `regional_intensity_regionid` |
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
gridflow pipeline open_meteo --all --last 24h
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
