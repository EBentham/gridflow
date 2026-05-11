# Bronze/Silver Partition Mismatch Audit

**Date:** 2026-05-11
**Scope:** All connectors and silver transformers in `gridflow`

---

## The Bug

Two variants of the same root cause — bronze files land in the wrong date partition, so the silver transformer can't find them.

### Variant A — Multi-day window, all files land on window-start date

The connector issues one API call covering a multi-day window and sets `data_date = period_start.date()` on **every** response. The bronze writer partitions all files under that single date. The silver transformer iterates `date_range(start, end)` and looks for a directory per day — days 2..N have no directory → warning → `rows_out: 0`.

**Key locations:**
- `src/gridflow/connectors/entsoe/client.py` — `_fetch_document()`: `data_date = datetime.strptime(period_start, ...).date()`
- `src/gridflow/connectors/entsog/client.py` — `data_date=start.date() if endpoint.requires_dates else None`
- `src/gridflow/connectors/neso/carbon_intensity.py` — `data_date=window_start.date()` for non-reference, non-daily-iteration endpoints
- `src/gridflow/bronze/writer.py` lines 34–37 — partition path derived from `data_date`
- `src/gridflow/silver/base.py` — `_bronze_date_dirs(target_date)`: exact single-day lookup

### Variant B — `data_date=None`, all files land in today's partition

The connector omits `data_date` entirely. The bronze writer falls back to `fetched_at.date()` (today). Historical silver runs look for files in the target date's partition and find nothing.

**Key locations:**
- `src/gridflow/connectors/openmeteo/client.py` — `_fetch_location()`: no `data_date` argument
- `src/gridflow/connectors/gie/client.py` — `_fetch_country()`: no `data_date` argument

---

## Dataset Status

### ENTSO-E

All document types use `_fetch_document()` in `src/gridflow/connectors/entsoe/client.py`, which sets `data_date = period_start.date()` uniformly. Every silver transformer under `src/gridflow/silver/entsoe/` uses the base class single-day lookup.

| Dataset | Status |
|---|---|
| `actual_generation_per_type` | **AFFECTED (A)** |
| `actual_generation_per_unit` | **AFFECTED (A)** |
| `actual_total_load` | **AFFECTED (A)** |
| `day_ahead_forecast_load` | **AFFECTED (A)** |
| `week_ahead_forecast_load` | **AFFECTED (A)** |
| `month_ahead_forecast_load` | **AFFECTED (A)** |
| `year_ahead_forecast_load` | **AFFECTED (A)** |
| `day_ahead_prices` | **AFFECTED (A)** |
| `cross_border_flows` | **AFFECTED (A)** |
| `scheduled_commercial_exchanges` | **AFFECTED (A)** |
| `physical_flows` | **AFFECTED (A)** |
| `actual_net_generation_per_type` | **AFFECTED (A)** |
| `wind_solar_forecast` | **AFFECTED (A)** |
| `offshore_wind_actual` | **AFFECTED (A)** |
| `water_reservoirs` | **AFFECTED (A)** — confirmed original case |
| `installed_capacity_per_type` | **AFFECTED (A)** |
| `installed_capacity_per_unit` | **AFFECTED (A)** |
| `unavailability_of_generation_units` | **AFFECTED (A)** |
| `unavailability_of_production_units` | **AFFECTED (A)** |
| `unavailability_of_transmission` | **AFFECTED (A)** |
| `unavailability_of_offshore` | **AFFECTED (A)** |
| All remaining doc types | **AFFECTED (A)** |

---

### Elexon

All `SETTLEMENT_DATE`, `DATE_PATH`, and `SETTLEMENT_DATE_PERIOD` datasets issue one call per day with `data_date = settlement_date`. Safe.

`PUBLISH_DATETIME` datasets use 24-hour or 4-hour chunks that never span midnight — safe under normal CLI use. Would become Variant A if called with multi-day chunks.

| Dataset group | Status |
|---|---|
| All `SETTLEMENT_DATE` datasets (`B1610`, `B1630`, `B0610`, etc.) | **SAFE** |
| All `DATE_PATH` datasets (`PHYBMDATA`, etc.) | **SAFE** |
| All `SETTLEMENT_DATE_PERIOD` datasets | **SAFE** |
| `PUBLISH_DATETIME` datasets (≤24h chunks, normal use) | **SAFE** |
| `bmunits_reference` | **SAFE** — silver uses `rglob` across all partitions |

---

### ENTSO-G

Date-windowed endpoints set `data_date = start.date()` for the whole window. Reference datasets (no dates) use `data_date = None` but their silver transformers use `rglob` — safe.

| Dataset | Status |
|---|---|
| `physical_flows` | **AFFECTED (A)** |
| `nominations` | **AFFECTED (A)** |
| `renominations` | **AFFECTED (A)** |
| `allocations` | **AFFECTED (A)** |
| `aggregated_lng_sendout` | **AFFECTED (A)** |
| `cmp_auction_premiums` | **AFFECTED (A)** |
| `cmp_unavailable_firm_capacity` | **AFFECTED (A)** |
| `cmp_unsuccesful_requests` | **AFFECTED (A)** |
| `interruptions` | **AFFECTED (A)** |
| `tariffs_reference` (date-windowed) | **AFFECTED (A)** |
| `tariff_simulations` | **AFFECTED (A)** |
| `aggregated_data` | **AFFECTED (A)** |
| `urgent_market_messages` | **SAFE** — reference, `rglob` |
| `connection_points` | **SAFE** — reference, `rglob` |
| `operators` | **SAFE** — reference, `rglob` |
| `balancing_zones` | **SAFE** — reference, `rglob` |
| `operator_point_directions` | **SAFE** — reference, `rglob` |
| `interconnections` | **SAFE** — reference, `rglob` |
| `aggregate_interconnections` | **SAFE** — reference, `rglob` |

---

### GIE (AGSI + ALSI)

| Dataset | Status | Notes |
|---|---|---|
| `agsi_storage` (per-country) | **SAFE** | Per-gas-day calls via `build_storage_query_plan(date_mode="exact")` |
| `agsi_*` non-storage endpoints | **AFFECTED (A)** | `data_date = start.date()` for windowed calls |
| `alsi_*` (legacy `_fetch_country()`) | **AFFECTED (B)** | No `data_date` → lands in today's partition |
| GIE unavailabilities | **MITIGATED** | Silver `UnavailabilityTransformer` has a window-scan fallback |

---

### NESO (Carbon Intensity API)

Endpoints with `daily_iteration=True` issue one call per day — safe. All others use a multi-day chunk with `data_date = window_start.date()`.

| Dataset | Status |
|---|---|
| `intensity_date` | **SAFE** — `daily_iteration=True` |
| `intensity_period` | **SAFE** — `daily_iteration=True` |
| `intensity_factors` | **SAFE** — reference, `rglob` |
| `carbon_intensity` | **AFFECTED (A)** |
| `intensity_at` | **AFFECTED (A)** |
| `intensity_fw24h` | **AFFECTED (A)** |
| `intensity_fw48h` | **AFFECTED (A)** |
| `intensity_pt24h` | **AFFECTED (A)** |
| `intensity_stats` | **AFFECTED (A)** |
| `intensity_stats_block` | **AFFECTED (A)** |
| `generation_pt24h` | **AFFECTED (A)** |
| `generation` | **AFFECTED (A)** |
| `regional_intensity_*` variants | **AFFECTED (A)** |
| `intensity_current`, `intensity_today` | **AFFECTED (A)** |
| `generation_current` | **AFFECTED (A)** |
| `regional_current/england/scotland/wales` | **AFFECTED (A)** |
| `regional_postcode`, `regional_regionid` | **AFFECTED (A)** |

---

### Open-Meteo

All 6 datasets are Variant B. `_fetch_location()` in `src/gridflow/connectors/openmeteo/client.py` omits `data_date`, so every file lands in `fetched_at.date()`. The silver transformer (`src/gridflow/silver/openmeteo/historical.py`) does an exact single-day partition lookup — it will never find historical data.

| Dataset | Status |
|---|---|
| `historical_demand` | **AFFECTED (B)** |
| `historical_wind` | **AFFECTED (B)** |
| `historical_solar` | **AFFECTED (B)** |
| `forecast_demand` | **AFFECTED (B)** |
| `forecast_wind` | **AFFECTED (B)** |
| `forecast_solar` | **AFFECTED (B)** |

---

## Summary

| Status | Count | Sources |
|---|---|---|
| **AFFECTED (A)** — multi-day window, wrong partition | ~65 datasets | All ENTSO-E, 12 ENTSO-G, 25 NESO non-daily, GIE AGSI non-storage |
| **AFFECTED (B)** — `data_date=None`, lands in today | 7 datasets | All 6 Open-Meteo, GIE ALSI `_fetch_country()` |
| **SAFE** | ~40 datasets | All Elexon, ENTSO-G reference, NESO daily/reference, GIE `agsi_storage` |
| **MITIGATED** | 1 dataset | GIE unavailabilities (silver has window-scan fallback) |

---

## Fix Strategy (not yet implemented)

### Variant B — simplest fix

Pass `data_date` explicitly in connectors that currently omit it:
- **Open-Meteo:** `_fetch_location()` already receives `start_date`/`end_date` — set `data_date=start_date`
- **GIE `_fetch_country()`:** pass `data_date=start.date()`

### Variant A — two options

**Option 1 — Fix at the connector** (precise but labour-intensive):
Set `data_date` to the date the *record pertains to*, not the window start. For XML-based APIs (ENTSO-E, ENTSO-G) this requires parsing the response to extract per-record dates before constructing `RawResponse`. For JSON APIs (NESO, GIE) it is simpler.

**Option 2 — Fix at the silver base** (single change point, covers all affected transformers):
Add a fallback in `_bronze_date_dirs(target_date)`: when the exact-day partition is empty, scan the same dataset's bronze directory for the nearest prior partition and check whether its sidecar metadata window covers `target_date`. Return that partition's files if it does.

This is the lower-risk approach — no connector changes, no schema changes, backward-compatible with all existing bronze data on disk.

> Note: Option 2 requires care around ambiguity if two overlapping fetches exist. The sidecar `period_start`/`period_end` metadata fields should be sufficient to resolve this unambiguously.
