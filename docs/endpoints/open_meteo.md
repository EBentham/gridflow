# Open-Meteo — Dataset Reference

**Source:** `open_meteo`
**Base URLs:** `https://archive-api.open-meteo.com/v1` (historical) · `https://api.open-meteo.com/v1` (forecast)
**Authentication:** None (public API)
**Rate limit:** 5 req/sec (cap; via `rate_limit_per_second` in `config/sources.yaml`)
**Resolution:** Hourly
**DATASET_VERSION:** `2.0.0` (bumped from `1.0.0` at F7.5)

---

## Datasets

F7.5 split the connector into three role-specific dataset families
(demand, wind, solar) at three role-specific location lists. Each
family has an archive (ERA5 historical) and a forecast variant.

| Dataset             | Locations | Endpoint | Variable count | Notes |
|---------------------|-----------|----------|----------------|-------|
| `historical_demand` | 7 demand  | archive  | 9              | UK population centres + snow |
| `forecast_demand`   | 7 demand  | forecast | 9              | same vars as historical_demand |
| `historical_wind`   | 12 wind   | archive  | 13             | hub heights {10m, 100m} only — see "Archive 10m+100m limitation" below |
| `forecast_wind`     | 12 wind   | forecast | 19             | full hub-height set {10, 80, 100, 120, 180}m + directions |
| `historical_solar`  | 6 solar   | archive  | 12             | GHI + DNI + DHI + GTI; `tilt=35&azimuth=0` |
| `forecast_solar`    | 6 solar   | forecast | 12             | same as historical_solar |

Per-location bronze files live under
`data/bronze/open_meteo/{dataset}__{location}/YYYY/MM/DD/raw_*.json`
(double-underscore separator).

---

## Locations

### Demand (population centres) — 7 sites

| Location    | Latitude | Longitude |
|-------------|---------:|----------:|
| London      |  51.5074 |   -0.1278 |
| Birmingham  |  52.4862 |   -1.8904 |
| Manchester  |  53.4808 |   -2.2426 |
| Leeds       |  53.8008 |   -1.5491 |
| Glasgow     |  55.8642 |   -4.2518 |
| Cardiff     |  51.4816 |   -3.1791 |
| Belfast     |  54.5973 |   -5.9301 |

### Wind (capacity-weighted) — 12 sites

| Location              | Lat     | Lon    | Region                       |
|-----------------------|--------:|-------:|------------------------------|
| Dogger Bank           |   54.95 |   1.95 | Offshore S North Sea         |
| Hornsea               |   53.88 |   1.79 | Offshore S North Sea         |
| East Anglia           |   52.50 |   2.50 | Offshore S North Sea         |
| Triton Knoll          |   53.45 |   0.42 | Offshore S North Sea         |
| Walney                |   54.04 |  -3.52 | Offshore Irish Sea           |
| Gwynt y Môr           |   53.46 |  -3.59 | Offshore Irish Sea           |
| Beatrice              |   58.26 |  -2.89 | Offshore Moray Firth         |
| Seagreen              |   56.59 |  -1.93 | Offshore Forth               |
| Highland Central      |   57.20 |  -4.40 | Onshore Scotland             |
| Borders Crystal Rig   |   55.85 |  -2.50 | Onshore Scotland             |
| Whitelee              |   55.69 |  -4.27 | Onshore Scotland             |
| Pen y Cymoedd         |   51.69 |  -3.61 | Onshore Wales                |

Coordinates are approximate site centroids — see
`docs/DECISION_LOG/ADR-020-openmeteo-location-approximation.md` for the
rationale.

### Solar (capacity-weighted) — 6 sites

| Location               | Lat     | Lon    |
|------------------------|--------:|-------:|
| East Anglia (Norfolk)  |   52.62 |   1.05 |
| Wiltshire/Somerset     |   51.20 |  -2.50 |
| Kent                   |   51.20 |   0.70 |
| Cornwall               |   50.30 |  -5.00 |
| Sussex                 |   50.95 |  -0.10 |
| Oxfordshire            |   51.75 |  -1.25 |

All solar sites are below 53° N (the Bristol-Norwich line). GB installed
solar capacity is heavily south-east biased; Glasgow at 55.9° N receives
~60% of the annual irradiance of Cornwall at 50.3° N.

---

## Archive 10m+100m limitation (verified 2026-05-09)

The ERA5 reanalysis backing
`https://archive-api.open-meteo.com/v1/archive` reliably exposes only
**`wind_speed_10m`** and **`wind_speed_100m`**. Heights `80m`, `120m`,
and `180m` return `units: "undefined"` and all-null values. Verified by
direct API calls at:

- Hornsea offshore (53.88, 1.79): 10m and 100m are non-null for all 168
  hours of a 2025-01-15 → 2025-01-21 probe; 80m, 120m, 180m return
  `units: "undefined"` and 0 non-null values. Correlation 10m vs 100m =
  0.949.
- Whitelee onshore (55.69, -4.27): same shape; 10m and 100m non-null
  for all hours, others all-null. Correlation 10m vs 100m = 0.977.

The 10m → 100m ratio differs by regime (offshore mean 1.14, onshore
1.66), which is itself a feature signal (the Hellmann shear exponent).
Both fields therefore carry distinct information despite the high
correlation.

`WIND_ARCHIVE_VARS` deliberately excludes `wind_speed_{80,120,180}m`
and the matching directions to avoid silver carrying empty columns.
`WIND_FORECAST_VARS` includes the wider set because UKMO UKV / ECMWF
IFS / GFS forecast models do publish hub-height data; Open-Meteo nulls
fields the underlying model does not carry, and `WindWeather` accepts
the null-degradation cleanly (all hub-height fields typed `float | None`).

---

## historical_demand

ERA5 historical observations at the 7 UK population centres. F0 set
plus winter-peak snow variables.

**API path:** `https://archive-api.open-meteo.com/v1/archive`
**Param style:** `?latitude=&longitude=&hourly=...&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`
**Hourly variables (9):** `temperature_2m, wind_speed_10m, wind_direction_10m, relative_humidity_2m, precipitation, shortwave_radiation, surface_pressure, snowfall, snow_depth`
**Silver schema:** `DemandWeather`
**Derived columns in silver:** `hdd` (max(0, 15.5 - T)), `cdd` (max(0, T - 22.0)), `air_density_kg_m3` (P / (287.05 * T_K)).

`historical_demand` is the renamed successor of the F0-era `historical`
dataset.

---

## forecast_demand

Near-term demand-weather forecast at the 7 population centres. Same
schema and derived columns as `historical_demand`. F7.5 renamed
successor of the F0-era `forecast`.

---

## historical_wind

ERA5 hub-height-aware observations at the 12 GB wind sites. Hub heights
limited to `{10m, 100m}` per the archive limitation above.

**API path:** `https://archive-api.open-meteo.com/v1/archive`
**Hourly variables (13):** `temperature_2m, surface_pressure, wind_speed_10m, wind_speed_100m, wind_direction_10m, wind_direction_100m, wind_gusts_10m, cloud_cover, cloud_cover_low, cloud_cover_mid, cloud_cover_high, dew_point_2m, precipitation`
**Silver schema:** `WindWeather`
**Derived:** `air_density_kg_m3`.

---

## forecast_wind

Near-term wind forecast at the 12 GB wind sites; carries the wider
hub-height variable set.

**API path:** `https://api.open-meteo.com/v1/forecast`
**Hourly variables (19):** archive set ∪ `{wind_speed_80m, wind_speed_120m, wind_speed_180m, wind_direction_80m, wind_direction_120m, wind_direction_180m}`
**Silver schema:** `WindWeather` (all hub heights `float | None` — fields the underlying model does not carry are silver-null).

---

## historical_solar

ERA5 irradiance-decomposition observations at the 6 GB solar sites.

**API path:** `https://archive-api.open-meteo.com/v1/archive`
**Hourly variables (12):** `temperature_2m, shortwave_radiation, direct_radiation, direct_normal_irradiance, diffuse_radiation, global_tilted_irradiance, cloud_cover, cloud_cover_low, cloud_cover_mid, cloud_cover_high, snowfall, snow_depth`
**Extra params:** `tilt=35&azimuth=0` (UK fixed-tilt rep geometry: latitude minus ~15°, due south).
**Silver schema:** `SolarWeather`.
**Derived:** none. Solar dataset does not request `surface_pressure`, so
no air-density derivation.

GHI = `shortwave_radiation`; the separation
`GHI ≈ DNI * cos(zenith) + DHI` is a useful invariant — silver passes
through the API's separation-model output and a property test
(`tests/unit/test_openmeteo_irradiance_components.py`) asserts
`direct_radiation + diffuse_radiation` is within 5% of
`shortwave_radiation` for daylight rows.

---

## forecast_solar

Near-term solar forecast at the 6 GB solar sites. Same variable set
and `tilt`/`azimuth` extra params as `historical_solar`.

---

## Silver canonical names (F15-B)

F15-B (2026-05-12) applied a column rename at the silver emission boundary
(`BaseOpenMeteoTransformer.transform()`). Bronze files preserve the
connector-native Open-Meteo names; silver emits canonical names with
explicit unit suffixes:

**Unit conversions (km/h → m/s, factor 1/3.6):**

| Bronze column       | Silver column        |
|---------------------|----------------------|
| `wind_speed_10m`    | `wind_speed_10m_mps` |
| `wind_speed_100m`   | `wind_speed_100m_mps`|
| `wind_speed_80m`    | `wind_speed_80m_mps` |
| `wind_speed_120m`   | `wind_speed_120m_mps`|
| `wind_speed_180m`   | `wind_speed_180m_mps`|
| `wind_gusts_10m`    | `wind_gusts_10m_mps` |

Wind speeds are **km/h in bronze** (Open-Meteo default when no `windspeed_unit`
param is sent). Silver divides by 3.6 to produce m/s. Verification:
`36.0 km/h → 10.0 m/s`.

**Pure renames (no value change):**

| Bronze column              | Silver column                   |
|----------------------------|---------------------------------|
| `temperature_2m`           | `temperature_2m_c`              |
| `dew_point_2m`             | `dew_point_2m_c`                |
| `surface_pressure`         | `surface_pressure_hpa`          |
| `relative_humidity_2m`     | `relative_humidity_2m_pct`      |
| `cloud_cover`              | `cloud_cover_pct`               |
| `cloud_cover_low/mid/high` | `cloud_cover_low/mid/high_pct`  |
| `shortwave_radiation`      | `shortwave_radiation_wm2`       |
| `direct_radiation`         | `direct_radiation_wm2`          |
| `direct_normal_irradiance` | `direct_normal_irradiance_wm2`  |
| `diffuse_radiation`        | `diffuse_radiation_wm2`         |
| `global_tilted_irradiance` | `global_tilted_irradiance_wm2`  |
| `precipitation`            | `precipitation_mm`              |
| `snowfall`                 | `snowfall_cm`                   |
| `snow_depth`               | `snow_depth_m`                  |
| `wind_direction_*`         | `wind_direction_*_deg`          |
| `hdd` (derived)            | `hdd_k`                         |
| `cdd` (derived)            | `cdd_k`                         |

The rename runs **after** `_add_derived()` so HDD/CDD and air-density
derivations read the connector-native names before renaming.

---

## Migration history

- F7.5 (2026-05-09): role split from `{historical, forecast}` to six
  role-specific datasets. `DATASET_VERSION` 1.0.0 → 2.0.0. Hard rename
  with no alias layer (no on-disk silver to migrate locally). Workstream
  C (15-min `minutely_15` forecast) deferred to backlog pending AROME
  2026 northern-boundary verification.
- F15-B (2026-05-12): canonical column renames applied at silver emission
  boundary. Wind speeds now m/s in silver. See table above.
