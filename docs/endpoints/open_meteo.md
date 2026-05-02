# Open-Meteo — Dataset Reference

**Source:** `open_meteo`
**Base URLs:** `https://archive-api.open-meteo.com/v1` (historical) · `https://api.open-meteo.com/v1` (forecast)
**Authentication:** None (public API)
**Coverage:** 7 UK locations — London, Birmingham, Manchester, Leeds, Glasgow, Cardiff, Belfast
**Resolution:** Hourly

| Location    | Latitude | Longitude |
|-------------|--------:|----------:|
| London      |  51.5074 |   -0.1278 |
| Birmingham  |  52.4862 |   -1.8904 |
| Manchester  |  53.4808 |   -2.2426 |
| Leeds       |  53.8008 |   -1.5491 |
| Glasgow     |  55.8652 |   -4.2576 |
| Cardiff     |  51.4816 |   -3.1791 |
| Belfast     |  54.5973 |   -5.9301 |

---

## historical

Verified hourly weather observations from ERA5 reanalysis, covering the 7 UK locations. Data is typically available with a 5-day lag. Used to build weather-demand correlation models, back-test forecasts, and produce heating/cooling degree day (HDD/CDD) series for energy demand analysis.

**API path:** `https://archive-api.open-meteo.com/v1/archive`
**Param style:** `?latitude=&longitude=&hourly=...&start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`
**Silver key columns:** `timestamp_utc`, `location`, `latitude`, `longitude`, `temperature_2m`, `wind_speed_10m`, `wind_direction_10m`, `relative_humidity_2m`, `precipitation`, `shortwave_radiation`, `surface_pressure`, `hdd`, `cdd`

| timestamp_utc          | location   | temperature_2m | wind_speed_10m | wind_direction_10m | precipitation | hdd  | cdd |
|------------------------|------------|---------------:|---------------:|-------------------:|--------------:|-----:|----:|
| 2024-06-15 00:00:00+00 | london     |           14.2 |            8.5 |                220 |           0.0 | 1.3  | 0.0 |
| 2024-06-15 01:00:00+00 | london     |           13.8 |            7.9 |                215 |           0.0 | 1.7  | 0.0 |
| 2024-06-15 00:00:00+00 | birmingham |           12.9 |            9.2 |                225 |           0.1 | 2.6  | 0.0 |
| 2024-06-15 00:00:00+00 | manchester |           12.1 |           11.4 |                230 |           0.3 | 3.4  | 0.0 |
| 2024-06-15 00:00:00+00 | glasgow    |           10.5 |           13.8 |                240 |           0.8 | 5.0  | 0.0 |

> `temperature_2m` in °C; `wind_speed_10m` in km/h; `wind_direction_10m` in degrees (0–360); `precipitation` in mm; `shortwave_radiation` in W/m²; `surface_pressure` in hPa.
> `hdd` = max(0, 15.5 − temperature_2m) — heating degree hours (base 15.5°C).
> `cdd` = max(0, temperature_2m − 22.0) — cooling degree hours (base 22°C).
> Deduplicated on `(timestamp_utc, location)`. All 7 locations are combined into a single Parquet file per date.

---

## forecast

Near-term hourly weather forecasts for the 7 UK locations, covering approximately 7 days ahead. Updated multiple times daily. Uses the same schema as `historical` but is sourced from the Open-Meteo forecast API. Suitable for day-ahead demand forecasting and operational planning; not suitable for historical analysis (use `historical` instead).

**API path:** `https://api.open-meteo.com/v1/forecast`
**Param style:** `?latitude=&longitude=&hourly=...` (no date range — returns rolling ~7-day window)
**Silver key columns:** Same as `historical` — `timestamp_utc`, `location`, `latitude`, `longitude`, `temperature_2m`, `wind_speed_10m`, `wind_direction_10m`, `relative_humidity_2m`, `precipitation`, `shortwave_radiation`, `surface_pressure`, `hdd`, `cdd`

| timestamp_utc          | location   | temperature_2m | wind_speed_10m | wind_direction_10m | precipitation | hdd  | cdd |
|------------------------|------------|---------------:|---------------:|-------------------:|--------------:|-----:|----:|
| 2024-06-16 00:00:00+00 | london     |           13.5 |            9.0 |                210 |           0.0 | 2.0  | 0.0 |
| 2024-06-16 01:00:00+00 | london     |           13.1 |            8.4 |                205 |           0.0 | 2.4  | 0.0 |
| 2024-06-16 00:00:00+00 | birmingham |           12.3 |           10.1 |                215 |           0.2 | 3.2  | 0.0 |
| 2024-06-16 00:00:00+00 | manchester |           11.8 |           12.0 |                220 |           0.5 | 3.7  | 0.0 |
| 2024-06-16 00:00:00+00 | glasgow    |           10.0 |           14.5 |                235 |           1.1 | 5.5  | 0.0 |

> Identical schema to `historical`. Data is written to `data/silver/open_meteo/forecast/forecast_{YYYYMMDD}.parquet`. Deduplicated on `(timestamp_utc, location)`.
