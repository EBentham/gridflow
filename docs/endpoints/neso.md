# NESO Carbon Intensity Platform - Dataset Reference

**Source:** `neso`
**Base URL:** `https://api.carbonintensity.org.uk`
**Authentication:** None
**Official docs:** https://carbon-intensity.github.io/api-definitions/
**Coverage:** Great Britain national and regional carbon intensity

The NESO Carbon Intensity API exposes five response families. Gridflow registers
all 33 documented route variants as active datasets and normalises them into
family-specific silver tables.

## Dataset Families

| Family | Silver shape | Notes |
|--------|--------------|-------|
| National intensity | One row per half-hour | Forecast, actual, and index values in `gCO2/kWh`. |
| National statistics | One row per stats block | Max, average, min, and index values over the requested range/block. |
| Emission factors | One row per fuel | Static generation fuel factors in `gCO2/kWh`; written as reference parquet. |
| Generation mix | One row per half-hour and fuel | Percent contribution by fuel. |
| Regional intensity | One row per half-hour, region, and fuel | Region metadata, forecast/index, and nested generation mix. |

## Active Endpoint Inventory

| Dataset | API path template | Family |
|---------|-------------------|--------|
| `intensity_current` | `/intensity` | National intensity |
| `intensity_today` | `/intensity/date` | National intensity |
| `intensity_date` | `/intensity/date/{date}` | National intensity |
| `intensity_period` | `/intensity/date/{date}/{period}` | National intensity; Gridflow fans this out across every GB settlement period for each requested date. |
| `intensity_factors` | `/intensity/factors` | Emission factors |
| `intensity_at` | `/intensity/{from}` | National intensity |
| `intensity_fw24h` | `/intensity/{from}/fw24h` | National intensity |
| `intensity_fw48h` | `/intensity/{from}/fw48h` | National intensity |
| `intensity_pt24h` | `/intensity/{from}/pt24h` | National intensity |
| `carbon_intensity` | `/intensity/{from}/{to}` | National intensity |
| `intensity_stats` | `/intensity/stats/{from}/{to}` | National statistics |
| `intensity_stats_block` | `/intensity/stats/{from}/{to}/{block}` | National statistics |
| `generation_current` | `/generation` | Generation mix |
| `generation_pt24h` | `/generation/{from}/pt24h` | Generation mix |
| `generation` | `/generation/{from}/{to}` | Generation mix |
| `regional_current` | `/regional` | Regional intensity |
| `regional_england` | `/regional/england` | Regional intensity |
| `regional_scotland` | `/regional/scotland` | Regional intensity |
| `regional_wales` | `/regional/wales` | Regional intensity |
| `regional_postcode` | `/regional/postcode/{postcode}` | Regional intensity |
| `regional_regionid` | `/regional/regionid/{regionid}` | Regional intensity |
| `regional_intensity_fw24h` | `/regional/intensity/{from}/fw24h` | Regional intensity |
| `regional_intensity_fw24h_postcode` | `/regional/intensity/{from}/fw24h/postcode/{postcode}` | Regional intensity |
| `regional_intensity_fw24h_regionid` | `/regional/intensity/{from}/fw24h/regionid/{regionid}` | Regional intensity |
| `regional_intensity_fw48h` | `/regional/intensity/{from}/fw48h` | Regional intensity |
| `regional_intensity_fw48h_postcode` | `/regional/intensity/{from}/fw48h/postcode/{postcode}` | Regional intensity |
| `regional_intensity_fw48h_regionid` | `/regional/intensity/{from}/fw48h/regionid/{regionid}` | Regional intensity |
| `regional_intensity_pt24h` | `/regional/intensity/{from}/pt24h` | Regional intensity |
| `regional_intensity_pt24h_postcode` | `/regional/intensity/{from}/pt24h/postcode/{postcode}` | Regional intensity |
| `regional_intensity_pt24h_regionid` | `/regional/intensity/{from}/pt24h/regionid/{regionid}` | Regional intensity |
| `regional_intensity` | `/regional/intensity/{from}/{to}` | Regional intensity |
| `regional_intensity_postcode` | `/regional/intensity/{from}/{to}/postcode/{postcode}` | Regional intensity |
| `regional_intensity_regionid` | `/regional/intensity/{from}/{to}/regionid/{regionid}` | Regional intensity |

## Defaults

Gridflow uses these defaults for path variables when callers do not override
them:

| Variable | Default | Reason |
|----------|---------|--------|
| `period` | `1` | Direct path-construction default only. Pipeline fetches iterate all settlement periods for `intensity_period`: normally 48, 46 on GB spring-clock-change dates, and 50 on GB autumn-clock-change dates. |
| `block` | `24` | Stats block endpoint accepts hour block sizes. |
| `postcode` | `RG10` | Official docs example and live-valid postcode district. |
| `regionid` | `13` | London region, live-valid and stable in the API. |

## Silver Key Columns

| Family | Key columns |
|--------|-------------|
| National intensity | `timestamp_utc`, `period_end_utc`, `forecast_gco2_kwh`, `actual_gco2_kwh`, `intensity_index` |
| National statistics | `timestamp_utc`, `period_end_utc`, `max_gco2_kwh`, `average_gco2_kwh`, `min_gco2_kwh`, `intensity_index` |
| Emission factors | `fuel`, `factor_gco2_kwh` |
| Generation mix | `timestamp_utc`, `period_end_utc`, `fuel`, `generation_percentage` |
| Regional intensity | `timestamp_utc`, `period_end_utc`, `regionid`, `shortname`, `postcode`, `forecast_gco2_kwh`, `intensity_index`, `fuel`, `generation_percentage` |

## Verification

- `tests/unit/test_neso_endpoints.py` checks endpoint inventory, path construction,
  defaults, and transformer registration.
- `tests/integration/test_neso_mocked_e2e.py` covers every active dataset through
  mocked connector fetch, bronze write, and silver transform. It also verifies
  `intensity_period` fetches every settlement period for normal, short, and
  long GB settlement days.
- `tests/integration/test_neso_live_e2e.py` is opt-in with `-m live` and proves
  real API responses for every active dataset can be written to bronze and
  transformed to silver, including multi-response datasets.
- `tests/integration/test_neso_cli_live_smoke.py` is opt-in with `-m live` and
  runs the public CLI `pipeline neso carbon_intensity` path in isolated temp
  directories.
