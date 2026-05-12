# GIE (AGSI+ / ALSI) — Dataset Reference

**Sources:** `gie_agsi` (gas storage) · `gie_alsi` (LNG terminals)
**Base URLs:** `https://agsi.gie.eu` / `https://alsi.gie.eu`
**Authentication:** API key required
**Coverage:**
- AGSI+ countries: AT, BE, DE, ES, FR, GB, IT, NL, PL
- ALSI countries: BE, ES, FR, GB, IT, NL, PL, PT
**Granularity:** Daily (one row per country per gas day)

---

## storage *(source: gie_agsi)*

Daily gas storage inventory and flow data from the AGSI+ (Aggregated Gas Storage Inventory) platform. Each row covers one country for one gas day, reporting the total volume held in underground storage, the daily change (injection/withdrawal), and how full storage is as a percentage of total working gas volume. Used to monitor European gas supply security and seasonal storage cycles.

**API path:** `https://agsi.gie.eu/api`
**Param style:** `?country={CC}&from=YYYY-MM-DD&till=YYYY-MM-DD`
**Silver key columns:** `gas_day`, `country_code`, `country_name`, `gas_in_storage_gwh`, `withdrawal_gwh`, `injection_gwh`, `working_gas_volume_gwh`, `storage_pct_full`, `trend`

| gas_day    | country_code | country_name   | gas_in_storage_gwh | withdrawal_gwh | injection_gwh | storage_pct_full | trend |
|------------|--------------|----------------|-------------------:|---------------:|--------------:|----------------:|------:|
| 2024-06-15 | GB           | United Kingdom |           35420.0 |          285.0 |           0.0 |            52.3 |  -0.4 |
| 2024-06-15 | DE           | Germany        |          185600.0 |         1250.0 |           0.0 |            72.1 |  -1.8 |
| 2024-06-15 | FR           | France         |           72800.0 |          620.0 |           0.0 |            68.5 |  -0.9 |
| 2024-06-15 | NL           | Netherlands    |           48200.0 |          380.0 |           0.0 |            55.2 |  -0.6 |
| 2024-06-15 | IT           | Italy          |          115300.0 |          940.0 |           0.0 |            71.8 |  -1.2 |

> All volumes in GWh. `storage_pct_full` is clamped to [0, 100]. `trend` = day-on-day change in `storage_pct_full` (percentage points). Positive `injection_gwh` indicates gas being added; positive `withdrawal_gwh` indicates gas being drawn down. Deduplicated on `(gas_day, country_code)`.

---

## lng *(source: gie_alsi)*

Daily LNG terminal inventory and send-out data from the ALSI (Aggregated LNG Storage Inventory) platform. Covers regasification and floating storage terminals across Europe. Used to track LNG import volumes, terminal utilisation, and the contribution of LNG to gas supply.

**API path:** `https://alsi.gie.eu/api`
**Param style:** `?country={CC}&from=YYYY-MM-DD&till=YYYY-MM-DD`
**Silver key columns:** `gas_day`, `country_code`, `country_name`, `lng_in_storage_gwh`, `send_out_gwh`, `injection_gwh`, `dtrs_pct_full`, `trend`

| gas_day    | country_code | country_name   | lng_in_storage_gwh | send_out_gwh | injection_gwh | dtrs_pct_full | trend |
|------------|--------------|----------------|-------------------:|-------------:|--------------:|--------------:|------:|
| 2024-06-15 | GB           | United Kingdom |            8450.0 |        420.0 |           0.0 |          58.2 |  -2.1 |
| 2024-06-15 | FR           | France         |           12800.0 |        680.0 |           0.0 |          62.5 |  -1.8 |
| 2024-06-15 | ES           | Spain          |           18200.0 |        950.0 |           0.0 |          71.0 |  -2.5 |
| 2024-06-15 | NL           | Netherlands    |            5600.0 |        310.0 |           0.0 |          48.3 |  -1.4 |
| 2024-06-15 | IT           | Italy          |            9100.0 |        510.0 |           0.0 |          55.7 |  -1.9 |

> All volumes in GWh. `dtrs_pct_full` = Day Tank Recirculation Storage percentage full, clamped to [0, 100]. `send_out_gwh` = regasified LNG delivered to the grid. `injection_gwh` = LNG loaded into terminal storage. Deduplicated on `(gas_day, country_code)`.
