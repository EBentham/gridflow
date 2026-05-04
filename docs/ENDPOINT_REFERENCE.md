# Gridflow Endpoint Reference

This document describes the **exact URL and parameter structure** for every API endpoint used by gridflow. All example URLs have been **verified against live APIs** (tested 2026-02-01).

**Reference date used in examples:** `2026-02-01`

---

## 1. Elexon (BMRS Insights API)

**Base URL:** `https://data.elexon.co.uk/bmrs/api/v1`
**Auth:** None (public API)
**Response format:** JSON

### Important: Actual API Parameter Styles

The Elexon API uses **five distinct parameter patterns** depending on the endpoint. The connector code currently treats many of these as `?settlementDate=` but the API has evolved. The actual working patterns are documented below.

### A) Path-Based Date

The date is embedded in the URL path, not as a query parameter.

```
GET {base_url}{path}/{YYYY-MM-DD}
```

| Dataset | Path | Verified Example URL |
|---------|------|----------------------|
| system_prices | `/balancing/settlement/system-prices/{date}` | `https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices/2026-02-01` |

> **Bug:** The connector sends `?settlementDate=2026-02-01` which returns **404**. The date must be a path segment.

### B) From/To Date Range

These endpoints require `from` and `to` query parameters (not `settlementDate`).

```
GET {base_url}{path}?from=2026-02-01T00:00Z&to=2026-02-02T00:00Z
```

| Dataset | Path | Verified Example URL |
|---------|------|----------------------|
| bod | `/datasets/BOD` | `https://data.elexon.co.uk/bmrs/api/v1/datasets/BOD?from=2026-02-01T00:00Z&to=2026-02-02T00:00Z` |
| disbsad | `/datasets/DISBSAD` | `https://data.elexon.co.uk/bmrs/api/v1/datasets/DISBSAD?from=2026-02-01T00:00Z&to=2026-02-02T00:00Z` |
| mid | `/datasets/MID` | `https://data.elexon.co.uk/bmrs/api/v1/datasets/MID?from=2026-02-01T00:00Z&to=2026-02-02T00:00Z` |
| netbsad | `/datasets/NETBSAD` | `https://data.elexon.co.uk/bmrs/api/v1/datasets/NETBSAD?from=2026-02-01T00:00Z&to=2026-02-02T00:00Z` |

> **Bug:** The connector sends `?settlementDate=2026-02-01` which returns **404** for all of these.

### C) Settlement Date + Period (Both Required)

This endpoint requires **both** `settlementDate` and `settlementPeriod` — date alone returns 404.

```
GET {base_url}{path}?settlementDate=2026-02-01&settlementPeriod=1
```

| Dataset | Path | Verified Example URL |
|---------|------|----------------------|
| pn | `/datasets/PN` | `https://data.elexon.co.uk/bmrs/api/v1/datasets/PN?settlementDate=2026-02-01&settlementPeriod=1` |

> **Bug:** The connector sends `?settlementDate=2026-02-01` without `settlementPeriod`, which returns **404**.

### D) Settlement Date Query Parameter (Working)

Only this endpoint still accepts `?settlementDate=` on its own.

```
GET {base_url}{path}?settlementDate=2026-02-01
```

| Dataset | Path | Verified Example URL |
|---------|------|----------------------|
| generation_by_fuel | `/generation/outturn/summary` | `https://data.elexon.co.uk/bmrs/api/v1/generation/outturn/summary?settlementDate=2026-02-01` |

### E) Publish Datetime Range

These all work correctly with `publishDateTimeFrom` / `publishDateTimeTo`.

```
GET {base_url}{path}?publishDateTimeFrom=2026-02-01T00:00:00Z&publishDateTimeTo=2026-02-02T00:00:00Z
```

| Dataset | Path | Verified Example URL |
|---------|------|----------------------|
| freq | `/datasets/FREQ` | `https://data.elexon.co.uk/bmrs/api/v1/datasets/FREQ?publishDateTimeFrom=2026-02-01T00:00:00Z&publishDateTimeTo=2026-02-02T00:00:00Z` |
| fuelhh | `/datasets/FUELHH` | `https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELHH?publishDateTimeFrom=2026-02-01T00:00:00Z&publishDateTimeTo=2026-02-02T00:00:00Z` |
| fuelinst | `/datasets/FUELINST` | `https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELINST?publishDateTimeFrom=2026-02-01T00:00:00Z&publishDateTimeTo=2026-02-02T00:00:00Z` |
| imbalngc | `/datasets/IMBALNGC` | `https://data.elexon.co.uk/bmrs/api/v1/datasets/IMBALNGC?publishDateTimeFrom=2026-02-01T00:00:00Z&publishDateTimeTo=2026-02-02T00:00:00Z` |
| ndf | `/datasets/NDF` | `https://data.elexon.co.uk/bmrs/api/v1/datasets/NDF?publishDateTimeFrom=2026-02-01T00:00:00Z&publishDateTimeTo=2026-02-02T00:00:00Z` |
| ndfd | `/datasets/NDFD` | `https://data.elexon.co.uk/bmrs/api/v1/datasets/NDFD?publishDateTimeFrom=2026-02-01T00:00:00Z&publishDateTimeTo=2026-02-02T00:00:00Z` |
| melngc | `/datasets/MELNGC` | `https://data.elexon.co.uk/bmrs/api/v1/datasets/MELNGC?publishDateTimeFrom=2026-02-01T00:00:00Z&publishDateTimeTo=2026-02-02T00:00:00Z` |
| fou2t14d | `/datasets/FOU2T14D` | `https://data.elexon.co.uk/bmrs/api/v1/datasets/FOU2T14D?publishDateTimeFrom=2026-02-01T00:00:00Z&publishDateTimeTo=2026-02-02T00:00:00Z` |
| uou2t14d | `/datasets/UOU2T14D` | `https://data.elexon.co.uk/bmrs/api/v1/datasets/UOU2T14D?publishDateTimeFrom=2026-02-01T00:00:00Z&publishDateTimeTo=2026-02-01T04:00:00Z` |
| windfor | `/datasets/WINDFOR` | `https://data.elexon.co.uk/bmrs/api/v1/datasets/WINDFOR?publishDateTimeFrom=2026-02-01T00:00:00Z&publishDateTimeTo=2026-02-02T00:00:00Z` |
| temp | `/datasets/TEMP` | `https://data.elexon.co.uk/bmrs/api/v1/datasets/TEMP?publishDateTimeFrom=2026-02-01T00:00:00Z&publishDateTimeTo=2026-02-02T00:00:00Z` |

> **Note on UOU2T14D:** Maximum query range is **4 hours**. A 24-hour range returns HTTP 400. The connector currently sends 24-hour chunks and needs to be updated to use 4-hour windows.

### F) No Parameters (Static Reference Data)

```
GET {base_url}{path}
```

| Dataset | Path | Verified Example URL |
|---------|------|----------------------|
| bmunits_reference | `/reference/bmunits/all` | `https://data.elexon.co.uk/bmrs/api/v1/reference/bmunits/all` |

### G) Removed/Broken Endpoints

| Dataset | Path | Status | Notes |
|---------|------|--------|-------|
| boal | `/datasets/BOAL` | **404 - Removed** | Endpoint has been decommissioned. Replacement is **BOALF** at `/datasets/BOALF` (uses `from`/`to` params). |
| indicative_imbalance_volumes | `/balancing/settlement/indicative-imbalance-volumes` | **404 - Removed** | Endpoint has been decommissioned entirely. No known replacement. |

**BOALF replacement example:**
```
https://data.elexon.co.uk/bmrs/api/v1/datasets/BOALF?from=2026-02-01T00:00Z&to=2026-02-02T00:00Z
```

### Pagination

- Response JSON contains `meta.page` (or `currentPage`) and `meta.totalPages` (or `lastPage`)
- Increment `page` param until `page >= totalPages`
- `bmunits_reference` has no pagination

---

## 2. ENTSO-E (Transparency Platform)

**Base URL:** `https://web-api.tp.entsoe.eu`
**API path:** `/api`
**Auth:** Query parameter `securityToken` (requires `ENTSOE_API_KEY` env var)
**Response format:** XML

### URL Pattern

```
GET https://web-api.tp.entsoe.eu/api?documentType={docType}&in_Domain.mRID={eic}&out_Domain.mRID={eic}&periodStart={yyyyMMddHHmm}&periodEnd={yyyyMMddHHmm}&securityToken={key}
```

Optional: `&processType={processType}` (when dataset requires it)

### Date Format

`%Y%m%d%H%M` — e.g. `202602010000` for 2026-02-01 00:00

### Bidding Zone EIC Codes

| Zone | EIC Code |
|------|----------|
| GB | `10YGB----------A` |
| FR | `10YFR-RTE------C` |
| NL | `10YNL----------L` |
| BE | `10YBE----------2` |
| DE-LU | `10Y1001A1001A82H` |
| IE-SEM | `10Y1001A1001A59C` |
| ES | `10YES-REE------0` |
| IT | `10YIT-GRTN-----B` |
| DK-1 | `10YDK-1--------W` |
| DK-2 | `10YDK-2--------M` |
| NO-1 | `10YNO-1--------2` |
| SE-1 | `10Y1001A1001A44P` |

**Default zones queried:** GB, FR, NL, BE, DE-LU, IE-SEM

### Datasets

| Dataset | documentType | processType | Iteration |
|---------|-------------|-------------|-----------|
| day_ahead_prices | A44 | *(none)* | Per zone (in=out=same zone) |
| actual_load | A65 | A16 | Per zone |
| load_forecast | A65 | A01 | Per zone |
| actual_generation | A75 | A16 | Per zone |
| wind_solar_forecast | A69 | A01 | Per zone |
| cross_border_flows | A88 | *(none)* | Per zone pair (in != out) |
| outages_generation | A80 | *(none)* | Per zone |
| installed_capacity | A68 | A33 | Per zone |

### Example URLs (GB zone, 2026-02-01)

> **Note:** All ENTSO-E URLs require a valid `securityToken`. Replace `{key}` with your `ENTSOE_API_KEY`. Without a key you will get HTTP 401.

**day_ahead_prices:**
```
https://web-api.tp.entsoe.eu/api?documentType=A44&in_Domain.mRID=10YGB----------A&out_Domain.mRID=10YGB----------A&periodStart=202602010000&periodEnd=202602020000&securityToken={key}
```

**actual_load:**
```
https://web-api.tp.entsoe.eu/api?documentType=A65&processType=A16&in_Domain.mRID=10YGB----------A&out_Domain.mRID=10YGB----------A&periodStart=202602010000&periodEnd=202602020000&securityToken={key}
```

**load_forecast:**
```
https://web-api.tp.entsoe.eu/api?documentType=A65&processType=A01&in_Domain.mRID=10YGB----------A&out_Domain.mRID=10YGB----------A&periodStart=202602010000&periodEnd=202602020000&securityToken={key}
```

**actual_generation:**
```
https://web-api.tp.entsoe.eu/api?documentType=A75&processType=A16&in_Domain.mRID=10YGB----------A&out_Domain.mRID=10YGB----------A&periodStart=202602010000&periodEnd=202602020000&securityToken={key}
```

**wind_solar_forecast:**
```
https://web-api.tp.entsoe.eu/api?documentType=A69&processType=A01&in_Domain.mRID=10YGB----------A&out_Domain.mRID=10YGB----------A&periodStart=202602010000&periodEnd=202602020000&securityToken={key}
```

**cross_border_flows (GB -> FR):**
```
https://web-api.tp.entsoe.eu/api?documentType=A88&in_Domain.mRID=10YGB----------A&out_Domain.mRID=10YFR-RTE------C&periodStart=202602010000&periodEnd=202602020000&securityToken={key}
```

**outages_generation:**
```
https://web-api.tp.entsoe.eu/api?documentType=A80&in_Domain.mRID=10YGB----------A&out_Domain.mRID=10YGB----------A&periodStart=202602010000&periodEnd=202602020000&securityToken={key}
```

**installed_capacity:**
```
https://web-api.tp.entsoe.eu/api?documentType=A68&processType=A33&in_Domain.mRID=10YGB----------A&out_Domain.mRID=10YGB----------A&periodStart=202602010000&periodEnd=202602020000&securityToken={key}
```

### Cross-Border Flow Zone Pairs

| From | To |
|------|----|
| GB | FR |
| GB | NL |
| GB | BE |
| GB | IE-SEM |
| FR | BE |
| FR | DE-LU |
| NL | DE-LU |
| NL | BE |

For cross-border flows, `in_Domain.mRID` = from-zone EIC, `out_Domain.mRID` = to-zone EIC.

---

## 3. ENTSO-G (Gas Transparency Platform)

**Base URL:** `https://transparency.entsog.eu/api/v1`
**Primary API path:** `/operationalData`
**Auth:** None (public API)
**Response format:** JSON

### URL Pattern

```
GET https://transparency.entsog.eu/api/v1/operationalData?from=2026-02-01&to=2026-02-02&indicator=Physical+Flow&periodType=day&pointDirection=UK-TSO-0001ITP-00005exit&timeZone=UCT&limit=-1
```

### Query Parameters

| Parameter | Value | Notes |
|-----------|-------|-------|
| `from` | `2026-02-01` | YYYY-MM-DD format |
| `to` | `2026-02-02` | YYYY-MM-DD format |
| `indicator` | `Physical Flow` | Fixed string (URL-encoded as `Physical+Flow`) |
| `periodType` | `day` | Fixed -- daily aggregates |
| `pointDirection` | `UK-TSO-0001ITP-00005exit` | Operator key + point key + direction key |
| `timeZone` | `UCT` | Case-sensitive parameter name; value is ENTSO-G convention |
| `limit` | `-1` | Return all records in a single response |

### Verified Example URL

```
https://transparency.entsog.eu/api/v1/operationalData?from=2026-02-01&to=2026-02-02&indicator=Physical+Flow&periodType=day&pointDirection=UK-TSO-0001ITP-00005exit&timeZone=UCT&limit=-1
```

### Notes

- `/operationalData` is case-sensitive and requires `pointDirection` for point-level data.
- Gridflow keeps an expanded endpoint catalog in `docs/entsog_endpoint_catalog.yaml`.
- No pagination needed for curated calls (`limit=-1`).

---

## 4. GIE AGSI (Gas Storage) & ALSI (LNG)

**Base URLs:**
- AGSI: `https://agsi.gie.eu`
- ALSI: `https://alsi.gie.eu`

**API path:** `/api`
**Auth:** Header `x-key: {GIE_API_KEY}`
**Response format:** JSON

### URL Pattern

```
GET https://agsi.gie.eu/api?country=GB&from=2026-02-01&till=2026-02-01&page=1&size=300
```

### Query Parameters

| Parameter | Value | Notes |
|-----------|-------|-------|
| `country` | `GB` | ISO 2-letter country code |
| `from` | `2026-02-01` | YYYY-MM-DD format |
| `till` | `2026-02-01` | YYYY-MM-DD format (note: `till` not `to`) |
| `page` | `1` | Page number (1-based) |
| `size` | `300` | Records per page |

### Countries

- **AGSI (gas storage):** AT, BE, DE, ES, FR, GB, IT, NL, PL
- **ALSI (LNG):** BE, ES, FR, GB, IT, NL, PL, PT

### Verified Example URLs

**AGSI (GB):**
```
https://agsi.gie.eu/api?country=GB&from=2026-02-01&till=2026-02-01&page=1&size=300
```

**ALSI (GB):**
```
https://alsi.gie.eu/api?country=GB&from=2026-02-01&till=2026-02-01&page=1&size=300
```

> **Note on auth:** GIE returns HTTP 200 even without an API key, but the JSON body contains `"error":"access denied"`. The `x-key` header is required for actual data.

### Iteration & Pagination

- One request per country
- Check response JSON: `total` and `pageSize` fields
- Continue if `page * pageSize < total`

---

## 5. Open-Meteo (Weather)

**Base URLs:**
- Historical: `https://archive-api.open-meteo.com/v1`
- Forecast: `https://api.open-meteo.com/v1`

**Auth:** None (public API)
**Response format:** JSON

### Hourly Variables

`temperature_2m,wind_speed_10m,wind_direction_10m,relative_humidity_2m,precipitation,shortwave_radiation,surface_pressure`

### Locations

| Location | Latitude | Longitude |
|----------|----------|-----------|
| london | 51.5074 | -0.1278 |
| birmingham | 52.4862 | -1.8904 |
| manchester | 53.4808 | -2.2426 |
| leeds | 53.8008 | -1.5491 |
| glasgow | 55.8642 | -4.2518 |
| cardiff | 51.4816 | -3.1791 |
| belfast | 54.5973 | -5.9301 |

### Verified Example URLs

**Forecast (London):**
```
https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&hourly=temperature_2m,wind_speed_10m,wind_direction_10m,relative_humidity_2m,precipitation,shortwave_radiation,surface_pressure&start_date=2026-02-01&end_date=2026-02-01&timezone=UTC
```

**Forecast (Birmingham):**
```
https://api.open-meteo.com/v1/forecast?latitude=52.4862&longitude=-1.8904&hourly=temperature_2m,wind_speed_10m,wind_direction_10m,relative_humidity_2m,precipitation,shortwave_radiation,surface_pressure&start_date=2026-02-01&end_date=2026-02-01&timezone=UTC
```

**Forecast (Manchester):**
```
https://api.open-meteo.com/v1/forecast?latitude=53.4808&longitude=-2.2426&hourly=temperature_2m,wind_speed_10m,wind_direction_10m,relative_humidity_2m,precipitation,shortwave_radiation,surface_pressure&start_date=2026-02-01&end_date=2026-02-01&timezone=UTC
```

**Forecast (Leeds):**
```
https://api.open-meteo.com/v1/forecast?latitude=53.8008&longitude=-1.5491&hourly=temperature_2m,wind_speed_10m,wind_direction_10m,relative_humidity_2m,precipitation,shortwave_radiation,surface_pressure&start_date=2026-02-01&end_date=2026-02-01&timezone=UTC
```

**Forecast (Glasgow):**
```
https://api.open-meteo.com/v1/forecast?latitude=55.8642&longitude=-4.2518&hourly=temperature_2m,wind_speed_10m,wind_direction_10m,relative_humidity_2m,precipitation,shortwave_radiation,surface_pressure&start_date=2026-02-01&end_date=2026-02-01&timezone=UTC
```

**Forecast (Cardiff):**
```
https://api.open-meteo.com/v1/forecast?latitude=51.4816&longitude=-3.1791&hourly=temperature_2m,wind_speed_10m,wind_direction_10m,relative_humidity_2m,precipitation,shortwave_radiation,surface_pressure&start_date=2026-02-01&end_date=2026-02-01&timezone=UTC
```

**Forecast (Belfast):**
```
https://api.open-meteo.com/v1/forecast?latitude=54.5973&longitude=-5.9301&hourly=temperature_2m,wind_speed_10m,wind_direction_10m,relative_humidity_2m,precipitation,shortwave_radiation,surface_pressure&start_date=2026-02-01&end_date=2026-02-01&timezone=UTC
```

**Historical (London) -- same params, different base URL:**
```
https://archive-api.open-meteo.com/v1/archive?latitude=51.5074&longitude=-0.1278&hourly=temperature_2m,wind_speed_10m,wind_direction_10m,relative_humidity_2m,precipitation,shortwave_radiation,surface_pressure&start_date=2026-02-01&end_date=2026-02-01&timezone=UTC
```

### Query Parameters

| Parameter | Value | Notes |
|-----------|-------|-------|
| `latitude` | e.g. `51.5074` | Location-specific |
| `longitude` | e.g. `-0.1278` | Location-specific |
| `hourly` | Comma-separated variable list | Always all 7 variables |
| `start_date` | `2026-02-01` | YYYY-MM-DD |
| `end_date` | `2026-02-01` | YYYY-MM-DD |
| `timezone` | `UTC` | Fixed |

### Iteration

- One request per location (7 requests per dataset)
- No pagination
- Historical vs forecast uses different base URL but identical query parameters

---

## 6. NESO (Carbon Intensity)

**Base URL:** `https://api.carbonintensity.org.uk`
**Auth:** None (public API)
**Response format:** JSON

### URL Pattern

```
GET https://api.carbonintensity.org.uk/intensity/{from_datetime}/{to_datetime}
```

- **from:** `%Y-%m-%dT%H:%MZ` format -- e.g. `2026-02-01T00:00Z`
- **to:** `%Y-%m-%dT%H:%MZ` format -- e.g. `2026-02-02T00:00Z`
- No query parameters -- dates are in the URL path

### Verified Example URL

```
https://api.carbonintensity.org.uk/intensity/2026-02-01T00:00Z/2026-02-02T00:00Z
```

### Chunking

- Maximum 14 days per request
- Ranges > 14 days are split into 14-day chunks automatically

### Notes

- Returns half-hourly carbon intensity data
- No pagination needed

---

## Summary of Connector Bugs Found

| Issue | Endpoint(s) | Current Behaviour | Fix Required |
|-------|------------|-------------------|--------------|
| Path-based date needed | `system_prices` | Sends `?settlementDate=` (404) | Move date to URL path segment |
| From/to params needed | `bod`, `disbsad`, `mid`, `netbsad` | Sends `?settlementDate=` (404) | Switch to `?from=&to=` params |
| Period required | `pn` | Sends `?settlementDate=` alone (404) | Must also send `settlementPeriod` |
| Endpoint removed | `boal` | Sends to `/datasets/BOAL` (404) | Replace with `/datasets/BOALF` + `from`/`to` params |
| Endpoint removed | `indicative_imbalance_volumes` | Sends to decommissioned path (404) | Remove from endpoint registry or find replacement |
| Max range exceeded | `uou2t14d` | Sends 24h chunks (400) | Reduce to 4-hour maximum chunks |
