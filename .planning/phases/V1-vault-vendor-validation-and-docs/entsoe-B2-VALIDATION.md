---
phase: V1
vendor: entsoe
batch: B2-generation-outages
validated: 2026-05-08
total_datasets: 13
---

# ENTSOE B2 (Generation + Outages) — V1 Validation Report

Live validation run against `https://web-api.tp.entsoe.eu/api` on
2026-05-08 using `curl --ssl-no-revoke`. Default GB domain
(`10YGB----------A`); EMPTY-GB cases were re-tested against DE-LU
(`10Y1001A1001A82H`), Norway-1 (`10YNO-1--------2`), Belgium
(`10YBE----------2`) or Netherlands (`10YNL----------L`) as appropriate
to disambiguate "GB has no published data" (EMPTY) from "request shape
incorrect" (FAIL).

## Summary

| Status | Count |
|--------|-------|
| PASS   | 4     |
| EMPTY  | 9     |
| FAIL   | 0     |

Total: 13.

## Per-dataset results

| Dataset | docType | procType | BusinessType | Domain param | Status | HTTP | Bytes | Time (s) | Cause | Vault page |
|---------|---------|----------|--------------|--------------|--------|------|-------|----------|-------|-----------|
| actual_generation | A75 | A16 | n/a | in_Domain | EMPTY | 200 | 971 | 0.25 | GB Brexit — DE-LU PASS confirms shape | [actual_generation.md](../../../../../OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/entsoe/datasets/actual_generation.md) |
| wind_solar_forecast | A69 | A01 | n/a | in_Domain | EMPTY | 200 | 962 | 0.59 | GB Brexit — DE-LU PASS | [wind_solar_forecast.md](../../../../../OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/entsoe/datasets/wind_solar_forecast.md) |
| generation_forecast | A71 | A01 | n/a | in_Domain | EMPTY | 200 | 966 | 0.16 | GB Brexit — DE-LU PASS | [generation_forecast.md](../../../../../OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/entsoe/datasets/generation_forecast.md) |
| actual_generation_units | A73 | A16 | n/a | in_Domain | EMPTY | 200 | 968 | 0.64 | GB Brexit AND DE-LU also empty (per-unit data not published continent-wide) | [actual_generation_units.md](../../../../../OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/entsoe/datasets/actual_generation_units.md) |
| water_reservoirs | A72 | A16 | n/a | in_Domain | EMPTY | 200 | 977 | 0.13 | GB has no hydro reservoirs to report; NO-1 30-day window PASS confirms shape | [water_reservoirs.md](../../../../../OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/entsoe/datasets/water_reservoirs.md) |
| installed_capacity | A68 | A33 | n/a | in_Domain | EMPTY | 200 | 975 | 0.33 | GB Brexit — DE-LU yearly PASS | [installed_capacity.md](../../../../../OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/entsoe/datasets/installed_capacity.md) |
| installed_capacity_units | A71 | A33 | n/a | in_Domain | PASS | 200 | 366199 | 0.87 | 230 TimeSeries — GB unit registry remains published | [installed_capacity_units.md](../../../../../OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/entsoe/datasets/installed_capacity_units.md) |
| generation_units_master_data | A95 | n/a | B11 | BiddingZone_Domain | PASS | 200 | 430018 | 0.73 | 230 TimeSeries — GB master data registry. Uses `Implementation_DateAndOrTime` single-date param. | [generation_units_master_data.md](../../../../../OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/entsoe/datasets/generation_units_master_data.md) |
| outages_generation | A80 | n/a | A53 | BiddingZone_Domain | PASS | 200 | 40528 | 1.29 | ZIP archive, 17 outage XMLs over 30-day GB window | [outages_generation.md](../../../../../OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/entsoe/datasets/outages_generation.md) |
| outages_consumption | A76 | n/a | A53 | BiddingZone_Domain | EMPTY | 200 | 984 | 0.19 | GB Brexit — DE-LU PASS | [outages_consumption.md](../../../../../OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/entsoe/datasets/outages_consumption.md) |
| outages_transmission | A78 | n/a | A53 | In_Domain+Out_Domain | PASS | 200 | 7588 | 0.44 | ZIP archive, 7 outage XMLs over 30-day GB-FR window | [outages_transmission.md](../../../../../OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/entsoe/datasets/outages_transmission.md) |
| outages_offshore_grid | A79 | n/a | n/a | BiddingZone_Domain | EMPTY | 200 | 963 | 1.14 | Structurally sparse — also EMPTY for BE and NL | [outages_offshore_grid.md](../../../../../OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/entsoe/datasets/outages_offshore_grid.md) |
| outages_production | A77 | n/a | A53 | BiddingZone_Domain | EMPTY | 200 | 1005 | 0.33 | GB Brexit — DE-LU 1-day PASS confirms shape (30d returned 400 over 200-record cap) | [outages_production.md](../../../../../OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/entsoe/datasets/outages_production.md) |

## Curl evidence

For all calls below, `${ENTSOE_API_KEY}` is the value loaded from
`C:/Users/Bobbo/OneDrive/Desktop/Python/gridflow/.env`. Throttle 1 req/s
(`sleep 1.1` between calls). Capture written to
`.tmp/entsoe-<key>.xml`.

### EMPTY: actual_generation (A75/A16) — GB

```
curl --ssl-no-revoke -fsS \
  -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?securityToken=${ENTSOE_API_KEY}&documentType=A75&processType=A16&in_Domain=10YGB----------A&periodStart=202605060000&periodEnd=202605070000"
```

Response (first 200B): `<Acknowledgement_MarketDocument ...><Reason><code>999</code><text>No matching data found for Data item AGGREGATED_GENERATION_PER_TYPE_R3 [16.1.B&C] (10YGB----------A) ...`

DE-LU fallback PASS: 17 TimeSeries, root `GL_MarketDocument`.

### EMPTY: wind_solar_forecast (A69/A01) — GB

```
curl --ssl-no-revoke -fsS \
  "https://web-api.tp.entsoe.eu/api?securityToken=${ENTSOE_API_KEY}&documentType=A69&processType=A01&in_Domain=10YGB----------A&periodStart=202605060000&periodEnd=202605070000"
```

Reason: `999`, `GENERATION_FORECAST_WIND_SOLAR [14.1.D]`.

DE-LU fallback PASS: 3 TimeSeries (B16/B18/B19).

### EMPTY: generation_forecast (A71/A01) — GB

```
curl --ssl-no-revoke -fsS \
  "https://web-api.tp.entsoe.eu/api?securityToken=${ENTSOE_API_KEY}&documentType=A71&processType=A01&in_Domain=10YGB----------A&periodStart=202605060000&periodEnd=202605070000"
```

Reason: `999`, `DAY_AHEAD_AGGREGATED_GENERATION_R3 [14.1.C]`.

DE-LU fallback PASS: 1 TimeSeries.

### EMPTY: actual_generation_units (A73/A16) — GB and DE-LU

```
curl --ssl-no-revoke -fsS \
  "https://web-api.tp.entsoe.eu/api?securityToken=${ENTSOE_API_KEY}&documentType=A73&processType=A16&in_Domain=10YGB----------A&periodStart=202605060000&periodEnd=202605070000"
```

Reason: `999`, `ACTUAL_GENERATION_OUTPUT_PER_UNIT_R3 [16.1.A]`. Same
result for DE-LU — A73/A16 is rarely published continent-wide. Request
shape valid (no syntactic error), simply no published data.

### EMPTY: water_reservoirs (A72/A16) — GB

```
curl --ssl-no-revoke -fsS \
  "https://web-api.tp.entsoe.eu/api?securityToken=${ENTSOE_API_KEY}&documentType=A72&processType=A16&in_Domain=10YGB----------A&periodStart=202605060000&periodEnd=202605070000"
```

Reason: `999`, `AGGREGATE_FILLING_RATE_OF_WATER_RESERVOIRS_R3 [16.1.D]`.
GB hydro inventories are not reported. NO-1 30-day fallback PASS:
`GL_MarketDocument`, 1 TimeSeries, `quantity_Measure_Unit.name=MWH`,
resolution `P7D`, value 10176 MWh.

### EMPTY: installed_capacity (A68/A33) — GB

```
curl --ssl-no-revoke -fsS \
  "https://web-api.tp.entsoe.eu/api?securityToken=${ENTSOE_API_KEY}&documentType=A68&processType=A33&in_Domain=10YGB----------A&periodStart=202601010000&periodEnd=202612310000"
```

Reason: `999`, `INSTALLED_GENERATION_CAPACITY_AGGREGATED_R3 [14.1.A]`.

DE-LU yearly fallback PASS: 20 TimeSeries (one per production type).

### PASS: installed_capacity_units (A71/A33) — GB

```
curl --ssl-no-revoke -fsS \
  "https://web-api.tp.entsoe.eu/api?securityToken=${ENTSOE_API_KEY}&documentType=A71&processType=A33&in_Domain=10YGB----------A&periodStart=202601010000&periodEnd=202612310000"
```

Response (first 200B): `<GL_MarketDocument ...><type>A71</type><process.processType>A33</process.processType>...`. 230 TimeSeries (one per registered unit). Unit names ABRBO, DRAXX-1 etc.

### PASS: generation_units_master_data (A95) — GB

```
curl --ssl-no-revoke -fsS \
  "https://web-api.tp.entsoe.eu/api?securityToken=${ENTSOE_API_KEY}&documentType=A95&BusinessType=B11&BiddingZone_Domain=10YGB----------A&Implementation_DateAndOrTime=2026-05-06"
```

Response root: `Configuration_MarketDocument` (not `Publication_*`),
process.processType A39, 230 TimeSeries with `<registeredResource.mRID>`,
`<registeredResource.name>`, `<MktPSRType>`,
`<implementation_DateAndOrTime.date>`. `BusinessType=B11` mandatory.
Note: this dataset uses single-date `Implementation_DateAndOrTime`
parameter, NOT `periodStart`/`periodEnd` — correctly handled by
`EntsoeDocType.date_param` mechanism in `endpoints.py` line 117.

### PASS: outages_generation (A80) — GB 30-day window

```
curl --ssl-no-revoke -fsS \
  "https://web-api.tp.entsoe.eu/api?securityToken=${ENTSOE_API_KEY}&documentType=A80&BusinessType=A53&BiddingZone_Domain=10YGB----------A&periodStart=202604010000&periodEnd=202605010000"
```

Response: ZIP archive (40 KB, magic bytes `PK\x03\x04`). Contains 17
inner XML files named `001-UNAVAILABILITY_OF_PRODUCTION_AND_GENERATION_UNITS_*.xml`.
Connector unzips transparently via `_iter_zip_xml()` in `client.py`.

### EMPTY: outages_consumption (A76) — GB

```
curl --ssl-no-revoke -fsS \
  "https://web-api.tp.entsoe.eu/api?securityToken=${ENTSOE_API_KEY}&documentType=A76&BusinessType=A53&BiddingZone_Domain=10YGB----------A&periodStart=202604010000&periodEnd=202605010000"
```

Reason: `999`, `UNAVAILABILITY_OF_CONSUMPTION_UNITS_AGGREGATED [7.1.A, 7.1.B]`.

DE-LU 30-day fallback PASS: `Unavailability_MarketDocument`, 1 TimeSeries.

### PASS: outages_transmission (A78) — GB→FR 30-day window

```
curl --ssl-no-revoke -fsS \
  "https://web-api.tp.entsoe.eu/api?securityToken=${ENTSOE_API_KEY}&documentType=A78&BusinessType=A53&In_Domain=10YGB----------A&Out_Domain=10YFR-RTE------C&periodStart=202604010000&periodEnd=202605010000"
```

Response: ZIP archive (7.6 KB) containing 7 inner
`UNAVAILABILITY_IN_TRANSMISSION_GRID_*.xml` files. Note **capital-I**
`In_Domain` / `Out_Domain` (distinct from prices/flows lowercase
`in_Domain` / `out_Domain`).

### EMPTY: outages_offshore_grid (A79) — GB / BE / NL all empty

```
curl --ssl-no-revoke -fsS \
  "https://web-api.tp.entsoe.eu/api?securityToken=${ENTSOE_API_KEY}&documentType=A79&BiddingZone_Domain=10YGB----------A&periodStart=202604010000&periodEnd=202605010000"
```

Reason: `999`, `UNAVAILABILITY_OF_OFFSHORE_GRID [10.1.C]`.

Fallbacks for BE and NL also EMPTY — A79 is structurally sparse
across all tested zones. No `BusinessType` parameter (correctly omitted
by code).

### EMPTY: outages_production (A77) — GB; DE-LU 1-day PASS

```
# GB 30-day:
curl --ssl-no-revoke -fsS \
  "https://web-api.tp.entsoe.eu/api?securityToken=${ENTSOE_API_KEY}&documentType=A77&BusinessType=A53&BiddingZone_Domain=10YGB----------A&periodStart=202604010000&periodEnd=202605010000"
```

GB Reason: `999`, `UNAVAILABILITY_OF_PRODUCTION_AND_GENERATION_UNITS [15.1.A, 15.1.B, 15.1.C, 15.1.D]`.

DE-LU 30-day returned HTTP 400 reason 999 "The number of instances
(3209) exceeds the allowed maximum (200)". DE-LU 1-day window: HTTP 200
ZIP (150 KB) — request shape valid, just needs smaller windows.

## Implementation deltas

The following deltas affect B2 datasets only. They are **not** code
fixes (V1 is documentation-only); they are recorded here and in each
dataset page's `## Implementation delta` for downstream awareness.

1. **`psrType` not in `optional_params` for `actual_generation`,
   `wind_solar_forecast`, `actual_generation_units`** — the API guide
   documents `psrType` as an optional filter, but `endpoints.py:DOC_TYPES`
   does not list it in `optional_params`. Callers can still inject it
   via `**params` because `_optional_filter_params` enforces only the
   declared list — the unknown key is silently dropped. Flag as
   `unverified` for connector ergonomics. **No code change in V1.**

2. **A95 single-date param** — `generation_units_master_data` correctly
   uses `Implementation_DateAndOrTime` (single ISO date), not
   `periodStart`/`periodEnd`. The `EntsoeDocType.date_param` mechanism in
   `endpoints.py` is the only example of this pattern. Any future
   dataset that needs a non-period parameter must reuse the pattern
   rather than hard-coding around it.

3. **`In_Domain` / `Out_Domain` capital-I for A78** —
   `outages_transmission` uses capital-I domain params. Code handles
   this via explicit `domain_params=("In_Domain", "Out_Domain")`. New
   contributors must not assume the lowercase form universal across
   ENTSO-E.

4. **A79 has no `BusinessType`** — `outages_offshore_grid` is the only
   outage dataset where `extra_params` does **not** carry
   `BusinessType=A53`. Adding one would break the request. Code is
   correct; this is a documentation note for symmetry with the other
   four outage datasets.

5. **A71 documentType collision** — `installed_capacity_units` (A71/A33)
   and `generation_forecast` (A71/A01) share the same documentType.
   Disambiguation is by `processType`. Be careful in any cross-reference
   that uses documentType alone as a key.

6. **`area_name` field in `EntsoeActualGeneration`** — declared in the
   schema but never populated by the transformer. Defaults to "". Flag
   as `unverified` in silver output.

7. **`DEFAULT_ZONES` is GB-centric** — datasets like A72
   `water_reservoirs` are useless for GB / FR / NL / BE / DE-LU /
   IE-SEM (none has hydro). Operators using the connector must override
   the zone list when ingesting hydro-relevant data. Configuration gap,
   not code bug.

8. **A77 200-record cap** — `outages_production` over a 30-day DE-LU
   window returns HTTP 400 reason 999 when >200 outage notifications
   exist. Connector does not currently retry with smaller windows on
   this specific error code; flag as `unverified` whether
   `RETRY_POLICY` (which retries on `httpx.HTTPStatusError`) cycles
   through to exhaustion or correctly bails out.

## Notes for downstream consumers

- The **GB EMPTY** pattern is a Brexit consequence, not a code fault —
  GB ceased publishing most operational ENTSO-E datasets after IEM
  exit. Use Elexon BMRS for GB equivalents:
  - A75 → Elexon `fuelhh` / `fuelinst`
  - A69 → Elexon `windfor`
  - A71/A01 → no direct GB equivalent (Elexon `ndf` is residual demand)
  - A68/A33 → Elexon `bmunits_reference` + DUKES
  - A76, A77, A80 → Elexon `remit` notifications
- A71/A33 (`installed_capacity_units`) and A95 (`generation_units_master_data`)
  remain published for GB — these are network-code obligations and
  survive the IEM exit.
- 30-day window for outages is the right operational default; daily
  ingestion can use rolling 30-day with dedup on `document_mrid`.

## Outage status code reference

For the 5 outage datasets (A80/A76/A78/A79/A77), the `<docStatus><value>`
field uses the EIC outage status codelist:

| Code | Meaning |
|------|---------|
| A05  | Active — outage notification is current and applies |
| A09  | Cancelled — outage notification was cancelled before activation |
| A13  | Withdrawn — outage notification was withdrawn after publication |

DocStatus is also accepted as a query parameter (`DocStatus=A05`) to
filter to active notifications. Without filter, all statuses are
returned and downstream silver dedup must keep latest revision per
`document_mrid`.

## End of B2

Inputs to the orchestrator: 13 dataset pages, 4 PASS / 9 EMPTY / 0
FAIL. Shared writes (`entsoe/README.md`, `entsoe/endpoints.md`)
deferred to B4 per V1-CONTEXT.md.
