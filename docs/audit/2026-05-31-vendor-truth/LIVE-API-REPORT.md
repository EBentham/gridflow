# Live API Verification Report (Priority 1)

Method: real `gridflow ingest`+`transform` into an isolated temp lake (see [AUDIT-PLAN.md](AUDIT-PLAN.md)). Verdicts are compact; raw results in `probes/results_*.json`.

Severity: **P1-BLOCKER** (data wrong/missing when run) · **P1-CANDIDATE** (smell from sample, needs confirmation) · **OK**.

---

## L0 — Connectivity + transform smoke (1 representative dataset × 6 vendors)

Run T0 · window mostly 2026-05-28→29 (forecasts 2026-05-30→31) · **7/7 returned non-empty silver, all `ingest_rc=0`, all `transform_rc=0`.**

| Vendor / dataset | Auth | Bronze→Silver | Cols | Verdict |
|---|---|---|---|---|
| elexon / system_prices | public | 2→48 rows | 13 | **OK** — settlement_date/period, sell/buy price, NIV, derivation_code. |
| entsoe / day_ahead_prices | **securityToken ✅ (not 401)** | 6→778 rows | 11 | **OK auth**; see C-ENTSOE-AREA below (zone coverage). |
| entsog / physical_flows | public | 1→990 rows | 14 | **OK** flow; see C-ENTSOG-UNIT below. |
| gie_agsi / storage | **x-key ✅** | 18→9 rows | 31 | **OK** GWh units; see C-GIE-URL below. |
| gie_alsi / lng | **x-key ✅** | 8→16 rows | 11 | **OK** send-out; see C-GIE-DTRS below. |
| open_meteo / forecast_solar | public | (sibling)→288 rows | 22 | **OK** — GTI + all irradiance components present (user's example passes). |
| neso / carbon_intensity | public | 1→49 rows | 11 | **OK** — forecast+actual gCO2/kWh, index, half-hourly. |

**Headline P1 outcome:** every configured live endpoint is reachable, both API keys (ENTSO-E, GIE) are valid, and every representative dataset flows end-to-end to non-empty silver. The user's "is solar included?" example is **confirmed present** (`shortwave_radiation_wm2`, `direct_radiation_wm2`, `direct_normal_irradiance_wm2`, `diffuse_radiation_wm2`, `global_tilted_irradiance_wm2`).

### Candidate findings surfaced by L0 samples (need confirmation in deep ticks)

- **C-ENTSOG-UNIT (P1-CANDIDATE, entsog).** Silver column is `flow_gwh_per_day` but the row's `unit` field is literally `"kWh/d"` (value `0.0` at the sampled point). Either the kWh/d→GWh/day conversion is not applied (value still raw) or `unit` is a stale raw label. **Verify in L2/S-ENTSOG-OPDATA:** pull a non-zero point and check magnitude vs the raw operationalData `value` + `unit`. A 1e6 factor error here corrupts all gas-flow magnitudes.
- **C-GIE-DTRS (P1-CANDIDATE, gie_alsi).** `dtrs_pct_full = 724.1` for Belgium — implausible as a percentage (>100%). Either mislabeled (it's an absolute GWh, not a %) or a unit error. **Verify in S-GIE:** compare to ALSI API `dtmi`/`dtrs`/`full` field definitions.
- **C-ENTSOE-AREA (P1-CANDIDATE, entsoe).** day_ahead_prices returned **778 rows for a 1-day window** with `area_code=10Y1001A1001A59C` in the sample. 778 ≈ many bidding zones × 24h, so the connector fetches a multi-zone set. **Verify in S-ENTSOE-PRICES:** confirm (a) the intended GB zone (`10YGB----------A`) is in the set, (b) the multi-zone breadth is intentional and documented in the vault, (c) `10Y1001A1001A59C` maps to the expected zone label.
- **C-GIE-URL (LOW, gie_agsi).** `entity_url = "AT"` looks like a country code, not a URL. Minor; confirm field mapping in S-GIE.
- **N-OPENMETEO-BRONZE (NOTE, not a bug).** Probe reported `bronze_files=0` for forecast_solar while silver had 288 rows — the openmeteo connector partitions bronze under per-location sibling datasets (`BRONZE_SIBLING_DATASETS`), which the probe's simple counter doesn't traverse. Data path is healthy.

---
## L1 — ENTSO-E shared-`document_type` payload diffs (silent-duplication test)

Run T1 · window 2026-05-26→27 · method: ingest each member, extract the response's self-reported classification markers (robust to volatile `<createdDateTime>`). **Result: 0 silent-dup-suspect pairs — every data-returning member is correctly differentiated.**

| Family (shared doc_type) | Member → marker | Verdict |
|---|---|---|
| **A25** | net_positions=`B09`, auction_revenue=`B07` | **OK** distinct businessType. congestion_income & transfer_capacity_use → **empty** (see F-ENTSOE-EMPTY). |
| **A26** | total_nominated_capacity=`B08`, total_capacity_allocated=`A29` | **OK** distinct businessType. |
| **A31** | continuous/implicit/explicit all `0 rows` | **empty, identical 7872B acks** (see F-ENTSOE-EMPTY). |
| **A63** | redispatching_internal=`A63/A16/A85` | **OK**; redispatching_cross_border → empty. |
| **A65** | actual_load=`A16`, load_forecast=`A01`, weekly=`A31`, monthly=`A32`, yearly=`A33` | **OK ✅✅ — all 5 process_types distinct, exactly as config.** |

### F-ENTSOE-GB-ABSENT (MEDIUM — verify intent) — `live-correctness / vault-completeness`
Resolves **C-ENTSOE-AREA**. ENTSO-E areas actually fetched:
- day_ahead_prices: `{10YNL----------L, 10Y1001A1001A82H (DE-LU), 10Y1001A1001A59C, 10YFR-RTE------C, 10YBE----------2}`
- A65 load family: `{BE, DE-LU, NL, FR}`

**GB (`10YGB----------A`) is absent from both day-ahead prices and load.** For a UK/EU pipeline this is material. Most likely **intentional** (GB sourced from Elexon; ENTSO-E stopped reliably publishing GB post-Brexit) — but then the vault/`area_codes.py` must state it explicitly. If unintentional, it's a P1 coverage gap. **Action (S-ENTSOE-PRICES/LOADGEN):** read `connectors/entsoe/area_codes.py` + vault; confirm the area set is deliberate and documented; identify `10Y1001A1001A59C`.

### F-ENTSOE-EMPTY (MEDIUM-candidate) — `live-correctness`
On a normal daily run these returned **no data** (envelope-only acks): `offered_transfer_capacity_{continuous,implicit,explicit}` (all identical 7872B), `congestion_income`, `transfer_capacity_use`, `redispatching_cross_border`. Could be legitimate no-data-for-window/border OR mis-built request params (auction.Category / in==out domain / border). **Not yet a confirmed bug.** Action (S-ENTSOE-TRANSMISSION): check the request-builder params vs the ENTSO-E Guide for these document_types, and whether a different border/window yields data.

## L2 — ENTSO-G `/operationalData` indicator diffs

Run T1 · window 2026-05-26→27 · **Result: 0 silent-dup — all 8 sampled indicators distinct.**

`physical_flows`=Physical Flow, `nominations`=Nomination, `allocations`=Allocation, `firm_booked`=Firm Booked, `interruptible_booked`=Interruptible Booked, `gcv`=GCV, `wobbe_index`=Wobbe Index, `available_through_surrender`=Available through Surrender. Units correctly differ by indicator (flows/bookings `kWh/d`; calorific/wobbe `kWh/Nm³`). ✅

### C-ENTSOG-UNIT — RESOLVED (conversion correct; downgraded to LOW label issue)
Raw physical_flows values ~49–759M **kWh/d**; silver `flow_gwh_per_day` max **759.73** → exactly raw÷1e6. **The kWh/d→GWh/day conversion is correct (not a magnitude bug).** Residual **LOW** finding **F-ENTSOG-UNITLABEL**: the silver row still carries a `unit` column literal `"kWh/d"` while the value column is GWh/day — a stale/contradictory label a downstream consumer could misread. Action (S-ENTSOG-OPDATA): drop or correct the `unit` column, or rename `flow_gwh_per_day`→ honest unit.

## L3 — Forecast & renewable completeness (the "is solar/wind included?" test)

Run T1 · **8/8 non-empty, ZERO all-null/high-null columns anywhere.** The probe now reports all-null columns explicitly (none found).

| Dataset | Rows×Cols | Completeness verdict |
|---|---|---|
| open_meteo forecast_wind | 576×30 | **OK ✅✅** wind_speed/direction at **10/80/100/120/180m all populated** + gusts + air_density. |
| open_meteo forecast_demand | 336×22 | **OK** temp, wind, humidity, shortwave, snow, **hdd_k/cdd_k** derived, air_density. |
| open_meteo historical_solar | 288×22 | **OK** ERA5 archive path works; GTI + all irradiance components present. |
| open_meteo historical_wind | 576×24 | **OK ✅** carries **only 10/100m** — the all-null ERA5 heights (80/120/180m) are correctly **excluded**, not null-filled. |
| entsoe wind_solar_forecast (A69) | 866×12 | **OK** `production_type` split preserved (B19 Wind-Offshore sampled). See F-ENTSOE-PSR-RAW. |
| elexon agpt | 275×15 | **OK** psr_type mapped to labels ("Biomass"…), REMIT-style doc_id/revision. |
| elexon agws | 93×15 | **OK** Solar + Wind psr types, business_type "Solar generation". |
| elexon windfor | 584×9 | **OK** latest_forecast_mw rolling wind forecast. |

**Headline:** the user's core fear — renewable columns silently missing — is **not present**. Solar (GTI + irradiance + Elexon Solar psr) and wind (all hub heights + Elexon/ENTSO-E) flow through to silver complete.

### F-ENTSOE-PSR-RAW (LOW) — `code-vs-doc / cross-source-consistency`
`entsoe wind_solar_forecast.production_type` is stored as the **raw ENTSO-E PSR code** ("B19") whereas `elexon agpt.psr_type` is mapped to human labels ("Biomass"). Inconsistent representation across sources for the same concept. Action (S-ENTSOE-LOADGEN): confirm B16(Solar)/B18(Wind Onshore)/B19(Wind Offshore) all present, and decide raw-code-vs-label policy + document the codes in the vault.

---

## L1–L3 net P1 assessment

- **Connectivity & auth:** all 6 vendors live, both keys valid. ✅
- **Silent duplication (the top fear):** **none found** — every shared-endpoint family that returns data is correctly differentiated (ENTSO-E businessType/processType, ENTSO-G indicator). ✅
- **Completeness:** renewable/forecast columns complete, no all-null leakage; unit conversions correct (ENTSO-G kWh/d→GWh/d). ✅
- **Open P1 items for the static phase:** GB absent from ENTSO-E (F-ENTSOE-GB-ABSENT, verify intent); several ENTSO-E capacity/auction datasets return empty on a normal run (F-ENTSOE-EMPTY, verify params); minor unit-label & psr-code consistency (F-ENTSOG-UNITLABEL, F-ENTSOE-PSR-RAW); GIE candidates from L0 (C-GIE-DTRS, C-GIE-URL) pending S-GIE.

**Bottom line for the user's #1 priority:** when you run the pipeline, data returns correctly and completely for the representative + shared-family datasets tested. The remaining P1 questions are *coverage/intent* (GB) and *a few capacity datasets that return nothing* — not corruption of the data that does return.

---

## L5 — GIE-01 live primary-source verification (added TF)

Probe `probes/verify_gie01.py` fetched the **live ALSI lng** payload and inspected raw keys. **Verdict: GIE-01 CONFIRMED, with a mechanism correction.**

- Live ALSI records carry keys **`dtrs`, `dtmi`, `sendOut`** — and **NO `full` key**. Sample raw values: Belgium `dtrs=724.1`, Italy `889.8`, Spain `2132.3` (all >100 → **`dtrs` is not a percentage**; it is a declared send-out reference capacity, GWh/d). `dtmi` = `{lng, gwh}` declared max inventory.
- ✅ **Confirmed bug:** the ALSI transformer maps `dtrs` → `dtrs_pct_full` (first priority), so the live `724.1` is persisted under a percent-named, percent-documented column.
- ⚠️ **Correction to the original GIE-01 detail:** it claimed the code "drops the true percentage `full`". The live payload has **no `full` field** — `full` exists only in the stale fixture `alsi_gb_response.json` (AGSI-shaped). So the correct fix is NOT "map `full` instead", but: relabel `dtrs` as a send-out-capacity column, capture `dtmi.{lng,gwh}`, and **derive** any %-full as `gas_in_storage / dtmi.gwh × 100` if needed — confirm field semantics against official ALSI docs first. This correction is reflected in REMEDIATION-BACKLOG VTA-GIE-DTRS-01.



