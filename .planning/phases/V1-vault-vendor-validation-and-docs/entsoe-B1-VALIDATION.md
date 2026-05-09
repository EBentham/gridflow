---
phase: V1
vendor: entsoe
batch: B1-load-prices
validated: 2026-05-08
total_datasets: 11
---

# ENTSOE B1 (Load + Prices + Imbalance) — V1 Validation Report

## Summary

| Status | Count |
|--------|-------|
| PASS   | 0     |
| EMPTY  | 11    |
| FAIL   | 0     |

All 11 B1 datasets returned **HTTP 200** with an `<Acknowledgement_MarketDocument>`
carrying `<code>999</code>` ("No matching data found") for the locked default
(GB area `10YGB----------A`, window `202605060000-202605070000`). Cross-check
against a known-good area (DE-LU `10Y1001A1001A82H` for non-balancing
endpoints, FR `10YFR-RTE------C` and NL `10YNL----------L` for balancing,
both with an older window for balancing) confirmed every connector request
shape is accepted by the live API and returns the documented document
envelope (`Publication_MarketDocument`, `GL_MarketDocument`, or
`Balancing_MarketDocument`) with at least one `<TimeSeries>`. Therefore each
dataset is classified **EMPTY by data availability, not FAIL by request
shape**.

**Cause hypothesis (not API-stated)**: the API response carries no
attribution beyond "No matching data found for Data item X
(10YGB----------A) and interval Y/Z". A likely cause is GB's removal from
the EU day-ahead and balancing data items after the post-Brexit market
reorganisation in January 2021, which is consistent with GB now flowing
through Elexon BMRS rather than ENTSOE. An alternative cause is a publication-lag
issue at the chosen test window — the 2026-05-06 window is recent enough that
some R3-tagged data items may simply not yet be backfilled. A future B1 re-run
should retest GB on an older window (e.g. 2024-Q3) before treating GB as
permanently unpublished.

## Pre-flight

- `.env` copied into worktree from `C:/Users/Bobbo/OneDrive/Desktop/Python/gridflow/.env`. `ENTSOE_API_KEY` length 36 (UUID).
- `.tmp/` created.
- carbonintensity smoke-test: `HTTP 200`.
- ENTSOE health smoke-test (A44, GB, default window): `HTTP 200` with
  `Acknowledgement_MarketDocument` + `code 999` — expected for GB.

## Per-dataset results

All calls used `curl --ssl-no-revoke -fsS -H "Accept: application/xml"`
against `https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&...`.
The "Status" column reflects the GB+default-window result. The "Cause"
explains the 999 reason for that data item; "Vault page" is the relative
link from this report.

| Dataset | docType | procType | businessType | Domain param | Status | HTTP | Bytes | Time (s) | Cause | Vault page |
|---------|---------|----------|--------------|--------------|--------|------|-------|----------|-------|-----------|
| day_ahead_prices | A44 | — | — | in_Domain + out_Domain | EMPTY | 200 | 963 | 0.16 | GB post-Brexit — `ENERGY_PRICES [12.1.D]` not published | [day_ahead_prices](../../../../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/day_ahead_prices.md) |
| actual_load | A65 | A16 | — | outBiddingZone_Domain | EMPTY | 200 | 951 | 0.45 | GB post-Brexit — `ACTUAL_TOTAL_LOAD_R3 [6.1.A]` not published | [actual_load](../../../../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/actual_load.md) |
| load_forecast | A65 | A01 | — | outBiddingZone_Domain | EMPTY | 200 | 963 | 0.41 | GB post-Brexit — `DAY_AHEAD_TOTAL_LOAD_FORECAST_R3 [6.1.B]` not published | [load_forecast](../../../../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/load_forecast.md) |
| load_forecast_weekly | A65 | A31 | — | outBiddingZone_Domain | EMPTY | 200 | 962 | 0.20 | GB post-Brexit — `TOTAL_LOAD_FORECAST [6.1.C&D&E]` not published | [load_forecast_weekly](../../../../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/load_forecast_weekly.md) |
| load_forecast_monthly | A65 | A32 | — | outBiddingZone_Domain | EMPTY | 200 | 962 | 0.15 | GB post-Brexit — `TOTAL_LOAD_FORECAST [6.1.C&D&E]` not published | [load_forecast_monthly](../../../../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/load_forecast_monthly.md) |
| load_forecast_yearly | A65 | A33 | — | outBiddingZone_Domain | EMPTY | 200 | 962 | 0.26 | GB post-Brexit — `TOTAL_LOAD_FORECAST [6.1.C&D&E]` not published | [load_forecast_yearly](../../../../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/load_forecast_yearly.md) |
| forecast_margin | A70 | A33 | — | outBiddingZone_Domain | EMPTY | 200 | 958 | 0.36 | GB post-Brexit — `YEAR_AHEAD_FORECAST_MARGIN_R3 [8.1]` not published | [forecast_margin](../../../../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/forecast_margin.md) |
| imbalance_prices | A85 | — | — | controlArea_Domain | EMPTY | 200 | 951 | 0.18 | GB post-Brexit — `IMBALANCE_PRICES_R3 [17.1.G]` not published | [imbalance_prices](../../../../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/imbalance_prices.md) |
| imbalance_volume | A86 | — | A19 | controlArea_Domain | EMPTY | 200 | 958 | 0.19 | GB post-Brexit — `TOTAL_IMBALANCE_VOLUMES_R3 [17.1.H]` not published | [imbalance_volume](../../../../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/imbalance_volume.md) |
| activated_balancing_prices | A84 | A16 | A96 | controlArea_Domain | EMPTY | 200 | 988 | 0.30 | GB post-Brexit — `PRICES_OF_ACTIVATED_BALANCING_ENERGY_R3 [TR 17.1.F, IF aFRR 3.16]` not published | [activated_balancing_prices](../../../../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/activated_balancing_prices.md) |
| contracted_reserves | A81 | A52 | B95 (+ Type_MarketAgreement.Type=A01) | controlArea_Domain | EMPTY | 200 | 1000 | 0.11 | GB post-Brexit — `AMOUNT_AND_PRICES_PAID_OF_BALANCING_RESERVES_UNDER_CONTRACT_R3 [17.1.B&C]` not published | [contracted_reserves](../../../../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/contracted_reserves.md) |

## Curl evidence

### day_ahead_prices (A44)

```bash
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&documentType=A44&in_Domain=10YGB----------A&out_Domain=10YGB----------A&periodStart=202605060000&periodEnd=202605070000" \
  -o .tmp/entsoe-day_ahead_prices.xml -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
```

Response (HTTP 200, 963 bytes):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Acknowledgement_MarketDocument xmlns="urn:iec62325.351:tc57wg16:451-1:acknowledgementdocument:7:0">
  <mRID>eeb333cf-bebe-4</mRID>
  <createdDateTime>2026-05-08T18:01:49Z</createdDateTime>
  <Reason>
    <code>999</code>
    <text>No matching data found for Data item ENERGY_PRICES [12.1.D] (10YGB----------A, 10YGB----------A) and interval 2026-05-06T00:00:00Z/2026-05-07T00:00:00Z.</text>
  </Reason>
</Acknowledgement_MarketDocument>
```

DE-LU fallback (HTTP 200, 57175 bytes — 8 `<TimeSeries>`):
shape valid, request accepted by API.

### actual_load (A65/A16)

```bash
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&documentType=A65&processType=A16&outBiddingZone_Domain=10YGB----------A&periodStart=202605060000&periodEnd=202605070000" \
  -o .tmp/entsoe-actual_load.xml -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
```

Response: 200, 951B, code 999 — `ACTUAL_TOTAL_LOAD_R3 [6.1.A]`.
DE-LU fallback returns `GL_MarketDocument` with `process.processType=A16`,
`outBiddingZone_Domain.mRID=10Y1001A1001A82H`, 1 `<TimeSeries>`, PT15M
quantity series — shape confirmed.

### load_forecast (A65/A01)

```bash
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&documentType=A65&processType=A01&outBiddingZone_Domain=10YGB----------A&periodStart=202605060000&periodEnd=202605070000" \
  -o .tmp/entsoe-load_forecast.xml -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
```

Response: 200, 963B, code 999 — `DAY_AHEAD_TOTAL_LOAD_FORECAST_R3 [6.1.B]`.
DE-LU fallback returns `GL_MarketDocument` with `process.processType=A01`,
PT15M quantity series — shape confirmed.

### load_forecast_weekly (A65/A31)

```bash
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&documentType=A65&processType=A31&outBiddingZone_Domain=10YGB----------A&periodStart=202605060000&periodEnd=202605070000" \
  -o .tmp/entsoe-load_forecast_weekly.xml -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
```

Response: 200, 962B, code 999 — `TOTAL_LOAD_FORECAST [6.1.C&D&E]`.
DE-LU fallback returns `GL_MarketDocument` with `process.processType=A31`,
2 `<TimeSeries>` — shape confirmed.

### load_forecast_monthly (A65/A32)

```bash
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&documentType=A65&processType=A32&outBiddingZone_Domain=10YGB----------A&periodStart=202605060000&periodEnd=202605070000" \
  -o .tmp/entsoe-load_forecast_monthly.xml -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
```

Response: 200, 962B, code 999 — `TOTAL_LOAD_FORECAST [6.1.C&D&E]`.
DE-LU fallback returns `GL_MarketDocument` with `process.processType=A32`,
2 `<TimeSeries>` — shape confirmed.

### load_forecast_yearly (A65/A33)

```bash
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&documentType=A65&processType=A33&outBiddingZone_Domain=10YGB----------A&periodStart=202605060000&periodEnd=202605070000" \
  -o .tmp/entsoe-load_forecast_yearly.xml -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
```

Response: 200, 962B, code 999 — `TOTAL_LOAD_FORECAST [6.1.C&D&E]`.
DE-LU fallback returns `GL_MarketDocument` with `process.processType=A33`,
2 `<TimeSeries>` — shape confirmed.

### forecast_margin (A70/A33)

```bash
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&documentType=A70&processType=A33&outBiddingZone_Domain=10YGB----------A&periodStart=202605060000&periodEnd=202605070000" \
  -o .tmp/entsoe-forecast_margin.xml -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
```

Response: 200, 958B, code 999 — `YEAR_AHEAD_FORECAST_MARGIN_R3 [8.1]`.
DE-LU fallback returns `GL_MarketDocument` with `type=A70`,
`process.processType=A33`, 1 `<TimeSeries>` — shape confirmed.

### imbalance_prices (A85)

```bash
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&documentType=A85&controlArea_Domain=10YGB----------A&periodStart=202605060000&periodEnd=202605070000" \
  -o .tmp/entsoe-imbalance_prices.xml -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
```

Response: 200, 951B, code 999 — `IMBALANCE_PRICES_R3 [17.1.G]`.
FR fallback (older window 2025-04-01 due to publication lag) returns
`Balancing_MarketDocument` (ZIP-of-XML, unpacked) with `type=A85`,
`process.processType=A16`, `area_Domain.mRID=10YFR-RTE------C`, multiple
`<TimeSeries>` per direction (businessType A19/A20), `imbalance_Price.amount`
points — shape confirmed.

### imbalance_volume (A86)

```bash
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&documentType=A86&businessType=A19&controlArea_Domain=10YGB----------A&periodStart=202605060000&periodEnd=202605070000" \
  -o .tmp/entsoe-imbalance_volume.xml -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
```

Response: 200, 958B, code 999 — `TOTAL_IMBALANCE_VOLUMES_R3 [17.1.H]`.
FR fallback (older window) returns `Balancing_MarketDocument` (ZIP-of-XML)
with `type=A86`, `process.processType=A16`, 4 `<TimeSeries>` —
shape confirmed.

### activated_balancing_prices (A84/A16)

```bash
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&documentType=A84&processType=A16&businessType=A96&controlArea_Domain=10YGB----------A&periodStart=202605060000&periodEnd=202605070000" \
  -o .tmp/entsoe-activated_balancing_prices.xml -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
```

Response: 200, 988B, code 999 — `PRICES_OF_ACTIVATED_BALANCING_ENERGY_R3 [TR 17.1.F, IF aFRR 3.16]`.
NL fallback (older window 2025-04-01) returns `Balancing_MarketDocument`
with `type=A84`, `process.processType=A16`, `businessType=A96`, 2
`<TimeSeries>` — shape confirmed.

### contracted_reserves (A81/A52)

```bash
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&documentType=A81&processType=A52&businessType=B95&Type_MarketAgreement.Type=A01&controlArea_Domain=10YGB----------A&periodStart=202605060000&periodEnd=202605070000" \
  -o .tmp/entsoe-contracted_reserves.xml -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
```

Response: 200, 1000B, code 999 — `AMOUNT_AND_PRICES_PAID_OF_BALANCING_RESERVES_UNDER_CONTRACT_R3 [17.1.B&C]`.
FR fallback (older window) returns `Balancing_MarketDocument` (ZIP-of-XML)
with `type=A81`, `process.processType=A52` — shape confirmed.

## Implementation deltas

The B1 plan's mandate is to compare the connector's request tuple
`(documentType, processType, businessType, area-param-name)` against the
canonical ENTSOE Static Content API Guide PDF for each dataset. The Guide
PDF at
`https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.pdf`
returns `HTTP 400` to both `WebFetch` and `curl --ssl-no-revoke -fsSI`
(likely Akamai/CloudFront protection requiring an interactive browser
session). The Guide HTML at the same URL with `.html` suffix and a
Postman documenter URL similarly fail. Therefore the canonical-tuple side
of every comparison is recorded as **`unverified - PDF fetch failed`**;
the *code-tuple* and *live-API-accepted-shape* sides are confirmed by
the cross-area fallback calls described above.

For each dataset, the API-accepted live response is consistent with the
connector's request tuple — every fallback call returns the documented
envelope (`Publication_MarketDocument` / `GL_MarketDocument` /
`Balancing_MarketDocument`) with `<type>` matching the requested
`documentType`, `<process.processType>` matching the requested
`processType` (where applicable), and `<TimeSeries>` containing the
expected `businessType` filter.

### Specific deltas worth flagging

- **`imbalance_volume` (A86)**: connector sends `businessType=A19` but
  no `processType`. The live response carries `process.processType=A16`,
  implying the API defaults to `A16` for `A86` requests. Worth confirming
  against the canonical Guide once accessible to ensure the default is
  not vendor-dependent.

- **`load_forecast_weekly` / `_monthly` / `_yearly` (A65/A31/A32/A33)**:
  the parser's `_RESOLUTION_MAP` approximates `P1M` as 30 days, `P1Y` as
  365 days, and `P7D` as 7 days. The first two are calendar-incorrect for
  multi-period windows (month length 28-31, leap years). The current
  silver schemas treat the timestamp as the start of the represented
  interval — acceptable for weekly products, an approximation for
  monthly/yearly. **Out of V1 scope**, but flag for a parser refinement
  follow-up.

- **`load_forecast_weekly` silver dedup**: the API returns two
  `<TimeSeries>` per (zone, week) — one for the weekly minimum load
  (businessType A60) and one for the maximum (businessType A61, or
  similar). The silver schema's dedup key is `(timestamp_utc, area_code)`
  which collapses the two into one row, losing the min/max distinction.
  Same caveat applies to monthly/yearly. **Out of V1 scope**, but flag
  for a follow-up to either preserve `business_type` in the silver schema
  or split into two datasets.

- **`activated_balancing_prices` (A84/A16)** request fixed at
  `businessType=A96` (aFRR only), while the silver schema supports four
  reserve types (fcr/afrr/mfrr/rr). The connector currently fetches only
  aFRR; mFRR/RR/FCR data is silently absent. **Out of V1 scope** — flag
  for either an extra-params iteration in the connector or a doc note.

- **`contracted_reserves` (A81/A52)**: `Type_MarketAgreement.Type=A01`
  is mandatory per live API but Postman documents it as optional. The
  inline comment in `connectors/entsoe/endpoints.py` already records this
  override. No code change needed; documented in the dataset page.

## Reference Doc Fetch Failure

A single root-cause WebFetch failure affects every B1 docs-vs-code tuple
comparison. Recording it here so it's not duplicated in each dataset page:

- **URL**: `https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.pdf`
- **Outcome**: `HTTP 400` from both Anthropic `WebFetch` tool and direct
  `curl --ssl-no-revoke -fsSI`.
- **Cause** (likely): CDN-level protection requiring an interactive
  browser session to fetch static content. Not a network/DNS/TLS issue
  (other ENTSOE endpoints under the API host return 200 fine).
- **Mitigation**: each dataset page records the code-tuple and the
  live-API-accepted shape; canonical-tuple comparison marked
  `unverified - PDF fetch failed`. Recommend the user manually downloads
  the Guide PDF in a browser and stores it at
  `quant-vault/30-vendors/entsoe/api-guide.pdf` for a later validation
  pass that can compare offline.

## Conclusion

All 11 B1 datasets pass *request-shape* validation against the live
API (every connector tuple is accepted, every response envelope matches
the documented document type, every `<TimeSeries>` carries the expected
businessType filter where applicable). The blanket EMPTY classification
reflects no GB data being returned at the chosen window — most likely a
post-Brexit removal of GB from the EU data items, with publication lag
on the recent test window as a secondary candidate. **No source-code
changes are warranted by B1 validation.** For GB-equivalent data,
gridflow's Elexon connector is the canonical source. A future re-run
should retest GB against an older window before treating GB as
permanently unpublished on each data item.
