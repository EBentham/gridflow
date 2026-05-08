---
phase: V1
plan_id: V1-PLAN-B4-entsoe-balancing
batch: B4-balancing
total_datasets: 6
generated: 2026-05-08
vendor: entsoe
---

# ENTSOE B4 Balancing Extension — Live Validation Report

## Summary

| Status | Count |
|--------|-------|
| PASS   | 0 |
| EMPTY  | 6 |
| FAIL   | 0 |
| **Total** | **6** |

All 6 H8 balancing-extension endpoints respond with HTTP 200 + valid
ENTSOE `Acknowledgement_MarketDocument` (Reason.code 999 — "No matching
data found"), confirming infrastructure health (auth, URL shape,
parameter casing, tuple correctness) but indicating that **National
Grid ESO does not publish to ENTSOE for any of these data items** for
the GB control area on the validated dates. This is the expected
post-Brexit pattern — Elexon BMRS is the GB equivalent.

Pre-flight checks: PASS
- carbonintensity smoke test: HTTP 200
- ENTSOE A44 health (FR control area): HTTP 200, `Publication_MarketDocument` returned (confirms API healthy and key valid)
- ENTSOE A44 GB: HTTP 200, `Acknowledgement` reason 999 (confirms expected GB EMPTY pattern)

## Endpoint validation rows

| Dataset | DocType | ProcessType | BusinessType | Area-param-name | Domain(s) used | HTTP | Bytes | Status | Reason |
|---------|---------|-------------|--------------|-----------------|----------------|------|-------|--------|--------|
| `current_balancing_state` | A86 | n/a | B33 | `area_Domain` | `10YGB----------A` (intra-day window) | 200 | 958 | EMPTY | Reason 999 — `CURRENT_BALANCING_STATE_R3 [12.3.A] (10YGB----------A)` no data for 2026-05-07 00:00-14:00 |
| `balancing_energy_bids` | A37 | A47 | B74 | `connecting_Domain` | `10YGB----------A` | 200 | 968 | EMPTY | Reason 999 — `BALANCING_ENERGY_BIDS_R3 [GL EB 12.3.B&C] (10YGB----------A)` no data for 2026-05-07/08 |
| `aggregated_balancing_energy_bids` | A24 | A51 | n/a | `area_Domain` | `10YGB----------A` | 200 | 967 | EMPTY | Reason 999 — `AGGREGATED_BALANCING_ENERGY_BIDS_R3 [12.3.E] (10YGB----------A)` no data for 2026-05-07/08 |
| `procured_balancing_capacity` | A15 | A51 | n/a | `area_Domain` | `10YGB----------A` | 200 | 962 | EMPTY | Reason 999 — `PROCURED_BALANCING_CAPACITY_R3 [12.3.F] (10YGB----------A)` no data for 2026-05-07/08 |
| `cross_zonal_balancing_capacity` | A38 | A51 | n/a | `Acquiring_Domain` + `Connecting_Domain` | `10YGB----------A` → `10YFR-RTE------C` | 200 | 1001 | EMPTY | Reason 999 — `ALLOCATION_AND_USE_CROSS_ZONAL_CAPACITY [GL EB 12.3.H&I] (10YGB→FR)` no data for 2026-05-07/08 |
| `balancing_financial_expenses_income` | A87 | n/a | n/a | `controlArea_Domain` | `10YGB----------A` | 200 | 978 | EMPTY | Reason 999 — `FINANCIAL_EXPENSES_AND_INCOME_FOR_BALANCING_R3 [17.1.I] (10YGB----------A)` no data for 2026-05-07/08 |

## Curl evidence

All calls used:
- Base: `https://web-api.tp.entsoe.eu/api`
- Auth: query param `securityToken=$ENTSOE_API_KEY` (UUID, 36 chars)
- Throttle: `sleep 1` between calls (1 req/s)
- TLS: `--ssl-no-revoke` (Avast workstation quirk)

Raw responses captured at `.tmp/entsoe-<dataset_key>.xml`. Stderr at
`.tmp/entsoe-<dataset_key>.err`.

### A86 — current_balancing_state

```bash
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?documentType=A86&businessType=B33&area_Domain=10YGB----------A&periodStart=202605070000&periodEnd=202605071400&securityToken=$ENTSOE_API_KEY" \
  -o .tmp/entsoe-current_balancing_state.xml \
  -w "HTTP %{http_code} | %{size_download} bytes | %{time_total}s\n"
# HTTP 200 | 958 bytes | 0.232s
```

### A37 — balancing_energy_bids

```bash
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?documentType=A37&processType=A47&businessType=B74&offset=0&connecting_Domain=10YGB----------A&periodStart=202605070000&periodEnd=202605080000&securityToken=$ENTSOE_API_KEY" \
  -o .tmp/entsoe-balancing_energy_bids.xml \
  -w "HTTP %{http_code} | %{size_download} bytes | %{time_total}s\n"
# HTTP 200 | 968 bytes | 0.264s
```

### A24 — aggregated_balancing_energy_bids

```bash
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?documentType=A24&processType=A51&area_Domain=10YGB----------A&periodStart=202605070000&periodEnd=202605080000&securityToken=$ENTSOE_API_KEY" \
  -o .tmp/entsoe-aggregated_balancing_energy_bids.xml \
  -w "HTTP %{http_code} | %{size_download} bytes | %{time_total}s\n"
# HTTP 200 | 967 bytes | 0.286s
```

### A15 — procured_balancing_capacity

```bash
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?documentType=A15&processType=A51&offset=0&area_Domain=10YGB----------A&periodStart=202605070000&periodEnd=202605080000&securityToken=$ENTSOE_API_KEY" \
  -o .tmp/entsoe-procured_balancing_capacity.xml \
  -w "HTTP %{http_code} | %{size_download} bytes | %{time_total}s\n"
# HTTP 200 | 962 bytes | 0.193s
```

### A38 — cross_zonal_balancing_capacity (GB→FR)

```bash
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?documentType=A38&processType=A51&Acquiring_Domain=10YGB----------A&Connecting_Domain=10YFR-RTE------C&periodStart=202605070000&periodEnd=202605080000&securityToken=$ENTSOE_API_KEY" \
  -o .tmp/entsoe-cross_zonal_balancing_capacity.xml \
  -w "HTTP %{http_code} | %{size_download} bytes | %{time_total}s\n"
# HTTP 200 | 1001 bytes | 0.139s
```

### A87 — balancing_financial_expenses_income

```bash
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?documentType=A87&controlArea_Domain=10YGB----------A&periodStart=202605070000&periodEnd=202605080000&securityToken=$ENTSOE_API_KEY" \
  -o .tmp/entsoe-balancing_financial_expenses_income.xml \
  -w "HTTP %{http_code} | %{size_download} bytes | %{time_total}s\n"
# HTTP 200 | 978 bytes | 0.287s
```

## Implementation delta — cross-cutting

These items affect multiple B4 datasets and are recorded both in
individual dataset pages and consolidated here:

1. **Area-parameter-name mismatch with orchestrator instructions.** The
   B4 orchestrator instructions stated A86/A24/A15/A38 use
   `controlArea_Domain`. The actual H8 spec, the gridflow code, and the
   ENTSOE responses confirm:
   - A86 (`current_balancing_state`) uses **`area_Domain`** (not
     `controlArea_Domain`).
   - A24 (`aggregated_balancing_energy_bids`) uses **`area_Domain`**.
   - A15 (`procured_balancing_capacity`) uses **`area_Domain`**.
   - A37 (`balancing_energy_bids`) uses **`connecting_Domain`**.
   - A38 (`cross_zonal_balancing_capacity`) uses **`Acquiring_Domain`
     + `Connecting_Domain`** (two-domain).
   - A87 (`balancing_financial_expenses_income`) is the **only** one
     using `controlArea_Domain` (legacy `Publication_MarketDocument`
     lineage).

   The code in `connectors/entsoe/endpoints.py` is correct; the
   orchestrator instruction was wrong. No code change needed.

2. **A86 dual mapping.** Documented as a hard cross-link requirement.
   `current_balancing_state.md` cross-links to `imbalance_volume.md`
   in its `## Implementation delta` section. Both share documentType
   A86 but differ by `businessType` (B33 vs A19).

3. **A87 uses Reason.code semantic classification.** Per ENTSOE API
   guide §17.1.I, A87 documents are classified via `Reason.code`
   rather than typed time series. The dataset page documents this and
   notes that the silver transformer currently surfaces `<businessType>`
   per TimeSeries (granular, present in fixture) rather than the
   document-level `Reason.code` (currently not exposed to silver). For
   modelling work this is sufficient.

4. **Pagination not iterated.** Both A37 and A15 have `offset=0`
   hardcoded as an `extra_param` in `endpoints.py`. For high-cardinality
   areas this would silently truncate results at 4800 TimeSeries. Out
   of scope for V1 — track as a connector-improvement follow-up.

5. **A87 schedule cadence over-zealous.** `config/sources.yaml`
   registers A87 as `schedule: daily, max_query_days: 1`. Real
   publication cadence is monthly. Out of scope for V1.

6. **GB has no data on any H8 balancing endpoint.** The recurring
   reason-999 pattern is consistent with NGESO not publishing to
   ENTSOE post-Brexit; equivalent data is on Elexon BMRS.

## Cross-link verification

| Check | Result |
|-------|--------|
| 6 dataset pages exist at `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\datasets\` | PASS |
| `current_balancing_state.md` contains `imbalance_volume.md` link | PASS — multiple references in Overview, Known issues, Implementation delta, and Links sections |
| ENTSOE tuple recorded in every page's `## Implementation delta` or `### Query parameters` | PASS — recorded in dataset-pages' Query-params section under "ENTSOE tuple:" |

## Out-of-scope deliverables (B5 ownership)

The following are owned by `V1-PLAN-B5-entsoe-aggregate.md`, not B4:

- ENTSOE-wide `README.md` updates
- ENTSOE-wide `endpoints.md` table refresh
- Consolidated `entsoe-VALIDATION.md` (across B1/B2/B3/B4)

B4 has not modified any of those.
