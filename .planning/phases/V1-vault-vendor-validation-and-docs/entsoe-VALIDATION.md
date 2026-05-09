---
phase: V1
vendor: entsoe
validated: 2026-05-08
total_datasets: 48
batches: [B1-load-prices, B2-generation-outages, B3-transmission-capacity, B4-balancing]
---

# ENTSO-E — V1 Validation Report (Consolidated)

Consolidates the four wave-1 ENTSO-E batches (B1, B2, B3, B4) executed against
the live ENTSO-E Transparency Platform on 2026-05-08. All calls used `curl
--ssl-no-revoke -fsS` against `https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&...`.

## Summary (all batches)

| Status | Count |
|--------|-------|
| PASS   | 9     |
| EMPTY  | 39    |
| FAIL   | 0     |
| Total  | 48    |

Every connector tuple `(documentType, processType, businessType,
area-param-name)` is accepted by the live API. The 39 EMPTY classifications
reflect *publication absences* (post-Brexit GB withdrawal from EU data items;
sparse cadence on some capacity / financial datasets) rather than request-shape
defects. No code changes are warranted by V1 validation.

## Per-batch summaries

| Batch | Total | PASS | EMPTY | FAIL | Source file |
|-------|-------|------|-------|------|-------------|
| B1 (load + prices) | 11 | 0 | 11 | 0 | [entsoe-B1-VALIDATION.md](./entsoe-B1-VALIDATION.md) |
| B2 (generation + outages) | 13 | 4 | 9 | 0 | [entsoe-B2-VALIDATION.md](./entsoe-B2-VALIDATION.md) |
| B3 (transmission + capacity) | 18 | 5 | 13 | 0 | [entsoe-B3-VALIDATION.md](./entsoe-B3-VALIDATION.md) |
| B4 (balancing) | 6 | 0 | 6 | 0 | [entsoe-B4-VALIDATION.md](./entsoe-B4-VALIDATION.md) |
| **All ENTSOE** | **48** | **9** | **39** | **0** | — |

## Per-dataset results (consolidated, sorted by family)

### B1 — Load + Prices + Imbalance (11 datasets)

| Dataset | docType | procType | businessType | Domain param | Status | HTTP | Bytes | Cause |
|---------|---------|----------|--------------|--------------|--------|------|-------|-------|
| day_ahead_prices | A44 | — | — | in_Domain + out_Domain | EMPTY | 200 | 963 | GB post-Brexit — `ENERGY_PRICES [12.1.D]` not published |
| actual_load | A65 | A16 | — | outBiddingZone_Domain | EMPTY | 200 | 951 | GB post-Brexit — `ACTUAL_TOTAL_LOAD_R3 [6.1.A]` not published |
| load_forecast | A65 | A01 | — | outBiddingZone_Domain | EMPTY | 200 | 963 | GB post-Brexit — `DAY_AHEAD_TOTAL_LOAD_FORECAST_R3 [6.1.B]` not published |
| load_forecast_weekly | A65 | A31 | — | outBiddingZone_Domain | EMPTY | 200 | 962 | GB post-Brexit — `TOTAL_LOAD_FORECAST [6.1.C&D&E]` not published |
| load_forecast_monthly | A65 | A32 | — | outBiddingZone_Domain | EMPTY | 200 | 962 | GB post-Brexit — `TOTAL_LOAD_FORECAST [6.1.C&D&E]` not published |
| load_forecast_yearly | A65 | A33 | — | outBiddingZone_Domain | EMPTY | 200 | 962 | GB post-Brexit — `TOTAL_LOAD_FORECAST [6.1.C&D&E]` not published |
| forecast_margin | A70 | A33 | — | outBiddingZone_Domain | EMPTY | 200 | 958 | GB post-Brexit — `YEAR_AHEAD_FORECAST_MARGIN_R3 [8.1]` not published |
| imbalance_prices | A85 | — | — | controlArea_Domain | EMPTY | 200 | 951 | GB post-Brexit — `IMBALANCE_PRICES_R3 [17.1.G]` not published |
| imbalance_volume | A86 | — | A19 | controlArea_Domain | EMPTY | 200 | 958 | GB post-Brexit — `TOTAL_IMBALANCE_VOLUMES_R3 [17.1.H]` not published |
| activated_balancing_prices | A84 | A16 | A96 | controlArea_Domain | EMPTY | 200 | 988 | GB post-Brexit — `PRICES_OF_ACTIVATED_BALANCING_ENERGY_R3 [TR 17.1.F, IF aFRR 3.16]` not published |
| contracted_reserves | A81 | A52 | B95 | controlArea_Domain | EMPTY | 200 | 1000 | GB post-Brexit — `AMOUNT_AND_PRICES_PAID_OF_BALANCING_RESERVES_UNDER_CONTRACT_R3 [17.1.B&C]` not published |

All B1 request shapes verified against live API via DE-LU / FR / NL fallbacks
(see entsoe-B1-VALIDATION.md for full evidence).
`contracted_reserves` also requires `Type_MarketAgreement.Type=A01`.

### B2 — Generation + Outages (13 datasets)

| Dataset | docType | procType | businessType | Domain param | Status | HTTP | Bytes | Cause / notes |
|---------|---------|----------|--------------|--------------|--------|------|-------|---------------|
| actual_generation | A75 | A16 | — | in_Domain | EMPTY | 200 | 971 | GB Brexit — DE-LU PASS confirms shape |
| wind_solar_forecast | A69 | A01 | — | in_Domain | EMPTY | 200 | 962 | GB Brexit — DE-LU PASS |
| generation_forecast | A71 | A01 | — | in_Domain | EMPTY | 200 | 966 | GB Brexit — DE-LU PASS |
| actual_generation_units | A73 | A16 | — | in_Domain | EMPTY | 200 | 968 | GB + DE-LU also empty (per-unit data not published continent-wide) |
| water_reservoirs | A72 | A16 | — | in_Domain | EMPTY | 200 | 977 | GB has no hydro reservoirs; NO-1 30-day window PASS confirms shape |
| installed_capacity | A68 | A33 | — | in_Domain | EMPTY | 200 | 975 | GB Brexit — DE-LU yearly PASS |
| installed_capacity_units | A71 | A33 | — | in_Domain | PASS | 200 | 366 199 | 230 TimeSeries — GB unit registry remains published |
| generation_units_master_data | A95 | — | B11 | BiddingZone_Domain | PASS | 200 | 430 018 | 230 TimeSeries — GB master data registry; uses single-date `Implementation_DateAndOrTime` |
| outages_generation | A80 | — | A53 | BiddingZone_Domain | PASS | 200 | 40 528 | ZIP archive, 17 outage XMLs over 30-day GB window |
| outages_consumption | A76 | — | A53 | BiddingZone_Domain | EMPTY | 200 | 984 | GB Brexit — DE-LU PASS |
| outages_transmission | A78 | — | A53 | In_Domain + Out_Domain | PASS | 200 | 7 588 | ZIP archive, 7 outage XMLs over 30-day GB→FR window. Capital-I param names |
| outages_offshore_grid | A79 | — | — | BiddingZone_Domain | EMPTY | 200 | 963 | Structurally sparse — also EMPTY for BE and NL |
| outages_production | A77 | — | A53 | BiddingZone_Domain | EMPTY | 200 | 1 005 | GB Brexit — DE-LU 1-day PASS confirms shape (30d returned 400 over 200-record cap) |

### B3 — Transmission + Capacity Allocation (18 datasets)

| Dataset | docType | procType | businessType | Domain param | Status | Bytes | TS | Notes |
|---------|---------|----------|--------------|--------------|--------|-------|----|-------|
| cross_border_flows | A11 | — | — | in_Domain + out_Domain | PASS | 4 340 | 1 | GB→FR 24h hourly flows |
| net_transfer_capacity | A61 | — | — | in_Domain + out_Domain | PASS | 1 580 | 1 | GB→FR single-point 3028 MW. Requires `contract_MarketAgreement.Type=A01` |
| dc_link_intraday_transfer_limits | A93 | — | — | in_Domain + out_Domain | EMPTY | 984 | 0 | A93 only publishes on revision events |
| commercial_schedules | A09 | — | — | in_Domain + out_Domain | PASS | 5 296 | 2 | GB→FR with `<contract_MarketAgreement.type>A01</...>` |
| commercial_schedules_net_positions | A09 | — | — | in_Domain + out_Domain | PASS | 5 296 | 2 | Identical query/payload to commercial_schedules — silver-label distinction only |
| redispatching_cross_border | A63 | — | A46 | in_Domain + out_Domain | EMPTY | 979 | 0 | Border has zero events in window; sparse cadence |
| redispatching_internal | A63 | — | A85 | in_Domain + out_Domain | EMPTY | 957 | 0 | Internal events sparse |
| countertrading | A91 | — | — | in_Domain + out_Domain | EMPTY | 967 | 0 | Countertrading events rare |
| congestion_management_costs | A92 | — | — | in_Domain (single-zone) | EMPTY | 965 | 0 | A92 is single-zone; weekly/monthly cadence |
| offered_transfer_capacity_continuous | A31 | — | — | In_Domain + Out_Domain (Capital) | EMPTY | 984 | 0 | GB outside continuous EU auctions post-Brexit; also requires `Auction.Type=A01` + `Contract_MarketAgreement.Type=A01` (Capital) |
| offered_transfer_capacity_implicit | A31 | — | — | in_Domain + out_Domain (lowercase) | EMPTY | 984 | 0 | GB not in SDAC; also requires `auction.Type=A01` + `contract_MarketAgreement.Type=A01` (lowercase) |
| offered_transfer_capacity_explicit | A31 | — | — | in_Domain + out_Domain (lowercase) | EMPTY | 984 | 0 | Same as implicit + `auction.Category=A01` |
| auction_revenue | A25 | — | B07 | in_Domain + out_Domain | EMPTY | 965 | 0 | Sparser than daily; requires `contract_MarketAgreement.Type=A01` |
| transfer_capacity_use | A25 | — | B05 | in_Domain + out_Domain | EMPTY | 974 | 0 | Sparser than daily; requires `Auction.Category=A01` (Capital) + `contract_MarketAgreement.Type=A01` (lowercase) |
| total_nominated_capacity | A26 | — | B08 | in_Domain + out_Domain | PASS | 5 505 | 3 | GB→FR 3 TS, 2029-3028 MW across 24h |
| total_capacity_allocated | A26 | — | A29 | in_Domain + out_Domain | EMPTY | 974 | 0 | Post-Brexit GB outside long-term EU allocation; requires `auction.Category=A01` + `contract_MarketAgreement.Type=A01` (lowercase) |
| congestion_income | A25 | — | B10 | in_Domain + out_Domain | EMPTY | 983 | 0 | GB not in flow-based / implicit coupling post-Brexit |
| net_positions | A25 | — | B09 | in_Domain only (single-zone) | EMPTY | 966 | 0 | GB not in SDAC; only A25 variant with `domain_style=zone` |

### B4 — Balancing extension (6 datasets)

| Dataset | docType | procType | businessType | Area-param-name | Status | HTTP | Bytes | Reason |
|---------|---------|----------|--------------|-----------------|--------|------|-------|--------|
| current_balancing_state | A86 | — | B33 | area_Domain | EMPTY | 200 | 958 | `CURRENT_BALANCING_STATE_R3 [12.3.A]` (10YGB----------A) — GB Brexit |
| balancing_energy_bids | A37 | A47 | B74 | connecting_Domain | EMPTY | 200 | 968 | `BALANCING_ENERGY_BIDS_R3 [GL EB 12.3.B&C]` — GB Brexit |
| aggregated_balancing_energy_bids | A24 | A51 | — | area_Domain | EMPTY | 200 | 967 | `AGGREGATED_BALANCING_ENERGY_BIDS_R3 [12.3.E]` — GB Brexit |
| procured_balancing_capacity | A15 | A51 | — | area_Domain | EMPTY | 200 | 962 | `PROCURED_BALANCING_CAPACITY_R3 [12.3.F]` — GB Brexit |
| cross_zonal_balancing_capacity | A38 | A51 | — | Acquiring_Domain + Connecting_Domain | EMPTY | 200 | 1 001 | `ALLOCATION_AND_USE_CROSS_ZONAL_CAPACITY [GL EB 12.3.H&I]` (GB→FR) — GB Brexit |
| balancing_financial_expenses_income | A87 | — | — | controlArea_Domain | EMPTY | 200 | 978 | `FINANCIAL_EXPENSES_AND_INCOME_FOR_BALANCING_R3 [17.1.I]` — GB Brexit |

## Cross-batch implementation deltas

These are vendor-level observations spanning more than one batch; per-batch
deltas are recorded in each batch report. V1 is documentation-and-validation
only — no source code changed.

### 1. Reference doc fetch failure (vendor-wide)

The canonical ENTSO-E Static Content API Guide PDF at
`https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.pdf`
returns **HTTP 400** to both the Anthropic `WebFetch` tool and direct
`curl --ssl-no-revoke -fsSI`. Likely cause: CDN-level (Akamai/CloudFront)
protection requiring an interactive browser session. The canonical-tuple
side of every B1/B2/B3/B4 docs-vs-code comparison is therefore recorded as
`unverified — PDF fetch failed`. Code-tuple and live-API-accepted-shape
sides are confirmed via the cross-area fallback calls described in each
batch report. **Recommendation**: manual user download in a browser, store
at `quant-vault/30-vendors/entsoe/api-guide.pdf` for offline comparison
in a later validation pass.

### 2. A25 four-way semantic split by `(businessType, domain_style)`

`documentType=A25` multiplexes four logically distinct datasets that share
the same docType. Disambiguation requires the full
`(documentType, processType, businessType, area-param-name)` tuple — the
locked V1 validation criterion — plus discriminator parameters:

| dataset_key | businessType | extra discriminator | domain_style | unit |
|-------------|--------------|---------------------|--------------|------|
| `auction_revenue` | B07 | `contract_MarketAgreement.Type=A01` | zone_pair | EUR |
| `transfer_capacity_use` | B05 | `Auction.Category=A01` (Capital) + `contract_MarketAgreement.Type=A01` (lowercase) | zone_pair | MW |
| `congestion_income` | B10 | `contract_MarketAgreement.Type=A01` | zone_pair | EUR |
| `net_positions` | B09 | `contract_MarketAgreement.Type=A01` | **zone (single-domain)** | MW |

`net_positions` is the only A25 variant with `domain_style=zone`. ENTSO-E
returns the same data-item label `IMPL_ALLOC_*` etc. in 999 reasons across
all four — the publishing side does treat them as separate datasets despite
the shared docType.

### 3. A26 split by `(businessType, extra_params)`

| dataset_key | businessType | extra discriminator |
|-------------|--------------|---------------------|
| `total_nominated_capacity` | B08 | (none — no `auction.Category`, no contract type in request) |
| `total_capacity_allocated` | A29 | `auction.Category=A01` + `contract_MarketAgreement.Type=A01` (both lowercase) |

### 4. A31 casing pattern (parameter-name capitalisation as discriminator)

The three A31 variants use **distinct parameter-name casings** as the API's
disambiguation device. ENTSO-E responds to all three with the same
`OFFERED_TRANSFER_CAPACITIES_IMPLICIT [11.1]` data-item label on EMPTY,
which is misleading — the publishing side does treat them as separate
datasets:

| dataset_key | Auction casing | Contract casing | auction.Category? |
|-------------|----------------|-----------------|--------------------|
| `_continuous` | `Auction.Type` (Capital A) | `Contract_MarketAgreement.Type` (Capital C) | no |
| `_implicit` | `auction.Type` (lowercase a) | `contract_MarketAgreement.Type` (lowercase c) | no |
| `_explicit` | `auction.Type` (lowercase a) | `contract_MarketAgreement.Type` (lowercase c) | **yes** `auction.Category=A01` |

Cross-checked against `connectors/entsoe/endpoints.py:184-226`. The connector
encodes the casing exactly.

### 5. A86 reused across B1 and B4 (different businessType + area-param)

| dataset_key | batch | businessType | area-param |
|-------------|-------|--------------|------------|
| `imbalance_volume` | B1 | A19 | `controlArea_Domain` |
| `current_balancing_state` | B4 | B33 | `area_Domain` |

Same `documentType=A86` but the two are logically and parametrically
distinct. The B4 dataset page `current_balancing_state.md` cross-links to
`imbalance_volume.md` to surface this.

### 6. A09 dual-keying — `commercial_schedules` and `commercial_schedules_net_positions`

Both datasets use **identical** `EntsoeDocType("A09", None, ..., zone_pair,
optional_params=("contract_MarketAgreement.Type",))` and return the
**same XML payload** (5296 bytes, 2 TS) for the same request. The dataset
key distinction is silver-transformer label only; no semantic difference at
bronze. **Registry duplication candidate**: drop one key or have the
net-positions transformer pair directions and emit a true `net_position_mw`
column. Out of V1 scope.

### 7. A71 documentType collision (B2)

`installed_capacity_units` (A71/A33) and `generation_forecast` (A71/A01)
share the same `documentType=A71`. Disambiguation is by `processType`. Be
careful in any cross-reference that uses `documentType` alone as a key.

### 8. B4 area-parameter-name mappings (corrects the B4 plan brief)

The B4 plan brief mis-stated several area-parameter names; the actual H8
spec, the gridflow code, and the live API confirm:

| docType | dataset | area-param-name |
|---------|---------|-----------------|
| A86 | `current_balancing_state` | `area_Domain` |
| A24 | `aggregated_balancing_energy_bids` | `area_Domain` |
| A15 | `procured_balancing_capacity` | `area_Domain` |
| A37 | `balancing_energy_bids` | `connecting_Domain` |
| A38 | `cross_zonal_balancing_capacity` | `Acquiring_Domain` + `Connecting_Domain` (two-domain) |
| A87 | `balancing_financial_expenses_income` | `controlArea_Domain` (legacy `Publication_MarketDocument` lineage) |

A87 is the only B4 dataset using `controlArea_Domain`. Code in
`connectors/entsoe/endpoints.py` is correct as-is; the orchestrator
instruction was wrong. No code change needed.

### 9. Outage-domain capitalisation split (B2)

Most outage datasets use lowercase `BiddingZone_Domain` (A80, A76, A77, A79).
**A78 `outages_transmission` is the exception** — uses capital-I `In_Domain`
/ `Out_Domain` (distinct from the prices/flows lowercase `in_Domain` /
`out_Domain`). Code handles this via explicit
`domain_params=("In_Domain", "Out_Domain")`. New contributors must not
assume the lowercase form is universal across ENTSO-E.

### 10. A95 single-date parameter (B2)

`generation_units_master_data` is the only ENTSO-E dataset that uses
`Implementation_DateAndOrTime` (single ISO date) instead of
`periodStart`/`periodEnd`. The `EntsoeDocType.date_param` mechanism in
`endpoints.py` (line 117) is the canonical pattern; future non-period
parameters must reuse it rather than hard-coding around it.

### 11. Pagination not iterated (B4)

Both A37 (`balancing_energy_bids`) and A15 (`procured_balancing_capacity`)
have `offset=0` hardcoded as an `extra_param` in `endpoints.py`. For
high-cardinality areas this would silently truncate results at 4800
TimeSeries. Out of V1 scope — track as a connector-improvement follow-up.

### 12. Universal GB-EMPTY pattern (post-Brexit)

39 of the 48 datasets returned EMPTY with reason 999 for the GB
control area `10YGB----------A`. This is a structural Brexit consequence
(GB withdrew from EU operational data items) rather than a code defect.
For GB-equivalent data, gridflow's Elexon connector is the canonical
source. Datasets that **remain published** for GB:

- `installed_capacity_units` (A71/A33) — network-code obligation
- `generation_units_master_data` (A95/B11) — network-code obligation
- `outages_generation` (A80/A53) — REMIT obligation, GB participates
- `outages_transmission` (A78/A53) — cross-border GB↔EU outages
- `cross_border_flows` (A11) — cross-border physical flows GB↔EU
- `net_transfer_capacity` (A61) — cross-border NTC GB↔EU
- `commercial_schedules` (A09) and `commercial_schedules_net_positions`
  (A09) — cross-border commercial schedules GB↔EU
- `total_nominated_capacity` (A26/B08) — cross-border long-term nominations

### 13. Pydantic schema vs silver Parquet column drift (B3)

`EntsoeCrossborderFlow` and `EntsoeNetTransferCapacity` schemas declare a
narrower set of fields than their transformers write to the silver
Parquet (`resolution`, `ingested_at`). The schema is permissive enough
that this does not raise; benign mismatch. Logged for awareness.

## Recommendations

1. **Manual download of ENTSO-E API Guide PDF** (highest priority). The
   PDF at
   `https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.pdf`
   is unfetchable programmatically (HTTP 400 from CDN). Recommend the user
   download in a browser and store at
   `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\api-guide.pdf`
   so a later validation pass can compare `(documentType, processType,
   businessType, area-param-name)` tuples offline.

2. **Re-test GB on an older window**. The 39 EMPTY classifications on GB are
   plausibly a post-Brexit removal of GB from EU data items (current
   hypothesis), but a rigorous confirmation requires retesting GB on a
   pre-Brexit window (e.g. 2019-Q4 or 2020-Q4) to distinguish "permanently
   not published" from "publication-lag at recent window". Out of V1 scope
   — track as a follow-up.

3. **Backlog candidate: A09 registry deduplication**. `commercial_schedules`
   and `commercial_schedules_net_positions` share an identical
   EntsoeDocType. Either drop one key or rewrite the
   `net_positions`-flavoured transformer to derive net positions
   (per-direction pairing → signed `net_position_mw`).

4. **Backlog candidate: A37/A15 pagination iteration**. Hardcoded `offset=0`
   in `endpoints.py` would silently truncate at 4800 TimeSeries on
   high-cardinality areas.

5. **Backlog candidate: parser resolution refinement**. The
   `_RESOLUTION_MAP` approximates `P1M` as 30 days and `P1Y` as 365 days,
   which is calendar-incorrect for multi-period windows.
   `load_forecast_monthly` and `load_forecast_yearly` are mildly affected.

6. **Backlog candidate: `activated_balancing_prices` reserve-type coverage**.
   Connector currently fixes `businessType=A96` (aFRR only); silver schema
   supports four reserve types (FCR/aFRR/mFRR/RR). mFRR/RR/FCR data is
   silently absent — extend the connector or document the limitation.

7. **Backlog candidate: A87 schedule cadence**. `config/sources.yaml`
   registers A87 (`balancing_financial_expenses_income`) as `schedule:
   daily, max_query_days: 1`. Real publication cadence is monthly. Adjust
   the schedule to avoid wasted live calls.

8. **Backlog candidate: GB→Elexon cross-reference table in vendor README**.
   Document the Elexon-equivalent dataset for each EMPTY GB ENTSO-E
   dataset (e.g. A75 → Elexon `fuelhh`, A69 → Elexon `windfor`, A76/A77/A80
   → Elexon `remit`). This is currently scattered across batch reports;
   consolidate at the vendor level for downstream consumers.

---

## V2 re-validation (2026-05-09)

**Fix commit:** `fix(V2-D): ENTSOE A09 dedup + B2 cleanup batch`
(SHA recorded by V2-PLAN-F aggregate close-out).

### V2-FIX-05: A09 commercial_schedules registry dedup (ADR-019)

**Disposition: Option A — drop key.** Per ADR-019
(`docs/DECISION_LOG/ADR-019-entsoe-a09-dedup.md`),
`commercial_schedules_net_positions` was removed from
`connectors/entsoe/endpoints.py::ENDPOINTS`, `config/sources.yaml`,
`silver/entsoe/h6_market.py` (`CommercialSchedulesNetPositionsTransformer`
class + `_TRANSFORMERS` list entry), `silver/entsoe/__init__.py`
(import + `__all__`), `docs/entsoe_endpoint_catalog.yaml` (status
`implemented` → `deferred` + reason), and the affected test
parametrize lists.

ENTSOE active dataset count drops from 48 → 47.

**No regression on the kept dataset.** Live re-validation 2026-05-09
of `commercial_schedules` GB→FR with `contract_MarketAgreement.Type=A01`:

```bash
curl --ssl-no-revoke -fsS \
  -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&documentType=A09&in_Domain=10YGB----------A&out_Domain=10YFR-RTE------C&periodStart=202605060000&periodEnd=202605070000&contract_MarketAgreement.Type=A01"
# → HTTP 200, 3078 bytes (V1 had 5296 bytes — different period, smaller
#   number of TimeSeries today; request shape unchanged)
```

A09 Option B (derive `net_position_mw`) recorded as a backlog item
for when a downstream gold consumer needs net positions.

### V2-FIX-06: B2 cleanup batch — partial (per the plan's MED+LOW disposition rule)

| Sub-item | Disposition | Notes |
|----------|-------------|-------|
| 5a — A37/A15 hardcoded `offset=0` pagination | **Backlog** | Non-trivial: connector-level offset iteration loop + bronze chunk aggregation. No GB data currently published for these endpoints (V1 returned EMPTY for all GB calls), so practical impact today is minimal. Deferred to a follow-up phase that can build proper pagination + add a respx-mocked test simulating the 4800-TS boundary. |
| 5b — A87 schedule cadence | **DONE** | `config/sources.yaml` `entsoe.balancing_financial_expenses_income` now `schedule: monthly, max_query_days: 31`. |
| 5c — A87 silver `Reason.code` exposure | **Backlog** | Requires base-class refactor of `_H8BalancingTransformer` to extract `<Reason><code>` from MarketDocument header + new `reason_code` schema column + new fixture-backed test. Deferred. |
| 5d — `area_name` field declared but unpopulated | **Backlog** | `EntsoeActualGeneration.area_name: str = ""` defaults to empty. A clean fix needs either a new area_code → name lookup table (preferred) or schema removal. Defer to backlog because no current gold consumer has flagged this as missing. |
| 5e — `psrType` in `optional_params` | **DONE** | Added to `actual_generation` (A75/A16), `wind_solar_forecast` (A69/A01), `outages_generation` (A80/A53), `outages_production` (A77/A53). |
| 5f — `DEFAULT_ZONES` review | **No change** | Current value `["GB", "FR", "NL", "BE", "DE-LU", "IE-SEM"]` already covers six GB-relevant zones. The "GB/EU-centric" framing was directional; no specific omission identified. Backlog row added for a wider EU baseline if a multi-region gold consumer materialises. |

### Regression tests

- `tests/unit/test_entsoe.py::TestV2BCleanup` — 4 new tests pinning
  the A09 dedup, psrType additions, and A87 monthly cadence.
- Pre-existing `tests/unit/test_entsoe_endpoint_catalog.py::test_implemented_catalog_entries_match_active_doc_types`
  — passes after adjusting `docs/entsoe_endpoint_catalog.yaml` status
  for the dropped key from `implemented` to `deferred`.
- `tests/integration/test_entsoe_mocked_e2e.py` — `commercial_schedules_net_positions`
  removed from `ZONE_PAIR_DATASETS` set and from the parametrize list
  for `test_h6_quantity_transformer`.
- `tests/unit/test_entsoe.py::TestPhaseH6Endpoints::test_h6_doc_types_populated`
  — list updated to drop `commercial_schedules_net_positions`.
- Full fast suite: `1040 passed, 251 deselected`
  (`uv run pytest -m "not live and not slow" -x -q`).
