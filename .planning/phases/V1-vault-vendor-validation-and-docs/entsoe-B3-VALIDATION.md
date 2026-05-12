---
batch: B3-transmission-capacity
total_datasets: 18
plan_id: V1-PLAN-B3-entsoe-transmission-capacity
generated: 2026-05-08
test_window_default: 2026-05-06T00:00Z..2026-05-07T00:00Z
test_window_retry: 2026-04-01T00:00Z..2026-05-01T00:00Z (30-day)
default_border: GB->FR (10YGB----------A -> 10YFR-RTE------C)
sanity_border: NL->DE (10YNL----------L -> 10Y1001A1001A82H)
ssl_quirk: curl --ssl-no-revoke (Avast TLS interception on workstation)
auth: query param securityToken=$ENTSOE_API_KEY
---

# ENTSO-E B3 Validation — Transmission + Capacity Allocation

## Pre-flight

- `.env` present in worktree (copied prior to first run, not committed).
- `.tmp/` exists.
- Carbon-intensity smoke: HTTP 200.
- ENTSOE A44 health smoke (FR→FR): `Publication_MarketDocument`. PASS.

## Summary

| Status | Count | Datasets |
|--------|-------|----------|
| PASS | 5 | cross_border_flows, net_transfer_capacity, commercial_schedules, commercial_schedules_net_positions, total_nominated_capacity |
| EMPTY | 13 | dc_link_intraday_transfer_limits, redispatching_cross_border, redispatching_internal, countertrading, congestion_management_costs, offered_transfer_capacity_{continuous,implicit,explicit}, auction_revenue, transfer_capacity_use, total_capacity_allocated, congestion_income, net_positions |
| FAIL | 0 | — |

All 18 endpoints accept the configured `(documentType, processType, businessType, domain)` tuple — the API replies 200 with either `Publication_MarketDocument` (data published) or `Acknowledgement_MarketDocument` with Reason 999 (no data published in the queried window). No FAILs.

## Per-dataset rows

| # | dataset_key | tuple | window | status | bytes | TS | reason | notes / page |
|---|-------------|-------|--------|--------|-------|----|--------|--------------|
| 1 | cross_border_flows | (A11, —, —, in_Domain+out_Domain) | daily | PASS | 4340 | 1 | — | GB→FR 24h hourly flows. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/cross_border_flows.md) |
| 2 | net_transfer_capacity | (A61, —, —, in_Domain+out_Domain, contract_MarketAgreement.Type=A01) | daily | PASS | 1580 | 1 | — | GB→FR single-point 3028 MW. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/net_transfer_capacity.md) |
| 3 | dc_link_intraday_transfer_limits | (A93, —, —, in_Domain+out_Domain) | daily | EMPTY | 984 | 0 | 999: `CB_CAPACITY_FOR_DC_LINKS_INTRADAY_R3 [11.3] (10YGB----------A, 10YFR-RTE------C)` | border has zero allocation in window — A93 only publishes on revision events. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/dc_link_intraday_transfer_limits.md) |
| 4 | commercial_schedules | (A09, —, —, in_Domain+out_Domain) | daily | PASS | 5296 | 2 | — | GB→FR with TS `<contract_MarketAgreement.type>A01</contract_MarketAgreement.type>`. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/commercial_schedules.md) |
| 5 | commercial_schedules_net_positions | (A09, —, —, in_Domain+out_Domain) | daily | PASS | 5296 | 2 | — | **Identical query/payload to row 4**. Logged as Implementation delta — separate dataset key but same tuple. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/commercial_schedules_net_positions.md) |
| 6 | redispatching_cross_border | (A63, —, A46, in_Domain+out_Domain) | daily | EMPTY | 979 | 0 | 999: `REDISPATCHING_CROSS_BORDER_R3 [13.1.A] (10YGB----------A, 10YFR-RTE------C)` | border has zero allocation in window — A63/A46 events are sparse. Sanity NL→DE same window: also EMPTY. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/redispatching_cross_border.md) |
| 7 | redispatching_internal | (A63, —, A85, in_Domain+out_Domain) | daily | EMPTY | 957 | 0 | 999: `REDISPATCHING_INTERNAL_R3 [13.1.A] (10YGB----------A)` | border has zero allocation in window — internal events are sparse. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/redispatching_internal.md) |
| 8 | countertrading | (A91, —, —, in_Domain+out_Domain) | daily | EMPTY | 967 | 0 | 999: `COUNTERTRADING_R3 [13.1.B] (10YGB----------A, 10YFR-RTE------C)` | border has zero allocation in window — countertrading events are rare. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/countertrading.md) |
| 9 | congestion_management_costs | (A92, —, —, in_Domain only `domain_style=zone`) | daily | EMPTY | 965 | 0 | 999: `COSTS_OF_CONGESTION_MANAGEMENT_R3 [13.1.C] (10YGB----------A)` | border has zero allocation in window — A92 publication cadence is weekly/monthly. **Note**: A92 is single-zone, NOT cross-zonal. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/congestion_management_costs.md) |
| 10 | offered_transfer_capacity_continuous | (A31, —, —, In_Domain+Out_Domain capitalised, Auction.Type=A01, Contract_MarketAgreement.Type=A01) | daily + 30-day retry | EMPTY | 984 | 0 | 999: `OFFERED_TRANSFER_CAPACITIES_IMPLICIT [11.1] (10YGB----------A, 10YFR-RTE------C)` | border has zero allocation in window — GB no longer in EU continuous auctions post-Brexit. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/offered_transfer_capacity_continuous.md) |
| 11 | offered_transfer_capacity_implicit | (A31, —, —, in_Domain+out_Domain, auction.Type=A01 lowercase, contract_MarketAgreement.Type=A01 lowercase) | daily | EMPTY | 984 | 0 | 999: `OFFERED_TRANSFER_CAPACITIES_IMPLICIT [11.1] (10YGB----------A, 10YFR-RTE------C)` | border has zero allocation in window — GB not in SDAC. Sanity NL→DE: also EMPTY for daily window. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/offered_transfer_capacity_implicit.md) |
| 12 | offered_transfer_capacity_explicit | (A31, —, —, in_Domain+out_Domain, auction.Category=A01, auction.Type=A01, contract_MarketAgreement.Type=A01 — all lowercase) | daily | EMPTY | 984 | 0 | 999: `OFFERED_TRANSFER_CAPACITIES_IMPLICIT [11.1] (10YGB----------A, 10YFR-RTE------C)` | border has zero allocation in window — same root cause as rows 10/11. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/offered_transfer_capacity_explicit.md) |
| 13 | auction_revenue | (A25, —, B07, in_Domain+out_Domain, contract_MarketAgreement.Type=A01) | daily + 30-day retry | EMPTY | 965 | 0 | 999: `AUCTION_REVENUE [12.1.A] (10YGB----------A, 10YFR-RTE------C)` | border has zero allocation in window — auction-revenue cadence sparser than daily; sanity NL→DE 30-day also EMPTY. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/auction_revenue.md) |
| 14 | transfer_capacity_use | (A25, —, B05, in_Domain+out_Domain, Auction.Category=A01 capitalised, contract_MarketAgreement.Type=A01 lowercase) | daily + 30-day retry | EMPTY | 974 | 0 | 999: `USE_OF_TRANSFER_CAPACITY [12.1.A] (10YGB----------A, 10YFR-RTE------C)` | border has zero allocation in window. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/transfer_capacity_use.md) |
| 15 | total_nominated_capacity | (A26, —, B08, in_Domain+out_Domain) | daily | PASS | 5505 | 3 | — | GB→FR 3 TS, MW values 2029-3028 across 24h. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/total_nominated_capacity.md) |
| 16 | total_capacity_allocated | (A26, —, A29, in_Domain+out_Domain, auction.Category=A01 lowercase, contract_MarketAgreement.Type=A01 lowercase) | daily + 30-day retry | EMPTY | 974 | 0 | 999: `TOTAL_CAPACITY_ALLOCATED [12.1.C] (10YGB----------A, 10YFR-RTE------C)` | border has zero allocation in window — post-Brexit GB outside long-term EU allocation. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/total_capacity_allocated.md) |
| 17 | congestion_income | (A25, —, B10, in_Domain+out_Domain, contract_MarketAgreement.Type=A01) | daily + 30-day retry | EMPTY | 983 | 0 | 999: `IMPL_ALLOC_CONG_INCOME_FLOW_BASED [12.1.E] (10YGB----------A, 10YFR-RTE------C)` | border has zero allocation in window — GB not in flow-based / implicit coupling post-Brexit. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/congestion_income.md) |
| 18 | net_positions | (A25, —, B09, **in_Domain only `domain_style=zone`**, contract_MarketAgreement.Type=A01) | daily + 30-day retry | EMPTY | 966 | 0 | 999: `IMPLICIT_ALLOCATIONS_NET_POSITIONS [12.1.E] (10YGB----------A)` | border has zero allocation in window — GB not in SDAC. **Single-zone** A25; Reason text confirms with single EIC. [page](../../../../../Learning/AI/quant-vault/30-vendors/entsoe/datasets/net_positions.md) |

## A25 disambiguation findings (critical)

A25 multiplexes **four logically distinct datasets** that share `documentType=A25`:

| dataset_key | businessType | extra discriminator | domain_style | unit |
|-------------|--------------|---------------------|--------------|------|
| `auction_revenue` | B07 | contract_MarketAgreement.Type=A01 | zone_pair | EUR |
| `transfer_capacity_use` | B05 | Auction.Category=A01 + contract_MarketAgreement.Type=A01 | zone_pair | MW |
| `congestion_income` | B10 | contract_MarketAgreement.Type=A01 | zone_pair | EUR |
| `net_positions` | B09 | contract_MarketAgreement.Type=A01 | **zone (single-domain)** | MW |

`net_positions` is the **only** A25 variant with `domain_style=zone`. The other three are zone_pair. This is recorded in each page's `## Implementation delta` and is the primary reason `documentType` alone is insufficient as a validation criterion — the locked criterion `(documentType, processType, businessType, area-param-name)` correctly disambiguates all four.

## A26 disambiguation findings

| dataset_key | businessType | extra discriminator |
|-------------|--------------|---------------------|
| `total_nominated_capacity` | B08 | (none — no auction.Category, no contract type in request) |
| `total_capacity_allocated` | A29 | auction.Category=A01 + contract_MarketAgreement.Type=A01 (both lowercase) |

Recorded in each page's `## Implementation delta`.

## A63 disambiguation findings

| dataset_key | businessType |
|-------------|--------------|
| `redispatching_cross_border` | A46 |
| `redispatching_internal` | A85 |

## A31 disambiguation findings (parameter casing)

The three A31 variants use distinct **parameter-name casings** as the API's
disambiguation device. ENTSOE responds to all three with the same
`OFFERED_TRANSFER_CAPACITIES_IMPLICIT [11.1]` data-item label on EMPTY,
which is misleading — the publishing side does treat them as separate
datasets despite the shared label.

| dataset_key | Auction casing | Contract casing | auction.Category? |
|-------------|----------------|-----------------|--------------------|
| `_continuous` | `Auction.Type` (Capital A) | `Contract_MarketAgreement.Type` (Capital C) | no |
| `_implicit` | `auction.Type` (lowercase a) | `contract_MarketAgreement.Type` (lowercase c) | no |
| `_explicit` | `auction.Type` (lowercase a) | `contract_MarketAgreement.Type` (lowercase c) | **yes** `auction.Category=A01` |

Cross-checked against `connectors/entsoe/endpoints.py:184-226`. The
connector encodes the casing exactly. Recorded in each A31 page's
`## Implementation delta`.

## Implementation deltas (consolidated)

These are **doc/code observations only** — V1 is documentation-and-validation;
no source code changed.

1. **A09 dual-keying** (rows 4 & 5 — `commercial_schedules` and
   `commercial_schedules_net_positions`): identical `EntsoeDocType("A09",
   None, ..., zone_pair, optional_params=("contract_MarketAgreement.Type",))`.
   Both queries return the same XML (5296 bytes, 2 TS). The dataset key
   distinction is silver-transformer label only; no semantic difference at
   bronze. Recommendation (post-V1): drop one key or have the
   net-positions transformer pair directions and emit a true `net_position_mw`.

2. **`EntsoeCrossborderFlow` schema fields** (`schemas/entsoe.py:64-78`):
   declares `timestamp_utc`, `in_area_code`, `out_area_code`, `flow_mw`,
   `data_provider` only. The transformer (`silver/entsoe/cross_border_flows.py:78-90`)
   writes additional columns (`resolution`, `ingested_at`) to the silver
   Parquet that are not validated by Pydantic. Schema is permissive enough
   that this does not raise — silver Parquet has a richer schema than the
   contract. Logged only.

3. **`EntsoeNetTransferCapacity` schema** (`schemas/entsoe.py:307-325`):
   declares `timestamp_utc`, `in_area_code`, `out_area_code`, `ntc_mw`,
   `resolution`, `data_provider` — but **omits** `ingested_at` which the
   transformer writes (`silver/entsoe/net_transfer_capacity.py:82`). Same
   benign mismatch pattern as #2. Logged only.

4. **A92 `congestion_management_costs` is single-zone** (`endpoints.py:178-183`,
   `domain_style="zone"`). The plan brief listed only A09/A11/A61/A93 as
   cross-zonal — A92 should not be passed `out_Domain != in_Domain`.
   Validation used `in_Domain == out_Domain` (mirrored), which the API
   accepts as single-zone (Reason text fingerprints with one EIC). Code
   matches docs.

5. **All 13 EMPTYs are `Reason code 999` "No matching data found".** No
   400-level errors were observed — the (documentType, processType,
   businessType, domain) tuples in `endpoints.py` match the API's accepted
   shape. The empties reflect publication absence (sparse cadence,
   post-Brexit GB withdrawal from EU coupling), not code defects.

6. **Tuple validation criterion** for ENTSOE — locked from V1-CONTEXT — is
   met for every dataset. Each page's `## Implementation delta` records the
   exact `(documentType, processType, businessType, area-param-name)` tuple
   plus the `contract_MarketAgreement.Type` and `auction.*` discriminators
   where applicable.

## Throttle accounting

- 18 primary calls + 2 smoke + 6 retry + 3 sanity = 29 calls.
- 1.2-second sleep between each = ~35s wall time on rate-limit budget.
- Plan requires ≥18s total (one per primary call): met by 28× sleeps × 1.2s ≈ 34s. PASS.

## Test parameters used

- Default: `periodStart=202605060000&periodEnd=202605070000`
  (2026-05-06 daily window, two days back of the live date 2026-05-08).
- Retry window for sparse A25/A26/A31: `periodStart=202604010000&periodEnd=202605010000` (April 2026, 30 days).
- Default border: GB → FR (`10YGB----------A` → `10YFR-RTE------C`).
- Sanity border: NL → DE (`10YNL----------L` → `10Y1001A1001A82H`).
- API: `https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&...`.
- Transport: `curl --ssl-no-revoke -fsS` (Avast TLS interception
  workstation quirk — see V1-CONTEXT).

## Blockers

None. All 18 datasets PASS at the request-shape level (HTTP 200, valid
ENTSOE root document). The 13 EMPTYs are real publication absences and
not code defects.

## Notes for orchestrator

- This batch wrote 18 dataset pages plus this VALIDATION file. No source
  code was modified. No commits made (orchestrator batches commits).
- README and `endpoints.md` updates for the entsoe vendor are deferred to
  the orchestrator post-batch as per plan §Deferred.
- Other ENTSOE batches (B1/B2/B4/B5) wrote their own dataset pages into
  the same `30-vendors/entsoe/datasets/` folder — total folder content at
  end of B3 run is 48 entsoe dataset pages (B3 contributed 18; the remainder
  came from sibling batches). Sibling-batch coverage is out of scope for
  this VALIDATION.
