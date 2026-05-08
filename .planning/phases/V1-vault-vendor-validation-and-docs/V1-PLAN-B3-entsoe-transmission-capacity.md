---
phase: V1
plan_id: V1-PLAN-B3-entsoe-transmission-capacity
slug: entsoe-transmission-capacity-vault-validation-and-docs
status: draft
milestone: v0.9
wave: 1
depends_on: []
autonomous: true
files_modified:
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\datasets\*.md  # 18 new files
  - .planning/phases/V1-vault-vendor-validation-and-docs/entsoe-B3-VALIDATION.md
requirements:
  - V1-VAULT-01
  - V1-VAULT-02
  - V1-VAULT-03
  - V1-VALID-01
  - V1-VALID-02
  - V1-VALID-03
---

# V1 Plan B3 — ENTSOE Transmission + Capacity Allocation

## Goal

Live-validate ENTSOE transmission and capacity-allocation endpoints (18 —
the largest ENTSOE batch), produce per-dataset vault pages, write
`entsoe-B3-VALIDATION.md`.

## Active datasets (18)

cross_border_flows (A11), net_transfer_capacity (A61),
dc_link_intraday_transfer_limits (A93), commercial_schedules (A09),
commercial_schedules_net_positions (A09),
redispatching_cross_border (A63), redispatching_internal (A63),
countertrading (A91), congestion_management_costs (A92),
offered_transfer_capacity_continuous (A31),
offered_transfer_capacity_implicit (A31),
offered_transfer_capacity_explicit (A31), auction_revenue (A25),
transfer_capacity_use (A25), total_nominated_capacity (A26),
total_capacity_allocated (A26), congestion_income (A25),
net_positions (A25)

## must_haves

1. 18 dataset pages under `quant-vault/30-vendors/entsoe/datasets/`.
2. Each page records the tuple AND the contract market agreement type
   (`contract_MarketAgreement.Type`) where applicable (A25, A26, A31).
3. `entsoe-B3-VALIDATION.md` written with 18 rows.

## ENTSOE-specific gotchas for this batch

- A09, A11, A61, A93 use **cross-zonal** params:
  `in_Domain` + `out_Domain` (a directional border, e.g.
  `in_Domain=10YGB----------A&out_Domain=10YBE----------2` for GB→BE).
- A31 (offered transfer capacity) requires
  `contract_MarketAgreement.Type` (`A01` daily, `A02` weekly, `A03`
  monthly, `A04` yearly). The `_continuous`, `_implicit`, `_explicit`
  variants additionally require `auction.Type` and `auction.Category`
  to disambiguate.
- A63 redispatching distinguishes by `businessType` (`A46` cross-border
  vs `A85` internal).
- A25 has multiple semantic mappings (auction_revenue,
  transfer_capacity_use, congestion_income, net_positions) — same
  `documentType` but different combinations of
  `(businessType, auction.Type)`. These collisions are why the dataset
  tuple is critical, not just `documentType`.

## Tasks

### Task 1 — Pre-flight smoke test
(Same as B1 Task 1.)

### Task 2 — Read official docs and source files
(Same files as B1 Task 2. Focus PDF on cross-zonal flow, transfer
capacity, and redispatching sections.)

### Task 3 — Live-validate (18 calls)

<action>
For each dataset:

- Cross-zonal datasets (A09, A11, A61, A93): use GB→FR border
  `in_Domain=10YGB----------A&out_Domain=10YFR-RTE------C`, daily window.
- A31 variants: use GB→FR with appropriate
  `contract_MarketAgreement.Type=A01` (daily), monthly window.
- A63 redispatching: use GB→FR for cross-border, single domain for
  internal.
- A25/A26: use GB→FR border, daily window for daily-resolution variants,
  yearly window for yearly-resolution variants.

Throttle 1 req/s. Capture `/tmp/entsoe-<key>.xml`.

PASS/EMPTY/FAIL same as B1. EMPTY is more common in this batch — record
cause as one of: "border has zero allocation in window",
"contract type mismatch — needs different MarketAgreement.Type",
"requires explicit auction.Type to disambiguate".
</action>

<acceptance_criteria>
- 18 curl invocations.
- Each non-PASS row records cause from the enumerated list.
- Throttle sleep ≥18s total.
</acceptance_criteria>

### Task 4 — Write 18 dataset pages
(Same template as B1 Task 4. Add `### Cross-zonal parameters` subsection
under `## API endpoint` for cross-zonal datasets, listing valid
(in_Domain, out_Domain) examples.)

<acceptance_criteria>
- 18 files exist.
- Each cross-zonal dataset page contains `### Cross-zonal parameters`
  subsection.
- Each A31 page contains `contract_MarketAgreement.Type` substring.
- Each A25 page (auction_revenue, transfer_capacity_use,
  congestion_income, net_positions) explicitly distinguishes itself
  from the other A25 variants in the `## Implementation delta` section.
</acceptance_criteria>

### Task 5 — Write entsoe-B3-VALIDATION.md
(Same format as B1 Task 5 with `batch: B3-transmission-capacity` and
`total_datasets: 18`.)

## Verification

| Check | Pass condition |
|-------|----------------|
| 18 dataset pages | listed files exist at the absolute vault path |
| VALIDATION rows | 18 rows in B3 file |
| Cross-zonal coverage | grep `Cross-zonal parameters` finds ≥4 hits |

## Deferred

- README and endpoints.md updates handled by orchestrator post-batch.
