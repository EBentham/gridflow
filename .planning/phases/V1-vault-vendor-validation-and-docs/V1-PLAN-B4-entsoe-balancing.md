---
phase: V1
plan_id: V1-PLAN-B4-entsoe-balancing
slug: entsoe-balancing-vault-validation-and-docs
status: draft
milestone: v0.9
wave: 1
depends_on: []
autonomous: true
files_modified:
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\datasets\*.md  # 6 new files
  - .planning/phases/V1-vault-vendor-validation-and-docs/entsoe-B4-VALIDATION.md
requirements:
  - V1-VAULT-01
  - V1-VAULT-02
  - V1-VAULT-03
  - V1-VALID-01
  - V1-VALID-02
  - V1-VALID-03
---

# V1 Plan B4 — ENTSOE Balancing Extension

## Goal

Live-validate ENTSOE balancing-extension endpoints (6, the H8 family),
produce per-dataset vault pages, write `entsoe-B4-VALIDATION.md`.

## Active datasets (6)

current_balancing_state (A86),
balancing_energy_bids (A37/A47),
aggregated_balancing_energy_bids (A24/A51),
procured_balancing_capacity (A15/A51),
cross_zonal_balancing_capacity (A38/A51),
balancing_financial_expenses_income (A87)

(Note: `activated_balancing_prices` and `contracted_reserves` are in B1
because they semantically pair with imbalance settlement. The H8 family
balancing extensions documented in `STATE.md` are these six.)

## must_haves

1. 6 dataset pages under `quant-vault/30-vendors/entsoe/datasets/`.
2. Each page records the tuple AND the
   `(BusinessType, type_MarketAgreement.Type)` where applicable.
3. `entsoe-B4-VALIDATION.md` written with 6 rows.

## ENTSOE balancing-specific gotchas

- A37 (balancing energy bids), A24 (aggregated bids), A15 (procured
  capacity), A38 (cross-zonal capacity) use `controlArea_Domain` (single
  domain), not `in_Domain`/`out_Domain`. Validation criterion includes
  the area-param-name.
- A86 has two semantic mappings: `imbalance_volume` (covered in B1) and
  `current_balancing_state`. They differ by presence/absence of
  `businessType=B33` (imbalance volume) vs none (state).
- A87 (financial balancing expenses/income) uses
  `Reason.code` rather than typed time series — page must document that.

## Tasks

### Task 1 — Pre-flight smoke test
(Same as B1 Task 1.)

### Task 2 — Read official docs and source files
(Same files as B1 Task 2. Focus PDF on Section 17 — Balancing.)

### Task 3 — Live-validate (6 calls)

<action>
For each dataset, build URL with the per-dataset tuple, GB control area
`controlArea_Domain=10YGB----------A`, daily window. For A38
(cross-zonal balancing capacity), use GB→FR cross-zonal. For A86
(current_balancing_state), use windowed `periodStart=202605070000&
periodEnd=202605071400` (intra-day window) since this dataset is current
state, not historical.

Throttle 1 req/s. Capture `/tmp/entsoe-<key>.xml`. PASS/EMPTY/FAIL as B1.
</action>

<acceptance_criteria>
- 6 curl invocations.
- Each row records area-param-name used (`controlArea_Domain` or
  `in_Domain`/`out_Domain`).
</acceptance_criteria>

### Task 4 — Write 6 dataset pages
(Same template as B1 Task 4. Add `### Control-area vs cross-zonal`
subsection where applicable. For A86 dataset, explicitly cross-link to
the imbalance_volume page in `## Implementation delta`, since they share
the docType but mean different things.)

<acceptance_criteria>
- 6 files exist.
- A86 (current_balancing_state) page contains a link to imbalance_volume.md.
- All pages have `Document type` and `Process type` (or `n/a`) rows.
</acceptance_criteria>

### Task 5 — Write entsoe-B4-VALIDATION.md
(Same format as B1 Task 5 with `batch: B4-balancing` and
`total_datasets: 6`.)

## Verification

| Check | Pass condition |
|-------|----------------|
| 6 dataset pages | listed files exist at the absolute vault path |
| VALIDATION rows | 6 rows in B4 file |
| A86 cross-link | grep `imbalance_volume.md` in current_balancing_state.md |

## Deferred

- README and endpoints.md updates handled by orchestrator post-batch.
