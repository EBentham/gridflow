---
phase: V1
plan_id: V1-PLAN-B2-entsoe-generation-outages
slug: entsoe-generation-outages-vault-validation-and-docs
status: draft
milestone: v0.9
wave: 1
depends_on: []
autonomous: true
files_modified:
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\datasets\*.md  # 13 new files
  - .planning/phases/V1-vault-vendor-validation-and-docs/entsoe-B2-VALIDATION.md
requirements:
  - V1-VAULT-01
  - V1-VAULT-02
  - V1-VAULT-03
  - V1-VALID-01
  - V1-VALID-02
  - V1-VALID-03
---

# V1 Plan B2 — ENTSOE Generation + Outages

## Goal

Live-validate ENTSOE generation, outages, installed-capacity, and water
reservoir endpoints (13), produce per-dataset vault pages, write
`entsoe-B2-VALIDATION.md`. Same ENTSOE-specific request shape as B1.

## Active datasets (13)

actual_generation (A75/A16), wind_solar_forecast (A69/A01),
generation_forecast (A71/A01), actual_generation_units (A73/A16),
water_reservoirs (A72/A16), installed_capacity (A68/A33),
installed_capacity_units (A71/A33), generation_units_master_data (A95),
outages_generation (A80), outages_consumption (A76),
outages_transmission (A78), outages_offshore_grid (A79),
outages_production (A77)

## must_haves

1. 13 dataset pages under `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe/datasets/`.
2. Each page records the `(documentType, processType, businessType,
   area-param-name)` tuple.
3. `entsoe-B2-VALIDATION.md` written with 13 rows.

## Tasks

### Task 1 — Pre-flight smoke test

<read_first>
- C:\Users\Bobbo\OneDrive\Desktop\Python\gridflow\.env  (source of API keys)
- V1-CONTEXT.md
- src/gridflow/connectors/entsoe/endpoints.py
- V1-PLAN-B1-entsoe-load-prices.md (Task 1 reference for sanity)
</read_first>

<action>
1. `[ -f .env ] || cp "C:/Users/Bobbo/OneDrive/Desktop/Python/gridflow/.env" .env`
2. `mkdir -p .tmp`
3. carbonintensity smoke-test (must print 200).
4. Load `ENTSOE_API_KEY` from `.env`; verify non-empty.
5. ENTSOE health: smoke-test the A44 day-ahead query (same as B1) to
   confirm `securityToken` works. Expect 200 + `<Publication_MarketDocument>`.
</action>

<acceptance_criteria>
- `.env` exists in worktree, `.tmp/` exists.
- carbonintensity prints 200.
- A44 smoke-test response contains `Publication_MarketDocument`.
</acceptance_criteria>

### Task 2 — Read official docs and source files
(Same files-to-read as B1 Task 2. Focus the API guide PDF read on
generation, outages, and installed-capacity sections.)

### Task 3 — Live-validate (13 calls)

<action>
For each dataset, build URL with the per-dataset tuple from
`endpoints.py`. Default GB domain `10YGB----------A`. Default window
`periodStart=202605060000&periodEnd=202605070000`.

For outages (A80, A76, A78, A79, A77): use a 30-day window
(`periodStart=202604010000&periodEnd=202605010000`) since outages
are sparse and a 1-day window often returns zero rows even in PASS state.

For weekly/yearly types (A95 master data, A68/A71 installed capacity):
use a yearly window (`periodStart=202601010000&periodEnd=202612310000`).

Run each curl with `sleep 1.0` between, capture `.tmp/entsoe-<key>.xml`.
Same PASS/EMPTY/FAIL classification as B1 Task 3.
</action>

<acceptance_criteria>
- 13 curl invocations, one per dataset.
- Outages use a 30-day window; A95/A68/A71 use a yearly window.
- Each result row written to local worksheet.
</acceptance_criteria>

### Task 4 — Write 13 dataset pages
(Same template as B1 Task 4. Add a `## Known gotchas` row mentioning
ENTSOE outage status code semantics for outage datasets, and the
`businessType` requirement for `actual_generation_units` (A73/A16
requires `psrType` filter for production-unit-level data).)

<acceptance_criteria>
- 13 files exist at `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe/datasets/<key>.md`.
- Each page contains `Document type` row and `Process type` row.
- For outage datasets, page mentions `BusinessType` and outage status
  codes (Active, Cancelled, Withdrawn).
</acceptance_criteria>

### Task 5 — Write entsoe-B2-VALIDATION.md
(Same format as B1 Task 5 but with `batch: B2-generation-outages` and
`total_datasets: 13`.)

<acceptance_criteria>
- File exists with `batch: B2-generation-outages`.
- Per-dataset table has 13 data rows.
- Summary counts add to 13.
</acceptance_criteria>

## Verification

| Check | Pass condition |
|-------|----------------|
| 13 dataset pages | `find C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe/datasets -name "*.md" -newer V1-CONTEXT.md \| wc -l` ≥ 13 in this batch |
| VALIDATION rows | 13 rows + header in B2 file |

## Deferred

- README and endpoints.md updates handled by orchestrator after all
  ENTSOE batches complete.
