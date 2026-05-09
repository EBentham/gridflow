---
phase: V1
plan_id: V1-PLAN-B5-entsoe-aggregate
slug: entsoe-aggregate-vault-validation-and-docs
status: draft
milestone: v0.9
wave: 2
depends_on: [V1-PLAN-B1-entsoe-load-prices, V1-PLAN-B2-entsoe-generation-outages, V1-PLAN-B3-entsoe-transmission-capacity, V1-PLAN-B4-entsoe-balancing]
autonomous: true
files_modified:
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\endpoints.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\README.md
  - .planning/phases/V1-vault-vendor-validation-and-docs/entsoe-VALIDATION.md
requirements:
  - V1-VAULT-02
  - V1-VAULT-03
---

# V1 Plan B5 — ENTSOE Aggregation (wave 2)

## Goal

After all four ENTSOE wave-1 batches (B1, B2, B3, B4) finish, consolidate
them into the vendor-level deliverables: `entsoe/endpoints.md`,
`entsoe/README.md`, and `entsoe-VALIDATION.md`. No live API calls — pure
aggregation of previously-written batch outputs.

## Pre-conditions (locked)

All four of these must exist on disk before B5 runs:
- `C:\Users\Bobbo\OneDrive\Desktop\Python\gridflow\.claude\worktrees\lucid-mccarthy-9ed3e0\.planning\phases\V1-vault-vendor-validation-and-docs\entsoe-B1-VALIDATION.md`
- `entsoe-B2-VALIDATION.md`
- `entsoe-B3-VALIDATION.md`
- `entsoe-B4-VALIDATION.md`
- 48 dataset pages under `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\datasets\`

If any are missing, halt with a single-line error in
`.planning/phases/V1-vault-vendor-validation-and-docs/B5-HALT.md` and
exit. The orchestrator will inspect and re-run the missing batches.

## must_haves

1. `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\endpoints.md`
   exists and lists all 48 active datasets, grouped by batch family,
   each linking to `./datasets/<key>.md`.
2. `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\README.md`
   has zero `TODO` markers, an `updated: 2026-05-08` frontmatter, and a
   `## Last validation` section.
3. `.planning/phases/V1-vault-vendor-validation-and-docs/entsoe-VALIDATION.md`
   exists with `total_datasets: 48` and consolidates results from all
   four batch reports.

## Tasks

### Task 1 — Pre-condition check

<read_first>
- .planning/phases/V1-vault-vendor-validation-and-docs/entsoe-B1-VALIDATION.md
- .planning/phases/V1-vault-vendor-validation-and-docs/entsoe-B2-VALIDATION.md
- .planning/phases/V1-vault-vendor-validation-and-docs/entsoe-B3-VALIDATION.md
- .planning/phases/V1-vault-vendor-validation-and-docs/entsoe-B4-VALIDATION.md
</read_first>

<action>
Verify the four batch reports exist and the dataset page count is 48:

```
for f in entsoe-B1-VALIDATION.md entsoe-B2-VALIDATION.md \
         entsoe-B3-VALIDATION.md entsoe-B4-VALIDATION.md; do
  test -f ".planning/phases/V1-vault-vendor-validation-and-docs/$f" || \
    { echo "MISSING: $f" > B5-HALT.md; exit 1; }
done

count=$(find "C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/entsoe/datasets" \
  -name "*.md" -type f | wc -l)
test "$count" -eq 48 || \
  { echo "MISSING DATASET PAGES: only $count of 48 exist" > B5-HALT.md; exit 1; }
```

If anything is missing, write `B5-HALT.md` and exit with non-zero. Do not
proceed to Tasks 2-4.
</action>

<acceptance_criteria>
- All 4 batch validation files exist.
- Exactly 48 dataset markdown files exist under
  `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\datasets\`.
- `B5-HALT.md` does NOT exist after this task.
</acceptance_criteria>

### Task 2 — Write ENTSOE endpoints.md

<read_first>
- All 48 files in `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\datasets\`
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\endpoints.md (existing format)
- The four batch validation files (for the per-batch dataset list).
</read_first>

<action>
Write
`C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\endpoints.md`:

```markdown
---
type: vendor-doc
status: active
vendor: entsoe
updated: 2026-05-08
---

# ENTSOE endpoints in use

Authoritative registry lives in
`src/gridflow/connectors/entsoe/endpoints.py`. This file is a
human-readable catalog. Every dataset has a per-dataset page in
`./datasets/<key>.md` with full request/response detail.

ENTSOE has a single base path `/api`; datasets differ by the tuple
`(documentType, processType, businessType, area-param-name)`.

## Load and prices (Batch B1)

| Dataset | docType | procType | Area param | Page |
| ... 11 rows ... |

## Generation and outages (Batch B2)

| ... 13 rows ... |

## Transmission and capacity allocation (Batch B3)

| ... 18 rows ... |

## Balancing extension (Batch B4)

| ... 6 rows ... |

## Validation status

All 48 active datasets validated 2026-05-08. See
`../../../../.planning/phases/V1-vault-vendor-validation-and-docs/entsoe-VALIDATION.md`.
```

Each dataset row links its key to `./datasets/<key>.md`. Pull the
docType/procType/area-param from the dataset page's `## API endpoint`
table, not from the live URL (the dataset page is the canonical
post-validation record).
</action>

<acceptance_criteria>
- File exists at the absolute vault path.
- Contains all 4 batch heading sections.
- Contains exactly 48 markdown links of the form `./datasets/<key>.md`.
- `updated: 2026-05-08` in frontmatter.
</acceptance_criteria>

### Task 3 — Update ENTSOE README.md

<read_first>
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\README.md
</read_first>

<action>
Edit the existing README:
- Resolve every `TODO` marker:
  - Auth: confirmed `?securityToken=<key>` query param.
  - Rate limit: state vendor-published value if found in API guide PDF;
    otherwise `vendor-published default not stated; project uses 1 req/s
    as a polite default verified 2026-05-08`.
  - Status URL: https://transparency.entsoe.eu/ or
    `no public status page; outages announced on the data portal`.
- Bump `updated: 2026-05-08` in frontmatter.
- Append a `## Last validation` section:

```markdown
## Last validation

All 48 active datasets validated 2026-05-08 via V1 milestone v0.9.
See [validation report](../../../../.planning/phases/V1-vault-vendor-validation-and-docs/entsoe-VALIDATION.md).
```
</action>

<acceptance_criteria>
- File contains zero occurrences of `TODO` (verified via `grep -c TODO ...`).
- File contains `## Last validation` heading.
- `updated: 2026-05-08` in frontmatter.
</acceptance_criteria>

### Task 4 — Write consolidated entsoe-VALIDATION.md

<read_first>
- The four batch validation files (B1, B2, B3, B4).
</read_first>

<action>
Write
`.planning/phases/V1-vault-vendor-validation-and-docs/entsoe-VALIDATION.md`:

```markdown
---
phase: V1
vendor: entsoe
validated: 2026-05-08
total_datasets: 48
batches: [B1-load-prices, B2-generation-outages, B3-transmission-capacity, B4-balancing]
---

# ENTSOE — V1 Validation Report (Consolidated)

## Summary (all batches)

| Status | Count |
|--------|-------|
| PASS   | <sum_from_4_batches> |
| EMPTY  | <sum_from_4_batches> |
| FAIL   | <sum_from_4_batches> |
| Total  | 48    |

## Per-batch summaries

| Batch | Total | PASS | EMPTY | FAIL | Source file |
|-------|-------|------|-------|------|-------------|
| B1 (load + prices) | 11 | <n> | <n> | <n> | [entsoe-B1-VALIDATION.md](./entsoe-B1-VALIDATION.md) |
| B2 (generation + outages) | 13 | <n> | <n> | <n> | [entsoe-B2-VALIDATION.md](./entsoe-B2-VALIDATION.md) |
| B3 (transmission + capacity) | 18 | <n> | <n> | <n> | [entsoe-B3-VALIDATION.md](./entsoe-B3-VALIDATION.md) |
| B4 (balancing) | 6 | <n> | <n> | <n> | [entsoe-B4-VALIDATION.md](./entsoe-B4-VALIDATION.md) |

## Per-dataset results (consolidated, sorted by family)

(Concatenate per-dataset rows from the 4 batch reports, in family order.
48 rows total.)

## Cross-batch implementation deltas

(Deltas affecting more than one batch — common XML parsing quirks,
shared securityToken handling, etc.)

## Recommendations

(Backlog items spawned during validation, e.g. wrong base URL,
deprecated endpoints, schema mismatches.)
```
</action>

<acceptance_criteria>
- File exists.
- Frontmatter has `total_datasets: 48` and lists 4 batches.
- Per-dataset table contains exactly 48 data rows (excluding header).
- File contains links to all 4 per-batch validation files.
</acceptance_criteria>

## Verification

| Check | Pass condition |
|-------|----------------|
| Pre-condition (Task 1) | no `B5-HALT.md` exists in phase dir |
| ENTSOE endpoints.md | 48 dataset links + 4 family headings |
| ENTSOE README.md | zero `TODO`, has `## Last validation` |
| Consolidated VALIDATION | `total_datasets: 48`, 48 per-dataset rows |

## Deferred

(none — B5 owns the consolidated ENTSOE V1-VAULT-02 / V1-VAULT-03 work)
