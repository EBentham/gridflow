---
phase: V1
plan_id: V1-PLAN-A-elexon
slug: elexon-vault-validation-and-docs
status: draft
milestone: v0.9
wave: 1
depends_on: []
autonomous: true
files_modified:
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\README.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\endpoints.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\datasets\*.md  # 33 new files
  - .planning/phases/V1-vault-vendor-validation-and-docs/elexon-VALIDATION.md
requirements:
  - V1-VAULT-01
  - V1-VAULT-02
  - V1-VAULT-03
  - V1-VALID-01
  - V1-VALID-02
  - V1-VALID-03
---

# V1 Plan A — Elexon Vault Validation And Docs

## Goal

Live-validate every active Elexon dataset (33), produce a per-dataset vault
page using the `gridflow-dataset-spec` template, refresh `endpoints.md` and
`README.md`, and write a per-vendor `elexon-VALIDATION.md` report.

## Active datasets (locked from config/sources.yaml)

system_prices, boal, disbsad, freq, fuelhh, fuelinst, imbalngc, mid,
netbsad, ndf, ndfd, pn, melngc, fou2t14d, uou2t14d, windfor, temp, agpt,
agws, atl, indo, itsdo, indod, nonbm, inddem, indgen, tsdf, tsdfd, lolpdrm,
remit, soso, market_depth, bmunits_reference

Total: **33 datasets**.

## must_haves (goal-backward verification)

1. `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\datasets\<key>.md` exists for each of the
   33 datasets, each following the `gridflow-dataset-spec` template
   verbatim (frontmatter with `source: elexon`, Overview, API endpoint,
   Working curl example, Bronze layer with sample, Silver layer with full
   schema table, Implementation delta).
2. `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\endpoints.md` lists all 33 datasets in a
   quick-summary table grouped by parameter style (settlement-date,
   publish-datetime, no-params reference).
3. `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\README.md` has no remaining `TODO`
   markers — auth, rate limit, status URL, gotchas all confirmed.
4. `.planning/phases/V1-vault-vendor-validation-and-docs/elexon-VALIDATION.md`
   has one row per dataset with PASS / FAIL / EMPTY status, cause, raw
   curl command, HTTP status, and a link to the dataset page.

## Tasks

### Task 1 — Pre-flight smoke test

<read_first>
- C:\Users\Bobbo\OneDrive\Desktop\Python\gridflow\.env  (source of API keys)
- C:\Users\Bobbo\OneDrive\Desktop\Python\gridflow\.claude\worktrees\lucid-mccarthy-9ed3e0\.planning\phases\V1-vault-vendor-validation-and-docs\V1-CONTEXT.md
</read_first>

<action>
1. **Copy .env into worktree (if not already present):**
   `[ -f .env ] || cp "C:/Users/Bobbo/OneDrive/Desktop/Python/gridflow/.env" .env`
   Do NOT modify the main repo's .env. Do NOT commit the worktree-local
   .env (it's already in .gitignore).
2. Make tmp dir: `mkdir -p .tmp`
3. Run: `curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" https://api.carbonintensity.org.uk/intensity`
   Verify exit 0 and output `200`. If not, halt and write a single-line
   error to `elexon-VALIDATION.md` then stop.
4. Load and verify the key:
   `ELEXON_API_KEY=$(grep -E "^ELEXON_API_KEY=" .env | cut -d= -f2- | tr -d '"' | tr -d "'")`
   `[ -n "$ELEXON_API_KEY" ] || { echo "missing ELEXON_API_KEY"; exit 1; }`
5. Hit Elexon health (try FUELHH first, fall back to FUELINST):
   `curl --ssl-no-revoke -fsS -H "apikey: $ELEXON_API_KEY" -o .tmp/elexon-smoke.json -w "%{http_code}\n" "https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELHH?settlementDate=2026-05-06&format=json"`
   If non-200, retry with `/datasets/FUELINST?format=json` (no settlement date — instantaneous).
   If both fail, write FAIL with HTTP code to `elexon-VALIDATION.md` Task 1 row, then continue with the remaining datasets where possible.
</action>

<acceptance_criteria>
- `.env` exists in worktree, contains `ELEXON_API_KEY=` followed by a non-empty value.
- `.tmp/` directory exists.
- The carbonintensity smoke-test curl exits 0 and prints `200`.
- Either the FUELHH or FUELINST baseline curl exits 0 with HTTP 200; if both fail, an explicit Task 1 FAIL row exists in `elexon-VALIDATION.md`.
</acceptance_criteria>

### Task 2 — Read official docs and source files

<read_first>
- src/gridflow/connectors/elexon/endpoints.py
- src/gridflow/connectors/elexon/client.py
- src/gridflow/connectors/elexon/parsers.py
- src/gridflow/silver/elexon/  (every file)
- src/gridflow/schemas/elexon.py
- tests/fixtures/elexon/  (every fixture)
- config/sources.yaml (lines under `sources.elexon`)
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\README.md
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\endpoints.md
- ~/.claude/skills/gridflow-dataset-spec/SKILL.md
- ~/.claude/skills/gridflow-dataset-spec/references/spec-template.md
- ~/.claude/skills/gridflow-dataset-spec/references/vendor-doc-urls.md
</read_first>

<action>
1. Open the Elexon Swagger docs at
   `https://bmrs.elexon.co.uk/api-documentation` via WebFetch. For each of
   the 33 active datasets, capture: full operation path, query parameters,
   response schema, pagination behaviour, and any documented limits.
2. Cross-reference each operation with
   `src/gridflow/connectors/elexon/endpoints.py` `ENDPOINTS` dict and
   `config/sources.yaml`. Note any path discrepancies.
3. Build an internal worksheet (in memory or temp file) mapping
   `dataset_key → (config_path, code_path, doc_path, param_style,
   silver_transformer_class, schema_class, fixture_file_if_any)`.
</action>

<acceptance_criteria>
- Worksheet covers all 33 datasets.
- Each entry has a `param_style` of `settlement_date`, `publish_datetime`,
  `date_in_path`, or `no_params`.
- Doc-vs-code path discrepancies are listed.
</acceptance_criteria>

### Task 3 — Live-validate each dataset (33 calls)

<read_first>
- src/gridflow/connectors/elexon/endpoints.py  (param-style enum)
- The worksheet from Task 2.
</read_first>

<action>
For each dataset key in the active list:

1. Build the live URL using base `https://data.elexon.co.uk/bmrs/api/v1` +
   the path from `endpoints.py`. Use the param style:
   - `settlement_date`: `?settlementDate=2026-05-06&format=json`
   - `publish_datetime`: `?publishDateTimeFrom=2026-05-06T00:00Z&publishDateTimeTo=2026-05-07T00:00Z&format=json`
   - `date_in_path`: substitute today-2 as ISO date.
   - `no_params`: just `?format=json`.
2. Run: `curl --ssl-no-revoke -fsS -H "Accept: application/json" -H "apikey: $ELEXON_API_KEY" "<URL>" -o ".tmp/elexon-<key>.json" -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"`
3. Throttle: `sleep 0.6` between calls (Elexon limit 2 req/s).
4. Capture: HTTP status, body size, first 200 bytes of response.
5. Classify:
   - **PASS** if HTTP 200 AND body parses as JSON AND `data` array (or
     equivalent) has ≥1 row AND fields match `schemas/elexon.py` Pydantic
     model for that dataset.
   - **EMPTY** if HTTP 200 but `data` is empty. Investigate by widening
     date window once (`?settlementDate=2026-04-01` or 30-day historical)
     and noting whether non-empty there. Cause categories: deprecated,
     wrong-window, requires-filter, known-empty.
   - **FAIL** if non-2xx or schema rejects.
6. Append a row to `elexon-VALIDATION.md` (per-row format below).
</action>

<acceptance_criteria>
- 33 curl invocations recorded in `elexon-VALIDATION.md`.
- Each row has columns: dataset, status, http_code, bytes, time_s, cause,
  curl_command, evidence_path.
- Total live time ≥16s (33 × 0.6s throttle minimum).
</acceptance_criteria>

### Task 4 — Write dataset pages (33 files)

<read_first>
- ~/.claude/skills/gridflow-dataset-spec/references/spec-template.md
- The template MUST be used verbatim — no section additions, removals, or
  reorderings.
- The captured live response body (`.tmp/elexon-<key>.json`).
- `src/gridflow/silver/elexon/<dataset>.py` for each dataset's silver
  transformer (field mapping, dedup logic).
- `src/gridflow/schemas/elexon.py` for Pydantic field definitions.
</read_first>

<action>
For each of the 33 datasets, write
`C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\datasets\<dataset_key>.md`
using the `spec-template.md` template. Frontmatter:

```yaml
---
source: elexon
dataset_key: <key>
vendor: Elexon BMRS
last_verified: 2026-05-08
layer_coverage: bronze, silver
---
```

Required sections (in this order, per template):

1. `# Elexon - <Friendly name> (\`<key>\`)`
2. `## Overview` — 2-4 sentences describing the data.
3. `## API endpoint` — table with Base URL, Path, Method, Auth, Rate limit
   (2 req/s), Pagination, Historical depth, Publication lag, Response format.
4. `### Query parameters` — every param documented with type, required,
   description, example.
5. `### Working curl example` — the exact command from Task 3 that
   returned the captured PASS/EMPTY response (replace `$ELEXON_API_KEY`
   with `<your-elexon-api-key>` placeholder for safety).
6. `## Bronze layer` — path pattern, format, granularity, sample (first
   ~200 bytes from `.tmp/elexon-<key>.json`).
7. `## Silver layer` — path pattern, transformer class, Pydantic schema,
   dedup key (include `run_type` for settlement datasets per CLAUDE.md),
   point-in-time field. Full schema table (Field, Type, Nullable,
   Source field, Notes).
8. `## Implementation delta` — any doc-vs-code conflicts found in Task 2 or
   3. If none: `No deltas — code matches docs as of 2026-05-08.`
9. `## Known gotchas` — Elexon-specific (settlement period 1..50, run-type
   ordering, BM unit casing, etc., where relevant to the dataset).
</action>

<acceptance_criteria>
- 33 files exist at `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\datasets\<key>.md`.
- Each file frontmatter has `source: elexon` and `dataset_key: <key>`.
- Each file contains the section headings: `## Overview`, `## API endpoint`,
  `### Query parameters`, `### Working curl example`, `## Bronze layer`,
  `## Silver layer`, `## Implementation delta`, `## Known gotchas`.
- Each file's `## Silver layer` section has a markdown schema table with
  at least one row.
- Each file's `### Working curl example` block contains
  `data.elexon.co.uk/bmrs/api/v1`.
</acceptance_criteria>

### Task 5 — Update endpoints.md

<read_first>
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\endpoints.md (existing format)
- The worksheet from Task 2.
</read_first>

<action>
Rewrite `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\endpoints.md`:

1. Keep the frontmatter (`type: vendor-doc`, `vendor: elexon`,
   `updated: 2026-05-08`).
2. Keep the intro line pointing at `connectors/elexon/endpoints.py` as
   source of truth.
3. Replace the body with three tables grouped by parameter style:
   `### Settlement-date style`, `### Publish-datetime style`,
   `### No params (reference data)`. Each row links the dataset key to
   `./datasets/<key>.md`.
4. Drop any rows for endpoints that have been removed (e.g., the deprecated
   `bod` and the duplicate `generation_by_fuel`).
5. Add a footer table of validation results from Task 3 (PASS / EMPTY /
   FAIL counts).
</action>

<acceptance_criteria>
- File `endpoints.md` exists, has `updated: 2026-05-08`.
- Contains exactly 3 grouped tables (settlement-date, publish-datetime,
  no-params).
- Every dataset key has a markdown link to its dataset page (`./datasets/<key>.md`).
- All 33 active datasets are listed.
- No `bod` or `generation_by_fuel` entries.
</acceptance_criteria>

### Task 6 — Update README.md

<read_first>
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\README.md
- The Swagger overview page for the vendor-published rate limit and status URL.
</read_first>

<action>
1. Replace the existing `Vendor-published: TODO — confirm the official limit`
   line with the confirmed number from the docs (or, if still undocumented
   after research, replace with `Vendor-published: not stated; project
   uses 2 req/s as a polite default verified 2026-05-08`).
2. Replace `Status page: TODO` with the confirmed status URL or with
   `Status: not published; outages announced on the BMRS portal at
   https://www.elexonportal.co.uk/ — confirmed 2026-05-08`.
3. Bump the `updated:` frontmatter to `2026-05-08`.
4. Append a `## Last validation` section: `Validated 2026-05-08 by V1.
   See `[validation report](../../../../.planning/phases/V1-vault-vendor-validation-and-docs/elexon-VALIDATION.md)`.`
</action>

<acceptance_criteria>
- `README.md` has `updated: 2026-05-08` in frontmatter.
- File contains zero occurrences of the substring `TODO`.
- File contains a `## Last validation` heading.
</acceptance_criteria>

### Task 7 — Write elexon-VALIDATION.md

<read_first>
- The Task 3 captured rows.
</read_first>

<action>
Write `.planning/phases/V1-vault-vendor-validation-and-docs/elexon-VALIDATION.md`:

```markdown
---
phase: V1
vendor: elexon
validated: 2026-05-08
total_datasets: 33
---

# Elexon — V1 Validation Report

## Summary

| Status | Count |
|--------|-------|
| PASS   | <n>   |
| EMPTY  | <n>   |
| FAIL   | <n>   |

## Per-dataset results

| Dataset | Status | HTTP | Bytes | Time (s) | Cause | Vault page |
|---------|--------|------|-------|----------|-------|-----------|
| <key>   | PASS   | 200  | ...   | ...      | n/a   | [page](C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\datasets\<key>.md) |
| ...     | ...    | ...  | ...   | ...      | ...   | ...       |

## Curl evidence

For each non-PASS row, append a fenced code block with the exact curl
command run, the response status, and the first 200 bytes of the response
body. PASS rows omit this section to keep the report compact.

## Implementation deltas (cross-cutting)

List any doc-vs-code conflicts that affect more than one dataset.

## Recommendations

Any follow-up work the executor would create as a backlog item.
```
</action>

<acceptance_criteria>
- `elexon-VALIDATION.md` exists.
- File has frontmatter with `vendor: elexon` and `total_datasets: 33`.
- Per-dataset table has 33 rows.
- Summary counts add to 33.
- Every non-PASS row has a curl-evidence code block.
</acceptance_criteria>

## Verification

| Check | Pass condition |
|-------|----------------|
| Dataset page count | `find C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\datasets -name "*.md" -type f \| wc -l` → 33 |
| endpoints.md links | grep `./datasets/<key>.md` 33 times in `endpoints.md` |
| README has no TODO | `grep -c TODO C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\README.md` → 0 |
| VALIDATION row count | grep `^\| ` rows in `elexon-VALIDATION.md` per-dataset table → 33 (+ header + separator) |
| Curl evidence | every FAIL/EMPTY row has at least one `curl --ssl-no-revoke` block |

## Deferred

- Live-call schema validation against `schemas/elexon.py` Pydantic models
  is best-effort: if `from gridflow.schemas.elexon import <Cls>` fails due
  to env/cert issues, classify the dataset by JSON-shape match against the
  fixture and note `schema-validation-deferred` in the cause column.
- Fixture refresh out of scope — log fixture-vs-live drift in the dataset
  page's `## Implementation delta` only.
