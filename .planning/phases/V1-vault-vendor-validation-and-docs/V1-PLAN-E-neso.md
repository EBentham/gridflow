---
phase: V1
plan_id: V1-PLAN-E-neso
slug: neso-vault-validation-and-docs
status: draft
milestone: v0.9
wave: 1
depends_on: []
autonomous: true
files_modified:
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso\README.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso\endpoints.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso\datasets\*.md  # 33 files (validate-and-refresh)
  - .planning/phases/V1-vault-vendor-validation-and-docs/neso-VALIDATION.md
requirements:
  - V1-VAULT-01
  - V1-VAULT-02
  - V1-VAULT-03
  - V1-VAULT-04
  - V1-VALID-01
  - V1-VALID-02
  - V1-VALID-03
---

# V1 Plan E — NESO Vault Validation And Refresh-In-Place

## Goal

Live-validate every active NESO Carbon Intensity dataset (33), validate
existing 33 dataset pages in place, patch drift but preserve accurate
content, refresh `endpoints.md` and `README.md`, write
`neso-VALIDATION.md`. Resolve the
`intensity_current` vs `carbon_intensity` config duplication.

## Active datasets (33)

intensity_current, intensity_today, intensity_date, intensity_period,
intensity_factors, intensity_at, intensity_fw24h, intensity_fw48h,
intensity_pt24h, carbon_intensity, intensity_stats,
intensity_stats_block, generation_current, generation_pt24h, generation,
regional_current, regional_england, regional_scotland, regional_wales,
regional_postcode, regional_regionid, regional_intensity_fw24h,
regional_intensity_fw24h_postcode, regional_intensity_fw24h_regionid,
regional_intensity_fw48h, regional_intensity_fw48h_postcode,
regional_intensity_fw48h_regionid, regional_intensity_pt24h,
regional_intensity_pt24h_postcode, regional_intensity_pt24h_regionid,
regional_intensity, regional_intensity_postcode,
regional_intensity_regionid

## must_haves

1. 33 NESO dataset pages exist and are accurate. Existing accurate
   content is preserved; drift is patched.
2. `intensity_current` vs `carbon_intensity` duplication is investigated
   and resolved (one alias-of, two-with-cross-link, or one-fold-and-doc).
3. `endpoints.md` and `README.md` refreshed.
4. `neso-VALIDATION.md` written with 33 rows.

## NESO-specific request shape (locked)

- Base URL: `https://api.carbonintensity.org.uk`
- Auth: none. Send `Accept: application/json`.
- Rate limit: not vendor-published. Project uses 10 req/s; throttle 5
  req/s in this plan to be polite.
- Path-templated routes (the majority): `{from}` and `{to}` are
  ISO-8601 path segments, NOT query params. Maximum window 14 days.
- Settlement period 1..50: 48 normal, 46 spring DST, 50 autumn DST.
- Fan-out fact: `intensity_period` must enumerate all valid GB
  settlement periods for a given date. Test with 48 to confirm.

## Tasks

### Task 1 — Pre-flight smoke test

<action>
1. `mkdir -p .tmp` (no .env copy needed — NESO is public).
2. Run carbonintensity smoke-test:
   `curl --ssl-no-revoke -fsS -H "Accept: application/json" -o .tmp/neso-smoke.json -w "HTTP %{http_code}\n" "https://api.carbonintensity.org.uk/intensity"`
   Expect `HTTP 200` and `/.tmp/neso-smoke.json` non-empty.
</action>

<acceptance_criteria>
- Smoke-test prints `HTTP 200`.
- `.tmp/neso-smoke.json` is non-empty.
</acceptance_criteria>

### Task 2 — Read source files and existing vault pages

<read_first>
- src/gridflow/connectors/neso/  (all files)
- src/gridflow/silver/neso/  (all files)
- src/gridflow/schemas/neso.py
- tests/fixtures/neso/  (every fixture)
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso\README.md
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso\endpoints.md
- All 33 existing files in `quant-vault\30-vendors\neso\datasets\`
- ~/.claude/skills/gridflow-dataset-spec/references/spec-template.md
</read_first>

<action>
1. WebFetch the NESO Carbon Intensity API docs
   (https://carbon-intensity.github.io/api-docs and
   https://carbon-intensity.github.io/api-docs/#get-regional).
2. Read every existing dataset page. Build a worksheet column for each:
   `(needs_full_rewrite, needs_minor_patch, accurate)`.
3. Investigate the `intensity_current` vs `carbon_intensity` config
   duplication. Both map to path `/intensity` in
   `config/sources.yaml`. Determine via source-code reading and live
   testing whether they are aliases or distinct datasets.
</action>

<acceptance_criteria>
- 33-row worksheet with classification per page.
- A documented finding for `intensity_current` vs `carbon_intensity`
  saved as a note for inclusion in `neso-VALIDATION.md`.
</acceptance_criteria>

### Task 3 — Live-validate (33 calls)

<action>
For each of the 33 datasets, build URL using path templates from
`src/gridflow/connectors/neso/endpoints.py`. Substitute date params:
- `{from}` → `2026-05-06T00:00Z`, `{to}` → `2026-05-06T23:30Z` for
  same-day; for ranges, use a 1-day window so URLs aren't zero-length
  (zero-length range returns 400 per STATE.md).
- `{date}` → `2026-05-06`.
- `{period}` → `1` (any settlement period 1..50).
- `{postcode}` → `RG41`.
- `{regionid}` → `13` (London).
- `{block}` → `1` (1-hour stats block).

For each dataset, run a literal:
```
curl --ssl-no-revoke -fsS -H "Accept: application/json" \
  "https://api.carbonintensity.org.uk<path>" \
  -o ".tmp/neso-<key>.json" \
  -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n" \
  2> ".tmp/neso-<key>.err"
sleep 0.2
```

Examples:
- `intensity_current` → `curl --ssl-no-revoke -fsS -H "Accept: application/json" "https://api.carbonintensity.org.uk/intensity" -o .tmp/neso-intensity_current.json -w "..."`
- `intensity_date` → `curl --ssl-no-revoke -fsS -H "Accept: application/json" "https://api.carbonintensity.org.uk/intensity/date/2026-05-06" -o .tmp/neso-intensity_date.json -w "..."`
- `intensity_period` → `.../intensity/date/2026-05-06/1`
- `regional_postcode` → `.../regional/postcode/RG41`
- `regional_intensity` → `.../regional/intensity/2026-05-06T00:00Z/2026-05-06T23:30Z`

Throttle 0.2s between calls (5 req/s).

Classification:
- **PASS** = HTTP 200 AND `data` array (or object) populated AND fields
  match the existing dataset page's silver schema.
- **EMPTY** = HTTP 200 AND empty `data`. Investigate cause.
- **FAIL** = non-2xx OR field schema mismatch.

Compare each PASS response against the existing dataset page's
"Bronze sample" (if present) — if drift, mark page for patch.
</action>

<acceptance_criteria>
- 33 curl invocations.
- Throttle sleep ≥6.6s total.
- Each row classified PASS / EMPTY / FAIL plus drift flag.
</acceptance_criteria>

### Task 4 — Validate-and-refresh dataset pages

<action>
For each of the 33 existing pages:

1. If `accurate` (no drift): bump frontmatter `last_verified: 2026-05-08`
   only. Do not rewrite the body.
2. If `needs_minor_patch`: targeted edits to the drifted section
   (usually a field name change in Silver schema, or a sample-bytes
   refresh). Preserve all other content. Bump `last_verified`.
3. If `needs_full_rewrite`: regenerate the page using the template,
   capturing the live response from Task 3 as ground truth.

For the `intensity_current` vs `carbon_intensity` resolution: write
both pages with explicit cross-links and an `## Implementation delta`
section noting the config duplication. If the connector treats them as
aliases, the resolution becomes "one canonical page +
`alias-of: intensity_current` frontmatter" on the duplicate.
</action>

<acceptance_criteria>
- All 33 pages have `last_verified: 2026-05-08`.
- Pages classified `accurate` have only the frontmatter `last_verified`
  field modified — body content unchanged when compared with
  `diff -B -w` (ignoring blank lines and whitespace).
- Pages classified `needs_minor_patch` have a single `## Implementation
  delta` row added.
- `intensity_current.md` and `carbon_intensity.md` cross-link each
  other.
</acceptance_criteria>

### Task 5 — Update endpoints.md

<action>
Refresh `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso/endpoints.md` to reflect all 33
active routes grouped by family (intensity, generation, regional). Add
the duplication finding from Task 2 as a footnote. Update `updated:` to
2026-05-08.
</action>

<acceptance_criteria>
- File lists 33 active datasets, each linked to `./datasets/<key>.md`.
- Footnote describes the intensity_current/carbon_intensity finding.
</acceptance_criteria>

### Task 6 — Update README.md

<action>
Resolve any `TODO` markers in
`C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso/README.md`. Add `## Last validation`
section linking to `neso-VALIDATION.md`. Bump `updated: 2026-05-08`.
</action>

<acceptance_criteria>
- File has zero `TODO` occurrences.
- File contains `## Last validation` heading.
</acceptance_criteria>

### Task 7 — Write neso-VALIDATION.md

<action>
Standard format with 33 rows + a dedicated `## Findings` section
covering:
- `intensity_current` vs `carbon_intensity` resolution.
- DST settlement-period coverage (46/48/50).
- Any drift between connector code and live response shape.
</action>

<acceptance_criteria>
- File exists with `total_datasets: 33`.
- Table has 33 rows.
- File contains the literal substring
  `intensity_current vs carbon_intensity`.
</acceptance_criteria>

## Verification

| Check | Pass condition |
|-------|----------------|
| 33 dataset pages | each has `last_verified: 2026-05-08` |
| accurate pages preserved | byte-diff vs originals shows only frontmatter changes for those flagged accurate |
| README has no TODO | grep -c TODO → 0 |
| VALIDATION rows | 33 rows in neso-VALIDATION.md |
| current/carbon resolution | both pages cross-link |

## Deferred

- DST settlement-period live testing across all 33 → only
  `intensity_period` is fanned out (per STATE.md).
- Regional-API rich subdivision metadata documentation — already
  documented in existing pages, only validate.
