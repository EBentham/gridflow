---
phase: V1
plan_id: V1-PLAN-B1-entsoe-load-prices
slug: entsoe-load-prices-vault-validation-and-docs
status: draft
milestone: v0.9
wave: 1
depends_on: []
autonomous: true
files_modified:
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\datasets\*.md  # 11 new files
  - .planning/phases/V1-vault-vendor-validation-and-docs/entsoe-B1-VALIDATION.md  # appended (shared with B2/B3/B4)
requirements:
  - V1-VAULT-01
  - V1-VAULT-02
  - V1-VAULT-03
  - V1-VALID-01
  - V1-VALID-02
  - V1-VALID-03
---

# V1 Plan B1 — ENTSOE Load + Prices + Imbalance

## Goal

Live-validate ENTSOE load, day-ahead price, imbalance price/volume, forecast
margin, and weekly/monthly/yearly load forecast endpoints (~11), produce
per-dataset vault pages, and append to the shared
`entsoe-B1-VALIDATION.md`. Note: ENTSOE has a single base path `/api`;
validation criterion is the `(documentType, processType, businessType,
area-param-name)` tuple matching the API guide PDF, not URL-path equivalence.

## Active datasets (11)

day_ahead_prices (A44), actual_load (A65/A16), load_forecast (A65/A01),
load_forecast_weekly (A65/A31), load_forecast_monthly (A65/A32),
load_forecast_yearly (A65/A33), forecast_margin (A70/A33),
imbalance_prices (A85), imbalance_volume (A86),
activated_balancing_prices (A84/A16), contracted_reserves (A81/A52)

(11 datasets — note `activated_balancing_prices` and `contracted_reserves`
are balancing flavours but rely on the imbalance settlement context;
included here rather than B4 to balance load.)

## must_haves

1. 11 dataset pages under
   `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\datasets\`.
2. Each page records the exact `(documentType, processType, businessType,
   area-param-name)` tuple used for the live call.
3. Each non-PASS dataset has a row appended to
   `.planning/phases/V1-vault-vendor-validation-and-docs/entsoe-B1-VALIDATION.md`
   with cause and curl evidence.

## ENTSOE-specific request shape (all datasets, locked)

- Base URL: `https://web-api.tp.entsoe.eu/api`
- Auth: `?securityToken=<ENTSOE_API_KEY>` (query param, not header)
- Response: XML
- Default test domain: `10YGB----------A` (Great Britain)
- Default test window: `periodStart=202605060000`, `periodEnd=202605070000`
- Document types and process types are mandatory and dataset-specific —
  see the per-dataset tuple in `src/gridflow/connectors/entsoe/endpoints.py`
  `ENDPOINTS` registry.

## Tasks

### Task 1 — Pre-flight smoke test

<read_first>
- .env (`ENTSOE_API_KEY` non-empty)
- V1-CONTEXT.md
- src/gridflow/connectors/entsoe/endpoints.py
</read_first>

<action>
1. Run the carbonintensity smoke-test: `curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" https://api.carbonintensity.org.uk/intensity` (must print 200).
2. Verify `ENTSOE_API_KEY` is set: `grep -E "^ENTSOE_API_KEY=." .env`.
3. Hit ENTSOE health: `curl --ssl-no-revoke -fsS "https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&documentType=A44&in_Domain=10YGB----------A&out_Domain=10YGB----------A&periodStart=202605060000&periodEnd=202605070000" -o /tmp/entsoe-smoke.xml -w "%{http_code}\n"`. Expect 200 and `<Publication_MarketDocument` in the response.
</action>

<acceptance_criteria>
- carbonintensity smoke-test prints `200`.
- `.env` has non-empty `ENTSOE_API_KEY`.
- `/tmp/entsoe-smoke.xml` contains the literal string `Publication_MarketDocument`.
</acceptance_criteria>

### Task 2 — Read official docs and source files

<read_first>
- src/gridflow/connectors/entsoe/endpoints.py
- src/gridflow/connectors/entsoe/client.py
- src/gridflow/connectors/entsoe/parsers.py
- src/gridflow/silver/entsoe/  (every file)
- src/gridflow/schemas/entsoe.py
- tests/fixtures/entsoe/  (every fixture)
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\README.md
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\endpoints.md
- ~/.claude/skills/gridflow-dataset-spec/references/spec-template.md
</read_first>

<action>
1. WebFetch the ENTSOE API guide PDF
   (https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.pdf)
   for sections covering load, prices, imbalance, and forecast margin.
2. For each of the 11 datasets, capture the exact tuple from the guide:
   `(documentType, processType, businessType, in_Domain or
   biddingZone_Domain or controlArea_Domain or BiddingZone_Domain)` and
   compare with the tuple in `endpoints.py`.
3. Capture the response schema (root document type, time-series structure,
   point fields).
</action>

<acceptance_criteria>
- 11-row internal mapping of (dataset_key → docs-tuple, code-tuple, deltas).
- Every delta is recorded as a candidate `## Implementation delta` entry.
</acceptance_criteria>

### Task 3 — Live-validate (11 calls)

<action>
For each dataset, build the URL using the code-tuple from `endpoints.py`,
substitute `securityToken=$ENTSOE_API_KEY`, default GB domain, default
window, then:

```
curl --ssl-no-revoke -fsS -H "Accept: application/xml" \
  "https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&<params>" \
  -o "/tmp/entsoe-<key>.xml" \
  -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
sleep 1.0
```

Throttle 1 req/s (ENTSOE rate limit not vendor-published, 1/s is polite).

Classification:
- **PASS** = HTTP 200 AND root `<Publication_MarketDocument>` /
  `<GL_MarketDocument>` / `<Balancing_MarketDocument>` per data type AND
  ≥1 `<TimeSeries>` element.
- **EMPTY** = HTTP 200 AND ≥1 `<Publication_MarketDocument>` root element
  AND zero `<TimeSeries>` elements (or a `<Reason>` element with code 999).
  Re-test with windowed-back date if same — investigate.
- **FAIL** = non-2xx, or root element name does not match the documented
  `documentType`, or required fields missing.
</action>

<acceptance_criteria>
- 11 curl invocations, one per dataset.
- Throttling sleep ≥10s total.
- Each result row written to local worksheet.
</acceptance_criteria>

### Task 4 — Write 11 dataset pages

<action>
For each dataset, write
`C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\datasets\<dataset_key>.md`
using the `spec-template.md` template. Frontmatter:

```yaml
---
source: entsoe
dataset_key: <key>
vendor: ENTSO-E Transparency Platform
last_verified: 2026-05-08
layer_coverage: bronze, silver
---
```

Required additions for ENTSOE specifically (still inside the standard
template sections):

- `## API endpoint` table includes a `Document type` row (e.g. `A44`),
  `Process type` row (e.g. `A16` or `n/a`), and `Domain param name` row
  (e.g. `in_Domain` / `biddingZone_Domain` / `controlArea_Domain`).
- `### Working curl example` includes `securityToken=<your-entsoe-api-key>`
  placeholder.
- `## Bronze layer` sample is XML (truncated to 200 bytes).
- `## Silver layer` field table maps source XPath to silver column.
- `## Implementation delta` records the docs-tuple vs code-tuple
  comparison result from Task 2.
</action>

<acceptance_criteria>
- 11 files exist, frontmatter `source: entsoe`, `dataset_key` matches the list.
- Each page contains the substring `documentType=` in the curl example.
- Each page contains the substring `Document type` in the API endpoint table.
- Each page contains the substring `## Implementation delta`.
</acceptance_criteria>

### Task 5 — Write entsoe-B1-VALIDATION.md (independent file, no shared writes)

<action>
Write `.planning/phases/V1-vault-vendor-validation-and-docs/entsoe-B1-VALIDATION.md`
with frontmatter and the standard format from V1-CONTEXT.md:

```markdown
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
| PASS   | <n>   |
| EMPTY  | <n>   |
| FAIL   | <n>   |

## Per-dataset results

| Dataset | docType | procType | Status | HTTP | Bytes | Time (s) | Cause | Vault page |
|---------|---------|----------|--------|------|-------|----------|-------|-----------|
| ...     | ...     | ...      | ...    | ...  | ...   | ...      | ...   | ...       |

## Curl evidence

(Per non-PASS dataset, with `### <key>` subheading and the exact curl
command + first 200 bytes of response body.)

## Implementation deltas

(Doc-vs-code tuple deltas affecting B1 datasets.)
```

Each ENTSOE batch (B1/B2/B3/B4) writes its own file. The orchestrator
merges them after all four finish — do not write to a shared file.
</action>

<acceptance_criteria>
- `entsoe-B1-VALIDATION.md` exists.
- Frontmatter has `total_datasets: 11`.
- Per-dataset table has 11 data rows.
- Summary counts add to 11.
</acceptance_criteria>

## Verification

| Check | Pass condition |
|-------|----------------|
| 11 dataset pages | each `<key>.md` exists at the absolute vault path |
| Each page has tuple | `grep -l 'Document type' .../entsoe/datasets/*.md \| wc -l` → at least 11 |
| VALIDATION rows | `grep -c '^\| <key>' entsoe-B1-VALIDATION.md` per key → 1 |

## Deferred

- ENTSOE README and endpoints.md updates are split across B1/B2/B3/B4 — the
  README is updated in B4 only (last plan to finish), endpoints.md is
  updated in B4 only. This avoids file-write races.
