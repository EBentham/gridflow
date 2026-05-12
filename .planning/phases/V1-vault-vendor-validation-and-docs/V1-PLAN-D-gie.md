---
phase: V1
plan_id: V1-PLAN-D-gie
slug: gie-agsi-vault-validation-and-docs
status: draft
milestone: v0.9
wave: 1
depends_on: []
autonomous: true
files_modified:
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\gie\README.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\gie\endpoints.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\gie\datasets\*.md  # 7 new files
  - .planning/phases/V1-vault-vendor-validation-and-docs/gie-VALIDATION.md
requirements:
  - V1-VAULT-01
  - V1-VAULT-02
  - V1-VAULT-03
  - V1-VALID-01
  - V1-VALID-02
  - V1-VALID-03
---

# V1 Plan D — GIE AGSI Vault Validation And Docs

## Goal

Live-validate every active GIE AGSI dataset (7), produce per-dataset
vault pages, refresh `endpoints.md` and `README.md`, write
`gie-VALIDATION.md`. Note: `gie_alsi` is excluded from active scope per
project decision (deferred to a follow-up phase).

## Active datasets (7)

storage_reports, storage, about_summary, about_listing, news, news_item,
unavailability

## must_haves

1. 7 dataset pages under
   `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\gie\datasets\`.
2. `quant-vault\30-vendors\gie\endpoints.md` updated.
3. `quant-vault\30-vendors\gie\README.md` has no remaining `TODO`.
4. `gie-VALIDATION.md` written with 7 rows + a note that ALSI is deferred.

## GIE-specific request shape (locked, from STATE.md)

- Base URL: `https://agsi.gie.eu`
- Auth: `x-key` header (lowercase). Value from `GIE_API_KEY` env var.
- Pagination: use `last_page` field as source of truth — `total` is
  per-page row count and is unreliable as a global count.
- Rate limit: 60 calls/minute → throttle 1 req/s minimum.
- `/api/about?show=listing` is the source of truth for company/facility
  expected counts.
- `/api/unavailability` — v007 docs ambiguous; flag in dataset page's
  `## Implementation delta`.
- ALSI LNG (`gie_alsi`): explicitly excluded from V1 scope. The plan
  must verify the connector still loads when `gie_alsi` is present in
  config but is not exercised, and add a one-line note to
  `gie-VALIDATION.md` confirming ALSI is deferred.

## Tasks

### Task 1 — Pre-flight smoke test

<action>
1. `[ -f .env ] || cp "C:/Users/Bobbo/OneDrive/Desktop/Python/gridflow/.env" .env`
2. `mkdir -p .tmp`
3. Run carbonintensity smoke-test (must print 200).
4. Load key: `GIE_API_KEY=$(grep -E "^GIE_API_KEY=" .env | cut -d= -f2- | tr -d '"' | tr -d "'")`; `[ -n "$GIE_API_KEY" ] || { echo "missing GIE_API_KEY"; exit 1; }`
5. Hit AGSI about endpoint:
   `curl --ssl-no-revoke -fsS -H "x-key: $GIE_API_KEY" "https://agsi.gie.eu/api/about" -o .tmp/gie-about-smoke.json -w "%{http_code}\n"`
   Expect 200 and a JSON body.
</action>

<acceptance_criteria>
- carbonintensity prints 200.
- `.env` has non-empty `GIE_API_KEY`.
- `.tmp/gie-about-smoke.json` is non-empty JSON.
</acceptance_criteria>

### Task 2 — Read official docs and source files

<read_first>
- src/gridflow/connectors/gie/  (all files)
- src/gridflow/silver/gie/  (all files)
- src/gridflow/schemas/gie.py
- tests/fixtures/gie/  (every fixture)
- docs/gie_agsi_endpoint_catalog.yaml
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\gie\README.md
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\gie\endpoints.md
- ~/.claude/skills/gridflow-dataset-spec/references/spec-template.md
</read_first>

<action>
WebFetch the GIE AGSI API docs page (https://agsi.gie.eu/api). Capture
endpoint paths, query params (country, company, facility, from, to,
type, page, size), pagination metadata fields, and field semantics
(working_volume, full, trend, gas_in_storage_mwh, etc.).

Cross-reference each of the 7 datasets against the catalog YAML and
the connector's path templates.
</action>

<acceptance_criteria>
- 7-row mapping (key, route, query-params, doc-tuple, code-tuple,
  deltas).
</acceptance_criteria>

### Task 3 — Live-validate (7 calls)

<action>
**Build each URL from `src/gridflow/connectors/gie/endpoints.py`'s
ENDPOINTS registry** — read the path template per dataset key, do not
hardcode `/api` patterns from this plan. The registry is the
implementation source of truth for V1 cross-checking.

Reference URLs to verify against (substitute actual paths from registry
if these differ):

- `storage_reports`: `https://agsi.gie.eu/api?country=GB&from=2026-05-01&to=2026-05-07`
  (per registry: usually `/api` with `country` + `from`/`to`).
- `storage`: `https://agsi.gie.eu/api?country=GB&date=2026-05-06`.
- `about_summary`: `https://agsi.gie.eu/api/about`.
- `about_listing`: `https://agsi.gie.eu/api/about?show=listing`.
- `news`: `https://agsi.gie.eu/api/news`.
- `news_item`: fetch `/api/news` first, pick first `id`, then
  `https://agsi.gie.eu/api/news?id=<id>`.
- `unavailability`: `https://agsi.gie.eu/api/unavailability?country=GB&from=2026-04-01&to=2026-05-07`.

If the registry path for any key differs from the URL above, USE the
registry's path (it is what the connector actually hits). Record the
difference in the dataset page's `## Implementation delta`.

Live call template:
```
curl --ssl-no-revoke -fsS -H "x-key: $GIE_API_KEY" \
  "<URL>" -o ".tmp/gie-<key>.json" \
  -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
sleep 1.0
```

Throttle 1 req/s. Capture `.tmp/gie-<key>.json`.

Classification:
- **PASS** = HTTP 200, JSON body parses, expected top-level key exists
  (`data` for paginated, `companies`/`facilities` for about_listing).
- **EMPTY** = HTTP 200, expected top-level key empty. Investigate
  cause (dates wrong, country has no storage in window, news has no
  recent items).
- **FAIL** = non-2xx, missing key, or registry path differs from
  upstream live behaviour.
</action>

<acceptance_criteria>
- 7 curl invocations recorded.
- Throttle sleep ≥7s.
- For `news_item`, the chosen ID is recorded in the cause column.
</acceptance_criteria>

### Task 4 — Write 7 dataset pages

<action>
For each, write
`C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\gie\datasets\<key>.md`.
Frontmatter:

```yaml
---
source: gie_agsi
dataset_key: <key>
vendor: GIE AGSI+ (Gas Storage)
last_verified: 2026-05-08
layer_coverage: bronze, silver
---
```

`## Known gotchas` section MUST include:
- Lowercase `x-key` header.
- `last_page` field is pagination source of truth (not `total`).
- Values in GWh.
- Rate limit 60 calls/min.

For `unavailability` page: include `## Implementation delta` noting v007
documentation ambiguity.
</action>

<acceptance_criteria>
- 7 files exist at absolute vault path.
- Every page contains the substring `x-key`.
- Every page contains the substring `last_page` or `pagination`.
- `unavailability.md` contains `v007` in the Implementation delta.
</acceptance_criteria>

### Task 5 — Update endpoints.md

<action>
Rewrite `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\gie/endpoints.md`. Document AGSI's 7
active endpoints in a single table. Add a footer note that ALSI LNG is
deferred (`gie_alsi.lng` excluded from V1 scope per project decision).
Update `updated:` to 2026-05-08.
</action>

<acceptance_criteria>
- File lists all 7 active datasets with links to `./datasets/<key>.md`.
- File contains the substring `ALSI` and `deferred`.
- `updated: 2026-05-08` in frontmatter.
</acceptance_criteria>

### Task 6 — Update README.md

<action>
Resolve `TODO` markers in
`C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\gie/README.md`. Confirm:
- Auth: `x-key` header, key from environment, registration at
  https://agsi.gie.eu/.
- Rate limit: 60 calls/minute.
- Status URL: confirm or "not published".
- Bump `updated: 2026-05-08`.
- Add `## Last validation` linking to `gie-VALIDATION.md`.
</action>

<acceptance_criteria>
- File has zero `TODO` occurrences.
- File contains `## Last validation` heading.
- `updated: 2026-05-08` in frontmatter.
</acceptance_criteria>

### Task 7 — Write gie-VALIDATION.md

<action>
Same structure as elexon-VALIDATION.md, with 7 per-dataset rows and a
final paragraph confirming `gie_alsi` is excluded from V1 active scope.
</action>

<acceptance_criteria>
- File exists with `total_datasets: 7`.
- Table has 7 rows.
- File contains the literal phrase `ALSI is deferred`.
</acceptance_criteria>

## Verification

| Check | Pass condition |
|-------|----------------|
| 7 dataset pages | files exist |
| README has no TODO | grep -c TODO → 0 |
| VALIDATION rows | 7 + ALSI-deferred note |

## Deferred

- ALSI LNG validation — backlog item.
- `connectors/ngeso/` empty package — out of scope for this plan.
