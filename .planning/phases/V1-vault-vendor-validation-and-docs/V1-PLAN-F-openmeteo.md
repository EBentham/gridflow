---
phase: V1
plan_id: V1-PLAN-F-openmeteo
slug: openmeteo-vault-validation-and-docs
status: draft
milestone: v0.9
wave: 1
depends_on: []
autonomous: true
files_modified:
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\open-meteo\README.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\open-meteo\endpoints.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\open-meteo\datasets\historical.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\open-meteo\datasets\forecast.md
  - .planning/phases/V1-vault-vendor-validation-and-docs/openmeteo-VALIDATION.md
requirements:
  - V1-VAULT-01
  - V1-VAULT-02
  - V1-VAULT-03
  - V1-VALID-01
  - V1-VALID-02
  - V1-VALID-03
---

# V1 Plan F — Open-Meteo Vault Validation And Docs

## Goal

Live-validate Open-Meteo's two active datasets (`historical`, `forecast`),
verify the two-host configuration in the connector, produce 2 dataset
pages, refresh `endpoints.md` and `README.md`, write
`openmeteo-VALIDATION.md`. Document the naming inconsistency
(vault `open-meteo`, code `openmeteo`, config `open_meteo`) without
renaming.

## Active datasets (2)

historical (path `archive`), forecast (path `forecast`)

## must_haves

1. 2 dataset pages under
   `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\open-meteo\datasets\`.
2. Both pages explicitly document the two-host design
   (`api.open-meteo.com` vs `archive-api.open-meteo.com`).
3. `endpoints.md` and `README.md` refreshed.
4. `openmeteo-VALIDATION.md` written with 2 rows + a section on the
   naming inconsistency.

## Open-Meteo two-host gotcha (locked)

- Forecast: `https://api.open-meteo.com/v1/forecast`
- Historical (ERA5 archive): `https://archive-api.open-meteo.com/v1/archive`

`config/sources.yaml` has `base_url: https://api.open-meteo.com/v1` and
the `historical` dataset path `archive`. The connector MUST override the
base URL for the archive dataset. The plan must verify this in
`connectors/openmeteo/client.py` and either confirm correct override or
record a FAIL in `## Implementation delta` if missing.

## Tasks

### Task 1 — Pre-flight smoke test

<action>
1. `mkdir -p .tmp` (no .env copy needed — Open-Meteo is public).
2. Run carbonintensity smoke-test (must print 200).
3. Hit Open-Meteo forecast: `curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" "https://api.open-meteo.com/v1/forecast?latitude=51.5&longitude=-0.1&hourly=temperature_2m"` → expect 200.
4. Hit Open-Meteo archive: `curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" "https://archive-api.open-meteo.com/v1/archive?latitude=51.5&longitude=-0.1&start_date=2025-05-01&end_date=2025-05-07&hourly=temperature_2m"` → expect 200.
</action>

<acceptance_criteria>
- All three smoke-tests print `200`.
</acceptance_criteria>

### Task 2 — Read source files and verify two-host handling

<read_first>
- src/gridflow/connectors/openmeteo/  (all files)
- src/gridflow/silver/openmeteo/  (all files — historical.py at minimum)
- src/gridflow/schemas/openmeteo.py (or equivalent — find the schema
  source if differently named)
- tests/fixtures/openmeteo/  (every fixture)
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\open-meteo\README.md
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\open-meteo\endpoints.md
- ~/.claude/skills/gridflow-dataset-spec/references/spec-template.md
</read_first>

<action>
1. WebFetch the Open-Meteo Historical Weather API docs
   (https://open-meteo.com/en/docs/historical-weather-api) and the
   Forecast API docs (https://open-meteo.com/en/docs).
2. Inspect `src/gridflow/connectors/openmeteo/client.py` and
   `endpoints.py`. Confirm the connector uses
   `archive-api.open-meteo.com` for the `historical` dataset
   (e.g. via per-dataset host override, or by hard-coded URL build).
3. If override is missing or incorrect, this is a **production bug** —
   record as a FAIL in `openmeteo-VALIDATION.md` AND in the historical
   dataset page's `## Implementation delta`. Do not modify the source
   code (out of V1 scope) — just document.
</action>

<acceptance_criteria>
- The two-host override status is recorded as `OK` or `MISSING/BROKEN`
  in the worksheet.
- If `MISSING/BROKEN`, the cause is captured for VALIDATION.md.
</acceptance_criteria>

### Task 3 — Live-validate (2 calls)

<action>
1. `forecast`:
   `curl --ssl-no-revoke -fsS "https://api.open-meteo.com/v1/forecast?latitude=51.5&longitude=-0.1&hourly=temperature_2m,wind_speed_10m,shortwave_radiation&forecast_days=2" -o .tmp/openmeteo-forecast.json -w "HTTP %{http_code} | %{size_download}B\n"`
2. `historical`:
   `curl --ssl-no-revoke -fsS "https://archive-api.open-meteo.com/v1/archive?latitude=51.5&longitude=-0.1&start_date=2025-05-01&end_date=2025-05-07&hourly=temperature_2m,wind_speed_10m,shortwave_radiation" -o .tmp/openmeteo-historical.json -w "HTTP %{http_code} | %{size_download}B\n"`
3. Verify each `hourly.time` array length matches expected
   (`2 days * 24 hours = 48` for forecast, `7 days * 24 hours = 168`
   for historical).
4. PASS / EMPTY / FAIL classification.
</action>

<acceptance_criteria>
- 2 curl invocations.
- Each captured response has expected `hourly.time` length.
</acceptance_criteria>

### Task 4 — Write 2 dataset pages

<action>
Write
`C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\open-meteo\datasets\historical.md`
and
`C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\open-meteo\datasets\forecast.md`.

Frontmatter:
```yaml
---
source: open_meteo  # config key — note underscore
dataset_key: historical | forecast
vendor: Open-Meteo
last_verified: 2026-05-08
layer_coverage: bronze, silver
---
```

`## API endpoint` table for both pages MUST list:
- Distinct `Base URL` (forecast vs archive host).
- Method GET.
- Auth: none (free tier).
- Soft rate limit: ~10 000 requests/day per IP (vendor-published).
- Pagination: none (chunk via `start_date`/`end_date`).

`## Known gotchas` section MUST include:
- Two-host design.
- ERA5 reanalysis lag (~5 days for historical/archive).
- Naming inconsistency: vault `open-meteo`, code `openmeteo`, config
  `open_meteo` — all three coexist by design; do not rename.

For `historical`, `## Implementation delta` records the connector's
two-host override mechanism (or its absence — see Task 2).
</action>

<acceptance_criteria>
- 2 files exist at absolute vault path.
- Both contain `archive-api.open-meteo.com` literal string somewhere
  (forecast page in cross-reference, historical page as Base URL).
- Both contain `## Known gotchas` heading.
- Both mention the naming inconsistency.
</acceptance_criteria>

### Task 5 — Update endpoints.md

<action>
Rewrite `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\open-meteo/endpoints.md` listing the
two datasets with their respective hosts, paths, and parameter
references. Update `updated: 2026-05-08`.
</action>

<acceptance_criteria>
- File lists both `historical` and `forecast` with correct hosts.
- Both link to `./datasets/<key>.md`.
</acceptance_criteria>

### Task 6 — Update README.md

<action>
Resolve `TODO` markers in
`C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\open-meteo/README.md`. Add `## Last validation`
section. Add a `## Naming` section explicitly documenting the
vault/code/config name divergence. Bump `updated: 2026-05-08`.
</action>

<acceptance_criteria>
- File has zero `TODO` occurrences.
- File contains a `## Naming` heading.
</acceptance_criteria>

### Task 7 — Write openmeteo-VALIDATION.md

<action>
Standard format with 2 rows + a top-level `## Notable findings` section
covering:
- Two-host configuration verification result.
- Naming inconsistency note (no action required, documented in vault).
</action>

<acceptance_criteria>
- File has `total_datasets: 2`.
- Per-dataset table has 2 rows.
- File contains `## Notable findings` heading.
</acceptance_criteria>

## Verification

| Check | Pass condition |
|-------|----------------|
| 2 dataset pages | files exist |
| Two-host documented | `archive-api.open-meteo.com` appears in both pages |
| Naming documented | README contains `## Naming` |
| VALIDATION rows | 2 rows in openmeteo-VALIDATION.md |

## Deferred

- Renaming connector package or vault folder — out of scope, document
  inconsistency only.
- Adding additional Open-Meteo datasets (NWP ensembles, marine,
  air-quality) — only `historical` and `forecast` are in active config.
