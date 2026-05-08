---
phase: V1
plan_id: V1-PLAN-E-neso
vendor: neso
validation_date: 2026-05-08
total_datasets: 33
pass_count: 33
empty_count: 0
fail_count: 0
---

# NESO Carbon Intensity validation report

Live-validated 2026-05-08 against `https://api.carbonintensity.org.uk` using
`curl --ssl-no-revoke` from the worktree
`C:\Users\Bobbo\OneDrive\Desktop\Python\gridflow\.claude\worktrees\lucid-mccarthy-9ed3e0`.
NESO is public — no API key. Throttle 0.2s between calls (5 req/s, polite vs
config 10 req/s).

Test parameters used:
- `{from}` → `2026-05-06T00:00Z`, `{to}` → `2026-05-06T23:30Z` (1-day window
  to avoid zero-length-range 400s).
- `{date}` → `2026-05-06`.
- `{period}` → `1`.
- `{postcode}` → `RG41`.
- `{regionid}` → `13` (London).
- `{block}` → `1`.

## Pre-flight smoke test

```
curl --ssl-no-revoke -fsS -H "Accept: application/json" \
  -o .tmp/neso-smoke.json \
  -w "HTTP %{http_code}\n" \
  "https://api.carbonintensity.org.uk/intensity"
# → HTTP 200
```

Smoke test passed; `.tmp/neso-smoke.json` non-empty.

## Per-endpoint results

Status legend: PASS = HTTP 2xx + populated `data` + shape matches silver schema
family; EMPTY = 2xx + empty body; FAIL = non-2xx OR field-shape mismatch.
Class = page classification (`accurate` / `needs_minor_patch` / `needs_full_rewrite`).
"Family" reflects which silver shape branch the route consumes
(intensity / factors / stats / generation / regional-period-keyed /
regional-region-keyed).

| # | Dataset | Path | HTTP | Bytes | Status | Class | Family | Notes |
|---|---------|------|------|------|--------|-------|--------|-------|
| 1 | intensity_current | /intensity | 200 | 187 | PASS | needs_minor_patch | intensity | Cross-link to `carbon_intensity` added |
| 2 | intensity_today | /intensity/date | 200 | 8463 | PASS | accurate | intensity | |
| 3 | intensity_date | /intensity/date/2026-05-06 | 200 | 8497 | PASS | accurate | intensity | |
| 4 | intensity_period | /intensity/date/2026-05-06/1 | 200 | 191 | PASS | accurate | intensity | Single record. 48-period day (non-DST). |
| 5 | intensity_factors | /intensity/factors | 200 | 339 | PASS | accurate | factors | 14 fuels in live response: Biomass, Coal, Dutch/French/Irish Imports, Gas (CCGT/OCGT), Hydro, Nuclear, Oil, Other, Pumped Storage, Solar, Wind. |
| 6 | intensity_at | /intensity/2026-05-06T00:00Z | 200 | 191 | PASS | accurate | intensity | Single record. |
| 7 | intensity_fw24h | /intensity/2026-05-06T00:00Z/fw24h | 200 | 5577 | PASS | accurate | intensity | |
| 8 | intensity_fw48h | /intensity/2026-05-06T00:00Z/fw48h | 200 | 10993 | PASS | accurate | intensity | |
| 9 | intensity_pt24h | /intensity/2026-05-06T00:00Z/pt24h | 200 | 5553 | PASS | accurate | intensity | |
| 10 | carbon_intensity | /intensity/2026-05-06T00:00Z/2026-05-06T23:30Z | 200 | 5466 | PASS | needs_minor_patch | intensity | Cross-link to `intensity_current` added |
| 11 | intensity_stats | /intensity/stats/2026-05-06T00:00Z/2026-05-06T23:30Z | 200 | 206 | PASS | accurate | stats | One record `{max,average,min,index}`. |
| 12 | intensity_stats_block | /intensity/stats/2026-05-06T00:00Z/2026-05-06T23:30Z/1 | 200 | 4613 | PASS | accurate | stats | 1-hour blocks across the day. |
| 13 | generation_current | /generation | 200 | 333 | PASS | needs_minor_patch | generation | Live returns `data: {…}` (object, not list). Sample patched. Silver `_data_entries` accepts both shapes. |
| 14 | generation_pt24h | /generation/2026-05-06T00:00Z/pt24h | 200 | 16036 | PASS | accurate | generation | |
| 15 | generation | /generation/2026-05-06T00:00Z/2026-05-06T23:30Z | 200 | 15677 | PASS | accurate | generation | |
| 16 | regional_current | /regional | 200 | 6976 | PASS | needs_minor_patch | regional period-keyed | Period-keyed shape; intensity/genmix nested in each region. |
| 17 | regional_england | /regional/england | 200 | 446 | PASS | needs_minor_patch | regional region-keyed | Single-region list, periods nested in `region.data[]`. |
| 18 | regional_scotland | /regional/scotland | 200 | 441 | PASS | needs_minor_patch | regional region-keyed | Same as england. |
| 19 | regional_wales | /regional/wales | 200 | 437 | PASS | needs_minor_patch | regional region-keyed | Same as england. |
| 20 | regional_postcode | /regional/postcode/RG41 | 200 | 472 | PASS | needs_minor_patch | regional region-keyed | Region object includes echoed `postcode`. |
| 21 | regional_regionid | /regional/regionid/13 | 200 | 449 | PASS | needs_minor_patch | regional region-keyed | |
| 22 | regional_intensity_fw24h | /regional/intensity/2026-05-06T00:00Z/fw24h | 200 | 341156 | PASS | needs_minor_patch | regional period-keyed | Period-keyed. Latent silver bug — see Findings §2. |
| 23 | regional_intensity_fw24h_postcode | /regional/intensity/2026-05-06T00:00Z/fw24h/postcode/RG41 | 200 | 18108 | PASS | needs_minor_patch | regional region-keyed-obj | `data` is a single region object, not a list. |
| 24 | regional_intensity_fw24h_regionid | /regional/intensity/2026-05-06T00:00Z/fw24h/regionid/13 | 200 | 18155 | PASS | needs_minor_patch | regional region-keyed-obj | Same. |
| 25 | regional_intensity_fw48h | /regional/intensity/2026-05-06T00:00Z/fw48h | 200 | 674795 | PASS | needs_minor_patch | regional period-keyed | Period-keyed. Latent silver bug. |
| 26 | regional_intensity_fw48h_postcode | /regional/intensity/2026-05-06T00:00Z/fw48h/postcode/RG41 | 200 | 35808 | PASS | needs_minor_patch | regional region-keyed-obj | |
| 27 | regional_intensity_fw48h_regionid | /regional/intensity/2026-05-06T00:00Z/fw48h/regionid/13 | 200 | 35824 | PASS | needs_minor_patch | regional region-keyed-obj | |
| 28 | regional_intensity_pt24h | /regional/intensity/2026-05-06T00:00Z/pt24h | 200 | 341257 | PASS | needs_minor_patch | regional period-keyed | Period-keyed. Latent silver bug. |
| 29 | regional_intensity_pt24h_postcode | /regional/intensity/2026-05-06T00:00Z/pt24h/postcode/RG41 | 200 | 18110 | PASS | needs_minor_patch | regional region-keyed-obj | |
| 30 | regional_intensity_pt24h_regionid | /regional/intensity/2026-05-06T00:00Z/pt24h/regionid/13 | 200 | 18195 | PASS | needs_minor_patch | regional region-keyed-obj | |
| 31 | regional_intensity | /regional/intensity/2026-05-06T00:00Z/2026-05-06T23:30Z | 200 | 334223 | PASS | needs_minor_patch | regional period-keyed | Period-keyed. Latent silver bug. |
| 32 | regional_intensity_postcode | /regional/intensity/2026-05-06T00:00Z/2026-05-06T23:30Z/postcode/RG41 | 200 | 17717 | PASS | needs_minor_patch | regional region-keyed-obj | |
| 33 | regional_intensity_regionid | /regional/intensity/2026-05-06T00:00Z/2026-05-06T23:30Z/regionid/13 | 200 | 17786 | PASS | needs_minor_patch | regional region-keyed-obj | |

**Totals: PASS 33 · EMPTY 0 · FAIL 0.**

Page classifications: `accurate` 12, `needs_minor_patch` 21, `needs_full_rewrite` 0.

- `accurate` (frontmatter `last_verified` bump only): intensity_today,
  intensity_date, intensity_period, intensity_factors, intensity_at,
  intensity_fw24h, intensity_fw48h, intensity_pt24h, intensity_stats,
  intensity_stats_block, generation_pt24h, generation.
- `needs_minor_patch` (frontmatter + targeted edit): intensity_current,
  carbon_intensity (cross-link delta lines); generation_current
  (bronze-sample shape + delta line); regional_current,
  regional_intensity_fw24h, regional_intensity_fw48h,
  regional_intensity_pt24h, regional_intensity (period-keyed bronze sample +
  delta line on the silver-bug); regional_england, regional_scotland,
  regional_wales, regional_postcode, regional_regionid (region-keyed-list
  bronze sample); plus the 8 region-keyed-obj variants
  (`regional_intensity_{fw24h,fw48h,pt24h}_{postcode,regionid}` and
  `regional_intensity_{postcode,regionid}`) get a region-keyed-object bronze
  sample.

**Note on minor-patch interpretation.** The plan body defines a minor patch
as "targeted edits to the drifted section (usually a field name change in
Silver schema, or a sample-bytes refresh)." For the 13 region-keyed pages
(5 region-keyed-list + 8 region-keyed-obj), the only drift was the bronze
sample shape; no `## Implementation delta` line was added because the
existing one already covered the regional family caveat (`actual` field
nullable). The 5 period-keyed pages got both a new bronze sample and a new
Implementation delta entry pointing to the latent silver bug. This matches
the body wording; if a stricter "every minor-patch page must add a NEW
delta row" reading is preferred later, the 13 region-keyed pages would each
need one additional bullet such as
`- **Region-keyed envelope**: live API places periods inside region.data[];
the *_postcode and *_regionid variants return data as a single object, not
a list.`

## Findings

### 1. `intensity_current` vs `carbon_intensity` resolution

**Question (per plan):** are these aliases or distinct datasets? Both map to
`endpoint: /intensity` in `config/sources.yaml`.

**Answer:** **distinct datasets, not aliases.** Resolution is "two pages with
explicit cross-links + `## Implementation delta` notes" on each page.

**Evidence:**

- `src/gridflow/connectors/neso/endpoints.py::ENDPOINTS` defines:
  - `intensity_current` → `path_template="/intensity"` (no path params, current
    half-hour record only).
  - `carbon_intensity` → `path_template="/intensity/{from_dt}/{to_dt}"`
    (range query, 14-day max via `_MAX_DAYS_PER_REQUEST`).
- The `endpoint:` field in `config/sources.yaml` for `carbon_intensity:
  endpoint: "/intensity"` is **unused at request time** — the connector reads
  the path template from the `ENDPOINTS` dict in `endpoints.py`. The YAML
  `endpoint:` is informational only for that dataset (it controls schedule,
  `max_query_days`, etc.).
- Silver: both consume `parser_family=ParserFamily.INTENSITY` and produce rows
  under `gridflow.schemas.neso.CarbonIntensity`. The legacy
  `CarbonIntensityTransformer` class is registered specifically for the
  `carbon_intensity` dataset for backward compatibility; functionally
  equivalent to the dynamic class generated for `intensity_current`.
- Live test confirms different responses for the same `/intensity*` family:
  `/intensity` returned 187 bytes (one record); `/intensity/{from}/{to}` over
  a one-day window returned 5466 bytes (48 records).

**Action taken:** added `## Implementation delta` cross-links between
[intensity_current](../../../../Learning/AI/quant-vault/30-vendors/neso/datasets/intensity_current.md)
and
[carbon_intensity](../../../../Learning/AI/quant-vault/30-vendors/neso/datasets/carbon_intensity.md)
explaining the YAML duplication and that they are not aliases. Footnote added
to `endpoints.md`.

### 2. Latent silver bug for the period-keyed regional family

**Surface:** the live API returns two distinct shapes for regional routes.

| Shape | Routes | `data[]` entry contains |
|-------|--------|------------------------|
| Period-keyed list | `/regional`, `/regional/intensity/{from}/{fw24h\|fw48h\|pt24h}`, `/regional/intensity/{from}/{to}` | `{from, to, regions:[{regionid, dnoregion, shortname, intensity:{…}, generationmix:[…]}, …×18]}` |
| Region-keyed list | `/regional/{england\|scotland\|wales}`, `/regional/postcode/{postcode}`, `/regional/regionid/{regionid}` | One region: `{regionid, dnoregion, shortname, [postcode,] data:[{from, to, intensity:{…}, generationmix:[…]}, …]}` |
| Region-keyed object | `/regional/intensity/{from}/{fw24h\|fw48h\|pt24h}/{postcode\|regionid}/X`, `/regional/intensity/{from}/{to}/{postcode\|regionid}/X` | `data` is the single region object, not a list. |

The silver code (`silver/neso/carbon_intensity.py`):

- `_extract_regional_rows` correctly distinguishes by checking for
  `entry.get("regions")` (period-keyed) vs `entry.get("data")` (region-keyed).
- BUT inside `_rows_from_region_period(region, period)` it reads
  `period.get("intensity", {})` and `_generation_mix_rows(period)` regardless
  of which branch called it.
- For the **period-keyed branch**, the live API places `intensity` and
  `generationmix` on each `region` dict (not on `period`). The silver lookup
  yields `None`/empty list, and rows for those datasets land in silver with
  null `forecast_gco2_kwh`, `actual_gco2_kwh`, `intensity_index`, `fuel`, and
  `generation_percentage`.
- For the **region-keyed branch**, intensity is correctly at period level
  (inside `region.data[]`), so the same code path works as intended.

Affected datasets (5): `regional_current`, `regional_intensity_fw24h`,
`regional_intensity_fw48h`, `regional_intensity_pt24h`, `regional_intensity`.

Per V1 scope, source code is not modified. Each affected dataset page now
carries an `## Implementation delta` line referencing the
`regional_current.md` write-up. Recommended fix (out of scope): in
`_rows_from_region_period`, prefer `region.get("intensity") or
period.get("intensity")` and similarly for `generationmix` — i.e., read from
whichever level holds the data.

### 3. DST settlement-period coverage (46 / 48 / 50)

The plan and STATE.md require period 1..50 (48 normal, 46 spring DST, 50 autumn
DST). The live test on 2026-05-06 (a normal 48-period day) covered period 1
only; a deeper DST scan would also need 2026-03-29 (UK spring forward, 46
periods) and 2026-10-25 (UK fall back, 50 periods). The connector's
`_settlement_period_count` helper computes period count from
`Europe/London` UTC offset deltas, so the implementation is DST-aware.
This validation pass did **not** fan out across the full DST set — only the
existence of period 1 on a normal day was confirmed live. This is consistent
with the plan's "Deferred" note: "DST settlement-period live testing across
all 33 → only `intensity_period` is fanned out (per STATE.md)."

### 4. `generation_current` envelope shape

`/generation` returns `data: {…}` (a single object) rather than `data: [{…}]`
(a single-item list). The silver `_data_entries` helper handles both
(`isinstance(data, dict)` branch wraps the dict into a one-element list), so
silver output is unaffected. The fixture-style sample on the existing dataset
page was patched to reflect the live object shape.

### 5. `intensity_factors` keyspace

Live response keyspace (14 fuels): `Biomass`, `Coal`, `Dutch Imports`,
`French Imports`, `Gas (Combined Cycle)`, `Gas (Open Cycle)`, `Hydro`,
`Irish Imports`, `Nuclear`, `Oil`, `Other`, `Pumped Storage`, `Solar`, `Wind`.

Note that `intensity_factors` keys use **mixed-case strings with spaces and
parentheses** (e.g. `"Gas (Combined Cycle)"`), unlike the lowercase canonical
fuel labels in `generation*` responses (`gas`, `coal`, `nuclear`, `biomass`,
`hydro`, `imports`, `solar`, `wind`, `other`). The silver
`_transform_factors` lowercases and snake-cases keys
(`"Gas (Combined Cycle)"` → `"gas_combined_cycle"`), so the fuel taxonomy in
`intensity_factors` (silver) is finer-grained than `generation_*` (e.g. it
distinguishes Open vs Combined Cycle gas, separates imports by source). Joins
between the two on `fuel` will need a mapping table (out of scope here).

## Verification checklist

- [x] 33 dataset pages have `last_verified: 2026-05-08`.
- [x] `intensity_current.md` and `carbon_intensity.md` cross-link each other.
- [x] `endpoints.md` `updated:` is `2026-05-08`; footnote on the duplication.
- [x] `README.md` has `## Last validation` section; zero `TODO` markers.
- [x] This file contains the literal substring
      `intensity_current vs carbon_intensity`.

## Curl evidence

Full per-endpoint stats are in `.tmp/neso-results.log` and `.tmp/neso-*.json`
within the worktree. Representative invocations:

```bash
# Smoke test
curl --ssl-no-revoke -fsS -H "Accept: application/json" \
  -o .tmp/neso-smoke.json \
  -w "HTTP %{http_code}\n" \
  "https://api.carbonintensity.org.uk/intensity"

# carbon_intensity (range)
curl --ssl-no-revoke -sS -H "Accept: application/json" \
  "https://api.carbonintensity.org.uk/intensity/2026-05-06T00:00Z/2026-05-06T23:30Z" \
  -o .tmp/neso-carbon_intensity.json \
  -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"

# regional_intensity_fw24h (period-keyed, ~341 KB)
curl --ssl-no-revoke -sS -H "Accept: application/json" \
  "https://api.carbonintensity.org.uk/regional/intensity/2026-05-06T00:00Z/fw24h" \
  -o .tmp/neso-regional_intensity_fw24h.json \
  -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
```

All 33 invocations completed without error; no 4xx/5xx.
