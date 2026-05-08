---
phase: V1
plan_id: V1-PLAN-C-entsog
slug: entsog-vault-validation-and-docs
status: draft
milestone: v0.9
wave: 1
depends_on: []
autonomous: true
files_modified:
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsog\README.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsog\endpoints.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsog\datasets\*.md  # 33 new files
  - .planning/phases/V1-vault-vendor-validation-and-docs/entsog-VALIDATION.md
requirements:
  - V1-VAULT-01
  - V1-VAULT-02
  - V1-VAULT-03
  - V1-VALID-01
  - V1-VALID-02
  - V1-VALID-03
---

# V1 Plan C — ENTSOG Vault Validation And Docs

## Goal

Live-validate every active ENTSOG dataset (33), produce per-dataset
vault pages, refresh `endpoints.md` and `README.md`, write
`entsog-VALIDATION.md`. ENTSOG is public — no API key needed.

## Active datasets (33)

physical_flows, nominations, allocations, renominations, firm_available,
firm_booked, firm_technical, interruptible_available,
interruptible_booked, interruptible_total, gcv, wobbe_index,
methane_content, hydrogen_content, oxygen_content,
available_through_oversubscription, available_through_surrender,
available_through_uioli_long_term, available_through_uioli_short_term,
cmp_unsuccessful_requests, cmp_unavailable_firm_capacity,
cmp_auction_premiums, interruptions, aggregated_physical_flows,
tariffs, tariff_simulations, urgent_market_messages, connection_points,
operators, balancing_zones, operator_point_directions, interconnections,
aggregate_interconnections

## must_haves

1. 33 dataset pages under
   `C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsog\datasets\`.
2. `quant-vault\30-vendors\entsog\endpoints.md` updated with all 33
   datasets grouped by route family.
3. `quant-vault\30-vendors\entsog\README.md` has no remaining `TODO`.
4. `entsog-VALIDATION.md` written with 33 rows.

## ENTSOG-specific request shape (locked)

- Base URL: `https://transparency.entsog.eu/api/v1`
- Auth: none (public).
- Response: JSON.
- 19 of the 33 datasets share path `/operationalData` and differ by
  `indicator` query param (`Physical Flow`, `Nominations`, `Allocations`,
  etc.) and `pointDirection` filter.
- Hard rule from STATE.md: ENTSOG operational requests must use
  exact-case `/operationalData`, `timeZone=UCT` (note the typo —
  ENTSOG's spelling), exact-case indicators, and `pointDirection`
  filters built from `operatorKey + pointKey + directionKey`.
- Reference data routes (operators, connection points, balancing zones,
  operator_point_directions, interconnections, aggregate_interconnections):
  no point-direction filter, paginate via `?limit=1000&offset=0`.

## Tasks

### Task 1 — Pre-flight smoke test

<action>
1. Run carbonintensity smoke-test (must print 200).
2. Hit ENTSOG public root: `curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" "https://transparency.entsog.eu/api/v1/operators?limit=5"` → expect 200.
3. Capture one valid `pointDirection` from the operators+points response
   for use in operationalData calls — pick a high-volume GB entry point
   (e.g. operator `21X-GB-A-A0A0A-Z` National Grid Gas, point
   `BACTON IUK 21Z000000000038N`, direction `entry`). If GB entry point
   not present in response, fall back to the largest by `flowRate` from
   the first 100 results.
</action>

<acceptance_criteria>
- ENTSOG operators endpoint returns 200.
- At least one (operatorKey, pointKey, directionKey) tuple captured for
  use in subsequent operational-data calls.
</acceptance_criteria>

### Task 2 — Read official docs and source files

<read_first>
- src/gridflow/connectors/entsog/  (all files)
- src/gridflow/silver/entsog/  (all files)
- src/gridflow/schemas/entsog.py
- tests/fixtures/entsog/  (every fixture)
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsog\README.md
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsog\endpoints.md
- ~/.claude/skills/gridflow-dataset-spec/references/spec-template.md
</read_first>

<action>
1. WebFetch the ENTSOG API manual PDF
   (https://transparency.entsog.eu/api/archiveDirectories/8/api-manual/TP_REG715_Documentation_TP_API%20-%20v2.1.pdf).
2. Cross-check each of the 33 datasets — for `/operationalData` family,
   record the exact `indicator` value used in code vs manual.
3. For non-operationalData routes (`/cmpUnavailables`, `/cmpAuctions`,
   `/cmpUnsuccessfulRequests`, `/interruptions`, `/aggregatedData`,
   `/tariffsFulls`, `/tariffsSimulations`, `/urgentMarketMessages`,
   `/connectionPoints`, `/operators`, `/balancingZones`,
   `/operatorPointDirections`, `/interconnections`,
   `/aggregateInterconnections`), capture path and pagination params.
</action>

<acceptance_criteria>
- 33-row mapping of (key, route, indicator?, query-params, doc-tuple,
  code-tuple, deltas).
- Indicators recorded with exact case (`Physical Flow`, not
  `physical_flow`).
</acceptance_criteria>

### Task 3 — Live-validate (33 calls)

<action>
For operationalData family (19 datasets): use captured pointDirection
tuple from Task 1, single-day window
`from=2026-05-06&to=2026-05-06`, `timeZone=UCT`,
`indicator=<exact-case indicator>`, `forceDownload=true`,
`limit=1000`. Throttle 1 req/s.

For reference data (operators, connection_points, balancing_zones,
operator_point_directions, interconnections,
aggregate_interconnections): no time params, `limit=100&offset=0`.

For non-operational routes (interruptions, aggregated_physical_flows,
tariffs, tariff_simulations, urgent_market_messages, cmp_*): use
single-day window where time-bound.

Capture `/tmp/entsog-<key>.json`. Classification:
- **PASS** = HTTP 200 AND `data` array non-empty.
- **EMPTY** = HTTP 200 AND `data` empty. Investigate by widening:
  - operationalData → expand to 30-day window once.
  - tariffs/tariff_simulations → reference data, may legitimately be
    empty for niche operators; check 100-row reference data fetch first.
- **FAIL** = non-2xx, missing `data` key, or shape mismatch.
</action>

<acceptance_criteria>
- 33 curl invocations.
- Each operationalData call uses the same captured pointDirection tuple.
- Throttle sleep ≥33s total.
- Each row has the indicator value (or n/a) recorded.
</acceptance_criteria>

### Task 4 — Write 33 dataset pages

<action>
For each dataset, write
`C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsog\datasets\<key>.md`
using the template. Frontmatter:

```yaml
---
source: entsog
dataset_key: <key>
vendor: ENTSOG Transparency Platform
last_verified: 2026-05-08
layer_coverage: bronze, silver
---
```

For operationalData datasets, the `## API endpoint` section MUST list:
- `Indicator` row with the exact-case value.
- `Time zone` row stating `UCT (ENTSOG's spelling)`.
- `pointDirection filter` row showing the
  `operatorKey + pointKey + directionKey` format.

For reference-data datasets, `## API endpoint` notes pagination is
required even for full-inventory fetches.

`## Known gotchas` section MUST include the ENTSOG hard rules from
gridflow STATE.md (exact-case indicators, UCT, snake_case
field-coalescing for `isCAMRelevant`/`isCamRelevant`, null-tolerant
`lastUpdateDateTime`).
</action>

<acceptance_criteria>
- 33 files exist at the absolute vault path.
- Every operationalData page contains the substring `timeZone=UCT`.
- Every operationalData page contains the substring `pointDirection`.
- Every page contains `## Known gotchas` heading.
</acceptance_criteria>

### Task 5 — Update endpoints.md

<action>
Rewrite `quant-vault/30-vendors/entsog/endpoints.md` grouped by route
family:
- `### Operational data (/operationalData)` — all 19 datasets with
  Indicator column.
- `### Capacity Market Platform (/cmp*)` — 3 datasets.
- `### Other operational (/interruptions, /aggregatedData)` — 2
  datasets.
- `### Tariffs and bulletins` — tariffs, tariff_simulations,
  urgent_market_messages.
- `### Reference data` — 6 datasets.

Each row links the dataset key to `./datasets/<key>.md`. Update
`updated:` to 2026-05-08.
</action>

<acceptance_criteria>
- File contains 5 grouped sections.
- 33 dataset links to `./datasets/<key>.md`.
- `updated: 2026-05-08` in frontmatter.
</acceptance_criteria>

### Task 6 — Update README.md

<action>
Resolve any remaining `TODO` markers in
`quant-vault/30-vendors/entsog/README.md`:
- Confirm rate limit (vendor-published or "not stated; project default").
- Confirm status URL or note that ENTSOG does not publish a status page.
- Confirm auth (public, no key).
- Add `## Last validation` section linking to
  `entsog-VALIDATION.md`.
- Bump `updated: 2026-05-08`.
</action>

<acceptance_criteria>
- File contains zero occurrences of `TODO`.
- File contains `## Last validation` heading.
- `updated: 2026-05-08` in frontmatter.
</acceptance_criteria>

### Task 7 — Write entsog-VALIDATION.md

<action>
Write `.planning/phases/V1-vault-vendor-validation-and-docs/entsog-VALIDATION.md`
with the same structure as elexon-VALIDATION.md (frontmatter + summary +
33-row table + curl-evidence section + cross-cutting deltas section).
</action>

<acceptance_criteria>
- File exists with `total_datasets: 33`.
- Per-dataset table has 33 rows.
- Summary counts add to 33.
</acceptance_criteria>

## Verification

| Check | Pass condition |
|-------|----------------|
| 33 dataset pages | files exist at absolute vault path |
| README has no TODO | grep -c TODO → 0 |
| VALIDATION rows | 33 rows in entsog-VALIDATION.md |
| Operational pointDirection | grep `pointDirection` in operationalData pages |

## Deferred

- Domain-specific ENTSOG silver schemas — already a backlog item; this
  plan only documents existing generic schema.
