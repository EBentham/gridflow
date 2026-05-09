---
phase: V2
plan_id: V2-PLAN-F-aggregate
slug: v2-aggregate-and-close-out
status: draft
milestone: v0.10
wave: 3
severity: ADMIN
depends_on:
  - V2-PLAN-A-elexon-freq-fix
  - V2-PLAN-B-neso-region-period-fields
  - V2-PLAN-C-elexon-misc
  - V2-PLAN-D-entsoe-cleanup
  - V2-PLAN-E-entsog-and-ngeso
autonomous: true
files_modified:
  - .planning/phases/V2-v1-vendor-bugfix-followups/V2-VALIDATION.md  # new aggregate report
  - .planning/STATE.md
  - .planning/ROADMAP.md  # mark v0.10 complete; backlog edits
  - .planning/phases/V1-vault-vendor-validation-and-docs/elexon-VALIDATION.md
  - .planning/phases/V1-vault-vendor-validation-and-docs/neso-VALIDATION.md
  - .planning/phases/V1-vault-vendor-validation-and-docs/entsoe-VALIDATION.md
  - .planning/phases/V1-vault-vendor-validation-and-docs/entsog-VALIDATION.md
requirements: []  # close-out plan; verifies all V2-FIX-* requirements
---

# V2 Plan F — Aggregate and Close-out

## Goal

Aggregate the V2 wave-1 and wave-2 outcomes into a single
`V2-VALIDATION.md` report, ensure each per-vendor V1 VALIDATION.md has
its `## V2 re-validation` section, update `STATE.md` and `ROADMAP.md`
to mark v0.10 complete, and queue any new backlog items uncovered by
plans A–E.

This plan does not touch source code. It runs after every wave 1 + wave
2 plan has committed.

## must_haves

1. `.planning/phases/V2-v1-vendor-bugfix-followups/V2-VALIDATION.md`
   exists, summarises every wave-1 + wave-2 fix with commit SHA, scope
   of change, and re-validation outcome.
2. Every per-vendor V1 `<vendor>-VALIDATION.md` has its `## V2 re-validation`
   section (verified — written by plans A–E; F sanity-checks they are
   present and consistent).
3. `.planning/STATE.md` is updated:
   - Active phase pointer moves from V1 to V2.
   - Decisions block records ADR-019 (and ADR-020 if used).
   - "Production code bugs surfaced" block from V1 STATE moves into the
     V2 results.
4. `.planning/ROADMAP.md`:
   - v0.10 milestone marked complete (top of file + the `<details>`
     section toggled).
   - V2 phase row marked `[x]` with completion date.
   - Backlog section gains any new items from plans A–E (e.g. "Bronze
     re-ingest of historical `freq` after V2-A param fix"; "Bronze
     re-ingest of NESO regional silver for the 5 affected datasets").
   - Items from V1 deferred-to-V2 list (if any) are removed (none in
     this case — V2 absorbs the V1 Implementation deltas, not deferred
     items).
5. `uv run pytest -m "not live and not slow" -x -q` passes on the
   final V2 commit (i.e. the test suite is green at end of wave 3).
6. `uv run ruff check src/ tests/` and
   `uv run mypy --strict src/gridflow/` exit `0`.
7. The V2 commit chain is clean: 5 fix commits (one per wave-1 + wave-2
   plan) + 1 docs commit (this aggregate plan).

## Tasks

### Task 1 — Sanity-check that wave 1 + wave 2 are complete

<read_first>
- .planning/phases/V2-v1-vendor-bugfix-followups/V2-PLAN-A-elexon-freq-fix.md
- .planning/phases/V2-v1-vendor-bugfix-followups/V2-PLAN-B-neso-region-period-fields.md
- .planning/phases/V2-v1-vendor-bugfix-followups/V2-PLAN-C-elexon-misc.md
- .planning/phases/V2-v1-vendor-bugfix-followups/V2-PLAN-D-entsoe-cleanup.md
- .planning/phases/V2-v1-vendor-bugfix-followups/V2-PLAN-E-entsog-and-ngeso.md
</read_first>

<action>
1. `git log --oneline | grep -E "^[a-f0-9]+ (fix|docs)\(V2-"` — list
   every V2 commit.
2. Verify there are at least 5 commits with prefixes `fix(V2-A):`,
   `fix(V2-B):`, `fix(V2-C):`, `fix(V2-D):`, `fix(V2-E):` (in any
   order).
3. For each V2 commit, capture the SHA and the affected file count for
   the V2-VALIDATION.md aggregate (Task 3).
4. For each per-vendor V1 VALIDATION.md, grep for
   `## V2 re-validation` — must be present:
   ```bash
   grep -l "## V2 re-validation" .planning/phases/V1-vault-vendor-validation-and-docs/*-VALIDATION.md
   ```
   Expect output to include `elexon-VALIDATION.md`,
   `neso-VALIDATION.md`, `entsoe-VALIDATION.md`,
   `entsog-VALIDATION.md`.
5. Run the test suite:
   `uv run pytest -m "not live and not slow" -x -q`. Must be green.

If any check fails, halt and append a single-line failure note to
`.planning/phases/V2-v1-vendor-bugfix-followups/V2-PLAN-F-RESULTS.md`,
then stop.
</action>

<acceptance_criteria>
- 5 V2 fix commits exist on the branch.
- 4 per-vendor VALIDATION.md files have their V2 re-validation section.
- The fast test suite is green.
</acceptance_criteria>

### Task 2 — Optionally re-run a sweep test

<action>
This task is gated by time budget. If wave 2 finished cleanly:

1. Run the live-marked Elexon/NESO/ENTSOG smoke tests (if any exist)
   to confirm post-fix live behaviour:
   ```bash
   uv run pytest -m live -x -q -k "freq or regional_intensity_fw24h or 404_no_result"
   ```
   These tests are opt-in (the `live` marker is excluded from default
   runs). If any fails, the V2 fix did not hold; document the
   regression and halt.

2. If no live tests exist for the V2 fixes (likely — V2 added respx
   mocks, not live tests), skip this task and note in V2-VALIDATION.md
   that future live monitoring is queued as backlog.
</action>

<acceptance_criteria>
- Either live sweep passed, or task is documented as "skipped — no
  live tests added in V2; respx coverage relied upon".
</acceptance_criteria>

### Task 3 — Author V2-VALIDATION.md aggregate report

<read_first>
- All 4 V1 per-vendor VALIDATION.md files (their new V2 re-validation
  sections)
- The 5 V2 fix commits (`git log --pretty=full -- src/gridflow/`)
</read_first>

<action>
Write `.planning/phases/V2-v1-vendor-bugfix-followups/V2-VALIDATION.md`:

```markdown
---
phase: V2
milestone: v0.10
validated: 2026-05-09
total_fixes: 7
plans: [V2-PLAN-A, V2-PLAN-B, V2-PLAN-C, V2-PLAN-D, V2-PLAN-E]
---

# V2 — V1 Vendor Bug-fix Follow-ups (Consolidated)

Aggregates the wave-1 (HIGH severity) and wave-2 (MED + LOW) bug
fixes that closed the production-code Implementation deltas surfaced
in V1's per-vendor VALIDATION reports.

## Summary

| Plan | Wave | Severity | Status | Fix commit | Vendor |
|------|------|----------|--------|------------|--------|
| V2-PLAN-A | 1 | HIGH | DONE | <sha> | Elexon (`freq`) |
| V2-PLAN-B | 1 | HIGH | DONE | <sha> | NESO (5 regional datasets) |
| V2-PLAN-C | 2 | MED | DONE | <sha> | Elexon (`remit`, `soso`, `system_prices`) |
| V2-PLAN-D | 2 | MED+LOW | DONE | <sha> | ENTSOE (A09 dedup + B2 hygiene) |
| V2-PLAN-E | 2 | LOW | DONE | <sha> | ENTSOG (404 short-circuit) + ngeso triage |

Total live re-validation calls: ~12 (per-plan curls, throttled
identically to V1's `curl --ssl-no-revoke`).

## Per-fix details

### V2-FIX-01 (Elexon `freq`) — HIGH

V1 evidence: `freq` connector sent `publishDateTimeFrom/To`; Swagger
declared `measurementDateTimeFrom/To`. API silently ignored wrong
names and returned latest 5761 samples regardless of window.

V2 fix: explicit `from_param`/`to_param` override in
`ENDPOINTS["freq"]`. Live re-validation 2026-05-09 narrow window
returned correctly bounded ~120 rows in 1h window.

Commit: `<sha>`. See V1 elexon-VALIDATION.md `## V2 re-validation`.

### V2-FIX-02 (NESO `_rows_from_region_period`) — HIGH

V1 evidence: 5 period-keyed regional datasets (`regional_current`,
`regional_intensity_fw24h`, `regional_intensity_fw48h`,
`regional_intensity_pt24h`, `regional_intensity`) wrote rows with
null `forecast`, `actual`, `index`, `fuel`, `perc` because the silver
function read those fields from `period` rather than `region`.

V2 fix: read from `region` first, fall back to `period`. Region-keyed
routes unchanged. Two regression fixtures (period-keyed +
region-keyed) and tests added.

Commit: `<sha>`. See V1 neso-VALIDATION.md `## V2 re-validation`.

### V2-FIX-03 (Elexon REMIT/SOSO 1-day cap) — MED

V1 evidence: HTTP 400 at default `max_chunk_hours=24` due to
undocumented vendor 1-day cap.

V2 fix: explicit `max_chunk_hours=23` on both endpoints. 23h window
returns 200; 25h window returns 400 (boundary confirmed
2026-05-09).

Commit: `<sha>`.

### V2-FIX-04 (Elexon `system_prices.priceDerivationCode`) — MED

V1 evidence: live API returned `priceDerivationCode = "N"` not in
the schema's `^(II|SF|R[1-3]|RF|DF)$` regex.

V2 fix: <Option α: regex + enum extended to include "N" (and any
other observed codes); meaning documented inline | Option β: field
mapping corrected — `priceDerivationCode` no longer maps to
`run_type`>.

Commit: `<sha>`.

### V2-FIX-05 (ENTSOE A09 dedup) — MED

V1 evidence: `commercial_schedules` and `commercial_schedules_net_positions`
share identical EntsoeDocType, return identical XML.

V2 fix: ADR-019 — drop `commercial_schedules_net_positions` (default
choice; derive-net-positions kept as backlog).

Commit: `<sha>`. See `docs/DECISION_LOG/ADR-019-entsoe-a09-dedup.md`.

### V2-FIX-06 (ENTSOE B2 cleanup batch) — LOW

V1 evidence: A37/A15 hardcoded `offset=0` (silent truncation at 4800
TS); A87 schedule daily where monthly is correct; A87 silver missing
`Reason.code` exposure; `area_name` declared but never populated;
`psrType` not in `optional_params`; `DEFAULT_ZONES` GB/EU-centric.

V2 fix: 6 sub-tasks, see V2-PLAN-D Sub-tasks 5a–5f. Some of these may
have routed to backlog rather than code change; the table below
records the disposition per sub-item.

| Sub-item | Disposition | Notes |
|----------|-------------|-------|
| A37/A15 pagination | Code fix | Iterate offset to cap 9600 |
| A87 schedule | Code fix | `monthly` + `max_query_days: 31` |
| A87 silver Reason.code | Code fix | New `reason_code` column |
| `area_name` field | <populate \| remove> | Per Sub-task 2d decision |
| `psrType` optional_params | Code fix | Added to identified endpoints |
| DEFAULT_ZONES | <touched \| backlog> | Per Sub-task 2f decision |

Commit: `<sha>`.

### V2-FIX-07 (ENTSOG 404 short-circuit) — LOW

V1 evidence: `@RETRY_POLICY` retries 404 + `{"message":"No result found"}`
up to N times. Vendor empty convention is HTTP 404, not HTTP 200 +
empty body.

V2 fix: short-circuit 404+empty body in `EntsogConnector._request` so
RETRY_POLICY does not consume budget; non-empty 404s preserve the
existing retry path. Two respx-mocked tests prove both branches.

Commit: `<sha>`.

### V2-TRIAGE-01 (`connectors/ngeso/`) — LOW

V1 evidence: empty placeholder package (`__init__.py` only).

V2 disposition: <deleted | retained per ADR-020>.

Commit: `<sha>`.

## Live re-validation summary

Per V2-CONTEXT, every fixed dataset was re-validated against the same
vendor API V1 used, using `curl --ssl-no-revoke`. Each per-vendor V1
`<vendor>-VALIDATION.md` has a `## V2 re-validation` section
appended; this aggregate links to those.

| Vendor | V1 status | V2 deltas applied | V2 re-validation rows |
|--------|-----------|-------------------|------------------------|
| Elexon | 33 PASS / 0 EMPTY / 0 FAIL | 3 (freq, remit/soso, system_prices) | freq narrow-window correct; remit/soso 23h pass / 25h fail; system_prices regex round-trip clean |
| NESO | 33 PASS / 0 EMPTY / 0 FAIL | 1 (5 datasets via 1 silver fix) | All 5 period-keyed datasets now populate carbon/mix |
| ENTSOE | 9 PASS / 39 EMPTY / 0 FAIL | 2 (A09 dedup + B2 hygiene) | Kept commercial_schedules unchanged on live; B2 fixes mocked |
| ENTSOG | 29 PASS / 4 EMPTY / 0 FAIL | 1 (404 short-circuit) | call_count = 1 for empty 404; retry preserved for genuine 404 |
| GIE | 7 PASS / 0 EMPTY / 0 FAIL | 0 | n/a — no V1 issues to fix |
| Open-Meteo | 2 PASS / 0 EMPTY / 0 FAIL | 0 | n/a |

## Test status at close

```
$ uv run pytest -m "not live and not slow" -x -q
<paste passing summary>
$ uv run mypy --strict src/gridflow/
<paste passing summary>
$ uv run ruff check src/ tests/
<paste passing summary>
```

## Backlog items added by V2

These items were uncovered during V2 work but are out of V2 scope.
Each is added to `.planning/ROADMAP.md` Backlog section:

| Item | Source | Notes |
|------|--------|-------|
| Historical `freq` bronze re-ingest after V2-A param fix | V2-A | Existing bronze captured "latest 5761" not the requested window — re-ingest needed for correct historical data |
| Historical NESO regional silver re-ingest for 5 affected datasets | V2-B | Existing silver carries null carbon/mix — re-run silver on existing bronze (or re-ingest if bronze missing fields) |
| ENTSOE A09 derive `net_position_mw` (Option B not taken in V2) | V2-D / ADR-019 | Keep both keys, pair zone-pair directions, emit signed net position. Useful for cross-border net flow analysis |
| ENTSOE `_RESOLUTION_MAP` calendar-correct `P1M`/`P1Y` | V1 entsoe-VALIDATION Recommendations §5 | Approximating month=30d, year=365d affects load_forecast_monthly, load_forecast_yearly |
| ENTSOE `activated_balancing_prices` reserve-type widening | V1 entsoe-VALIDATION Recommendations §6 | Connector currently fixes businessType=A96 (aFRR); silver schema supports FCR/aFRR/mFRR/RR |
| ENTSOE Pydantic schema vs silver Parquet column drift (B3) | V1 entsoe-VALIDATION §13 | EntsoeCrossborderFlow / EntsoeNetTransferCapacity declare narrower fields than transformer outputs |
| Manual ENTSOE Guide.pdf download | V1 entsoe-VALIDATION Recommendations §1 | CDN protection blocks programmatic fetch; human download recommended |
| ENTSOE GB pre-Brexit window re-validation | V1 entsoe-VALIDATION Recommendations §2 | Distinguish "permanently not published" from "publication-lag" via 2019/2020 GB window |
| GIE ALSI LNG validation | V1 V0.7 deferred | Backlog, unchanged |
| Vault directory rename `open-meteo` → `openmeteo` | V1 V0.7 deferred | Backlog, unchanged |

## Metadata

- Total live HTTP calls: ~12 (well below the V1 ~50 + per-vendor budget).
- Throttle: same as V1 — Elexon 0.6s, NESO 0.2s, ENTSOG 1s, ENTSOE 1s.
- Tools used: `curl --ssl-no-revoke -fsS`, Python 3.13 stdlib + polars
  for re-validation pipelines.
- Avast `--ssl-no-revoke` workaround used throughout.
- `gsd-sdk` unavailable — this report and STATE.md updated by direct
  edits.
```
</action>

<acceptance_criteria>
- `V2-VALIDATION.md` exists with the structure above; commit SHAs are
  filled in (not `<sha>` placeholders).
- The disposition column for V2-FIX-06 sub-items reflects actual
  Sub-task 2 decisions from PLAN-D.
- Test status at close is real captured output.
</acceptance_criteria>

### Task 4 — Update `.planning/STATE.md`

<read_first>
- .planning/STATE.md (current state, after V1 close)
</read_first>

<action>
Update the Current Position block:

Replace:

```yaml
---
milestone: v0.9
milestone_name: Vault Vendor Validation And Docs
status: complete
progress:
  phases_total: 1
  phases_complete: 1
  plans_total: 10
  plans_complete: 10
---
```

with:

```yaml
---
milestone: v0.10
milestone_name: V1 Vendor Bug-fix Follow-ups
status: complete
progress:
  phases_total: 1
  phases_complete: 1
  plans_total: 6
  plans_complete: 6
---
```

Replace the "Current Position" prose with the V2 close-out summary.
Move the V1 close-out into a "Prior milestone" block (V1 was the
prior; F0 moves further down).

Add to Decisions block:
- ADR-019: ENTSOE A09 commercial_schedules_net_positions deprecated.
- ADR-020: <if used>
- V2 fixed Elexon `freq` connector parameter names — `freq` ingest now
  honours the requested window.
- V2 fixed NESO regional carbon/mix silver bug — 5 period-keyed
  datasets now populate forecast/actual/index/fuel/perc.
- V2 capped Elexon REMIT/SOSO `max_chunk_hours=23` to honour
  undocumented vendor 1-day query cap.
- V2 widened Elexon `system_prices.run_type` schema to accept
  live-observed `priceDerivationCode = "N"`.
- V2 ENTSOG short-circuits HTTP 404 + `{"message":"No result found"}`
  as the vendor's empty convention.
- V2 <deleted | retained> empty `connectors/ngeso/` placeholder.

Add to Roadmap Evolution block:
- v0.10 started: V1 vendor bug-fix follow-ups; single phase V2 with
  6 plans across 3 waves.
- V2 completed: HIGH (Elexon freq, NESO regional), MED (Elexon
  REMIT/SOSO + system_prices, ENTSOE A09), LOW (ENTSOE B2 batch,
  ENTSOG 404, ngeso) all closed.

Update "Blockers": none new.
</action>

<acceptance_criteria>
- `.planning/STATE.md` frontmatter shows v0.10 complete with 1/1
  phases and 6/6 plans.
- New decisions are present in the Decisions block.
- v0.10 added to Roadmap Evolution.
</acceptance_criteria>

### Task 5 — Update `.planning/ROADMAP.md`

<read_first>
- .planning/ROADMAP.md (especially the v0.10 section authored at the
  start of V2 planning)
</read_first>

<action>
1. Top-of-file Milestones block: change v0.10 from "Current" to
   "Complete":
   - Was: `- Current **v0.10-v1-vendor-bugfix-followups** - Fix the
     production bugs surfaced (but not patched) by V1 across Elexon,
     NESO, ENTSOE, ENTSOG (V2)`
   - To: `- Complete **v0.10-v1-vendor-bugfix-followups** - Fix the
     production bugs surfaced (but not patched) by V1 across Elexon,
     NESO, ENTSOE, ENTSOG (V2) (completed 2026-05-09)`

2. v0.10 `<details>` section:
   - Change `<details open>` to `<details>`.
   - Change `IN PLANNING 2026-05-09` to `COMPLETED 2026-05-09`.
   - Change `Phase V2: V1 vendor bug-fix follow-ups - in planning
     2026-05-09` to `Phase V2: V1 vendor bug-fix follow-ups -
     completed 2026-05-09` and toggle `[ ]` to `[x]`.
   - Update each plan checkbox `[ ]` to `[x]`.

3. Backlog section: append the rows from V2-VALIDATION.md "Backlog
   items added by V2" (Task 3) — do not duplicate existing rows.
</action>

<acceptance_criteria>
- ROADMAP.md `Milestones` shows v0.10 as Complete with completion date.
- v0.10 `<details>` section is closed (no `open` attribute) and shows
  COMPLETED 2026-05-09.
- All V2 plan checkboxes are `[x]`.
- Backlog table grew by N rows (V2's new items).
</acceptance_criteria>

### Task 6 — Commit

<action>
Stage and commit:

```
git add .planning/phases/V2-v1-vendor-bugfix-followups/V2-VALIDATION.md
git add .planning/STATE.md
git add .planning/ROADMAP.md
```

Conventional-commit message:

```
docs(V2-F): aggregate V2 close-out — VALIDATION report, STATE, ROADMAP

Summarises wave 1 (HIGH: Elexon freq, NESO regional) and wave 2
(MED: Elexon REMIT/SOSO + system_prices, ENTSOE A09 dedup; LOW:
ENTSOE B2 batch, ENTSOG 404 short-circuit, ngeso triage).

Each fix has a re-validation row in its V1 per-vendor VALIDATION.md.
ROADMAP marked v0.10 complete; STATE.md decisions and roadmap
evolution updated. Backlog absorbs V2-uncovered items (historical
re-ingest, A09 derive net positions, ENTSOE schema drift, etc.).

Closes V2.
```
</action>

<acceptance_criteria>
- `git log --oneline -1` shows commit prefix `docs(V2-F):`.
- `git status` clean.
- The full V2 commit chain on the branch reads (top to bottom):
  - `docs(V2-F): aggregate V2 close-out`
  - `fix(V2-E): ENTSOG short-circuits 404+empty body...`
  - `fix(V2-D): ENTSOE A09 dedup + B2 cleanup batch`
  - `fix(V2-C): elexon REMIT/SOSO honour 1-day cap; system_prices...`
  - `fix(V2-B): NESO _rows_from_region_period reads carbon/mix...`
  - `fix(V2-A): elexon freq sends measurementDateTimeFrom/To...`
  - (then earlier docs(V2): scaffolding commits from this planning
    phase + the V1 commit chain)
</acceptance_criteria>

## Risks / known-unknowns

- **Branch merge.** The V2 commit chain sits on top of the V1 commits
  on `claude/lucid-mccarthy-9ed3e0`. Merging to `master` will bring
  both V1 and V2 in. The PR description should call this out.
- **Live test markers.** V2 added respx-mocked tests, not live tests.
  Live monitoring of the V2 fixes is an open question (V1 raised
  scheduled-monitoring as a backlog item; not changed by V2).

## Verification

Final guardrail before closing V2:

```bash
uv run pytest -m "not live and not slow" -x -q
uv run mypy --strict src/gridflow/
uv run ruff check src/ tests/
git log --oneline | grep -E "fix\(V2-[A-E]\):" | wc -l  # must be >= 5
git log --oneline | grep "docs(V2-F):" | wc -l         # must be 1
```

All four must pass.
