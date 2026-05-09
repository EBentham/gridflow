# Phase V2: V1 Vendor Bug-fix Follow-ups - Context

**Gathered:** 2026-05-09
**Status:** Ready for planning
**Source:** Phase V1 close-out reports (per-vendor VALIDATION.md, consolidated entsoe-VALIDATION.md, V1-CONTEXT.md) + user scoping conversation

<domain>
## Phase Boundary

V2 is a **production-code bug-fix** phase. It modifies connector, schema,
silver-transformer, and config source code to close out the bugs explicitly
flagged "out of V1 scope" in the V1 cross-cutting Implementation deltas.

In scope:
- Modify `src/gridflow/connectors/<vendor>/`,
  `src/gridflow/silver/<vendor>/`, `src/gridflow/schemas/<vendor>.py`, and
  `config/sources.yaml` for each documented bug.
- Re-run a targeted live validation curl for every fixed dataset against
  the same vendor API V1 used (V1-CONTEXT pattern: `curl --ssl-no-revoke`).
  Append the new evidence to the existing per-vendor V1 `<vendor>-VALIDATION.md`
  under a `## V2 re-validation` section — do **not** rewrite V1's tables.
- Add at least one regression test per fix (mocked-shape `respx` for the
  request-shape fixes, fixture-backed silver test for the silver fixes,
  Pydantic `pytest.raises` / `assert obj` for schema fixes).
- Update vault dataset pages (`quant-vault/30-vendors/<vendor>/datasets/<key>.md`)
  whose `## Implementation delta` block recorded the bug — lift the delta into
  the page body and bump `last_verified` to V2's date.
- Refresh the per-vendor `endpoints.md` quick-summary line if a parameter
  name or path changes (Elexon `freq`).

Out of scope:
- Brand-new endpoint coverage (any new connector dataset, new silver family,
  new vendor) — V2 is strictly bug-fix.
- Bitemporal lineage changes (F0 territory).
- ENTSOE `Guide.pdf` programmatic fetch — left as a manual user action per
  V1 recommendation.
- ENTSOE GB pre-Brexit re-validation window — left as a backlog item.
- Vault directory rename `open-meteo` → `openmeteo` — backlog item.
- Fixture regeneration unless a fix forces it (silver tests depend on
  fixture stability).
- Adding Pydantic schemas to the 21 Elexon datasets that lack one — that's
  a separate consistency phase per V1 recommendation 4.
- ENTSOE `activated_balancing_prices` reserve-type widening (FCR/aFRR/mFRR/RR)
  per V1 recommendation 6 — backlog candidate.
- ENTSOE parser `_RESOLUTION_MAP` calendar-correctness for `P1M`/`P1Y` per
  V1 recommendation 5 — backlog candidate.
- GB→Elexon cross-reference table in vendor README per V1 recommendation 8 —
  vault docs phase, separate.

</domain>

<decisions>
## Implementation Decisions

### Phase shape (locked)

- **Single phase V2 with 6 plans across 3 waves**:
  - Wave 1 (HIGH severity, parallel): two plans, must close before wave 2
    starts so MED/LOW retests can baseline against fixed HIGH behaviour.
  - Wave 2 (MED + LOW severity, parallel): three plans grouped per-vendor.
  - Wave 3 (close-out aggregator): one plan.
- The plan IDs (locked):
  - `V2-PLAN-A-elexon-freq-fix` (Wave 1, HIGH)
  - `V2-PLAN-B-neso-region-period-fields` (Wave 1, HIGH)
  - `V2-PLAN-C-elexon-misc` (Wave 2, MED — REMIT/SOSO chunk + system_prices regex)
  - `V2-PLAN-D-entsoe-cleanup` (Wave 2, MED + LOW — A09 dedup + B2 cleanup batch)
  - `V2-PLAN-E-entsog-and-ngeso` (Wave 2, LOW — RETRY_POLICY 404 short-circuit + ngeso triage)
  - `V2-PLAN-F-aggregate` (Wave 3, close-out)

### Worktree (locked)

V2 ships on the same `claude/lucid-mccarthy-9ed3e0` worktree as V1
(`C:\Users\Bobbo\OneDrive\Desktop\Python\gridflow\.claude\worktrees\lucid-mccarthy-9ed3e0`).
Reasons: V1 already shipped 5 commits ahead of master on this branch but
has not been merged; V2 patches the bugs V1 surfaced; bundling the
documentation pass and the production-code follow-ups onto one branch keeps
the merge story coherent. V2 commits use the `feat(V2-…):` / `fix(V2-…):`
prefix; V1 commits keep the `docs(V1-…):` prefix already in history.

### Live API access (locked — same as V1)

- API keys (Elexon, ENTSOE, GIE) live in
  `C:/Users/Bobbo/OneDrive/Desktop/Python/gridflow/.env`. Worktree-local
  `.env` is gitignored and may already be present from V1; if missing, every
  plan's pre-flight task copies it from the main repo.
- ENTSOG, NESO, Open-Meteo are public, no key needed.
- **Avast TLS quirk (locked, inherited from V1-CONTEXT):** every live call
  uses `curl --ssl-no-revoke -fsS`. Python `httpx` directly fails with
  `CERTIFICATE_VERIFY_FAILED: unable to get local issuer certificate` on
  this Windows workstation. Test code that uses `respx` mocks is fine
  because `respx` intercepts before TLS — only **live** calls need curl.

### Pre-flight smoke test (mandatory in every plan)

```
curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" https://api.carbonintensity.org.uk/intensity
```

Expected output: `200`. If this fails, halt the plan and write a single-line
error to the plan's results section, then stop. Every subsequent live call
will share the same root cause (Avast root cert / Windows cert store
revocation block).

### Re-validation evidence (locked)

For each fixed dataset, V2 must append a `## V2 re-validation` section to
the existing V1 `.planning/phases/V1-vault-vendor-validation-and-docs/<vendor>-VALIDATION.md`
file. The new section records:
- Date + commit SHA of the fix.
- Identical curl command to the V1 evidence (same params, same throttle).
- New HTTP status, byte count, row count, time.
- Diff against V1 row count (especially the Elexon `freq` 5761→correct
  delta, which is the key proof the bug is gone).

V2 does **not** rewrite V1's tables. The existing V1 row stays as-is for
historical context; the new V2 row sits below it.

### Vault page edits (locked)

For each fix that touches a documented Implementation delta in a vault
dataset page, the editing plan must:
1. Move the delta from `## Implementation delta` into the page body where
   appropriate (e.g. correct param names go into the `## API endpoint`
   section).
2. Add a single-line entry under `## Changelog` (or create the section if
   missing) noting `2026-05-09 — fixed in V2 — see commit <SHA>`.
3. Bump frontmatter `last_verified: 2026-05-09`.
4. **Do not rewrite the page from scratch** — edit-in-place only.

Vault paths use the absolute `VENDOR_ROOT` from V1-CONTEXT:
`C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\<vendor>\`.

### Test discipline (locked)

- **No live calls in tests.** Live validation is curl in plan tasks.
  Repository tests use `respx` for HTTP mocking (Elexon, ENTSOE, ENTSOG)
  and fixture-backed silver tests for transformer logic (NESO).
- **Fast suite passes:** `uv run pytest -m "not live and not slow" -x -q`
  must pass at the end of each wave.
- **Strict typing preserved:** `uv run mypy --strict` must remain clean.
- **Fixtures only regenerated when forced.** If a fix changes the silver
  output schema (e.g. NESO carbon-intensity rows for the 5 affected datasets
  start carrying real `forecast`/`actual`/`generationmix` values), update
  the affected fixtures and the regression test must compare against the
  new fixture, not the old null-filled one. Document the regen in the plan.

### Severity routing (locked)

| Severity | Source | Disposition |
|----------|--------|-------------|
| HIGH | V1 cross-cutting deltas Elexon §1 (freq) + NESO §2 (region-period) | Code fix mandatory in V2 |
| MED | V1 Elexon §3 (REMIT/SOSO 1-day cap), V1 Elexon (system_prices `priceDerivationCode` regex), V1 ENTSOE §6 (A09 dedup) | Code fix in V2 OR explicit `defer` ADR in `docs/DECISION_LOG/` |
| LOW | V1 ENTSOE B2 cleanup batch (A37/A15 pagination, A87 schedule, A87 silver Reason.code, area_name, psrType, DEFAULT_ZONES), V1 ENTSOG §1 (404 short-circuit), V1 ngeso placeholder | Code fix OR backlog row in `.planning/ROADMAP.md` |

### Pre-known investigation items (locked)

Each item is owned by a specific plan. The plans must investigate and
either fix or document a deliberate defer.

1. **Elexon `freq` Swagger source of truth (PLAN-A).** V1 evidence shows
   the API silently ignores wrong-named params and returns the latest
   ~5761 samples regardless of window. PLAN-A's pre-fix step is to
   re-confirm that Swagger declares `measurementDateTimeFrom/To` (per V1
   Implementation delta §1) by curling the Swagger JSON if reachable, OR
   by reading the V1 Bronze sample evidence already captured. The fix is
   the override of `from_param`/`to_param` on `ENDPOINTS["freq"]`. The
   regression test sends a known-narrow window and asserts the response
   data falls within that window.

2. **Elexon `system_prices.priceDerivationCode = "N"` (PLAN-C).** V1
   user notes flag that the live API returned a `priceDerivationCode` of
   `"N"` not currently in the schema's regex `^(II|SF|R[1-3]|RF|DF)$`.
   PLAN-C's investigation step is to:
   - Curl `/balancing/settlement/system-prices/2026-05-06` and capture
     the full set of `priceDerivationCode` values present.
   - Cross-reference Elexon docs (Swagger / developer portal) for the
     canonical value list.
   - Either expand the regex + `SettlementRunType` enum to include `N`
     (with a one-line comment explaining the meaning, e.g. "Not yet
     derived" or as documented), OR — if the value turns out to be a
     different field semantics — fix the field mapping in
     `silver/elexon/system_prices.py::_data_provider_columns` (line 67
     today, mapping `priceDerivationCode` → `run_type`).

3. **ENTSOE A09 dedup (PLAN-D).** V1 entsoe-VALIDATION §6 shows
   `commercial_schedules` and `commercial_schedules_net_positions` issue
   identical requests and return identical 5 296-byte XML payloads. PLAN-D
   must choose between two options and write a short ADR
   (`docs/DECISION_LOG/ADR-019-entsoe-a09-dedup.md`):
   - **Option A — drop one key.** Remove
     `commercial_schedules_net_positions` from
     `connectors/entsoe/endpoints.py` and `config/sources.yaml`;
     migration: existing silver tables remain valid, future ingest only
     writes one set.
   - **Option B — derive net positions.** Keep both keys but rewrite the
     `CommercialSchedulesNetPositionsTransformer` to pair the two
     directions per zone-pair and emit a signed `net_position_mw` column.
     Higher engineering cost but preserves a logical-net-position
     dataset.
   The default for V2 is **Option A (drop key)** because no downstream
   gold consumer currently uses the redundant net-positions silver
   table. Option B is recorded as a follow-up.

4. **ENTSOE B2 cleanup batch (PLAN-D).** V1 entsoe-VALIDATION §11 lists
   the cluster of small B2 follow-ups: A37/A15 hardcoded `offset=0`
   pagination, A87 schedule cadence (`daily` should be `monthly`), A87
   silver `Reason.code` field exposure, `area_name` field declared but
   never populated by the transformer, `psrType` not in `optional_params`,
   `DEFAULT_ZONES` GB/EU-centric. Each sub-item gets either a 1–2 line
   fix or a backlog row in V2's close-out aggregator.

5. **ENTSOG `@RETRY_POLICY` 404 short-circuit (PLAN-E).** V1
   entsog-VALIDATION §1 shows the vendor convention is HTTP 404 + body
   `{"message":"No result found"}` for empty datasets. The current
   `RETRY_POLICY` retries on `httpx.HTTPStatusError` and would retry
   404s up to N times before reraising. PLAN-E adds a body-shape check
   in `EntsogConnector._request` (or in a custom `RETRY_POLICY` factory
   for ENTSOG) so 404 + the empty-message body short-circuits to a
   fast empty-bronze with no retries.

6. **`connectors/ngeso/` (PLAN-E).** Single `__init__.py` only. PLAN-E's
   triage step: either add a comment-only deletion patch (with
   `git rm src/gridflow/connectors/ngeso/__init__.py` and a short ADR if
   the directory was provisional) OR write `docs/DECISION_LOG/ADR-020-ngeso-placeholder.md`
   recording why it stays. **Default: delete.**

### Non-goals (locked)

- V2 does not change the V1 vault pages' authoring template, structure, or
  authority hierarchy. V1's `gridflow-dataset-spec` skill discipline stays.
- V2 does not start `gridflow_models` work — that's the separate repo's
  F9 next phase.
- V2 does not touch the Open-Meteo two-host gotcha (V1 verified working;
  no bug surfaced).
- V2 does not touch the GIE AGSI pipeline (V1: 7 PASS / 0 EMPTY / 0 FAIL;
  no bugs surfaced).

### Claude's Discretion

- Choice of the regression-test mocked response payload — pick a payload
  derived from the V1 curl evidence (already captured in `.tmp/<vendor>-<dataset>.json`
  during V1 execution) where it remains on disk; otherwise build a
  representative XML/JSON from the V1 dataset page Bronze sample.
- Internal grouping of the B2 cleanup batch within PLAN-D — sub-task per
  sub-item is fine, but small touch-once edits (one line each for the
  pagination iterators and the A87 schedule cadence) can be bundled in a
  single sub-task labelled "B2 hygiene".
- ADR numbering (next free number under `docs/DECISION_LOG/`).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before fixing source code.**

### V1 close-out (read every plan's relevant sub-section)

- `.planning/phases/V1-vault-vendor-validation-and-docs/V1-CONTEXT.md` —
  Avast workaround, vault paths, rate limits, authority hierarchy.
- `.planning/phases/V1-vault-vendor-validation-and-docs/elexon-VALIDATION.md`
  §"Implementation deltas (cross-cutting)" — the canonical source for the
  Elexon `freq`, REMIT/SOSO, BOAL→BOALF, system_prices issues.
- `.planning/phases/V1-vault-vendor-validation-and-docs/neso-VALIDATION.md`
  §"Findings §2 Latent silver bug for the period-keyed regional family" —
  exact code-level description of the `_rows_from_region_period` fix.
- `.planning/phases/V1-vault-vendor-validation-and-docs/entsoe-VALIDATION.md`
  §"Cross-batch implementation deltas" §6 (A09 dedup), §11 (pagination),
  §"Recommendations" §3, §4, §5, §6, §7 (B2 batch).
- `.planning/phases/V1-vault-vendor-validation-and-docs/entsog-VALIDATION.md`
  §"Findings §1" (404 retry short-circuit).

### Project rules

- `CLAUDE.md` (repo root) — gridflow hard rules (settlement period 1..50,
  no pandas, polars only, parameterised SQL).
- `.planning/STATE.md` — project decisions and history. Read-only.
- `docs/DECISION_LOG/` — ADR home. PLAN-D and PLAN-E may add ADRs.

### Source files (read for cross-reference, modify per plan)

- Elexon:
  - `src/gridflow/connectors/elexon/endpoints.py` — `freq`, `remit`, `soso`
  - `src/gridflow/connectors/elexon/parsers.py` — `RUN_PRECEDENCE` map
  - `src/gridflow/silver/elexon/system_prices.py` — `_data_provider_columns`
    field mapping (line 67 maps `priceDerivationCode` → `run_type`)
  - `src/gridflow/schemas/elexon.py` — `SettlementRunType` enum,
    `ElexonSystemPrice.run_type` regex (line 34)
- NESO:
  - `src/gridflow/silver/neso/carbon_intensity.py::_rows_from_region_period`
    (line 257) — the exact function to patch
  - `src/gridflow/silver/neso/carbon_intensity.py::_extract_regional_rows`
    (line 227) — caller, distinguishes period-keyed vs region-keyed
- ENTSOE:
  - `src/gridflow/connectors/entsoe/endpoints.py` — A09 (commercial_schedules),
    A37 (balancing_energy_bids), A15 (procured_balancing_capacity)
    `extra_param` `offset=0` literals; `optional_params`; A87
    (`balancing_financial_expenses_income`) entry
  - `src/gridflow/silver/entsoe/h8_balancing.py` — A37/A15 transformer
    pagination iteration; A87 transformer Reason.code field
  - `src/gridflow/silver/entsoe/` — find `area_name` declaration; trace
    why it's never populated
  - `config/sources.yaml` — `entsoe.balancing_financial_expenses_income`
    schedule cadence
  - `src/gridflow/connectors/entsoe/zones.py` (or wherever `DEFAULT_ZONES`
    lives) — review the GB/EU-centric default list
- ENTSOG:
  - `src/gridflow/connectors/entsog/client.py` — `@RETRY_POLICY`-decorated
    `_request` (line 77); add 404-short-circuit
  - `src/gridflow/utils/retry.py` — read `RETRY_POLICY` to confirm
    `httpx.HTTPStatusError` retry surface area
- ngeso:
  - `src/gridflow/connectors/ngeso/__init__.py` (only file)

### Tests (read existing patterns; add regression tests)

- `tests/unit/connectors/test_elexon_endpoints.py` (or equivalent) —
  pattern for parametric ENDPOINTS-dict assertions
- `tests/integration/test_elexon_e2e.py` — for narrow-window respx
  fixtures
- `tests/integration/test_neso_silver.py` (or wherever NESO regional
  silver is tested) — extend with period-keyed fixtures from V1's
  captured `.tmp/neso-regional_intensity_fw24h.json`
- `tests/unit/silver/test_system_prices.py` — extend with `priceDerivationCode = "N"`
  case
- `tests/integration/test_entsog_e2e.py` — extend with 404-short-circuit
  retry-budget assertion
- `tests/fixtures/<vendor>/` — existing live response shapes; add new
  fixtures only if a fix forces the schema change

### V1 vault pages affected (edit-in-place per plan)

- Elexon: `freq.md`, `remit.md`, `soso.md`, `system_prices.md`
- NESO: `regional_current.md`, `regional_intensity_fw24h.md`,
  `regional_intensity_fw48h.md`, `regional_intensity_pt24h.md`,
  `regional_intensity.md`
- ENTSOE: `commercial_schedules.md` (and possibly drop / cross-link
  `commercial_schedules_net_positions.md`); A37/A15 dataset pages;
  `balancing_financial_expenses_income.md`
- ENTSOG: vendor `README.md` only (404 retry behaviour is a connector
  detail, not per-dataset)

</canonical_refs>

<specifics>
## Specific Ideas

### Bugs in scope, mapped to plans

| Severity | Bug | Plan | One-line fix |
|----------|-----|------|--------------|
| HIGH | Elexon `freq` connector sends `publishDateTimeFrom/To`; Swagger declares `measurementDateTimeFrom/To` | A | Override `from_param`/`to_param` on `ENDPOINTS["freq"]` |
| HIGH | NESO `_rows_from_region_period` reads carbon/mix from `period`; live API places them on `region` for period-keyed payloads | B | Read from `region` first, fall back to `period`; fix affects 5 datasets |
| MED | Elexon REMIT/SOSO max-1-day vendor cap not honoured by `max_chunk_hours=24` | C | Set `max_chunk_hours=23` on `ENDPOINTS["remit"]` and `ENDPOINTS["soso"]` |
| MED | Elexon `ElexonSystemPrice.run_type` regex `^(II\|SF\|R[1-3]\|RF\|DF)$` rejects live value `"N"` | C | Investigate; add `N` to enum + regex (or fix mapping) |
| MED | ENTSOE `commercial_schedules` and `commercial_schedules_net_positions` use IDENTICAL `EntsoeDocType` — registry duplication | D | Drop `commercial_schedules_net_positions` (default) OR derive net_position_mw (Option B); ADR |
| LOW | ENTSOE A37 `balancing_energy_bids` and A15 `procured_balancing_capacity` hardcode `offset=0` (no pagination iteration) — silently truncate at 4800 TS | D | Iterate offset until empty TimeSeries page returned |
| LOW | ENTSOE A87 `balancing_financial_expenses_income` schedule `daily, max_query_days: 1` — real cadence is monthly | D | Update `config/sources.yaml` to `schedule: monthly, max_query_days: 31` |
| LOW | ENTSOE A87 silver missing `Reason.code` exposure | D | Add `reason_code` column to A87 transformer output |
| LOW | ENTSOE `area_name` field declared but never populated by transformer | D | Trace; populate from area-domain reverse-lookup or remove field |
| LOW | ENTSOE `psrType` not in `optional_params` | D | Add `psrType` to relevant endpoint `optional_params` tuples |
| LOW | ENTSOE `DEFAULT_ZONES` GB/EU-centric | D | Review default list; recommend wider EU coverage for cross-border consumers |
| LOW | Elexon BOAL→BOALF doc-only fix (already correct in code) | (no plan — vault already updated in V1) | n/a |
| LOW | ENTSOG `@RETRY_POLICY` retries on 404 (vendor's documented empty convention) — wastes retry budget | E | Short-circuit 404 + `{"message":"No result found"}` body |
| LOW | `connectors/ngeso/` — empty placeholder, only `__init__.py` | E | Delete (default) OR keep with ADR |

### Re-validation curl matrix (locked)

For every fixed dataset, the plan must run a re-validation curl identical to
V1's evidence and append the result to V1's `<vendor>-VALIDATION.md`.

| Plan | Dataset | Curl (extends V1 evidence) |
|------|---------|----------------------------|
| A | Elexon `freq` | `curl --ssl-no-revoke -fsS "https://data.elexon.co.uk/bmrs/api/v1/datasets/FREQ?measurementDateTimeFrom=2024-01-01T00:00Z&measurementDateTimeTo=2024-01-01T03:00Z&format=json"` — expect ≈721 rows of Jan 2024 data, NOT 5761 of latest |
| B | NESO `regional_intensity_fw24h` | `curl --ssl-no-revoke -fsS "https://api.carbonintensity.org.uk/regional/intensity/2026-05-06T00:00Z/fw24h"` — pipe through `gridflow transform neso regional_intensity_fw24h` (or programmatic test); silver rows must have non-null `forecast_gco2_kwh`, `intensity_index`, `fuel`, `generation_percentage` |
| C | Elexon `remit` | `curl --ssl-no-revoke -fsS "https://data.elexon.co.uk/bmrs/api/v1/datasets/REMIT?publishDateTimeFrom=2026-05-06T00:00Z&publishDateTimeTo=2026-05-07T00:00Z"` — single 23-hour chunk now succeeds, no 400 |
| C | Elexon `soso` | same shape with SOSO |
| C | Elexon `system_prices` (regex) | `curl --ssl-no-revoke -fsS "https://data.elexon.co.uk/bmrs/api/v1/datasets/SYSTEM-PRICES/2026-05-06"` — Pydantic validation accepts every `priceDerivationCode` value present in payload |
| D | ENTSOE `commercial_schedules` | `curl --ssl-no-revoke -fsS "https://web-api.tp.entsoe.eu/api?securityToken=$ENTSOE_API_KEY&documentType=A09&in_Domain=10YGB----------A&out_Domain=10YFR-RTE------C&periodStart=202605060000&periodEnd=202605070000&contract_MarketAgreement.Type=A01"` — should still return 5 296-byte XML; only the registry has changed |
| E | ENTSOG empty 404 | `curl --ssl-no-revoke "https://transparency.entsog.eu/api/v1/operationalData?... methaneContent ... ITP-00005-exit"` — connector should observe 404 + empty-message and return empty bronze without retries (assert via test, not curl) |

### Deferred-item migration

Items currently in V1's `.planning/STATE.md` "Deferred Items" or in
ROADMAP Backlog that V2 absorbs:

- (none — V2 is for V1's Implementation deltas, which are not in V1's
  deferred list. The deferred list stays as-is.)

</specifics>

<deferred>
## Deferred Ideas

- ENTSOE `Guide.pdf` programmatic fetch — manual user download.
- ENTSOE `_RESOLUTION_MAP` calendar correctness for `P1M`/`P1Y` — backlog.
- ENTSOE `activated_balancing_prices` reserve-type widening (FCR / aFRR /
  mFRR / RR) — backlog.
- Elexon Pydantic schema coverage for the 21 datasets without one — backlog.
- Vault directory rename `open-meteo` → `openmeteo` — backlog (touches
  vault, not gridflow).
- GB→Elexon cross-reference table in ENTSOE README — vault docs phase.
- GIE ALSI LNG validation (still deferred per V0.7).
- A09 Option B (derive `net_position_mw` from paired directions) — recorded
  in V2 ADR if Option A is taken; itself a backlog item.

</deferred>

---

*Phase: V2-v1-vendor-bugfix-followups*
*Context gathered: 2026-05-09 via direct user scoping (no /gsd-discuss-phase) + V1 close-out reports*
