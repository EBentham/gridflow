---
phase: V2
plan_id: V2-PLAN-D-entsoe-cleanup
slug: entsoe-cleanup-batch
status: draft
milestone: v0.10
wave: 2
severity: MEDIUM_AND_LOW
depends_on:
  - V2-PLAN-A-elexon-freq-fix
  - V2-PLAN-B-neso-region-period-fields
autonomous: false  # ADR-019 + ADR-020 are user-decision moments
files_modified:
  - src/gridflow/connectors/entsoe/endpoints.py
  - src/gridflow/silver/entsoe/h6_market.py  # commercial_schedules transformer
  - src/gridflow/silver/entsoe/h8_balancing.py  # A37 / A15 / A87 transformers
  - src/gridflow/silver/entsoe/  # area_name population — find owning module
  - src/gridflow/connectors/entsoe/  # DEFAULT_ZONES location — find via grep
  - config/sources.yaml  # A87 schedule cadence; possibly remove commercial_schedules_net_positions
  - docs/DECISION_LOG/ADR-019-entsoe-a09-dedup.md  # new ADR
  - tests/integration/test_entsoe_e2e.py
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\datasets\commercial_schedules.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\datasets\commercial_schedules_net_positions.md  # delete or annotate
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\datasets\balancing_energy_bids.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\datasets\procured_balancing_capacity.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\datasets\balancing_financial_expenses_income.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\entsoe\endpoints.md
  - .planning/phases/V1-vault-vendor-validation-and-docs/entsoe-VALIDATION.md
requirements:
  - V2-FIX-05  # A09 commercial_schedules dedup
  - V2-FIX-06  # B2 cleanup batch
---

# V2 Plan D — ENTSOE Cleanup Batch (A09 dedup + B2 hygiene)

## Goal

Resolve the cluster of ENTSOE follow-ups V1 surfaced but did not fix:

1. **A09 `commercial_schedules` registry duplication** (V2-FIX-05, MED).
   `commercial_schedules` and `commercial_schedules_net_positions` use
   identical `EntsoeDocType` and return identical XML payloads (V1
   entsoe-VALIDATION §6). Resolve via ADR-019: drop one key (default) or
   derive a true signed `net_position_mw`.

2. **B2 cleanup batch** (V2-FIX-06, LOW). Six small follow-ups from V1
   entsoe-VALIDATION §11 + Recommendations §3, §4, §5, §6, §7:
   - 2a. A37 `balancing_energy_bids` and A15 `procured_balancing_capacity`
     hardcode `offset=0` — silently truncate at 4800 TimeSeries.
   - 2b. A87 `balancing_financial_expenses_income` schedule should be
     monthly (currently daily), `max_query_days` adjust accordingly.
   - 2c. A87 silver transformer missing `Reason.code` exposure (silver
     output drops the document-level Reason block).
   - 2d. `area_name` field declared in some silver schema but never
     populated by the transformer.
   - 2e. `psrType` not in `optional_params` for the relevant endpoints.
   - 2f. `DEFAULT_ZONES` GB/EU-centric review.

## must_haves (goal-backward verification)

1. `docs/DECISION_LOG/ADR-019-entsoe-a09-dedup.md` exists, records
   decision, lists alternatives, names winner, includes rollback plan.
2. The chosen A09 fix is applied:
   - **Default — drop key.** `commercial_schedules_net_positions` is
     removed from `connectors/entsoe/endpoints.py` `ENDPOINTS`,
     `config/sources.yaml`, and the relevant silver transformer
     registry entry. Existing silver Parquet under
     `silver/entsoe/commercial_schedules_net_positions/` left in place
     (historical artefact); add a short README noting the dataset is
     deprecated.
   - **Alternative — derive net positions.** Rewrite the
     `CommercialSchedulesNetPositionsTransformer` to pair the two
     directions per zone-pair and emit a real signed `net_position_mw`.
3. A37 `balancing_energy_bids` and A15 `procured_balancing_capacity`
   connector code iterates `offset` until an empty TimeSeries page is
   returned (or a documented max — say `offset=9600`). Confirmed via a
   respx-mocked test that simulates a 4800-TS first page + a 200-TS
   second page.
4. `config/sources.yaml` `entsoe.balancing_financial_expenses_income`
   has `schedule: monthly` (or whichever cadence the V2-PLAN-D
   investigation confirms — see Sub-task 2b) and `max_query_days: 31`.
5. A87 silver transformer in `src/gridflow/silver/entsoe/h8_balancing.py`
   exposes `reason_code` in its silver output. Mocked respx fixture +
   transformer test confirms the column is present and populated when
   the source XML carries a `Reason.code` element.
6. `area_name` field: traced (via grep) to its declaring module; either
   populated from area-domain reverse-lookup (best) or removed from the
   silver schema (acceptable — record decision in a one-line plan note).
7. `psrType` is in `optional_params` for the endpoints that V1's deltas
   identified as needing it (find via reading V1 deltas + the ENTSOE
   API guide reference — likely outage and unit endpoints).
8. `DEFAULT_ZONES` (location TBD via Sub-task 2f) is reviewed; if the
   list is GB/EU-centric in a way that excludes a reasonable European
   default, expand it OR record a backlog item with the rationale.
9. Re-validation: a curl against the kept A09 dataset confirms no
   regression; respx-mocked tests for the rest.
10. Vault pages updated for affected datasets; `last_verified:
    2026-05-09`.
11. `uv run pytest -m "not live and not slow" -x -q` passes.

## Tasks

### Task 1 — Pre-flight (same shape as plans A/B/C)

<read_first>
- C:\Users\Bobbo\OneDrive\Desktop\Python\gridflow\.env
- .planning/phases/V1-vault-vendor-validation-and-docs/V1-CONTEXT.md
- .planning/phases/V2-v1-vendor-bugfix-followups/V2-CONTEXT.md
</read_first>

<action>
1. `[ -f .env ] || cp "C:/Users/Bobbo/OneDrive/Desktop/Python/gridflow/.env" .env`
2. `mkdir -p .tmp`
3. `curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" https://api.carbonintensity.org.uk/intensity` — `200`
4. ENTSOE health (no-data is acceptable — we just need the API reachable):
   ```bash
   curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" \
     "https://web-api.tp.entsoe.eu/api?securityToken=${ENTSOE_API_KEY}&documentType=A09&in_Domain=10YGB----------A&out_Domain=10YFR-RTE------C&periodStart=202605060000&periodEnd=202605070000&contract_MarketAgreement.Type=A01"
   ```
   Expect `200`.
</action>

<acceptance_criteria>
- All three smoke tests return `200`.
</acceptance_criteria>

### Sub-task 2 — Investigate before deciding

#### 2a. A37 / A15 pagination

<read_first>
- src/gridflow/connectors/entsoe/endpoints.py (find A37 / A15 entries
  and their `extra_params`; the V1 entsoe-VALIDATION §11 says
  `offset=0` is hardcoded)
- src/gridflow/connectors/entsoe/client.py (or wherever request building
  happens — find the offset folding logic)
</read_first>

<action>
1. Confirm the A37 / A15 entries declare `offset=0` as a literal.
2. Read the ENTSOE API guide section on `offset` (V1 says PDF unfetchable;
   skip if so). Otherwise grep for prior code that handles ENTSOE
   pagination (search `offset` across `connectors/entsoe/`).
3. Decide pagination scheme: iterate `offset` in steps of 4800 until a
   page returns < 4800 TimeSeries; cap at offset=9600 (~14 400 TS) for
   safety.
4. Document the decision in plan results.
</action>

<acceptance_criteria>
- A documented offset-iteration plan exists, with a max value (default
  9600).
</acceptance_criteria>

#### 2b. A87 schedule cadence

<read_first>
- config/sources.yaml `entsoe.balancing_financial_expenses_income` block
- src/gridflow/silver/entsoe/h8_balancing.py (A87 transformer — search
  for `A87` or `BalancingFinancial`)
</read_first>

<action>
1. Confirm current `schedule: daily, max_query_days: 1`.
2. Read V1 entsoe-VALIDATION Recommendation §7 (rationale: real cadence
   is monthly).
3. Decide: `schedule: monthly, max_query_days: 31`.
</action>

<acceptance_criteria>
- A documented cadence change is decided.
</acceptance_criteria>

#### 2c. A87 silver `Reason.code` exposure

<read_first>
- src/gridflow/silver/entsoe/h8_balancing.py (A87 transformer)
- src/gridflow/silver/entsoe/  (find a transformer that DOES expose
  `Reason.code` — most outage transformers do)
</read_first>

<action>
1. Confirm A87's transformer drops `Reason.code` from output.
2. Plan the addition: new column `reason_code` (str | None) sourced
   from `<Reason><code>...` in the GL_MarketDocument (or
   Publication_MarketDocument — A87 uses legacy lineage per V1
   entsoe-VALIDATION §8).
</action>

<acceptance_criteria>
- A specific column-add plan exists for A87 silver output.
</acceptance_criteria>

#### 2d. `area_name` field tracing

<read_first>
- `grep -rn "area_name" src/gridflow/`
</read_first>

<action>
1. List every silver schema or transformer file that declares
   `area_name` and whether it populates it.
2. Decide:
   - If 1–2 transformers populate it but a wider set declares-and-drops
     it: populate via area-domain reverse-lookup.
   - If no transformer populates it: remove the field from the silver
     schema(s).
</action>

<acceptance_criteria>
- A documented decision exists (populate-via-lookup or remove-field).
</acceptance_criteria>

#### 2e. `psrType` `optional_params`

<read_first>
- src/gridflow/connectors/entsoe/endpoints.py (find every endpoint's
  `optional_params` tuple; identify which need `psrType`)
- V1 entsoe-VALIDATION (B2 batch) for hints on which endpoints use
  `psrType`
</read_first>

<action>
1. List the candidate endpoints (likely outages_generation,
   actual_generation, generation_forecast, wind_solar_forecast — any
   per-fuel-type filter).
2. Add `psrType` to those endpoints' `optional_params` tuples.
</action>

<acceptance_criteria>
- A specific list of endpoints to receive `psrType` exists.
</acceptance_criteria>

#### 2f. `DEFAULT_ZONES` review

<read_first>
- `grep -rn "DEFAULT_ZONES" src/gridflow/connectors/entsoe/`
</read_first>

<action>
1. Locate the constant and inspect contents.
2. If the list is e.g. `[GB, FR, DE-LU]` and excludes major neighbours
   (NL, BE, IE), decide whether to expand. The V1 finding's wording
   "GB/EU-centric" suggests it leans toward a particular subset; the
   right baseline is debatable.
3. Default decision: leave as-is and add a backlog row for the wider
   review unless an obvious omission appears (e.g. NL is missing despite
   being a major flow neighbour).
</action>

<acceptance_criteria>
- A documented decision exists (touch / backlog).
</acceptance_criteria>

### Sub-task 3 — Author ADR-019 (A09 dedup decision)

<read_first>
- `docs/DECISION_LOG/` (read 1–2 existing ADRs for project format)
- V1 entsoe-VALIDATION §6 + Recommendations §3
</read_first>

<action>
Write `docs/DECISION_LOG/ADR-019-entsoe-a09-dedup.md` with sections:

```markdown
# ADR-019: ENTSOE A09 commercial_schedules registry dedup

**Status:** Accepted
**Date:** 2026-05-09
**Phase:** V2 (v0.10 vendor bug-fix follow-ups)

## Context

V1 live validation 2026-05-08 confirmed `commercial_schedules` and
`commercial_schedules_net_positions` use identical `EntsoeDocType`
and return identical 5 296-byte XML payloads for the same request.
The dataset key distinction is silver-transformer label only; no
semantic difference at bronze. This is registry duplication.

## Decision

**Drop `commercial_schedules_net_positions`.** Remove the key from
`connectors/entsoe/endpoints.py::ENDPOINTS`, `config/sources.yaml`,
and the silver transformer registry. Mark the existing
`silver/entsoe/commercial_schedules_net_positions/` Parquet directory
as deprecated; do not delete (historical artefact).

## Alternatives considered

1. **Derive net positions** — keep both keys; rewrite the
   `CommercialSchedulesNetPositionsTransformer` to pair zone-pair
   directions and emit a signed `net_position_mw` column. Rejected
   because no current downstream gold consumer needs net positions;
   the engineering cost is not justified by current needs. Recorded
   as backlog item.

## Consequences

- Future `gridflow ingest entsoe commercial_schedules_net_positions`
  fails with "unknown dataset" — acceptable; no scheduled job uses
  it.
- Existing silver Parquet remains queryable but is frozen at last-V1
  ingest.
- A real signed-net-positions dataset can be added later in a fresh
  phase if a gold consumer materialises.

## Rollback

Re-add the key to `ENDPOINTS` and `config/sources.yaml` from this
commit's git diff. The silver transformer class is unchanged.
```
</action>

<acceptance_criteria>
- `docs/DECISION_LOG/ADR-019-entsoe-a09-dedup.md` exists with the
  sections above filled.
- A short note added to `.planning/STATE.md` `Decisions` block:
  "ADR-019: A09 commercial_schedules_net_positions deprecated;
  derive-net-positions kept as backlog."
</acceptance_criteria>

### Sub-task 4 — Apply A09 dedup (Default — drop key)

<action>
1. Edit `src/gridflow/connectors/entsoe/endpoints.py`: remove the
   `commercial_schedules_net_positions` ENDPOINTS entry (and any
   `optional_params` tuple unique to it).
2. Edit `config/sources.yaml`: remove the `entsoe.commercial_schedules_net_positions`
   block.
3. Edit `src/gridflow/silver/entsoe/h6_market.py` (or wherever the
   `CommercialSchedulesNetPositionsTransformer` is registered): remove
   its registration call (`register_transformer("entsoe",
   "commercial_schedules_net_positions", ...)`) and the class itself.
4. Add a short README at
   `silver/entsoe/commercial_schedules_net_positions_DEPRECATED.md`
   noting V2 deprecation and pointing at ADR-019.
5. Update `tests/` — find any test that imports
   `CommercialSchedulesNetPositionsTransformer` (grep) and either
   delete the test or assert that the dataset is now absent.
</action>

<acceptance_criteria>
- `grep -rn "commercial_schedules_net_positions" src/` returns 0 hits
  (other than the DEPRECATED README).
- `grep -rn "commercial_schedules_net_positions" config/sources.yaml`
  returns 0 hits.
- `uv run mypy --strict` and `uv run ruff check` clean.
- `uv run pytest -m "not live and not slow" -x -q` passes.
</acceptance_criteria>

### Sub-task 5 — Apply B2 hygiene fixes

#### 5a. A37 / A15 pagination iteration

<action>
1. In the connector or client code that builds A37/A15 requests
   (`src/gridflow/connectors/entsoe/`), replace the literal `offset=0`
   in `extra_params` with an iteration loop that:
   - Issues offset=0
   - If response has 4800 TimeSeries, issue offset=4800
   - If still 4800, issue offset=9600
   - Cap at 14 400 (offset=9600 is the last page); document the cap.
2. Add a respx-mocked test that simulates a two-page response and
   asserts the connector issues two requests with offsets `0` and
   `4800`.
</action>

<acceptance_criteria>
- `grep "offset=0" src/gridflow/connectors/entsoe/` shows the literal
  is gone (replaced by iteration logic).
- New test asserts 2-request behaviour for the two-page mock.
</acceptance_criteria>

#### 5b. A87 schedule cadence

<action>
Edit `config/sources.yaml`. Replace:

```yaml
balancing_financial_expenses_income:
  schedule: daily
  max_query_days: 1
```

with:

```yaml
balancing_financial_expenses_income:
  schedule: monthly
  max_query_days: 31
```
</action>

<acceptance_criteria>
- `grep -A3 balancing_financial_expenses_income config/sources.yaml`
  shows `schedule: monthly` and `max_query_days: 31`.
</acceptance_criteria>

#### 5c. A87 silver `Reason.code` exposure

<action>
1. In `src/gridflow/silver/entsoe/h8_balancing.py`, locate the A87
   transformer (search `A87` or `BalancingFinancial`).
2. Add a `reason_code` column extraction from the document-level
   `<Reason><code>...` element.
3. Update the silver Pydantic schema (likely
   `gridflow.schemas.entsoe.EntsoeBalancingFinancial` or similar) to
   declare the new field as `Optional[str]`.
4. Add a fixture-backed test that proves `reason_code` is populated
   from a fixture XML carrying a Reason block, and `None` when the
   Reason is absent.
</action>

<acceptance_criteria>
- A87 silver output has a `reason_code` column.
- Test passes for both Reason-present and Reason-absent fixture cases.
</acceptance_criteria>

#### 5d. `area_name` field

<action>
Apply the decision from Sub-task 2d:
- **Populate path:** add a reverse-lookup helper in
  `connectors/entsoe/zones.py` (or wherever) mapping
  `10YGB----------A → "United Kingdom"` etc.; update the relevant
  silver transformer(s) to call it.
- **Remove path:** delete the `area_name` field from the silver
  schema(s) and any column references.

Add a regression test for the chosen path.
</action>

<acceptance_criteria>
- The chosen path's edits are applied; tests pass.
- The other path's files are NOT touched.
</acceptance_criteria>

#### 5e. `psrType` optional_params

<action>
1. In `src/gridflow/connectors/entsoe/endpoints.py`, add `psrType` to
   the `optional_params` tuple of every endpoint identified in
   Sub-task 2e.
2. Add a test that asserts `psrType` is propagated when passed as a
   keyword argument to the connector's request builder.
</action>

<acceptance_criteria>
- `grep "psrType" src/gridflow/connectors/entsoe/endpoints.py` shows
  it inside the relevant `optional_params` tuples.
- New test passes.
</acceptance_criteria>

#### 5f. DEFAULT_ZONES

<action>
Apply the decision from Sub-task 2f:
- **Touch path:** edit the constant; add a regression test that
  asserts the new list contents.
- **Backlog path:** add a row to `.planning/ROADMAP.md` Backlog (this
  edit is bundled into V2-PLAN-F's close-out aggregator — do not edit
  ROADMAP.md here directly).
</action>

<acceptance_criteria>
- The chosen path's edits are applied OR the backlog action is queued
  for PLAN-F.
</acceptance_criteria>

### Task 6 — Re-validation evidence

<action>
1. **A09 kept dataset live curl:** confirm no regression on the kept
   `commercial_schedules` dataset against GB→FR with
   `contract_MarketAgreement.Type=A01`. Same curl as V1 evidence.

2. **A87 monthly cadence rationale:** capture a curl with a 31-day
   window and confirm response size + non-empty TimeSeries (against a
   non-GB area, e.g. DE-LU, since GB returns EMPTY post-Brexit).

3. **A37 / A15 pagination:** mocked-only (the live API would need
   GB-balancing publishing data which V1 confirmed is post-Brexit
   absent; verify via mocked respx fixture instead).

Append a `## V2 re-validation` block to V1's
`entsoe-VALIDATION.md` (do not rewrite).
</action>

<acceptance_criteria>
- A09 kept-dataset curl returns 200 with same payload size as V1.
- A87 monthly-window curl succeeds.
- entsoe-VALIDATION.md has the new V2 section.
</acceptance_criteria>

### Task 7 — Vault page edits

<action>
1. `commercial_schedules.md`: bump `last_verified: 2026-05-09`; add
   `## Changelog` line "2026-05-09 — V2: A09 dedup. The
   `commercial_schedules_net_positions` companion dataset deprecated
   (ADR-019)."
2. `commercial_schedules_net_positions.md`: replace body with a
   one-paragraph deprecation notice cross-linking
   `commercial_schedules.md` and `ADR-019`.
3. `balancing_energy_bids.md`, `procured_balancing_capacity.md`:
   `last_verified: 2026-05-09`; mention pagination iteration in
   `## Changelog`.
4. `balancing_financial_expenses_income.md`: `last_verified: 2026-05-09`;
   schedule line in body updated to `monthly`; `Reason.code` added to
   silver schema table; changelog bullet.
5. `endpoints.md`: line for `commercial_schedules_net_positions`
   removed (or annotated DEPRECATED with strikethrough); A87 schedule
   column updated.
</action>

<acceptance_criteria>
- All 5 affected dataset pages reference the V2 commit SHA.
- `endpoints.md` reflects the registry change.
</acceptance_criteria>

### Task 8 — Commit

<action>
Stage and commit (one commit; the changes are tightly coupled to the
ADR):

```
git add docs/DECISION_LOG/ADR-019-entsoe-a09-dedup.md
git add src/gridflow/connectors/entsoe/endpoints.py
git add src/gridflow/silver/entsoe/h6_market.py
git add src/gridflow/silver/entsoe/h8_balancing.py
git add src/gridflow/silver/entsoe/  # any other files touched in 5c, 5d
git add config/sources.yaml
git add tests/  # all new B2 tests
git add .planning/phases/V1-vault-vendor-validation-and-docs/entsoe-VALIDATION.md
git add .planning/STATE.md  # ADR-019 mention
```

Commit message:

```
fix(V2-D): ENTSOE A09 dedup + B2 cleanup batch

- ADR-019: drop `commercial_schedules_net_positions` (registry dup
  with `commercial_schedules`). Derive-net-positions kept as backlog.
- A37 `balancing_energy_bids` and A15 `procured_balancing_capacity`
  now iterate offset until empty page; cap at offset=9600.
- A87 `balancing_financial_expenses_income` schedule monthly,
  max_query_days=31. Silver now exposes `reason_code`.
- `area_name`: <populate via zone-name lookup | removed from schema>.
- `psrType` added to optional_params for the relevant endpoints.
- DEFAULT_ZONES: <touched | backlog rationale>.

No regression on live `commercial_schedules` GB→FR curl 2026-05-09.

Closes V2-FIX-05, V2-FIX-06.
```
</action>

<acceptance_criteria>
- `git log --oneline -1` shows commit prefix `fix(V2-D):`.
- `git status` clean modulo `.tmp/` and out-of-tree vault.
</acceptance_criteria>

## Risks / known-unknowns

- **Pagination cap.** `offset=9600` (≈14 400 TS) is a heuristic. If a
  high-cardinality area returns >14 400 TS in a single time window, the
  connector silently truncates. Document in commit body; record as
  `## Implementation delta` on the affected dataset pages if so.
- **A87 fixture.** If `tests/fixtures/entsoe/` lacks an A87 XML with a
  `Reason.code` element, capture a fresh one via live DE-LU 31-day curl
  and trim. (GB returns EMPTY post-Brexit.)
- **commercial_schedules silver classes.** If
  `CommercialSchedulesNetPositionsTransformer` shares any helper logic
  with `CommercialSchedulesTransformer`, the dedup must keep the shared
  helpers. Diff carefully before deletion.
- **`config/sources.yaml` schema cadence.** If the existing pipeline
  cadence-runner expects monthly cadence in a specific format, confirm
  via `grep -rn "schedule:" src/gridflow/` that "monthly" is recognised
  (otherwise use the actual cadence keyword the runtime supports).

## Verification

```bash
uv run pytest -m "not live and not slow" -x -q
uv run mypy --strict src/gridflow/connectors/entsoe/ src/gridflow/silver/entsoe/
uv run ruff check src/gridflow/
```

All three must exit `0`.
