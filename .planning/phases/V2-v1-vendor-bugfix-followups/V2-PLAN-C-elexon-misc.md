---
phase: V2
plan_id: V2-PLAN-C-elexon-misc
slug: elexon-medium-severity-bundle
status: draft
milestone: v0.10
wave: 2
severity: MEDIUM
depends_on:
  - V2-PLAN-A-elexon-freq-fix
autonomous: true
files_modified:
  - src/gridflow/connectors/elexon/endpoints.py
  - src/gridflow/schemas/elexon.py
  - src/gridflow/silver/elexon/system_prices.py  # only if mapping fix is needed (Sub-task 2 outcome)
  - tests/integration/test_elexon_e2e.py
  - tests/unit/silver/test_system_prices.py
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\datasets\remit.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\datasets\soso.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\datasets\system_prices.md
  - .planning/phases/V1-vault-vendor-validation-and-docs/elexon-VALIDATION.md
requirements:
  - V2-FIX-03  # REMIT/SOSO 1-day cap
  - V2-FIX-04  # system_prices priceDerivationCode regex
---

# V2 Plan C — Elexon Medium-Severity Bundle

## Goal

Close two medium-severity Elexon bugs surfaced by V1:

1. **REMIT / SOSO max-1-day cap** (V2-FIX-03). The Elexon API enforces an
   undocumented "PublishDateTimeFrom..PublishDateTimeTo must not exceed
   1 day" rule for both `/datasets/REMIT` and `/datasets/SOSO`,
   returning HTTP 400 otherwise. The connector currently uses
   `max_chunk_hours=24` (the default), which is exactly at the boundary
   and may cross over with DST shifts or off-by-one window expansions.

2. **`system_prices.priceDerivationCode` regex too narrow** (V2-FIX-04).
   `ElexonSystemPrice.run_type` enforces `pattern=r"^(II|SF|R[1-3]|RF|DF)$"`,
   but live API responses include the value `"N"` (per V1 user notes).
   Either the regex is missing a value or the field mapping in
   `silver/elexon/system_prices.py::_data_provider_columns` (line 67)
   maps the wrong API field to `run_type`. PLAN-C's investigation step
   resolves which.

## must_haves (goal-backward verification)

1. `ENDPOINTS["remit"]` and `ENDPOINTS["soso"]` carry an explicit
   `max_chunk_hours=23` (not the default `24`).
2. A live re-validation curl with a 23-hour window succeeds (HTTP 200);
   a 25-hour window returns HTTP 400 — proving the boundary.
3. `ElexonSystemPrice.run_type` (or its replacement schema field) accepts
   every `priceDerivationCode` value present in a live `/system-prices`
   response over a representative settlement date. The fix is either:
   - **Option α — extend regex + enum.** Add `N` (and any other
     observed values) to `SettlementRunType` and the regex.
   - **Option β — fix mapping.** If `priceDerivationCode = "N"` semantically
     denotes something other than a run type, `silver/elexon/system_prices.py::_data_provider_columns`
     line 67 (`"priceDerivationCode": "run_type"`) is the wrong mapping;
     correct it to map `priceDerivationCode` to a different (or new)
     silver column and source `run_type` from the actual API field
     (`runType`?).
4. A regression test in `tests/unit/silver/test_system_prices.py`
   validates that an `ElexonSystemPrice` instance can be constructed with
   the fixed schema for every observed `priceDerivationCode` value.
5. A regression test in `tests/integration/test_elexon_e2e.py` (or
   adjacent) parametrically asserts `ENDPOINTS["remit"].max_chunk_hours
   == 23` and `ENDPOINTS["soso"].max_chunk_hours == 23`.
6. Re-validation evidence appended to V1 `elexon-VALIDATION.md` under
   `## V2 re-validation`.
7. Vault pages `remit.md`, `soso.md`, `system_prices.md` updated with
   resolved deltas + `last_verified: 2026-05-09`.
8. `uv run pytest -m "not live and not slow" -x -q` passes.

## Tasks

### Task 1 — Pre-flight smoke + .env + Elexon health

<read_first>
- C:\Users\Bobbo\OneDrive\Desktop\Python\gridflow\.env
- .planning/phases/V1-vault-vendor-validation-and-docs/V1-CONTEXT.md
- .planning/phases/V2-v1-vendor-bugfix-followups/V2-CONTEXT.md
</read_first>

<action>
1. `[ -f .env ] || cp "C:/Users/Bobbo/OneDrive/Desktop/Python/gridflow/.env" .env`
2. `mkdir -p .tmp`
3. Carbon-intensity smoke test (same as PLAN-A Task 1):
   `curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" https://api.carbonintensity.org.uk/intensity`
4. Elexon FUELHH health curl (same as PLAN-A Task 1).
</action>

<acceptance_criteria>
- Smoke + Elexon health both return `200`. Halt and record otherwise.
</acceptance_criteria>

### Sub-task 2 — Investigate `priceDerivationCode = "N"`

<read_first>
- src/gridflow/silver/elexon/system_prices.py (line 67 area —
  `_data_provider_columns` mapping)
- src/gridflow/schemas/elexon.py (lines 13–34 — `SettlementRunType` enum
  and `ElexonSystemPrice.run_type` regex)
- src/gridflow/silver/elexon/system_prices.py:25 — `RUN_PRECEDENCE`
  dict (canonical run-type list inside silver code)
- src/gridflow/connectors/elexon/parsers.py:54-60 — same
  `RUN_PRECEDENCE` map (duplicated)
</read_first>

<action>
1. Live curl `/system-prices/2026-05-06` and capture the full set of
   `priceDerivationCode` values present:
   ```bash
   curl --ssl-no-revoke -fsS -H "Accept: application/json" \
     "https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices/2026-05-06" \
     -o .tmp/elexon-system-prices.json -w "HTTP %{http_code} | %{size_download}B\n"
   ```
   Expect HTTP 200, ~42 KB, 48 rows (one per settlement period).

2. Extract distinct `priceDerivationCode` values:
   ```bash
   python -c "import json; d=json.load(open('.tmp/elexon-system-prices.json')); rows=d.get('data',[]); print(sorted({r.get('priceDerivationCode') for r in rows}))"
   ```
   Record the result in V2 plan results.

3. Live curl another date a few weeks back (where reconciliation should
   have produced different run-type codes):
   ```bash
   curl --ssl-no-revoke -fsS -H "Accept: application/json" \
     "https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices/2026-04-15" \
     -o .tmp/elexon-system-prices-older.json -w "HTTP %{http_code} | %{size_download}B\n"
   python -c "import json; d=json.load(open('.tmp/elexon-system-prices-older.json')); rows=d.get('data',[]); print(sorted({r.get('priceDerivationCode') for r in rows}))"
   ```

4. Cross-reference Elexon docs:
   `WebFetch https://bmrs.elexon.co.uk/api-documentation` and search for
   "priceDerivationCode" or "run_type". Record the canonical value list.
   (If WebFetch fails because the Swagger UI is JS-rendered, capture the
   payload's distinct values from the live curl as the source of truth.)

5. Decide:
   - **Option α (likely):** the live API returns `N` because it's a
     valid Elexon code we missed (probably "N" = "Indicative" or
     similar — confirmed via docs).
   - **Option β (less likely):** `priceDerivationCode` is the WRONG
     field to feed `run_type`; the correct field is e.g. `runType` or
     `settlementRunType`.

   Whichever option, document the decision in the plan results before
   doing Sub-task 3 or 4.
</action>

<acceptance_criteria>
- `.tmp/elexon-system-prices.json` and
  `.tmp/elexon-system-prices-older.json` exist; HTTP 200; row count 48
  each.
- The set of `priceDerivationCode` values seen across both is recorded
  (one or both should include `"N"` per V1 user note).
- A documented decision exists for whether to extend the regex (Option
  α) or fix the mapping (Option β).
</acceptance_criteria>

### Sub-task 3 — Apply fix per chosen option

<action>
**If Option α (extend regex):**

Edit `src/gridflow/schemas/elexon.py`:
1. Add `N = "N"  # ...meaning per Sub-task 2 docs investigation` to
   `SettlementRunType` enum.
2. Update regex on `ElexonSystemPrice.run_type` to
   `pattern=r"^(II|SF|R[1-3]|RF|DF|N)$"` (and any other newly-observed
   codes).
3. If `RUN_PRECEDENCE` in `src/gridflow/silver/elexon/system_prices.py:25`
   and `src/gridflow/connectors/elexon/parsers.py:54` need a precedence
   for `N`, add it (likely `"N": 0` to denote "no run yet").

**If Option β (fix mapping):**

Edit `src/gridflow/silver/elexon/system_prices.py`:
1. Change line 67 mapping
   `"priceDerivationCode": "run_type"` to the correct mapping (e.g.
   `"priceDerivationCode": "price_derivation_code"` and source
   `run_type` from a different API field — confirm via Sub-task 2).
2. If a new silver column is introduced, update
   `gridflow.schemas.elexon.ElexonSystemPrice` to declare it (typed
   `Optional[str]` until/unless we discover constraints).
3. Update existing tests that assert on the silver schema (search:
   `grep -rn "priceDerivationCode\|run_type" tests/`).
</action>

<acceptance_criteria>
- The chosen option's edits are applied; the file passes
  `uv run mypy --strict` and `uv run ruff check`.
- The other option's files are NOT touched.
</acceptance_criteria>

### Sub-task 4 — REMIT / SOSO `max_chunk_hours = 23`

<read_first>
- src/gridflow/connectors/elexon/endpoints.py (lines 230–242,
  `remit` and `soso` entries)
</read_first>

<action>
Edit `src/gridflow/connectors/elexon/endpoints.py`. Replace:

```python
"remit": ElexonEndpoint(
    path="/datasets/REMIT",
    description="REMIT Outage and Unavailability Messages",
    param_style=ParamStyle.PUBLISH_DATETIME,
),

# --- SO-SO prices ---
"soso": ElexonEndpoint(
    path="/datasets/SOSO",
    description="SO-SO Prices (Cross-Border Interconnector Trading)",
    param_style=ParamStyle.PUBLISH_DATETIME,
),
```

with:

```python
"remit": ElexonEndpoint(
    path="/datasets/REMIT",
    description="REMIT Outage and Unavailability Messages",
    param_style=ParamStyle.PUBLISH_DATETIME,
    max_chunk_hours=23,  # vendor enforces undocumented max-1-day window
),

# --- SO-SO prices ---
"soso": ElexonEndpoint(
    path="/datasets/SOSO",
    description="SO-SO Prices (Cross-Border Interconnector Trading)",
    param_style=ParamStyle.PUBLISH_DATETIME,
    max_chunk_hours=23,  # vendor enforces undocumented max-1-day window
),
```
</action>

<acceptance_criteria>
- `grep -A6 '"remit": ElexonEndpoint' src/gridflow/connectors/elexon/endpoints.py`
  shows `max_chunk_hours=23`.
- Same for `"soso"`.
- `uv run mypy --strict src/gridflow/connectors/elexon/endpoints.py`
  exits `0`.
</acceptance_criteria>

### Task 5 — Add regression tests

<read_first>
- tests/integration/test_elexon_e2e.py (look for any chunking-related
  tests; the new test should sit alongside)
- tests/unit/silver/test_system_prices.py (or wherever
  `ElexonSystemPrice` tests live)
</read_first>

<action>
1. **REMIT / SOSO chunk test.** Add a parametrized test:
   ```python
   import pytest
   from gridflow.connectors.elexon.endpoints import ENDPOINTS

   @pytest.mark.parametrize("key", ["remit", "soso"])
   def test_remit_soso_max_chunk_hours_below_24(key):
       """V2-FIX-03: vendor enforces undocumented max-1-day window;
       use 23h to leave a margin for DST shifts."""
       assert ENDPOINTS[key].max_chunk_hours == 23
   ```

2. **system_prices regex / mapping test.** Add a per-option test:

   **Option α path:**
   ```python
   from gridflow.schemas.elexon import ElexonSystemPrice
   from datetime import date, datetime, UTC

   @pytest.mark.parametrize("code", ["II", "SF", "R1", "R2", "R3", "RF", "DF", "N"])
   def test_system_price_run_type_accepts_all_observed(code):
       p = ElexonSystemPrice(
           settlement_date=date(2026,5,6),
           settlement_period=1,
           timestamp_utc=datetime(2026,5,6,0,0,tzinfo=UTC),
           system_sell_price=50.0,
           system_buy_price=51.0,
           net_imbalance_volume=100.0,
           run_type=code,
       )
       assert p.run_type == code

   def test_system_price_run_type_rejects_unknown():
       with pytest.raises(ValueError):
           ElexonSystemPrice(
               settlement_date=date(2026,5,6),
               settlement_period=1,
               timestamp_utc=datetime(2026,5,6,0,0,tzinfo=UTC),
               system_sell_price=50.0,
               system_buy_price=51.0,
               net_imbalance_volume=100.0,
               run_type="XX",  # not in the enum
           )
   ```

   **Option β path:** test that the new silver column is populated and
   the original `run_type` is unchanged for the canonical 7 values.

Both tests must FAIL before Sub-task 3/4 changes and PASS after.
</action>

<acceptance_criteria>
- New REMIT/SOSO test exists and asserts `max_chunk_hours == 23`.
- New system-prices test(s) cover the chosen option.
- `uv run pytest -m "not live and not slow" -x -q -k "remit or soso or system_price" tests/`
  passes.
- `uv run pytest -m "not live and not slow" -x -q` (full fast suite) passes.
</acceptance_criteria>

### Task 6 — Live re-validation evidence

<action>
1. **REMIT 23-hour window:**
   ```bash
   curl --ssl-no-revoke -fsS -H "Accept: application/json" \
     "https://data.elexon.co.uk/bmrs/api/v1/datasets/REMIT?publishDateTimeFrom=2026-05-06T00:00Z&publishDateTimeTo=2026-05-06T23:00Z&format=json" \
     -o .tmp/elexon-remit-23h.json -w "HTTP %{http_code} | %{size_download}B\n"
   ```
   Expect HTTP 200.

2. **REMIT 25-hour window (must FAIL):**
   ```bash
   curl --ssl-no-revoke -sS -H "Accept: application/json" \
     "https://data.elexon.co.uk/bmrs/api/v1/datasets/REMIT?publishDateTimeFrom=2026-05-06T00:00Z&publishDateTimeTo=2026-05-07T01:00Z&format=json" \
     -o .tmp/elexon-remit-25h.json -w "HTTP %{http_code} | %{size_download}B\n"
   ```
   Expect HTTP 400. (Note: `-fsS` removed so the curl does not exit
   non-zero on 4xx — we want to capture the 400.)

3. Repeat for SOSO.

4. **system_prices `priceDerivationCode` round-trip:**
   ```bash
   curl --ssl-no-revoke -fsS -H "Accept: application/json" \
     "https://data.elexon.co.uk/bmrs/api/v1/balancing/settlement/system-prices/2026-05-06" \
     -o .tmp/elexon-system-prices-revalidation.json
   python -c "
   import json, polars as pl
   from gridflow.silver.elexon.system_prices import SystemPricesTransformer
   payload = json.load(open('.tmp/elexon-system-prices-revalidation.json'))
   # Pipe through the silver transform to confirm no Pydantic ValidationError
   # for any priceDerivationCode value.
   t = SystemPricesTransformer()
   df = t.transform(pl.DataFrame(payload['data']))
   print(df.height, sorted(df['run_type'].unique().to_list()))
   "
   ```
   Expect: no exceptions, 48 rows, run_type values include all observed
   codes from Sub-task 2.

5. Append a new `## V2 re-validation` block to V1's
   `elexon-VALIDATION.md` (or extend the one PLAN-A added):

   ```markdown
   ### V2 re-validation rows (PLAN-C, 2026-05-09)

   | Dataset | V1 issue | V2 outcome |
   |---------|----------|------------|
   | `remit` | HTTP 400 on >1-day window with default max_chunk_hours=24 | 23h window 200; 25h window 400 (boundary confirmed) |
   | `soso` | same as remit | same as remit |
   | `system_prices` | Pydantic regex rejected live `priceDerivationCode = "N"` | regex/mapping fixed; 48-row round-trip succeeds with all observed codes |
   ```
</action>

<acceptance_criteria>
- All four .json files in `.tmp/` exist.
- The 25h request returns HTTP 400 (verifies the vendor's 1-day cap is
  the actual cause, not a coincidence).
- The 23h request returns HTTP 200.
- The system_prices Python round-trip prints `48` and a list of run_type
  codes that includes `"N"` (or whichever the live data carries).
- V1's `elexon-VALIDATION.md` has the new re-validation block appended.
</acceptance_criteria>

### Task 7 — Vault page edits

<read_first>
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\datasets\remit.md
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\datasets\soso.md
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\datasets\system_prices.md
</read_first>

<action>
1. `remit.md` and `soso.md`:
   - Replace the `## Implementation delta` paragraph flagging the 1-day
     cap with a one-line "Resolved in V2 (2026-05-09): connector
     `max_chunk_hours=23`. Vendor's undocumented limit confirmed via 25h
     request returning HTTP 400."
   - Bump `last_verified: 2026-05-09`.
   - Add `## Changelog` bullet.

2. `system_prices.md`:
   - Replace the `## Implementation delta` block referencing the regex
     issue with a one-line "Resolved in V2 (2026-05-09): added `N` to
     `SettlementRunType` enum + regex" (Option α) or "field mapping
     corrected" (Option β).
   - In the `## Silver layer` schema table, update the `run_type` row
     to reflect the new accepted values (or the new mapping).
   - Bump `last_verified: 2026-05-09`.
   - Add `## Changelog` bullet.
</action>

<acceptance_criteria>
- All 3 vault pages have `last_verified: 2026-05-09`.
- Each page references the V2 commit SHA in its delta block.
- No other vault pages touched.
</acceptance_criteria>

### Task 8 — Commit

<action>
Stage and commit the gridflow-tree changes (vault edits remain
out-of-tree):

```
git add src/gridflow/connectors/elexon/endpoints.py
git add src/gridflow/schemas/elexon.py
git add src/gridflow/silver/elexon/system_prices.py  # only if Option β
git add tests/  # the regression tests
git add .planning/phases/V1-vault-vendor-validation-and-docs/elexon-VALIDATION.md
```

Conventional-commit message:

```
fix(V2-C): elexon REMIT/SOSO honour 1-day cap; system_prices accepts run_type "N"

REMIT and SOSO now declare `max_chunk_hours=23` so connector chunking
stays under the vendor's undocumented 1-day query cap. Confirmed live
2026-05-09: 23h window returns 200, 25h returns 400 with the exact
"date range ... must not exceed 1 day" error.

system_prices: <Option α | Option β description per Sub-task 2 outcome>.
Round-trip 48 rows on 2026-05-06 succeed with every observed
priceDerivationCode value.

Closes V2-FIX-03, V2-FIX-04.
```
</action>

<acceptance_criteria>
- `git log --oneline -1` shows the new commit with prefix `fix(V2-C):`.
- `git status` is clean modulo `.tmp/` and out-of-tree vault.
</acceptance_criteria>

## Risks / known-unknowns

- **`priceDerivationCode = "N"` semantics.** If Sub-task 2 reveals that
  `N` is something obscure (e.g. "Not yet derived", returned only for
  in-flight settlement periods), the silver column may need to shift
  to `Optional[str]` and downstream consumers (the F5 demand forecast,
  F6 wind/solar — see `gridflow_models` repo) may need updating to
  handle the new value. **Out of V2 scope** — record as a follow-up if
  encountered.
- **`max_chunk_hours=23` may produce more chunks per backfill.** A 7-day
  backfill that previously made 7 calls now makes 8 (one per 23h chunk).
  Acceptable cost; the connector's rate-limit gate
  (`asyncio.sleep(0.5)`) handles the extra call.
- **DST-day chunking math.** Confirm the connector's
  `_chunk_publish_datetime_window` (or equivalent helper, find with
  `grep -n max_chunk_hours src/gridflow/connectors/elexon/`) still
  emits non-overlapping windows when `max_chunk_hours=23` and the
  window crosses 2026-03-29 (UK spring-forward, 23h day) or 2026-10-25
  (UK fall-back, 25h day). Add a unit test if uncertain.

## Verification

```bash
uv run pytest -m "not live and not slow" -x -q
uv run mypy --strict src/gridflow/connectors/elexon/endpoints.py src/gridflow/schemas/elexon.py
uv run ruff check src/gridflow/
```

All three must exit `0`.
