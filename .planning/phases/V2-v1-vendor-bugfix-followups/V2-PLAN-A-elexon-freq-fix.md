---
phase: V2
plan_id: V2-PLAN-A-elexon-freq-fix
slug: elexon-freq-param-name-fix
status: draft
milestone: v0.10
wave: 1
severity: HIGH
depends_on: []
autonomous: true
files_modified:
  - src/gridflow/connectors/elexon/endpoints.py
  - tests/integration/test_elexon_e2e.py
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\datasets\freq.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\endpoints.md
  - .planning/phases/V1-vault-vendor-validation-and-docs/elexon-VALIDATION.md
requirements:
  - V2-FIX-01
---

# V2 Plan A — Elexon `freq` Parameter-Name Fix

## Goal

Fix the silent-corruption bug in the Elexon `freq` connector where the
endpoint sends `publishDateTimeFrom` / `publishDateTimeTo` but the API
expects `measurementDateTimeFrom` / `measurementDateTimeTo`. The API
silently ignores wrong-named params and returns the latest ~5761 samples
regardless of the requested window. Bronze files produced by `gridflow
ingest elexon freq --last 24h` (or any windowed call) currently capture
the wrong rows.

After this plan: `freq` ingest captures the correct window, a regression
test holds the line, and V1's vault page + endpoint summary reflect the
fix.

## must_haves (goal-backward verification)

1. `src/gridflow/connectors/elexon/endpoints.py::ENDPOINTS["freq"]` has
   `from_param="measurementDateTimeFrom"` and
   `to_param="measurementDateTimeTo"` — explicit, not relying on the
   `PUBLISH_DATETIME` defaults.
2. A new regression test under `tests/integration/test_elexon_e2e.py` (or
   an adjacent unit test if `respx` E2E does not exist for `freq` yet)
   asserts that the connector emits a query string containing
   `measurementDateTimeFrom=...` and `measurementDateTimeTo=...`, NOT
   `publishDateTimeFrom`/`To`. The test would have failed before the fix.
3. A live re-validation curl against
   `https://data.elexon.co.uk/bmrs/api/v1/datasets/FREQ?measurementDateTimeFrom=2024-01-01T00:00Z&measurementDateTimeTo=2024-01-01T03:00Z`
   returns ≈700 rows of *January 2024* data, NOT 5761 samples of latest.
   Evidence appended to the existing
   `.planning/phases/V1-vault-vendor-validation-and-docs/elexon-VALIDATION.md`
   under a new `## V2 re-validation` section.
4. `quant-vault/30-vendors/elexon/datasets/freq.md` body documents the
   correct param names (move the delta from `## Implementation delta` to
   the body); `last_verified: 2026-05-09`; new `## Changelog` line
   `2026-05-09 — fixed in V2`.
5. `quant-vault/30-vendors/elexon/endpoints.md` line for `freq` shows the
   correct param names.
6. `uv run pytest -m "not live and not slow" -x -q tests/integration/test_elexon_e2e.py`
   passes.
7. `uv run mypy --strict src/gridflow/connectors/elexon/endpoints.py`
   stays clean.

## Tasks

### Task 1 — Pre-flight smoke + .env load

<read_first>
- C:\Users\Bobbo\OneDrive\Desktop\Python\gridflow\.env
- .planning/phases/V1-vault-vendor-validation-and-docs/V1-CONTEXT.md
- .planning/phases/V2-v1-vendor-bugfix-followups/V2-CONTEXT.md
</read_first>

<action>
1. If worktree-local `.env` is missing:
   `cp "C:/Users/Bobbo/OneDrive/Desktop/Python/gridflow/.env" .env`
   (Already gitignored. Do NOT modify the main repo's `.env`.)
2. `mkdir -p .tmp`
3. `curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" https://api.carbonintensity.org.uk/intensity`
   Expect `200`. If not, halt, write a single-line error to
   `.planning/phases/V2-v1-vendor-bugfix-followups/V2-PLAN-A-RESULTS.md`,
   stop.
4. Verify Elexon API reachable (BMRS Insights API needs no key for FREQ):
   `curl --ssl-no-revoke -fsS -o .tmp/elexon-freq-baseline.json -w "%{http_code}\n" "https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELHH?settlementDate=2026-05-06&format=json"`
   Expect `200`. (Mirror V1's pre-flight pattern.)
</action>

<acceptance_criteria>
- `.env` exists in the worktree with non-empty `ELEXON_API_KEY` (even
  though `freq` does not need it; later tasks may).
- Carbon-intensity smoke test exits `0` with `200`.
- Elexon FUELHH baseline returns `200`.
</acceptance_criteria>

### Task 2 — Confirm Swagger source of truth (live)

<read_first>
- src/gridflow/connectors/elexon/endpoints.py (lines 100–110, current
  `freq` entry)
- .planning/phases/V1-vault-vendor-validation-and-docs/elexon-VALIDATION.md
  §"Implementation deltas (cross-cutting)" §1 (the V1 evidence)
</read_first>

<action>
1. The V1 Implementation delta already names
   `measurementDateTimeFrom/To` as the Swagger declaration. To
   double-confirm without depending on Swagger UI rendering, run two
   live curls:

   **Wrong-name (current behaviour):**
   ```bash
   curl --ssl-no-revoke -fsS \
     -H "Accept: application/json" \
     "https://data.elexon.co.uk/bmrs/api/v1/datasets/FREQ?publishDateTimeFrom=2024-01-01T00:00Z&publishDateTimeTo=2024-01-01T03:00Z&format=json" \
     -o .tmp/elexon-freq-wrong-name.json \
     -w "HTTP %{http_code} | %{size_download}B\n"
   ```
   Expected: HTTP 200, ~5761 rows, none from January 2024.

   **Correct-name (target behaviour):**
   ```bash
   curl --ssl-no-revoke -fsS \
     -H "Accept: application/json" \
     "https://data.elexon.co.uk/bmrs/api/v1/datasets/FREQ?measurementDateTimeFrom=2024-01-01T00:00Z&measurementDateTimeTo=2024-01-01T03:00Z&format=json" \
     -o .tmp/elexon-freq-correct-name.json \
     -w "HTTP %{http_code} | %{size_download}B\n"
   ```
   Expected: HTTP 200, ≈721 rows, all dated 2024-01-01.

2. Parse both JSONs with `python3 -c "import json,sys; d=json.load(open(sys.argv[1])); rows=d.get('data',[]); print(len(rows), rows[0].get('measurementTime',rows[0].get('settlementDate')) if rows else None)" .tmp/elexon-freq-wrong-name.json`
   and confirm the wrong-name response has measurementTime values from
   ~2026-05-09 while the correct-name response has values from
   2024-01-01.
3. Throttle: sleep 0.6 between the two calls (Elexon 2 req/s).
</action>

<acceptance_criteria>
- Both curls exit `0` with HTTP `200`.
- The wrong-name JSON has ≥5000 rows AND none of them dated 2024-01-01.
- The correct-name JSON has 50–1000 rows AND every row dated 2024-01-01
  (UTC).
- The two JSONs are saved at `.tmp/elexon-freq-wrong-name.json` and
  `.tmp/elexon-freq-correct-name.json`.
</acceptance_criteria>

### Task 3 — Add failing regression test (RED)

> TDD ordering: write the test against the unfixed code first; confirm
> it fails for the reason we expect (wrong param names emitted). Then
> Task 4 applies the fix and the same test goes green. Avoids any
> stash-and-replay choreography.

<read_first>
- tests/integration/test_elexon_e2e.py (or wherever existing Elexon
  endpoint-shape tests live — search with
  `grep -rn "publishDateTimeFrom\|build_params" tests/`)
- src/gridflow/connectors/elexon/endpoints.py::build_params
- src/gridflow/connectors/elexon/client.py (for how params are folded
  into the request URL)
</read_first>

<action>
Add a new test to the most appropriate file (prefer
`tests/unit/connectors/test_elexon_endpoints.py` if it exists; create
under `tests/integration/test_elexon_e2e.py` otherwise — match the
project's existing test placement pattern). The test must:

1. Call `build_params(ENDPOINTS["freq"], start=datetime(2024,1,1,0,0,tzinfo=UTC), end=datetime(2024,1,1,3,0,tzinfo=UTC))`.
2. Assert `"measurementDateTimeFrom" in params and "measurementDateTimeTo" in params`.
3. Assert `"publishDateTimeFrom" not in params and "publishDateTimeTo" not in params`.

Add a second test using `respx` (if Elexon E2E tests use respx; otherwise
skip and do this in a follow-up):
1. Mock `https://data.elexon.co.uk/bmrs/api/v1/datasets/FREQ` with a
   2-row JSON payload.
2. Call the connector's freq fetch with a 3-hour window.
3. Assert the recorded request URL contains `measurementDateTimeFrom`
   and does NOT contain `publishDateTimeFrom`.

Run the new tests against the unfixed code:
```bash
uv run pytest -m "not live and not slow" -x -q -k freq tests/
```

Confirm at least one new test FAILS with an assertion error pointing
at `publishDateTimeFrom` being present (or `measurementDateTimeFrom`
being absent). This proves the test would have caught the V1 bug.

Do NOT commit the test on its own — keep it in the working tree;
Task 4 applies the fix and Task 7 commits the test + fix together
under one `fix(V2-A):` commit (regression test alongside the fix it
covers).
</action>

<acceptance_criteria>
- New test(s) exist with `freq` and `measurementDateTime` in name or
  body.
- Running the new tests against the unfixed `endpoints.py` produces
  at least one FAILED test with an assertion mentioning
  `publishDateTime` or `measurementDateTime`.
- The previously-green portion of the test suite still passes — the
  new failures are limited to the `freq` regression tests.
- No commit is made yet; the test file is staged-pending.
</acceptance_criteria>

### Task 4 — Apply the fix (GREEN)

<read_first>
- src/gridflow/connectors/elexon/endpoints.py (full file, focus on the
  `freq` entry around line 104 and the `ElexonEndpoint` dataclass at
  line 23)
</read_first>

<action>
Edit `src/gridflow/connectors/elexon/endpoints.py`. Replace the current
`freq` entry:

```python
"freq": ElexonEndpoint(
    path="/datasets/FREQ",
    description="System Frequency",
    param_style=ParamStyle.PUBLISH_DATETIME,
    supports_pagination=True,
),
```

with:

```python
"freq": ElexonEndpoint(
    path="/datasets/FREQ",
    description="System Frequency",
    param_style=ParamStyle.PUBLISH_DATETIME,
    from_param="measurementDateTimeFrom",
    to_param="measurementDateTimeTo",
    supports_pagination=True,
),
```

Do not change `ParamStyle.PUBLISH_DATETIME` itself or the
`build_params()` logic — the dataclass already supports per-endpoint
override of `from_param`/`to_param`.

Re-run the freq tests:
```bash
uv run pytest -m "not live and not slow" -x -q -k freq tests/
```

Confirm all the previously-failing tests now pass.
</action>

<acceptance_criteria>
- `grep "freq" src/gridflow/connectors/elexon/endpoints.py` shows
  `from_param="measurementDateTimeFrom"` and
  `to_param="measurementDateTimeTo"` on the `freq` entry.
- `uv run pytest -m "not live and not slow" -x -q -k freq tests/`
  exits `0` — all freq tests pass.
- `uv run pytest -m "not live and not slow" -x -q` (full fast suite)
  exits `0` — no other tests broke.
- `uv run mypy --strict src/gridflow/connectors/elexon/endpoints.py`
  exits `0`.
- `uv run ruff check src/gridflow/connectors/elexon/endpoints.py` exits
  `0`.
- No other endpoint entry's `from_param`/`to_param` changes.
</acceptance_criteria>

### Task 5 — Live re-validation evidence

<action>
1. Repeat the correct-name curl from Task 2 against a fresh narrow
   window:
   ```bash
   curl --ssl-no-revoke -fsS \
     -H "Accept: application/json" \
     "https://data.elexon.co.uk/bmrs/api/v1/datasets/FREQ?measurementDateTimeFrom=2026-05-09T00:00Z&measurementDateTimeTo=2026-05-09T01:00Z&format=json" \
     -o .tmp/elexon-freq-v2-revalidation.json \
     -w "HTTP %{http_code} | %{size_download}B | %{time_total}s\n"
   ```
2. Confirm rows count (~120 for a 1-hour window) and that all
   `measurementTime` values fall within the requested 1-hour window.
3. Capture `git rev-parse HEAD` at the time of fix application; record it
   in the V2 re-validation section.

Append a new section to
`.planning/phases/V1-vault-vendor-validation-and-docs/elexon-VALIDATION.md`
(do NOT rewrite existing rows):

```markdown
## V2 re-validation (2026-05-09)

**Fix commit:** <SHA>

| Dataset | V1 Status | V1 Rows | V2 Status | V2 Rows | Window OK? |
|---------|-----------|---------|-----------|---------|------------|
| `freq` | PASS (latest 5761, *wrong* window) | 5761 | PASS (≈120 rows in 1h window) | <count> | YES |

### `freq` re-validation curl

\`\`\`bash
curl --ssl-no-revoke -fsS \
  -H "Accept: application/json" \
  "https://data.elexon.co.uk/bmrs/api/v1/datasets/FREQ?measurementDateTimeFrom=2026-05-09T00:00Z&measurementDateTimeTo=2026-05-09T01:00Z&format=json"
\`\`\`

- HTTP: `200`
- Bytes: `<bytes>`
- Rows: `<count>`
- All `measurementTime` values in `[2026-05-09T00:00Z, 2026-05-09T01:00Z]`: **yes**
```
</action>

<acceptance_criteria>
- `.tmp/elexon-freq-v2-revalidation.json` exists; HTTP 200; row count
  20–500 (consistent with a 1-hour 1-second-resolution window).
- All `measurementTime` (or equivalent) values within the requested
  window.
- `elexon-VALIDATION.md` has a new `## V2 re-validation` section with
  the `freq` row appended; the original V1 row stays unchanged.
</acceptance_criteria>

### Task 6 — Update vault dataset page + endpoints.md

<read_first>
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\datasets\freq.md
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\elexon\endpoints.md
</read_first>

<action>
1. In `freq.md`:
   - Update the `## API endpoint` section's "Query parameters" subsection
     to show the correct param names
     `measurementDateTimeFrom`/`measurementDateTimeTo` (lift content from
     the `## Implementation delta`).
   - Replace the `## Implementation delta` paragraph that flagged the
     bug with a one-line "Resolved in V2 (2026-05-09)" reference; do not
     remove the section entirely (it's part of the template).
   - Bump frontmatter `last_verified: 2026-05-09`.
   - Add a `## Changelog` section (if absent) with a single bullet
     `2026-05-09 — V2 fix: connector now sends measurementDateTimeFrom/To
     instead of publishDateTimeFrom/To. See gridflow commit <SHA>.`
   - Update the "Working curl example" block to use the correct param
     names.

2. In `endpoints.md`:
   - Find the `freq` row in the quick-summary table.
   - Update its parameter-style or notes column to read
     `measurementDateTimeFrom/To` instead of `publishDateTimeFrom/To`
     (keep all other columns identical).
   - Bump the file's `updated:` (or footer date) to `2026-05-09`.
</action>

<acceptance_criteria>
- `freq.md` opens with `last_verified: 2026-05-09` in frontmatter.
- `freq.md` `## Implementation delta` section no longer describes the
  bug as open; instead references V2 commit.
- `endpoints.md` `freq` row uses the correct param names.
- No other vault pages or endpoint rows are touched.
</acceptance_criteria>

### Task 7 — Commit

<action>
1. Stage:
   ```
   git add src/gridflow/connectors/elexon/endpoints.py
   git add tests/  # the regression test path
   git add .planning/phases/V1-vault-vendor-validation-and-docs/elexon-VALIDATION.md
   git add C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/datasets/freq.md
   git add C:/Users/Bobbo/OneDrive/Desktop/Learning/AI/quant-vault/30-vendors/elexon/endpoints.md
   ```
   Note: vault files live OUTSIDE the gridflow git tree (V1-CONTEXT.md
   §"Vault absolute path") — they will not stage. The vault edits are
   captured in the Obsidian vault's own change log; do not attempt to
   pull them into a gridflow commit.

2. Commit with conventional-commit prefix:
   ```
   fix(V2-A): elexon freq sends measurementDateTimeFrom/To not publishDateTimeFrom/To

   Without this fix, the API silently ignores the wrong-named params
   and returns the latest 5761 samples regardless of the requested
   window. Bronze files produced by `gridflow ingest elexon freq --last
   24h` previously captured the wrong rows.

   Live-revalidated against /datasets/FREQ on 2026-05-09 with a 1-hour
   window; response correctly bounded.

   Closes V2-FIX-01.
   ```
</action>

<acceptance_criteria>
- `git log --oneline -1` shows the new commit with prefix `fix(V2-A):`.
- `git status` is clean (modulo untracked `.tmp/*.json` artefacts and the
  vault files outside the tree).
</acceptance_criteria>

## Risks / known-unknowns

- **Schedule cadence question.** `config/sources.yaml` for `freq` may
  declare `max_query_days` based on the assumption that windows are
  publish-time. After the fix, the param semantics change to
  measurement-time. Verify `max_query_days` still makes sense — if the
  API enforces a max measurement-window range, that value may need
  reducing. (Likely irrelevant — high-frequency data — but flag it in
  the commit body if encountered.)
- **`@RETRY_POLICY` and pagination.** The `supports_pagination=True`
  flag stays. After the fix, page sizes change (correctly bounded
  windows produce smaller pages). The existing pagination loop in the
  Elexon connector should handle this — but the regression test should
  call a 1-day window once and confirm the connector exits the
  pagination loop in finite time.
- **Bronze re-ingest.** Existing bronze files for `freq` are wrong —
  every windowed call produced "latest 5761 samples". Re-ingesting
  historical `freq` is **out of V2 scope**; record as a backlog item in
  V2-PLAN-F's close-out aggregator.

## Verification

```bash
uv run pytest -m "not live and not slow" -x -q
uv run mypy --strict src/gridflow/connectors/elexon/endpoints.py
uv run ruff check src/gridflow/connectors/elexon/endpoints.py
```

All three must exit `0` before this plan is marked done.
