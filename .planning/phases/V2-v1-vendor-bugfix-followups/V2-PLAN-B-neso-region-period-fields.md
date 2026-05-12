---
phase: V2
plan_id: V2-PLAN-B-neso-region-period-fields
slug: neso-region-period-field-resolution
status: draft
milestone: v0.10
wave: 1
severity: HIGH
depends_on: []
autonomous: true
files_modified:
  - src/gridflow/silver/neso/carbon_intensity.py
  - tests/integration/test_neso_silver.py  # or wherever NESO regional silver tests live
  - tests/fixtures/neso/regional_intensity_fw24h_period_keyed.json  # new period-keyed fixture
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso\datasets\regional_current.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso\datasets\regional_intensity.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso\datasets\regional_intensity_fw24h.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso\datasets\regional_intensity_fw48h.md
  - C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso\datasets\regional_intensity_pt24h.md
  - .planning/phases/V1-vault-vendor-validation-and-docs/neso-VALIDATION.md
requirements:
  - V2-FIX-02
---

# V2 Plan B — NESO `_rows_from_region_period` Region/Period Field Resolution

## Goal

Fix the silver-layer field-extraction bug in NESO regional carbon-intensity
data. For period-keyed regional payloads (5 datasets: `regional_current`,
`regional_intensity_fw24h`, `regional_intensity_fw48h`,
`regional_intensity_pt24h`, `regional_intensity`), the live API places
`intensity` and `generationmix` on each `region` dict, not on the `period`
dict. The current `_rows_from_region_period` function reads them
unconditionally from `period`, so silver rows for these 5 datasets land
with null `forecast`, `actual`, `index`, `fuel`, and `perc` columns.

After this plan: the function reads from whichever level holds the data
(`region` first, fall back to `period`), region-keyed payloads continue to
work identically, and a new fixture-backed regression test holds the line.

## Background

The live API returns three regional shapes (per V1 neso-VALIDATION
Findings §2):

| Shape | Routes | `data[]` entry |
|-------|--------|----------------|
| Period-keyed list | `/regional`, `/regional/intensity/{from}/{fw24h\|fw48h\|pt24h}`, `/regional/intensity/{from}/{to}` | `{from, to, regions:[{regionid, dnoregion, shortname, intensity:{…}, generationmix:[…]}, …×18]}` |
| Region-keyed list | `/regional/{england\|scotland\|wales}`, `/regional/postcode/{postcode}`, `/regional/regionid/{regionid}` | One region: `{regionid, dnoregion, shortname, [postcode,] data:[{from, to, intensity:{…}, generationmix:[…]}, …]}` |
| Region-keyed object | `/regional/intensity/{from}/{fw24h\|fw48h\|pt24h}/{postcode\|regionid}/X`, `/regional/intensity/{from}/{to}/{postcode\|regionid}/X` | `data` is the single region object, not a list. |

The bug is in the period-keyed branch only.

## must_haves (goal-backward verification)

1. `_rows_from_region_period(region, period)` reads `intensity` from
   `region.get("intensity") or period.get("intensity") or {}`,
   reads `generationmix` from `region.get("generationmix") or
   period.get("generationmix") or []`. (Or an equivalent
   "first-source-with-data wins" idiom.)
2. The 5 affected datasets produce silver rows with non-null
   `forecast`, `index`, `fuel`, `perc` columns when the period-keyed live
   payload is processed. (`actual` may legitimately be null for forward
   periods — that's vendor behaviour, not a bug.)
3. Region-keyed routes (12 datasets — already correct in V1) continue to
   produce identical silver output to before the fix. Regression test
   holds this.
4. A new fixture
   `tests/fixtures/neso/regional_intensity_fw24h_period_keyed.json`
   captures a real period-keyed live response (or a trimmed version) and
   the regression test asserts that the silver rows derived from it have
   the populated columns.
5. V1 `neso-VALIDATION.md` gets a `## V2 re-validation` section recording
   the fix commit + a re-validation run that pipes the live
   `regional_intensity_fw24h` response through the patched silver and
   confirms non-null carbon/mix columns.
6. The 5 affected vault dataset pages have their `## Implementation
   delta` blocks updated to "Resolved in V2 (2026-05-09)"; frontmatter
   `last_verified: 2026-05-09`; new `## Changelog` line.
7. `uv run pytest -m "not live and not slow" -x -q` passes.

## Tasks

### Task 1 — Pre-flight smoke + .env (NESO is keyless)

<read_first>
- .planning/phases/V1-vault-vendor-validation-and-docs/V1-CONTEXT.md
- .planning/phases/V2-v1-vendor-bugfix-followups/V2-CONTEXT.md
</read_first>

<action>
1. NESO is public — no key needed. Still copy `.env` if missing for
   consistency with other plans in the wave (Task 1 mirrors PLAN-A
   exactly):
   `[ -f .env ] || cp "C:/Users/Bobbo/OneDrive/Desktop/Python/gridflow/.env" .env`
2. `mkdir -p .tmp`
3. `curl --ssl-no-revoke -fsS -o /dev/null -w "%{http_code}\n" https://api.carbonintensity.org.uk/intensity`
   Expect `200`.
</action>

<acceptance_criteria>
- Carbon-intensity smoke test exits `0` with `200`.
- `.tmp/` exists.
</acceptance_criteria>

### Task 2 — Capture period-keyed live payload as fixture

<read_first>
- src/gridflow/silver/neso/carbon_intensity.py (full file, especially
  lines 227–295: `_extract_regional_rows` →
  `_rows_from_period_regions` → `_rows_from_region_container` →
  `_rows_from_region_period` → `_generation_mix_rows`)
- tests/fixtures/neso/  (existing fixtures — find the file naming
  convention and reuse it)
</read_first>

<action>
1. Live curl the period-keyed `regional_intensity_fw24h` route and save
   the response (Throttle 0.2s between NESO calls):
   ```bash
   curl --ssl-no-revoke -fsS -H "Accept: application/json" \
     "https://api.carbonintensity.org.uk/regional/intensity/2026-05-06T00:00Z/fw24h" \
     -o .tmp/neso-regional_intensity_fw24h-period-keyed.json \
     -w "HTTP %{http_code} | %{size_download}B\n"
   ```
   Expect HTTP 200, ~341 KB.

2. Curl the region-keyed counterpart so the test can compare both
   shapes:
   ```bash
   curl --ssl-no-revoke -fsS -H "Accept: application/json" \
     "https://api.carbonintensity.org.uk/regional/intensity/2026-05-06T00:00Z/fw24h/regionid/13" \
     -o .tmp/neso-regional_intensity_fw24h-region-keyed.json \
     -w "HTTP %{http_code} | %{size_download}B\n"
   ```
   Expect HTTP 200, ~18 KB.

3. Trim the period-keyed payload to a fixture-friendly size: keep the
   first 2 periods × all 18 regions (so the fixture exercises the full
   18-region nested loop but stays under 50 KB). Use Python:
   ```python
   import json
   d = json.load(open(".tmp/neso-regional_intensity_fw24h-period-keyed.json"))
   d["data"] = d["data"][:2]
   open("tests/fixtures/neso/regional_intensity_fw24h_period_keyed.json","w").write(json.dumps(d, indent=2))
   ```

4. Save the region-keyed payload as
   `tests/fixtures/neso/regional_intensity_fw24h_region_keyed.json`
   (full payload — small enough).
</action>

<acceptance_criteria>
- `tests/fixtures/neso/regional_intensity_fw24h_period_keyed.json`
  exists, < 50 KB, contains `data` (list) and the first entry has both
  `regions` (list of 18) and each region has `intensity` and
  `generationmix`.
- `tests/fixtures/neso/regional_intensity_fw24h_region_keyed.json`
  exists, contains `data` (list of 1) with `data[0].regionid == 13` and
  `data[0].data` populated.
</acceptance_criteria>

### Task 3 — Add failing regression test (red)

<read_first>
- tests/integration/test_neso_silver.py (or wherever NESO regional
  silver is tested — find with
  `grep -rn "regional_intensity\|_extract_regional_rows" tests/`)
- The captured fixtures from Task 2.
</read_first>

<action>
Add a new test to the appropriate file. Pseudocode:

```python
import json
from pathlib import Path

import polars as pl
import pytest

from gridflow.silver.neso.carbon_intensity import (
    _extract_regional_rows,
    _transform_regional,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "neso"


def test_period_keyed_regional_payload_populates_carbon_and_mix():
    payload = json.loads(
        (FIXTURES / "regional_intensity_fw24h_period_keyed.json").read_text()
    )
    rows = _extract_regional_rows(payload)
    df = _transform_regional(pl.DataFrame(rows, infer_schema_length=None))

    # Each region in each period should produce one row per fuel in the mix.
    # Forward-looking intensity has forecast non-null but actual nullable.
    assert df.height > 0, "no rows extracted"
    assert df["forecast"].null_count() < df.height, (
        "every forecast value is null — the period-keyed bug is back"
    )
    assert df["fuel"].null_count() < df.height, (
        "every fuel value is null — generation_mix lookup failed"
    )


def test_region_keyed_regional_payload_unchanged_after_fix():
    payload = json.loads(
        (FIXTURES / "regional_intensity_fw24h_region_keyed.json").read_text()
    )
    rows = _extract_regional_rows(payload)
    df = _transform_regional(pl.DataFrame(rows, infer_schema_length=None))

    # Region-keyed payload was correct before the fix; assert unchanged.
    assert df.height > 0
    assert df["forecast"].null_count() < df.height
    assert df["fuel"].null_count() < df.height
```

Run the tests:
```bash
uv run pytest -m "not live and not slow" -x -q -k regional_payload tests/
```

The first test (`period_keyed_…`) must FAIL before the fix in Task 4 (proving
the regression test would have caught the V1 bug).

The second test (`region_keyed_…`) must PASS both before and after the fix
(proves no regression for the working branch).
</action>

<acceptance_criteria>
- The two new tests exist and are discovered by pytest.
- Before Task 4 fix: `test_period_keyed_regional_payload_populates_carbon_and_mix`
  FAILS (every `forecast` and `fuel` value is null).
- Before Task 4 fix: `test_region_keyed_regional_payload_unchanged_after_fix`
  PASSES.
</acceptance_criteria>

### Task 4 — Apply the fix

<read_first>
- src/gridflow/silver/neso/carbon_intensity.py (lines 257–286)
</read_first>

<action>
Edit `src/gridflow/silver/neso/carbon_intensity.py`. Replace the current
`_rows_from_region_period`:

```python
def _rows_from_region_period(
    region: dict[str, Any],
    period: dict[str, Any],
) -> list[dict[str, Any]]:
    base = {
        "regionid": region.get("regionid"),
        "dnoregion": region.get("dnoregion"),
        "shortname": region.get("shortname"),
        "postcode": region.get("postcode"),
        "from": period.get("from"),
        "to": period.get("to"),
    }
    intensity = period.get("intensity", {}) or {}
    base.update({
        "forecast": intensity.get("forecast"),
        "actual": intensity.get("actual"),
        "index": intensity.get("index", ""),
    })

    mixes = _generation_mix_rows(period)
    if not mixes:
        return [base]
    return [
        {
            **base,
            "fuel": mix.get("fuel"),
            "perc": mix.get("perc"),
        }
        for mix in mixes
    ]
```

with:

```python
def _rows_from_region_period(
    region: dict[str, Any],
    period: dict[str, Any],
) -> list[dict[str, Any]]:
    # Live API places intensity and generationmix on `region` for
    # period-keyed regional payloads (`/regional`, `/regional/intensity/.../{fw24h|fw48h|pt24h|to}`),
    # and on `period` for region-keyed payloads (`/regional/{name}`,
    # `/regional/postcode/{pc}`, `/regional/regionid/{id}`). Read from
    # whichever level holds the data.
    base = {
        "regionid": region.get("regionid"),
        "dnoregion": region.get("dnoregion"),
        "shortname": region.get("shortname"),
        "postcode": region.get("postcode"),
        "from": period.get("from"),
        "to": period.get("to"),
    }
    intensity = region.get("intensity") or period.get("intensity") or {}
    base.update({
        "forecast": intensity.get("forecast"),
        "actual": intensity.get("actual"),
        "index": intensity.get("index", ""),
    })

    mixes = _generation_mix_rows(region) or _generation_mix_rows(period)
    if not mixes:
        return [base]
    return [
        {
            **base,
            "fuel": mix.get("fuel"),
            "perc": mix.get("perc"),
        }
        for mix in mixes
    ]
```

Note: this is the only function that needs touching. `_extract_regional_rows`
(line 227) and `_generation_mix_rows` (line 289) stay as-is — both already
support being called with either a region dict or a period dict.
</action>

<acceptance_criteria>
- `_rows_from_region_period` reads `intensity` from
  `region.get("intensity") or period.get("intensity") or {}`.
- `_rows_from_region_period` reads `generationmix` via
  `_generation_mix_rows(region) or _generation_mix_rows(period)`.
- `uv run mypy --strict src/gridflow/silver/neso/carbon_intensity.py`
  exits `0`.
- `uv run ruff check src/gridflow/silver/neso/carbon_intensity.py`
  exits `0`.
- The two regression tests from Task 3 both PASS (the previously-failing
  one now passes, the previously-passing one still passes).
</acceptance_criteria>

### Task 5 — Live re-validation evidence

<action>
1. Run the live curl from Task 2 again on a fresh date and pipe through
   the patched silver:
   ```bash
   curl --ssl-no-revoke -fsS -H "Accept: application/json" \
     "https://api.carbonintensity.org.uk/regional/intensity/2026-05-09T00:00Z/fw24h" \
     -o .tmp/neso-regional_intensity_fw24h-revalidation.json \
     -w "HTTP %{http_code} | %{size_download}B\n"
   ```

2. Programmatically pipe through the patched silver and confirm
   non-null fractions:
   ```python
   import json, polars as pl
   from gridflow.silver.neso.carbon_intensity import (
       _extract_regional_rows, _transform_regional,
   )
   payload = json.loads(open(".tmp/neso-regional_intensity_fw24h-revalidation.json").read())
   rows = _extract_regional_rows(payload)
   df = _transform_regional(pl.DataFrame(rows, infer_schema_length=None))
   print({
       "rows": df.height,
       "forecast_null_pct": df["forecast"].null_count() / df.height,
       "fuel_null_pct": df["fuel"].null_count() / df.height,
   })
   ```
   Expect `forecast_null_pct ≈ 0.0` (forecast available for all periods),
   `fuel_null_pct ≈ 0.0` (one row per fuel per period per region).

3. Append a new section to
   `.planning/phases/V1-vault-vendor-validation-and-docs/neso-VALIDATION.md`
   (preserving V1's existing tables):

   ```markdown
   ## V2 re-validation (2026-05-09)

   **Fix commit:** <SHA>

   The 5 period-keyed regional datasets now populate carbon and mix
   columns from `region`-level fields, falling back to `period`-level if
   absent. Region-keyed routes unchanged.

   | Dataset | V1 carbon/mix populated? | V2 carbon/mix populated? |
   |---------|-------------------------|--------------------------|
   | `regional_current` | NO | YES |
   | `regional_intensity_fw24h` | NO | YES |
   | `regional_intensity_fw48h` | NO | YES |
   | `regional_intensity_pt24h` | NO | YES |
   | `regional_intensity` | NO | YES |

   Re-validation pipeline (representative — `regional_intensity_fw24h`):

   \`\`\`bash
   curl --ssl-no-revoke -fsS -H "Accept: application/json" \
     "https://api.carbonintensity.org.uk/regional/intensity/2026-05-09T00:00Z/fw24h" \
     | python -c "import sys,json,polars as pl; from gridflow.silver.neso.carbon_intensity import _extract_regional_rows,_transform_regional; p=json.load(sys.stdin); df=_transform_regional(pl.DataFrame(_extract_regional_rows(p),infer_schema_length=None)); print(df.head())"
   \`\`\`

   - `forecast` non-null for all 18 regions × N periods.
   - `fuel` non-null for all rows (one row per region × period × fuel).
   ```
</action>

<acceptance_criteria>
- `.tmp/neso-regional_intensity_fw24h-revalidation.json` exists; HTTP 200.
- The Python pipeline prints `forecast_null_pct` and `fuel_null_pct`
  both close to `0.0` (within rounding for forward-looking `actual`).
- `neso-VALIDATION.md` has a new `## V2 re-validation` section as
  above; V1 tables untouched.
</acceptance_criteria>

### Task 6 — Update vault dataset pages (5 affected)

<read_first>
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso\datasets\regional_current.md
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso\datasets\regional_intensity.md
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso\datasets\regional_intensity_fw24h.md
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso\datasets\regional_intensity_fw48h.md
- C:\Users\Bobbo\OneDrive\Desktop\Learning\AI\quant-vault\30-vendors\neso\datasets\regional_intensity_pt24h.md
</read_first>

<action>
For each of the 5 pages:
1. Locate the `## Implementation delta` block referencing the
   `_rows_from_region_period` bug. Replace it with a one-line "Resolved
   in V2 (2026-05-09)" reference; do not delete the section.
2. Bump frontmatter `last_verified: 2026-05-09`.
3. Add a `## Changelog` section (or a single bullet under existing
   one): `2026-05-09 — V2 fix: period-keyed silver now populates
   forecast/actual/index/fuel/perc. See gridflow commit <SHA>.`
4. Where the page's `## Silver layer` schema table notes "currently
   null", remove that caveat (the columns are no longer null).
</action>

<acceptance_criteria>
- All 5 pages have `last_verified: 2026-05-09`.
- All 5 pages reference V2 commit in their delta block.
- No NESO pages outside the 5 affected datasets are touched.
</acceptance_criteria>

### Task 7 — Commit

<action>
Stage and commit (vault files outside the gridflow tree are NOT staged):
```
git add src/gridflow/silver/neso/carbon_intensity.py
git add tests/  # the regression tests
git add tests/fixtures/neso/regional_intensity_fw24h_period_keyed.json
git add tests/fixtures/neso/regional_intensity_fw24h_region_keyed.json
git add .planning/phases/V1-vault-vendor-validation-and-docs/neso-VALIDATION.md
```

Commit message:
```
fix(V2-B): NESO _rows_from_region_period reads carbon/mix from whichever level holds it

For period-keyed regional payloads (regional_current,
regional_intensity, regional_intensity_fw24h,
regional_intensity_fw48h, regional_intensity_pt24h — 5 datasets),
the live NESO API places `intensity` and `generationmix` on each
`region` dict, not on the `period` dict. The previous code read
unconditionally from `period`, dropping all carbon and mix data
for those 5 datasets.

Read from `region` first, fall back to `period`. Region-keyed
routes continue to work unchanged. Two regression fixtures
(period-keyed + region-keyed) and tests added.

Live-revalidated against /regional/intensity/.../fw24h on
2026-05-09; all 18 regions × 24 hours have non-null
forecast/index and one row per fuel.

Closes V2-FIX-02.
```
</action>

<acceptance_criteria>
- `git log --oneline -1` shows the new commit with prefix `fix(V2-B):`.
- `git status` is clean (modulo `.tmp/*.json` artefacts and out-of-tree
  vault files).
</acceptance_criteria>

## Risks / known-unknowns

- **Existing silver fixtures.** If any existing NESO silver fixture
  under `tests/fixtures/neso/` was generated from the buggy code path
  (i.e. has all-null carbon/mix for the 5 period-keyed datasets), tests
  that compare against those fixtures will start FAILING after the fix.
  Find them with
  `grep -l "regional_intensity\|regional_current" tests/fixtures/neso/`
  and sanity-check before commit. **Likely outcome:** fixtures need
  regeneration; do that in this plan, document the regen in the commit
  body.
- **Bronze re-ingest.** Existing silver tables for the 5 affected
  datasets contain null carbon/mix for every row. Re-ingesting historical
  data (or re-running silver from existing bronze) is **out of V2 scope**;
  record as a backlog item in V2-PLAN-F's close-out aggregator.
- **Schema strictness.** Confirm that the silver Pydantic schema (if any)
  for these rows does not have `forecast` / `actual` / `index` /
  `fuel` declared as required-non-null. If it does, the schema would
  have rejected V1's null-filled rows — but V1 wrote them, so the
  schema is permissive. Verify `gridflow.schemas.neso.CarbonIntensity`
  fields are `Optional` for the carbon/mix columns.

## Verification

```bash
uv run pytest -m "not live and not slow" -x -q
uv run mypy --strict src/gridflow/silver/neso/carbon_intensity.py
uv run ruff check src/gridflow/silver/neso/carbon_intensity.py
```

All three must exit `0` before this plan is marked done.
