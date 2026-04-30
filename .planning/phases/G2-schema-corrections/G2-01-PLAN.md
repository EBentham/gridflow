---
phase: G2-schema-corrections
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - src/gridflow/schemas/entsoe.py
  - src/gridflow/connectors/entsoe/endpoints.py
  - src/gridflow/silver/entsoe/load_forecast.py
  - src/gridflow/silver/entsoe/load_forecast_weekly.py
  - src/gridflow/silver/entsoe/wind_solar_forecast.py
  - src/gridflow/silver/entsoe/installed_capacity.py
  - tests/unit/test_entsoe.py
autonomous: true
requirements:
  - GAP-01
  - GAP-02
  - GAP-03a
  - GAP-05
  - GAP-08

must_haves:
  truths:
    - "EntsoeLoadForecast schema has forecast_horizon field with default 'day_ahead'"
    - "EntsoeLoadForecastWeekly schema has forecast_horizon field with default 'week_ahead'"
    - "EntsoeWindSolarForecast schema uses generation_forecast_mw (not forecast_mw)"
    - "EntsoeInstalledCapacity schema uses capacity_mw (not installed_capacity_mw)"
    - "imbalance_volume DOC_TYPES entry has process_type=None (not 'A16')"
    - "LoadForecastTransformer silver output contains forecast_horizon column with value 'day_ahead'"
    - "LoadForecastWeeklyTransformer silver output contains forecast_horizon column with value 'week_ahead'"
    - "WindSolarForecastTransformer silver output column is generation_forecast_mw"
    - "InstalledCapacityTransformer silver output column is capacity_mw"
    - "All unit tests pass after changes"
  artifacts:
    - path: "src/gridflow/schemas/entsoe.py"
      provides: "Updated Pydantic schemas for 4 datasets"
      contains: "forecast_horizon"
    - path: "src/gridflow/connectors/entsoe/endpoints.py"
      provides: "Fixed imbalance_volume process_type"
      contains: "imbalance_volume"
    - path: "src/gridflow/silver/entsoe/wind_solar_forecast.py"
      provides: "Renamed column transformer"
      contains: "generation_forecast_mw"
    - path: "src/gridflow/silver/entsoe/installed_capacity.py"
      provides: "Renamed column transformer"
      contains: "capacity_mw"
    - path: "tests/unit/test_entsoe.py"
      provides: "Updated assertions for all changed fields"
      contains: "forecast_horizon"
  key_links:
    - from: "src/gridflow/schemas/entsoe.py"
      to: "tests/unit/test_entsoe.py"
      via: "Pydantic model instantiation in schema tests"
      pattern: "EntsoeWindSolarForecast.*generation_forecast_mw"
    - from: "src/gridflow/silver/entsoe/wind_solar_forecast.py"
      to: "src/gridflow/schemas/entsoe.py"
      via: "transformer output column must match schema field name"
      pattern: "generation_forecast_mw"
    - from: "src/gridflow/connectors/entsoe/endpoints.py"
      to: "tests/unit/test_entsoe.py"
      via: "TestPhase3Endpoints.test_imbalance_volume_doc_type assertion"
      pattern: "process_type is None"
---

<objective>
Apply 5 targeted schema corrections to align Phase 1/2 ENTSO-E silver layer with the spec (entsoe-extension-audit.md GAP-01, GAP-02, GAP-03a, GAP-05, GAP-08).

Purpose: Silver schemas and transformers have column name mismatches and missing fields vs. the spec. Downstream gold builders and consumers expect the spec-defined names.

Output: Updated schemas, 4 transformer files, 1 endpoint file, and updated tests. No new files created.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/ROADMAP.md

<!-- GAP definitions (from entsoe-extension-audit.md):
  GAP-01 (load_forecast):         Missing forecast_horizon field; spec requires "day_ahead" literal default.
  GAP-02 (load_forecast_weekly):  Missing forecast_horizon field; spec requires "week_ahead" literal default.
  GAP-03a (wind_solar_forecast):  Column forecast_mw; spec requires generation_forecast_mw.
  GAP-05 (installed_capacity):    Column installed_capacity_mw; spec requires capacity_mw.
  GAP-08 (imbalance_volume):      DOC_TYPES process_type="A16"; spec requires None.
-->
</context>

<tasks>

<task type="auto">
  <name>Task 1: Update schemas and endpoints contracts (GAP-01, GAP-02, GAP-03a, GAP-05, GAP-08)</name>
  <read_first>
    - src/gridflow/schemas/entsoe.py (current schema definitions — read fully before editing)
    - src/gridflow/connectors/entsoe/endpoints.py (current DOC_TYPES table — read fully before editing)
    - src/gridflow/schemas/entsoe.py lines 163-183 (EntsoeGenerationForecast — reference for generation_forecast_mw naming already used there)
  </read_first>
  <files>src/gridflow/schemas/entsoe.py, src/gridflow/connectors/entsoe/endpoints.py</files>
  <action>
Make exactly these changes to `src/gridflow/schemas/entsoe.py`:

1. **EntsoeLoadForecast** (lines 81-95): Add `forecast_horizon: str = "day_ahead"` field after `resolution`. Update the docstring to mention A65/A01.
   Final field order: timestamp_utc, area_code, load_forecast_mw, resolution, forecast_horizon, data_provider.

2. **EntsoeWindSolarForecast** (lines 98-116): Rename the field `forecast_mw: float` → `generation_forecast_mw: float`. Update the docstring accordingly (remove "forecast_mw:" comment if present).

3. **EntsoeInstalledCapacity** (lines 141-160): Rename the field `installed_capacity_mw: float` → `capacity_mw: float`. Update the docstring: change "installed_capacity_mw: Total installed capacity in MW." → "capacity_mw: Total installed capacity in MW."

4. **EntsoeLoadForecastWeekly** (lines 185-199): Add `forecast_horizon: str = "week_ahead"` field after `resolution`. Update the docstring to mention A65/A31.
   Final field order: timestamp_utc, area_code, load_forecast_mw, resolution, forecast_horizon, data_provider.

Make exactly this change to `src/gridflow/connectors/entsoe/endpoints.py`:

5. **imbalance_volume DOC_TYPES entry** (line 36): Change `"imbalance_volume": EntsoeDocType("A86", "A16", ...)` → `"imbalance_volume": EntsoeDocType("A86", None, "Imbalance volumes", domain_style="control_area")`.
  </action>
  <verify>
    <automated>
      cd /c/Users/Bobbo/OneDrive/Desktop/Python/gridflow && python -c "
from gridflow.schemas.entsoe import EntsoeLoadForecast, EntsoeLoadForecastWeekly, EntsoeWindSolarForecast, EntsoeInstalledCapacity
from gridflow.connectors.entsoe.endpoints import DOC_TYPES
from datetime import datetime, UTC
ts = datetime(2024, 1, 15, tzinfo=UTC)
lf = EntsoeLoadForecast(timestamp_utc=ts, area_code='X', load_forecast_mw=1.0)
assert lf.forecast_horizon == 'day_ahead', lf.forecast_horizon
lfw = EntsoeLoadForecastWeekly(timestamp_utc=ts, area_code='X', load_forecast_mw=1.0)
assert lfw.forecast_horizon == 'week_ahead', lfw.forecast_horizon
wsf = EntsoeWindSolarForecast(timestamp_utc=ts, area_code='X', production_type='B19', generation_forecast_mw=1.0)
assert wsf.generation_forecast_mw == 1.0
ic = EntsoeInstalledCapacity(timestamp_utc=ts, area_code='X', production_type='B19', capacity_mw=1.0)
assert ic.capacity_mw == 1.0
assert DOC_TYPES['imbalance_volume'].process_type is None, DOC_TYPES['imbalance_volume'].process_type
print('Task 1 contracts OK')
"
    </automated>
  </verify>
  <done>
    - EntsoeLoadForecast has forecast_horizon: str = "day_ahead"
    - EntsoeLoadForecastWeekly has forecast_horizon: str = "week_ahead"
    - EntsoeWindSolarForecast field is generation_forecast_mw (not forecast_mw)
    - EntsoeInstalledCapacity field is capacity_mw (not installed_capacity_mw)
    - DOC_TYPES["imbalance_volume"].process_type is None
    - Python import/instantiation check passes with no AssertionError
  </done>
</task>

<task type="auto">
  <name>Task 2: Update the 4 silver transformers to emit corrected column names and forecast_horizon literals</name>
  <read_first>
    - src/gridflow/silver/entsoe/load_forecast.py (full file — see current output_cols and with_columns call)
    - src/gridflow/silver/entsoe/load_forecast_weekly.py (full file)
    - src/gridflow/silver/entsoe/wind_solar_forecast.py (full file — note forecast_mw rename needed in 4 places)
    - src/gridflow/silver/entsoe/installed_capacity.py (full file — note installed_capacity_mw rename needed in 4 places)
    - src/gridflow/silver/entsoe/generation_forecast.py (read as reference: it already uses generation_forecast_mw correctly and emits it via output_cols — mirror this exact pattern)
  </read_first>
  <files>
    src/gridflow/silver/entsoe/load_forecast.py,
    src/gridflow/silver/entsoe/load_forecast_weekly.py,
    src/gridflow/silver/entsoe/wind_solar_forecast.py,
    src/gridflow/silver/entsoe/installed_capacity.py
  </files>
  <action>
**load_forecast.py** — Add `forecast_horizon` literal to the DataFrame (GAP-01):
- In `transform()`, extend the `with_columns([...])` block that adds `data_provider` and `ingested_at` to also add `pl.lit("day_ahead").alias("forecast_horizon")`.
- Add `"forecast_horizon"` to the `output_cols` list between `"resolution"` and `"data_provider"`.
- No rename changes needed (load_forecast_mw is already correct).

**load_forecast_weekly.py** — Add `forecast_horizon` literal (GAP-02):
- Same change as load_forecast.py but use `pl.lit("week_ahead").alias("forecast_horizon")`.
- Add `"forecast_horizon"` to `output_cols` between `"resolution"` and `"data_provider"`.

**wind_solar_forecast.py** — Rename `forecast_mw` → `generation_forecast_mw` (GAP-03a):
- Line with `df = raw_df.rename({"value": "forecast_mw", ...})` → change to `{"value": "generation_forecast_mw", ...}`.
- Line `df = df.with_columns(pl.col("forecast_mw").cast(pl.Float64))` → change `"forecast_mw"` to `"generation_forecast_mw"`.
- `output_cols` list: replace `"forecast_mw"` with `"generation_forecast_mw"`.
- The module docstring references "forecast_mw" — update to "generation_forecast_mw" if present.
- Do NOT change anything else (production_type handling, dedup logic, sort order).

**installed_capacity.py** — Rename `installed_capacity_mw` → `capacity_mw` (GAP-05):
- Line `df = raw_df.rename({"value": "installed_capacity_mw", ...})` → change to `{"value": "capacity_mw", ...}`.
- Line `df = df.with_columns(pl.col("installed_capacity_mw").cast(pl.Float64))` → change to `"capacity_mw"`.
- `output_cols` list: replace `"installed_capacity_mw"` with `"capacity_mw"`.
- Class docstring: change `installed_capacity_mw: Total installed capacity in MW.` → `capacity_mw: Total installed capacity in MW.`
- Do NOT change anything else (production_type handling, dedup logic, sort order).
  </action>
  <verify>
    <automated>
      cd /c/Users/Bobbo/OneDrive/Desktop/Python/gridflow && python -c "
import polars as pl
from datetime import datetime, UTC
from gridflow.connectors.entsoe.parsers import parse_timeseries_xml
from pathlib import Path
from gridflow.silver.entsoe.load_forecast import LoadForecastTransformer
from gridflow.silver.entsoe.load_forecast_weekly import LoadForecastWeeklyTransformer
from gridflow.silver.entsoe.wind_solar_forecast import WindSolarForecastTransformer
from gridflow.silver.entsoe.installed_capacity import InstalledCapacityTransformer

FIXTURES = Path('tests/fixtures/entsoe')

def make_t(cls):
    t = cls.__new__(cls)
    t.data_dir = Path('/tmp/test')
    t.bronze_dir = Path('/tmp/test/bronze/entsoe/' + cls.dataset)
    t.silver_dir = Path('/tmp/test/silver/entsoe/' + cls.dataset)
    return t

def make_df(fname, vtag):
    xml = (FIXTURES / fname).read_bytes()
    return pl.DataFrame(parse_timeseries_xml(xml, value_tag=vtag))

lf = make_t(LoadForecastTransformer).transform(make_df('load_forecast_gb.xml', 'quantity'))
assert 'forecast_horizon' in lf.columns, 'load_forecast missing forecast_horizon'
assert lf['forecast_horizon'][0] == 'day_ahead', lf['forecast_horizon'][0]

lfw = make_t(LoadForecastWeeklyTransformer).transform(make_df('load_forecast_weekly_gb.xml', 'quantity'))
assert 'forecast_horizon' in lfw.columns, 'load_forecast_weekly missing forecast_horizon'
assert lfw['forecast_horizon'][0] == 'week_ahead', lfw['forecast_horizon'][0]

wsf = make_t(WindSolarForecastTransformer).transform(make_df('wind_solar_forecast_gb.xml', 'quantity'))
assert 'generation_forecast_mw' in wsf.columns, 'wind_solar_forecast missing generation_forecast_mw'
assert 'forecast_mw' not in wsf.columns, 'wind_solar_forecast still has old forecast_mw'

ic = make_t(InstalledCapacityTransformer).transform(make_df('installed_capacity_gb.xml', 'quantity'))
assert 'capacity_mw' in ic.columns, 'installed_capacity missing capacity_mw'
assert 'installed_capacity_mw' not in ic.columns, 'installed_capacity still has old installed_capacity_mw'

print('Task 2 transformers OK')
"
    </automated>
  </verify>
  <done>
    - LoadForecastTransformer output DataFrame has column "forecast_horizon" == "day_ahead"
    - LoadForecastWeeklyTransformer output DataFrame has column "forecast_horizon" == "week_ahead"
    - WindSolarForecastTransformer output has "generation_forecast_mw" and no "forecast_mw"
    - InstalledCapacityTransformer output has "capacity_mw" and no "installed_capacity_mw"
    - Inline python verification script exits with no AssertionError
  </done>
</task>

<task type="auto">
  <name>Task 3: Update tests/unit/test_entsoe.py to match all corrected names and fields</name>
  <read_first>
    - tests/unit/test_entsoe.py (full file — mandatory; do not rely on memory of line numbers)
  </read_first>
  <files>tests/unit/test_entsoe.py</files>
  <action>
Update `tests/unit/test_entsoe.py`. Find every assertion and instantiation that uses the old names and replace them. All changes are find-and-replace; no test logic changes.

**TestWindSolarForecastTransformer (class around line 517):**
- `test_transform_basic`: change `assert "forecast_mw" in result.columns` → `assert "generation_forecast_mw" in result.columns`
- `test_forecast_values`: change `result["forecast_mw"][0]` → `result["generation_forecast_mw"][0]`
- Add new test method at the end of the class:
  ```python
  def test_generation_forecast_mw_column_name(self):
      raw = _make_df_from_xml("wind_solar_forecast_gb.xml", "quantity")
      result = self.t.transform(raw)
      assert "generation_forecast_mw" in result.columns
      assert "forecast_mw" not in result.columns
  ```

**TestInstalledCapacityTransformer (class around line 614):**
- `test_transform_basic`: change `assert "installed_capacity_mw" in result.columns` → `assert "capacity_mw" in result.columns`
- `test_capacity_values`: change `result["installed_capacity_mw"][0]` → `result["capacity_mw"][0]`
- Add new test method at the end of the class:
  ```python
  def test_capacity_mw_column_name(self):
      raw = _make_df_from_xml("installed_capacity_gb.xml", "quantity")
      result = self.t.transform(raw)
      assert "capacity_mw" in result.columns
      assert "installed_capacity_mw" not in result.columns
  ```

**TestLoadForecastTransformer (class around line 467):**
- Add new test method at the end of the class:
  ```python
  def test_forecast_horizon_day_ahead(self):
      raw = _make_df_from_xml("load_forecast_gb.xml", "quantity")
      result = self.t.transform(raw)
      assert "forecast_horizon" in result.columns
      assert result["forecast_horizon"][0] == "day_ahead"
  ```

**TestLoadForecastWeeklyTransformer (class around line 826):**
- Add new test method at the end of the class:
  ```python
  def test_forecast_horizon_week_ahead(self):
      raw = _make_df_from_xml("load_forecast_weekly_gb.xml", "quantity")
      result = self.t.transform(raw)
      assert "forecast_horizon" in result.columns
      assert result["forecast_horizon"][0] == "week_ahead"
  ```

**TestEntsoeWindSolarForecastSchema (class around line 692):**
- `test_valid_record`: change `forecast_mw=3200.0` → `generation_forecast_mw=3200.0`. Change `assert r.production_type == "B19"` — keep that assertion. Add assertion: `assert r.generation_forecast_mw == 3200.0`.
- `test_naive_timestamp_rejected`: change `forecast_mw=3200.0` → `generation_forecast_mw=3200.0`.

**TestEntsoeInstalledCapacitySchema (class around line 747):**
- `test_valid_record`: change `installed_capacity_mw=15200.0` → `capacity_mw=15200.0`. Change `assert r.installed_capacity_mw == 15200.0` → `assert r.capacity_mw == 15200.0`.
- `test_naive_timestamp_rejected`: change `installed_capacity_mw=15200.0` → `capacity_mw=15200.0`.

**TestEntsoeLoadForecastSchema (class around line 670):**
- `test_valid_record`: add assertion `assert r.forecast_horizon == "day_ahead"` after the existing assertions.

**TestEntsoeLoadForecastWeeklySchema (class around line 941):**
- `test_valid_record`: add assertion `assert r.forecast_horizon == "week_ahead"` after the existing assertions.

**TestPhase3Endpoints.test_imbalance_volume_doc_type (class around line 1086):**
- Change `assert iv.process_type == "A16"` → `assert iv.process_type is None`

Do NOT modify any other tests. Do NOT add imports (all needed classes are already imported at the top of the file).
  </action>
  <verify>
    <automated>cd /c/Users/Bobbo/OneDrive/Desktop/Python/gridflow && uv run pytest tests/unit/test_entsoe.py -x -q 2>&1 | tail -20</automated>
  </verify>
  <done>
    - uv run pytest tests/unit/test_entsoe.py -x -q passes with 0 failures
    - "generation_forecast_mw" appears in TestWindSolarForecastTransformer assertions
    - "capacity_mw" appears in TestInstalledCapacityTransformer assertions
    - "forecast_horizon" appears in TestLoadForecastTransformer and TestLoadForecastWeeklyTransformer assertions
    - test_imbalance_volume_doc_type asserts process_type is None
  </done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| N/A | All changes are internal column renames and default-value additions. No new trust boundaries introduced. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-G2-01 | Tampering | schema/transformer column names | accept | Internal rename with no external input; Pydantic validation unchanged |
</threat_model>

<verification>
Full suite smoke check after all three tasks:

```bash
cd /c/Users/Bobbo/OneDrive/Desktop/Python/gridflow && uv run pytest tests/unit/test_entsoe.py -x -q
```

Expected: all tests pass. No new imports required.

Column-name correctness checks (grep must return matches, must not return old names):
```bash
grep "generation_forecast_mw" src/gridflow/silver/entsoe/wind_solar_forecast.py
grep -v "installed_capacity_mw" src/gridflow/silver/entsoe/installed_capacity.py | grep "capacity_mw"
grep "forecast_horizon" src/gridflow/silver/entsoe/load_forecast.py
grep "forecast_horizon" src/gridflow/silver/entsoe/load_forecast_weekly.py
```
</verification>

<success_criteria>
- `uv run pytest tests/unit/test_entsoe.py -x -q` exits 0 with no failures
- `grep "installed_capacity_mw" src/gridflow/silver/entsoe/installed_capacity.py` returns no matches (old name gone from transformer)
- `grep "forecast_mw" src/gridflow/silver/entsoe/wind_solar_forecast.py` returns no matches (old name gone from transformer)
- `grep "forecast_horizon" src/gridflow/silver/entsoe/load_forecast.py` returns at least 2 matches (lit + output_cols)
- `python -c "from gridflow.connectors.entsoe.endpoints import DOC_TYPES; assert DOC_TYPES['imbalance_volume'].process_type is None"` exits 0
</success_criteria>

<output>
After completion, create `.planning/phases/G2-schema-corrections/G2-01-SUMMARY.md` following the summary template.
</output>
