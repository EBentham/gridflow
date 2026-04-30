---
phase: G3-balancing-code-mapping
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - src/gridflow/schemas/entsoe.py
  - src/gridflow/silver/entsoe/imbalance_prices.py
  - src/gridflow/silver/entsoe/imbalance_volume.py
  - tests/unit/test_entsoe.py
autonomous: true
requirements:
  - GAP-06
  - GAP-07

must_haves:
  truths:
    - "EntsoeImbalancePrices schema has direction: str and price_eur_mwh: float (no business_type, no price_gbp_mwh)"
    - "EntsoeImbalanceVolume schema has direction: str (no flow_direction field)"
    - "ImbalancePricesTransformer.transform() maps businessType A19→'long', A20→'short' via replace_strict and emits direction column"
    - "ImbalancePricesTransformer.transform() renames value column to price_eur_mwh (not price_gbp_mwh)"
    - "ImbalanceVolumeTransformer.transform() maps flow_direction A01→'long', A02→'short' via replace_strict and emits direction column"
    - "Both transformers emit ingested_at using pl.lit(now).cast(pl.Datetime('us', 'UTC')) matching Phase 1/2 pattern"
    - "Dedup keys updated: imbalance_prices on (timestamp_utc, area_code, direction); imbalance_volume on (timestamp_utc, area_code, direction)"
    - "All updated tests pass under uv run pytest tests/unit/test_entsoe.py -x -q"
  artifacts:
    - path: "src/gridflow/schemas/entsoe.py"
      provides: "Updated EntsoeImbalancePrices and EntsoeImbalanceVolume schemas"
      contains: "price_eur_mwh"
    - path: "src/gridflow/silver/entsoe/imbalance_prices.py"
      provides: "Transformer with direction mapping and price_eur_mwh"
      contains: "replace_strict"
    - path: "src/gridflow/silver/entsoe/imbalance_volume.py"
      provides: "Transformer with direction mapping"
      contains: "replace_strict"
  key_links:
    - from: "src/gridflow/silver/entsoe/imbalance_prices.py"
      to: "src/gridflow/schemas/entsoe.py"
      via: "EntsoeImbalancePrices(**sample) contract validation"
      pattern: "EntsoeImbalancePrices\\(\\*\\*sample\\)"
    - from: "src/gridflow/silver/entsoe/imbalance_volume.py"
      to: "src/gridflow/schemas/entsoe.py"
      via: "EntsoeImbalanceVolume(**sample) contract validation"
      pattern: "EntsoeImbalanceVolume\\(\\*\\*sample\\)"
---

<objective>
Update the two imbalance dataset schemas and transformers to emit human-readable
direction strings instead of raw ENTSO-E codes, fix the currency field name, and
add ingested_at to close GAP-06 and GAP-07 for these two datasets.

Purpose: Downstream imbalance-price models match on "long"/"short" strings, not raw
ENTSO-E A19/A20 codes. The GBP currency label is also misleading — ENTSO-E prices
are in EUR.

Output: Updated schemas (direction, price_eur_mwh), updated transformers
(replace_strict mapping, ingested_at), updated tests.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@/c/Users/Bobbo/OneDrive/Desktop/Python/gridflow/.planning/ROADMAP.md
@/c/Users/Bobbo/OneDrive/Desktop/Python/gridflow/CLAUDE.md

<interfaces>
<!-- Extracted from existing files for executor reference. -->

From src/gridflow/schemas/entsoe.py (current state to be changed):
```python
class EntsoeImbalancePrices(BaseSchema):
    timestamp_utc: datetime
    area_code: str
    business_type: str  # A19=excess, A20=shortage  ← REMOVE, replace with direction
    price_gbp_mwh: float                             ← RENAME to price_eur_mwh
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    # ingested_at absent                             ← ADD as datetime | None = None

class EntsoeImbalanceVolume(BaseSchema):
    timestamp_utc: datetime
    area_code: str
    flow_direction: str  # A01=up, A02=down          ← REMOVE, replace with direction
    volume_mwh: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    # ingested_at absent                             ← ADD as datetime | None = None
```

Target schema shape (from spec docs/specs/entsoe-connector-extension.md):
```python
class EntsoeImbalancePrices(BaseSchema):
    timestamp_utc: datetime
    area_code: str
    direction: str          # "long" (A19=surplus) | "short" (A20=deficit)
    price_eur_mwh: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

class EntsoeImbalanceVolume(BaseSchema):
    timestamp_utc: datetime
    area_code: str
    direction: str          # "long" (A01) | "short" (A02)
    volume_mwh: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None
```

From src/gridflow/silver/entsoe/imbalance_prices.py (current transform() pattern):
```python
df = (
    raw_df.rename({"value": "price_gbp_mwh", "control_area_domain": "area_code"})
    .select(["timestamp_utc", "area_code", "business_type", "price_gbp_mwh", "resolution"])
    .unique(subset=["timestamp_utc", "area_code", "business_type"], keep="last")
    .sort(["timestamp_utc", "area_code", "business_type"])
    .with_columns([
        pl.lit("entsoe").alias("data_provider"),
        pl.col("timestamp_utc").dt.replace_time_zone("UTC"),
    ])
)
```

From src/gridflow/silver/entsoe/actual_load.py (ingested_at pattern to replicate):
```python
now = datetime.now(UTC)
df = df.with_columns([
    pl.lit("entsoe").alias("data_provider"),
    pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
])
output_cols = ["timestamp_utc", "area_code", "load_mw", "resolution", "data_provider", "ingested_at"]
available = [c for c in output_cols if c in df.columns]
return df.select(available).sort(...)
```

Polars replace_strict API (correct for Polars >= 1.0):
```python
pl.col("business_type").replace_strict({"A19": "long", "A20": "short"}).alias("direction")
```
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Update EntsoeImbalancePrices and EntsoeImbalanceVolume schemas</name>
  <read_first>
    src/gridflow/schemas/entsoe.py
  </read_first>
  <files>src/gridflow/schemas/entsoe.py</files>
  <action>
In src/gridflow/schemas/entsoe.py, update two schema classes:

**EntsoeImbalancePrices** (currently lines ~225-244):
- Remove field `business_type: str`
- Remove field `price_gbp_mwh: float`
- Add field `direction: str  # "long" (A19=system surplus) | "short" (A20=system deficit)`
- Add field `price_eur_mwh: float`
- Add field `ingested_at: datetime | None = None`
- Update the class docstring to remove the stale `business_type` / `price_gbp_mwh` references

New class body:
```python
class EntsoeImbalancePrices(BaseSchema):
    """Silver-layer schema for ENTSO-E imbalance prices (A85).

    direction: "long" = system surplus (A19), "short" = system deficit (A20).
    price_eur_mwh: Imbalance settlement price in EUR/MWh.
    """

    timestamp_utc: datetime
    area_code: str  # control area EIC mRID
    direction: str  # "long" | "short"
    price_eur_mwh: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v
```

**EntsoeImbalanceVolume** (currently lines ~247-266):
- Remove field `flow_direction: str`
- Add field `direction: str  # "long" (A01) | "short" (A02)`
- Add field `ingested_at: datetime | None = None`
- Update docstring: remove `flow_direction` reference, add `direction` explanation
- `volume_mwh` stays unchanged

New class body:
```python
class EntsoeImbalanceVolume(BaseSchema):
    """Silver-layer schema for ENTSO-E imbalance volumes (A86).

    direction: "long" (A01=generation excess) | "short" (A02=consumption excess).
    volume_mwh: Imbalance volume in MWh.
    """

    timestamp_utc: datetime
    area_code: str
    direction: str  # "long" | "short"
    volume_mwh: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

    @field_validator("timestamp_utc")
    @classmethod
    def must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp_utc must be timezone-aware (UTC)")
        return v
```

No other schema classes in this file are touched.
  </action>
  <verify>
    <automated>cd /c/Users/Bobbo/OneDrive/Desktop/Python/gridflow && grep -n "price_eur_mwh\|direction\|ingested_at" src/gridflow/schemas/entsoe.py | grep -E "ImbalancePrice|ImbalanceVolume|price_eur|flow_direction"</automated>
  </verify>
  <acceptance_criteria>
    - `grep "price_gbp_mwh" src/gridflow/schemas/entsoe.py` returns zero matches inside EntsoeImbalancePrices (the class is gone from schema)
    - `grep "flow_direction" src/gridflow/schemas/entsoe.py` returns zero matches inside EntsoeImbalanceVolume
    - `grep "price_eur_mwh" src/gridflow/schemas/entsoe.py` returns at least one match
    - `grep "ingested_at" src/gridflow/schemas/entsoe.py` returns at least two matches (one per schema class)
    - `grep "direction" src/gridflow/schemas/entsoe.py` returns matches in both EntsoeImbalancePrices and EntsoeImbalanceVolume
  </acceptance_criteria>
  <done>Both schema classes updated with direction, price_eur_mwh (prices only), ingested_at fields; old fields removed.</done>
</task>

<task type="auto">
  <name>Task 2: Update imbalance_prices and imbalance_volume transformers</name>
  <read_first>
    src/gridflow/silver/entsoe/imbalance_prices.py
    src/gridflow/silver/entsoe/imbalance_volume.py
    src/gridflow/silver/entsoe/actual_load.py
  </read_first>
  <files>
    src/gridflow/silver/entsoe/imbalance_prices.py
    src/gridflow/silver/entsoe/imbalance_volume.py
  </files>
  <action>
**imbalance_prices.py** — rewrite transform() method:

1. Add `from datetime import UTC` to imports (if not already present). Add `datetime` import from stdlib.
2. Update `required` set in transform(): replace `"business_type"` with `"business_type"` (it still comes from the parser, so keep it in required for the guard check), keep all others.
3. Change the rename dict: `"value": "price_eur_mwh"` (was `"price_gbp_mwh"`).
4. After rename, add the direction mapping before `.select()`:
   ```python
   .with_columns(
       pl.col("business_type").replace_strict(
           {"A19": "long", "A20": "short"}
       ).alias("direction")
   )
   ```
5. Update `.select()` list: `["timestamp_utc", "area_code", "direction", "price_eur_mwh", "resolution"]`
   (drop `"business_type"` from select — it's consumed by the mapping, not emitted)
6. Update `.unique(subset=...)`: change to `["timestamp_utc", "area_code", "direction"]`
7. Update `.sort(...)`: change to `["timestamp_utc", "area_code", "direction"]`
8. In the `.with_columns([...])` block after sort, add ingested_at:
   ```python
   now = datetime.now(UTC)
   .with_columns([
       pl.lit("entsoe").alias("data_provider"),
       pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
       pl.col("timestamp_utc").dt.replace_time_zone("UTC"),
   ])
   ```
   Note: `now` must be computed before the chain, assigned to a variable, then used as `pl.lit(now)`.
   Structure the code to set `now = datetime.now(UTC)` BEFORE the DataFrame chain (identical to actual_load.py pattern).
9. After the chain, emit a final select for column ordering:
   ```python
   output_cols = [
       "timestamp_utc", "area_code", "direction",
       "price_eur_mwh", "resolution", "data_provider", "ingested_at",
   ]
   available = [c for c in output_cols if c in df.columns]
   df = df.select(available)
   ```
10. Update the docstring of ImbalancePricesTransformer class to mention direction instead of business_type.
11. Update the contract validation sample: `EntsoeImbalancePrices(**sample)` — no change needed since it already uses the schema name.

**imbalance_volume.py** — rewrite transform() method:

1. Add `from datetime import UTC` and `datetime` imports.
2. Keep `required` check but change `"flow_direction"` key to be present (it still comes from parser).
   Keep `required = {"timestamp_utc", "value", "control_area_domain", "flow_direction", "resolution"}`.
3. After the rename `{"value": "volume_mwh", "control_area_domain": "area_code"}`, add direction mapping:
   ```python
   .with_columns(
       pl.col("flow_direction").replace_strict(
           {"A01": "long", "A02": "short"}
       ).alias("direction")
   )
   ```
4. Update `.select()`: `["timestamp_utc", "area_code", "direction", "volume_mwh", "resolution"]`
   (drop `"flow_direction"`)
5. Update `.unique(subset=...)`: `["timestamp_utc", "area_code", "direction"]`
6. Update `.sort(...)`: `["timestamp_utc", "area_code", "direction"]`
7. Add ingested_at using the same pattern as imbalance_prices.py above.
8. Add final `output_cols` select:
   ```python
   output_cols = [
       "timestamp_utc", "area_code", "direction",
       "volume_mwh", "resolution", "data_provider", "ingested_at",
   ]
   available = [c for c in output_cols if c in df.columns]
   df = df.select(available)
   ```
9. Update class docstring to mention direction instead of flow_direction.
  </action>
  <verify>
    <automated>cd /c/Users/Bobbo/OneDrive/Desktop/Python/gridflow && python -c "
from tests.unit.test_entsoe import _make_entsoe_transformer, _make_df_from_xml
from gridflow.silver.entsoe.imbalance_prices import ImbalancePricesTransformer
from gridflow.silver.entsoe.imbalance_volume import ImbalanceVolumeTransformer
t1 = _make_entsoe_transformer(ImbalancePricesTransformer)
r1 = t1.transform(_make_df_from_xml('imbalance_prices_gb.xml', 'price.amount'))
assert 'direction' in r1.columns, f'Missing direction in imbalance_prices: {r1.columns}'
assert 'price_eur_mwh' in r1.columns, f'Missing price_eur_mwh: {r1.columns}'
assert set(r1['direction'].to_list()) == {'long', 'short'}, f'Wrong directions: {set(r1[\"direction\"].to_list())}'
assert 'ingested_at' in r1.columns, f'Missing ingested_at: {r1.columns}'
t2 = _make_entsoe_transformer(ImbalanceVolumeTransformer)
r2 = t2.transform(_make_df_from_xml('imbalance_volume_gb.xml', 'quantity'))
assert 'direction' in r2.columns, f'Missing direction in imbalance_volume: {r2.columns}'
assert set(r2['direction'].to_list()) == {'long', 'short'}, f'Wrong directions: {set(r2[\"direction\"].to_list())}'
assert 'ingested_at' in r2.columns, f'Missing ingested_at: {r2.columns}'
print('ALL OK')
"
</automated>
  </verify>
  <acceptance_criteria>
    - Python check above prints "ALL OK"
    - `grep "price_gbp_mwh" src/gridflow/silver/entsoe/imbalance_prices.py` returns zero matches
    - `grep "replace_strict" src/gridflow/silver/entsoe/imbalance_prices.py` returns a match containing "A19"
    - `grep "replace_strict" src/gridflow/silver/entsoe/imbalance_volume.py` returns a match containing "A01"
    - `grep "ingested_at" src/gridflow/silver/entsoe/imbalance_prices.py` returns a match
    - `grep "ingested_at" src/gridflow/silver/entsoe/imbalance_volume.py` returns a match
    - `grep "flow_direction" src/gridflow/silver/entsoe/imbalance_volume.py` returns matches ONLY in the required-columns guard and the replace_strict expression (not in select/unique/sort)
  </acceptance_criteria>
  <done>Both transformers produce direction column with "long"/"short" values; price_eur_mwh emitted; ingested_at present; dedup keys updated.</done>
</task>

<task type="auto">
  <name>Task 3: Update tests for imbalance_prices and imbalance_volume</name>
  <read_first>
    tests/unit/test_entsoe.py
  </read_first>
  <files>tests/unit/test_entsoe.py</files>
  <action>
Roadmap tasks 7 and 8 explicitly authorize updating tests for Phase 3 transformers.
These are the ONLY test changes permitted — do NOT touch tests for other transformers.

**TestImbalancePricesTransformer** (find class around line 1168):

Replace `test_transform_basic` — old asserts `"price_gbp_mwh"` and `"business_type"` columns:
```python
def test_transform_basic(self):
    raw = _make_df_from_xml("imbalance_prices_gb.xml", "price.amount")
    result = self.t.transform(raw)
    assert not result.is_empty()
    assert "price_eur_mwh" in result.columns      # was price_gbp_mwh
    assert "area_code" in result.columns
    assert "direction" in result.columns           # was business_type
```

Replace `test_business_types_preserved` → rename to `test_direction_values`:
```python
def test_direction_values(self):
    raw = _make_df_from_xml("imbalance_prices_gb.xml", "price.amount")
    result = self.t.transform(raw)
    dirs = set(result["direction"].to_list())
    assert "long" in dirs     # was A19
    assert "short" in dirs    # was A20
```

Replace `test_price_values` — column name change:
```python
def test_price_values(self):
    raw = _make_df_from_xml("imbalance_prices_gb.xml", "price.amount")
    result = self.t.transform(raw).filter(
        pl.col("direction") == "long"    # was business_type == "A19"
    ).sort("timestamp_utc")
    assert abs(result["price_eur_mwh"][0] - 95.50) < 0.01   # was price_gbp_mwh
```

Add new test:
```python
def test_ingested_at_present(self):
    raw = _make_df_from_xml("imbalance_prices_gb.xml", "price.amount")
    result = self.t.transform(raw)
    assert "ingested_at" in result.columns
```

Remove `test_business_types_preserved` (replaced by test_direction_values above).

**TestImbalanceVolumeTransformer** (find class around line 1230):

Replace `test_transform_basic` — old asserts `"flow_direction"`:
```python
def test_transform_basic(self):
    raw = _make_df_from_xml("imbalance_volume_gb.xml", "quantity")
    result = self.t.transform(raw)
    assert not result.is_empty()
    assert "volume_mwh" in result.columns
    assert "direction" in result.columns     # was flow_direction
    assert "area_code" in result.columns
```

Replace `test_flow_directions_preserved` → rename to `test_direction_values`:
```python
def test_direction_values(self):
    raw = _make_df_from_xml("imbalance_volume_gb.xml", "quantity")
    result = self.t.transform(raw)
    dirs = set(result["direction"].to_list())
    assert "long" in dirs     # was A01
    assert "short" in dirs    # was A02
```

Replace `test_volume_values` — filter column changed:
```python
def test_volume_values(self):
    raw = _make_df_from_xml("imbalance_volume_gb.xml", "quantity")
    result = self.t.transform(raw).filter(
        pl.col("direction") == "long"    # was flow_direction == "A01"
    ).sort("timestamp_utc")
    assert abs(result["volume_mwh"][0] - 150) < 0.1
```

Add new test:
```python
def test_ingested_at_present(self):
    raw = _make_df_from_xml("imbalance_volume_gb.xml", "quantity")
    result = self.t.transform(raw)
    assert "ingested_at" in result.columns
```

**TestEntsoeImbalancePricesSchema** (find class around line 1412):

Replace `test_valid_record` — fields changed:
```python
def test_valid_record(self):
    r = EntsoeImbalancePrices(
        timestamp_utc=self._TS,
        area_code="10YGB----------A",
        direction="long",          # was business_type="A19"
        price_eur_mwh=95.50,      # was price_gbp_mwh=95.50
    )
    assert r.data_provider == "entsoe"
    assert r.price_eur_mwh == 95.50   # was price_gbp_mwh
    assert r.direction == "long"       # was business_type == "A19"
```

Replace `test_naive_timestamp_rejected` — update instantiation args:
```python
def test_naive_timestamp_rejected(self):
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        EntsoeImbalancePrices(
            timestamp_utc=datetime(2024, 1, 15),
            area_code="10YGB----------A",
            direction="long",
            price_eur_mwh=95.50,
        )
```

**TestEntsoeImbalanceVolumeSchema** (find class around line 1437):

Replace `test_valid_record` — `flow_direction` → `direction`:
```python
def test_valid_record(self):
    r = EntsoeImbalanceVolume(
        timestamp_utc=self._TS,
        area_code="10YGB----------A",
        direction="long",     # was flow_direction="A01"
        volume_mwh=150.0,
    )
    assert r.data_provider == "entsoe"
    assert r.volume_mwh == 150.0
    assert r.direction == "long"   # was flow_direction == "A01"
```

Replace `test_naive_timestamp_rejected`:
```python
def test_naive_timestamp_rejected(self):
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        EntsoeImbalanceVolume(
            timestamp_utc=datetime(2024, 1, 15),
            area_code="10YGB----------A",
            direction="long",
            volume_mwh=150.0,
        )
```

After all edits, run the full test suite to confirm no regressions.
  </action>
  <verify>
    <automated>cd /c/Users/Bobbo/OneDrive/Desktop/Python/gridflow && uv run pytest tests/unit/test_entsoe.py -x -q 2>&1 | tail -5</automated>
  </verify>
  <acceptance_criteria>
    - `uv run pytest tests/unit/test_entsoe.py -x -q` passes with 0 failures
    - `grep "price_gbp_mwh" tests/unit/test_entsoe.py` returns zero matches (old column gone from test assertions)
    - `grep "flow_direction.*A01\|flow_direction.*A02" tests/unit/test_entsoe.py` returns zero matches (raw codes gone from assertions)
    - `grep "direction.*long\|direction.*short" tests/unit/test_entsoe.py` returns matches in both imbalance transformer test classes
    - `grep "ingested_at" tests/unit/test_entsoe.py` returns at least two matches (one per transformer)
  </acceptance_criteria>
  <done>All tests pass; imbalance_prices and imbalance_volume test classes assert direction strings and updated column names.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| XML bronze → transformer | Untrusted XML bytes are parsed before reaching transform(); parser already validated in prior phases |
| replace_strict mapping | Unknown codes (not in mapping dict) will raise Polars InvalidOperationError — acceptable, surfaces data quality issues |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-G3-01 | Tampering | replace_strict code mapping | accept | Unknown A-codes raise at transform time; silver stays empty rather than emitting garbage |
| T-G3-02 | Information Disclosure | ingested_at timestamp | accept | No PII; timestamp is audit metadata only |
</threat_model>

<verification>
```bash
cd /c/Users/Bobbo/OneDrive/Desktop/Python/gridflow

# Schema changes
grep -n "price_eur_mwh\|direction\|ingested_at" src/gridflow/schemas/entsoe.py

# No stale field names
! grep "price_gbp_mwh" src/gridflow/schemas/entsoe.py
! grep "flow_direction" src/gridflow/schemas/entsoe.py

# Transformer code mappings
grep "replace_strict" src/gridflow/silver/entsoe/imbalance_prices.py
grep "replace_strict" src/gridflow/silver/entsoe/imbalance_volume.py

# Full test suite
uv run pytest tests/unit/test_entsoe.py -x -q
```
</verification>

<success_criteria>
- `uv run pytest tests/unit/test_entsoe.py -x -q` passes with 0 failures
- `grep "price_gbp_mwh" src/gridflow/schemas/entsoe.py` → 0 matches in EntsoeImbalancePrices
- `grep "flow_direction" src/gridflow/schemas/entsoe.py` → 0 matches in EntsoeImbalanceVolume
- `grep "replace_strict" src/gridflow/silver/entsoe/imbalance_prices.py` → match with "A19"/"A20"
- `grep "replace_strict" src/gridflow/silver/entsoe/imbalance_volume.py` → match with "A01"/"A02"
- `grep "ingested_at" src/gridflow/silver/entsoe/imbalance_prices.py` → match
- `grep "ingested_at" src/gridflow/silver/entsoe/imbalance_volume.py` → match
</success_criteria>

<output>
After completion, create `.planning/phases/G3-balancing-code-mapping/G3-01-SUMMARY.md`
using the template at `$HOME/.claude/get-shit-done/templates/summary.md`.
</output>
