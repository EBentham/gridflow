---
phase: G3-balancing-code-mapping
plan: 02
type: execute
wave: 2
depends_on:
  - G3-01
files_modified:
  - src/gridflow/schemas/entsoe.py
  - src/gridflow/silver/entsoe/activated_balancing_qty.py
  - src/gridflow/silver/entsoe/activated_balancing_prices.py
  - src/gridflow/silver/entsoe/contracted_reserves.py
  - tests/fixtures/entsoe/activated_balancing_qty_gb.xml
  - tests/fixtures/entsoe/activated_balancing_prices_gb.xml
  - tests/unit/test_entsoe.py
autonomous: true
requirements:
  - GAP-06
  - GAP-07

must_haves:
  truths:
    - "EntsoeActivatedBalancingQty schema has reserve_type: str and direction: str (no business_type)"
    - "EntsoeActivatedBalancingPrices schema has reserve_type, direction, price_eur_mwh (no business_type, no price_gbp_mwh)"
    - "EntsoeContractedReserves schema has reserve_type: str (no business_type)"
    - "ActivatedBalancingQtyTransformer maps businessType A95→'fcr', A96→'afrr', A97→'mfrr', A98→'rr' via replace_strict"
    - "ActivatedBalancingQtyTransformer maps flowDirection A01→'up', A02→'down' via replace_strict"
    - "ActivatedBalancingPricesTransformer applies same reserve_type + direction mappings; emits price_eur_mwh"
    - "ContractedReservesTransformer maps businessType A95→'fcr', A96→'afrr', A97→'mfrr', A98→'rr' via replace_strict"
    - "All three transformers emit ingested_at"
    - "activated_balancing_qty_gb.xml and activated_balancing_prices_gb.xml fixtures updated to include <flowDirection.direction> elements (A01 and A02) enabling direction-column testing"
    - "All tests pass under uv run pytest tests/unit/test_entsoe.py -x -q"
  artifacts:
    - path: "src/gridflow/schemas/entsoe.py"
      provides: "Updated EntsoeActivatedBalancingQty, EntsoeActivatedBalancingPrices, EntsoeContractedReserves schemas"
      contains: "reserve_type"
    - path: "src/gridflow/silver/entsoe/activated_balancing_qty.py"
      provides: "Transformer with reserve_type + direction mapping"
      contains: "replace_strict"
    - path: "src/gridflow/silver/entsoe/activated_balancing_prices.py"
      provides: "Transformer with reserve_type + direction mapping and price_eur_mwh"
      contains: "replace_strict"
    - path: "src/gridflow/silver/entsoe/contracted_reserves.py"
      provides: "Transformer with reserve_type mapping"
      contains: "replace_strict"
    - path: "tests/fixtures/entsoe/activated_balancing_qty_gb.xml"
      provides: "Fixture with flowDirection.direction elements"
      contains: "flowDirection.direction"
    - path: "tests/fixtures/entsoe/activated_balancing_prices_gb.xml"
      provides: "Fixture with flowDirection.direction elements"
      contains: "flowDirection.direction"
  key_links:
    - from: "src/gridflow/silver/entsoe/activated_balancing_qty.py"
      to: "src/gridflow/schemas/entsoe.py"
      via: "EntsoeActivatedBalancingQty(**sample) contract validation"
      pattern: "EntsoeActivatedBalancingQty\\(\\*\\*sample\\)"
    - from: "src/gridflow/silver/entsoe/activated_balancing_prices.py"
      to: "src/gridflow/schemas/entsoe.py"
      via: "EntsoeActivatedBalancingPrices(**sample) contract validation"
      pattern: "EntsoeActivatedBalancingPrices\\(\\*\\*sample\\)"
    - from: "src/gridflow/silver/entsoe/contracted_reserves.py"
      to: "src/gridflow/schemas/entsoe.py"
      via: "EntsoeContractedReserves(**sample) contract validation"
      pattern: "EntsoeContractedReserves\\(\\*\\*sample\\)"
---

<objective>
Update the three remaining Phase 3 dataset schemas and transformers to emit
human-readable reserve_type and direction strings instead of raw A-codes, fix the
currency field name for balancing prices, add ingested_at, and update fixtures and
tests to exercise the new direction column.

Purpose: Closes GAP-06 and GAP-07 for activated_balancing_qty, activated_balancing_prices,
and contracted_reserves. The activated_balancing fixtures currently lack
flowDirection elements — they are updated here so tests can assert "up"/"down" direction values.

Output: Three updated schemas, three updated transformers, two updated fixtures,
updated test assertions, all tests green.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@/c/Users/Bobbo/OneDrive/Desktop/Python/gridflow/.planning/ROADMAP.md
@/c/Users/Bobbo/OneDrive/Desktop/Python/gridflow/CLAUDE.md
@/c/Users/Bobbo/OneDrive/Desktop/Python/gridflow/.planning/phases/G3-balancing-code-mapping/G3-01-SUMMARY.md

<interfaces>
<!-- Current schema state (to be changed) -->

From src/gridflow/schemas/entsoe.py — EntsoeActivatedBalancingQty (current):
```python
class EntsoeActivatedBalancingQty(BaseSchema):
    timestamp_utc: datetime
    area_code: str
    business_type: str  # A95=upward, A96=downward   ← REMOVE
    quantity_mwh: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    # no ingested_at                                  ← ADD
```

From src/gridflow/schemas/entsoe.py — EntsoeActivatedBalancingPrices (current):
```python
class EntsoeActivatedBalancingPrices(BaseSchema):
    timestamp_utc: datetime
    area_code: str
    business_type: str  # A95=upward, A96=downward   ← REMOVE
    price_gbp_mwh: float                              ← RENAME to price_eur_mwh
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    # no ingested_at                                  ← ADD
```

From src/gridflow/schemas/entsoe.py — EntsoeContractedReserves (current):
```python
class EntsoeContractedReserves(BaseSchema):
    timestamp_utc: datetime
    area_code: str
    business_type: str                               ← REMOVE
    quantity_mw: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    # no ingested_at                                  ← ADD
```

Target schema shapes (from spec docs/specs/entsoe-connector-extension.md Phase 3c/3d/3e):
```python
class EntsoeActivatedBalancingQty(BaseSchema):
    timestamp_utc: datetime
    area_code: str
    reserve_type: str   # "fcr"(A95) | "afrr"(A96) | "mfrr"(A97) | "rr"(A98)
    direction: str      # "up"(A01) | "down"(A02)
    quantity_mwh: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

class EntsoeActivatedBalancingPrices(BaseSchema):
    timestamp_utc: datetime
    area_code: str
    reserve_type: str
    direction: str
    price_eur_mwh: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None

class EntsoeContractedReserves(BaseSchema):
    timestamp_utc: datetime
    area_code: str
    reserve_type: str   # "fcr"(A95) | "afrr"(A96) | "mfrr"(A97) | "rr"(A98)
    quantity_mw: float
    resolution: str = ""
    data_provider: str = Field(default="entsoe")
    ingested_at: datetime | None = None
```

Current transformer pattern (from activated_balancing_qty.py):
```python
df = (
    raw_df.rename({"value": "quantity_mwh", "control_area_domain": "area_code"})
    .select(["timestamp_utc", "area_code", "business_type", "quantity_mwh", "resolution"])
    .unique(subset=["timestamp_utc", "area_code", "business_type"], keep="last")
    .sort(["timestamp_utc", "area_code", "business_type"])
    .with_columns([
        pl.lit("entsoe").alias("data_provider"),
        pl.col("timestamp_utc").dt.replace_time_zone("UTC"),
    ])
)
```

Polars replace_strict API:
```python
pl.col("business_type").replace_strict(
    {"A95": "fcr", "A96": "afrr", "A97": "mfrr", "A98": "rr"}
).alias("reserve_type")

pl.col("flow_direction").replace_strict(
    {"A01": "up", "A02": "down"}
).alias("direction")
```

ingested_at pattern (from G3-01 and actual_load.py):
```python
now = datetime.now(UTC)
# ... in with_columns:
pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
```

Fixture structure — current activated_balancing_qty_gb.xml has:
```xml
<TimeSeries>
  <businessType>A95</businessType>
  <controlArea_Domain.mRID ...>10YGB----------A</controlArea_Domain.mRID>
  <!-- NO flowDirection.direction element currently -->
</TimeSeries>
```

Required fixture structure (add flowDirection after businessType):
```xml
<TimeSeries>
  <businessType>A95</businessType>
  <flowDirection.direction>A01</flowDirection.direction>
  <controlArea_Domain.mRID ...>10YGB----------A</controlArea_Domain.mRID>
</TimeSeries>
<TimeSeries>
  <businessType>A96</businessType>
  <flowDirection.direction>A02</flowDirection.direction>
  <controlArea_Domain.mRID ...>10YGB----------A</controlArea_Domain.mRID>
</TimeSeries>
```
</interfaces>
</context>

<tasks>

<task type="auto">
  <name>Task 1: Update three schemas in entsoe.py</name>
  <read_first>
    src/gridflow/schemas/entsoe.py
  </read_first>
  <files>src/gridflow/schemas/entsoe.py</files>
  <action>
In src/gridflow/schemas/entsoe.py, update three schema classes. G3-01 already
updated EntsoeImbalancePrices and EntsoeImbalanceVolume — do NOT touch those.

**EntsoeActivatedBalancingQty** (currently after EntsoeImbalanceVolume):
- Remove `business_type: str`
- Add `reserve_type: str  # "fcr"(A95) | "afrr"(A96) | "mfrr"(A97) | "rr"(A98)`
- Add `direction: str  # "up"(A01) | "down"(A02)`
- Add `ingested_at: datetime | None = None`
- Update docstring

New class body:
```python
class EntsoeActivatedBalancingQty(BaseSchema):
    """Silver-layer schema for ENTSO-E activated balancing energy quantity (A83/A16).

    reserve_type: "fcr"(A95) | "afrr"(A96) | "mfrr"(A97) | "rr"(A98).
    direction: "up"(A01=upward activation) | "down"(A02=downward activation).
    quantity_mwh: Activated quantity in MWh.
    """

    timestamp_utc: datetime
    area_code: str
    reserve_type: str  # "fcr" | "afrr" | "mfrr" | "rr"
    direction: str     # "up" | "down"
    quantity_mwh: float
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

**EntsoeActivatedBalancingPrices** (after EntsoeActivatedBalancingQty):
- Remove `business_type: str`
- Remove `price_gbp_mwh: float`
- Add `reserve_type: str  # "fcr"(A95) | "afrr"(A96) | "mfrr"(A97) | "rr"(A98)`
- Add `direction: str  # "up"(A01) | "down"(A02)`
- Add `price_eur_mwh: float`
- Add `ingested_at: datetime | None = None`
- Update docstring

New class body:
```python
class EntsoeActivatedBalancingPrices(BaseSchema):
    """Silver-layer schema for ENTSO-E activated balancing energy prices (A84/A16).

    reserve_type: "fcr"(A95) | "afrr"(A96) | "mfrr"(A97) | "rr"(A98).
    direction: "up"(A01) | "down"(A02).
    price_eur_mwh: Activation price in EUR/MWh.
    """

    timestamp_utc: datetime
    area_code: str
    reserve_type: str  # "fcr" | "afrr" | "mfrr" | "rr"
    direction: str     # "up" | "down"
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

**EntsoeContractedReserves** (after EntsoeActivatedBalancingPrices):
- Remove `business_type: str`
- Add `reserve_type: str  # "fcr"(A95) | "afrr"(A96) | "mfrr"(A97) | "rr"(A98)`
- Add `ingested_at: datetime | None = None`
- Update docstring

New class body:
```python
class EntsoeContractedReserves(BaseSchema):
    """Silver-layer schema for ENTSO-E contracted reserves (A81).

    reserve_type: "fcr"(A95) | "afrr"(A96) | "mfrr"(A97) | "rr"(A98).
    quantity_mw: Contracted reserve quantity in MW.
    """

    timestamp_utc: datetime
    area_code: str
    reserve_type: str  # "fcr" | "afrr" | "mfrr" | "rr"
    quantity_mw: float
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
  </action>
  <verify>
    <automated>cd /c/Users/Bobbo/OneDrive/Desktop/Python/gridflow && python -c "
from gridflow.schemas.entsoe import (
    EntsoeActivatedBalancingQty, EntsoeActivatedBalancingPrices, EntsoeContractedReserves
)
from datetime import datetime, UTC
ts = datetime(2024, 1, 15, tzinfo=UTC)
q = EntsoeActivatedBalancingQty(timestamp_utc=ts, area_code='10YGB----------A', reserve_type='fcr', direction='up', quantity_mwh=320.0)
p = EntsoeActivatedBalancingPrices(timestamp_utc=ts, area_code='10YGB----------A', reserve_type='fcr', direction='up', price_eur_mwh=110.0)
c = EntsoeContractedReserves(timestamp_utc=ts, area_code='10YGB----------A', reserve_type='afrr', quantity_mw=500.0)
assert q.reserve_type == 'fcr'
assert p.price_eur_mwh == 110.0
assert c.reserve_type == 'afrr'
print('Schema OK')
"
</automated>
  </verify>
  <acceptance_criteria>
    - Python check above prints "Schema OK"
    - `grep "business_type" src/gridflow/schemas/entsoe.py` returns zero matches in the three updated classes (only comments allowed if present)
    - `grep "price_gbp_mwh" src/gridflow/schemas/entsoe.py` returns zero matches in EntsoeActivatedBalancingPrices
    - `grep "reserve_type" src/gridflow/schemas/entsoe.py` returns at least three matches (one per class)
    - `grep "ingested_at" src/gridflow/schemas/entsoe.py` returns at least 5 matches total (2 from G3-01 + 3 new)
  </acceptance_criteria>
  <done>Three schema classes updated with reserve_type (and direction for balancing datasets), ingested_at added, stale fields removed.</done>
</task>

<task type="auto">
  <name>Task 2: Update fixtures and transformers for activated_balancing and contracted_reserves</name>
  <read_first>
    tests/fixtures/entsoe/activated_balancing_qty_gb.xml
    tests/fixtures/entsoe/activated_balancing_prices_gb.xml
    tests/fixtures/entsoe/contracted_reserves_gb.xml
    src/gridflow/silver/entsoe/activated_balancing_qty.py
    src/gridflow/silver/entsoe/activated_balancing_prices.py
    src/gridflow/silver/entsoe/contracted_reserves.py
    src/gridflow/silver/entsoe/actual_load.py
  </read_first>
  <files>
    tests/fixtures/entsoe/activated_balancing_qty_gb.xml
    tests/fixtures/entsoe/activated_balancing_prices_gb.xml
    src/gridflow/silver/entsoe/activated_balancing_qty.py
    src/gridflow/silver/entsoe/activated_balancing_prices.py
    src/gridflow/silver/entsoe/contracted_reserves.py
  </files>
  <action>
**Step A — Update fixtures (must be done before transformers are tested):**

**tests/fixtures/entsoe/activated_balancing_qty_gb.xml** — add `<flowDirection.direction>` element to each TimeSeries, immediately after `<businessType>`:
- TimeSeries 1 (businessType A95): add `<flowDirection.direction>A01</flowDirection.direction>`
- TimeSeries 2 (businessType A96): add `<flowDirection.direction>A02</flowDirection.direction>`

The fixture has 2 TimeSeries × 2 points = 4 records. Record count stays at 4.

**tests/fixtures/entsoe/activated_balancing_prices_gb.xml** — same change:
- TimeSeries 1 (businessType A95): add `<flowDirection.direction>A01</flowDirection.direction>`
- TimeSeries 2 (businessType A96): add `<flowDirection.direction>A02</flowDirection.direction>`

Do NOT change contracted_reserves_gb.xml — contracted reserves have no direction column in the spec.

**Step B — Update activated_balancing_qty.py transformer:**

1. Add `from datetime import UTC` and `datetime` to imports.
2. Keep `required` guard — `"business_type"` and `"flow_direction"` are still needed from parser.
   Update required set: `{"timestamp_utc", "value", "control_area_domain", "business_type", "flow_direction", "resolution"}`.
   Note: `"flow_direction"` is now required because the updated fixture provides it and the direction mapping depends on it.
3. After rename `{"value": "quantity_mwh", "control_area_domain": "area_code"}`, add both mappings:
   ```python
   .with_columns([
       pl.col("business_type").replace_strict(
           {"A95": "fcr", "A96": "afrr", "A97": "mfrr", "A98": "rr"}
       ).alias("reserve_type"),
       pl.col("flow_direction").replace_strict(
           {"A01": "up", "A02": "down"}
       ).alias("direction"),
   ])
   ```
4. Update `.select()`: `["timestamp_utc", "area_code", "reserve_type", "direction", "quantity_mwh", "resolution"]`
5. Update `.unique(subset=...)`: `["timestamp_utc", "area_code", "reserve_type", "direction"]`
6. Update `.sort(...)`: `["timestamp_utc", "area_code", "reserve_type", "direction"]`
7. Add ingested_at using the standard pattern:
   ```python
   now = datetime.now(UTC)
   # ... set now BEFORE the DataFrame chain, then:
   .with_columns([
       pl.lit("entsoe").alias("data_provider"),
       pl.lit(now).cast(pl.Datetime("us", "UTC")).alias("ingested_at"),
       pl.col("timestamp_utc").dt.replace_time_zone("UTC"),
   ])
   ```
8. Add final output_cols select:
   ```python
   output_cols = [
       "timestamp_utc", "area_code", "reserve_type", "direction",
       "quantity_mwh", "resolution", "data_provider", "ingested_at",
   ]
   available = [c for c in output_cols if c in df.columns]
   df = df.select(available)
   ```
9. Update class docstring and contract validation call (schema name stays `EntsoeActivatedBalancingQty`).

**Step C — Update activated_balancing_prices.py transformer:**

Same pattern as activated_balancing_qty.py but:
- Rename `"value"` → `"price_eur_mwh"` (was `"price_gbp_mwh"`)
- Same reserve_type and direction mappings from business_type and flow_direction
- output_cols: `["timestamp_utc", "area_code", "reserve_type", "direction", "price_eur_mwh", "resolution", "data_provider", "ingested_at"]`
- Required set: `{"timestamp_utc", "value", "control_area_domain", "business_type", "flow_direction", "resolution"}`
- Unique subset and sort: `["timestamp_utc", "area_code", "reserve_type", "direction"]`

**Step D — Update contracted_reserves.py transformer:**

contracted_reserves has no direction column (only reserve_type):
1. Add `from datetime import UTC` and `datetime`.
2. Keep `required = {"timestamp_utc", "value", "control_area_domain", "business_type", "resolution"}` (no flow_direction needed).
3. After rename `{"value": "quantity_mw", "control_area_domain": "area_code"}`, add reserve_type mapping:
   ```python
   .with_columns(
       pl.col("business_type").replace_strict(
           {"A95": "fcr", "A96": "afrr", "A97": "mfrr", "A98": "rr"}
       ).alias("reserve_type")
   )
   ```
4. Update `.select()`: `["timestamp_utc", "area_code", "reserve_type", "quantity_mw", "resolution"]`
5. Update `.unique(subset=...)`: `["timestamp_utc", "area_code", "reserve_type"]`
6. Update `.sort(...)`: `["timestamp_utc", "area_code", "reserve_type"]`
7. Add ingested_at using standard pattern.
8. output_cols: `["timestamp_utc", "area_code", "reserve_type", "quantity_mw", "resolution", "data_provider", "ingested_at"]`
  </action>
  <verify>
    <automated>cd /c/Users/Bobbo/OneDrive/Desktop/Python/gridflow && python -c "
from tests.unit.test_entsoe import _make_entsoe_transformer, _make_df_from_xml
from gridflow.silver.entsoe.activated_balancing_qty import ActivatedBalancingQtyTransformer
from gridflow.silver.entsoe.activated_balancing_prices import ActivatedBalancingPricesTransformer
from gridflow.silver.entsoe.contracted_reserves import ContractedReservesTransformer

tq = _make_entsoe_transformer(ActivatedBalancingQtyTransformer)
rq = tq.transform(_make_df_from_xml('activated_balancing_qty_gb.xml', 'quantity'))
assert 'reserve_type' in rq.columns, f'Missing reserve_type: {rq.columns}'
assert 'direction' in rq.columns, f'Missing direction: {rq.columns}'
assert set(rq['reserve_type'].to_list()).issubset({'fcr','afrr','mfrr','rr'}), f'Bad reserve_type: {set(rq[\"reserve_type\"].to_list())}'
assert set(rq['direction'].to_list()).issubset({'up','down'}), f'Bad direction: {set(rq[\"direction\"].to_list())}'
assert 'ingested_at' in rq.columns

tp = _make_entsoe_transformer(ActivatedBalancingPricesTransformer)
rp = tp.transform(_make_df_from_xml('activated_balancing_prices_gb.xml', 'price.amount'))
assert 'price_eur_mwh' in rp.columns, f'Missing price_eur_mwh: {rp.columns}'
assert 'reserve_type' in rp.columns
assert 'direction' in rp.columns

tc = _make_entsoe_transformer(ContractedReservesTransformer)
rc = tc.transform(_make_df_from_xml('contracted_reserves_gb.xml', 'quantity'))
assert 'reserve_type' in rc.columns, f'Missing reserve_type: {rc.columns}'
assert 'direction' not in rc.columns, 'contracted_reserves should NOT have direction'
assert 'ingested_at' in rc.columns

print('ALL OK')
"
</automated>
  </verify>
  <acceptance_criteria>
    - Python check above prints "ALL OK"
    - `grep "flowDirection.direction" tests/fixtures/entsoe/activated_balancing_qty_gb.xml` returns two matches (A01, A02)
    - `grep "flowDirection.direction" tests/fixtures/entsoe/activated_balancing_prices_gb.xml` returns two matches (A01, A02)
    - `grep "price_gbp_mwh" src/gridflow/silver/entsoe/activated_balancing_prices.py` returns zero matches
    - `grep "replace_strict" src/gridflow/silver/entsoe/activated_balancing_qty.py` returns two matches (reserve_type + direction)
    - `grep "replace_strict" src/gridflow/silver/entsoe/activated_balancing_prices.py` returns two matches
    - `grep "replace_strict" src/gridflow/silver/entsoe/contracted_reserves.py` returns one match (reserve_type only)
    - `grep "business_type" src/gridflow/silver/entsoe/contracted_reserves.py` matches ONLY the required-guard and replace_strict source expression (not in select/unique/sort)
  </acceptance_criteria>
  <done>Fixtures updated with flowDirection elements; all three transformers produce reserve_type/direction/ingested_at; price_eur_mwh emitted by balancing_prices.</done>
</task>

<task type="auto">
  <name>Task 3: Update tests for activated_balancing and contracted_reserves</name>
  <read_first>
    tests/unit/test_entsoe.py
  </read_first>
  <files>tests/unit/test_entsoe.py</files>
  <action>
Roadmap tasks 7 and 8 explicitly authorize updating Phase 3 transformer tests.
Update only the three Phase 3 test classes below — do NOT touch any other test class.

**TestActivatedBalancingQtyTransformer** (find class around line 1275):

Replace `test_transform_basic`:
```python
def test_transform_basic(self):
    raw = _make_df_from_xml("activated_balancing_qty_gb.xml", "quantity")
    result = self.t.transform(raw)
    assert not result.is_empty()
    assert "quantity_mwh" in result.columns
    assert "reserve_type" in result.columns   # was business_type
    assert "direction" in result.columns      # new column
```

Replace `test_business_types_preserved` → rename to `test_reserve_type_values`:
```python
def test_reserve_type_values(self):
    raw = _make_df_from_xml("activated_balancing_qty_gb.xml", "quantity")
    result = self.t.transform(raw)
    rtypes = set(result["reserve_type"].to_list())
    assert "fcr" in rtypes     # was A95
    assert "afrr" in rtypes    # was A96
```

Add new test for direction:
```python
def test_direction_values(self):
    raw = _make_df_from_xml("activated_balancing_qty_gb.xml", "quantity")
    result = self.t.transform(raw)
    dirs = set(result["direction"].to_list())
    assert "up" in dirs     # A01
    assert "down" in dirs   # A02
```

Replace `test_upward_qty_values` — filter column changed from `business_type` to `reserve_type` and `direction`:
```python
def test_fcr_up_qty_values(self):
    raw = _make_df_from_xml("activated_balancing_qty_gb.xml", "quantity")
    result = self.t.transform(raw).filter(
        (pl.col("reserve_type") == "fcr") & (pl.col("direction") == "up")
    ).sort("timestamp_utc")
    assert abs(result["quantity_mwh"][0] - 320) < 0.1
```

Add ingested_at test:
```python
def test_ingested_at_present(self):
    raw = _make_df_from_xml("activated_balancing_qty_gb.xml", "quantity")
    result = self.t.transform(raw)
    assert "ingested_at" in result.columns
```

**TestActivatedBalancingPricesTransformer** (find class around line 1319):

Replace `test_transform_basic`:
```python
def test_transform_basic(self):
    raw = _make_df_from_xml("activated_balancing_prices_gb.xml", "price.amount")
    result = self.t.transform(raw)
    assert not result.is_empty()
    assert "price_eur_mwh" in result.columns   # was price_gbp_mwh
    assert "reserve_type" in result.columns    # was business_type
    assert "direction" in result.columns       # new column
```

Replace `test_business_types_preserved` → rename to `test_reserve_type_values`:
```python
def test_reserve_type_values(self):
    raw = _make_df_from_xml("activated_balancing_prices_gb.xml", "price.amount")
    result = self.t.transform(raw)
    rtypes = set(result["reserve_type"].to_list())
    assert "fcr" in rtypes
    assert "afrr" in rtypes
```

Add direction test:
```python
def test_direction_values(self):
    raw = _make_df_from_xml("activated_balancing_prices_gb.xml", "price.amount")
    result = self.t.transform(raw)
    dirs = set(result["direction"].to_list())
    assert "up" in dirs
    assert "down" in dirs
```

Replace `test_upward_price_values`:
```python
def test_fcr_up_price_values(self):
    raw = _make_df_from_xml("activated_balancing_prices_gb.xml", "price.amount")
    result = self.t.transform(raw).filter(
        (pl.col("reserve_type") == "fcr") & (pl.col("direction") == "up")
    ).sort("timestamp_utc")
    assert abs(result["price_eur_mwh"][0] - 110.00) < 0.01   # was price_gbp_mwh
```

Add ingested_at test:
```python
def test_ingested_at_present(self):
    raw = _make_df_from_xml("activated_balancing_prices_gb.xml", "price.amount")
    result = self.t.transform(raw)
    assert "ingested_at" in result.columns
```

**TestContractedReservesTransformer** (find class around line 1363):

Replace `test_transform_basic`:
```python
def test_transform_basic(self):
    raw = _make_df_from_xml("contracted_reserves_gb.xml", "quantity")
    result = self.t.transform(raw)
    assert not result.is_empty()
    assert "quantity_mw" in result.columns
    assert "reserve_type" in result.columns   # was business_type
```

Replace `test_business_types_preserved` → rename to `test_reserve_type_values`:
```python
def test_reserve_type_values(self):
    raw = _make_df_from_xml("contracted_reserves_gb.xml", "quantity")
    result = self.t.transform(raw)
    rtypes = set(result["reserve_type"].to_list())
    assert "fcr" in rtypes     # was A95
    assert "afrr" in rtypes    # was A96
```

Replace `test_quantity_values` — filter column changed:
```python
def test_quantity_values(self):
    raw = _make_df_from_xml("contracted_reserves_gb.xml", "quantity")
    result = self.t.transform(raw).filter(
        pl.col("reserve_type") == "fcr"    # was business_type == "A95"
    ).sort("timestamp_utc")
    assert abs(result["quantity_mw"][0] - 500) < 0.1
```

Add ingested_at test:
```python
def test_ingested_at_present(self):
    raw = _make_df_from_xml("contracted_reserves_gb.xml", "quantity")
    result = self.t.transform(raw)
    assert "ingested_at" in result.columns
```

**Schema validation test classes — update three:**

**TestEntsoeActivatedBalancingQtySchema** (around line 1462):
```python
def test_valid_record(self):
    r = EntsoeActivatedBalancingQty(
        timestamp_utc=self._TS,
        area_code="10YGB----------A",
        reserve_type="fcr",   # was business_type="A95"
        direction="up",       # new required field
        quantity_mwh=320.0,
    )
    assert r.data_provider == "entsoe"
    assert r.quantity_mwh == 320.0
    assert r.reserve_type == "fcr"

def test_naive_timestamp_rejected(self):
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        EntsoeActivatedBalancingQty(
            timestamp_utc=datetime(2024, 1, 15),
            area_code="10YGB----------A",
            reserve_type="fcr",
            direction="up",
            quantity_mwh=320.0,
        )
```

**TestEntsoeActivatedBalancingPricesSchema** (around line 1486):
```python
def test_valid_record(self):
    r = EntsoeActivatedBalancingPrices(
        timestamp_utc=self._TS,
        area_code="10YGB----------A",
        reserve_type="fcr",    # was business_type="A95"
        direction="up",        # new required field
        price_eur_mwh=110.0,  # was price_gbp_mwh
    )
    assert r.data_provider == "entsoe"
    assert r.price_eur_mwh == 110.0    # was price_gbp_mwh
    assert r.reserve_type == "fcr"

def test_naive_timestamp_rejected(self):
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        EntsoeActivatedBalancingPrices(
            timestamp_utc=datetime(2024, 1, 15),
            area_code="10YGB----------A",
            reserve_type="fcr",
            direction="up",
            price_eur_mwh=110.0,
        )
```

**TestEntsoeContractedReservesSchema** (around line 1510):
```python
def test_valid_record(self):
    r = EntsoeContractedReserves(
        timestamp_utc=self._TS,
        area_code="10YGB----------A",
        reserve_type="fcr",   # was business_type="A95"
        quantity_mw=500.0,
    )
    assert r.data_provider == "entsoe"
    assert r.quantity_mw == 500.0
    assert r.reserve_type == "fcr"

def test_naive_timestamp_rejected(self):
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        EntsoeContractedReserves(
            timestamp_utc=datetime(2024, 1, 15),
            area_code="10YGB----------A",
            reserve_type="fcr",
            quantity_mw=500.0,
        )
```

Run the full test suite after all edits:
```bash
uv run pytest tests/unit/test_entsoe.py -x -q
```
  </action>
  <verify>
    <automated>cd /c/Users/Bobbo/OneDrive/Desktop/Python/gridflow && uv run pytest tests/unit/test_entsoe.py -x -q 2>&1 | tail -5</automated>
  </verify>
  <acceptance_criteria>
    - `uv run pytest tests/unit/test_entsoe.py -x -q` passes with 0 failures
    - `grep "price_gbp_mwh" tests/unit/test_entsoe.py` returns zero matches
    - `grep "business_type.*A95\|business_type.*A96" tests/unit/test_entsoe.py` returns zero matches (raw codes gone from test assertions)
    - `grep "reserve_type.*fcr\|reserve_type.*afrr" tests/unit/test_entsoe.py` returns matches in at least three test classes
    - `grep "direction.*up\|direction.*down" tests/unit/test_entsoe.py` returns matches in both activated_balancing test classes
    - `grep "ingested_at" tests/unit/test_entsoe.py` returns at least 5 matches total (2 from G3-01 + 3 new)
  </acceptance_criteria>
  <done>All tests pass; three test classes assert reserve_type/direction strings, updated column names, and ingested_at presence.</done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| XML bronze → parser → transformer | Parser already validated; transformer receives DataFrame with string codes |
| replace_strict on business_type/flow_direction | Unknown codes (e.g., A97, A98 not in fixture) raise Polars error — this is the correct behaviour, surfaces data quality issues early |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-G3-03 | Tampering | replace_strict on reserve_type | accept | Unknown reserve type codes (e.g., future A99) raise at transform time; silver not written, alerting the operator |
| T-G3-04 | Tampering | replace_strict on direction | accept | Same rationale — unknown flow direction codes surface as errors, not silent data corruption |
| T-G3-05 | Information Disclosure | ingested_at | accept | No PII; audit timestamp only |
</threat_model>

<verification>
```bash
cd /c/Users/Bobbo/OneDrive/Desktop/Python/gridflow

# Schema changes
python -c "
from gridflow.schemas.entsoe import EntsoeActivatedBalancingQty, EntsoeActivatedBalancingPrices, EntsoeContractedReserves
from datetime import datetime, UTC
ts = datetime(2024, 1, 15, tzinfo=UTC)
q = EntsoeActivatedBalancingQty(timestamp_utc=ts, area_code='10YGB----------A', reserve_type='fcr', direction='up', quantity_mwh=320.0)
p = EntsoeActivatedBalancingPrices(timestamp_utc=ts, area_code='10YGB----------A', reserve_type='fcr', direction='up', price_eur_mwh=110.0)
c = EntsoeContractedReserves(timestamp_utc=ts, area_code='10YGB----------A', reserve_type='afrr', quantity_mw=500.0)
print('Schemas OK:', q.reserve_type, p.price_eur_mwh, c.reserve_type)
"

# No stale field names in schemas
! grep "business_type\|price_gbp_mwh" src/gridflow/schemas/entsoe.py | grep -v "^#\|#.*business_type"

# All tests green
uv run pytest tests/unit/test_entsoe.py -x -q

# Ruff clean
uv run ruff check src/gridflow/silver/entsoe/activated_balancing_qty.py src/gridflow/silver/entsoe/activated_balancing_prices.py src/gridflow/silver/entsoe/contracted_reserves.py src/gridflow/schemas/entsoe.py
```
</verification>

<success_criteria>
- `uv run pytest tests/unit/test_entsoe.py -x -q` passes with 0 failures
- `grep "reserve_type" src/gridflow/schemas/entsoe.py` returns matches for all three updated classes
- `grep "price_gbp_mwh" src/gridflow/schemas/entsoe.py` returns zero matches (all removed)
- `grep "business_type" src/gridflow/schemas/entsoe.py` returns zero matches in the three updated class bodies
- `grep "flowDirection.direction" tests/fixtures/entsoe/activated_balancing_qty_gb.xml` returns two matches
- `grep "flowDirection.direction" tests/fixtures/entsoe/activated_balancing_prices_gb.xml` returns two matches
- `uv run ruff check src/gridflow/silver/entsoe/` passes with 0 errors
</success_criteria>

<output>
After completion, create `.planning/phases/G3-balancing-code-mapping/G3-02-SUMMARY.md`
using the template at `$HOME/.claude/get-shit-done/templates/summary.md`.
</output>
