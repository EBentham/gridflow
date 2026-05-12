---
phase: G4-outages-unit-schema
plan: 01
status: complete
commit: 273a3c0
depends_on: []
---

# G4-01 Summary — outages_generation unit-level schema

## What was done

Replaced the zone-aggregate `outages_generation` silver schema and transformer
with a unit-level shape, closing GAP-04. Each silver row now represents one
generation unit's unavailable MW for one timestamp, identified by
`unit_mrid` (RegisteredResource.mRID) with `outage_type` mapped from ENTSO-E
businessType A-codes.

## Changes

### src/gridflow/connectors/entsoe/parsers.py
- Added `unit_mrid = unit_name = ""` to per-TimeSeries variable initialisation
- Added `elif tag == "RegisteredResource":` branch that extracts `mRID` and
  `name` sub-elements via `_strip_ns` — empty string defaults preserve
  backward-compatibility with non-A80 documents
- Added `"unit_mrid"` and `"unit_name"` keys to every `records.append({...})` call
- Updated docstring Returns section to list the new keys

### src/gridflow/schemas/entsoe.py
- `EntsoeOutagesGeneration`: removed `production_type: str = ""` and
  `available_capacity_mw: float`; added `unit_mrid: str`, `unit_name: str = ""`,
  `outage_type: str`, `unavailable_mw: float`, `ingested_at: datetime | None = None`
- Class docstring updated to describe unit-level row semantics

### src/gridflow/silver/entsoe/outages_generation.py
- `OutagesGenerationTransformer.transform()` rewritten:
  - Required-column guard now checks `unit_mrid` and `business_type`
  - Renames `value` → `unavailable_mw` (was `available_capacity_mw`)
  - Maps `business_type` A53→"planned", A54→"unplanned" via `replace_strict`
  - Dedup key: `(timestamp_utc, unit_mrid)` (was `(timestamp_utc, area_code, production_type)`)
  - `ingested_at` added using `now = datetime.now(UTC)` pattern
  - Output column order matches new schema
- Class docstring updated

### tests/fixtures/entsoe/outages_generation_gb.xml
- Replaced single-TimeSeries fixture (businessType A54, no RegisteredResource) with
  two-TimeSeries fixture:
  - TimeSeries 1: A53 (planned), `<RegisteredResource>` UNIT-DRAX-3 / "Drax Unit 3", quantity=800
  - TimeSeries 2: A54 (unplanned), `<RegisteredResource>` UNIT-HEYSHAM-2 / "Heysham 2", quantity=1200
  - 2 Points per TimeSeries → 4 silver rows total

### tests/unit/test_entsoe.py
- `TestOutagesGenerationTransformer`: replaced 6 old tests with 11 new tests asserting
  `unit_mrid_values`, `unit_name_values`, `outage_type_mapping`, `four_records`,
  `unavailable_mw_values`, `dedup_on_timestamp_unit`, `ingested_at_present`,
  `data_provider`; old `test_production_type_present` / `test_two_records` /
  `test_capacity_value` removed
- `TestEntsoeOutagesGenerationSchema`: replaced 3 old tests with `test_valid_record`
  (unit_mrid, outage_type, unavailable_mw), `test_unit_name_optional`, and
  `test_naive_timestamp_rejected` using updated required fields

## Verification

- `uv run python -c "...PARSER OK..."`: PARSER OK
- `python -m pytest tests/unit/test_entsoe.py -x -q`: 196 passed
- `python -m pytest -m "not live" -x -q`: 546 passed, 44 deselected
- `uv run ruff check src/gridflow/connectors/entsoe/parsers.py src/gridflow/silver/entsoe/outages_generation.py`: all checks passed
  (pre-existing TC003 in schemas/entsoe.py unchanged)

## Downstream impact

Gold views in `src/gridflow/gold/views/` do not reference `outages_generation` —
no downstream breakage.

## Requirements closed

GAP-04 (unit-level outages_generation schema) fully closed.
