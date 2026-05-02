---
phase: G3-balancing-code-mapping
plan: 02
status: complete
commit: 6912dd5
depends_on: G3-01 (2f4b4fd)
---

# G3-02 Summary — Balancing reserve/direction codes mapped to semantic strings

## What was done

Updated `EntsoeActivatedBalancingQty`, `EntsoeActivatedBalancingPrices`, and
`EntsoeContractedReserves` schemas, transformers, fixtures, and tests to emit
human-readable reserve_type and direction strings instead of raw ENTSO-E A-codes,
fix the currency field name for balancing prices, and add `ingested_at`.

## Changes

### src/gridflow/schemas/entsoe.py
- `EntsoeActivatedBalancingQty`: removed `business_type`; added `reserve_type: str`, `direction: str`, `ingested_at: datetime | None = None`
- `EntsoeActivatedBalancingPrices`: removed `business_type`, `price_gbp_mwh`; added `reserve_type: str`, `direction: str`, `price_eur_mwh: float`, `ingested_at: datetime | None = None`
- `EntsoeContractedReserves`: removed `business_type`; added `reserve_type: str`, `ingested_at: datetime | None = None`

### src/gridflow/silver/entsoe/activated_balancing_qty.py
- Maps `business_type` A95→"fcr", A96→"afrr", A97→"mfrr", A98→"rr" via `replace_strict`
- Maps `flow_direction` A01→"up", A02→"down" via `replace_strict`
- Dedup key: `(timestamp_utc, area_code, reserve_type, direction)`; `ingested_at` added

### src/gridflow/silver/entsoe/activated_balancing_prices.py
- Same mappings; renames `value` → `price_eur_mwh`; `ingested_at` added

### src/gridflow/silver/entsoe/contracted_reserves.py
- Maps `business_type` A95→"fcr" etc. via `replace_strict`; no direction column (spec has none)
- Dedup key: `(timestamp_utc, area_code, reserve_type)`; `ingested_at` added

### tests/fixtures/entsoe/
- `activated_balancing_qty_gb.xml`: added `<flowDirection.direction>A01/A02</flowDirection.direction>` per TimeSeries
- `activated_balancing_prices_gb.xml`: same fixture update

### tests/unit/test_entsoe.py
- `TestActivatedBalancingQtyTransformer`: `test_business_types_preserved` → `test_reserve_type_values`; added `test_direction_values`, `test_ingested_at_present`; renamed `test_upward_qty_values` → `test_fcr_up_qty_values`
- `TestActivatedBalancingPricesTransformer`: same pattern; `test_upward_price_values` → `test_fcr_up_price_values`; `price_eur_mwh` throughout
- `TestContractedReservesTransformer`: updated to `reserve_type`; added `test_ingested_at_present`
- Three schema test classes updated to use new field names

## Verification

- `python -m pytest tests/unit/test_entsoe.py -x -q`: 191 passed
- `python -m pytest -m "not live" -x -q`: 541 passed, 44 deselected
- `python -m ruff check src/gridflow/silver/entsoe/activated_balancing_*.py src/gridflow/silver/entsoe/contracted_reserves.py`: all checks passed

## Requirements closed

GAP-06 (direction/reserve_type fields) and GAP-07 (currency rename + ingested_at) fully closed across all 5 Phase 3 datasets (G3-01 + G3-02). Phase G3 complete.
