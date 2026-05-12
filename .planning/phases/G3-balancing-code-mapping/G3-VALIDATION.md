---
phase: G3
slug: G3-balancing-code-mapping
status: complete
nyquist_compliant: true
wave_0_complete: true
created: 2026-05-02
---

# Phase G3 — Validation Strategy (Reconstructed)

> Reconstructed from G3-01-SUMMARY.md, G3-02-SUMMARY.md, and live test collection.
> State B reconstruction: no prior VALIDATION.md existed.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest |
| **Config file** | `pyproject.toml` |
| **Quick run command** | `python -m pytest tests/unit/test_entsoe.py -x -q` |
| **Full suite command** | `python -m pytest -m "not live" -x -q` |
| **Estimated runtime** | ~2.5 seconds |

---

## Sampling Rate

- **After every task commit:** `python -m pytest tests/unit/test_entsoe.py -x -q`
- **After every plan wave:** `python -m pytest -m "not live" -x -q`
- **Before verify-work:** Full suite must be green
- **Max feedback latency:** ~3 seconds

---

## Per-Task Verification Map

### G3-01 (Wave 1 — imbalance_prices + imbalance_volume)

| Task ID | Plan | Requirement | Must-Have Truth | Test Type | Test | Status |
|---------|------|-------------|-----------------|-----------|------|--------|
| G3-01-T1a | G3-01 | GAP-06/07 | EntsoeImbalancePrices has direction + price_eur_mwh (no business_type/price_gbp_mwh) | unit/schema | `TestEntsoeImbalancePricesSchema::test_valid_record` | ✅ green |
| G3-01-T1b | G3-01 | GAP-06 | EntsoeImbalanceVolume has direction (no flow_direction) | unit/schema | `TestEntsoeImbalanceVolumeSchema::test_valid_record` | ✅ green |
| G3-01-T2a | G3-01 | GAP-06 | ImbalancePrices: A19→"long", A20→"short" via replace_strict | unit | `TestImbalancePricesTransformer::test_direction_values` | ✅ green |
| G3-01-T2b | G3-01 | GAP-07 | ImbalancePrices: renames value→price_eur_mwh | unit | `TestImbalancePricesTransformer::test_transform_basic` + `test_price_values` | ✅ green |
| G3-01-T2c | G3-01 | GAP-07 | ImbalancePrices: emits ingested_at | unit | `TestImbalancePricesTransformer::test_ingested_at_present` | ✅ green |
| G3-01-T2d | G3-01 | GAP-06 | ImbalancePrices: dedup on (timestamp_utc, area_code, direction) | unit | `TestImbalancePricesTransformer::test_dedup` | ✅ green |
| G3-01-T2e | G3-01 | GAP-06 | ImbalanceVolume: A01→"long", A02→"short" via replace_strict | unit | `TestImbalanceVolumeTransformer::test_direction_values` | ✅ green |
| G3-01-T2f | G3-01 | GAP-07 | ImbalanceVolume: emits ingested_at | unit | `TestImbalanceVolumeTransformer::test_ingested_at_present` | ✅ green |
| G3-01-T2g | G3-01 | GAP-06 | ImbalanceVolume: dedup on (timestamp_utc, area_code, direction) | unit | `TestImbalanceVolumeTransformer::test_dedup` | ✅ green |
| G3-01-T2h | G3-01 | GAP-06/07 | Schema contract validation (EntsoeImbalancePrices(**sample)) | unit | transformer calls schema on sample row | ✅ green |

### G3-02 (Wave 2 — activated_balancing_qty/prices + contracted_reserves)

| Task ID | Plan | Requirement | Must-Have Truth | Test Type | Test | Status |
|---------|------|-------------|-----------------|-----------|------|--------|
| G3-02-T1a | G3-02 | GAP-06 | EntsoeActivatedBalancingQty has reserve_type + direction (no business_type) | unit/schema | `TestEntsoeActivatedBalancingQtySchema::test_valid_record` | ✅ green |
| G3-02-T1b | G3-02 | GAP-06/07 | EntsoeActivatedBalancingPrices has reserve_type, direction, price_eur_mwh | unit/schema | `TestEntsoeActivatedBalancingPricesSchema::test_valid_record` | ✅ green |
| G3-02-T1c | G3-02 | GAP-06 | EntsoeContractedReserves has reserve_type (no business_type) | unit/schema | `TestEntsoeContractedReservesSchema::test_valid_record` | ✅ green |
| G3-02-T2a | G3-02 | GAP-06 | ActivatedBalancingQty: A95→"fcr", A96→"afrr", A97→"mfrr", A98→"rr" | unit | `TestActivatedBalancingQtyTransformer::test_reserve_type_values` | ✅ green |
| G3-02-T2b | G3-02 | GAP-06 | ActivatedBalancingQty: A01→"up", A02→"down" | unit | `TestActivatedBalancingQtyTransformer::test_direction_values` | ✅ green |
| G3-02-T2c | G3-02 | GAP-07 | ActivatedBalancingQty: emits ingested_at | unit | `TestActivatedBalancingQtyTransformer::test_ingested_at_present` | ✅ green |
| G3-02-T2d | G3-02 | GAP-06 | ActivatedBalancingQty: dedup on (timestamp_utc, area_code, reserve_type, direction) | unit | `TestActivatedBalancingQtyTransformer::test_dedup` | ✅ green |
| G3-02-T2e | G3-02 | GAP-06 | ActivatedBalancingPrices: same reserve_type + direction mapping | unit | `TestActivatedBalancingPricesTransformer::test_reserve_type_values` + `test_direction_values` | ✅ green |
| G3-02-T2f | G3-02 | GAP-07 | ActivatedBalancingPrices: emits price_eur_mwh (not price_gbp_mwh) | unit | `TestActivatedBalancingPricesTransformer::test_transform_basic` + `test_fcr_up_price_values` | ✅ green |
| G3-02-T2g | G3-02 | GAP-07 | ActivatedBalancingPrices: emits ingested_at | unit | `TestActivatedBalancingPricesTransformer::test_ingested_at_present` | ✅ green |
| G3-02-T2h | G3-02 | GAP-06 | ActivatedBalancingPrices: dedup on (timestamp_utc, area_code, reserve_type, direction) | unit | `TestActivatedBalancingPricesTransformer::test_dedup` | ✅ green |
| G3-02-T2i | G3-02 | GAP-06 | ContractedReserves: A95→"fcr" etc. reserve_type mapping | unit | `TestContractedReservesTransformer::test_reserve_type_values` | ✅ green |
| G3-02-T2j | G3-02 | GAP-07 | ContractedReserves: emits ingested_at | unit | `TestContractedReservesTransformer::test_ingested_at_present` | ✅ green |
| G3-02-T2k | G3-02 | GAP-06 | ContractedReserves: dedup on (timestamp_utc, area_code, reserve_type) | unit | `TestContractedReservesTransformer::test_dedup` | ✅ green |
| G3-02-T2l | G3-02 | GAP-06 | Fixtures: flowDirection elements added to enable direction testing | integration | direction tests pass using updated XML fixtures | ✅ green |
| G3-02-T2m | G3-02 | GAP-06/07 | Schema contract validation on all 3 transformers | unit | each transformer calls schema(**sample) on first row | ✅ green |

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements.

- `tests/unit/test_entsoe.py` — all G3 tests present in this file
- `tests/fixtures/entsoe/` — XML fixtures for all 5 Phase 3 datasets
- pytest + polars already installed

---

## Manual-Only Verifications

All phase behaviors have automated verification.

---

## Validation Audit 2026-05-02

| Metric | Count |
|--------|-------|
| Requirements (GAP-IDs) | 2 (GAP-06, GAP-07) |
| Must-have truths verified | 13 (G3-01: 7 + G3-02: 10, some shared) |
| Tasks with automated coverage | 25/25 |
| Gaps found | 4 (PARTIAL — missing dedup tests) |
| Resolved | 4 |
| Escalated | 0 |
| Final test count | 200 passed |

---

## Validation Sign-Off

- [x] All tasks have automated verify
- [x] Sampling continuity: every behavior has at least one test
- [x] Wave 0: existing infrastructure used, no stubs needed
- [x] No watch-mode flags
- [x] Feedback latency < 3s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-05-02
