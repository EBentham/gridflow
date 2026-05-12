---
phase: G4
slug: G4-outages-unit-schema
status: complete
nyquist_compliant: true
wave_0_complete: true
created: 2026-05-02
---

# Phase G4 — Validation Strategy

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest |
| **Config file** | `pyproject.toml` |
| **Quick run command** | `python -m pytest tests/unit/test_entsoe.py -x -q` |
| **Full suite command** | `python -m pytest -m "not live" -x -q` |
| **Estimated runtime** | ~2.7 seconds |

---

## Sampling Rate

- **After every task commit:** `python -m pytest tests/unit/test_entsoe.py -x -q`
- **After every plan wave:** `python -m pytest -m "not live" -x -q`
- **Before verify-work:** Full suite must be green
- **Max feedback latency:** ~3 seconds

---

## Per-Task Verification Map

### G4-01 (Wave 1 — outages_generation unit-level schema)

| Task ID | Requirement | Must-Have Truth | Test Type | Test | Status |
|---------|-------------|-----------------|-----------|------|--------|
| G4-T1a | Parser extracts unit_mrid and unit_name from RegisteredResource | `parse_timeseries_xml` records include `unit_mrid` and `unit_name` keys (A80 fixture) | unit | `TestParseTimeseriesXml::test_record_has_expected_keys` | ✅ green |
| G4-T1b | Parser backward-compat: empty strings when RegisteredResource absent | Non-A80 XML produces `unit_mrid == ""` and `unit_name == ""` for all records | unit | `TestParseTimeseriesXml::test_unit_mrid_unit_name_empty_for_non_a80` | ✅ green |
| G4-T2a | EntsoeOutagesGeneration schema has unit_mrid, outage_type, unavailable_mw | Schema accepts valid record with required unit-level fields | unit/schema | `TestEntsoeOutagesGenerationSchema::test_valid_record` | ✅ green |
| G4-T2b | Schema: unit_name defaults to empty string | unit_name is optional with `""` default | unit/schema | `TestEntsoeOutagesGenerationSchema::test_unit_name_optional` | ✅ green |
| G4-T2c | Schema: naive timestamp rejected | `must_be_utc` validator raises on tz-naive datetime | unit/schema | `TestEntsoeOutagesGenerationSchema::test_naive_timestamp_rejected` | ✅ green |
| G4-T3a | Transformer produces 4 rows from 2-TimeSeries fixture | Two TimeSeries × 2 Points each = 4 silver rows | unit | `TestOutagesGenerationTransformer::test_four_records` | ✅ green |
| G4-T3b | Transformer maps A53 → "planned", A54 → "unplanned" | `outage_type` column contains semantic strings only | unit | `TestOutagesGenerationTransformer::test_outage_type_mapping` | ✅ green |
| G4-T3c | Transformer emits correct unit_mrid values | UNIT-DRAX-3 and UNIT-HEYSHAM-2 present in output | unit | `TestOutagesGenerationTransformer::test_unit_mrid_values` | ✅ green |
| G4-T3d | Transformer emits correct unit_name values | "Drax Unit 3" and "Heysham 2" present in output | unit | `TestOutagesGenerationTransformer::test_unit_name_values` | ✅ green |
| G4-T3e | Transformer emits correct unavailable_mw values | 800.0 and 1200.0 present in output | unit | `TestOutagesGenerationTransformer::test_unavailable_mw_values` | ✅ green |
| G4-T3f | Dedup on (timestamp_utc, unit_mrid) | Duplicate rows collapsed to one per unit per timestamp | unit | `TestOutagesGenerationTransformer::test_dedup_on_timestamp_unit` | ✅ green |
| G4-T3g | Transformer emits ingested_at | `ingested_at` column is tz-aware UTC datetime | unit | `TestOutagesGenerationTransformer::test_ingested_at_present` | ✅ green |
| G4-T3h | Transformer emits data_provider="entsoe" | `data_provider` literal column present | unit | `TestOutagesGenerationTransformer::test_data_provider` | ✅ green |
| G4-T3i | Transformer handles empty input | Empty DataFrame in → empty DataFrame out | unit | `TestOutagesGenerationTransformer::test_empty_input` | ✅ green |
| G4-T3j | Output column shape matches new schema | New cols present; old cols (production_type, available_capacity_mw) absent | unit | `TestOutagesGenerationTransformer::test_transform_basic` | ✅ green |
| G4-T3k | timestamp_utc dtype is Polars Datetime UTC | Column dtype is `pl.Datetime("us", "UTC")` | unit | `TestOutagesGenerationTransformer::test_timestamp_dtype` | ✅ green |
| G4-T3l | Schema contract validation on sample row | `EntsoeOutagesGeneration(**sample)` called on first output row | unit | `TestOutagesGenerationTransformer::test_transform_basic` (schema guard fires) | ✅ green |

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements.

- `tests/unit/test_entsoe.py` — all G4 tests present in this file
- `tests/fixtures/entsoe/outages_generation_gb.xml` — A80 fixture with 2 TimeSeries and RegisteredResource elements
- `tests/fixtures/entsoe/load_forecast_gb.xml` — non-A80 fixture used for backward-compat test
- pytest + polars already installed

---

## Manual-Only Verifications

All phase behaviors have automated verification.

---

## Validation Audit 2026-05-02

| Metric | Count |
|--------|-------|
| Requirements (GAP-IDs) | 1 (GAP-04) |
| Must-have truths verified | 17 |
| Tasks with automated coverage | 17/17 |
| Gaps found | 2 (PARTIAL — parser key assertions missing) |
| Resolved | 2 |
| Escalated | 0 |
| Final test count | 551 passed |

---

## Validation Sign-Off

- [x] All tasks have automated verify
- [x] Sampling continuity: every behavior has at least one test
- [x] Wave 0: existing infrastructure used, no stubs needed
- [x] No watch-mode flags
- [x] Feedback latency < 3s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-05-02
