---
status: complete
phase: H5-entsoe-generation-unit-sources
source: [H5-01-SUMMARY.md, H5-VERIFICATION.md]
started: 2026-05-03T00:00:00Z
updated: 2026-05-03T00:00:00Z
---

## Current Test

[testing complete]

## Tests

### 1. H5 datasets are registered and catalog-aligned
expected: `installed_capacity_units`, `actual_generation_units`, `water_reservoirs`, and `generation_units_master_data` appear in runtime `DOC_TYPES`, `config/sources.yaml`, and `docs/entsoe_endpoint_catalog.yaml` with matching document/process metadata.
result: pass
evidence: `tests/unit/test_entsoe_endpoint_catalog.py` and the non-live H5 pytest gate passed.

### 2. H5 datasets transform fixture-backed bronze XML to silver output
expected: Each H5 dataset has a realistic XML fixture and writes non-empty schema-valid silver output with the expected unit, reservoir, or master-data columns.
result: pass
evidence: `tests/integration/test_entsoe_mocked_e2e.py` passed as part of the non-live H5 pytest gate.

### 3. ENTSO-E live request-shape probe accepts H5 request families
expected: The opt-in live request-shape probe fetches representative ENTSO-E request families, including `actual_generation_units` and `generation_units_master_data`, without unsupported parameter-name errors or token leakage.
result: pass
evidence: `uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py::TestEntsoeLiveAllDatasets::test_live_request_shape_uses_supported_domain_params -q -rs` reported 8 passed.

### 4. A95 generation-unit master data parses live XML records
expected: A live `generation_units_master_data` response parses into records with `area_code`, `unit_mrid`, `unit_name`, `production_type`, and `implementation_datetime_utc`.
result: pass
evidence: Targeted live sanity check parsed 230 records from the first GB A95 response.

### 5. H4 bronze data-date partition regression is covered
expected: ENTSO-E connector responses set `RawResponse.data_date` from the requested period start, and bronze writes partition by that data date rather than ingestion time.
result: pass
evidence: `tests/integration/test_entsoe_mocked_e2e.py` includes connector `data_date` and bronze partition regression checks; the non-live H5 pytest gate passed.

## Summary

total: 5
passed: 5
issues: 0
pending: 0
skipped: 0

## Gaps

[none]
