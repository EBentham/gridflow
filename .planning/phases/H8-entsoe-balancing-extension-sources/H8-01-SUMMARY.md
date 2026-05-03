---
phase: H8-entsoe-balancing-extension-sources
plan: 01
subsystem: entsoe
tags: [entsoe, balancing, parser, silver, live-tests]
requires:
  - phase: H7-entsoe-outage-sources
    provides: outage source completion and live request-shape baseline
provides:
  - current_balancing_state silver source
  - balancing_energy_bids silver source
  - aggregated_balancing_energy_bids silver source
  - procured_balancing_capacity silver source
  - cross_zonal_balancing_capacity silver source
  - balancing_financial_expenses_income silver source
affects: [entsoe_endpoint_catalog, entsoe_live_tests]
tech-stack:
  added: []
  patterns: [metadata-driven H8 balancing request construction, shared H8 balancing transformer]
key-files:
  created:
    - src/gridflow/silver/entsoe/h8_balancing.py
    - tests/fixtures/entsoe/current_balancing_state_gb.xml
    - tests/fixtures/entsoe/balancing_energy_bids_gb.xml
    - tests/fixtures/entsoe/aggregated_balancing_energy_bids_gb.xml
    - tests/fixtures/entsoe/procured_balancing_capacity_gb.xml
    - tests/fixtures/entsoe/cross_zonal_balancing_capacity_gb_fr.xml
    - tests/fixtures/entsoe/balancing_financial_expenses_income_gb.xml
  modified:
    - src/gridflow/connectors/entsoe/endpoints.py
    - src/gridflow/connectors/entsoe/parsers.py
    - src/gridflow/schemas/entsoe.py
    - src/gridflow/silver/entsoe/__init__.py
    - config/sources.yaml
    - docs/entsoe_endpoint_catalog.yaml
    - tests/unit/test_entsoe.py
    - tests/integration/test_entsoe_mocked_e2e.py
    - tests/integration/test_entsoe_live.py
key-decisions:
  - "H8 bid and capacity payloads use dedicated silver schemas/transformers so bid identity, product, direction, agreement, and cross-zonal domain fields are not flattened away."
  - "High-volume H8 bid/capacity request metadata sends offset=0 by default to avoid ENTSO-E's 100-instance live API cap during request-shape probes."
patterns-established:
  - "H8 balancing datasets share one transformer base while selecting dataset-specific area columns and value families."
  - "The generic ENTSO-E parser now preserves area_Domain, connecting_Domain, Acquiring_Domain, market agreement, and market product metadata."
requirements-completed: [SRC-BAL-01, SRC-BAL-02, SRC-BAL-03, SRC-BAL-04, COVER-03, LIVE-05]
duration: 90 min
completed: 2026-05-03
---

# Phase H8 Plan 01: ENTSO-E Balancing Extension Sources Summary

**Balancing state, bid, capacity, cross-zonal capacity, and financial balancing sources added through the medallion path**

## Performance

- **Duration:** 90 min
- **Started:** 2026-05-03T12:05:00+01:00
- **Completed:** 2026-05-03T13:35:00+01:00
- **Tasks:** 4
- **Files modified:** 18

## Accomplishments

- Added six H8 balancing-extension datasets to ENTSO-E DOC_TYPES, source config, endpoint catalog, mocked URL coverage, and live request-shape coverage.
- Extended the ENTSO-E XML parser to preserve H8 balancing area, bid, product, direction, market-agreement, and cross-zonal domain metadata.
- Added H8 silver schemas and a shared H8 transformer module for quantity, bid, capacity, cross-zonal capacity, and financial amount payloads.
- Added realistic XML fixtures and bronze-to-silver integration coverage for all implemented H8 datasets.
- Kept archive, SO GL, and implementation-framework balancing rows deferred with explicit H9/backlog reasons.

## Task Commits

1. **H8 implementation** - `6cd5d65` (`feat(H8-01): add ENTSO-E balancing extension sources`)

## Verification

- `uv run --extra dev ruff check src/gridflow/connectors/entsoe src/gridflow/schemas/entsoe.py src/gridflow/silver/entsoe tests/unit/test_entsoe.py tests/unit/test_entsoe_endpoint_catalog.py tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_live.py` - passed
- `uv run --extra dev pytest tests/unit/test_entsoe.py tests/unit/test_entsoe_endpoint_catalog.py tests/integration/test_entsoe_connector.py tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_live.py -m "not live" -x -q` - 378 passed, 122 deselected, 1 warning
- `uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py::TestEntsoeLiveAllDatasets::test_live_request_shape_uses_supported_domain_params -q -rs` - 21 passed

## Deviations from Plan

### Runtime Fallback

`gsd-sdk` was not available on PATH in this PowerShell runtime, so execution used the documented inline fallback instead of SDK-driven wave orchestration. The phase still followed the H8 plan, produced the required artifacts, and passed the exact verification gates.

### Live Offset Default

The first live request-shape run found valid H8 bid/capacity parameters but ENTSO-E rejected unpaged calls because result sets exceeded its 100-instance cap. H8 metadata now sends `offset=0` by default for those high-volume endpoints; callers can still override `offset` through documented optional params.

**Total deviations:** 2 implementation/runtime adjustments.
**Impact on plan:** No product-scope change; the offset default is required to make the newly implemented request families live-compatible.

## Issues Encountered

- `uv` cache access required running verification commands outside the sandbox.
- The live request-shape command needed a longer timeout because it performs credentialed, rate-limited ENTSO-E calls.

## User Setup Required

None beyond the existing optional `ENTSOE_API_KEY` for live tests.

## Next Phase Readiness

H8 closes the near-term planned ENTSO-E catalog rows. Future balancing archive, SO GL, and implementation-framework rows remain cataloged as deferred H9/backlog work.

## Self-Check: PASSED

The required files exist, H8 requirements are reflected in tests and tracking docs, and all plan-level verification gates passed.

---
*Phase: H8-entsoe-balancing-extension-sources*
*Completed: 2026-05-03*
