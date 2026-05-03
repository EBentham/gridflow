---
phase: I1-elexon-inventory-test-scaffolding
plan: 01
subsystem: testing
tags: [elexon, inventory, live-tests, silver-registry]
requires:
  - phase: v0.4 requirements
    provides: Elexon validation scope and I1 inventory requirements
provides:
  - Elexon inventory contract tests across config, endpoint registry, and silver registry
  - Explicit excluded Elexon endpoint manifest
  - Elexon live endpoint diagnostics helper
affects: [I2, I3, elexon-live-validation]
tech-stack:
  added: []
  patterns:
    - Registry-driven inventory contract tests
    - Source-scoped live API diagnostics
key-files:
  created:
    - .planning/phases/I1-elexon-inventory-test-scaffolding/I1-01-SUMMARY.md
    - .planning/phases/I1-elexon-inventory-test-scaffolding/I1-VERIFICATION.md
  modified:
    - config/sources.yaml
    - src/gridflow/connectors/elexon/endpoints.py
    - tests/unit/test_elexon_endpoints.py
    - tests/endpoints/test_endpoint_urls.py
    - tests/endpoints/test_endpoint_live.py
key-decisions:
  - "Use config, endpoint registry, and silver registry as the inventory contract instead of duplicating a full active dataset list in tests."
  - "Keep excluded Elexon datasets in a named EXCLUDED_ENDPOINTS manifest with reasons."
  - "Treat full live endpoint-file ENTSO-E failures as out of I1 scope; verify the Elexon live classes directly."
patterns-established:
  - "Inventory drift tests compare production registries and explicit exclusions."
  - "Elexon live assertion failures include source, dataset, stage, parameter style, URL, status, and bounded body preview."
requirements-completed:
  - ELEXON-INV-01
  - ELEXON-INV-02
  - ELEXON-INV-03
duration: 32min
completed: 2026-05-03
---

# Phase I1 Plan 01: Elexon Inventory Contract and Live-Test Scaffolding Summary

**Elexon active dataset inventory is now tested across config, endpoint definitions, and silver transformer registration with explicit exclusions and source-scoped live diagnostics.**

## Performance

- **Duration:** 32 min
- **Started:** 2026-05-03T12:58:00Z
- **Completed:** 2026-05-03T13:30:42Z
- **Tasks:** 5
- **Files modified:** 14

## Accomplishments

- Added registry-driven inventory tests proving active Elexon config datasets match `ENDPOINTS`, endpoint paths, parameter styles, and registered silver transformers.
- Added `EXCLUDED_ENDPOINTS` for `bod`, `generation_by_fuel`, and `indicative_imbalance_volumes` with reason strings.
- Corrected the configured `boal` endpoint path to `/datasets/BOALF` so source config matches the active endpoint registry.
- Added Elexon live assertion helpers that include source, dataset, stage, parameter style, URL, status, and bounded response preview.
- Cleaned lint issues in the Elexon verification target so the phase lint gate passes.

## Task Commits

1. **Inventory contract and live-test scaffolding** - `e6d1078` (`test(I1-01): add elexon inventory contract tests`)

## Files Created/Modified

- `config/sources.yaml` - aligned active `boal` config path with BOALF replacement endpoint.
- `src/gridflow/connectors/elexon/endpoints.py` - added explicit excluded endpoint manifest and kept request-param logic lint-clean.
- `tests/unit/test_elexon_endpoints.py` - added inventory contract coverage against config, endpoints, param styles, and silver registry.
- `tests/endpoints/test_endpoint_urls.py` - made broad Elexon inventory assertions config-driven instead of list-duplicated.
- `tests/endpoints/test_endpoint_live.py` - added Elexon live diagnostics helper and applied it to Elexon live probes.
- `src/gridflow/connectors/elexon/client.py`, `src/gridflow/silver/elexon/*.py`, and `tests/integration/test_elexon_connector.py` - lint cleanup required by the phase verification target.

## Decisions Made

- The active Elexon inventory contract should compare real registries rather than maintaining another hard-coded active dataset list.
- `boal` remains the GridFlow dataset name, but its configured API path now matches the active Elexon BOALF replacement.
- The full `tests/endpoints/test_endpoint_live.py` run currently includes ENTSO-E live checks when `ENTSOE_API_KEY` is present; I1 treats those failures as unrelated and verifies the Elexon live classes directly.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing Critical] Config path drift for `boal`**
- **Found during:** Task 1 (inventory contract tests)
- **Issue:** `config/sources.yaml` used `/datasets/BOAL` while `ENDPOINTS["boal"]` correctly used `/datasets/BOALF`.
- **Fix:** Updated `boal` config path to `/datasets/BOALF`.
- **Files modified:** `config/sources.yaml`
- **Verification:** Fast non-live tests passed; Elexon live BOALF replacement probe passed.
- **Committed in:** `e6d1078`

**2. [Rule 3 - Blocking] Lint gate exposed existing Elexon target style issues**
- **Found during:** Task 5 (verification)
- **Issue:** The required ruff command failed on pre-existing issues inside the Elexon verification target.
- **Fix:** Applied mechanical lint cleanup limited to files included in the phase gate.
- **Files modified:** `src/gridflow/connectors/elexon/client.py`, `src/gridflow/connectors/elexon/endpoints.py`, `src/gridflow/silver/elexon/*.py`, `tests/endpoints/test_endpoint_urls.py`, `tests/endpoints/test_endpoint_live.py`, `tests/integration/test_elexon_connector.py`, `tests/unit/test_elexon_endpoints.py`
- **Verification:** Ruff passed.
- **Committed in:** `e6d1078`

---

**Total deviations:** 2 auto-fixed (1 missing critical, 1 blocking).
**Impact on plan:** Both changes were required to make the I1 inventory contract and verification gate meaningful. No new Elexon datasets were added.

## Issues Encountered

- Full live endpoint-file verification failed because ENTSO-E live tests returned 400 with the local `ENTSOE_API_KEY`. This is outside I1's Elexon scope. The Elexon-only live endpoint smoke suite passed.

## Verification

- `uv run --extra dev ruff check src/gridflow/connectors/elexon src/gridflow/silver/elexon tests/unit/test_elexon_endpoints.py tests/endpoints/test_endpoint_urls.py tests/endpoints/test_endpoint_live.py tests/integration/test_elexon_connector.py` - passed.
- `uv run --extra dev pytest tests/unit/test_elexon_endpoints.py tests/endpoints/test_endpoint_urls.py tests/integration/test_elexon_connector.py -m "not live" -x -q` - passed, 118 tests.
- `uv run --extra dev pytest -m live tests/endpoints/test_endpoint_live.py::TestElexonLivePathDate tests/endpoints/test_endpoint_live.py::TestElexonLiveFromTo tests/endpoints/test_endpoint_live.py::TestElexonLiveSettlementDatePeriod tests/endpoints/test_endpoint_live.py::TestElexonLiveBrokenEndpoints tests/endpoints/test_endpoint_live.py::TestElexonLiveSettlementDateQuery tests/endpoints/test_endpoint_live.py::TestElexonLivePublishDatetime tests/endpoints/test_endpoint_live.py::TestElexonLiveNoParams -q -rs` - passed, 26 tests.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

I2 can now build mocked request-shape and fixture-backed bronze-to-silver tests on a stable active Elexon dataset inventory and explicit exclusion manifest. I3 can reuse the live diagnostics helper when moving from endpoint pings to live bronze-to-silver assertions.

## Self-Check: PASSED

- Plan tasks completed.
- Requirements `ELEXON-INV-01`, `ELEXON-INV-02`, and `ELEXON-INV-03` satisfied.
- Fast and Elexon-live verification passed.
- Full live endpoint-file ENTSO-E failure documented as unrelated to I1.

---
*Phase: I1-elexon-inventory-test-scaffolding*
*Completed: 2026-05-03*
