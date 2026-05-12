---
phase: L1
plan: L1-01
type: implementation
wave: 1
depends_on: []
files_modified:
  - docs/gie_agsi_endpoint_catalog.yaml
  - src/gridflow/connectors/gie/endpoints.py
  - tests/unit/test_gie_endpoint_catalog.py
  - tests/fixtures/gie/agsi_listing_response.json
autonomous: true
requirements_addressed:
  - AGSI-01
  - AGSI-03
---

# L1-01 Plan: GIE AGSI Endpoint Catalog And Inventory Contract

<objective>
Create the AGSI endpoint catalog and listing-derived query planning contract
needed for later bronze and silver implementation. This plan should leave
runtime connector behavior compatible with existing tests while making AGSI
endpoint families, query scopes, and expected-count semantics explicit.
</objective>

<threat_model>
## Threat Model

| Threat | Severity | Mitigation |
|--------|----------|------------|
| API key leakage in tests or docs | High | Never write `GIE_API_KEY` values into fixtures, logs, docs, or snapshots. Use only masked/provenance-safe examples. |
| Live API overuse from inventory tests | Medium | Keep L1 validation fixture-based; reserve live API calls for L4. |
| Silent endpoint omission | Medium | Catalog-to-metadata tests must fail if official endpoint families are missing or unclassified. |
| Wrong bronze expectations from stale fixture shape | Medium | Include fixture fields needed for companies/facilities and assert parse failures are explicit. |
</threat_model>

<tasks>

## Task 1 - Add AGSI Endpoint Catalog

**Type:** docs
**Files:** `docs/gie_agsi_endpoint_catalog.yaml`

**Action:**
- Create a catalog covering these official AGSI endpoint families:
  - `/api`
  - `/api/about`
  - `/api/about?show=listing`
  - `/api/news`
  - `/api/news?turl={id}`
  - `/api/unavailability`
- For each row include: `id`, `path`, `method`, `status`, `family`, `query_scopes`, `date_params`, `pagination`, `response_key`, `source_doc`, `implementation_phase`, and `notes`.
- Mark `/api/unavailability` as active with an ambiguity note because v007 both says unavailability is outside API coverage and documents/live-serves the endpoint.
- Mark ALSI rows out of scope or deferred, not active.

**Verify:**
- Catalog parses as YAML.
- Every row has a status from an explicit allowed set.

**Acceptance Criteria:**
- `AGSI-01` has a human-readable auditable catalog.

## Task 2 - Add Endpoint Metadata And Query Scope Helpers

**Type:** code
**Files:** `src/gridflow/connectors/gie/endpoints.py`

**Action:**
- Replace or extend simple constants with endpoint metadata while preserving existing `GIE_API_PATH`, `AGSI_COUNTRIES`, `ALSI_COUNTRIES`, and `DEFAULT_PAGE_SIZE` imports for compatibility.
- Add metadata for storage reports, listing flat, listing hierarchy, news, news item, and unavailability.
- Add query scope types or constants for aggregate type, country, company, and facility.
- Add pure helper functions for listing parsing and expected query planning. Keep them side-effect free and fixture-testable.
- Ensure helpers can calculate expected requests for:
  - one aggregate type over one exact date
  - all configured countries over one exact date
  - all companies in a listing fixture
  - all facilities in a listing fixture

**Verify:**
- Existing `tests/unit/test_gie.py` still imports country constants.
- New helpers require no network access.

**Acceptance Criteria:**
- `AGSI-03` has a deterministic inventory/query planning API available to tests and L2 implementation.

## Task 3 - Add Listing Fixture

**Type:** test
**Files:** `tests/fixtures/gie/agsi_listing_response.json`

**Action:**
- Add a small sanitized fixture shaped like `/api/about?show=listing`.
- Include at least two companies, multiple countries, and at least three facilities.
- Preserve representative fields: `name`, `short_name`, `type`, `eic`, `country`, `url`, `facilities`, facility `name`, `type`, `eic`, `country`, `company`, and `url`.

**Verify:**
- Fixture has no real API key or secrets.
- Fixture supports country/company/facility count tests.

**Acceptance Criteria:**
- Inventory tests do not need live AGSI calls.

## Task 4 - Add Catalog And Inventory Tests

**Type:** test
**Files:** `tests/unit/test_gie_endpoint_catalog.py`

**Action:**
- Test catalog YAML parses and every active endpoint has metadata.
- Test metadata active families match catalog active rows.
- Test `/api/about?show=listing` fixture parses into expected companies/facilities.
- Test expected query planning counts for aggregate, country, company, and facility scopes.
- Test exact-date and date-range query planning returns the intended gas-day targets.

**Verify:**
- Run `uv run pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py -q`.
- Run `uv run pytest -q` if the focused gate passes.

**Acceptance Criteria:**
- Non-live tests prove the AGSI endpoint/inventory contract before fetch behavior changes.

</tasks>

<verification>

1. `uv run pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py -q`
2. `uv run pytest -q`
3. Inspect `docs/gie_agsi_endpoint_catalog.yaml` to confirm AGSI active/deferred rows match v0.7 scope.

</verification>

<success_criteria>

- `docs/gie_agsi_endpoint_catalog.yaml` exists and classifies all official AGSI endpoint families.
- `src/gridflow/connectors/gie/endpoints.py` exposes endpoint metadata and fixture-testable query planning helpers.
- Listing-derived expected counts work for aggregate, country, company, and facility scopes.
- Focused and full non-live tests pass.

</success_criteria>

