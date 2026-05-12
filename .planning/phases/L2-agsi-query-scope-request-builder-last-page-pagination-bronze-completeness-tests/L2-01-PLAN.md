---
phase: L2
plan: L2-01
type: implementation
wave: 1
depends_on: []
files_modified:
  - config/sources.yaml
  - src/gridflow/connectors/gie/endpoints.py
  - src/gridflow/connectors/gie/client.py
  - tests/unit/test_gie_endpoint_catalog.py
  - tests/integration/test_gie_agsi_mocked_bronze.py
  - tests/fixtures/gie/agsi_listing_response.json
autonomous: true
requirements_addressed:
  - AGSI-02
  - AGSI-04
  - AGSI-05
  - AGSI-06
requirements:
  - AGSI-02
  - AGSI-04
  - AGSI-05
  - AGSI-06
---

# L2-01 Plan: AGSI Query-Scope Request Builder, last_page Pagination, And Bronze Completeness

<objective>
Implement AGSI bronze fetching so requests are built from documented query
scopes, paginated by `last_page`, partitioned by requested gas day, and covered
by non-live tests proving aggregate, country, company, and facility bronze
completeness against the L1 expected query-plan contract.
</objective>

## Execution Context

Read these before editing:

- `.planning/phases/L2-agsi-query-scope-request-builder-last-page-pagination-bronze-completeness-tests/L2-RESEARCH.md`
- `.planning/phases/L2-agsi-query-scope-request-builder-last-page-pagination-bronze-completeness-tests/L2-VALIDATION.md`
- `.planning/phases/L2-agsi-query-scope-request-builder-last-page-pagination-bronze-completeness-tests/L2-PATTERNS.md`
- `.planning/phases/L1-gie-agsi-endpoint-research-catalog-inventory-contract/L1-01-SUMMARY.md`
- `.planning/REQUIREMENTS.md`
- `.planning/ROADMAP.md`
- `.planning/research/GIE-AGSI-API-RESEARCH.md`
- `docs/gie_agsi_endpoint_catalog.yaml`
- `config/sources.yaml`
- `src/gridflow/connectors/base.py`
- `src/gridflow/connectors/gie/endpoints.py`
- `src/gridflow/connectors/gie/client.py`
- `src/gridflow/bronze/writer.py`
- `tests/unit/test_gie.py`
- `tests/unit/test_gie_endpoint_catalog.py`
- `tests/integration/test_entsog_mocked_e2e.py`
- `tests/integration/test_neso_mocked_e2e.py`
- `tests/fixtures/gie/agsi_listing_response.json`

## Must Haves

- Satisfy `AGSI-02`, `AGSI-04`, `AGSI-05`, and `AGSI-06`.
- Preserve `storage` as a compatibility alias while adding catalog-aligned
  `storage_reports` behavior for AGSI storage bronze fetching.
- Active AGSI inventory must align across `docs/gie_agsi_endpoint_catalog.yaml`,
  `src/gridflow/connectors/gie/endpoints.py`, `config/sources.yaml`, and
  `GieConnector.list_datasets()`.
- Storage request params must use documented `date`, `from`, `to`, `type`,
  `country`, `company`, `facility`, `page`, and `size` keys as appropriate.
- Runtime pagination must stop on `last_page`; `total` must be treated only as
  the current-page count.
- Bronze `RawResponse` objects must set `page`, `total_pages`, and `data_date`
  for storage reports.
- Tests must not call the live AGSI API and must not require `GIE_API_KEY`.
- Completeness tests must assert exact expected request/page/file counts, not
  just non-empty responses.

<threat_model>
## Threat Model

| Threat | Severity | Mitigation |
|--------|----------|------------|
| `GIE_API_KEY` leaks through tests or captured request metadata | High | Use only mocked `respx` calls and test config keys such as `test-key`; never snapshot real env values. |
| Mocked tests accidentally hit live AGSI | High | Use `respx.mock`, strict route assertions, and non-live pytest commands. |
| Dataset inventory drifts between catalog, config, and connector | Medium | Add alignment tests comparing active catalog ids, `ENDPOINTS`, config datasets, and connector `list_datasets()`. |
| Pagination silently misses pages | Medium | Mock `last_page=2` responses and assert exactly two pages with `total_pages == 2`. |
| Exact-day requests write out-of-window gas days | Medium | Use exact `date` request specs, set `RawResponse.data_date`, and assert mocked payload gas-day fields match the requested day before bronze writes. |
| Silver scope creeps into L2 | Low | Limit this plan to bronze request/provenance behavior; defer field-preserving silver work to L3. |
</threat_model>

<tasks>

## Task 1 - Align AGSI Source Inventory And Endpoint Helpers

**Type:** code
**Files:**
- `config/sources.yaml`
- `src/gridflow/connectors/gie/endpoints.py`
- `tests/unit/test_gie_endpoint_catalog.py`

**Action:**
- Update `config/sources.yaml` for `gie_agsi` so active AGSI dataset ids align
  with the active catalog/metadata rows:
  - `storage_reports`
  - `about_summary`
  - `about_listing`
  - `news`
  - `news_item`
  - `unavailability`
- Keep the existing `storage` dataset as a legacy alias if needed for
  compatibility with `GasStorageTransformer.dataset == "storage"`.
- Set `gie_agsi.rate_limit_per_second` to `1` to respect the documented
  60 calls/minute limit.
- Extend endpoint helper tests to assert active catalog ids are exposed by
  source config and connector metadata.
- Fix AGSI company/facility scope parameter helpers so company requests include
  `country` and `company`, and facility requests include `country`, `company`,
  and `facility` when built from listing inventory.

**Verify:**
- `uv run --extra dev pytest tests/unit/test_gie_endpoint_catalog.py tests/unit/test_gie.py -q`

**Acceptance Criteria:**
- `AGSI-02` has an automated inventory alignment check.
- The old `storage` import/transformer path remains usable until L3 revisits
  silver dataset names.

## Task 2 - Implement Metadata-Driven AGSI Storage Fetching

**Type:** code
**Files:**
- `src/gridflow/connectors/gie/client.py`
- `src/gridflow/connectors/gie/endpoints.py`

**Action:**
- Refactor `GieConnector.fetch()` so `gie_agsi` storage fetching is driven by
  L1 query-plan helpers rather than `_COUNTRY_MAP` loops.
- Accept these storage kwargs:
  - `scope`: `aggregate_type`, `country`, `company`, or `facility`
  - `aggregate_types`: optional tuple/list, defaulting to `("EU",)`
  - `countries`: optional tuple/list, defaulting to `AGSI_COUNTRIES`
  - `listing_payload`: optional fixture/live listing payload for company and
    facility planning
  - `size`: optional page size capped by `DEFAULT_PAGE_SIZE`
- For `company` and `facility` scopes, use `listing_payload` when provided; if
  absent, fetch `/api/about?show=listing` once and derive the inventory.
- For exact-day and date-window calls, build one exact `date=YYYY-MM-DD`
  request per planned gas day so each response can set the correct
  `RawResponse.data_date`.
- Preserve current ALSI behavior unless tests show a compatibility issue.
- Support dataset aliases so `storage` and `storage_reports` both fetch AGSI
  storage reports, but emit `RawResponse.dataset` in a way existing downstream
  code can handle.
- Remove the AGSI `till` parameter. Use documented `date` for exact requests
  and only `from`/`to` for any explicit range-mode extension.

**Verify:**
- New mocked tests in Task 4 prove aggregate/country/company/facility request
  params and no live network dependency.

**Acceptance Criteria:**
- `AGSI-04` storage requests use documented query-scope and gas-day params.
- Existing `tests/unit/test_gie.py` remains green.

## Task 3 - Implement last_page Pagination And Bronze Provenance

**Type:** code
**Files:**
- `src/gridflow/connectors/gie/client.py`

**Action:**
- Replace the current `total`/`pageSize` loop with JSON parsing of
  `last_page`.
- After each page response, append a `RawResponse` with:
  - `page` set to the requested page
  - `total_pages` set to parsed `last_page`, defaulting to `1` if absent
  - `request_params` set to the actual query params sent to AGSI
  - `data_date` set to the expected gas day for storage reports
  - `api_version` kept stable as `"v1"`
- Treat `total` as current-page row count only; do not use it for pagination
  loop exit.
- Add a small payload gas-day guard for storage reports so exact-day mocked
  responses with data rows outside the requested gas day are not returned as
  valid bronze responses. Do not mutate raw response bodies.
- Keep exception handling narrow enough that unexpected implementation bugs
  fail tests instead of being silently swallowed.

**Verify:**
- `uv run --extra dev pytest tests/integration/test_gie_agsi_mocked_bronze.py -m "not live" -q`

**Acceptance Criteria:**
- `AGSI-05` is covered by automated last-page and provenance assertions.

## Task 4 - Add Mocked Request-Shape And Bronze Completeness Tests

**Type:** test
**Files:**
- `tests/integration/test_gie_agsi_mocked_bronze.py`
- `tests/fixtures/gie/agsi_listing_response.json`

**Action:**
- Create `tests/integration/test_gie_agsi_mocked_bronze.py` with:
  - imports for `json`, `re`, `date`, `datetime`, `UTC`, `Path`, `httpx`,
    `pytest`, and `respx`
  - imports for `BronzeWriter`, `load_settings`, `GieConnector`,
    `QueryScope`, `build_storage_query_plan`, and `expected_records_for_plan`
  - `BASE_URL = "https://agsi.gie.eu"`
  - deterministic `TARGET_DATE = date(2026, 5, 1)`, `START`, and `END`
  - a `_gie_config()` helper returning `gie_agsi` config with
    `api_key="test-key"`, `rate_limit_per_second=1000`, and `timeout=5`
  - a `_storage_body(gas_day, last_page=1, rows=1)` helper returning AGSI-shaped
    JSON bytes with `last_page`, `total`, `gas_day`, and `data`
  - a `_load_listing_fixture()` helper using the existing L1 fixture
- Add request-shape tests for:
  - aggregate scope: `type=EU`, `date=2026-05-01`, `page=1`, `size=300`
  - country scope: selected countries such as `DE` and `FR`
  - company scope: company EICs derived from the listing fixture, including
    `country` and `company`
  - facility scope: facility EICs derived from the listing fixture, including
    `country`, `company`, and `facility`
- Add `test_storage_paginates_with_last_page_not_total` where page 1 returns
  `last_page=2` and `total=300`, page 2 returns `last_page=2`; assert exactly
  two responses and `total_pages == 2`.
- Add a bronze completeness test that:
  - builds the expected plan for selected aggregate/country/company/facility
    scopes
  - fetches through the mocked connector
  - writes each response through `BronzeWriter`
  - asserts the number of bronze JSON files equals the expected request/page
    count
  - asserts every `.meta.json` sidecar records `page`, `total_pages`,
    `request_params`, `source == "gie_agsi"`, and the expected dataset id
  - asserts bronze paths include `2026/05/01`
- Add an out-of-window exact-day test where a mocked response for
  `date=2026-05-01` contains a row for `2026-05-02`; assert the connector does
  not return it as a valid storage response or raises the expected validation
  error.

**Verify:**
- `uv run --extra dev pytest tests/integration/test_gie_agsi_mocked_bronze.py -m "not live" -q`

**Acceptance Criteria:**
- `AGSI-04`, `AGSI-05`, and `AGSI-06` have deterministic non-live coverage.
- No test is marked `@pytest.mark.live`.

## Task 5 - Run Verification And Keep L2 Non-Live

**Type:** validation
**Files:**
- `src/gridflow/connectors/gie/endpoints.py`
- `src/gridflow/connectors/gie/client.py`
- `tests/unit/test_gie.py`
- `tests/unit/test_gie_endpoint_catalog.py`
- `tests/integration/test_gie_agsi_mocked_bronze.py`

**Action:**
- Run:

```powershell
uv run --extra dev ruff check src/gridflow/connectors/gie tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py
uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py -m "not live" -q
uv run --extra dev pytest -m "not live" -q
```

- If the focused test module needs extra fixture payloads, add only small
  sanitized JSON files under `tests/fixtures/gie/`.
- Do not run live AGSI calls or require `GIE_API_KEY`.

**Verify:**
- Ruff exits 0.
- Focused pytest exits 0.
- Full non-live pytest exits 0 or any unrelated pre-existing failure is
  recorded clearly in the phase summary.

**Acceptance Criteria:**
- L2 is ready for execution with no live API dependency.

</tasks>

<verification>

Run:

```powershell
uv run --extra dev ruff check src/gridflow/connectors/gie tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py
uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py -m "not live" -q
uv run --extra dev pytest -m "not live" -q
```

</verification>

<success_criteria>

- `AGSI-02`: AGSI active dataset inventory aligns across catalog, endpoint
  metadata, source config, and connector dataset listing.
- `AGSI-04`: Users can fetch AGSI storage bronze data by aggregate type,
  country, company, facility, and exact gas day using documented params.
- `AGSI-05`: AGSI pagination uses `last_page`, not `total`, and bronze
  sidecars record page and total-page provenance.
- `AGSI-06`: Mocked bronze completeness tests prove expected request/page/file
  counts for selected query scopes and exact-day windows.
- Focused and full non-live verification pass without `GIE_API_KEY` or live API
  calls.

</success_criteria>

