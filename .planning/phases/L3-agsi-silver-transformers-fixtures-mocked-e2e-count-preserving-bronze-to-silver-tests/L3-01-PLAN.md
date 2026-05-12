---
phase: L3
plan: L3-01
type: implementation
wave: 1
depends_on:
  - L2
files_modified:
  - src/gridflow/silver/gie/agsi.py
  - src/gridflow/silver/gie/__init__.py
  - src/gridflow/schemas/gie.py
  - tests/unit/test_gie.py
  - tests/integration/test_gie_agsi_mocked_e2e.py
  - tests/integration/test_gie_agsi_mocked_bronze.py
  - tests/fixtures/gie/*.json
  - docs/gie_agsi_endpoint_catalog.yaml
autonomous: true
requirements_addressed:
  - AGSI-07
  - AGSI-08
  - AGSI-09
  - AGSI-10
requirements:
  - AGSI-07
  - AGSI-08
  - AGSI-09
  - AGSI-10
---

# L3-01 Plan: AGSI Silver Transformers, Fixtures, Mocked E2E, And Count-Preserving Bronze-To-Silver Tests

<objective>
Extend AGSI silver transformation beyond the legacy compact `storage` dataset so
storage reports preserve live payload fields across aggregate, country, company,
and facility scopes, active listing/news/unavailability families either produce
deterministic silver parquet or are explicitly catalog-deferred, and non-live
fixture-backed E2E tests prove AGSI bronze rows reach silver without network
access.
</objective>

## Execution Context

Read these before editing:

- `.planning/phases/L3-agsi-silver-transformers-fixtures-mocked-e2e-count-preserving-bronze-to-silver-tests/L3-RESEARCH.md`
- `.planning/phases/L3-agsi-silver-transformers-fixtures-mocked-e2e-count-preserving-bronze-to-silver-tests/L3-VALIDATION.md`
- `.planning/phases/L3-agsi-silver-transformers-fixtures-mocked-e2e-count-preserving-bronze-to-silver-tests/L3-PATTERNS.md`
- `.planning/phases/L2-agsi-query-scope-request-builder-last-page-pagination-bronze-completeness-tests/L2-01-SUMMARY.md`
- `.planning/phases/L2-agsi-query-scope-request-builder-last-page-pagination-bronze-completeness-tests/L2-01-PLAN.md`
- `.planning/phases/L2-agsi-query-scope-request-builder-last-page-pagination-bronze-completeness-tests/L2-RESEARCH.md`
- `.planning/phases/L1-gie-agsi-endpoint-research-catalog-inventory-contract/L1-01-SUMMARY.md`
- `.planning/REQUIREMENTS.md`
- `.planning/ROADMAP.md`
- `.planning/research/GIE-AGSI-API-RESEARCH.md`
- `docs/gie_agsi_endpoint_catalog.yaml`
- `config/sources.yaml`
- `src/gridflow/connectors/gie/endpoints.py`
- `src/gridflow/connectors/gie/client.py`
- `src/gridflow/bronze/writer.py`
- `src/gridflow/silver/base.py`
- `src/gridflow/silver/registry.py`
- `src/gridflow/silver/gie/agsi.py`
- `src/gridflow/silver/gie/__init__.py`
- `src/gridflow/schemas/gie.py`
- `tests/conftest.py`
- `tests/unit/test_gie.py`
- `tests/unit/test_gie_endpoint_catalog.py`
- `tests/integration/test_gie_agsi_mocked_bronze.py`
- `tests/integration/test_entsog_mocked_e2e.py`
- `tests/integration/test_neso_mocked_e2e.py`
- `tests/fixtures/gie/`

## Must Haves

- Satisfy `AGSI-07`, `AGSI-08`, `AGSI-09`, and `AGSI-10`.
- Preserve compatibility for `gie_agsi/storage` while registering and testing
  the L2 catalog-aligned `gie_agsi/storage_reports` dataset.
- Storage silver must preserve inventory, flow, capacity, fullness, status,
  update time, entity metadata, and service-announcement fields from live-shaped
  AGSI payloads.
- Aggregate, country, company, and facility storage rows must remain
  distinguishable in silver using explicit entity-level/entity-key columns.
- Listing, news, news item, and unavailability active AGSI families must either
  produce deterministic silver parquet or be explicitly catalog-deferred with a
  reason. Prefer transform coverage unless implementation evidence shows a
  family cannot be safely normalized in L3.
- Fixture-backed tests must use `BronzeWriter` plus registered transformers,
  not only direct `transform()` calls.
- Mocked E2E tests must not call the live AGSI API and must not require
  `GIE_API_KEY`.
- Existing L2 mocked bronze request-shape, `last_page`, and expected-count tests
  must remain green.

<threat_model>
## Threat Model

| ID | Threat | Severity | Mitigation |
|----|--------|----------|------------|
| T-L3-01 | Storage silver silently drops live AGSI fields needed downstream. | High | Add unit and E2E assertions for each required inventory/flow/capacity/fullness/status/update/entity field. |
| T-L3-02 | Deduplication collapses aggregate, country, company, and facility rows for the same gas day. | High | Add entity-level/entity-key columns and deduplicate by gas day plus entity scope and code/url. |
| T-L3-03 | Active listing/news/unavailability families remain configured but untransformable. | Medium | Register deterministic transformers or update the catalog with explicit deferred reasons and tests proving classification. |
| T-L3-04 | Tests bypass the real bronze-to-silver path and miss registry or file-layout regressions. | Medium | Use `BronzeWriter`, `get_transformer`, `run(target_date)`, and `pl.read_parquet` in integration tests. |
| T-L3-05 | L3 introduces live API dependency or weakens L2 non-live coverage. | High | Use local fixture `RawResponse` objects only, no live markers, and run both L2 mocked bronze and L3 mocked E2E suites. |
</threat_model>

<tasks>

## Task 1 - Expand AGSI Storage Silver Schema And Registration

**Type:** code
**Files:**
- `src/gridflow/silver/gie/agsi.py`
- `src/gridflow/silver/gie/__init__.py`
- `src/gridflow/schemas/gie.py`
- `tests/unit/test_gie.py`

**Action:**
- Refactor `GasStorageTransformer` so it can be used for both legacy
  `dataset = "storage"` and catalog-aligned `storage_reports`.
- Register a transformer for `("gie_agsi", "storage_reports")` while keeping
  `("gie_agsi", "storage")` intact.
- Teach storage bronze reading to parse AGSI `{"data": [...]}` payloads from
  the selected dataset path without mutating raw JSON.
- Normalize and preserve these storage fields when present:
  - `gasDayStart` -> `gas_day`
  - `gasDayEnd` -> `gas_day_end`
  - `updatedAt` -> `updated_at`
  - `name` -> `entity_name`
  - `code` -> `entity_code`
  - `url` -> `entity_url`
  - `gasInStorage`, `consumption`, `consumptionFull`, `injection`,
    `withdrawal`, `netWithdrawal`, `workingGasVolume`,
    `injectionCapacity`, `withdrawalCapacity`, `contractedCapacity`,
    `availableCapacity`, `coveredCapacity`, `full`, `trend`
  - `status` and `info`
- Derive `entity_level` from the available request metadata or row shape:
  aggregate when request params contain `type`, country when they contain
  `country` only, company when they contain `company` without `facility`, and
  facility when they contain `facility`.
- Preserve `country_code` and `country_name` for backward compatibility where
  existing rows/tests expect them.
- Replace storage deduplication with a key that includes `gas_day`,
  `entity_level`, `entity_code`, and `entity_url` where available.
- Add focused unit tests proving:
  - existing `agsi_gb_response.json` compatibility still passes
  - `storage_reports` registration exists
  - live-shaped storage fields are present and numeric/datetime typed
  - aggregate/country/company/facility rows on the same gas day do not collapse

**Verify:**
- `uv run --extra dev pytest tests/unit/test_gie.py -q`

**Acceptance Criteria:**
- `AGSI-07` storage preservation is covered by automated tests.
- Both `storage` and `storage_reports` transformers are registered for
  `gie_agsi`.

## Task 2 - Add AGSI Listing, News, And Unavailability Silver Transformers

**Type:** code
**Files:**
- `src/gridflow/silver/gie/agsi.py`
- `src/gridflow/silver/gie/__init__.py`
- `docs/gie_agsi_endpoint_catalog.yaml`
- `tests/unit/test_gie.py`

**Action:**
- Add deterministic AGSI transformers for active non-storage families:
  - `about_summary`
  - `about_listing`
  - `news`
  - `news_item`
  - `unavailability`
- Prefer small family-specific transformer classes over a broad abstraction if
  field shapes differ.
- Normalize columns to snake_case and preserve provider metadata.
- For listing payloads, preserve company/facility EIC, name, country, URL,
  entity type, and parent company fields.
- For news/news item payloads, preserve URL/turl, title, summary/details,
  start/end timestamps, update timestamps if present, and linked entities where
  feasible as JSON strings.
- For unavailability payloads, preserve storage/entity identifiers, status,
  event windows, capacity/volume fields, update timestamps, and any reason/info
  fields present in fixtures.
- If any active dataset cannot be transformed safely, update
  `docs/gie_agsi_endpoint_catalog.yaml` to classify it as deferred with a clear
  L3 reason and add a test proving that catalog-backed classification. Do not
  silently leave an active configured dataset without a transformer or deferral.
- Add unit tests for the new transformer classes using small in-memory
  DataFrames or new fixture records.

**Verify:**
- `uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py -q`

**Acceptance Criteria:**
- `AGSI-08` is satisfied by transformer output coverage or explicit
  catalog-backed deferral with tests.
- `gridflow.silver.gie` imports register every transformed active AGSI dataset.

## Task 3 - Add Sanitized AGSI Fixtures For All L3 Families

**Type:** test
**Files:**
- `tests/fixtures/gie/agsi_storage_reports_response.json`
- `tests/fixtures/gie/agsi_about_summary_response.json`
- `tests/fixtures/gie/agsi_news_response.json`
- `tests/fixtures/gie/agsi_news_item_response.json`
- `tests/fixtures/gie/agsi_unavailability_response.json`
- `tests/fixtures/gie/agsi_listing_response.json`

**Action:**
- Add compact sanitized JSON fixtures shaped like AGSI responses:
  - storage rows for aggregate, country, company, and facility scopes on
    `2026-05-01`
  - about summary/listing rows with deterministic company/facility metadata
  - news listing rows and one news item/detail payload
  - unavailability rows with event/status/capacity/update metadata
- Reuse the existing `agsi_listing_response.json` where possible rather than
  duplicating entity inventory.
- Keep fixtures small and credential-free. Do not include real API keys,
  request headers, or live URLs that expose private account context.
- Ensure fixture records contain enough fields for the assertions in Tasks 1,
  2, and 4.

**Verify:**
- `uv run --extra dev pytest tests/unit/test_gie.py -q`

**Acceptance Criteria:**
- `AGSI-09` has fixture inputs for aggregate, country, company, facility,
  listing, news, and unavailability families.
- Fixtures are deterministic, small, and safe to commit.

## Task 4 - Add Fixture-Backed AGSI Mocked Bronze-To-Silver E2E Tests

**Type:** test
**Files:**
- `tests/integration/test_gie_agsi_mocked_e2e.py`
- `tests/integration/test_gie_agsi_mocked_bronze.py`
- `tests/fixtures/gie/*.json`

**Action:**
- Create `tests/integration/test_gie_agsi_mocked_e2e.py` with:
  - `from __future__ import annotations`
  - imports for `json`, `date`, `datetime`, `UTC`, `Path`, `polars as pl`,
    `pytest`
  - imports for `BronzeWriter`, `RawResponse`, and `get_transformer`
  - `import gridflow.silver.gie  # noqa: F401` before registry access
  - `TARGET_DATE = date(2026, 5, 1)` and deterministic `fetched_at`
  - a `_raw_response(dataset, body, request_params=None, data_date=TARGET_DATE,
    page=1, total_pages=1)` helper that builds `RawResponse` objects with
    `source="gie_agsi"`, `content_type="application/json"`, `api_version="v1"`,
    and no live request dependency
  - a `_silver_path(data_dir, dataset, target_date)` helper matching the
    current `BaseSilverTransformer` output layout unless a transformer
    intentionally uses a reference path
  - an `_assert_bronze_sidecar(path, dataset)` helper checking source, dataset,
    request URL, request params, API version, status, body hash/size, page, and
    total pages
- Add a parametrized fixture-backed test for transformed datasets:
  - `storage_reports`
  - `about_listing`
  - `news`
  - `news_item`
  - `unavailability`
  - include `about_summary` if implemented as a transformer
- For each case:
  - write fixture bytes through `BronzeWriter`
  - assert raw file and `.meta.json` exist
  - call `get_transformer("gie_agsi", dataset, tmp_data_dir)`
  - run `rows = transformer.run(TARGET_DATE)`
  - assert `rows > 0`
  - read parquet with Polars
  - assert expected columns and `data_provider == "gie_agsi"`
- Add a storage-specific test that writes aggregate, country, company, and
  facility storage responses for the same gas day and asserts silver row count
  preserves all scopes and entity levels.
- Add an active coverage test comparing active AGSI config/catalog dataset ids
  to either a registered transformer or a catalog-deferred reason.
- Ensure no test uses `@pytest.mark.live`.

**Verify:**
- `uv run --extra dev pytest tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q`

**Acceptance Criteria:**
- `AGSI-09` fixture-backed bronze-to-silver coverage proves each required AGSI
  family reaches silver or is explicitly deferred.
- `AGSI-10` is strengthened by active-family non-live E2E coverage.

## Task 5 - Run L3 Verification And Preserve L2 Guarantees

**Type:** validation
**Files:**
- `src/gridflow/silver/gie/agsi.py`
- `src/gridflow/silver/gie/__init__.py`
- `src/gridflow/schemas/gie.py`
- `tests/unit/test_gie.py`
- `tests/unit/test_gie_endpoint_catalog.py`
- `tests/integration/test_gie_agsi_mocked_bronze.py`
- `tests/integration/test_gie_agsi_mocked_e2e.py`

**Action:**
- Run:

```powershell
uv run --extra dev ruff check src/gridflow/silver/gie tests/unit/test_gie.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py
uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q
uv run --extra dev pytest -m "not live" -q
```

- If full non-live pytest exposes unrelated pre-existing failures, record them
  in the phase summary with command output and keep the focused L3 suite green.
- Do not run live AGSI calls or require `GIE_API_KEY`.
- Do not mark any new L3 tests as live; L4 owns opt-in live API-to-silver and
  CLI smoke checks.

**Verify:**
- Ruff exits 0.
- Focused pytest exits 0.
- Full non-live pytest exits 0 or unrelated pre-existing failures are recorded
  clearly in the phase summary.

**Acceptance Criteria:**
- L3 is ready for verification with automated non-live coverage for all
  targeted requirements.

</tasks>

<verification>

Run:

```powershell
uv run --extra dev ruff check src/gridflow/silver/gie tests/unit/test_gie.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py
uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q
uv run --extra dev pytest -m "not live" -q
```

</verification>

<success_criteria>

- `AGSI-07`: AGSI storage silver preserves inventory, flow, capacity, fullness,
  status, update, service-announcement, and entity metadata from live-shaped
  storage payloads.
- `AGSI-08`: AGSI listing, news, news item, and unavailability payloads produce
  deterministic silver output or are explicitly catalog-deferred with tested
  reasons.
- `AGSI-09`: Fixture-backed bronze-to-silver tests prove aggregate, country,
  company, facility, listing, news, and unavailability families transform into
  schema-valid silver parquet.
- `AGSI-10`: Non-live tests cover active AGSI endpoint inventory alignment,
  mocked request shapes, pagination by `last_page`, expected-count accounting,
  and representative bronze-to-silver flows.
- Focused and full non-live verification pass without `GIE_API_KEY` or live API
  calls, except any unrelated pre-existing failure recorded in the summary.

</success_criteria>
