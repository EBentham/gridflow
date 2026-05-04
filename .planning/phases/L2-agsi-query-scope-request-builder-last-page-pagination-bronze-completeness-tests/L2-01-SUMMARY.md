---
phase: L2-agsi-query-scope-request-builder-last-page-pagination-bronze-completeness-tests
plan: L2-01
subsystem: gie-agsi
tags: [gie, agsi, bronze, pagination, request-shape, completeness]
requirements-completed:
  - AGSI-02
  - AGSI-04
  - AGSI-05
  - AGSI-06
completed: 2026-05-04
status: completed
---

# L2-01 Summary: AGSI Query-Scope Request Builder And Bronze Completeness

L2 is complete. AGSI bronze fetching now uses the L1 endpoint/query-plan
contract for aggregate, country, company, and facility storage scopes, records
page provenance from `last_page`, and has non-live mocked tests proving exact
request/page/file counts for selected query plans.

## Accomplishments

- Expanded `config/sources.yaml` so `gie_agsi` exposes active AGSI endpoint
  families from the catalog, while retaining the legacy `storage` dataset alias.
- Lowered `gie_agsi.rate_limit_per_second` to 1 request/second to match the
  documented 60 calls/minute limit.
- Updated AGSI query planning so company requests carry `country+company` and
  facility requests carry `country+company+facility`.
- Refactored `GieConnector` so AGSI storage fetches are metadata/query-plan
  driven, support `storage` and `storage_reports`, fetch listing inventory when
  company/facility planning needs it, and partition exact-day bronze by the
  requested gas day.
- Replaced AGSI pagination behavior with `last_page`-based looping and populated
  `RawResponse.page`, `RawResponse.total_pages`, `request_params`, and
  `data_date`.
- Added an exact-day gas-day guard so out-of-window storage rows are rejected
  before they can be written as valid bronze.
- Added `tests/integration/test_gie_agsi_mocked_bronze.py` covering aggregate,
  country, company, facility, pagination, bronze sidecars, and out-of-window
  rejection without live AGSI calls or `GIE_API_KEY`.

## Requirement Coverage

| Requirement | Status | Evidence |
| --- | --- | --- |
| AGSI-02 | Complete | Active catalog rows are asserted against `gie_agsi` source config; config now exposes storage, listing, news, and unavailability families. |
| AGSI-04 | Complete | Mocked tests assert documented `date`, `type`, `country`, `company`, `facility`, `page`, and `size` request params for storage scopes. |
| AGSI-05 | Complete | `test_storage_paginates_with_last_page_not_total` proves two-page fetching from `last_page` and response provenance. |
| AGSI-06 | Complete | Bronze completeness test writes all expected scoped responses through `BronzeWriter` and checks sidecars plus `2026/05/01` partitions; out-of-window rows are rejected. |

## Verification

- `uv run --extra dev ruff check src/gridflow/connectors/gie tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py` - passed.
- `uv run --extra dev pytest tests/unit/test_gie_endpoint_catalog.py tests/unit/test_gie.py tests/integration/test_gie_agsi_mocked_bronze.py -m "not live" -q` - passed, 47 tests.
- `uv run --extra dev pytest -m "not live" -q` - passed, 967 tests, 244 deselected, 1 existing `pythonjsonlogger` deprecation warning.

## Decisions

- `storage` remains a compatibility alias so existing AGSI silver transformer
  registration is not disturbed before L3.
- `storage_reports` is the catalog-aligned dataset id for new AGSI storage
  bronze behavior.
- Exact-day AGSI storage fetches use one `date=YYYY-MM-DD` request per planned
  gas day. Range-style silver preservation and broader E2E flow remain L3/L4
  work.

## Next Phase Readiness

L3 can now build AGSI silver and fixture-backed bronze-to-silver tests on top of
count-checked bronze request provenance, deterministic company/facility query
plans, and `last_page` pagination.

