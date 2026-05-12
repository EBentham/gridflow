# Phase L2 Research: AGSI Query Scope Request Builder And Bronze Completeness

**Researched:** 2026-05-04
**Status:** Complete

## Research Complete

Phase L2 should wire the L1 AGSI endpoint contract into runtime bronze
fetching. The current connector still behaves like the pre-L1 implementation:
it iterates a hard-coded country list, sends `from` plus `till`, and decides
pagination from `total`/`pageSize`. That conflicts with the AGSI documentation
and L1 research, where storage requests must use documented `date` or
`from`/`to` controls and pagination must be driven by `last_page`.

## What Matters For Planning

- `src/gridflow/connectors/gie/endpoints.py` already exposes endpoint metadata,
  query scopes, gas-day planning helpers, and a listing parser.
- `docs/gie_agsi_endpoint_catalog.yaml` marks `storage_reports`,
  `about_summary`, `about_listing`, `news`, `news_item`, and
  `unavailability` as active AGSI endpoint families.
- `config/sources.yaml` still exposes only `storage` and uses a 5 requests/sec
  rate limit, while GIE documents 60 calls/minute.
- The existing `GieConnector` only fetches countries. L2 must support aggregate
  type, country, company, and facility storage scopes.
- Company/facility storage request planning must come from
  `/api/about?show=listing`; tests can supply the L1 fixture, while runtime can
  fetch listing metadata when needed.
- Bronze provenance already has useful fields: `request_params`, `page`,
  `total_pages`, and `data_date`. L2 should populate them rather than creating
  a new provenance format.

## Implementation Guidance

1. Keep `storage` as a legacy alias for the storage report dataset if local
   silver code still uses it, but make `storage_reports` the catalog-aligned
   dataset id for new AGSI bronze fetching.
2. Align `config/sources.yaml`, `ENDPOINTS`, and `GieConnector.list_datasets()`
   for active AGSI endpoint families.
3. Build request specs from L1 query-planning helpers instead of rebuilding
   scope/date logic inside `client.py`.
4. For exact-day and range calls, prefer exact daily requests using `date` so
   every `RawResponse.data_date` partitions to the gas day being requested.
5. Include `page` and `size` on every paginated request and loop until
   `page >= last_page`.
6. Treat `total` as current-page row count only. Do not use it for loop exit.
7. Add a mocked integration test module with `respx` that covers aggregate,
   country, company, and facility request shapes, including a two-page
   `last_page` response.
8. Add bronze completeness assertions using `BronzeWriter` to prove file counts,
   metadata sidecars, request/page provenance, and data-date partitions match
   the expected query plan.

## Validation Architecture

Use non-live pytest coverage only. The fast feedback target is a focused GIE
suite:

```powershell
uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py -m "not live" -q
```

The full pre-execution gate should add linting and the broader non-live suite:

```powershell
uv run --extra dev ruff check src/gridflow/connectors/gie tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py
uv run --extra dev pytest -m "not live" -q
```

## Risks To Carry Into The Plan

- API key leakage: mocked tests must not use `GIE_API_KEY`.
- Live API overuse: L2 must not add live tests.
- Dataset-id churn: keep compatibility with the existing `storage` transformer
  path until L3 deliberately revisits silver datasets.
- False completeness: tests must assert exact expected request/page counts, not
  just "at least one response".
- Out-of-window bronze: exact-day mocked responses must prove only the requested
  gas day is returned and partitioned.

