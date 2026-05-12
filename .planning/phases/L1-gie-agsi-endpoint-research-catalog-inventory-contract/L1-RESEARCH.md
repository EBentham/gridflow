# Phase L1 Research: GIE AGSI Endpoint Catalog And Inventory Contract

**Researched:** 2026-05-04
**Status:** Complete

## Research Complete

Phase L1 should turn the API research into a local contract. The key technical
work is not HTTP fetching yet; it is making AGSI query families explicit enough
that L2/L3 can be implemented without guessing what "all expected data" means.

## What Matters For Planning

- GIE AGSI has a small number of endpoint paths but many query scopes.
- `/api/about?show=listing` is the correct inventory source for company and facility EICs.
- `/api` supports aggregate, country, company, and facility storage report filters.
- `/api/news` and `/api/unavailability` are separate response families from storage reports.
- Pagination must use `last_page`.
- `total` is not reliable as a global total; live response examples make it behave as current-page count.
- The existing code has GIE modules already, so L1 should extend local patterns rather than introduce a new connector style.

## Recommended Local Shapes

1. Add a `GieEndpoint` dataclass or equivalent metadata object in `src/gridflow/connectors/gie/endpoints.py`.
2. Add a `GieParserFamily` or string family enum for `storage`, `listing`, `news`, and `unavailability`.
3. Add a query scope model for `aggregate_type`, `country`, `company`, and `facility`.
4. Add helper functions:
   - `build_storage_params(endpoint, start, end, scope)`
   - `entity_inventory_from_listing(payload)`
   - `build_expected_query_plan(dataset, listing, start, end, scope_limit=None)`
5. Add `docs/gie_agsi_endpoint_catalog.yaml` with `active`, `planned`, `deferred`, and `ambiguous` statuses.
6. Add tests that compare catalog rows to endpoint metadata and prove listing fixtures produce expected request counts.

## Validation Focus

- Catalog coverage: every official AGSI endpoint family has a row.
- Metadata drift: every active catalog row has matching endpoint metadata.
- Query count: fixture listing with N companies and M facilities produces deterministic request counts.
- Date count: exact-day query plans produce one gas-day target, while ranges produce inclusive gas-day lists matching live GIE behavior.

