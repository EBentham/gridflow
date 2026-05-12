# GIE AGSI API Research

**Researched:** 2026-05-04
**Scope:** GIE AGSI gas storage API at `https://agsi.gie.eu`
**Primary docs:** `GIE_API_documentation_v006.pdf` supplied by the user, plus the newer `GIE_API_documentation_v007.pdf`.

## Executive Summary

GIE AGSI is an authenticated JSON REST API for daily European gas storage transparency data. The API requires an `x-key` header, publishes daily gas-day data, and supports both aggregate and entity-level queries. The same documentation family also covers ALSI LNG, but this milestone is scoped to AGSI gas storage.

The current gridflow GIE connector is present but under-validated. It handles a small country list and one `storage` dataset, but it does not yet have an auditable endpoint catalog, full query-scope model, `last_page` pagination, listing-driven company/facility inventory, or the ENTSOG/NESO-style mocked/live bronze-to-silver confidence gates.

## Official Endpoint Families

| Family | Path | AGSI relevance | Notes |
|--------|------|----------------|-------|
| EIC listing, flat | `/api/about?show=listing` | Active | Returns companies and underlying facilities with EICs, countries, API URLs, and facility metadata. Use as the inventory source for company/facility query planning. |
| EIC listing, hierarchy | `/api/about` | Active | Returns hierarchical operator/facility/country metadata. Live probe returned top-level `SSO`. |
| Service announcements | `/api/news` and `/api/news?turl={id}` | Active | Documentation says all AGSI/ALSI news items are available. Live probe returned `data` items with `url`, `start_at`, `end_at`, `title`, `summary`, `details`, and `entities`. |
| Storage facility reports | `/api` | Active | Main AGSI storage endpoint. Supports aggregate and entity-level filtering by `type`, `country`, `company`, `facility`, dates, sort/order/reverse, page, and size. |
| Unavailability reports | `/api/unavailability` | Active with doc ambiguity | v007 says unavailability is not part of API coverage, but the same PDF documents the endpoint and live probe returned paginated data. Implement with explicit catalog note and no-data/error classification. |

## Storage Query Scopes

The `/api` storage endpoint supports several distinct query scopes:

| Scope | Required params | Example |
|-------|-----------------|---------|
| Europe / Non-Europe / Additional Info aggregate | `type=EU`, `type=NE`, or `type=AI` | `/api?type=eu&date=2022-03-31` |
| Country aggregate | `country={CC}` | `/api?country=de&date=2022-03-31` |
| Company aggregate | `country={CC}&company={company_eic}` | `/api?country=de&company=21X000000001160J&date=2022-03-31` |
| Facility | `country={CC}&company={company_eic}&facility={facility_eic}` | `/api?country=de&company=21X000000001160J&facility=21Z000000000271O&date=2022-03-31` |

Date controls:
- `date=YYYY-MM-DD` requests one gas day.
- `from=YYYY-MM-DD&to=YYYY-MM-DD` requests a gas-day range.
- Omitting `from` and `to` returns all available history up to the current gas day.
- Live probe confirmed `country=DE&from=2026-05-01&to=2026-05-02` returns two daily rows for `2026-05-01` and `2026-05-02`.

Pagination:
- Every paginated response must be driven by `last_page`.
- `total` is the count of entries on the current page in GIE docs and live behavior, not the global result count.
- `size` defaults to 30 and is capped at 300.

Rate limiting:
- GIE documents a 60 calls/minute limit and a 60-second wait/queue penalty after excess calls.
- gridflow should configure AGSI around 1 call/second, not the current 5 calls/second.
- Full company/facility live gates must be opt-in and slow by design.

## Response Fields To Preserve

AGSI storage live probe for `type=EU` and `country=DE` on `2026-05-01` returned:

- Top-level: `last_page`, `total`, `dataset`, `gas_day`, `data`
- Data rows: `name`, `code`, `url`, `updatedAt`, `gasDayStart`, `gasDayEnd`, `gasInStorage`, `consumption`, `consumptionFull`, `injection`, `withdrawal`, `netWithdrawal`, `workingGasVolume`, `injectionCapacity`, `withdrawalCapacity`, `contractedCapacity`, `availableCapacity`, `coveredCapacity`, `status`, `trend`, `full`, `info`

The silver layer should preserve current compact fields and add missing live fields rather than discarding them. The most important new fields are `gas_day_end`, `updated_at`, `net_withdrawal_gwh_per_day`, capacity fields, `status`, `entity_level`, entity EIC/code/url metadata, and service-announcement info.

## Local Implementation Gaps

1. `src/gridflow/connectors/gie/client.py` paginates using `total`/`pageSize`; it must use `last_page`.
2. The connector hard-codes a short country list; company and facility queries should be driven by `/api/about?show=listing`.
3. The source config exposes only `storage` and `lng`; AGSI needs dataset/query-scope metadata aligned with an endpoint catalog.
4. The current AGSI transformer preserves only a subset of live AGSI fields.
5. Existing tests are unit-level fixture checks only; there are no AGSI mocked request-shape tests, no live API-to-silver tests, and no live CLI smoke tests.
6. The current `rate_limit_per_second` for GIE is 5, while GIE's documented cap is 60 calls/minute.

## Implementation Guidance

- Build `docs/gie_agsi_endpoint_catalog.yaml` as the audit source for active, deferred, and ambiguous endpoints.
- Introduce `gridflow.connectors.gie.endpoints` metadata similar to ENTSOG/NESO: path, parser family, response key, reference flag, date requirement, default params, and query scope.
- Model AGSI storage as query-scope families, not just one country-only dataset.
- For full entity coverage, use `/api/about?show=listing` to create a request plan. Store expected entity count/page count in provenance metadata where possible.
- Make same-day requests exact: a query for `2026-05-01` must not silently return current-history data, and a range must only write rows with gas days in the requested range.
- For bronze completeness tests, assert expected request count, page count, `data_date`, and row dates. Do not accept "at least one response" as sufficient.
- For live tests, separate representative live smoke from slower full-inventory live gates to stay kind to the API and the developer's day.

## Sources

- GIE API documentation v006: https://www.gie.eu/transparency-platform/GIE_API_documentation_v006.pdf
- GIE API documentation v007: https://www.gie.eu/transparency-platform/GIE_API_documentation_v007.pdf
- AGSI platform: https://agsi.gie.eu
- AGSI data definition page: https://agsi.gie.eu/data-definition

