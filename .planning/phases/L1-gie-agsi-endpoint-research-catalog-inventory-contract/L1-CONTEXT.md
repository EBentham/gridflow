# Phase L1: GIE AGSI Endpoint Research, Catalog, Inventory Contract, And Expected-Count Model - Context

**Gathered:** 2026-05-04
**Status:** Ready for planning
**Source:** User request plus inline API/codebase research

<domain>
## Phase Boundary

This phase prepares AGSI implementation by making the API surface auditable and
testable. It does not need to rewrite the connector fetch loop or silver
transformers yet; those belong to L2/L3. L1 should leave the repo with a clear
endpoint catalog, code metadata, listing fixture strategy, and tests that can
catch catalog/metadata drift.

</domain>

<decisions>
## Implementation Decisions

### Locked By User Request
- Build the GIE gas storage datasource using the GSD workflow before implementation.
- Research the official API thoroughly, including endpoint families and query parameters.
- Plan for full end-to-end testing, including opt-in live API tests.
- Bronze completeness must be checked: if a user queries a date such as 1 May, all expected responses for that query must be fetched and preserved into silver.

### API Decisions
- Use the newer `GIE_API_documentation_v007.pdf` as the planning contract while preserving the user-supplied v006 link as a source reference.
- Scope v0.7 to AGSI gas storage at `https://agsi.gie.eu`; ALSI LNG is deferred.
- Use `/api/about?show=listing` as the inventory source for companies and facilities.
- Treat `/api/unavailability` as active but catalog its documentation ambiguity because v007 both says unavailability is not API coverage and documents/live-serves the endpoint.
- Use `last_page` for pagination; do not use `total` as a global result count.
- Keep live full-inventory checks opt-in and slow because GIE documents a 60 calls/minute limit.

### the agent's Discretion
- Exact names for new dataset identifiers may follow local naming conventions, as long as catalog, endpoint metadata, source config, and tests agree.
- L1 may add fixture files for listing/unavailability/news if that makes the inventory contract deterministic.

</decisions>

<canonical_refs>
## Canonical References

### GIE API
- `.planning/research/GIE-AGSI-API-RESEARCH.md` - API research summary and local gap analysis.
- `https://www.gie.eu/transparency-platform/GIE_API_documentation_v006.pdf` - user-supplied documentation.
- `https://www.gie.eu/transparency-platform/GIE_API_documentation_v007.pdf` - newer API documentation used for planning.

### Local Patterns
- `src/gridflow/connectors/gie/client.py` - current minimal GIE connector.
- `src/gridflow/connectors/gie/endpoints.py` - current GIE constants.
- `src/gridflow/connectors/entsog/endpoints.py` - endpoint metadata pattern for a gas datasource.
- `docs/entsog_endpoint_catalog.yaml` - catalog pattern for active/deferred endpoint coverage.
- `docs/neso_endpoint_catalog.yaml` - route catalog pattern for a public JSON API.
- `tests/unit/test_entsog.py` and `tests/unit/test_neso_endpoints.py` - inventory and endpoint metadata test patterns.

</canonical_refs>

<specifics>
## Specific Ideas

- Catalog endpoint families: `/api`, `/api/about`, `/api/about?show=listing`, `/api/news`, `/api/news?turl={id}`, `/api/unavailability`.
- Catalog storage scopes: `type=EU/NE/AI`, `country`, `country+company`, `country+company+facility`.
- Add an expected-query-plan helper that can say how many requests/pages should be generated before any HTTP calls are made.
- Use listing fixtures to cover full company/facility inventory without live API rate pressure.

</specifics>

<deferred>
## Deferred Ideas

- ALSI LNG validation.
- Scheduled external live monitoring outside pytest.
- Gold-layer gas storage modelling views.

</deferred>

---
*Phase: L1-gie-agsi-endpoint-research-catalog-inventory-contract*
*Context gathered: 2026-05-04 via inline GSD planning*

