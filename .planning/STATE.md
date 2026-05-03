---
milestone: v0.4
milestone_name: Elexon Pipeline Validation
status: ready_to_execute
progress:
  phases_total: 4
  phases_complete: 1
  plans_total: 4
  plans_complete: 1
---

## Current Position

Phase: I2 - Elexon mocked request-shape and fixture-backed bronze-to-silver tests
Plan: I2-01 planned
Status: Phase I2 planned; ready to execute
Last activity: 2026-05-03 - Phase I2 planned

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-03)

**Core value:** Every connector reliably fetches real data and every silver transformer
produces schema-valid output - verified end-to-end, not just in unit tests.
**Current focus:** v0.4 Elexon Pipeline Validation

## Accumulated Context

### Decisions

- `all` as a positional dataset argument is a recurring UX confusion - treat it as `--all` rather than erroring.
- Live tests must be opt-in (`@pytest.mark.live`) so they do not run in CI without explicit selection.
- H1 implemented the `all` positional alias centrally in `_resolve_datasets`; keep future CLI dataset aliases in the shared helper.
- H2 uses mocked `respx` URL-shape tests plus fixture-backed bronze-to-silver runs to validate ENTSO-E without touching the live API.
- H3 added a pytest collection gate so live-marked tests only execute when selected with `-m live`, even if local credentials are present.
- H3 changed ENTSO-E CLI ingest/transform failure handling to finish attempted datasets, report failed dataset names, and exit non-zero.
- H4 treats the official ENTSO-E Postman collection as the endpoint inventory source for URL-shape and missing-source coverage.
- H4 request construction must be endpoint-metadata-driven; ENTSO-E area parameter names are not interchangeable across load, generation, outage, balancing, and transmission endpoints.
- H4-02 uses `docs/entsoe_endpoint_catalog.yaml` as the auditable source of truth for implemented/planned/deferred/excluded ENTSO-E endpoint coverage.
- The first missing-source batch promotes load month/year forecasts and year-ahead forecast margin; generation-unit, transmission, outage, and balancing-extension families remain planned catalog batches.
- H5 adds generation-unit, reservoir, and generation-unit master-data sources, including an A95 reference-date request style.
- H6 adds transmission/market source rows through metadata-driven request construction, shared zone-pair quantity/amount transformers, and exact-cased optional filter forwarding.
- H6 defers `flow_based_allocations` because B09 allocation documents need dedicated parser/schema review rather than the generic TimeSeries transformer path.
- H7 adds primary consumption, transmission, offshore-grid, and production outage sources with document/status/asset metadata preserved in silver output.
- H7 keeps transmission net-position impact, transmission available capacity, and fallback outage variants deferred because they need separate interpretation/schema passes beyond the primary outage rows.
- H8 adds balancing state, bid, aggregated bid, procured capacity, cross-zonal capacity, and financial balancing sources with H8-specific metadata preserved in silver output.
- H8 keeps balancing archive variants, SO GL, and implementation-framework balancing extensions deferred with H9/backlog reasons.
- I1 planned: Elexon inventory contract, explicit exclusions, request-style baseline, and live-test diagnostics scope.
- I1 completed: active Elexon config, endpoint registry, and silver transformer registrations are covered by tests; excluded endpoints have explicit reasons.

### Roadmap Evolution

- Phase H4 added: ENTSO-E endpoint catalog + request builder correction.
- H4-01 completed: existing 16 ENTSO-E datasets now use documented endpoint-specific request parameter styles.
- H4-02 completed: official endpoint catalog/gap matrix added, all entries classified, and first load-domain missing source batch implemented.
- H5-H8 planned: remaining ENTSO-E catalog rows are split into generation/reference, transmission/market, outage, and balancing-extension source batches.
- H5-01 completed: generation unit/reference source batch implemented and verified.
- H5.5 inserted before H6 to clean up live all-dataset behavior for sources implemented through H5.
- H5.5 completed: active ENTSO-E live suite passes; A83 activated balancing quantity is deferred for H8/default-control-area strategy.
- H6 completed: 16 transmission/market datasets implemented and live request-shape probes pass for representative H6 families.
- H7 completed: primary outage datasets implemented, endpoint catalog synchronized, and H7 live request-shape probes pass.
- H8 completed: six balancing-extension datasets implemented, endpoint catalog synchronized, and H8 live request-shape probes pass.
- v0.4 started: Elexon validation will mirror the ENTSO-E testing shape while accounting for Elexon's public JSON API, no-key auth model, and distinct parameter styles.
- I1 planned: first execution plan created for inventory alignment and live-test scaffolding.
- I1 completed: inventory contract tests, explicit exclusions, BOALF config alignment, and Elexon live diagnostics are in place.
- I2 planned: mocked request-shape tests and fixture-backed bronze-to-silver integration tests will cover active Elexon datasets without live network access.

### Blockers

- H3 live verification record remains human-needed for the original all-dataset credentialed run; later H5.5/H8 live gates passed and this is acknowledged as deferred at v0.3 close.
- H5.5 resolved live failures from invalid A83 metadata, zipped outage payloads, live unit/outage tag variants, and fixed-date no-data acknowledgements.

## Deferred Items

Items acknowledged and deferred at milestone close on 2026-05-03:

| Category | Item | Status |
|----------|------|--------|
| debug | entsoe-live-suite-h5.5 | unknown |
| uat | H3-live-entsoe-test-suite/H3-HUMAN-UAT.md | partial; 1 pending scenario |
| uat | H4-entsoe-endpoint-catalog-request-builder/H4-UAT.md | diagnosed; 0 pending scenarios |
| verification | H3-live-entsoe-test-suite/H3-VERIFICATION.md | human_needed |

### Todos

(none)
