---
milestone: v0.7
milestone_name: GIE AGSI Gas Storage Validation
status: planning
progress:
  phases_total: 4
  phases_complete: 0
  plans_total: 1
  plans_complete: 0
---

## Current Position

Phase: L1 ready for planning
Plan: GIE AGSI endpoint research, catalog, inventory contract, and expected-count model
Status: Planning GIE AGSI Gas Storage Validation
Last activity: 2026-05-04 - Started v0.7 GIE AGSI gas storage planning from official API docs and live API probes

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-04)

**Core value:** Every connector reliably fetches real data and every silver transformer
produces schema-valid output - verified end-to-end, not just in unit tests.
**Current focus:** v0.7 GIE AGSI Gas Storage Validation

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
- I2 completed: mocked request-shape coverage now spans every active configured Elexon dataset, while fixture-backed bronze-to-silver tests validate representative transformer families without live network access.
- I4 completed: live Elexon CLI smoke tests cover `pipeline`, separate `ingest`/`transform`, and `backfill` for curated datasets under temp-root `GRIDFLOW_*` paths.
- Runtime `GRIDFLOW_DATA_DIR`, `GRIDFLOW_DUCKDB_PATH`, and `GRIDFLOW_LOG_DIR` overrides must take precedence over YAML pipeline paths so live smoke tests and manual checks can isolate local outputs.
- ENTSOG operational requests must use exact-case `/operationalData`, `timeZone=UCT`, exact-case indicators, and `pointDirection` filters built from operator key + point key + direction key.
- ENTSOG generic silver parsing must tolerate API placeholders such as empty strings, `N/A`, and human-formatted last-update timestamps by producing null datetimes rather than failing the whole transform.
- ENTSOG generic silver parsing must coalesce same-normalized-name fields such as `isCAMRelevant` and `isCamRelevant` into one canonical snake_case column.
- NESO Carbon Intensity API is public, JSON-only, and path-parameter based; all 33 documented route variants are active v0.6 datasets.
- NESO regional payloads appear in both period-with-`regions` and region-with-`data` shapes; silver parsing must preserve intensity and nested generation mix in long rows.
- NESO `/generation` returns a single `data` object while generation range routes return `data` arrays; both are normalised to the same long generation-mix schema.
- NESO `intensity_period` must fan out each requested settlement date across
  all valid GB settlement periods: 48 on normal days, 46 on spring DST transition
  dates, and 50 on autumn DST transition dates.
- NESO `{from}/{to}` range endpoints must expand same-day CLI/API requests to a
  one-day API window; zero-length range URLs return 400 and produce empty bronze.

### Quick Tasks Completed

| Date | Task | Result |
|------|------|--------|
| 2026-05-04 | NESO settlement-period iteration | `intensity_period` now requests every valid GB settlement period per requested date and writes the full response set through bronze and silver. |

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
- I2 completed: non-live mocked Elexon E2E suite passes with bronze metadata, pagination/chunking, no-param, and representative silver output assertions.
- I3 planned: opt-in live Elexon API-to-silver tests will call representative public no-key Elexon datasets, write live responses to temp bronze, transform to silver, and classify empty/deferred outcomes explicitly.
- I3 completed: opt-in live tests now prove `system_prices`, `boal`, `freq`, `pn`, and `bmunits_reference` can flow from the public Elexon API through temp bronze into silver parquet.
- I4 completed: live CLI/backfill smoke tests now prove `system_prices`, `freq`, and `bmunits_reference` flow through user-facing commands without polluting normal project data on verified runs.
- v0.4 completed: Elexon validation is archived with 4 phases, 4 plans, and 16/16 requirements complete.
- v0.5 started: ENTSOG validation will mirror the ENTSO-E/Elexon confidence pattern while accounting for the gas Transparency Platform's public JSON API, exact-case indicators, mandatory point-direction filters, and no-data responses.
- J1-J4 completed: ENTSOG endpoint research/catalog, metadata-driven bronze requests, generic and specialised silver transformers, mocked E2E tests, opt-in live API-to-silver tests, and isolated CLI smoke tests are implemented.
- v0.5 completed: ENTSOG Pipeline Validation shipped and archived on 2026-05-04.
- v0.6 shipped: NESO Carbon Intensity Platform extends the existing single national intensity route to all documented national, statistics, generation, factors, and regional endpoints.
- K1-K4 shipped: 33 NESO route variants implemented with endpoint catalog, source config, metadata-driven connector paths, family-aware silver transforms, mocked all-dataset E2E tests, opt-in live API-to-silver tests, and CLI smoke coverage.

### Blockers

- H3 live verification record remains human-needed for the original all-dataset credentialed run; later H5.5/H8 live gates passed and this is acknowledged as deferred at v0.3 close.
- H5.5 resolved live failures from invalid A83 metadata, zipped outage payloads, live unit/outage tag variants, and fixed-date no-data acknowledgements.
- No new v0.4 blockers remain open at milestone close.
- v0.5 has no open implementation blockers. `gsd-sdk` is unavailable in this runtime, so STATE.md was updated directly rather than through `state.milestone-switch`.
- ENTSOG live tests classify seven documented no-data/empty API outcomes as skips for the 2024-01-15 test window; this is expected source behavior rather than an implementation blocker.
- No v0.5 milestone audit file exists because `gsd-sdk` is unavailable in this runtime; milestone close proceeded based on passing non-live, live, CLI smoke, and targeted backfill verification.

## Deferred Items

Items acknowledged and deferred at milestone close on 2026-05-03:

| Category | Item | Status |
|----------|------|--------|
| debug | entsoe-live-suite-h5.5 | unknown |
| uat | H3-live-entsoe-test-suite/H3-HUMAN-UAT.md | partial; 1 pending scenario |
| uat | H4-entsoe-endpoint-catalog-request-builder/H4-UAT.md | diagnosed; 0 pending scenarios |
| verification | H3-live-entsoe-test-suite/H3-VERIFICATION.md | human_needed |

Items acknowledged and deferred at milestone close on 2026-05-04:

| Category | Item | Status |
|----------|------|--------|
| follow_up | ENTSOG domain-specific typed silver schemas | deferred until downstream gas gold consumers need them |
| follow_up | Scheduled live endpoint monitoring | future cross-source decision |
| follow_up | GIE AGSI/ALSI connector validation | next connector-confidence candidate |

### Todos

(none)
