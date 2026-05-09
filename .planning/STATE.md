---
milestone: v0.10
milestone_name: V1 Vendor Bug-fix Follow-ups
status: in_planning
progress:
  phases_total: 1
  phases_complete: 0
  plans_total: 6
  plans_complete: 0
---

## Current Position

Phase V2 plans authored on `claude/lucid-mccarthy-9ed3e0` (worktree),
2026-05-09. V2 is the bug-fix follow-up to V1: production code fixes
for the Implementation deltas surfaced (but explicitly out of scope)
during V1 live validation.

V2 ships 6 plans across 3 waves:

- **Wave 1 (HIGH severity, parallel):**
  - `V2-PLAN-A-elexon-freq-fix` — override `from_param` /
    `to_param` on `ENDPOINTS["freq"]` to `measurementDateTimeFrom` /
    `measurementDateTimeTo`. Without the fix, the API silently
    ignores the wrong-named params and returns latest 5761 samples
    regardless of window.
  - `V2-PLAN-B-neso-region-period-fields` — patch
    `silver/neso/carbon_intensity.py::_rows_from_region_period` to
    read `intensity` and `generationmix` from whichever of `region`
    or `period` holds the data. Affects 5 datasets:
    `regional_current`, `regional_intensity_fw24h`,
    `regional_intensity_fw48h`, `regional_intensity_pt24h`,
    `regional_intensity`.

- **Wave 2 (MED + LOW severity, parallel):**
  - `V2-PLAN-C-elexon-misc` — set `max_chunk_hours=23` on
    `remit` / `soso` to honour the undocumented vendor 1-day cap;
    expand `ElexonSystemPrice.run_type` regex (and enum) to admit the
    live-observed `priceDerivationCode = "N"` after live-confirming
    the value list.
  - `V2-PLAN-D-entsoe-cleanup` — ADR-019 to drop
    `commercial_schedules_net_positions` (registry duplicate of
    `commercial_schedules`); B2 cleanup batch (A37/A15 pagination,
    A87 schedule cadence + silver `Reason.code`, `area_name`,
    `psrType`, `DEFAULT_ZONES`).
  - `V2-PLAN-E-entsog-and-ngeso` — short-circuit
    HTTP 404 + body `{"message":"No result found"}` in
    `EntsogConnector._request` so `@RETRY_POLICY` does not waste
    retry budget on the documented empty convention; triage
    (default: delete) the empty `connectors/ngeso/` placeholder.

- **Wave 3 (close-out aggregator):**
  - `V2-PLAN-F-aggregate` — `V2-VALIDATION.md`,
    re-validation-row sanity checks against V1 per-vendor reports,
    STATE + ROADMAP updates, backlog absorption.

V1 already shipped 5 commits ahead of master on this worktree
branch and has not been merged. V2 commits build on top with
`fix(V2-X):` prefixes.

The Avast `curl --ssl-no-revoke` workaround locked in V1-CONTEXT.md
applies to every V2 live re-validation curl.

## Prior milestone

Phase V1 complete on `claude/lucid-mccarthy-9ed3e0` (worktree).

V1 validated 156 active gridflow datasets across 6 vendors. All vendors
have full vault documentation (`quant-vault/30-vendors/<vendor>/`):
per-dataset pages, endpoints.md quick-summary, refreshed README, plus
per-vendor VALIDATION reports under
`.planning/phases/V1-vault-vendor-validation-and-docs/`.

Aggregate live-call results:
- **113 PASS / 43 EMPTY / 0 FAIL** across 156 datasets
- Elexon 33/0/0, ENTSOE 9/39/0, ENTSOG 29/4/0, GIE 7/0/0, NESO 33/0/0,
  Open-Meteo 2/0/0
- All 39 ENTSOE EMPTY results stem from GB post-Brexit data drop;
  request shapes verified correct via DE-LU/FR/NL fallback calls
- 4 ENTSOG EMPTY are sparse-at-test-point only — all indicators
  verified correct via 30-day or no-filter retry

Production code bugs surfaced (NOT fixed in V1 — out of scope, recorded
in vault Implementation deltas + per-vendor VALIDATION reports for
follow-up phases):
- Elexon `freq` connector parameter-name mismatch (HIGH — silently
  ignored by API, returns wrong window)
- NESO silver `_rows_from_region_period` reads carbon/mix from wrong
  level — affects 5 period-keyed regional datasets
- Elexon REMIT/SOSO 1-day vendor cap not honoured by connector default
- Elexon `system_prices.priceDerivationCode` Pydantic regex too narrow
- ENTSOE `commercial_schedules` and `commercial_schedules_net_positions`
  use IDENTICAL EntsoeDocType (registry duplication)

Worktree-machine-specific workaround: Avast antivirus TLS interception
breaks Python httpx with cert verify failures; `curl --ssl-no-revoke`
is the live-call mechanism (locked in V1-CONTEXT.md, used by all V1
agents). Documented for future runs.

Last activity: 2026-05-08 — V1 phase shipped.

## Earlier milestones — F0 / F7 (v0.8 and earlier)

Phase: F0 complete; F7 Workstream A complete on `feat/f7-stack-and-bitemporal`
Plan: F7-PLAN Stack Model Data Infrastructure (Workstream A — gridflow side)
Status: F7-A delivers `APPEND_ONLY` flag on `BaseSilverTransformer`, run-suffixed
filenames keyed off `available_at` (idempotent re-ingest), REMIT revision
preservation (`DATASET_VERSION` 2.0.0), `DATASET_VERSION` / `APPEND_ONLY`
attributes on REMIT/FOU2T14D/BMUnits/InstalledCapacityUnits, plus
ADR-017 / ADR-018 in `docs/DECISION_LOG/`. The eleven new F7 tests pass
under `uv run pytest`; the wider repo suite is green (1025 passed,
253 pre-existing skips).
F7-REINGEST-01 deferred to the user — bronze tree empty locally, the four
re-ingest commands are documented in
`.planning/phases/F7-stack-model-data-infrastructure/F7-A-RESULTS.md`.
Once F0 merges to master, this branch should rebase before merge.
Last activity: 2026-05-07 - F7-A implemented; gridflow_models Workstream B
also complete; verifier returned PASS_WITH_DEFERRALS.

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-05)

**Core value:** Every connector reliably fetches real data and every silver transformer
produces schema-valid output - verified end-to-end, not just in unit tests.
**Current focus:** F0 complete; next decision is whether to begin `gridflow_models` F1 or first run historical reingest with available bronze

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
- GIE AGSI v0.7 is scoped to gas storage at `https://agsi.gie.eu`; ALSI LNG is deferred.
- GIE AGSI requires `GIE_API_KEY` sent as lowercase `x-key` header.
- GIE AGSI `last_page` is the pagination source of truth; `total` is per-page row count and must not be used as global total.
- GIE AGSI company/facility expected-count planning must derive from `/api/about?show=listing`.
- GIE AGSI live tests must respect 60 calls/minute and keep full-inventory checks opt-in.
- L1 exposes exact-date and range storage query planning helpers so bronze completeness tests can know the expected gas-day rows before L2 changes runtime fetching.
- GIE AGSI news and unavailability are active catalog endpoint families, but storage pipeline implementation remains phased: storage bronze in L2, silver/mocked E2E in L3, live/CLI in L4.
- L2 completed: AGSI source config now exposes active catalog families, storage fetching is query-plan driven for aggregate/country/company/facility scopes, pagination uses `last_page`, and mocked bronze completeness tests prove exact-day request/page/file counts.
- L3 completed: AGSI storage silver now preserves live-shaped fields across entity scopes, active listing/news/unavailability families have registered deterministic transformers, and fixture-backed non-live E2E proves bronze payloads reach silver parquet.
- L4 completed: credentialed AGSI live tests prove representative aggregate, country, company, and facility storage responses flow through bronze into silver; isolated CLI smoke tests prove pipeline, ingest/transform, and backfill paths.
- v0.8 planning started: `gridflow_models` needs point-in-time silver data before model scaffolding begins.
- F0 should use repo-actual source/dataset names, especially `open_meteo/historical`.
- F0 has an explicit issue-time discussion because `elexon/ndf` and `elexon/windfor` are forecast-style datasets even though the supplied F0 text mostly describes four base bitemporal columns.
- F0 completed: silver outputs now stamp `event_time`, `available_at`, `source_run_id`, and `dataset_version`; `elexon/ndf` and `elexon/windfor` preserve `issue_time`.
- F0 reingest support is implemented and tested, but this workspace has no `data/bronze/` partitions for the historical broad re-transform.

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
- v0.7 started: GIE AGSI Gas Storage Validation will add endpoint catalog, query-scope metadata, `last_page` pagination, expected-count bronze tests, silver preservation, opt-in live API-to-silver tests, and CLI smoke coverage.
- L1-L4 planned: research/inventory, bronze request semantics, silver/mocked E2E, and live/CLI close-out.
- L1 completed: `docs/gie_agsi_endpoint_catalog.yaml`, GIE endpoint metadata, listing inventory fixture, and query planning tests now cover AGSI endpoint families and listing-derived expected counts.
- L3 completed: one non-live plan expanded AGSI storage silver preservation, added listing/news/unavailability fixture-backed silver coverage, and kept L2 mocked bronze request/count guarantees green.
- L4 completed: opt-in live API-to-silver and CLI smoke tests passed, with full-inventory AGSI validation remaining an explicit slow gate and ALSI LNG deferred to backlog.
- v0.8 started: Fundamentals Model Silver Foundations will add bitemporal-lite silver lineage for first demand-forecast modelling datasets.
- F0 planned: one phase covers BaseSilverTransformer bitemporal injection, run-id propagation, re-ingest sidecar timestamps, five-dataset re-transform, tests, DuckDB sanity checks, and `F0-RESULTS.md`.
- F0 completed: bitemporal injection, run-id propagation, reingest sidecar lookup, issue-time preservation, static/reference event-time fallback, DuckDB verification, and close-out docs are complete.

### Blockers

- H3 live verification record remains human-needed for the original all-dataset credentialed run; later H5.5/H8 live gates passed and this is acknowledged as deferred at v0.3 close.
- H5.5 resolved live failures from invalid A83 metadata, zipped outage payloads, live unit/outage tag variants, and fixed-date no-data acknowledgements.
- No new v0.4 blockers remain open at milestone close.
- v0.5 has no open implementation blockers. `gsd-sdk` is unavailable in this runtime, so STATE.md was updated directly rather than through `state.milestone-switch`.
- ENTSOG live tests classify seven documented no-data/empty API outcomes as skips for the 2024-01-15 test window; this is expected source behavior rather than an implementation blocker.
- No v0.5 milestone audit file exists because `gsd-sdk` is unavailable in this runtime; milestone close proceeded based on passing non-live, live, CLI smoke, and targeted backfill verification.
- F0 has no open implementation blockers. Historical broad re-transform remains blocked in this workspace by absent `data/bronze/` partitions and is documented as a caveat rather than an implementation failure.

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
| follow_up | GIE ALSI LNG connector validation | deferred while v0.7 focuses AGSI gas storage |
| follow_up | F0 historical broad re-transform | run documented `--reingest` commands when bronze partitions are available |

### Todos

(none)
