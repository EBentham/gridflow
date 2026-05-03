---
milestone: v0.3
milestone_name: ENTSO-E Pipeline Validation
status: active
progress:
  phases_total: 9
  phases_complete: 5
  plans_total: 11
  plans_complete: 8
---

## Current Position

Phase: H6 - ENTSO-E transmission and market data sources
Plan: H6-01 next
Status: Ready to execute H6 on top of a passing active ENTSO-E live baseline
Last activity: 2026-05-03 - H5.5 complete; full active ENTSO-E live suite passes with expected no-data skips

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-03)

**Core value:** Every connector reliably fetches real data and every silver transformer
produces schema-valid output — verified end-to-end, not just in unit tests.
**Current focus:** v0.3 ENTSO-E Pipeline Validation

## Accumulated Context

### Decisions

- `all` as a positional dataset argument is a recurring UX confusion — treat it as `--all` rather than erroring
- Live tests must be opt-in (`@pytest.mark.live`) so they don't run in CI without an API key
- H1 implemented the `all` positional alias centrally in `_resolve_datasets`; keep future CLI dataset aliases in the shared helper
- H2 uses mocked `respx` URL-shape tests plus fixture-backed bronze-to-silver runs to validate ENTSO-E without touching the live API
- H3 added a pytest collection gate so live-marked tests only execute when selected with `-m live`, even if local credentials are present
- H3 changed ENTSO-E CLI ingest/transform failure handling to finish attempted datasets, report failed dataset names, and exit non-zero
- H4 treats the official ENTSO-E Postman collection as the endpoint inventory source for URL-shape and missing-source coverage.
- H4 request construction must be endpoint-metadata-driven; ENTSO-E area parameter names are not interchangeable across load, generation, outage, balancing, and transmission endpoints.
- H4-02 uses `docs/entsoe_endpoint_catalog.yaml` as the auditable source of truth for implemented/planned/deferred/excluded ENTSO-E endpoint coverage.
- The first missing-source batch promotes load month/year forecasts and year-ahead forecast margin; generation-unit, transmission, outage, and balancing-extension families remain planned catalog batches.
- H5 adds generation-unit, reservoir, and generation-unit master-data sources, including an A95 reference-date request style.

### Roadmap Evolution

- Phase H4 added: ENTSO-E endpoint catalog + request builder correction.
- H4-01 completed: existing 16 ENTSO-E datasets now use documented endpoint-specific request parameter styles.
- H4-02 completed: official endpoint catalog/gap matrix added, all entries classified, and first load-domain missing source batch implemented.
- H5-H8 planned: remaining ENTSO-E catalog rows are split into generation/reference, transmission/market, outage, and balancing-extension source batches.
- H5-01 completed: generation unit/reference source batch implemented and verified.
- H5.5 inserted before H6 to clean up live all-dataset behavior for sources implemented through H5.
- H5.5 completed: active ENTSO-E live suite passes; A83 activated balancing quantity is deferred for H8/default-control-area strategy.

### Blockers

- Full pytest suite currently fails during collection because `src/gridflow/silver/elexon/__init__.py` imports missing Elexon silver modules such as `agpt`; H1 focused tests pass, but milestone-level gates should address this package import mismatch.
- H3 live verification requires `ENTSOE_API_KEY`; without it, the live suite is implemented but cannot prove real ENTSO-E fetch/bronze/silver/CLI behavior.
- H4 UAT reported a bronze partition/backfill issue caused by missing `RawResponse.data_date`; H5 added regression coverage for ENTSO-E `data_date` and bronze partitioning.
- H5.5 resolved live failures from invalid A83 metadata, zipped outage payloads, live unit/outage tag variants, and fixed-date no-data acknowledgements.

### Todos

(none)
