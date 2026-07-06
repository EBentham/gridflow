# Changelog

All notable changes to **gridflow** — a local-first Python data pipeline for UK/EU power and gas
market data — are documented in this file. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Version numbers correspond to project
milestones (not SemVer), dated by milestone completion. v0.13 (a planned dataset-coverage
expansion) was paused before release and is intentionally absent.

## [v0.16] - 2026-06-07

**Codebase Hardening** — internal tech-debt milestone: remediated 31 of 32 open items from the
June 2026 codebase re-audit across security, pipeline correctness, read-path performance, and
architecture (PRs #25–#29). Test suite grew from 1387 to 1530 passing; mypy is now strict-clean
(52 errors → 0).

### Added
- Opt-in incremental ingest (`--incremental`) driven by monotonic per-dataset watermarks, with
  non-rewinding backfill (PR #26).
- `prune` retention command; source-qualified DuckDB views (`silver_{source}_{dataset}`) with
  backward-compatible aliases for the old single-token names (PR #28).

### Changed
- CLI and scripts now drive one shared pipeline runner; CLI behaviour verified byte-identical via
  19 golden tests (PR #28).
- Gold builds and quality checks read silver via lazy, partition-pruned Parquet scans instead of
  loading whole dataset trees; silver CSV output is now opt-in (PR #27).

### Fixed
- Security: API keys/tokens sanitised out of bronze metadata, hardened XML parsing at all four
  ENTSO-E parse sites, path-containment guards (plus `--dry-run`) on `reset`, and injection guards
  on CSV export (PR #25).
- Partial ingest runs now finish as `completed_with_warnings` instead of appearing fully
  successful; import, telemetry, and reporter failures are surfaced (PR #26).
- Latent gold-build crash across a silver schema boundary; Parquet readers now tolerate
  mixed-width silver files (null-filling missing columns) and skip in-flight temp files
  (PRs #27, #29).
- Serving-SDK `get_gas_storage`/`get_imbalance_context` were broken against any real catalogue —
  found and fixed by a new operational test sweep (PR #28).

## [v0.15] - 2026-06-04

**Vendor-Truth Cleanup** — closed the items deferred by the v0.14 vendor-truth audit; the headline
was a confirmed silent solar-irradiance data bug. 1387 tests passing.

### Fixed
- Open-Meteo solar global-tilted-irradiance (GTI) bug: the connector requested north-facing panels
  (`azimuth=180`, Open-Meteo convention `0=S`) instead of south-facing, understating tilted
  irradiance by roughly half at peak. Fixed with a regression test; historical solar data
  re-ingested and verified south-facing (PR #22).
- Elexon DISBSAD field drift: the live API now sends `storFlag` where only `storProviderFlag` was
  mapped, so the flag was silently always false. Both spellings mapped, cast fix for `id`, and
  fixtures refreshed from a live capture (PR #23).
- ENTSO-E congestion-management-costs (A92) requests corrected to single-zone parameters after a
  live probe showed zone-pair queries are rejected (PR #24).

### Changed
- ENTSO-G gas-quality content indicators un-marked as "removed" in the docs — live probes showed
  the endpoints are recognised but sparse; documentation corrected across all three doc layers.

## [v0.14] - 2026-06-03

**Vendor-Truth Remediation** — closed the write-path-guarantee and documentation-truth gaps from
the 2026-05-31 vendor-truth audit (88 findings across 13 units). +51 tests (1334 → 1385 passing).

### Added
- Fail-soft Pydantic schema validation on every silver write: failures are logged, counted, and
  surfaced as `completed_with_warnings` — never raised, never silently dropped. 123 datasets wired
  (PR #17).
- Dedicated contract schema for Elexon fuel-mix (FUELINST) and `currency` labels on imbalance and
  activated-balancing prices (PR #21).

### Fixed
- Elexon `market_depth` column mapping (priced-accepted volumes) (PRs #18/#19).
- ENTSO-E generation-type (PSR) labels corrected: B16 Solar, B18 Wind Offshore, B19 Wind Onshore.
- ENTSO-G tariffs/tariff-simulations bronze-read date-window bug; GIE LNG `dtrs` field
  conservatively relabelled with raw fields preserved and derived `lng_pct_full` added.

### Changed
- Every actioned vault/doc statement made true against official vendor docs and live code,
  propagated across gridflow, the documentation vault, and the front-end mirror (PRs #20/#21).

## [v0.12] - 2026-05-23

**Schema Drift Cleanup** — schema-drift cleanup and post-audit polish, shipped as one batch
(PR #8).

### Fixed
- Recovered the `published_at` column across 12 Elexon silver transformers.
- Fixed `lolpdrm` (loss-of-load probability / de-rated margin) request chunking and closed a
  pre-existing failing test.

### Changed
- Cleared a five-item ENTSO-E backlog: result pagination beyond the first page, imbalance
  `reason_code` exposure, human-readable `area_name` population, calendar-correct monthly/yearly
  resolutions, and wider reserve-type coverage for activated balancing prices.
- Propagated the new Elexon schema deltas to the documentation vault and front-end mirror.

## [v0.11] - 2026-05-09

**Open-Meteo Renewable Extension** — extended the Open-Meteo connector and silver layer into
role-specific demand/wind/solar dataset families for renewable forecasting. +74 tests
(1116 passing).

### Added
- Role-split datasets with role-specific location lists: demand (7 UK population centres), wind
  (12 capacity-weighted GB sites), solar (6 GB sites).
- Hub-height wind at 10 m + 100 m (ERA5 archive-verified), full irradiance components
  (GHI/DNI/DHI/GTI), cloud-cover decomposition, and snow variables.
- Derived air density from the ideal-gas law, property-tested over 18 temperature/pressure
  combinations.

### Changed
- Datasets renamed `historical`/`forecast` → `historical_demand`/`forecast_demand`; silver
  dataset version bumped to 2.0.0. A migration sweep caught two stale references.

## [v0.10] - 2026-05-09

**Vendor Bug-Fix Follow-ups** — fixed the production bugs surfaced (but not patched) by the v0.9
live validation, across Elexon, NESO, ENTSO-E, and ENTSO-G; every fixed dataset re-validated live.

### Fixed
- Elexon `freq` used the wrong window parameter names, returning the "latest 5761 samples" instead
  of the requested time window.
- NESO regional carbon-intensity and generation-mix fields were empty for five period-keyed
  regional datasets (values read from the wrong payload level).
- Elexon REMIT/SOSO request chunking capped to honour the vendor's undocumented 1-day limit;
  `system_prices` accepts the live-observed `N` settlement-run type.
- ENTSO-E commercial-schedules (A09) registry duplication resolved; ENTSO-G no longer wastes retry
  budget on the API's documented "no result found" 404 convention.

## [v0.9] - 2026-05-08

**Vault Vendor Validation & Docs** — live-validated every active endpoint (156 datasets across six
vendors) against official vendor documentation and populated the documentation vault.

### Added
- An authoritative vault page per active dataset, plus per-vendor endpoint summary tables, README
  auth/rate-limit/gotcha entries, and PASS/EMPTY/FAIL validation reports with evidence.
- Validation results: Elexon 33 PASS; ENTSO-G 29 PASS / 4 EMPTY; GIE 7 PASS; NESO 33 PASS;
  Open-Meteo 2 PASS; ENTSO-E 9 PASS / 39 EMPTY (GB data gaps post-Brexit, verified via EU fallback
  zones); zero FAILs.
- Every connector endpoint URL checked verbatim against official docs, with discrepancies recorded
  as explicit implementation deltas rather than silently resolved.

## [v0.8] - 2026-05-05

**Fundamentals Model Silver Foundations** — point-in-time (bitemporal) lineage for silver outputs,
groundwork for downstream forecasting models.

### Added
- Bitemporal columns injected by the base silver transformer: `event_time`, `available_at`,
  `source_run_id`, `dataset_version`.
- Transform run-id propagation through `transform`, `pipeline`, `backfill`, and the pipeline
  scripts.
- A re-ingest path that reconstructs historical `available_at` from bronze sidecar timestamps for
  five datasets; forecast datasets (`ndf`, `windfor`) preserve `issue_time` where published.

## [v0.7] - 2026-05-04

**GIE AGSI Gas Storage Validation** — validated the GIE AGSI (EU gas storage inventory) connector
end to end.

### Added
- AGSI endpoint catalog and inventory contract tests, with a deterministic expected-count model
  for aggregate, country, company, and facility query scopes.
- Query-scope request builder with `last_page`-driven pagination and page provenance; exact-day
  requests write exactly the requested gas day.
- Storage silver transformers preserving inventory, injection/withdrawal, capacities, fullness,
  and status fields, with count-preserving bronze-to-silver tests.
- Opt-in live API-to-silver tests and isolated CLI smoke tests honouring the 60 calls/min limit.

## [v0.6] - 2026-05-04

**NESO Carbon Intensity Platform** — expanded NESO Carbon Intensity from a single national route
to all documented route families.

### Added
- Endpoint catalog covering national intensity, statistics, factors, generation, and regional
  route families.
- Path-template request construction, with path values preserved in bronze provenance.
- Family-aware silver transforms writing deterministic Parquet; mocked all-dataset end-to-end
  tests, opt-in live API-to-silver tests, and CLI smoke coverage.

### Fixed
- Same-day range windows and settlement-period iteration now fetch complete target dates instead
  of empty or partial windows.

## [v0.5] - 2026-05-04

**ENTSOG Pipeline Validation** — validated the ENTSO-G (EU gas transparency) pipeline: 33 active
datasets from endpoint research through live CLI confidence. 857 non-live tests passing.

### Added
- ENTSO-G endpoint catalog with metadata-driven bronze requests (exact-case paths and indicators,
  `timeZone=UCT`, mandatory point-direction defaults).
- Specialised and generic silver transforms: physical flows with GWh/day normalisation, plus
  operational, congestion-management/event, tariff, aggregated, and reference datasets.
- Opt-in live API-to-silver validation with live no-data outcomes explicitly classified.

### Fixed
- Duplicate snake_case column collisions (e.g. `isCAMRelevant`/`isCamRelevant`) now coalesce, and
  the real congestion-management auction-premiums backfill succeeds.

## [v0.4] - 2026-05-04

**Elexon Pipeline Validation** — validated the Elexon (GB balancing mechanism) pipeline from
inventory through live CLI smoke coverage. 81 non-live regression tests plus 10 live tests passing.

### Added
- Mocked request-shape tests for every active Elexon dataset and fixture-backed bronze-to-silver
  transformer tests.
- Opt-in live API-to-silver tests for system prices, bid-offer acceptances, frequency, physical
  notifications, and BM-unit reference data.
- Live CLI and backfill smoke tests running under isolated temporary data paths.

### Fixed
- Runtime `GRIDFLOW_DATA_DIR`/`GRIDFLOW_DUCKDB_PATH`/`GRIDFLOW_LOG_DIR` overrides now beat YAML
  defaults.

## [v0.3] - 2026-05-03

**ENTSO-E Pipeline Validation** — expanded the ENTSO-E pipeline from 16 to 48 active datasets with
an auditable endpoint catalog. 378 tests passing on the close-out gate.

### Added
- New source families: generation (unit-level generation, reservoirs, master data),
  transmission/market, outages, and balancing extensions.
- Official endpoint catalog (`docs/entsoe_endpoint_catalog.yaml`) tracking implementation,
  deferral, and scope decisions — deferred endpoints are explicit follow-ups, not silent gaps.
- Credential-gated live request-shape tests and mocked bronze-to-silver coverage across the fleet.

### Fixed
- Live payload handling for zipped XML, tag variants, and no-data acknowledgements; CLI positional
  `all` now behaves as `--all`.

## [v0.2] - 2026-05-02

**ENTSO-E Extension Gap Closure** — closed all 10 gap findings from the ENTSO-E connector
extension audit, making the ENTSO-E silver layer spec-compliant. 551 tests passing.

### Fixed
- Doubled bronze-read path in five transformers.
- Schema corrections: forecast-horizon literal fields, `generation_forecast_mw` and `capacity_mw`
  renames, and imbalance-volume process type.
- Raw ENTSO-E balancing A-codes replaced with semantic values (direction "long"/"short", reserve
  types "fcr"/"afrr"/"mfrr"/"rr", direction "up"/"down"); prices correctly named `price_eur_mwh`.

### Added
- Unit-level generation outages: the XML parser now extracts unit IDs and names from outage
  documents, and the outages dataset was redesigned to a unit-level schema with outage-type
  mapping.

[v0.16]: https://github.com/EBentham/gridflow/releases/tag/v0.16
[v0.15]: https://github.com/EBentham/gridflow/releases/tag/v0.15
[v0.14]: https://github.com/EBentham/gridflow/releases/tag/v0.14
[v0.12]: https://github.com/EBentham/gridflow/releases/tag/v0.12
[v0.11]: https://github.com/EBentham/gridflow/releases/tag/v0.11
[v0.10]: https://github.com/EBentham/gridflow/releases/tag/v0.10
[v0.9]: https://github.com/EBentham/gridflow/commit/b3b5659
[v0.8]: https://github.com/EBentham/gridflow/releases/tag/v0.8
[v0.7]: https://github.com/EBentham/gridflow/releases/tag/v0.7
[v0.6]: https://github.com/EBentham/gridflow/releases/tag/v0.6
[v0.5]: https://github.com/EBentham/gridflow/releases/tag/v0.5
[v0.4]: https://github.com/EBentham/gridflow/releases/tag/v0.4-elexon-validation
[v0.3]: https://github.com/EBentham/gridflow/releases/tag/v0.3-entsoe-validation
[v0.2]: https://github.com/EBentham/gridflow/releases/tag/v0.2-entsoe-gaps
