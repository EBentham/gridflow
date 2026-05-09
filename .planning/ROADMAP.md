# gridflow - Roadmap

---

## Milestones

- Complete **v0.2-entsoe-gaps** - Gap Closure G1-G4 (shipped 2026-05-02)
- Complete **v0.3-entsoe-validation** - ENTSO-E Pipeline Validation H1-H8 (shipped 2026-05-03)
- Complete **v0.4-elexon-validation** - Elexon Pipeline Validation I1-I4 (shipped 2026-05-04)
- Complete **v0.5-entsog-pipeline-validation** - ENTSOG Pipeline Validation J1-J4 (shipped 2026-05-04)
- Complete **v0.6-neso-carbon-intensity-platform** - NESO Carbon Intensity Platform K1-K4 (completed 2026-05-04)
- Complete **v0.7-gie-agsi-gas-storage-validation** - GIE AGSI Gas Storage Validation L1-L4 (completed 2026-05-04)
- Complete **v0.8-fundamentals-model-silver-foundations** - Fundamentals Model Silver Foundations F0 (completed 2026-05-05)
- Complete **v0.9-vault-vendor-validation-and-docs** - Live-validate every active gridflow endpoint and populate `quant-vault/30-vendors/` V1 (completed 2026-05-08)
- Complete **v0.10-v1-vendor-bugfix-followups** - Fix the production bugs surfaced (but not patched) by V1 across Elexon, NESO, ENTSOE, ENTSOG (V2) (completed 2026-05-09)
- Complete **v0.11-open-meteo-renewable-extension** - Extend Open-Meteo connector for wind/solar forecasting (F7.5) (completed 2026-05-09)

---

## Phases

<details>
<summary>Complete v0.2-entsoe-gaps - ENTSO-E Extension Gap Closure (G1-G4) - SHIPPED 2026-05-02</summary>

- [x] Phase G1: Fix Phase 3 bronze read path + connector test - Done
- [x] Phase G2: Minor schema corrections (GAP-01/02/03a/05/08) - Done 2026-04-30
- [x] Phase G3: Map balancing codes to semantic values (GAP-06/07) - Done 2026-05-02
- [x] Phase G4: outages_generation unit-level schema (GAP-04) - Done 2026-05-02

See full details: [milestones/v0.2-entsoe-gaps-ROADMAP.md](milestones/v0.2-entsoe-gaps-ROADMAP.md)

</details>

---

<details>
<summary>Complete v0.3-entsoe-validation - ENTSO-E Pipeline Validation (H1-H8) - SHIPPED 2026-05-03</summary>

- [x] Phase H1: Fix CLI `all` positional alias - completed 2026-05-02
- [x] Phase H2: ENTSO-E mocked E2E tests - completed 2026-05-02
- [x] Phase H3: Live ENTSO-E test suite scaffolding and credential-gated all-dataset coverage - implementation complete; credentialed close-out deferred
- [x] Phase H4: ENTSO-E endpoint catalog + request builder correction - completed 2026-05-03
- [x] Phase H5: ENTSO-E generation unit and reference data sources - completed 2026-05-03
- [x] Phase H5.5: ENTSO-E live cleanup before H6 - completed 2026-05-03
- [x] Phase H6: ENTSO-E transmission and market data sources - completed 2026-05-03
- [x] Phase H7: ENTSO-E outage extension data sources - completed 2026-05-03
- [x] Phase H8: ENTSO-E balancing extension sources - completed 2026-05-03

See full details: [milestones/v0.3-entsoe-validation-ROADMAP.md](milestones/v0.3-entsoe-validation-ROADMAP.md)

Known deferred close-out items: 4; see [STATE.md](STATE.md).

</details>

---

<details>
<summary>Complete v0.4-elexon-validation - Elexon Pipeline Validation (I1-I4) - SHIPPED 2026-05-04</summary>

- [x] Phase I1: Elexon inventory, test scaffolding, and request-style baseline - completed 2026-05-03
- [x] Phase I2: Elexon mocked request-shape and fixture-backed bronze-to-silver tests - completed 2026-05-04
- [x] Phase I3: Elexon live API to silver test suite - completed 2026-05-04
- [x] Phase I4: Elexon CLI/backfill live smoke tests and milestone close-out docs - completed 2026-05-04

See full details: [milestones/v0.4-elexon-validation-ROADMAP.md](milestones/v0.4-elexon-validation-ROADMAP.md)

</details>

---

<details>
<summary>Complete v0.5-entsog-pipeline-validation - ENTSOG Pipeline Validation (J1-J4) - SHIPPED 2026-05-04</summary>

- [x] Phase J1: ENTSOG endpoint catalog, research, and inventory contract - completed 2026-05-04
- [x] Phase J2: ENTSOG connector request builder and bronze endpoint coverage - completed 2026-05-04
- [x] Phase J3: ENTSOG silver transformers, mocked request-shape tests, and fixture-backed bronze-to-silver tests - completed 2026-05-04
- [x] Phase J4: ENTSOG opt-in live API-to-silver tests, CLI smoke tests, and close-out docs - completed 2026-05-04

See full details: [milestones/v0.5-entsog-pipeline-validation-ROADMAP.md](milestones/v0.5-entsog-pipeline-validation-ROADMAP.md)

</details>

---

<details>
<summary>Complete v0.6-neso-carbon-intensity-platform - NESO Carbon Intensity Platform (K1-K4) - COMPLETED 2026-05-04</summary>

- [x] Phase K1: NESO endpoint research, catalog, source config, and inventory contract
- [x] Phase K2: NESO connector path-template request builder and mocked request-shape tests
- [x] Phase K3: NESO family-aware silver transformers and fixture-backed bronze-to-silver tests
- [x] Phase K4: NESO opt-in live API-to-silver tests, CLI smoke test, and close-out verification

Research: [NESO-CARBON-INTENSITY-RESEARCH.md](research/NESO-CARBON-INTENSITY-RESEARCH.md)

</details>

---

<details>
<summary>Current v0.7-gie-agsi-gas-storage-validation - GIE AGSI Gas Storage Validation (L1-L4) - COMPLETED 2026-05-04</summary>

- [x] Phase L1: GIE AGSI endpoint research, catalog, inventory contract, and expected-count model - completed 2026-05-04
- [x] Phase L2: AGSI query-scope request builder, `last_page` pagination, and bronze completeness tests - completed 2026-05-04
- [x] Phase L3: AGSI silver transformers, fixtures, mocked E2E, and count-preserving bronze-to-silver tests - completed 2026-05-04
- [x] Phase L4: AGSI opt-in live API-to-silver tests, CLI smoke tests, and close-out verification - completed 2026-05-04

Research: [GIE-AGSI-API-RESEARCH.md](research/GIE-AGSI-API-RESEARCH.md)

### Phase Details

**Phase L1: GIE AGSI endpoint research, catalog, inventory contract, and expected-count model**

Goal: Make the AGSI API surface auditable before changing runtime behavior.

Requirements: AGSI-01, AGSI-03

Success criteria:
1. `docs/gie_agsi_endpoint_catalog.yaml` classifies `/api`, `/api/about`, `/api/about?show=listing`, `/api/news`, `/api/news?turl`, and `/api/unavailability`.
2. `docs/gie_agsi_endpoint_catalog.yaml` and `src/gridflow/connectors/gie/endpoints.py` agree on active, planned, and deferred AGSI endpoint families.
3. A deterministic query-plan helper can derive expected request/page/entity counts for aggregate, country, company, and facility scopes from listing fixtures.
4. Inventory tests fail on catalog/connector drift before implementation can silently omit a GIE endpoint family.

**Phase L2: AGSI query-scope request builder, `last_page` pagination, and bronze completeness tests**

Goal: Fetch AGSI bronze data exactly for the requested query scope and gas-day window.

Requirements: AGSI-02, AGSI-04, AGSI-05, AGSI-06

Success criteria:
1. AGSI requests use documented `date`, `from`, `to`, `type`, `country`, `company`, `facility`, `start`, `end`, `end_flag`, `page`, and `size` parameters as appropriate.
2. Pagination loops over `last_page`, not `total`, and records page/total page provenance.
3. Exact-day requests such as 2026-05-01 write only that gas day for exact-day datasets.
4. Mocked tests prove bronze file counts equal the expected request/page plan for selected aggregate, country, company, and facility scopes.

**Phase L3: AGSI silver transformers, fixtures, mocked E2E, and count-preserving bronze-to-silver tests**

Goal: Preserve AGSI payload data through silver parquet with deterministic schemas.

Requirements: AGSI-07, AGSI-08, AGSI-09, AGSI-10

Success criteria:
1. Storage silver keeps live AGSI fields for inventory, injection, withdrawal, net withdrawal, capacities, fullness, status, update times, entity metadata, and service announcements.
2. Listing, news, and unavailability payloads are transformed or explicitly deferred with catalog-backed reasons.
3. Fixture-backed tests prove bronze rows for aggregate, country, company, facility, listing, news, and unavailability families reach silver.
4. Mocked E2E tests cover endpoint inventory, request shapes, pagination, and bronze-to-silver outputs without live network access.

**Phase L4: AGSI opt-in live API-to-silver tests, CLI smoke tests, and close-out verification**

Goal: Prove real AGSI data can move through the public user paths without polluting local data.

Requirements: AGSI-11, AGSI-12

Success criteria:
1. `pytest -m live` proves representative AGSI aggregate, country, company, and facility queries flow from live API response to bronze to silver.
2. A slower explicit full-inventory live gate can verify listing-derived expected request counts while respecting the documented 60 calls/minute limit.
3. Live CLI smoke tests run under isolated `GRIDFLOW_DATA_DIR`, `GRIDFLOW_DUCKDB_PATH`, and `GRIDFLOW_LOG_DIR` paths.
4. Close-out docs record live pass/skip classifications, API doc ambiguity around unavailability, and any ALSI follow-up.

Plans:
- [x] `L4-01-PLAN.md` - Added opt-in live API-to-silver tests, isolated AGSI CLI smoke tests, and close-out verification artifacts.

Live close-out: `tests/integration/test_gie_agsi_live_e2e.py` and
`tests/integration/test_gie_agsi_cli_live_smoke.py` passed representative
credentialed AGSI live gates. The slow full-inventory gate remains explicit via
`GRIDFLOW_AGSI_FULL_INVENTORY_LIVE=1`. ALSI LNG remains a backlog follow-up.

</details>

---

<details open>
<summary>Complete v0.8-fundamentals-model-silver-foundations - Fundamentals Model Silver Foundations (F0) - COMPLETED 2026-05-05</summary>

- [x] Phase F0: Bitemporal fundamentals datasets and silver run lineage - completed 2026-05-05

Artifacts:
- [F0-CONTEXT.md](phases/F0-bitemporal-fundamentals-datasets/F0-CONTEXT.md)
- [F0-PLAN.md](phases/F0-bitemporal-fundamentals-datasets/F0-PLAN.md)
- [F0-RESULTS.md](phases/F0-bitemporal-fundamentals-datasets/F0-RESULTS.md)
- [F0-VERIFICATION.md](phases/F0-bitemporal-fundamentals-datasets/F0-VERIFICATION.md)

### Phase Details

**Phase F0: Bitemporal fundamentals datasets and silver run lineage**

Goal: Add point-in-time lineage to gridflow silver outputs for the first demand-forecast model datasets, without starting the separate `gridflow_models` project yet.

Requirements: F0-BITEMP-01, F0-BITEMP-02, F0-BITEMP-03, F0-BITEMP-04, F0-ISSUE-01, F0-RUN-01, F0-RUN-02, F0-REINGEST-01, F0-REINGEST-02, F0-VERIFY-01, F0-VERIFY-02, F0-VERIFY-03

Success criteria:
1. `BaseSilverTransformer.run()` accepts run context, injects `event_time`, `available_at`, `source_run_id`, and `dataset_version`, and keeps direct test usage working with an ad hoc run id.
2. Forecast-like in-scope datasets (`elexon/ndf`, `elexon/windfor`) either populate `issue_time` from publish-time metadata or document a specific source limitation in `F0-RESULTS.md`.
3. `gridflow transform`, `gridflow pipeline`, `gridflow backfill`, and `scripts/run_pipeline.py` pass the active transform run id into silver transformers.
4. A re-ingest path uses bronze sidecar timestamps to reconstruct historical `available_at` for `elexon/indo`, `elexon/fuelhh`, `elexon/windfor`, `elexon/ndf`, and `open_meteo/historical`.
5. Tests and DuckDB checks prove bitemporal columns are present, UTC-aware, run-traceable, queryable, and documented for `gridflow_models` handoff.

Plans:
- [x] `F0-PLAN.md` - Executed with bitemporal silver lineage, run-id propagation, reingest support, DuckDB verification, and close-out docs.

Close-out notes:
1. The supplied F0 spec names `openmeteo/historical`; the implementation uses the repo source name `open_meteo/historical`.
2. `elexon/ndf` and `elexon/windfor` now preserve `issue_time` from publish-time metadata where present.
3. The implementation stayed dependency-neutral with parametrized pytest coverage rather than adding Hypothesis.
4. Historical broad re-transform is documented but not run locally because this workspace has no `data/bronze/` partitions.

</details>

---

<details>
<summary>Complete v0.9-vault-vendor-validation-and-docs - Live-validate every active gridflow endpoint and populate the vault (V1) - COMPLETED 2026-05-08</summary>

- [x] Phase V1: Vault vendor validation and docs - completed 2026-05-08

### Phase Details

**Phase V1: Vault vendor validation and docs**

Goal: Live-validate every active gridflow endpoint against vendor official documentation and populate `quant-vault/30-vendors/<vendor>/` with authoritative dataset pages, endpoint summaries, README updates, and per-vendor validation reports for all six active vendors (Elexon, ENTSOE, ENTSOG, GIE AGSI, NESO, Open-Meteo).

Requirements: V1-VAULT-01, V1-VAULT-02, V1-VAULT-03, V1-VAULT-04, V1-VALID-01, V1-VALID-02, V1-VALID-03

Success criteria:
1. Every active dataset in `config/sources.yaml` (Elexon 33, ENTSOE 48, ENTSOG 33, GIE AGSI 7, NESO 33, Open-Meteo 2 = 156 datasets) has a `quant-vault/30-vendors/<vendor>/datasets/<key>.md` page following the `gridflow-dataset-spec` skill template.
2. Each vendor's `endpoints.md` is a complete quick-summary table covering all active datasets with path, param style, and one-line description.
3. Each vendor's `README.md` has confirmed (no remaining `TODO`) values for auth method, rate limit, and known gotchas, all cross-checked against vendor docs and live API behaviour.
4. Each vendor has a `<vendor>-VALIDATION.md` report classifying every active endpoint as PASS / FAIL / EMPTY, with cause and curl-or-respx evidence for every non-PASS case.
5. Every endpoint URL in `src/gridflow/connectors/<vendor>/endpoints.py` matches the official-docs URL pattern verbatim (or the discrepancy is recorded in the dataset page's `## Implementation delta`).
6. Authority hierarchy honoured: official docs > test fixtures > codebase. Doc/code conflicts logged as deltas, never silently resolved.

Plans (9 in wave 1 + 1 aggregation in wave 2):
- [x] `V1-PLAN-A-elexon.md` - Elexon (33 datasets, 2 req/s) — wave 1 — 33 PASS / 0 EMPTY / 0 FAIL
- [x] `V1-PLAN-B1-entsoe-load-prices.md` - ENTSOE load + prices + imbalance (11) — 0 PASS / 11 EMPTY / 0 FAIL (GB post-Brexit; tuples verified via DE-LU/FR/NL fallback)
- [x] `V1-PLAN-B2-entsoe-generation-outages.md` - ENTSOE generation + outages (13) — 4 PASS / 9 EMPTY / 0 FAIL
- [x] `V1-PLAN-B3-entsoe-transmission-capacity.md` - ENTSOE transmission + capacity (18) — 5 PASS / 13 EMPTY / 0 FAIL
- [x] `V1-PLAN-B4-entsoe-balancing.md` - ENTSOE balancing (6) — 0 PASS / 6 EMPTY / 0 FAIL
- [x] `V1-PLAN-B5-entsoe-aggregate.md` - ENTSOE endpoints.md + README + consolidated VALIDATION — wave 2 — done
- [x] `V1-PLAN-C-entsog.md` - ENTSOG (33 datasets, public) — 29 PASS / 4 EMPTY / 0 FAIL
- [x] `V1-PLAN-D-gie.md` - GIE AGSI (7 endpoints, 60 calls/min, x-key header) — 7 PASS / 0 EMPTY / 0 FAIL
- [x] `V1-PLAN-E-neso.md` - NESO (33 datasets, validate-and-refresh-in-place) — 33 PASS / 0 EMPTY / 0 FAIL
- [x] `V1-PLAN-F-openmeteo.md` - Open-Meteo (2 datasets, two hosts verified) — 2 PASS / 0 EMPTY / 0 FAIL

</details>

---

<details>
<summary>Complete v0.10-v1-vendor-bugfix-followups - Fix V1-surfaced production bugs (V2) - COMPLETED 2026-05-09</summary>

- [x] Phase V2: V1 vendor bug-fix follow-ups - completed 2026-05-09

### Phase Details

**Phase V2: V1 vendor bug-fix follow-ups**

Goal: Fix the production code bugs surfaced (but explicitly out of scope) by Phase V1 — Elexon `freq` parameter-name mismatch, NESO `_rows_from_region_period` field-level bug for period-keyed regional payloads, plus medium and low-severity follow-ups across Elexon, ENTSOE, and ENTSOG. Re-validate every fixed dataset live against the same vendor APIs V1 used. Update vault dataset pages, V1 VALIDATION reports, and silver fixtures only where a fix invalidates them.

Requirements: V2-FIX-01 (Elexon `freq` window), V2-FIX-02 (NESO regional carbon/mix), V2-FIX-03 (Elexon REMIT/SOSO 1-day cap), V2-FIX-04 (Elexon `system_prices.run_type` accepts `N`), V2-FIX-05 (ENTSOE A09 commercial_schedules registry dedup), V2-FIX-06 (ENTSOE B2 cleanup batch — A37/A15 pagination, A87 schedule + silver Reason.code, area_name + psrType + DEFAULT_ZONES hygiene), V2-FIX-07 (ENTSOG `@RETRY_POLICY` 404 short-circuit), V2-TRIAGE-01 (`connectors/ngeso/` empty placeholder)

Success criteria:
1. Every HIGH-severity bug from the V1 cross-vendor recommendations has a code fix on `master` (or an explicit `wontfix` ADR), with at least one regression test that would have caught the bug.
2. Every MEDIUM-severity bug from the V1 recommendations has a code fix or an explicit `defer` decision recorded in `docs/DECISION_LOG/`.
3. Every LOW-severity bug from the V1 recommendations has a code fix or a backlog row in `.planning/ROADMAP.md` Backlog section.
4. Each fixed dataset is re-validated live against the same vendor API V1 used, with curl evidence and PASS/EMPTY/FAIL classification appended to the relevant V1 `<vendor>-VALIDATION.md` report under a `## V2 re-validation` section.
5. The Avast `curl --ssl-no-revoke` workaround documented in V1-CONTEXT.md continues to be used verbatim for all live calls — no Python `httpx` direct calls.
6. `uv run pytest -x -q` passes locally on the worktree before V2 is closed.

Plans (2 in wave 1 — HIGH bugs, parallel · 3 in wave 2 — MED/LOW bundles, parallel · 1 in wave 3 — close-out aggregator):

- [x] `V2-PLAN-A-elexon-freq-fix.md` - Wave 1 - Override `from_param` / `to_param` on `ENDPOINTS["freq"]` to `measurementDateTimeFrom` / `measurementDateTimeTo`; add regression test that sends a known-narrow window and asserts response time-window matches the request, not "latest 5761 samples".
- [x] `V2-PLAN-B-neso-region-period-fields.md` - Wave 1 - Patch `silver/neso/carbon_intensity.py::_rows_from_region_period` to read `intensity` and `generationmix` from whichever level (region or period) holds the data. Affects 5 period-keyed datasets — `regional_current`, `regional_intensity_fw24h`, `regional_intensity_fw48h`, `regional_intensity_pt24h`, `regional_intensity`.
- [x] `V2-PLAN-C-elexon-misc.md` - Wave 2 - Cap `remit` and `soso` `max_chunk_hours = 23` to honour the undocumented vendor 1-day cap; expand the `system_prices.run_type` regex (and `SettlementRunType` enum) to accept the live-observed `N` derivation code, after live-confirming what `N` denotes.
- [x] `V2-PLAN-D-entsoe-cleanup.md` - Wave 2 - Resolve A09 `commercial_schedules` / `commercial_schedules_net_positions` registry duplication (drop one key, or rewrite the net-positions transformer to derive a real signed `net_position_mw`); B2 cleanup batch — A37/A15 pagination iteration, A87 schedule cadence, A87 silver `Reason.code` exposure, `area_name` field population, `psrType` in `optional_params`, `DEFAULT_ZONES` GB/EU bias review.
- [x] `V2-PLAN-E-entsog-and-ngeso.md` - Wave 2 - Short-circuit HTTP 404 + body `{"message":"No result found"}` in `EntsogConnector._request` so `@RETRY_POLICY` does not waste retry budget on the documented empty convention. Triage `connectors/ngeso/` (empty placeholder besides `__init__.py`) — either delete or open a tracking ADR; flagged in V1 close-out.
- [x] `V2-PLAN-F-aggregate.md` - Wave 3 - Author consolidated `V2-VALIDATION.md`, append re-validation rows to V1's per-vendor VALIDATION reports, update `.planning/STATE.md`, update `.planning/ROADMAP.md` Backlog section to remove items absorbed into V2 fixes.

</details>

---

<details open>
<summary>Complete v0.11-open-meteo-renewable-extension - Open-Meteo Connector Extension for Renewable Forecasting (F7.5) - COMPLETED 2026-05-09</summary>

- [x] Phase F7.5: Open-Meteo connector extension for wind/solar forecasting - completed 2026-05-09

### Phase Details

**Phase F7.5: Open-Meteo Connector Extension for Renewable Forecasting**

Depends on: F0 (bitemporal pattern in `BaseSilverTransformer` — already on master)

Goal: Extend the Open-Meteo bronze and silver layers to provide the variable set and spatial coverage required for production-grade UK wind and solar forecasting downstream. After F7.5, silver carries hub-height wind (10m + 100m, archive-verified), full irradiance components (GHI/DNI/DHI/GTI), cloud-cover decomposition, and snow variables; locations split into capacity-weighted wind (12 sites) and solar (6 sites) lists alongside the existing 7 demand population centres. Workstream C (15-min/AROME forecast resolution) is deferred to backlog pending AROME 2026 boundary verification.

Requirements:
- F7.5-VARS-01: Hub-height wind (`wind_speed_100m`, `wind_direction_100m`) added to wind dataset; archive limited to 10m+100m (verified 2026-05-09 against ERA5: 80/120/180m return all-null undefined units); forecast variable list permits wider hub heights and lets the API null-degrade fields the underlying model lacks.
- F7.5-VARS-02: Wind gusts at 10m (`wind_gusts_10m`) added to wind dataset.
- F7.5-VARS-03: Solar dataset adds `direct_radiation`, `direct_normal_irradiance`, `diffuse_radiation`, `global_tilted_irradiance` alongside existing `shortwave_radiation` (GHI). GTI request includes `tilt=35&azimuth=180` query params (UK fixed-tilt rep geometry).
- F7.5-VARS-04: Wind and solar datasets include `cloud_cover` plus `cloud_cover_low`, `cloud_cover_mid`, `cloud_cover_high`.
- F7.5-VARS-05: Demand and solar datasets gain `snowfall`, `snow_depth`. Wind dataset gains `dew_point_2m` (icing risk).
- F7.5-VARS-06: Silver transformers derive `air_density_kg_m3` from ideal-gas law (`ρ = P / (287.05 * T_K)`) for any dataset that fetches `surface_pressure` and `temperature_2m`.
- F7.5-LOC-01: `WIND_LOCATIONS` covers 12 capacity-weighted GB wind sites: Dogger Bank, Hornsea, East Anglia, Triton Knoll, Walney, Gwynt y Môr, Beatrice, Seagreen, Highland Central, Borders Crystal Rig, Whitelee, Pen y Cymoedd.
- F7.5-LOC-02: `SOLAR_LOCATIONS` covers 6 capacity-weighted GB solar sites: East Anglia (Norfolk), Wiltshire/Somerset, Kent, Cornwall, Sussex, Oxfordshire.
- F7.5-LOC-03: `DEMAND_LOCATIONS` preserves the existing 7 UK population centres unchanged (London, Birmingham, Manchester, Leeds, Glasgow, Cardiff, Belfast).
- F7.5-LOC-04: Per-location bronze dataset names use `f"{dataset}__{loc.name}"` (double underscore) to disambiguate parsing of multi-word location names.
- F7.5-MIG-01: `DATASET_VERSION` bumps from 1.0.0 → 2.0.0 on all openmeteo silver transformers; existing dataset names `historical` and `forecast` are renamed to `historical_demand` and `forecast_demand` (hard rename — no on-disk silver to migrate per STATE.md). Re-ingest commands documented in F7.5-RESULTS.md.
- F7.5-COMPAT-01: Existing tests `tests/unit/test_openmeteo.py`, `tests/unit/test_bitemporal_columns.py`, and `tests/endpoints/test_endpoint_urls.py` are updated to the new dataset naming and continue to pass; the `historical_london` bronze fixture is renamed/copied as `historical_demand__london`.
- F7.5-VAULT-01: `quant-vault/30-vendors/open-meteo/README.md`, `endpoints.md`, and the dataset pages reflect the six new datasets, the three location lists, and the archive 10m+100m limitation.

Plans (1 wave, sequential within wave):
- [x] `F7.5-01-PLAN.md` - Connector + endpoints refactor + silver schema split + transformer split + config + tests + vault docs - completed 2026-05-09

Outcomes (4 commits):
- `7369f15 feat(F7.5): role-split openmeteo connector, schemas, transformers, tests`
- `698db64 feat(F7.5): six openmeteo dataset blocks in config/sources.yaml`
- `0aa85a6 docs(F7.5): in-repo open-meteo docs + ADR-020 + RESULTS close-out`
- `ec914e1 fix(F7.5): sweep migration stragglers caught by code review`

Verification: PASS_WITH_DEFERRALS — 12/13 requirements verified, all 6 threat-model mitigations have explicit tests; F7.5-VAULT-01 partial (in-repo `docs/endpoints/open_meteo.md` and `docs/ENDPOINT_REFERENCE.md` are updated; Obsidian vault sync deferred to a session with `obsidian-vault` MCP server access).

Code review: 1 HIGH (`serving/client.py::get_weather()` queried deleted `silver_historical` view) + 2 MEDIUM (`scripts/run_all_sources.py` mapped `["historical","forecast"]`; `tests/.../test_openmeteo_air_density.py` dead assert) — all fixed in `ec914e1`.

Test results: **1116 passed, 251 deselected** (`pytest -m "not live and not slow"`). Net +74 over pre-F7.5 baseline.

</details>

---

## Backlog

| Item | Source | Notes |
|------|--------|-------|
| GAP-03b: wind_solar_forecast psrType mapping (B16 -> solar, B18 -> wind_onshore, B19 -> wind_offshore) | v0.2 gap closure audit | Deferred - no gold consumers yet |
| Extend E2E coverage to GIE ALSI LNG | v0.7 scoping | v0.7 focuses AGSI gas storage; ALSI LNG remains a follow-up connector-confidence candidate |
| Domain-specific ENTSOG silver schemas | v0.5 close-out | Add when downstream gas gold consumers require typed models beyond generic normalised records |
| Scheduled live endpoint monitoring | v0.5 close-out | Consider outside the normal unit-test suite for ENTSOG and other public APIs |
| Append-only revision storage for revising modelling datasets | gridflow_models architecture v3.1 | Deferred until stack, imbalance, or carbon model scope needs it |
| Historical `freq` bronze re-ingest after V2-A param fix | v0.10 V2-A | Existing bronze captured "latest 5761 samples" not the requested window; re-ingest needed for correct historical data |
| Historical NESO regional silver re-ingest for 5 affected datasets | v0.10 V2-B | `regional_current`, `regional_intensity`, `regional_intensity_fw24h`, `regional_intensity_fw48h`, `regional_intensity_pt24h` — existing silver carries null carbon/mix; re-run silver from existing bronze |
| ENTSOE A09 derive `net_position_mw` (Option B not taken in V2) | v0.10 V2-D / ADR-019 | Keep both keys, pair zone-pair directions, emit signed net position. Useful for cross-border net flow analysis when a consumer materialises |
| ENTSOE A37 / A15 pagination iteration (offset > 0) | v0.10 V2-D / 5a | Currently silently truncates at 4800 TS for high-cardinality areas |
| ENTSOE A87 silver `Reason.code` exposure | v0.10 V2-D / 5c | `_H8BalancingTransformer` refactor + new `reason_code` schema field |
| ENTSOE `area_name` field population | v0.10 V2-D / 5d | New `area_code → name` lookup table OR schema removal |
| ENTSOE `DEFAULT_ZONES` wider EU baseline | v0.10 V2-D / 5f | If a multi-region gold consumer materialises |
| ENTSOE `_RESOLUTION_MAP` calendar-correct `P1M`/`P1Y` | V1 entsoe-VALIDATION Recommendations §5 | Approximating month=30d, year=365d affects load_forecast_monthly, load_forecast_yearly |
| ENTSOE `activated_balancing_prices` reserve-type widening | V1 entsoe-VALIDATION Recommendations §6 | Connector currently fixes businessType=A96 (aFRR); silver schema supports FCR/aFRR/mFRR/RR |
| ENTSOE Pydantic schema vs silver Parquet column drift (B3) | V1 entsoe-VALIDATION §13 | `EntsoeCrossborderFlow` / `EntsoeNetTransferCapacity` declare narrower fields than transformer outputs |
| Manual ENTSOE Guide.pdf download | V1 entsoe-VALIDATION Recommendations §1 | CDN protection blocks programmatic fetch; human download into vault recommended |
| ENTSOE GB pre-Brexit window re-validation | V1 entsoe-VALIDATION Recommendations §2 | Distinguish "permanently not published" from "publication-lag" via 2019/2020 GB window |
| Vault directory rename `open-meteo` → `openmeteo` | V1 V0.7 deferred | Backlog, unchanged |
| Project-wide ruff baseline cleanup | v0.10 V2 observation | ~83 pre-existing warnings (TC003, UP042, UP017) tolerated today; would clean up on a focused chore branch |
| Open-Meteo `minutely_15` forecast resolution (Workstream C) | v0.11 F7.5 scoping | Deferred at F7.5 plan stage — gated on AROME 2026 northern-boundary verification from Open-Meteo model coverage docs. Useful only for AROME-covered locations (broadly south of ~53°N); outside that footprint Open-Meteo interpolates from hourly so the 15-min file would mislead consumers. Revisit when conformal-prediction-grade features are needed. |
| Open-Meteo ensemble endpoint (`/v1/ensemble`) | F7.5 follow-up plan section | ~30 perturbed forecast members per call as direct-conditional features for conformal prediction (per-issuance ensemble spread = calibrated uncertainty proxy MAPIE can consume). Out of scope at F7.5; consider once F7.5 silver is producing residuals downstream. |
| Open-Meteo wind/solar bronze backfill 2018-2025 | v0.11 F7.5 execution | F7.5 sets up the schema and request shapes but the historical backfill (~52K location-days for wind+solar) requires explicit user-confirmed live ingestion. Re-ingest commands are documented in F7.5-RESULTS.md and only run on user request. |
