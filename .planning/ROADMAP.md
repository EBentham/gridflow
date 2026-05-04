# gridflow - Roadmap

---

## Milestones

- Complete **v0.2-entsoe-gaps** - Gap Closure G1-G4 (shipped 2026-05-02)
- Complete **v0.3-entsoe-validation** - ENTSO-E Pipeline Validation H1-H8 (shipped 2026-05-03)
- Complete **v0.4-elexon-validation** - Elexon Pipeline Validation I1-I4 (shipped 2026-05-04)
- Complete **v0.5-entsog-pipeline-validation** - ENTSOG Pipeline Validation J1-J4 (shipped 2026-05-04)
- Complete **v0.6-neso-carbon-intensity-platform** - NESO Carbon Intensity Platform K1-K4 (completed 2026-05-04)
- Current **v0.7-gie-agsi-gas-storage-validation** - GIE AGSI Gas Storage Validation L1-L4

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

<details open>
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

## Backlog

| Item | Source | Notes |
|------|--------|-------|
| GAP-03b: wind_solar_forecast psrType mapping (B16 -> solar, B18 -> wind_onshore, B19 -> wind_offshore) | v0.2 gap closure audit | Deferred - no gold consumers yet |
| Extend E2E coverage to GIE ALSI LNG | v0.7 scoping | v0.7 focuses AGSI gas storage; ALSI LNG remains a follow-up connector-confidence candidate |
| Domain-specific ENTSOG silver schemas | v0.5 close-out | Add when downstream gas gold consumers require typed models beyond generic normalised records |
| Scheduled live endpoint monitoring | v0.5 close-out | Consider outside the normal unit-test suite for ENTSOG and other public APIs |
