# gridflow - Roadmap

---

## Milestones

- Complete **v0.2-entsoe-gaps** - Gap Closure G1-G4 (shipped 2026-05-02)
- Complete **v0.3-entsoe-validation** - ENTSO-E Pipeline Validation H1-H8 (shipped 2026-05-03)
- Complete **v0.4-elexon-validation** - Elexon Pipeline Validation I1-I4 (shipped 2026-05-04)
- Complete **v0.5-entsog-pipeline-validation** - ENTSOG Pipeline Validation J1-J4 (shipped 2026-05-04)
- Current **v0.6-neso-carbon-intensity-platform** - NESO Carbon Intensity Platform K1-K4

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

<details open>
<summary>Current v0.6-neso-carbon-intensity-platform - NESO Carbon Intensity Platform (K1-K4)</summary>

- [x] Phase K1: NESO endpoint research, catalog, source config, and inventory contract
- [x] Phase K2: NESO connector path-template request builder and mocked request-shape tests
- [x] Phase K3: NESO family-aware silver transformers and fixture-backed bronze-to-silver tests
- [x] Phase K4: NESO opt-in live API-to-silver tests, CLI smoke test, and close-out verification

Research: [NESO-CARBON-INTENSITY-RESEARCH.md](research/NESO-CARBON-INTENSITY-RESEARCH.md)

</details>

---

## Backlog

| Item | Source | Notes |
|------|--------|-------|
| GAP-03b: wind_solar_forecast psrType mapping (B16 -> solar, B18 -> wind_onshore, B19 -> wind_offshore) | v0.2 gap closure audit | Deferred - no gold consumers yet |
| Extend E2E coverage to GIE connectors | v0.5 close-out | ENTSO-E, Elexon, and ENTSOG are validated; GIE remains next connector candidate |
| Domain-specific ENTSOG silver schemas | v0.5 close-out | Add when downstream gas gold consumers require typed models beyond generic normalised records |
| Scheduled live endpoint monitoring | v0.5 close-out | Consider outside the normal unit-test suite for ENTSOG and other public APIs |
