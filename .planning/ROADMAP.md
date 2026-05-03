# gridflow — Roadmap

---

## Milestones

- ✅ **v0.2-entsoe-gaps** — Gap Closure G1–G4 (shipped 2026-05-02)
- ✅ **v0.3-entsoe-validation** — ENTSO-E Pipeline Validation H1–H8 (shipped 2026-05-03)

---

## Phases

<details>
<summary>✅ v0.2-entsoe-gaps — ENTSO-E Extension Gap Closure (G1–G4) — SHIPPED 2026-05-02</summary>

- [x] Phase G1: Fix Phase 3 bronze read path + connector test — ✅ Done
- [x] Phase G2: Minor schema corrections (GAP-01/02/03a/05/08) — ✅ Done 2026-04-30
- [x] Phase G3: Map balancing codes to semantic values (GAP-06/07) — ✅ Done 2026-05-02
- [x] Phase G4: outages_generation unit-level schema (GAP-04) — ✅ Done 2026-05-02

See full details: [milestones/v0.2-entsoe-gaps-ROADMAP.md](milestones/v0.2-entsoe-gaps-ROADMAP.md)

</details>

---

<details>
<summary>✅ v0.3-entsoe-validation — ENTSO-E Pipeline Validation (H1-H8) — SHIPPED 2026-05-03</summary>

- [x] Phase H1: Fix CLI `all` positional alias — completed 2026-05-02
- [x] Phase H2: ENTSO-E mocked E2E tests — completed 2026-05-02
- [x] Phase H3: Live ENTSO-E test suite scaffolding and credential-gated all-dataset coverage — implementation complete; credentialed close-out deferred
- [x] Phase H4: ENTSO-E endpoint catalog + request builder correction — completed 2026-05-03
- [x] Phase H5: ENTSO-E generation unit and reference data sources — completed 2026-05-03
- [x] Phase H5.5: ENTSO-E live cleanup before H6 — completed 2026-05-03
- [x] Phase H6: ENTSO-E transmission and market data sources — completed 2026-05-03
- [x] Phase H7: ENTSO-E outage extension data sources — completed 2026-05-03
- [x] Phase H8: ENTSO-E balancing extension data sources — completed 2026-05-03

See full details: [milestones/v0.3-entsoe-validation-ROADMAP.md](milestones/v0.3-entsoe-validation-ROADMAP.md)

Known deferred close-out items: 4; see [STATE.md](STATE.md).

</details>

---

## Backlog

| Item | Source | Notes |
|------|--------|-------|
| GAP-03b: wind_solar_forecast psrType mapping (B16→solar, B18→wind_onshore, B19→wind_offshore) | v0.2 gap closure audit | Deferred — no gold consumers yet |
| Extend E2E coverage to Elexon, ENTSO-G, GIE connectors | v0.3 scope decision | ENTSO-E first; other connectors deferred |
