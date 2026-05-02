# gridflow — Milestone History

---

## v0.2-entsoe-gaps — ENTSO-E Extension Gap Closure

**Shipped:** 2026-05-02
**Phases:** G1–G4 (4 phases, 5 plans)
**Commits:** 21 | **Files:** 43 changed | **Lines:** +5213 / −67
**Test suite:** 551 passed, 44 deselected (live)

### Delivered

Closed all 10 gap-IDs from the ENTSO-E connector extension audit, transforming a
partially-correct ENTSO-E silver layer into a fully spec-compliant one.

### Key Accomplishments

1. **G1 — Bronze path + connector tests:** Fixed doubled `read_bronze()` path in all 5 Phase 3 transformers; added 4-test integration suite for `_fetch_control_area` parameter correctness
2. **G2 — Schema corrections:** Applied 5 targeted fixes — `forecast_horizon` literal fields for load forecasts, `generation_forecast_mw` and `capacity_mw` renames, `process_type=None` for imbalance_volume
3. **G3 — Balancing code mapping:** Eliminated all raw ENTSO-E A-codes from Phase 3 silver; direction and reserve_type now emit semantic strings ("long"/"short", "fcr"/"afrr"/"mfrr"/"rr", "up"/"down"); `price_eur_mwh` correctly named; `ingested_at` added to all 5 datasets
4. **G4 — Unit-level outages:** Parser extended to extract `RegisteredResource.mRID`/`name` from A80 XML (backward-compat); `outages_generation` silver redesigned to unit-level schema with `outage_type` A-code mapping

### Deferred (Backlog)

- GAP-03b: wind_solar_forecast psrType semantic mapping (B16→solar, B18→wind_onshore, B19→wind_offshore)

### Archive

- [v0.2-entsoe-gaps-ROADMAP.md](milestones/v0.2-entsoe-gaps-ROADMAP.md)
- [v0.2-entsoe-gaps-MILESTONE-AUDIT.md](milestones/v0.2-entsoe-gaps-MILESTONE-AUDIT.md)

---
