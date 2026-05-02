# gridflow — Retrospective

---

## Milestone: v0.2-entsoe-gaps — ENTSO-E Extension Gap Closure

**Shipped:** 2026-05-02
**Phases:** 4 (G1–G4) | **Plans:** 5

### What Was Built

1. Fixed Phase 3 bronze read paths and added connector integration tests (G1)
2. Applied 5 targeted schema corrections for Phase 1/2 datasets (G2)
3. Mapped all Phase 3 balancing A-codes to semantic strings; corrected currency field names (G3)
4. Redesigned outages_generation as a unit-level silver schema via XML parser extension (G4)

### What Worked

- **Milestone audit before planning** (gsd-audit-milestone) surfaced all 10 gaps and prioritised them into 4 phases — execution was focused and sequential with zero scope creep
- **replace_strict as a data-quality gate** — using Polars `replace_strict` for A-code mapping means unknown codes surface immediately at transform time rather than silently passing through as raw strings
- **output_cols / available_cols pattern** — final `df.select([c for c in output_cols if c in df.columns])` allowed the parser to emit new fields (unit_mrid, unit_name) without breaking any of the 15 non-A80 transformers
- **Nyquist validation (gsd-validate-phase)** — running validation after G3 and G4 caught missing dedup tests (G3: 4 gaps) and missing parser key assertions (G4: 2 gaps) before archiving; the automated test net is now dense

### What Was Inefficient

- **G1 predates .planning/ setup** — no SUMMARY.md or VERIFICATION.md exists for G1; its correctness is confirmed only by integration test assertions and git history; retroactive documentation is possible but was not done
- **G3/G4 VERIFICATION.md absent** — formal `gsd-verify-phase` was not run for these phases; self-check evidence (passing test suite, grep assertions) was accepted as sufficient; future milestones should run verification before audit
- **Schema guard added post-audit** — the outages_generation transformer was the only one of 16 ENTSO-E transformers missing the `SchemaClass(**sample)` runtime contract check; this should have been caught in the G4 plan review

### Patterns Established

- **All ENTSO-E silver transformers call `SchemaClass(**sample)` on the first output row** — this is now the verified convention for the full transformer fleet
- **Nyquist validation cadence** — run gsd-validate-phase immediately after execute-phase, before audit-milestone; gaps found post-audit are fixable but add a cleanup loop
- **Fixture XMLs should include code variants** — A-code mapping tests require XML fixtures with multiple code values; updated fixture strategy (multiple TimeSeries per fixture) is the right pattern

### Key Lessons

- When extending a shared parser (`parse_timeseries_xml`), verify backward-compat with a dedicated test asserting empty-string defaults on non-target document types — not just that the new feature works on the target fixture
- A milestone audit with `tech_debt` status (not `passed`) is fine to proceed with — the tech_debt classification is a signal to document, not a blocker
- GAP-03b (psrType semantic mapping) is a recurring backlog item; if another ENTSO-E milestone is planned, include it early before other code stabilises around raw B-codes

---

## Cross-Milestone Trends

| Metric | v0.2-entsoe-gaps |
|--------|-----------------|
| Phases | 4 |
| Plans | 5 |
| Tests (final) | 551 |
| Files changed | 43 |
| Nyquist gaps found | 6 (G3: 4, G4: 2) |
| Nyquist gaps resolved | 6 |
| Deferred items | 1 (GAP-03b) |
