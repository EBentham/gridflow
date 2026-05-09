# gridflow — Retrospective

---

## Milestone: v0.11-open-meteo-renewable-extension - Open-Meteo Renewable Extension

**Shipped:** 2026-05-09
**Phases:** 1 (F7.5) | **Plans:** 1 (4 commits)

### What Was Built

1. Role-split the Open-Meteo connector into 6 dataset families (demand/wind/solar × archive/forecast) via a `WeatherDatasetSpec` frozen dataclass lookup table.
2. Three new Pydantic schemas (`DemandWeather`, `WindWeather`, `SolarWeather`) replaced the monolithic `WeatherObservation`.
3. 25-site location coverage: 12 capacity-weighted wind sites, 6 solar sites, 7 existing demand population centres.
4. Archive wind variable list restricted to 10m + 100m after live ERA5 probe confirmed 80/120/180m return all-null.
5. Solar GTI parameters (`tilt=35, azimuth=180`) injected via `extra_params` tuple on the spec.
6. Air density derived via ideal-gas law for demand + wind; property-tested over 18 (T, P) combinations.
7. 74 net new tests; 1116 total passing after code-review fix sweep.

### What Worked

- **Live API probe before coding** (two curl calls to ERA5) directly shaped the archive variable list and saved a full backfill of null-filled data.
- **Advisor pre-execution review** caught that wave 1 alone couldn't stay green (test imports of deleted symbols) before any code was written, avoiding a broken intermediate commit.
- **Plan-time formula verification** at joint extremes widened the air density test band from [0.95, 1.40] to [0.95, 1.55] — would have been a test suite bug otherwise.
- **Code review** found two migration-sweep misses (`serving/client.py` and `scripts/run_all_sources.py`) that tests couldn't catch (string-literal references outside the grep scope).
- **Property tests + contract tests** gave structural guarantees (spec invariants, formula identity, irradiance decomposition) that example-based tests would have missed.

### What Was Inefficient

- **Migration grep scope too narrow:** Scoped to `connectors/openmeteo/`, `silver/openmeteo/`, and `cli.py`. Missed `serving/` and `scripts/` which carry dataset names as string literals. A repo-wide grep sweep after any hard rename would have caught both stragglers before code review.
- **Dead assert slipped through:** `test_openmeteo_air_density.py:107` had `assert ... if False else True` — always `True`. No linter caught it; code review did. A ruff rule for constant-condition assertions would close this gap.
- **Vault sync not possible in-session:** F7.5-VAULT-01 required `obsidian-vault` MCP access which wasn't available in this session. Vault requirements should be flagged as MCP-gated at phase start, not discovered at DoD time.

### Patterns Established

- `WeatherDatasetSpec` frozen dataclass as single source of truth for all per-dataset variation (locations, variables, request params). Applicable to any connector with N role-differentiated datasets sharing the same fetch/transform logic.
- `DERIVE_*` class variable pattern (e.g. `DERIVE_AIR_DENSITY`) for optional silver derivations. More declarative than branching on role name in base class.
- Live API verification at plan time as a first-class verification step, not an afterthought.
- Repo-wide string grep after any hard rename (`grep -r "'old_name'" src/ scripts/ tests/`).

### Key Lessons

- Pre-execution advisor review pays off most on refactors touching many test files simultaneously.
- Physical formula property tests should always be written after verifying bounds at joint extremes, not at typical values.
- Vault requirements that depend on external MCP servers must be flagged as conditional at phase-planning time.
- String-literal dataset names in serving code and scripts are invisible to import-graph analysis — only a repo-wide string grep catches migration stragglers.

### Cost Observations

- Sessions: 1 (extended worktree session spanning plan → execute → verify → review → fix → learnings → milestone)
- Notable: F7.5 was planned, executed, verified, code-reviewed, and closed in a single session using parallel agent spawning for verification and review.

---

## Milestone: v0.5-entsog-pipeline-validation - ENTSOG Pipeline Validation

**Shipped:** 2026-05-04
**Phases:** 4 (J1-J4) | **Plans:** Inline implementation

### What Was Built

1. Added ENTSOG endpoint research and an auditable endpoint catalog covering 33 active datasets.
2. Replaced one-off physical-flow request logic with metadata-driven bronze requests across operational, CMP/event, aggregated, tariff, UMM, and reference endpoint families.
3. Added generic ENTSOG JSON silver transformers while preserving the specialised physical-flow GWh/day normalisation.
4. Added mocked request-shape tests, fixture-backed bronze-to-silver tests, opt-in live API-to-silver tests, and isolated live CLI smoke coverage.
5. Fixed a live CMP auction premium regression where `isCAMRelevant` and `isCamRelevant` normalized to the same snake_case column.

### What Worked

- **Live API probing early** exposed ENTSOG's case-sensitive `/operationalData`, `timeZone`, exact indicator values, and mandatory `pointDirection` semantics before the connector hardened around old assumptions.
- **Endpoint metadata as source of truth** kept source config, connector request construction, docs, and tests aligned across a wide JSON API surface.
- **Generic silver with targeted hardening** gave broad endpoint coverage quickly while still allowing specialised physical-flow modelling where it already mattered.
- **Real user backfill validation** caught a sparse live payload collision that mocked fixtures did not initially cover.

### What Was Inefficient

- **No `gsd-sdk` in this runtime** meant milestone audit and archival had to be completed manually.
- **Inline implementation skipped normal per-phase SUMMARY artifacts**, so close-out relied on requirements, research, tests, and milestone archive notes.
- **The initial mocked CMP fixture was too tidy** and missed mixed field casing that appeared in live `cmp_auction_premiums` data.

### Patterns Established

- Public JSON connector milestones should include live payload shape probes for sparse fields, not only first-row examples.
- Generic transformer column normalization should be collision-aware whenever API fields vary by casing or punctuation.
- ENTSOG no-data outcomes should be explicit skips in live tests when request construction is valid and the API returns `404 No result found` or empty arrays.

### Key Lessons

- Exact casing matters for ENTSOG paths, parameters, and indicators; docs and tests should enforce casing rather than relying on API tolerance.
- A broad generic silver layer is useful for coverage, but live regressions should immediately become offline fixture regressions.
- Connector validation milestones are strongest when they end with one real CLI/backfill command that mirrors how the user will actually run the source.

---

## Milestone: v0.4-elexon-validation - Elexon Pipeline Validation

**Shipped:** 2026-05-04
**Phases:** 4 (I1-I4) | **Plans:** 4

### What Was Built

1. Added Elexon inventory contract tests across source config, endpoint definitions, explicit exclusions, and silver transformer registration.
2. Built mocked request-shape coverage for every active configured Elexon dataset plus fixture-backed bronze-to-silver tests for representative transformer families.
3. Added opt-in live API-to-silver tests using public no-key Elexon responses for `system_prices`, `boal`, `freq`, `pn`, and `bmunits_reference`.
4. Added live CLI smoke tests for `pipeline`, separate `ingest`/`transform`, and `backfill` under isolated temp-root `GRIDFLOW_*` paths.
5. Fixed runtime config precedence so environment overrides beat YAML pipeline paths during CLI runs.

### What Worked

- **Registry-driven inventory checks** avoided another hard-coded dataset list and made config, endpoint, and transformer drift visible.
- **Representative live coverage** kept the live suite fast while proving each major request style reaches silver parquet.
- **Non-live sentinels** made live-only test files still produce useful assertions in normal `-m "not live"` runs.
- **CLI temp-root isolation** turned a smoke-test discovery into a production config precedence fix.

### What Was Inefficient

- **The first I4 smoke run wrote ignored local `data/` artifacts** before the `GRIDFLOW_*` precedence bug was fixed.
- **`gsd-sdk` remained unavailable in this runtime**, so the milestone audit and archive steps needed direct `.planning/` handling.
- **Validation matrix status rows were not mechanically updated** even though phase summaries and verification artifacts record the checks as passed.

### Patterns Established

- For public no-key connectors, pair mocked request-shape coverage with a small opt-in live matrix that proves RawResponse, BronzeWriter, transformer, and parquet paths.
- Use environment overrides as the highest-precedence path isolation mechanism for CLI smoke tests.
- Keep removed, duplicate, empty, or intentionally excluded endpoints visible in source-owned manifests rather than hidden in tests.

### Key Lessons

- Live CLI smoke tests are valuable because they exercise config loading, command parsing, connector fetches, bronze writes, and silver transforms together.
- Mark only network-touching tests as `live`; keep documentation and exclusion assertions non-live so normal test selection stays meaningful.
- A connector-validation milestone should finish with archive-ready requirements before deleting the milestone-scoped `REQUIREMENTS.md`.

---

## Milestone: v0.3-entsoe-validation — ENTSO-E Pipeline Validation

**Shipped:** 2026-05-03
**Phases:** 9 (H1-H8) | **Plans:** 11

### What Was Built

1. Added CLI `all` positional alias handling and ENTSO-E mocked bronze-to-silver E2E coverage.
2. Built credential-gated live test scaffolding plus request-shape and medallion-path verification for ENTSO-E.
3. Reworked ENTSO-E request construction around documented endpoint parameter families and an auditable endpoint catalog.
4. Added generation/reference, transmission/market, outage, and balancing extension source families through metadata, parsers, schemas, transformers, fixtures, and tests.
5. Cleaned up live payload handling for zip XML responses, live tag variants, fixed-date no-data acknowledgements, and high-volume bid/capacity paging.

### What Worked

- **Endpoint catalog as source of truth** kept a large source expansion reviewable and made every implemented, planned, deferred, or excluded row visible.
- **Family-batched phases** let parser/schema risk stay isolated across generation, transmission, outage, and balancing domains.
- **Live cleanup before further expansion** prevented H6-H8 from building on invalid active-source metadata and brittle payload assumptions.
- **Request-shape live probes** gave useful API compatibility evidence without requiring every full bronze-to-silver live path to run in every phase.

### What Was Inefficient

- **H3 credential dependency remained open** until later phases produced stronger live evidence; the original H3 verification artifact still needed explicit deferral at close.
- **`gsd-sdk` was unavailable in this runtime**, so several phase and milestone workflows needed inline fallback handling.
- **Endpoint volume grew quickly**, making hard-coded test assumptions brittle; the final test amendment removed the old fixed ENTSO-E dataset count assertion.

### Patterns Established

- ENTSO-E endpoint metadata owns document type, domain parameter style, optional filters, date parameter variants, and default live paging behavior.
- New ENTSO-E source batches should include catalog row, parser/schema support, transformer registration, fixture-backed unit coverage, mocked E2E, and live request-shape coverage.
- Genuine ENTSO-E no-data acknowledgements are test outcomes, not parser failures, when the request shape is valid.

### Key Lessons

- Avoid fixed dataset-count assertions in tests for catalog-driven connectors; assert registry/config alignment instead.
- Keep deferred official endpoints explicit with owner batch and rationale, especially when the payload family needs separate modelling.
- Live test records need a single canonical close-out path so later passing live gates can retire earlier human-needed artifacts cleanly.

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

| Metric | v0.2-entsoe-gaps | v0.3-entsoe-validation | v0.4-elexon-validation | v0.5-entsog-pipeline-validation |
|--------|-----------------|------------------------|------------------------|----------------------------------|
| Phases | 4 | 9 | 4 | 4 |
| Plans | 5 | 11 | 4 | Inline |
| Tests (final) | 551 | 378 non-live gate; 97 final focused | 81 non-live; 5 live CLI; 5 live API-to-silver | 857 non-live; 26 live API-to-silver; 1 live CLI; targeted backfill passed |
| Files changed | 43 | 137 | 39 | 20+ |
| Nyquist gaps found | 6 (G3: 4, G4: 2) | n/a | n/a | n/a |
| Nyquist gaps resolved | 6 | n/a | n/a | n/a |
| Deferred items | 1 (GAP-03b) | 4 close-out artifacts | 0 new | 3 future follow-ups |
