# Phase H3: Live ENTSO-E test suite - Context

**Gathered:** 2026-05-02
**Status:** Ready for planning

<domain>
## Phase Boundary

H3 delivers opt-in live ENTSO-E test coverage that proves the connector, CLI commands,
bronze writes, XML parsing, and silver transforms work against the real ENTSO-E API.
This phase is intentionally ENTSO-E-only. Equivalent live/E2E work for Elexon is important
but belongs to a follow-up project.

</domain>

<decisions>
## Implementation Decisions

### Live Coverage Scope
- **D-01:** Live coverage should exercise all 16 configured/registered ENTSO-E datasets,
  not only a representative subset. Slower tests are acceptable because the purpose is
  to catch real API and command failures.
- **D-02:** H2 already proves URL shape with mocked HTTP; H3 should prove that real
  requests, real responses, and downstream parsing/transformation paths actually work.

### Command-Level Coverage
- **D-03:** H3 must include CLI-level live tests for commonly used commands, especially
  `gridflow pipeline entsoe all --last 24h`, because current failures are observed when
  running the command rather than only at isolated connector level.
- **D-04:** Planner should include live command coverage for the normal ENTSO-E workflow
  surface: at minimum `pipeline`, and likely `ingest` and `transform` if those commands
  can be exercised safely through temporary data directories.

### Failure Policy
- **D-05:** Live tests should hard fail with useful diagnostics rather than skip/xfail
  external-service conditions after they have opted in and an API key is present.
- **D-06:** Useful diagnostics should distinguish auth/key problems, HTTP errors, rate
  limits, empty/no-data responses, malformed XML, bronze write failures, and transform
  failures as clearly as possible.
- **D-07:** Tests should still auto-skip when `ENTSOE_API_KEY` is absent, preserving the
  locked requirement that live tests are opt-in and not accidentally run in CI.

### Safety and Runtime
- **D-08:** Slower live tests are acceptable if needed for real coverage. The planner
  may still use conservative date windows, temporary data roots, and pytest markers to
  keep the suite safe and explicit.
- **D-09:** Live tests must not write into the developer's normal production data
  directory. Use temporary data roots or explicit test isolation for bronze/silver output.

### the agent's Discretion
- The planner may choose the exact live date window(s), retry/pacing strategy, and helper
  structure, provided all 16 ENTSO-E datasets are covered and command-level failures are
  surfaced.
- The planner may decide whether full bronze-to-silver verification for every dataset is
  practical in one test file or should be split into connector-level, CLI-level, and
  transformer-level live checks.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Project Scope and Requirements
- `.planning/ROADMAP.md` - Defines H3 scope, success criteria, and ENTSO-E-only boundary.
- `.planning/REQUIREMENTS.md` - Defines LIVE-01, LIVE-02, and LIVE-03.
- `.planning/PROJECT.md` - Captures project constraints, live-test opt-in decision, and H2 state.
- `.planning/STATE.md` - Current milestone position and known full-suite blocker.

### Prior Phase Artifacts
- `.planning/phases/H2-entsoe-mocked-e2e-tests/H2-01-SUMMARY.md` - H2 mocked coverage already completed.
- `.planning/phases/H2-entsoe-mocked-e2e-tests/H2-VERIFICATION.md` - Shows H2 coverage and the unrelated Elexon full-suite blocker.
- `.planning/phases/H2-entsoe-mocked-e2e-tests/H2-VALIDATION.md` - Contains H2 validation commands and testing expectations.

### Codebase Maps
- `.planning/codebase/TESTING.md` - Existing pytest, respx, fixture, integration, and contract-test patterns.
- `.planning/codebase/CONVENTIONS.md` - Import, naming, error handling, and logging conventions.
- `.planning/codebase/STRUCTURE.md` - Where connector, CLI, fixture, and transformer code lives.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `tests/integration/test_entsoe_mocked_e2e.py`: H2's structure can seed H3 helpers for
  ENTSO-E dataset iteration, bronze writing, and transformer assertions.
- `tests/conftest.py`: Provides `tmp_data_dir`, which should be used to avoid live tests
  writing into normal data directories.
- `src/gridflow/connectors/entsoe/client.py`: Public `EntsoeConnector.fetch()` surface
  should be exercised with real API calls.
- `src/gridflow/connectors/entsoe/endpoints.py`: `DOC_TYPES` is the authoritative 16-dataset
  registry to drive all-dataset live coverage.
- `src/gridflow/cli.py`: Command-level tests should cover the same surface users run,
  especially `pipeline entsoe all --last 24h`.

### Established Patterns
- Tests use pytest with behavior-oriented names and native `assert`.
- Live tests must use the existing `@pytest.mark.live` marker from `pyproject.toml`.
- Integration tests should use real production code for parsing and transformations rather
  than mocking silver behavior.
- Filesystem pipeline tests should write through production helpers and read generated
  parquet where practical.

### Integration Points
- Live API key comes from `ENTSOE_API_KEY`.
- Settings are loaded through `load_settings()` and source config resolution.
- Bronze writes go through `BronzeWriter`; silver runs go through concrete transformer
  classes or CLI command flow.
- CLI command tests should isolate `data_dir` and avoid touching the developer's normal
  configured output paths.

</code_context>

<specifics>
## Specific Ideas

- User explicitly wants full end-to-end testing because running real commands currently
  does not work reliably.
- User explicitly approved slower live tests if that is what it takes to ensure all bases
  are covered.
- User specifically named `gridflow pipeline entsoe all --last 24h` as a command that must
  be tested, along with other commonly used ENTSO-E CLI commands.
- User prefers hard failures with useful diagnostics after opting into live tests.

</specifics>

<deferred>
## Deferred Ideas

- Add equivalent command-level live/E2E coverage for Elexon in a follow-up project. The user
  mentioned Elexon command failures as an example of the same testing gap, but H3 remains
  ENTSO-E-only.

</deferred>

---

*Phase: H3-live-entsoe-test-suite*
*Context gathered: 2026-05-02*
