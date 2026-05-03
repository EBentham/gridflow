---
phase: H3
plan: 02
type: execute
wave: 2
depends_on:
  - H3-01
files_modified:
  - tests/integration/test_entsoe_live.py
autonomous: true
requirements:
  - LIVE-01
  - LIVE-02
  - LIVE-03

must_haves:
  truths:
    - "`pytest -m live tests/integration/test_entsoe_live.py -x -q` fetches real ENTSO-E data for all 16 datasets when `ENTSOE_API_KEY` is present."
    - "Live tests auto-skip without `ENTSOE_API_KEY` and do not run accidentally in default non-live pytest runs."
    - "Every live fetch failure hard-fails with useful dataset/stage diagnostics after opt-in."
    - "Live bronze-to-silver coverage writes real fetched XML through `BronzeWriter` and runs real ENTSO-E transformers."
    - "Command-level live coverage includes `gridflow pipeline entsoe all --last 24h` and isolates output paths under pytest temp directories."
    - "Command-level live failures are treated as test failures, not hidden behind zero-exit CLI output."
  artifacts:
    - path: "tests/integration/test_entsoe_live.py"
      provides: "All-dataset live ENTSO-E connector, transform, and CLI command coverage"
      exports: ["TestEntsoeLiveAllDatasets", "TestEntsoeLiveCliCommands"]
  key_links:
    - from: "tests/integration/test_entsoe_live.py"
      to: "src/gridflow/connectors/entsoe/client.py"
      via: "EntsoeConnector.fetch"
      pattern: "live async fetch"
    - from: "tests/integration/test_entsoe_live.py"
      to: "src/gridflow/silver/entsoe/__init__.py"
      via: "transformer registration import"
      pattern: "get_transformer"
    - from: "tests/integration/test_entsoe_live.py"
      to: "src/gridflow/cli.py"
      via: "gridflow pipeline/ingest/transform commands"
      pattern: "CliRunner or subprocess command"
---

<objective>
Add the real live ENTSO-E E2E suite.

Purpose: satisfy LIVE-01, LIVE-02, and LIVE-03 by proving all 16 ENTSO-E datasets can
fetch real API responses, write bronze, transform to silver, and run through common CLI
commands when the developer opts in with `pytest -m live` and provides `ENTSOE_API_KEY`.

Output:
- `tests/integration/test_entsoe_live.py` contains all-dataset live connector and
  bronze-to-silver tests.
- The same module contains command-level live coverage for `pipeline`, plus safe `ingest`
  and `transform` command paths where feasible.
</objective>

<execution_context>
@.planning/PROJECT.md
@.planning/ROADMAP.md
@.planning/REQUIREMENTS.md
@.planning/STATE.md
@.planning/phases/H3-live-entsoe-test-suite/H3-CONTEXT.md
@.planning/phases/H3-live-entsoe-test-suite/H3-RESEARCH.md
@.planning/phases/H3-live-entsoe-test-suite/H3-PATTERNS.md
@.planning/phases/H3-live-entsoe-test-suite/H3-01-PLAN.md
@tests/integration/test_entsoe_live.py
@src/gridflow/connectors/entsoe/client.py
@src/gridflow/connectors/entsoe/endpoints.py
@src/gridflow/silver/entsoe/__init__.py
@src/gridflow/silver/registry.py
</execution_context>

<context>
All live tests in this plan must be marked `@pytest.mark.live` and must require
`ENTSOE_API_KEY`. After opt-in and key presence, do not skip/xfail API, no-data,
parse, transform, or command failures.

Slower tests are acceptable. The goal is real end-to-end confidence, not a cheap smoke test.
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Add all-16 live connector coverage</name>
  <read_first>
    - tests/integration/test_entsoe_live.py
    - src/gridflow/connectors/entsoe/client.py
    - src/gridflow/connectors/entsoe/endpoints.py
    - config/sources.yaml
  </read_first>
  <files>tests/integration/test_entsoe_live.py</files>
  <action>
Add `TestEntsoeLiveAllDatasets`.

Required tests:

1. `test_live_config_and_doc_types_cover_same_16_datasets`
   - marked live;
   - asserts configured ENTSO-E datasets match `DOC_TYPES` and length 16.

2. `test_live_fetch_returns_real_xml_for_every_dataset`
   - marked live, async, parametrized over `sorted(DOC_TYPES)`;
   - obtains real source config through `load_settings().get_source_config("entsoe")`;
   - fails with a clear message if API key is missing despite marker setup;
   - calls `EntsoeConnector.fetch(dataset, start, end)` with a deliberate live date window;
   - asserts responses are non-empty and pass `_assert_live_responses`.

Date-window guidance:
- Use a shared default window if it works for all datasets.
- If some datasets require different realistic windows, define a `LIVE_WINDOWS` mapping keyed
  by dataset and document why in a comment.
- Do not silently accept no-data responses; update the window or fail with diagnostics.
  </action>
  <verify>
    <automated>uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - All 16 `DOC_TYPES` datasets are parametrized
    - Real API responses are fetched for every dataset
    - Empty responses or empty bodies fail hard
    - No API key values appear in assertion output
  </acceptance_criteria>
  <done>All 16 ENTSO-E datasets have live fetch coverage.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Add live bronze-to-silver coverage for all registered transformers</name>
  <read_first>
    - tests/integration/test_entsoe_live.py
    - src/gridflow/silver/entsoe/__init__.py
    - src/gridflow/silver/registry.py
    - src/gridflow/silver/base.py
  </read_first>
  <files>tests/integration/test_entsoe_live.py</files>
  <action>
Extend live tests so fetched real XML is written to bronze and transformed to silver.

Implementation guidance:
- Import `gridflow.silver.entsoe` to trigger all ENTSO-E transformer registrations.
- Use `get_transformer("entsoe", dataset, tmp_data_dir)`.
- For each dataset in `sorted(DOC_TYPES)`, fetch live responses, write them with
  `_write_live_responses_to_bronze`, run `transformer.run(target_date)`, and assert silver output
  via `_assert_silver_output`.
- Use `tmp_data_dir`; do not write to normal `data/`.
- If a transformer returns zero rows for a live non-empty response, fail with dataset/stage
  diagnostics. Do not xfail.

If one or more registered datasets have no transformer despite being in `DOC_TYPES`, that is an
H3 failure to surface, not a skip.
  </action>
  <verify>
    <automated>uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - Real live responses go through `BronzeWriter`
    - All 16 registered ENTSO-E datasets attempt real transformer `run()`
    - Silver parquet exists for every dataset
    - Output row counts match transformer return values
    - Failures identify dataset and transform stage
  </acceptance_criteria>
  <done>All 16 ENTSO-E datasets have live bronze-to-silver coverage.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Add command-level live coverage for common ENTSO-E CLI paths</name>
  <read_first>
    - tests/integration/test_entsoe_live.py
    - src/gridflow/cli.py
    - src/gridflow/config/settings.py
  </read_first>
  <files>tests/integration/test_entsoe_live.py</files>
  <action>
Add `TestEntsoeLiveCliCommands`.

Required live command tests:
- `test_pipeline_entsoe_all_last_24h_live`
  - runs the command equivalent of `gridflow pipeline entsoe all --last 24h`;
  - uses a temp config/data root;
  - asserts exit code 0;
  - asserts output does not contain `FAILED`;
  - asserts bronze and silver output exists for all 16 datasets.

- `test_ingest_entsoe_all_last_24h_live`
  - runs `gridflow ingest entsoe all --last 24h` or direct equivalent;
  - asserts non-zero failures are not hidden and bronze output exists.

- `test_transform_entsoe_all_last_24h_live`
  - after live ingest, runs `gridflow transform entsoe all --last 24h` or direct equivalent;
  - asserts silver output exists for all 16 datasets.

Use `typer.testing.CliRunner` if it can isolate cwd and env cleanly. If `CliRunner` cannot reflect
installed command behavior well enough, use `uv run gridflow ...` through subprocess from the temp
cwd. In either case, keep `ENTSOE_API_KEY` out of captured failure output.
  </action>
  <verify>
    <automated>uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - `gridflow pipeline entsoe all --last 24h` is covered
    - Ingest and transform command paths are covered where safe
    - Commands use temp data/log/DuckDB paths
    - Command failures produce non-zero test failures with useful output
    - Command outputs do not leak `ENTSOE_API_KEY`
  </acceptance_criteria>
  <done>Common ENTSO-E CLI commands have opt-in live coverage.</done>
</task>

<task type="auto">
  <name>Task 4: Run final H3 verification and create summary</name>
  <read_first>
    - .planning/phases/H3-live-entsoe-test-suite/H3-VALIDATION.md
    - .planning/STATE.md
  </read_first>
  <files>.planning/phases/H3-live-entsoe-test-suite/H3-02-SUMMARY.md</files>
  <action>
Run:

```powershell
uv run --extra dev pytest tests/integration/test_entsoe_live.py -m "not live" -x -q
uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py -x -q
uv run --extra dev pytest tests/integration/test_entsoe_live.py tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_connector.py tests/unit/test_entsoe.py -x -q
uv run --extra dev pytest -x -q
```

If `ENTSOE_API_KEY` is absent, record that live verification could not be completed and mark the
summary self-check failed or human_needed rather than pretending H3 passed.

If the full suite fails only because of the known Elexon import blocker, record that as a
pre-existing blocker. Do not edit Elexon files in H3.

Create `H3-02-SUMMARY.md` after implementation using the project summary template.
  </action>
  <verify>
    <automated>Test-Path .planning/phases/H3-live-entsoe-test-suite/H3-02-SUMMARY.md</automated>
  </verify>
  <acceptance_criteria>
    - Non-live H3 checks pass
    - Live H3 gate passes when `ENTSOE_API_KEY` is present
    - ENTSO-E focused regression command passes
    - Full-suite attempt is recorded honestly
    - Summary lists commits and verification outputs
  </acceptance_criteria>
  <done>Phase verification recorded.</done>
</task>

</tasks>

<verification>

Before H3 is marked complete:

1. `uv run --extra dev pytest tests/integration/test_entsoe_live.py -m "not live" -x -q` passes without credentials.
2. `uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py -x -q` passes with `ENTSOE_API_KEY`.
3. `uv run --extra dev pytest tests/integration/test_entsoe_live.py tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_connector.py tests/unit/test_entsoe.py -x -q` passes, or any remaining failure is documented as a non-H3 environment issue.
4. `uv run --extra dev pytest -x -q` is attempted and result recorded.
5. No live API key or secret value is written to docs, logs, assertions, or committed files.

</verification>

<success_criteria>

- LIVE-01: Live test suite fetches real data for all 16 ENTSO-E datasets when `ENTSOE_API_KEY` is set.
- LIVE-02: Fetched live XML responses parse and transform to silver through real production paths.
- LIVE-03: Live tests are opt-in with `@pytest.mark.live` and skip automatically when the API key is absent.
- Command-level coverage catches failures in `gridflow pipeline entsoe all --last 24h`.
- H3 stays ENTSO-E-only and leaves Elexon live coverage for the follow-up project.

</success_criteria>
