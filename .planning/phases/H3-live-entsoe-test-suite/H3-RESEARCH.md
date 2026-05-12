---
phase: H3
slug: live-entsoe-test-suite
status: complete
created: 2026-05-02
---

# Phase H3 - Research

## Research Goal

Plan the live ENTSO-E test phase so it catches the real failure mode the user sees when
running `gridflow pipeline entsoe all --last 24h`, while keeping live API usage opt-in,
diagnostic, and isolated from normal local data output.

## Relevant Existing Behavior

- `pyproject.toml` already defines a `live` pytest marker.
- `load_settings()` resolves `ENTSOE_API_KEY` into the ENTSO-E source config through
  `PipelineSettings.entsoe_api_key` and `SourceConfig.api_key_env`.
- `EntsoeConnector.fetch()` returns one or more `RawResponse` objects per dataset,
  depending on zone, control-area, or zone-pair behavior.
- `DOC_TYPES` in `src/gridflow/connectors/entsoe/endpoints.py` is the authoritative
  registry for the 16 ENTSO-E datasets.
- `tests/integration/test_entsoe_mocked_e2e.py` already proves URL shape for all 16
  datasets and fixture-backed bronze-to-silver coverage for representative transformers.
- `ingest()` and `transform()` currently catch per-dataset exceptions and continue; this
  can hide failures from command-level tests unless the CLI is changed to exit non-zero
  when any dataset fails.

## Planning Implications

- H3 should add a real live test module, likely `tests/integration/test_entsoe_live.py`,
  rather than mixing live tests into the mocked H2 module.
- Tests should auto-skip only when `ENTSOE_API_KEY` is absent. Once the marker is selected
  and the key is present, live API failures should fail hard with detailed diagnostics.
- The live all-dataset test should iterate over `sorted(DOC_TYPES)` and assert config
  alignment with the 16 configured datasets before making requests.
- Command-level coverage should use temporary config/data roots so `gridflow pipeline`,
  `gridflow ingest`, and `gridflow transform` do not write into the developer's normal
  `./data`, `./logs`, or DuckDB path.
- Because the CLI loads settings inside command functions, command tests can isolate output
  by running under a temporary working directory containing a copied `config/` directory
  with rewritten `settings.yaml` paths.
- The plan should include a small CLI failure-propagation fix so per-dataset failures result
  in non-zero command exits and actionable summaries.

## Failure Diagnostics

Live tests should include helper assertions/messages that identify:

- missing API key vs opted-in test failure;
- HTTP status/auth/rate-limit failures;
- empty response lists;
- empty response bodies;
- XML parse failures or zero parsed records;
- bronze write failures;
- missing transformer registrations;
- zero-row silver transform output;
- CLI command output that contains `FAILED`.

## Security and Secret Handling

- Never write or print the ENTSO-E API key.
- Do not include `.env` contents in tests, docs, or summaries.
- Live tests should read only the presence/value through environment resolution and use
  redacted diagnostics.
- Temporary data roots should keep live API outputs out of committed fixture directories.

## Validation Architecture

H3 validation should use layered gates:

1. Non-live unit/integration checks prove helper behavior, skip behavior, and CLI failure
   propagation without requiring credentials.
2. `pytest -m live tests/integration/test_entsoe_live.py -x -q` is the H3 live gate and
   requires `ENTSOE_API_KEY`.
3. Command-level live tests must include `gridflow pipeline entsoe all --last 24h`.
4. Full-suite attempt remains expected to hit the unrelated Elexon silver package import
   blocker until that follow-up work is addressed.

## Open Risks for Execution

- Some ENTSO-E datasets may not return usable data for a naive `last 24h` window. The
  executor should use either dataset-specific stable windows or a conservative recent window
  strategy, but no-data should be treated as a failing diagnostic after opt-in.
- Running all 16 datasets can be slow and API-sensitive. That is acceptable per user decision,
  but the implementation should keep the command explicit with `-m live`.
- Current dirty worktree edits in CLI/config/logging may interact with H3 files. Executor
  must inspect current file contents before editing and preserve unrelated changes.

## RESEARCH COMPLETE
