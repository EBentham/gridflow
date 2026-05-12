---
phase: I4
slug: elexon-cli-backfill-live-smoke-tests-and-milestone-close-out-docs
status: complete
created: 2026-05-04
requirements:
  - ELEXON-CLI-01
  - ELEXON-CLI-02
  - ELEXON-CLI-03
  - ELEXON-DOC-01
  - ELEXON-DOC-02
---

# I4 Research - Elexon CLI/Backfill Live Smoke Tests and Close-Out Docs

## RESEARCH COMPLETE

Phase I4 should prove the user-facing Typer CLI can run the already-validated
Elexon medallion path through `pipeline`, `ingest`, `transform`, and `backfill`
against the public no-key Elexon Insights API. The tests must stay opt-in with
`@pytest.mark.live`, isolate all outputs under pytest temp roots, and document
the live commands, chosen datasets/windows, expected skips, and traceability.

## Source Artifacts Read

- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/I1-elexon-inventory-test-scaffolding/I1-01-SUMMARY.md`
- `.planning/phases/I2-elexon-mocked-request-shape-and-fixture-backed-bronze-to-silver-tests/I2-01-SUMMARY.md`
- `.planning/phases/I3-elexon-live-api-to-silver-test-suite/I3-01-SUMMARY.md`
- `.planning/phases/I3-elexon-live-api-to-silver-test-suite/I3-01-PLAN.md`
- `CLAUDE.md`
- `config/sources.yaml`
- `src/gridflow/cli.py`
- `src/gridflow/config/settings.py`
- `tests/conftest.py`
- `tests/integration/test_elexon_live_e2e.py`
- `tests/integration/test_elexon_mocked_e2e.py`
- `tests/unit/test_cli_resolve_datasets.py`

## Key Findings

### CLI Surface

`src/gridflow/cli.py` exposes the I4 target commands:

- `gridflow ingest <source> <dataset> --start YYYY-MM-DD --end YYYY-MM-DD`
- `gridflow transform <source> <dataset> --start YYYY-MM-DD --end YYYY-MM-DD`
- `gridflow pipeline <source> <dataset> --start YYYY-MM-DD --end YYYY-MM-DD`
- `gridflow backfill <source> <dataset> --start YYYY-MM-DD --end YYYY-MM-DD --chunk-days N`

The CLI does not currently expose `--data-dir`, `--duckdb-path`, or config-path
options. Isolation should therefore use `PipelineSettings` environment overrides:

- `GRIDFLOW_DATA_DIR`
- `GRIDFLOW_DUCKDB_PATH`
- `GRIDFLOW_LOG_DIR`

`load_settings()` still reads the repository `config/sources.yaml`, so live smoke
tests should pass explicit dataset names rather than `--all`.

### Existing Failure Semantics

I3/H3 established that ingest and transform should attempt requested datasets,
record per-dataset failures, and exit non-zero when real dataset errors occur.
I4 should assert this behavior through the CLI rather than bypassing it with
direct connector/transformer calls.

### Live Dataset Selection

Use the smallest stable public Elexon set that exercises the relevant command
paths:

| Dataset | Request style | Role |
| --- | --- | --- |
| `system_prices` | `DATE_PATH` | Fast `pipeline` smoke and path-date backfill case |
| `freq` or `boal` | `PUBLISH_DATETIME` | Separate `ingest` then `transform`, plus publish/from-to backfill case |
| `bmunits_reference` | `NO_PARAMS` | Reference/no-param backfill case and non-date silver path |

`pn` is useful for settlement-period connector coverage but is not required for
the I4 CLI/backfill requirements because I3 already proved it through live
API-to-silver tests.

### Test Harness Pattern

Use `typer.testing.CliRunner` against `gridflow.cli.app` so assertions can inspect
exit codes and output without shell quoting fragility. Keep the tests functionally
equivalent to the documented CLI commands:

- live-marked test functions call the Typer app with the same arguments users run;
- environment variables isolate data, DuckDB, and logs under `tmp_path`;
- assertions inspect `bronze/elexon/<dataset>` and `silver/elexon/<dataset>` under
  the temp data root;
- assertions include command output on failure.

For one true subprocess smoke, use `uv run gridflow --help` or a documented manual
command only if the executor decides the installed entry point needs coverage.
The critical behavior can be covered more reliably through `CliRunner`.

## Validation Architecture

Fast non-live guard:

```powershell
uv run --extra dev pytest tests/integration/test_elexon_cli_live_smoke.py -m "not live" -q
```

This should pass via a non-live documentation/traceability sentinel and deselect
the real live CLI smoke tests.

Live verification:

```powershell
uv run --extra dev pytest tests/integration/test_elexon_cli_live_smoke.py -m live -q -rs
```

Regression guard:

```powershell
uv run --extra dev ruff check tests/integration/test_elexon_cli_live_smoke.py tests/integration/test_elexon_live_e2e.py tests/integration/test_elexon_mocked_e2e.py tests/unit/test_cli_resolve_datasets.py
uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py tests/unit/test_cli_resolve_datasets.py -m "not live" -q
```

Documentation verification:

```powershell
Test-Path .planning/phases/I4-elexon-cli-backfill-live-smoke-tests-and-milestone-close-out-docs/I4-LIVE-COMMANDS.md
Select-String -Path .planning/phases/I4-elexon-cli-backfill-live-smoke-tests-and-milestone-close-out-docs/I4-LIVE-COMMANDS.md -Pattern "pipeline","ingest","transform","backfill","system_prices","bmunits_reference"
```

## Risks and Mitigations

| Risk | Mitigation |
| --- | --- |
| Live CLI tests write into the normal `data/` directory. | Set `GRIDFLOW_DATA_DIR`, `GRIDFLOW_DUCKDB_PATH`, and `GRIDFLOW_LOG_DIR` per test and assert all created artifacts are under `tmp_path`. |
| No-param reference data does not fit date-loop assumptions in backfill. | Include `bmunits_reference` explicitly and document any current CLI limitation if execution exposes one; fix CLI only if needed to satisfy the smoke requirement. |
| Live service drift makes the close-out flaky. | Keep tests opt-in, use narrow deterministic windows, include output diagnostics, and document expected skips only for actual no-data/service states. |
| CLI tests accidentally become direct unit tests of helper functions. | Invoke the Typer app (`app`) with real command arguments and assert exit codes/output/artifacts. |
| Requirements traceability is marked complete without phase artifacts. | Add `I4-LIVE-COMMANDS.md` and ensure the final summary/verification maps all five I4 requirements. |

## Recommended Plan Shape

One Wave 1 plan is sufficient:

- `I4-01`: add opt-in live CLI smoke tests for `pipeline`, `ingest` + `transform`,
  and `backfill`; add close-out command/troubleshooting docs; run live/non-live
  gates; then update phase/milestone traceability artifacts.
