---
phase: I4
slug: elexon-cli-backfill-live-smoke-tests-and-milestone-close-out-docs
status: ready
created: 2026-05-04
---

# I4 Validation Strategy

## Validation Architecture

I4 validation uses three layers:

1. Live-marked CLI smoke tests that invoke the Typer app for `pipeline`, `ingest`,
   `transform`, and `backfill` against public Elexon datasets.
2. Non-live guard tests proving the live suite remains opt-in and still has a
   normal-run sentinel assertion.
3. Documentation and traceability checks proving phase artifacts capture commands,
   windows, expected skips, troubleshooting, and requirement ownership.

## Required Gates

```powershell
uv run --extra dev ruff check tests/integration/test_elexon_cli_live_smoke.py tests/integration/test_elexon_live_e2e.py tests/integration/test_elexon_mocked_e2e.py tests/unit/test_cli_resolve_datasets.py
uv run --extra dev pytest tests/integration/test_elexon_cli_live_smoke.py -m "not live" -q
uv run --extra dev pytest tests/integration/test_elexon_cli_live_smoke.py -m live -q -rs
uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py tests/unit/test_cli_resolve_datasets.py -m "not live" -q
```

## Human-Readable Evidence

The executor summary should record:

- exact CLI commands or equivalent Typer argument vectors exercised;
- temp data, DuckDB, and log isolation evidence;
- bronze and silver output paths created for each smoke case;
- live skips, if any, with dataset/window/stage reason;
- requirement mapping for `ELEXON-CLI-01`, `ELEXON-CLI-02`,
  `ELEXON-CLI-03`, `ELEXON-DOC-01`, and `ELEXON-DOC-02`.

## Failure Policy

- A real HTTP 4xx/5xx for an active curated dataset should fail the live smoke
  unless it is clearly a transient service outage documented in the test output.
- Empty/no-data responses may skip only with source, dataset, command, URL/status,
  and window diagnostics.
- Any artifact written outside the pytest temp root is a blocker.
- A live test that passes without creating expected bronze/silver evidence is a
  false pass and must be fixed before completing the phase.
