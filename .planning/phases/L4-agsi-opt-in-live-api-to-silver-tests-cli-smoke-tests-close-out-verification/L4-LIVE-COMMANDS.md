# L4 Live Commands - GIE AGSI API And CLI Smoke Tests

## Scope

These commands validate the credentialed GIE AGSI gas storage API through live
API-to-silver tests and user-facing `gridflow` CLI commands. They are opt-in
network checks and must not run as part of the default non-live test suite.

## API Key

Set `GIE_API_KEY` before running live gates. The key is sent as the lowercase
`x-key` header by the configured `gie_agsi` source.

PowerShell example:

```powershell
$env:GIE_API_KEY = "<your-gie-api-key>"
```

## Isolated Environment

Use isolated paths for manual CLI smoke runs so normal project data is untouched:

```powershell
$env:GRIDFLOW_DATA_DIR = "$PWD\.tmp\l4-live\data"
$env:GRIDFLOW_DUCKDB_PATH = "$PWD\.tmp\l4-live\catalogue\gridflow.duckdb"
$env:GRIDFLOW_LOG_DIR = "$PWD\.tmp\l4-live\logs"
```

## CLI Commands

Pipeline smoke for AGSI storage reports:

```powershell
uv run gridflow pipeline gie_agsi storage_reports --start 2026-05-01 --end 2026-05-01
```

Separate ingest and transform smoke:

```powershell
uv run gridflow ingest gie_agsi storage_reports --start 2026-05-01 --end 2026-05-01
uv run gridflow transform gie_agsi storage_reports --start 2026-05-01 --end 2026-05-01
```

Backfill smoke:

```powershell
uv run gridflow backfill gie_agsi storage_reports --start 2026-05-01 --end 2026-05-02 --chunk-days 1
```

## Pytest Gates

Live API-to-silver and CLI smoke gates:

```powershell
uv run --extra dev pytest tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py -m live -q -rs
```

Default-safe non-live sentinel:

```powershell
uv run --extra dev pytest tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py -m "not live" -q
```

Slow full-inventory count gate:

```powershell
$env:GRIDFLOW_AGSI_FULL_INVENTORY_LIVE = "1"
uv run --extra dev pytest tests/integration/test_gie_agsi_live_e2e.py::test_live_agsi_full_inventory_expected_counts_gate -m live -q -rs
```

## Dataset Window

| Dataset | Window | Reason |
| --- | --- | --- |
| `storage_reports` | 2026-05-01 | Deterministic exact gas-day request covering the active AGSI storage endpoint with bronze and silver output. |
| `storage_reports` backfill | 2026-05-01 to 2026-05-02 | Backfill uses an exclusive loop boundary, so the one-day smoke uses the next-day end boundary. |

Representative live API tests cover aggregate, country, company, and facility
storage query scopes. Company and facility tests use a trimmed live listing
payload to avoid accidental full-inventory bursts.

## Expected Pass And Skip Classification

- Missing `GIE_API_KEY` should skip with `source=gie_agsi stage=setup outcome=missing GIE_API_KEY`.
- Empty live responses should skip with source, dataset, stage, URL, status, and body preview.
- HTTP failures for active storage scopes should fail with source, dataset, stage, status, URL, and body preview.
- At least one successful storage API-to-silver path and one successful CLI path are required before marking `AGSI-11` and `AGSI-12` complete.

## Rate Limit

GIE documents a 60 calls/minute limit. Keep the representative live gates narrow.
Only enable the full-inventory expected-count gate deliberately, and keep it
slow enough for the configured `rate_limit_per_second: 1` source behavior.

## Unavailability Ambiguity

The endpoint catalog keeps AGSI `unavailability` active because the endpoint is
documented and live-served, but the documentation wording is ambiguous about API
coverage. L4 live tests classify empty or unavailable `unavailability` outcomes
explicitly instead of treating them as silent failures.

## ALSI Follow-Up

ALSI LNG remains deferred while v0.7 focuses on AGSI gas storage. Follow-up work
should validate ALSI separately against the ALSI host and LNG-specific endpoint
semantics.

## Requirements Traceability

| Requirement | Evidence |
| --- | --- |
| AGSI-11 | `test_live_agsi_storage_scopes_fetch_transform_or_classify_empty`, `test_live_agsi_unavailability_fetches_or_classifies_documented_ambiguity`, and `test_live_agsi_full_inventory_expected_counts_gate`. |
| AGSI-12 | `test_live_pipeline_gie_agsi_storage_reports_creates_bronze_and_silver`, `test_live_ingest_then_transform_gie_agsi_storage_reports_creates_outputs`, and `test_live_backfill_gie_agsi_storage_reports_creates_outputs`. |
