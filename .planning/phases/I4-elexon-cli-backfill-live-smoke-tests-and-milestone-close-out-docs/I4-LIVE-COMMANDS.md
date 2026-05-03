# I4 Live Commands - Elexon CLI/Backfill Smoke Tests

## Scope

These commands validate the public no-key Elexon Insights API through the user-facing
`gridflow` CLI. They are live smoke checks, not default CI tests. Use isolated
`GRIDFLOW_*` paths when running them manually so normal project data is untouched.

## Isolated Environment

PowerShell example:

```powershell
$env:GRIDFLOW_DATA_DIR = "$PWD\.tmp\i4-live\data"
$env:GRIDFLOW_DUCKDB_PATH = "$PWD\.tmp\i4-live\catalogue\gridflow.duckdb"
$env:GRIDFLOW_LOG_DIR = "$PWD\.tmp\i4-live\logs"
```

## Commands

Pipeline smoke for the path-date dataset:

```powershell
uv run gridflow pipeline elexon system_prices --start 2026-02-01 --end 2026-02-02
```

Separate ingest and transform smoke for the publish/from-to datetime dataset:

```powershell
uv run gridflow ingest elexon freq --start 2026-02-01 --end 2026-02-02
uv run gridflow transform elexon freq --start 2026-02-01 --end 2026-02-02
```

Backfill smoke for curated request styles:

```powershell
uv run gridflow backfill elexon system_prices --start 2026-02-01 --end 2026-02-02 --chunk-days 1
uv run gridflow backfill elexon freq --start 2026-02-01 --end 2026-02-02 --chunk-days 1
uv run gridflow backfill elexon bmunits_reference --start 2026-02-01 --end 2026-02-02 --chunk-days 1
```

Automated pytest equivalents:

```powershell
uv run --extra dev pytest tests/integration/test_elexon_cli_live_smoke.py -m live -q -rs
uv run --extra dev pytest tests/integration/test_elexon_cli_live_smoke.py -m "not live" -q
```

## Dataset Windows

| Dataset | Window | Reason |
| --- | --- | --- |
| `system_prices` | 2026-02-01 to 2026-02-02 | Stable path-date settlement endpoint with bronze and silver outputs. |
| `freq` | 2026-02-01 to 2026-02-02 | Compact publish/from-to dataset that validates separate ingest and transform. |
| `bmunits_reference` | 2026-02-01 to 2026-02-02 | No-param reference endpoint with the non-date-partitioned silver parquet path. |

## Expected Skips

None are expected for the selected windows. A public service outage or an empty live
window should be reported with the command, dataset, stage, HTTP status or CLI exit
code, and output preview. Do not treat missing bronze or silver artifacts as a skip.

## Troubleshooting

- Live tests are opt-in. Use `-m live` for network checks and `-m "not live"` for
  the default-safe sentinel.
- If the public Elexon service returns HTTP 4xx or 5xx for an active curated
  dataset, keep the failure visible. That may indicate request-shape drift.
- If outputs appear under `data/` or `logs/`, stop and check the `GRIDFLOW_DATA_DIR`,
  `GRIDFLOW_DUCKDB_PATH`, and `GRIDFLOW_LOG_DIR` overrides.
- If `bmunits_reference` creates silver output at
  `silver/elexon/bmunits_reference/bmunits_reference.parquet`, that is expected
  for reference data.
- If a command exits non-zero, read the per-dataset CLI output first; ingest and
  transform are expected to report the failing dataset and return a non-zero exit
  on real dataset errors.

## Requirements Traceability

| Requirement | Evidence |
| --- | --- |
| ELEXON-CLI-01 | `test_live_pipeline_elexon_system_prices_creates_bronze_and_silver` and the `pipeline elexon system_prices` command. |
| ELEXON-CLI-02 | `test_live_ingest_then_transform_elexon_freq_creates_outputs` and the separate `ingest` / `transform` commands. |
| ELEXON-CLI-03 | `test_live_backfill_elexon_curated_dataset_creates_outputs` for `system_prices`, `freq`, and `bmunits_reference`. |
| ELEXON-DOC-01 | This artifact documents live commands, dataset windows, expected skips, and troubleshooting notes. |
| ELEXON-DOC-02 | This traceability table plus I4 summary, verification, roadmap, requirements, and state updates map all close-out requirements. |
