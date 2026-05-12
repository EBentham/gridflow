---
phase: L4
status: passed
verified: 2026-05-04
requirements:
  - AGSI-11
  - AGSI-12
---

# Phase L4 Verification

## Verification Complete

Status: passed

Phase goal: Prove real AGSI data can move through the public user paths without polluting local data.

## Requirement Results

| Requirement | Status | Evidence |
|-------------|--------|----------|
| AGSI-11 | passed | `tests/integration/test_gie_agsi_live_e2e.py` uses `GIE_API_KEY`, fetches representative aggregate, country, company, and facility `storage_reports` scopes, writes live responses through `BronzeWriter`, runs the registered `storage_reports` transformer, reads generated silver parquet, and classifies empty/error outcomes explicitly. |
| AGSI-12 | passed | `tests/integration/test_gie_agsi_cli_live_smoke.py` runs live `pipeline`, separate `ingest`/`transform`, and `backfill` commands under isolated `GRIDFLOW_DATA_DIR`, `GRIDFLOW_DUCKDB_PATH`, and `GRIDFLOW_LOG_DIR` paths and verifies bronze plus silver outputs. |

## Must-Have Results

| Must Have | Status | Evidence |
|-----------|--------|----------|
| Representative live storage API-to-silver coverage | passed | Live gate passed for aggregate, country, company, and facility storage scopes. |
| Explicit full-inventory expected-count gate | passed | `test_live_agsi_full_inventory_expected_counts_gate` validates representative listing-derived counts by default and documents `GRIDFLOW_AGSI_FULL_INVENTORY_LIVE=1` for the slow full-inventory run. |
| Isolated CLI smoke paths | passed | CLI smoke tests set and assert temp-root `GRIDFLOW_*` outputs. |
| Close-out docs | passed | `L4-LIVE-COMMANDS.md` documents live commands, pass/skip classifications, GIE rate limits, unavailability ambiguity, and ALSI follow-up. |

## Automated Checks

```powershell
uv run --extra dev ruff check tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py
```

Result: passed.

```powershell
uv run --extra dev pytest tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py -m "not live" -q
```

Result: 1 passed, 9 deselected.

```powershell
uv run --extra dev pytest tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py -m live -q -rs
```

Result: 8 passed, 1 skipped, 1 deselected, 1 warning. The skipped test was the intentional representative-only full-inventory gate.

```powershell
uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q
```

Result: 63 passed.

```powershell
uv run --extra dev pytest -m "not live" -q
```

Result: 984 passed, 253 deselected, 1 dependency deprecation warning.

## Human Verification

None required. The live gates ran with credentials and passed representative API-to-silver and CLI smoke coverage.

## Gaps

None.
