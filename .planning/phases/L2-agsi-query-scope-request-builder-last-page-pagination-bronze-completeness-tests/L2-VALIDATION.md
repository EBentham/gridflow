---
phase: L2
slug: agsi-query-scope-request-builder-last-page-pagination-bronze-completeness-tests
status: draft
nyquist_compliant: true
wave_0_complete: true
created: 2026-05-04
---

# Phase L2 - Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest with respx, httpx, polars |
| **Config file** | `pyproject.toml` |
| **Quick run command** | `uv run --extra dev pytest tests/integration/test_gie_agsi_mocked_bronze.py -m "not live" -q` |
| **Full suite command** | `uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py -m "not live" -q` |
| **Estimated runtime** | ~30 seconds |

---

## Sampling Rate

- **After every task commit:** Run `uv run --extra dev pytest tests/integration/test_gie_agsi_mocked_bronze.py -m "not live" -q`.
- **After every plan wave:** Run `uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py -m "not live" -q`.
- **Before `$gsd-verify-work`:** Full non-live suite must be green.
- **Max feedback latency:** 30 seconds.

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| L2-01-01 | 01 | 1 | AGSI-02 | T-L2-02 | Active inventory alignment fails on drift | unit | `uv run --extra dev pytest tests/unit/test_gie_endpoint_catalog.py -q` | W0 | pending |
| L2-01-02 | 01 | 1 | AGSI-04 | T-L2-03 | Request params match documented AGSI query scopes | integration | `uv run --extra dev pytest tests/integration/test_gie_agsi_mocked_bronze.py -m "not live" -q` | W0 | pending |
| L2-01-03 | 01 | 1 | AGSI-05 | T-L2-04 | `last_page` controls pagination and sidecars record page totals | integration | `uv run --extra dev pytest tests/integration/test_gie_agsi_mocked_bronze.py -m "not live" -q` | W0 | pending |
| L2-01-04 | 01 | 1 | AGSI-06 | T-L2-05 | Bronze files and partitions match expected query plan counts | integration | `uv run --extra dev pytest tests/integration/test_gie_agsi_mocked_bronze.py -m "not live" -q` | W0 | pending |
| L2-01-05 | 01 | 1 | AGSI-02, AGSI-04, AGSI-05, AGSI-06 | T-L2-01 | No live API dependency or credential use | lint + pytest | `uv run --extra dev ruff check src/gridflow/connectors/gie tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py` | W0 | pending |

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements:

- `tests/conftest.py` provides `tmp_data_dir`.
- `tests/fixtures/gie/agsi_listing_response.json` provides deterministic company/facility inventory.
- `tests/unit/test_gie_endpoint_catalog.py` already covers L1 catalog and query-plan helpers.
- `tests/integration/test_entsog_mocked_e2e.py` and `tests/integration/test_neso_mocked_e2e.py` show the mocked request and bronze-sidecar pattern.

---

## Manual-Only Verifications

All L2 behaviors have automated non-live verification. Live AGSI checks are deferred to L4.

---

## Validation Sign-Off

- [x] All tasks have automated verification or Wave 0 dependencies.
- [x] Sampling continuity: no 3 consecutive tasks without automated verify.
- [x] Wave 0 covers all missing references.
- [x] No watch-mode flags.
- [x] Feedback latency < 30 seconds.
- [x] `nyquist_compliant: true` set in frontmatter.

**Approval:** approved 2026-05-04

