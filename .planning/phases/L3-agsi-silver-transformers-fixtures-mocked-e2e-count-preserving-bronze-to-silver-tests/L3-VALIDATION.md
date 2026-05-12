---
phase: L3
slug: agsi-silver-transformers-fixtures-mocked-e2e-count-preserving-bronze-to-silver-tests
status: draft
nyquist_compliant: true
wave_0_complete: true
created: 2026-05-04
---

# Phase L3 - Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest with respx, httpx, polars |
| **Config file** | `pyproject.toml` |
| **Quick run command** | `uv run --extra dev pytest tests/unit/test_gie.py tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q` |
| **Full suite command** | `uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q` |
| **Estimated runtime** | ~45 seconds |

---

## Sampling Rate

- **After every task commit:** Run `uv run --extra dev pytest tests/unit/test_gie.py tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q`.
- **After every plan wave:** Run `uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q`.
- **Before `$gsd-verify-work`:** Run full non-live pytest unless an unrelated pre-existing failure is recorded in the summary.
- **Max feedback latency:** 45 seconds for focused checks.

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| L3-01-01 | 01 | 1 | AGSI-07 | T-L3-01, T-L3-02 | Storage silver preserves live fields and distinct query scopes | unit + integration | `uv run --extra dev pytest tests/unit/test_gie.py tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q` | W0 | pending |
| L3-01-02 | 01 | 1 | AGSI-08 | T-L3-03 | Listing/news/unavailability families transform or are catalog-deferred with reasons | integration | `uv run --extra dev pytest tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q` | W0 | pending |
| L3-01-03 | 01 | 1 | AGSI-09 | T-L3-04 | Fixture bronze rows reach schema-valid silver parquet | integration | `uv run --extra dev pytest tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q` | W0 | pending |
| L3-01-04 | 01 | 1 | AGSI-10 | T-L3-05 | Existing mocked request/pagination/count tests remain green with new silver E2E | integration | `uv run --extra dev pytest tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q` | W0 | pending |
| L3-01-05 | 01 | 1 | AGSI-07, AGSI-08, AGSI-09, AGSI-10 | T-L3-05 | No live API dependency or credential use | lint + pytest | `uv run --extra dev ruff check src/gridflow/silver/gie tests/unit/test_gie.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py` | W0 | pending |

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements:

- `tests/conftest.py` provides `tmp_data_dir`.
- `tests/fixtures/gie/agsi_listing_response.json` provides deterministic entity inventory.
- `tests/integration/test_gie_agsi_mocked_bronze.py` proves request shapes, pagination, provenance, and expected bronze counts.
- `src/gridflow/silver/base.py` and `src/gridflow/silver/registry.py` provide the bronze-to-silver execution contract.
- `tests/integration/test_entsog_mocked_e2e.py` and `tests/integration/test_neso_mocked_e2e.py` show the fixture-backed E2E pattern.

---

## Manual-Only Verifications

All L3 behaviors have automated non-live verification. Live AGSI checks are deferred to L4.

---

## Validation Sign-Off

- [x] All tasks have automated verification or Wave 0 dependencies.
- [x] Sampling continuity: no 3 consecutive tasks without automated verify.
- [x] Wave 0 covers all missing references.
- [x] No watch-mode flags.
- [x] Feedback latency < 45 seconds.
- [x] `nyquist_compliant: true` set in frontmatter.

**Approval:** approved 2026-05-04
