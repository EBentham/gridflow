---
phase: I2
slug: elexon-mocked-request-shape-and-fixture-backed-bronze-to-silver-tests
status: draft
nyquist_compliant: true
wave_0_complete: true
created: 2026-05-03
---

# Phase I2 - Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest with respx, httpx, polars |
| **Config file** | `pyproject.toml` |
| **Quick run command** | `uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py -m "not live" -q` |
| **Full suite command** | `uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py -m "not live" -q` |
| **Estimated runtime** | ~30 seconds |

---

## Sampling Rate

- **After every task commit:** Run `uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py -m "not live" -q`
- **After every plan wave:** Run `uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py -m "not live" -q`
- **Before `$gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 30 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| I2-01-01 | 01 | 1 | ELEXON-MOCK-01 | T-I2-01 | No live network dependency | integration | `uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py -m "not live" -q` | W0 | pending |
| I2-01-02 | 01 | 1 | ELEXON-MOCK-01 | T-I2-02 | Exact request params asserted | integration | `uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py -m "not live" -q` | W0 | pending |
| I2-01-03 | 01 | 1 | ELEXON-MOCK-02 | T-I2-03 | Fixtures stay local and deterministic | integration | `uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py -m "not live" -q` | W0 | pending |
| I2-01-04 | 01 | 1 | ELEXON-MOCK-03 | T-I2-04 | Bronze metadata and partition assertions prove provenance | integration | `uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py -m "not live" -q` | W0 | pending |
| I2-01-05 | 01 | 1 | ELEXON-MOCK-01, ELEXON-MOCK-02, ELEXON-MOCK-03 | T-I2-05 | Regression suite remains non-live | lint + pytest | `uv run --extra dev ruff check tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py` | W0 | pending |

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements:

- `tests/conftest.py` provides `tmp_data_dir`.
- `tests/fixtures/elexon/*.json` provides representative JSON responses.
- `tests/integration/test_elexon_connector.py` already uses `respx`.
- `tests/integration/test_bronze_to_silver.py` already demonstrates `BronzeWriter` to silver flow.
- `tests/unit/test_elexon_endpoints.py` already proves active inventory consistency.

---

## Manual-Only Verifications

All phase behaviors have automated verification. No live API or human credentials are required.

---

## Validation Sign-Off

- [x] All tasks have automated verification or Wave 0 dependencies.
- [x] Sampling continuity: no 3 consecutive tasks without automated verify.
- [x] Wave 0 covers all missing references.
- [x] No watch-mode flags.
- [x] Feedback latency < 30 seconds.
- [x] `nyquist_compliant: true` set in frontmatter.

**Approval:** approved 2026-05-03

