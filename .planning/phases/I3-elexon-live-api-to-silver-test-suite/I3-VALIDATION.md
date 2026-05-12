---
phase: I3
slug: elexon-live-api-to-silver-test-suite
status: draft
nyquist_compliant: true
wave_0_complete: true
created: 2026-05-04
---

# Phase I3 - Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest with httpx, polars, live marker gate |
| **Config file** | `pyproject.toml` |
| **Quick run command** | `uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m "not live" -q` |
| **Full suite command** | `uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m live -q -rs` |
| **Estimated runtime** | ~60 seconds live, <10 seconds non-live skip guard |

---

## Sampling Rate

- **After every task commit:** Run `uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m "not live" -q`
- **After every live-test implementation task:** Run `uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m live -q -rs`
- **After every plan wave:** Run the live command plus the existing I2 non-live regression command.
- **Before `$gsd-verify-work`:** Ruff, live suite, and non-live Elexon regression suite must be green or have explicit skip/deferred classifications.
- **Max feedback latency:** 90 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| I3-01-01 | 01 | 1 | ELEXON-LIVE-01, ELEXON-LIVE-05 | T-I3-01 | Live tests remain opt-in and no-key | live + collection | `uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m "not live" -q` | W0 | pending |
| I3-01-02 | 01 | 1 | ELEXON-LIVE-01, ELEXON-LIVE-04 | T-I3-02 | Empty/no-data outcomes are classified with diagnostics | live | `uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m live -q -rs` | W0 | pending |
| I3-01-03 | 01 | 1 | ELEXON-LIVE-02, ELEXON-LIVE-03 | T-I3-03 | Live responses use temp bronze/silver only | live integration | `uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m live -q -rs` | W0 | pending |
| I3-01-04 | 01 | 1 | ELEXON-LIVE-01, ELEXON-LIVE-02, ELEXON-LIVE-03, ELEXON-LIVE-04, ELEXON-LIVE-05 | T-I3-04 | Existing mocked and inventory tests remain green | lint + pytest | `uv run --extra dev ruff check tests/integration/test_elexon_live_e2e.py tests/endpoints/test_endpoint_live.py tests/integration/test_elexon_mocked_e2e.py` | W0 | pending |

---

## Wave 0 Requirements

Existing infrastructure covers all phase requirements:

- `tests/conftest.py` provides `tmp_data_dir` and the opt-in live marker gate.
- `tests/endpoints/test_endpoint_live.py` provides Elexon live diagnostics helpers.
- `tests/integration/test_elexon_mocked_e2e.py` provides bronze/silver helper patterns.
- `src/gridflow/connectors/elexon/client.py` supports all active Elexon `ParamStyle` variants.
- `src/gridflow/bronze/writer.py` and registered silver transformers already support the bronze-to-silver flow.

---

## Manual-Only Verifications

All phase behaviors have automated verification. The live API suite is opt-in but still automated.

---

## Validation Sign-Off

- [x] All tasks have automated verification or Wave 0 dependencies.
- [x] Sampling continuity: no 3 consecutive tasks without automated verify.
- [x] Wave 0 covers all missing references.
- [x] No watch-mode flags.
- [x] Feedback latency < 90 seconds.
- [x] `nyquist_compliant: true` set in frontmatter.

**Approval:** approved 2026-05-04
