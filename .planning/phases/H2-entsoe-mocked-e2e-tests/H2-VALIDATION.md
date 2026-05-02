---
phase: H2
slug: entsoe-mocked-e2e-tests
status: ready
nyquist_compliant: true
wave_0_complete: false
created: 2026-05-02
---

# Phase H2 - Validation Strategy

## Test Infrastructure

| Property | Value |
|----------|-------|
| Framework | pytest |
| HTTP mocking | respx |
| Config file | `pyproject.toml` |
| Quick run command | `uv run --extra dev pytest tests/integration/test_entsoe_mocked_e2e.py -x -q` |
| Phase run command | `uv run --extra dev pytest tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_connector.py tests/unit/test_entsoe.py -x -q` |
| Full suite command | `uv run --extra dev pytest -x -q` |
| Estimated quick runtime | <5 seconds |

## Sampling Rate

- After dependency task: run `uv run --extra dev pytest tests/unit/test_entsoe.py -x -q`
- After test-file task: run quick H2 command.
- Before execution summary: run phase command and attempt full suite.
- Max expected feedback latency: <10 seconds for quick/phase commands.

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| H2-01-01 | 01 | 1 | prerequisite | env/test | `uv run --extra dev pytest tests/unit/test_entsoe.py -x -q` | existing | pending |
| H2-01-02 | 01 | 1 | MOCK-01, MOCK-03 | integration | `uv run --extra dev pytest tests/integration/test_entsoe_mocked_e2e.py -x -q` | W0 | pending |
| H2-01-03 | 01 | 1 | MOCK-02 | integration | `uv run --extra dev pytest tests/integration/test_entsoe_mocked_e2e.py -x -q` | W0 | pending |
| H2-01-04 | 01 | 1 | MOCK-01, MOCK-02, MOCK-03 | regression | `uv run --extra dev pytest tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_connector.py tests/unit/test_entsoe.py -x -q` | mixed | pending |

## Wave 0 Requirements

- [ ] `tests/integration/test_entsoe_mocked_e2e.py` - new mocked E2E integration file.
- [ ] `pyproject.toml` - includes `tzdata` so Windows Polars UTC conversions can run.

## Manual-Only Verifications

None. All H2 behaviors are covered by mocked automated tests.

## Known Non-H2 Full-Suite Blocker

`uv run --extra dev pytest -x -q` may still fail during collection because
`src/gridflow/silver/elexon/__init__.py` imports missing Elexon silver modules. If the
targeted H2/ENTSO-E command passes and the full-suite failure is only this known Elexon
issue, record the override in the H2 summary and verification report.

## Validation Sign-Off

- [x] All tasks have automated verify or Wave 0 dependencies.
- [x] Sampling continuity: every task has a command.
- [x] Wave 0 covers all missing H2 test files.
- [x] No watch-mode flags.
- [x] Feedback latency <10 seconds.
- [x] `nyquist_compliant: true` set in frontmatter.

**Approval:** approved 2026-05-02
