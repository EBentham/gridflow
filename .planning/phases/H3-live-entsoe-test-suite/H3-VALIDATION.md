---
phase: H3
slug: live-entsoe-test-suite
status: ready
nyquist_compliant: true
wave_0_complete: false
created: 2026-05-02
---

# Phase H3 - Validation Strategy

## Test Infrastructure

| Property | Value |
|----------|-------|
| Framework | pytest |
| Marker | `@pytest.mark.live` |
| API key | `ENTSOE_API_KEY` |
| Quick non-live command | `uv run --extra dev pytest tests/unit/test_cli_resolve_datasets.py tests/integration/test_entsoe_live.py -m "not live" -x -q` |
| Live gate | `uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py -x -q` |
| Command live gate | Included inside `tests/integration/test_entsoe_live.py` |
| Full suite command | `uv run --extra dev pytest -x -q` |

## Sampling Rate

- After CLI failure propagation: run non-live CLI/helper tests.
- After live fixtures/helpers: run `pytest tests/integration/test_entsoe_live.py -m "not live" -x -q`.
- After all-dataset live connector tests: run `pytest -m live tests/integration/test_entsoe_live.py -x -q`.
- After command-level tests: run the full live gate again.
- Before summary: attempt full suite and document the known Elexon blocker if still present.

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| H3-01-01 | 01 | 1 | LIVE-03 | non-live | `uv run --extra dev pytest tests/integration/test_entsoe_live.py -m "not live" -x -q` | W0 | pending |
| H3-01-02 | 01 | 1 | LIVE-01, LIVE-03 | unit/integration | `uv run --extra dev pytest tests/integration/test_entsoe_live.py -m "not live" -x -q` | mixed | pending |
| H3-01-03 | 01 | 1 | command diagnostics | unit/integration | `uv run --extra dev pytest tests/integration/test_entsoe_live.py -m "not live" -x -q` | mixed | pending |
| H3-02-01 | 02 | 2 | LIVE-01 | live | `uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py -x -q` | W0 | pending |
| H3-02-02 | 02 | 2 | LIVE-02 | live | `uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py -x -q` | mixed | pending |
| H3-02-03 | 02 | 2 | LIVE-01, LIVE-02, LIVE-03 | live CLI | `uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py -x -q` | mixed | pending |

## Wave 0 Requirements

- [ ] `tests/integration/test_entsoe_live.py` - live test module with non-live skip/helper tests.
- [ ] `src/gridflow/cli.py` - per-dataset failures propagate to non-zero CLI exits.

## Manual-Only Verifications

- The user or executor must provide `ENTSOE_API_KEY` to run the live gate. Without it, live tests must skip.

## Known Non-H3 Full-Suite Blocker

`uv run --extra dev pytest -x -q` may still fail during collection because
`src/gridflow/silver/elexon/__init__.py` imports missing Elexon silver modules. If the
targeted H3 live command passes and the full-suite failure is only this known Elexon
issue, record the override in the H3 summary and verification report.

## Validation Sign-Off

- [x] All tasks have automated verify or Wave 0 dependencies.
- [x] Sampling continuity: every task has a command.
- [x] Wave 0 covers all missing H3 test infrastructure files.
- [x] No watch-mode flags.
- [x] Live testing remains opt-in.
- [x] `nyquist_compliant: true` set in frontmatter.
