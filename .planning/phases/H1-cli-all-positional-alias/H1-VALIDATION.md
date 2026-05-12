---
phase: H1
slug: cli-all-positional-alias
status: ready
nyquist_compliant: true
wave_0_complete: false
created: 2026-05-02
---

# Phase H1 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (project standard) |
| **Config file** | none detected (use pytest directly) |
| **Quick run command** | `uv run pytest tests/unit/test_cli_resolve_datasets.py -x -q` |
| **Full suite command** | `uv run pytest -x -q` |
| **Estimated runtime** | ~5 seconds |

---

## Sampling Rate

- **After every task commit:** Run `uv run pytest tests/unit/test_cli_resolve_datasets.py -x -q`
- **After every plan wave:** Run `uv run pytest -x -q`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** ~5 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| H1-01-01 | 01 | 1 | CLI-01, CLI-02 | — | N/A | unit | `uv run pytest tests/unit/test_cli_resolve_datasets.py -x -q` | ❌ W0 | ⬜ pending |
| H1-01-02 | 01 | 1 | CLI-01, CLI-02 | — | N/A | unit | `uv run pytest -x -q` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/unit/test_cli_resolve_datasets.py` — 9-test RED/GREEN file for CLI-01, CLI-02, including ENTSO-E 16-dataset expansion from `config/sources.yaml`

*Existing infrastructure (conftest.py `sample_config` fixture) covers all other phase requirements.*

---

## Manual-Only Verifications

*All phase behaviors have automated verification.*

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 5s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** approved 2026-05-02
