---
status: partial
phase: H3-live-entsoe-test-suite
source: [H3-VERIFICATION.md]
started: 2026-05-02T18:43:00+01:00
updated: 2026-05-02T18:43:00+01:00
---

## Current Test

Awaiting credentialed ENTSO-E live verification.

## Tests

### 1. Run all live ENTSO-E tests with real credentials

expected: With `ENTSOE_API_KEY` set, `uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py -x -q` passes for all live connector, bronze-to-silver, and CLI command tests.
result: pending

## Summary

total: 1
passed: 0
issues: 0
pending: 1
skipped: 0
blocked: 0

## Gaps
