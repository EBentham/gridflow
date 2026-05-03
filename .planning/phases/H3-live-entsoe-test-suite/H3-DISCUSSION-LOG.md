# Phase H3: Live ENTSO-E test suite - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md - this log preserves the alternatives considered.

**Date:** 2026-05-02
**Phase:** H3-live-entsoe-test-suite
**Areas discussed:** Live coverage subset, command-level coverage, failure policy, deferred Elexon coverage

---

## Live Coverage Subset

| Option | Description | Selected |
|--------|-------------|----------|
| Representative subset | Cover only a few high-value ENTSO-E datasets to keep runtime low. | |
| All 16 datasets | Cover every configured/registered ENTSO-E dataset, accepting slower tests. | Yes |

**User's choice:** Test everything.
**Notes:** User said slower tests are acceptable if needed to ensure proper testing.

---

## Command-Level Coverage

| Option | Description | Selected |
|--------|-------------|----------|
| Connector and transformer only | Exercise live API calls and parsing without invoking CLI commands. | |
| Include common CLI commands | Add live tests for user-facing commands such as `gridflow pipeline entsoe all --last 24h`. | Yes |

**User's choice:** Include command-level tests.
**Notes:** User specifically named `gridflow pipeline entsoe all --last 24h` and other commonly used CLI commands.

---

## Failure Policy

| Option | Description | Selected |
|--------|-------------|----------|
| Hard fail with diagnostics | Once opted in and API key is present, fail on real service/data/parse/transform problems with useful detail. | Yes |
| Skip/xfail external-service conditions | Treat API errors, empty data, or malformed responses as skips/xfails. | |

**User's choice:** Hard fail with useful diagnostics.
**Notes:** Live tests should auto-skip only when `ENTSOE_API_KEY` is absent.

---

## Deferred Elexon Coverage

| Option | Description | Selected |
|--------|-------------|----------|
| Fold Elexon into H3 | Expand H3 beyond ENTSO-E to also fix/test Elexon. | |
| Defer Elexon follow-up | Keep H3 ENTSO-E-only and record equivalent Elexon live/E2E coverage for the next project. | Yes |

**User's choice:** Defer Elexon follow-up.
**Notes:** User mentioned Elexon as an example of the same command-level testing gap, not as H3 scope.

---

## the agent's Discretion

- Planner may choose exact live date windows and pacing strategy.
- Planner may decide how to split all-dataset live coverage across connector, CLI, and transformer tests.
- Planner should use temporary data roots to avoid normal developer data output.

## Deferred Ideas

- Follow-up project: equivalent live/command-level E2E coverage for Elexon.
