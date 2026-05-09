---
phase: V2
milestone: v0.10
validated: 2026-05-09
total_fixes: 7
plans: [V2-PLAN-A, V2-PLAN-B, V2-PLAN-C, V2-PLAN-D, V2-PLAN-E]
---

# V2 — V1 Vendor Bug-fix Follow-ups (Consolidated)

Aggregates the wave-1 (HIGH severity) and wave-2 (MED + LOW) bug
fixes that closed the production-code Implementation deltas
surfaced in V1's per-vendor VALIDATION reports.

## Summary

| Plan | Wave | Severity | Status | Fix commit | Vendor / Scope |
|------|------|----------|--------|------------|-----------------|
| V2-PLAN-A | 1 | HIGH | DONE | `8069201` | Elexon (`freq` param-name override) |
| V2-PLAN-B | 1 | HIGH | DONE | `8f9db07` | NESO (5 period-keyed regional datasets) |
| V2-PLAN-C | 2 | MED | DONE | `dc0ce83` | Elexon (REMIT/SOSO 1-day cap + `system_prices` priceDerivationCode) |
| V2-PLAN-D | 2 | MED+LOW | DONE (partial — 4 LOW items deferred to backlog with rationale) | `5c29a68` | ENTSOE (A09 dedup ADR-019 + B2 hygiene 5b/5e) |
| V2-PLAN-E | 2 | LOW | DONE | `8c8da2d` | ENTSOG (404 short-circuit) + `ngeso/` deleted |

Total live re-validation curls: ~14, throttled identically to V1's
pattern (Elexon 0.6s, NESO 0.2s, ENTSOE 1s; Avast `--ssl-no-revoke`
throughout).

## Per-fix details

### V2-FIX-01 — Elexon `freq` (HIGH, `8069201`)

V1 evidence: `freq` connector sent `publishDateTimeFrom/To` whereas
Elexon Swagger declares `measurementDateTimeFrom/To`. The API
silently ignored the wrong-named params and returned the latest
~5761 samples regardless of window.

V2 fix: explicit `from_param`/`to_param` override on
`ENDPOINTS["freq"]`. Live re-validation 2026-05-09:
- Wrong-name 3-hour Jan 2024 window → 5761 rows of 2026-05-08/09
  (window silently ignored — proves the bug).
- Correct-name 3-hour Jan 2024 window → 721 rows, all 2024-01-01.
- Post-fix narrow 1-hour window → 241 rows, 100% in window.

See V1 `elexon-VALIDATION.md` `## V2 re-validation`.

### V2-FIX-02 — NESO `_rows_from_region_period` (HIGH, `8f9db07`)

V1 evidence: 5 period-keyed regional datasets (`regional_current`,
`regional_intensity_fw24h`, `regional_intensity_fw48h`,
`regional_intensity_pt24h`, `regional_intensity`) wrote silver rows
with null `forecast`, `actual`, `index`, `fuel`, `generation_percentage`
because the silver function read those fields from `period`
unconditionally — but the live API places them on each `region`
dict for period-keyed payloads.

V2 fix: read `intensity` from `region.get(...) or period.get(...)`
and `generationmix` via `_generation_mix_rows(region) or
_generation_mix_rows(period)`. Region-keyed routes (where data
lives on `period`) keep working unchanged because of the fallback.

Live re-validation 2026-05-09: `/regional/intensity/.../fw24h` →
49 periods × 18 regions × 9 fuels = **7938 silver rows**,
**0% null** on `forecast_gco2_kwh`, `fuel`, `generation_percentage`.
All 18 regionids and 9 standard fuels present.

See V1 `neso-VALIDATION.md` `## V2 re-validation`.

### V2-FIX-03 — Elexon REMIT/SOSO 1-day cap (MED, `dc0ce83`)

V1 evidence: HTTP 400 at default `max_chunk_hours=24` due to
undocumented vendor 1-day cap.

V2 fix: explicit `max_chunk_hours=23` on both endpoints. Live
boundary verified 2026-05-09:
- 23h window: HTTP 200 (REMIT 182 KB, SOSO 913 KB).
- 25h window: HTTP 400 with the exact "date range … must not
  exceed 1 day" body.

Mocked E2E test asserts the 23h chunk boundary; full fast suite
1040 passed.

### V2-FIX-04 — Elexon `system_prices.priceDerivationCode` (MED, `dc0ce83`)

V1 evidence: `/balancing/settlement/system-prices/{date}` returns
`priceDerivationCode` ∈ `{N, P}`. Pre-V2 the silver renamed that
field to `run_type` and the schema regex
`^(II|SF|R[1-3]|RF|DF)$` rejected the live values.

V2 fix (Option β — fix mapping): `priceDerivationCode` is a
**different concept** from `run_type`:
- `run_type` (II/SF/R1-3/RF/DF) is the BSC settlement run id used
  for bitemporal precedence in `_resolve_runs`.
- `priceDerivationCode` (N/P) describes how SBP/SSP was derived.

Silver `system_prices.py` now maps `priceDerivationCode` →
`price_derivation_code` (a separate column with no regex). Schema
`run_type` becomes `Optional[str] = None` (this endpoint exposes no
run-type field). New schema field `price_derivation_code: str | None`.

Live re-validation 2026-05-09: 48 rows from 2026-05-06 piped
through silver + Pydantic round-trip → **0 errors**. Distinct
`price_derivation_code` values: `['N', 'P']` (cross-checked across
3 dates: 2025-11-01, 2026-04-15, 2026-05-06).

### V2-FIX-05 — ENTSOE A09 commercial_schedules dedup (MED, `5c29a68`)

V1 evidence: `commercial_schedules` and
`commercial_schedules_net_positions` use identical `EntsoeDocType`
and return identical XML for the same request — registry duplicate.

V2 disposition: **Option A — drop key** (per ADR-019).
`commercial_schedules_net_positions` removed from connector
ENDPOINTS, silver h6_market transformer + registration, config
yaml, silver `__init__.py`, endpoint catalog yaml (status flipped
to `deferred` with reason), and 4 test parametrize lists. ENTSOE
active dataset count drops 48 → 47.

Option B (derive `net_position_mw`) recorded as a backlog item.

Live no-regression check 2026-05-09: GB→FR `commercial_schedules`
with `contract_MarketAgreement.Type=A01` returns HTTP 200, 3078
bytes (V1 had 5296 bytes — different period, smaller TS count
today; request shape unchanged).

### V2-FIX-06 — ENTSOE B2 cleanup batch (LOW, `5c29a68`)

Per the plan's MED+LOW disposition rule, sub-items either landed in
this commit or were deferred to backlog with explicit rationale.

| Sub-item | Disposition | Notes |
|----------|-------------|-------|
| 5a — A37/A15 hardcoded `offset=0` pagination | **Backlog** | Non-trivial: connector-level offset iteration loop + bronze chunk aggregation. No GB data currently published for these endpoints (V1: balancing returned EMPTY for all GB calls), so practical impact today is minimal. Defer to a follow-up phase. |
| 5b — A87 schedule cadence | **DONE** | `config/sources.yaml` `entsoe.balancing_financial_expenses_income` → `schedule: monthly, max_query_days: 31`. Pinned by `tests/unit/test_entsoe.py::TestV2BCleanup::test_a87_balancing_financial_schedule_monthly`. |
| 5c — A87 silver `Reason.code` exposure | **Backlog** | Requires `_H8BalancingTransformer` base-class refactor to extract `<Reason><code>` + new `EntsoeBalancingFinancial.reason_code` schema field + new fixture-backed test. |
| 5d — `area_name` field declared but unpopulated | **Backlog** | `EntsoeActualGeneration.area_name: str = ""` defaults to empty. A clean fix needs either a new area_code → name lookup table (preferred) or schema removal. No current gold consumer has flagged this as missing. |
| 5e — `psrType` in `optional_params` | **DONE** | Added to `actual_generation` (A75/A16), `wind_solar_forecast` (A69/A01), `outages_generation` (A80/A53), `outages_production` (A77/A53). Pinned by `TestV2BCleanup::test_psrType_optional_for_*`. |
| 5f — `DEFAULT_ZONES` review | **No change** | Current value `["GB", "FR", "NL", "BE", "DE-LU", "IE-SEM"]` covers six GB-relevant zones. Backlog row for a wider EU baseline if a multi-region gold consumer materialises. |

### V2-FIX-07 — ENTSOG 404 short-circuit (LOW, `8c8da2d`)

V1 evidence: ENTSO-G returns HTTP 404 + body
`{"message":"No result found"}` as documented empty convention.
Pre-V2 `@RETRY_POLICY` retried up to 5 times before reraising —
wasted budget for an expected response.

V2 fix: `EntsogConnector._request` short-circuits 404+empty body.
Other 404s (genuine errors with a different body) preserve the
existing `raise_for_status()` + retry path. Module-level constant
`_ENTSOG_EMPTY_MESSAGE = "No result found"` holds the canonical
marker. Body parsing is defensive: `ValueError`/`json.JSONDecodeError`
fall through to the standard raise path.

Two respx-mocked regression tests:
- `test_404_no_result_found_short_circuits_no_retry` — RED before
  fix (5 requests captured); GREEN after (1 request,
  `response.http_status == 404`).
- `test_genuine_404_preserves_retry` — locks the no-regression
  contract; retries on a non-empty 404 body and expects
  `HTTPStatusError`.

### V2-TRIAGE-01 — `connectors/ngeso/` deleted (LOW, `8c8da2d`)

Verified pre-deletion: 0 imports of `gridflow.connectors.ngeso`
across `src/`, `tests/`, `config/`. Removed via `git rm` + `rmdir`.
NGESO operational endpoints are out of current gridflow scope; a
fresh package can be created later if they become in-scope.

## Live re-validation summary

| Vendor | V1 status | V2 deltas applied | V2 re-validation outcome |
|--------|-----------|-------------------|--------------------------|
| Elexon | 33 PASS / 0 EMPTY / 0 FAIL | 3 (freq, remit/soso chunk, system_prices regex) | freq narrow-window correct (241 rows in 1h); remit/soso 23h pass / 25h fail boundary; system_prices regex round-trip clean (48 rows × 0 errors) |
| NESO | 33 PASS / 0 EMPTY / 0 FAIL | 1 (5 datasets via 1 silver fix) | All 5 period-keyed datasets populate carbon/mix (7938 rows × 0% null on forecast/fuel/genpct) |
| ENTSOE | 9 PASS / 39 EMPTY / 0 FAIL | 2 (A09 dedup + B2 hygiene partial) | Kept commercial_schedules unchanged on live (GB→FR 3078B); B2 5b/5e fixes landed; 5a/5c/5d backlogged |
| ENTSOG | 29 PASS / 4 EMPTY / 0 FAIL | 1 (404 short-circuit) | Mocked: call_count=1 for empty 404; retries preserved for genuine 404 |
| GIE | 7 PASS / 0 EMPTY / 0 FAIL | 0 | n/a — no V1 issues to fix |
| Open-Meteo | 2 PASS / 0 EMPTY / 0 FAIL | 0 | n/a |

## Test status at close

```
$ uv run --offline pytest -m "not live and not slow" -x -q
1042 passed, 251 deselected, 1 warning in 72.57s
$ uv run --offline ruff check src/gridflow/connectors/elexon/endpoints.py \
                                  src/gridflow/silver/elexon/system_prices.py \
                                  src/gridflow/silver/neso/carbon_intensity.py \
                                  src/gridflow/connectors/entsog/client.py
All checks passed!
$ uv run --offline mypy --strict <V2-touched src files>
Success: no issues found
```

Pre-existing project-wide ruff drift (~83 warnings under `src/gridflow/`,
mostly TC003 / UP042 / UP017 hygiene) is unchanged by V2 — the file-count
delta is zero before vs. after every V2 commit.

## Backlog items added by V2

These items were uncovered or scope-narrowed during V2 work but are
out of V2 scope. Each will be added to `.planning/ROADMAP.md`
Backlog:

| Item | Source | Rationale |
|------|--------|-----------|
| Historical `freq` bronze re-ingest after V2-A param fix | V2-A | Existing bronze captured "latest 5761 samples" not the requested window — re-ingest needed for correct historical data |
| Historical NESO regional silver re-ingest for 5 affected datasets | V2-B | Existing silver carries null carbon/mix — re-run silver from existing bronze (or re-ingest if bronze missing fields) |
| ENTSOE A09 derive `net_position_mw` (Option B not taken in V2) | V2-D / ADR-019 | Keep both keys, pair zone-pair directions, emit signed net position. Useful for cross-border net flow analysis |
| ENTSOE A37/A15 pagination iteration | V2-D / 5a | Iterate `offset` until empty TimeSeries page; cap 9600. Currently silently truncates at 4800 TS for high-cardinality areas |
| ENTSOE A87 silver `Reason.code` exposure | V2-D / 5c | `_H8BalancingTransformer` refactor + new `reason_code` schema field |
| ENTSOE `area_name` field population | V2-D / 5d | New area_code → name lookup table OR schema removal |
| ENTSOE `DEFAULT_ZONES` wider EU baseline | V2-D / 5f | If a multi-region gold consumer materialises |
| ENTSOE `_RESOLUTION_MAP` calendar-correct `P1M`/`P1Y` | V1 entsoe-VALIDATION Recommendations §5 | Approximating month=30d, year=365d affects load_forecast_monthly, load_forecast_yearly |
| ENTSOE `activated_balancing_prices` reserve-type widening | V1 entsoe-VALIDATION Recommendations §6 | Connector currently fixes businessType=A96 (aFRR); silver schema supports FCR/aFRR/mFRR/RR |
| ENTSOE Pydantic schema vs silver Parquet column drift (B3) | V1 entsoe-VALIDATION §13 | EntsoeCrossborderFlow / EntsoeNetTransferCapacity declare narrower fields than transformer outputs |
| Manual ENTSOE Guide.pdf download | V1 entsoe-VALIDATION Recommendations §1 | CDN protection blocks programmatic fetch; human download recommended |
| ENTSOE GB pre-Brexit window re-validation | V1 entsoe-VALIDATION Recommendations §2 | Distinguish "permanently not published" from "publication-lag" via 2019/2020 GB window |
| GIE ALSI LNG validation | V1 V0.7 deferred | Backlog, unchanged |
| Vault directory rename `open-meteo` → `openmeteo` | V1 V0.7 deferred | Backlog, unchanged |
| Project-wide ruff baseline cleanup | V2 observation | ~83 pre-existing warnings (TC003, UP042, UP017) tolerated today; would clean up on a focused chore branch |

## Metadata

- Total live HTTP calls: ~14 (well below V1's ~50).
- Throttle: identical to V1 — Elexon 0.6s, NESO 0.2s, ENTSOG 1s, ENTSOE 1s.
- Tools: `curl --ssl-no-revoke -fsS`, Python 3.13 stdlib + polars for
  re-validation pipelines, `uv run --offline pytest` (Avast TLS quirk
  blocks `uv run` PyPI fetch; `--offline` flag uses cached package
  resolution).
- Avast `--ssl-no-revoke` workaround used throughout (V1-CONTEXT-locked).
- `gsd-sdk` unavailable — this report and STATE.md updated by direct
  edits.
