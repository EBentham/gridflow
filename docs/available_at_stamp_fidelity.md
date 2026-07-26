# `available_at` stamp-fidelity

**Date:** 2026-07-17 (corrected 2026-07-26, R1-B batch, F-20) · **Authority:** [ADR-025](DECISION_LOG/ADR-025-temporal-vintage-and-revision-capture.md) §3 · **Item:** v0.17 P1.1 (R1-F07 + R5-F04)

`available_at` is derived at the silver-write boundary
(`silver/base.py::_add_bitemporal_columns`) as a **row-wise coalesce**:

```
available_at = coalesce(published_at, ingest_time)
```

When a transformer emits a non-null `published_at` (the vendor publication
vintage) for a row, that row's `available_at` is the vintage; otherwise it falls
back to the ingest/reingest clock. This table records, per dataset, whether
`available_at` is a genuine vendor vintage or an honest ingest-time fallback — the
contract downstream consumers (the `gridflow_models` point-in-time barrier;
review items P2.30/P2.32) read to know which datasets support honest historical
`as_of` queries.

## Fidelity labels

- **true vintage** — `published_at` is sourced from a vendor publication stamp
  whose semantics are established (a forecast/auction issue time, or an Elexon
  `publishTime`); `available_at` is a genuine "when this was knowable" instant.
- **unverified vintage** — `published_at` is mechanically wired from the ENTSO-E
  document `createdDateTime`, but that field's provenance for this document family
  is unconfirmed (submission-time vs. serialization-time).
  **RESOLVED 2026-07-26 (C-13 probe): request/serialization artifact** — see the
  ENTSO-E section and "Upgrade path (resolved)" below. Label resolves to
  ingest-grade per the pre-committed rule.
- **ingest-time fallback** — no vendor stamp is emitted (the feed has none, or the
  transformer consumes `publishTime` as its event/timestamp axis rather than
  emitting `published_at`); `available_at` is the ingest/reingest clock, honestly.

## Elexon

| Dataset | Fidelity | Source field | Notes |
|---|---|---|---|
| agpt, agws, atl, demand_forecast, fou2t14d, imbalngc, inddem, indgen, indo, itsdo, lolpdrm, melngc, nonbm, tsdfd, uou2t14d, **windfor** | **true vintage** | `publishTime` / `publishDateTime` → `published_at` | 16 emitters. `windfor` is the only one with on-disk silver — re-transformed under 4.1b so its `available_at` reflects the forecast-issue spread. |
| fuelhh | **true vintage** (code fixed 2026-07-26; on-disk repair at R2 exit) | `publishTime` / `publishDateTime` (coalesced) → `published_at` | R1-B batch (F-03): the transformer previously mapped only `publishDateTime`, so bronze rows carrying `publishTime` instead (29,400/29,400 on-disk rows) had `published_at` silently null. The code now coalesces both raw field names. No re-transform was performed as part of this fix — existing on-disk silver stays null until the R2-exit rebuild re-runs this transformer from bronze. |
| soso, indod, tsdf | **true vintage** (code fixed 2026-07-26; on-disk repair at R2 exit) | `publishTime` → `published_at` (independent of, and distinct from, their own event-time axis: `start_time` / `settlement_date` / `settlement_period`) | R1-B batch (F-08): all three receive their own `publishTime` alongside separate event-time fields — a genuinely independent vendor vintage, same class as the 16-emitter cohort above. Previously the rename map produced `published_at` but `output_cols` dropped it before write (the same W2.2-pattern bug G6 fixed elsewhere), so it never reached silver. The former doc entry lumped these three in with remit/fuelinst below — a distinct class error: remit/fuelinst have NO independent vintage field (their `publishTime` IS their event time, consumed into `timestamp_utc` and not retained separately); soso/indod/tsdf do. No re-transform performed; on-disk repair rides the R2-exit rebuild. |
| system_prices | ingest-time fallback | — | live DISEBSP DATE_PATH feed exposes no `publishTime` (ADR-025 §Context); honest fallback, not a gap. |
| bmunits_reference | ingest-time fallback | — | static reference snapshot; `publishTime` not mapped. |
| remit, fuelinst | ingest-time fallback | `publishTime` → `timestamp_utc` | map `publishTime` as their event/timestamp axis and do NOT retain a separate `published_at` in output — REMIT's publish time and FUELINST's instantaneous reading time both ARE the event time for these datasets, so there is nothing independent left to emit once `timestamp_utc` is derived. **Correction (R1-B, F-20):** the previous rationale here — "emitting it for remit would invert `event_time <= available_at`" — was FALSE. No such invariant exists in this codebase: `windfor` already legitimately inverts it (X2-F03: 802/1022 rows), and soso/indod/tsdf above emit an independent vintage without issue. The real reason remit/fuelinst don't emit a separate `published_at` is architectural (no independent field survives once consumed into `timestamp_utc`), not invariant-protection. |
| temp, and all other Elexon datasets | ingest-time fallback | — | no `publishTime` emitted as `published_at`. |

## ENTSO-E

Every timeseries-parsed dataset (25 of 26 modules) now emits `published_at` from
the document `createdDateTime` (typed-null when the document lacks it). Fidelity is
split by whether `createdDateTime` is an established vendor vintage.

| Dataset(s) | Fidelity | Notes |
|---|---|---|
| generation_forecast, load_forecast, load_forecast_weekly, load_forecast_monthly, load_forecast_yearly, wind_solar_forecast | **true vintage** | forecast issue-time; established semantics. |
| day_ahead_prices, forecast_margin, contracted_reserves, net_transfer_capacity, installed_capacity, installed_capacity_units | **true vintage — CAVEAT measured 2026-07-26** | ex-ante / day-ahead products; `installed_capacity*` noted "thin spread — annual cadence". **Caveat:** the C-13 probe ran on A44 (day_ahead_prices) itself and proved the ENVELOPE `createdDateTime` is request-generated. The issue-time reading therefore holds only under prompt periodic ingest (fetch ≈ publication); a backfilled `published_at` is fetch-time for these rows too. Per-family re-derivation of these labels is filed as v0.18 N-3. |
| actual_load, actual_generation, actual_generation_units, cross_border_flows, imbalance_prices, imbalance_volume, activated_balancing_prices, activated_balancing_qty, outages_generation, water_reservoirs; the 4 outages (h7) datasets; the 15 transmission/market (h6) datasets; the 6 balancing (h8) datasets | **ingest-grade (RESOLVED 2026-07-26 — request artifact, measured)** | observational / mixed families. The authorized C-13 probe (see Upgrade path) measured the API envelope `createdDateTime` as a **per-request generation stamp**, so `published_at` for these families is fetch-time: an acceptable publication proxy ONLY under prompt periodic ingest, never for backfills. Do NOT treat backfilled `published_at` as vendor publication history. |
| generation_units_master_data | ingest-time fallback (structural) | its parser carries no `createdDateTime` field; deliberately NOT wired. |

## Open-Meteo

| Dataset(s) | Fidelity | Notes |
|---|---|---|
| all Open-Meteo datasets | ingest-time fallback | the API exposes no document-level publication timestamp; no coalesce input exists. |

## Upgrade path — RESOLVED 2026-07-26 (C-13 probe)

The rule pre-committed here was: "a value that tracks fetch time indicates a
request/serialization artifact (label stays ingest-grade)." The authorized probe
(2026-07-26, orchestrator-run, no bronze written) applied an equivalent-or-stronger
protocol: two A44 fetches of the SAME historical window (DE-LU, 2024-01-15), 32
seconds apart. Result: **identical price payload, but a fresh `mRID` and a moving
`createdDateTime` (14:06:41Z → 14:07:13Z) each time** — the API generates the
document per request and stamps it with the request clock. For a January-2024
window fetched in July 2026, `createdDateTime` was the fetch moment, not any 2024
publication instant.

Consequences: the **unverified** families above resolve to **ingest-grade** per the
pre-committed rule (not upgraded — resolved downward, honestly). The
**true-vintage** ENTSO-E rows keep their label only under the prompt-ingest
reading (fetch ≈ publication); their per-family re-derivation is v0.18 N-3.
Elexon labels are unaffected (`publishTime` is an in-payload vendor field, not an
envelope stamp).

Downstream: P2.30 (per-fold publication cutoff) and P2.32 (tie-break dedupe on
`available_at` / `published_at`) consume the **true vintage** column — do not weaken
a label to make the table look more complete.
