# ADR-031 — Vintage policy reconstruction for historical ingest-clock series

**Status:** proposed
**Date:** 2026-09-06
**Phase:** v0.20 V-a, Backfill and Benchmark
**Cross-references:** ADR-024 (manifest), ADR-025 (capture and availability),
ADR-018 (append-only filenames), ADR-026 (partition windows).

## Context

MID, DISEBSP system prices, and the three Open-Meteo historical weather
transformers emit no vendor publication timestamp. Backfilling a 2021 event in
2026 currently makes its `available_at` a 2026 capture/transform instant. A models
availability barrier for 2021–2025 therefore admits nothing.

An unconditional minimum of ingest and event-plus-lag would also backdate honest
live DISEBSP revision captures. Its feed has neither publication time nor run
type; separate sidecars are the only reliable live vintage signal. The amended
V-a spec adopts the review's exclusive event-time cutover to protect that era.

## Decision

Source ruling: programme page § 4 T3 clause / `v0.20-ROADMAP.md` §Scoping
decision 2 (2026-09-05): reconstruct from explicit, dated, approximate
per-dataset rules, labelled per row.

Declare frozen `VintagePolicy(name, lag, dated, rule, applies_before)` on each
opted-in transformer; the base defaults to `None`. The cutover must be tz-aware
UTC. Derive event time in UTC before computing availability:

1. Non-null vendor `published_at` wins, labelled `vendor`.
2. Otherwise use `event_time + lag` only if `event_time < applies_before` **and**
   `event_time + lag < ingest_stamp`, labelled with the exact policy name.
3. Otherwise use ingest, labelled `ingest-clock`. Equality at either boundary
   and a null event time retain ingest.

The logic does not inspect CLI mode. Ingest still means the existing scalar
clock, maximum reingest sidecar, per-file DISEBSP capture, or lockstep row carrier,
as appropriate. Preserve the original capture scalar for append-only filenames;
reconstruction must not collapse distinct files or change reingest idempotency.

`vintage_policy` is a non-null String lineage column with a closed label set
checked at derivation; invalid generated values raise. It is **not** a Pydantic
schema field. Measurement validation still precedes lineage stamping. Forecast
weather subclasses explicitly opt out with `VINTAGE_POLICY=None`. Datasets with
no policy keep their existing bitemporal derivation and gain no label column.

The ADR-024 manifest conditionally declares the lineage column and exposes a
summary containing policy name, prose rule, integer `lag_seconds`, `dated`
(a date, exported as an ISO string), and ISO UTC `applies_before`; the
system-prices serving alias carries the same contract. Legacy rows read as null;
models code filtering on the label must treat null as unknown. An all-legacy
selection can lack the column altogether; absence also means unknown until
Phase D retransforms bronze. No schema migration or reader rejection is added.

`vintage_policy` joins `BITEMPORAL_EXCLUDE`, following availability's exclusion
semantics. The seat ruled that the label travels with the stamp and applied
`_VINTAGE_VISIBLE = ("available_at", "vintage_policy")` in `serving/client.py`.
System-price and imbalance-context convenience reads retain both. The imbalance
gold view projects the winning label via `to_json(sp)->>'vintage_policy'`,
returning NULL for an absent legacy column. No new client method is added.
The optional source-run tie-break in `_latest` is omitted.

## Dated assumptions

All policies are dated **2026-09-06**, named
`<source>-<dataset>/vp-2026-09`. The seat transcribes these rules to the vault.

| Dataset family | Assumed lag from event time | Exclusive assumed cutover |
|---|---|---|
| `open_meteo/historical_demand`, `historical_wind`, `historical_solar` | 5 days | 2026-08-01T00:00Z |
| `elexon/mid` | 60 minutes (period end +30 minutes) | 2026-08-01T00:00Z |
| `elexon/system_prices` | 90 minutes (period end +60 minutes) | 2026-07-31T00:00Z |

- **Open-Meteo ASSUMPTION:** event time +5 days, based on SPEC's citation to
  `30-vendors/open-meteo/datasets/historical_demand.md:45`: "~5 days behind real
  time, ERA5 reanalysis cadence". **ASSUMPTION cutover: 2026-08-01T00:00Z**, the
  August smoke ingest. The vault was not accessed.
- **MID — measured 2026-09-06, and the declared lag is a conservative upper
  bound.** The value in code stays period end +30 minutes; it was originally
  an analogy with INDO's measured latency (99.6% of 87,261 rows) and carried
  `TODO: verify`. That TODO is now answered by observation rather than by
  vendor documentation, which does not state a publication cadence for this
  dataset.

  At 15:01Z the public endpoint's latest available period was settlement
  period 32, covering 14:30–15:00Z, which had ended one minute earlier.
  Settlement period 33 was in progress and absent. Two conclusions follow.
  MID is **not** published ahead of delivery, so treating it as knowable
  before its period ends would be wrong. And its true latency after period
  end is **at most about one minute**, far shorter than the declared +30.

  The declared lag therefore errs in the safe direction: a consumer sees the
  price roughly 30 minutes later than it truly became available, never
  earlier, so the reconstruction cannot leak. Tightening it toward the
  observed value would improve realism and is backlogged with the run-type
  work, because it changes stamped bytes and needs a re-transform. Caveat
  the evidence honestly: one observation, on one day, of the current
  endpoint. **ASSUMPTION cutover: 2026-08-01T00:00Z** is unchanged.

  Note for anyone repeating this check: settlement periods are numbered on
  UK local time, so during BST the period's UTC start is one hour behind its
  local label.
- **DISEBSP ASSUMPTION — TODO: verify DISEBSP initial-publication latency and
  revision timing.** Proposed period end +60 minutes is an analytical allowance
  for price calculation beyond MID's assumed delay, neither a vendor cadence
  nor a proven conservative bound. **ASSUMPTION cutover: 2026-07-31T00:00Z**, where
  live silver begins and the backfill target ends. The seat takes the proposed
  lag to Bobbo.

## Consequences

Pre-cutover events become available under a visible approximation. This does
not recover historical publication or revision times: a later correction of an
old event may be admitted earlier than its actual publication. Policy labels
communicate that limitation; they are not proof of leakage-free history.

Two disclosed residuals remain. First, two pre-cutover captures of the same
period collapse to one `available_at` even with null `run_type`, as the
integration test demonstrates. `_latest` then falls to the run-rank tie-break;
with both run types null, that also ties, so the winning capture is unspecified.
Distinct capture files survive, but their availability ordering does not.
Second, re-ingesting a pre-cutover period after a late settlement revision
(II→R1→R3) stamps each revision `event_time + lag`, making it visible earlier
than it existed. This is the accepted approximation, including the boundary
window roughly May through 2026-07-30 whose later runs land after cutover.
The fix class is run-type-aware system-price lags, filed in
`.planning/BACKLOG.md` for a Bobbo ruling, outside this unit.

Mixed legacy and labelled silver files rely on `union_by_name=true` when
registering silver parquet views; missing labels read as NULL. The gold
projection also tolerates an all-legacy tree with no label column at all.

At/after cutover, DISEBSP keeps honest captures and live revision ordering, even
when fetched months later. Fixed timedelta arithmetic on UTC period starts
handles settlement periods 1..50 on DST days. Changes to a lag or cutover need
a new dated policy identity and deliberate re-transformation.

Validation covers each policy transformer, both strict boundaries, vendor
precedence, lockstep stamps, forecast opt-outs, DST periods, manifest export,
temporary-root backfill integration, capture-file preservation, and unchanged
no-policy frame/Parquet bytes. Existing golden/parity tests remain untouched.
The `run_backfill` integration test stubs `_register_gold_views` because its
temporary fixtures lack unrelated cross-source inputs; it does not prove full
gold registration. Separate imbalance-context tests exercise that actual SQL
view and its serving convenience read for labelled and legacy silver.
