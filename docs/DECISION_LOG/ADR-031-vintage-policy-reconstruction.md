# ADR-031 — Vintage policy reconstruction for historical ingest-clock series

**Status:** proposed
**Date:** 2026-09-06
**Phase:** v0.20 V-a, Backfill and Benchmark
**Cross-references:** ADR-024 (manifest), ADR-025 (capture and availability),
ADR-018 (append-only filenames), ADR-026 (partition windows).

## Context

MID and the three Open-Meteo historical weather transformers emit no vendor
publication timestamp. DISEBSP raw records carry `createdDateTime`, but silver
omitted it until v0.21-L. Backfilling a 2021 event in 2026 therefore previously
made its `available_at` a reconstructed or capture/transform instant.

The latest-settlement-run endpoint describes which settlement calculation is
returned; that endpoint semantic is separate from the unresolved meaning of
each record's `createdDateTime`. Vendor publication now wins when present. The
existing exclusive-cutover policy remains only a logged missing-vendor fallback.

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

Policies carry their own declaration date and identity.

| Dataset family | Assumed lag from event time | Exclusive assumed cutover |
|---|---|---|
| `open_meteo/historical_demand`, `historical_wind`, `historical_solar` | 5 days | 2026-08-01T00:00Z |
| `elexon/mid` | 35 minutes (period end +5 minutes), `elexon-mid/vp-2026-09b`, dated 2026-09-07 | 2026-08-01T00:00Z |
| `elexon/system_prices` | Vendor `createdDateTime`; 90-minute declared fallback, dated 2026-09-06 | 2026-07-31T00:00Z |

- **Open-Meteo ASSUMPTION:** event time +5 days, based on SPEC's citation to
  `30-vendors/open-meteo/datasets/historical_demand.md:45`: "~5 days behind real
  time, ERA5 reanalysis cadence". **ASSUMPTION cutover: 2026-08-01T00:00Z**, the
  August smoke ingest. The vault was not accessed.
- **MID — measured 2026-09-06.** The declaration is
  `elexon-mid/vp-2026-09b`, dated 2026-09-07. **ASSUMPTION: 35 minutes from
  period start**, equivalent to period end plus a five-minute margin.

  At 15:01Z the public endpoint's latest available period was settlement
  period 32, covering 14:30–15:00Z, which had ended one minute earlier.
  Settlement period 33 was in progress and absent. Two conclusions follow.
  MID is **not** published ahead of delivery. The approximately one-minute
  observation motivates the five-minute margin but does not prove a historical
  upper bound. Current Insights MID publication latency remains undocumented.
  Provider submission targets in
  [BSCP01](https://bscdocs.elexon.co.uk/bsc-procedures/bscp-01-overview-of-trading-arrangements)
  and the superseded BMRA requirement do not establish a current five-minute
  availability bound. **TODO:** obtain current vendor documentation.
  **ASSUMPTION cutover: 2026-08-01T00:00Z** is unchanged.

  Note for anyone repeating this check: settlement periods are numbered on
  UK local time, so during BST the period's UTC start is one hour behind its
  local label.
- **DISEBSP vendor stamp and fallback.** `createdDateTime` is now preserved as
  `published_at` and drives availability. The unchanged declared 90-minute
  policy is used only when that field is missing, with an explicit counter and
  warning. The [current OpenAPI](https://data.elexon.co.uk/swagger/v1/swagger.json)
  documents latest-settlement-run messages. **TODO:** establish whether
  `createdDateTime` identifies the initial calculation, latest/D+1 refresh, or
  merely the record held.

  [Elexon's February 2024 announcement](https://www.elexon.co.uk/bsc/article/indicative-settlement-price-data-now-available-on-the-insights-solution/)
  documents the new D+1 refresh. Its timing matches the measured breakpoint;
  direct attribution of these stamps is unconfirmed. **TODO:** vendor
  confirmation and a pre-February backfill selection rule. Measured raw stamp
  latency shifts from a median near 0.87 hours in 2021–2023 to about 24.74
  hours in 2024–2026. Vendor stamping is therefore not uniformly conservative:
  historical availability can move earlier while later years move substantially
  later. The fallback cutover remains **2026-07-31T00:00Z**.

## Consequences

Pre-cutover events become available under a visible approximation. This does
not recover historical publication or revision times: a later correction of an
old event may be admitted earlier than its actual publication. Policy labels
communicate that limitation; they are not proof of leakage-free history.

System-price vendor stamps resolve this defect without inventing run-type-aware
availability. In the measured doubled-key cohort, the existing `_latest`
projection selects the later stamp for all **85** keys. For **14** keys, the
winning payload is identical at tied physical captures, so no deterministic
filename winner is claimed. Capture files and multiplicity remain preserved;
this projection is not a conservation deduplication rule.

Residual 2c remains open: latest-settlement-run semantics do not establish what
`createdDateTime` means, and neither the OpenAPI nor the D+1 announcement proves
an initial-publication timestamp. The TODOs above remain required; this ADR does
not declare that residual closed.

Mixed legacy and labelled silver files rely on `union_by_name=true` when
registering silver parquet views; missing labels read as NULL. The gold
projection also tolerates an all-legacy tree with no label column at all.

DISEBSP now orders known vintages by the vendor stamp independently of cutover;
capture remains the fallback and append-only filename scalar. Fixed timedelta
arithmetic on UTC period starts handles settlement periods 1..50 on DST days.
Changes to a fallback lag or cutover need a new dated policy identity and
deliberate re-transformation.

Validation covers each policy transformer, both strict boundaries, vendor
precedence, lockstep stamps, forecast opt-outs, DST periods, manifest export,
temporary-root backfill integration, capture-file preservation, and unchanged
no-policy frame/Parquet bytes. Existing golden/parity tests remain untouched.
The `run_backfill` integration test stubs `_register_gold_views` because its
temporary fixtures lack unrelated cross-source inputs; it does not prove full
gold registration. Separate imbalance-context tests exercise that actual SQL
view and its serving convenience read for labelled and legacy silver.
