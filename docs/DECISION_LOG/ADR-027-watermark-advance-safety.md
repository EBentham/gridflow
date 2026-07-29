# ADR-027 - Watermark advance safety: compare-and-set writes, independent clamp/suppression

**Status:** Proposed
**Date:** 2026-07-29
**Phase:** R2-C (Partition integrity), `.planning/phases/R2-partition-integrity/R2-C-PLAN.md`
**Findings closed:** F-09 (watermark freeze → unbounded incremental window growth), C-8
(stamped-empty response counted as ingest evidence)
**Cross-references:** ADR-025 (temporal vintage / `available_at`), ADR-026 (partition-window
filters), `observability.py::{read_watermark,advance_watermark,update_watermark}`,
`pipeline/runner.py::resolve_incremental_window`

## Context

The ingest frontier (`pipeline_watermarks.last_end`) decides which time windows are ever
fetched again. Two defects met here.

**F-09.** When the frontier stopped advancing — legitimately, e.g. a vendor window that
returned nothing — every subsequent incremental run requested a window one run-interval
wider than the last, unboundedly, with no clamp and no signal. Per-day request chunking
amplifies it: a 30-day stall on Elexon PN is 30 days × up to 50 settlement periods of
requests.

**C-8.** A `200` response carrying a parsed-empty record array counted as evidence and
advanced the frontier past a window that returned nothing. It self-healed only via the
72h `incremental_overlap_hours` backstop.

The two are causally coupled and the coupling runs one way: fixing C-8 makes frontier
freezes *more* frequent (an all-empty window now correctly freezes instead of falsely
advancing), so F-09's fix is what makes a freeze survivable and visible. Shipping C-8
without F-09 strictly worsens the window-growth hazard.

Bounding the window is the easy half. The hard half is that the obvious implementation —
fetch a clamped tail, then advance the watermark — **permanently orphans** the skipped
interval, converting F-09 from *slow but complete* into *fast but lossy*. That is the
silent-data-loss class v0.18 exists to remove.

## Decision

### D-1 — The clamp bounds the fetch window only; the frontier never advances past unfetched time

`max_incremental_lookback_hours` (default 168h) caps the span of an incremental fetch
window. It never licenses an advance over time that was not requested in that same run.

### D-2 — Clamp and advance-suppression are INDEPENDENT predicates

These are separate concerns keyed on different conditions:

| Concept | Predicate | Effect |
|---|---|---|
| **Clamp** | `end_dt - raw_start > max_lookback` | window bounded, INFO |
| **Unfetched gap** | `clamp_start > frontier` | advance refused, WARNING |

A clamped run whose fetched window still covers all never-fetched time
(`clamp_start <= frontier`) **advances normally**. Coupling the two — "any clamped run
refuses to advance" — wedges the pipeline into a permanent false stall requiring manual
repair after any absence beyond the clamp trip point. At this deployment's ad-hoc run
cadence that fires routinely. The failure mode is invisible to tests; it was caught by
reasoning about operator cadence.

Advance permission is one ordered predicate evaluated on every path, with
`now_at_resolution` captured once:

1. snapshot unreadable → denied (`FRONTIER_UNREADABLE`)
2. `end_dt > now_at_resolution` → denied (`FUTURE_END`)
3. frontier known → permitted iff `start <= frontier` (else `UNFETCHED_GAP`)
4. frontier absent → permitted iff not clamped

### D-3 — The frontier write is compare-and-set

`advance_watermark(..., expected: WatermarkRead)` advances only if the stored value still
matches the snapshot the decision was made from: a conditional `UPDATE ... WHERE last_end = ?`
for the present case, `INSERT ... ON CONFLICT DO NOTHING` for the absent case, classified by
rows-changed. A mismatch writes nothing, fails closed, and reports `CAS_MISMATCH`.

Exactly one `read_watermark` call per resolution feeds both the window derivation and the
gap detection, so the two can never disagree.

**This CAS is not dead weight, and the reason is easy to get wrong.** An earlier revision
argued the race was unreachable: DuckDB permits one read/write *process*, the connection is
held for a whole run, and the dataset loop is sequential. **That premise is false.** DuckDB's
lock is per-**process**; multiple concurrent writers *within* that process are permitted —
confirmed against vendor documentation and by live probe (`con.cursor()` and a second
in-process `connect()` both write successfully). Correctness must not rest on a topology
argument that connection pooling, a service mode, or a split read-only snapshot would
silently invalidate.

`update_watermark` retains its unconditional `GREATEST` upsert for the seed/admin arm and is
the **only** unconditional writer; an AST test pins that no production module outside
`observability.py` calls it.

### D-4 — `record_count` is three-valued, and a parse failure is never zero

`RawResponse.record_count: int | None` — `None` = this connector did not determine a count
(treated as evidence, preserving prior behaviour), `0` = the vendor returned zero records,
`>0` = that many. The ingest-boundary predicate is
`http_status < 400 and record_count != 0`.

**A parse failure stamps `None`, never `0`.** A `0`-on-failure sentinel would convert every
malformed body into a permanent frontier freeze. Note ENTSO-E's `count_timeseries_or_none`
deliberately maps a parse failure to `None` for stamping while the pagination loop keeps its
own zero-on-failure termination behaviour — the two contracts are separate on purpose.

A site is stamped only where the parse already exists on its path (never add a parse to
count); every unstamped site is declared exempt-with-reason in a machine-checked registry.
`record_count` is deliberately **absent** from the bronze sidecar — it is derived, and the
sidecar is irreproducible.

## Alternatives considered

- **Clamp the END instead of the start** (fetch the oldest window from the frozen frontier
  and walk forward) — rejected for this cycle. It self-heals and satisfies D-1, but silently
  trades away *freshness*: a pipeline stalled 30 days would not fetch today's prices for
  several more runs, and nothing in the signal would say so. Recorded as a follow-up
  question, not taken.
- **Advance to the clamp start** — rejected: that *is* the orphan.
- **A "sole writer" topology proof instead of CAS** — rejected because its premise is false
  (above). Retained only as a defence-in-depth comment naming what would break it.
- **Persisting each suppressed run's coverage** to remove the bookkeeping residual —
  rejected: new per-pair persisted state, which exceeds this unit's agreed scope. Filed as an
  open question alongside an admin `watermark set` escape hatch.
- **Changing `update_watermark`'s signature to require `expected=`** — rejected: 11 test call
  sites, 9 of them pure seeding, two outside this cycle. An optional `expected=None` was also
  rejected — a safety parameter that defaults to unsafe decays.

## Consequences

**Makes safer.** No incremental run can advance the frontier over time it did not fetch. A
stall is bounded in cost and visible: one aggregated record per (source, dataset) per run,
severity = max of fired conditions, naming the frozen frontier, stall duration, run count,
gap bounds and a **gap-bounded** repair command. A concurrent writer can no longer make a
stale-snapshot decision land.

**Makes harder.** A frontier with a true gap does **not** self-heal — deliberately. It needs
the operator to run the gap-bounded repair, and it re-fetches the full clamped window each
run until then (duplicate bronze bytes, which is safe but wasteful). CAS also introduces a
new refusal mode: a legitimate advance can lose to a competing writer, fail closed, warn, and
be retried next run. That is strictly preferable to a silent orphan.

**Accepted risks.** An operator who runs the repair with `--end <now>` instead of the
gap-bounded form can advance on tail evidence past a still-empty gap; the emitted warning
never suggests that form. And an always-empty dataset now freezes its frontier rather than
falsely advancing — a real defect surfaced rather than hidden, but it requires a human.

**Unchanged.** `incremental_overlap_hours` stays 72 and remains the backstop for unstamped
connectors and for late/revised publications. No schema change, no on-disk format change, no
data operation. `written_at` remains a start-of-write stamp (`bronze/writer.py`), so no
silver-side reconstruction can literally establish "never earlier than the true write time" —
that residual is tracked separately and is unaffected by this ADR.
