# ADR-026 - Partition-window filters (publication-window + event-window)

**Status:** Proposed
**Date:** 2026-07-26 (amended same day, and again 2026-08-03 — see the "Amendment" sections below)
**Phase:** R2-A (Partition integrity), `.planning/phases/R2-partition-integrity/R2-A-PLAN.md`
**Findings closed:** F-04 (Elexon boundary duplication), F-16 (duplicate-check key). F-10
(ENTSO-E vendor over-span) is closed for the opted-in population only — OPEN pending the N-9
gate, 5 datasets unclassified — scoped, see D-9 below
**Cross-references:** ADR-025 (temporal vintage / `available_at`, incl. its §3 residual for
`remit`/`fou2t14d`), `_EXACT_PARTITION_ONLY_SOURCES` (`silver/base.py`)

## Amendment (2026-08-03, R2-g / F-05) — `entsog` joins `_EXACT_PARTITION_ONLY_SOURCES`

A membership change to a decision this ADR already owns, not a new decision.

`entsog` joins `entsoe` in `_EXACT_PARTITION_ONLY_SOURCES` (`silver/base.py`), so neither of the
two gated callers — `_bronze_path_for_date` (the READ path) and `_bronze_date_dirs` (the vintage
path) — may resolve a covering bronze partition for an ENTSO-G date. **Evidence:**
`EntsogConnector.fetch` (`connectors/entsog/client.py`) chunks every multi-day window into one
request per covered UTC calendar day, so a correctly-fetched ENTSO-G date either has its own
exact partition or has no bronze at all; a covering-fallback hit could only relabel a
neighbouring day's rows under the requested date. Before this change,
`PhysicalFlowsTransformer.read_bronze` could reach back **35 days**.

Post-R2-g the `_bronze_date_dirs` gate is **dead with respect to `entsog`** — the lockstep read
path resolves the vintage from the same vouched set as the read, so neither ENTSO-G family calls
that method any more. The gate is left in place because it remains correct for `entsoe`.

**The vintage half of F-05 is NOT recorded here.** See **ADR-028** for the exclude-until-vouched
ruling, the one-scan lockstep invariant, the per-row vintage attribution, and the three-valued
run outcome — including why the two halves had to ship together, and the narrowed statement of
what "F-05 closed" does and does not cover.

## Amendment (2026-07-26, same day as first draft)

This ADR's first draft applied Elexon's neighbour-durability proof (D-3b) to ENTSO-E's lower
bound too, framing both as "asymmetric bound enforcement, both proven by durability" and
accepting a "Known limitation" that batch-edge partitions **retain** their vendor over-span
indefinitely when no neighbour bronze exists. Sol — the cross-model reviewer that had approved
the originating plan across five review passes — independently reached the same conclusion the
executor raised during implementation: that framing is **wrong**, and the plan it came from was
internally inconsistent. The decisive distinction is the **vendor's own request-interval
semantics (closed vs half-open)**, not which proof mechanism happens to be reused. This amendment
replaces the D-3/D-3d and Consequences sections below with the corrected TRIM semantics; the
Context, D-1/D-2, D-3b/D-3c (Elexon only), D-7(e), D-5, D-6, D-8, D-9, and Residuals sections are
unaffected and unchanged from the first draft.

## Context

Two independent vendor behaviours were producing incorrect silver output from otherwise-correct
bronze:

1. **F-04 — Elexon publication-window boundary duplication.** Elexon's `PUBLISH_DATETIME`
   endpoints (`indo`, `fuelhh`, `remit`, and 21 others) chunk requests into contiguous
   `[publishDateTimeFrom, publishDateTimeTo]` windows that the vendor treats as **closed at both
   ends**. Consecutive chunks share their boundary instant, so the record published exactly at
   that instant — SP2 of settlement day D+1, published at D+1 00:00:00Z — is written into **both**
   the D and the D+1 bronze partitions. Measured on-disk: 29 duplicated keys in `indo` (1470 rows,
   1441 distinct), 580 in `fuelhh` (29400 rows, 28820 distinct; each duplicated key is 20 fuel-type
   rows), both exactly at the SP2 boundary.

2. **F-10 — ENTSO-E vendor over-span.** A UTC-day silver read requests
   `[periodStart, periodEnd)` but the vendor returns whole **CET/CEST delivery days**, over-spanning
   on both sides of the request. Measured: a `2024-01-15T00:00Z → 2024-01-16T00:00Z` request
   returned points spanning `2024-01-14T23:00Z → 2024-01-16T23:00Z` (48h instead of 24h).

A third, related defect (F-16) was that the duplicate-quality-check hardcoded
`(settlement_date, settlement_period)` as every Elexon dataset's business key, which falsely
flagged every genuinely-distinct row of a finer-grained dataset (`fuelhh`'s `fuel_type`) as a
duplicate — masking the real F-04 duplication under 27,959 false positives.

### The settlement-date key is a data-loss trap, not a simplification (§1.2)

The obvious "fix" — filter Elexon rows to `settlement_date == partition_date` — was measured and
rejected. An Elexon `PUBLISH_DATETIME` partition is a **publication** window, not a settlement
day: `indo_20260710.parquet` holds `settlement_date=2026-07-10` SP2..SP48 **and**
`settlement_date=2026-07-11` SP1, SP2 — while `2026-07-10` SP1 lives entirely in the PRIOR day's
partition (published 2026-07-09T23:30Z). A settlement-date-keyed filter would permanently delete
**SP1 of every settlement day** — 31 keys for `indo`, 620 for `fuelhh` — a much larger loss than
the 29/580 duplication it was meant to fix. The half-open **request-window** filter (below) is
exact and independent of chunk size or midnight alignment, correctly handling `remit`/`soso` (23h
chunks) and non-midnight-aligned `--last 24h` windows where a calendar-date filter would silently
delete rows.

## Decision

### D-1/D-2 — two partition semantics, one primitive family, two application paths

`silver/partition_window.py` hosts a source-agnostic primitive family applied under two different
**vendor interval semantics** (`IntervalSemantics`, see D-3 below):

| source | partition means | filter dimension | window params |
|---|---|---|---|
| `elexon` (23 of 24 in-scope datasets) | a **publication** window | `published_at` | `publishDateTimeFrom`/`publishDateTimeTo` |
| `elexon/remit` (D-1b) | same, but `remit` derives `timestamp_utc` deterministically from `published_at` and drops the source column at select | `timestamp_utc` | same |
| `entsoe` (opted-in per `EVENT_WINDOW_CLASSIFICATION`, D-9) | a **UTC delivery day** | `timestamp_utc` | `periodStart`/`periodEnd` |

For ENTSO-E, `[periodStart, periodEnd)` is DST-invariant because CET/CEST never enters the
computation (D-2) — the window is a UTC instant range, not a calendar-local one.

### D-3 — the decisive distinction is vendor interval semantics, not source name (amended)

Two request-application paths exist in `silver/partition_window.py`, selected by
`IntervalSemantics` (`CLOSED` | `HALF_OPEN`) — a property of **how the vendor interprets its own
`[from, to]` request**, carried on `_PublicationWindowPlan.interval_semantics` (default `CLOSED`),
never an `if source == "..."` branch. This is deliberate: a future connector inherits the correct
behaviour automatically by declaring which interval its own vendor uses, without anyone
re-deriving the argument below.

**`CLOSED` (Elexon) — `filter_frame_to_window`.** The vendor treats its OWN request window as
closed at BOTH ends (`publishDateTimeFrom`/`To`, §1.1 above). The row AT the boundary is
genuinely PART of the request and is legitimately returned by two adjacent chunks — ownership is
real and shared, so removing it from one side requires a neighbour-durability proof (D-3b, below)
before the trim is safe. Elexon enforces its UPPER bound only (rows below `publishDateTimeFrom`
are counted and kept, `WARNING`) — the trailing partition with no successor is the only case this
gate needs to cover, and it correctly **retains** there (duplication preferred to loss). **This
path, and its retention behaviour at an unproven boundary, is unchanged by this ADR's amendment.**

**`HALF_OPEN` (ENTSO-E) — `exclude_out_of_window`.** The vendor's OWN request is
`[periodStart, periodEnd)`, but the vendor may return whole CET/CEST delivery days beyond it
(measured over-span, S1.4). A row outside that half-open interval was **never part of THIS
partition's request in the first place** — there is no ownership to resolve, because nothing
ever claimed joint ownership of it. Keeping it in silver both violates this partition's own
declared scope and plants a duplicate for whenever the owning date is properly ingested. Bronze
is immutable and remains the source of truth for that row under its own correctly-scoped
partition (once/if that partition is ever transformed) — excluding it here is "do not
materialise an out-of-scope row in silver", never "delete the source of truth". Both bounds are
therefore enforced **unconditionally**, with no ownership-verdict argument at all: dropped,
counted (`last_partition_filter_dropped_count`), and logged
(`WindowReason.OUT_OF_REQUEST_SCOPE`) — never silent.

**The mechanism this replaces.** An earlier revision of this design applied the CLOSED-interval
neighbour-durability proof to ENTSO-E's lower bound too (a per-instant, never-per-date resolution
against a predecessor partition — sound in isolation, and its underlying primitive machinery
remains correct, tested, and available for any future CLOSED-interval connector that needs it).
Sol confirmed this was the wrong semantics for a HALF_OPEN vendor interval: it treated
genuinely-out-of-scope vendor over-return as if it were a genuinely-requested, jointly-owned
boundary row, and its stated justification ("ENTSO-E enforces both bounds because a trimmed row
is re-homed in an adjacent UTC-day partition") does not hold at a fetch batch's own edges, where
no adjacent partition was ever requested — the row cannot be "re-homed" anywhere, so retaining it
indefinitely was the actual defect, not the fix.

### D-3b/D-3c — the neighbour-durability proof (Elexon / CLOSED interval only, unchanged)

A row is trimmed at a window bound **only** when a neighbour partition is **proven** to hold that
row's own instant durably — never on the promise of a future ingest, and never on the inference
that "the fetch completed" implies "the partition is durable". `pipeline/runner.py:415-417` writes
responses **one at a time**, atomic only per body/sidecar pair (`bronze/writer.py:64-86`), so a
crash between two writes can leave a partition where every *present* body is correctly paired
(no orphan) yet the response set is not actually complete — inferring durability from pairing
alone would let a predecessor "prove" ownership of a row it does not durably contain, deleting it
from its only other home. `covering_chunk_is_durable` closes this gap with a **page-completeness
proof**: it groups the partition's valid sidecars by their recorded `(from, to)` window, takes the
group covering the target instant, and requires that group to be BOTH fully paired (every page's
sidecar has its raw body on disk) and page-complete (the pages present are exactly `1..
max(total_pages)`).

**Why chunk-scoped, not partition-wide.** The obligation is an **existence** proof — *some*
durable response set covering the instant exists — not a claim that the covering chunk is the
*only* thing that could physically carry the row. Overlapping historical requests and the
vendor's own inclusive responses can and do produce other carriers of the same instant (that is
the very mechanism behind F-04). Reducing the proof to "one covering chunk materialised" (rather
than "the whole partition materialised") is what keeps this a bounded, ingest-layer-change-free
fix: a whole-partition proof would need a per-fetch response count or manifest bronze does not
record today. A future reader must not "simplify" this back to a partition-wide check — it would
not be safer, and the likely resolution under time pressure would be to drop the proof entirely.

An orphan raw body in a **non-covering** chunk of the *neighbour* partition must not block
ownership — sidecars are discovered by scanning `raw_*.meta.json` (not raw bodies), so a body with
no sidecar in a different window group never enters any group and cannot affect the covering
group's verdict.

This proof, and its retention-on-unproven behaviour, is **exclusively a CLOSED-interval (Elexon)
mechanism** — `exclude_out_of_window` (ENTSO-E's HALF_OPEN path) takes no ownership-verdict
argument and calls none of this machinery. The per-instant (never per-date) resolution this
proof's lower-bound variant used is still correct, tested primitive machinery
(`filter_frame_to_window`'s `lower_bound_ownership` parameter) — simply not currently exercised by
any live call site, since Elexon never enforces its own lower bound (D-3) and ENTSO-E no longer
uses a durability proof at all. A future CLOSED-interval connector needing a lower-bound proof
would reuse it unchanged; the underlying per-instant-not-per-date regression stays pinned at
`tests/unit/test_partition_window_filter.py::TestNeighbourOwns::
test_lower_bound_ownership_is_resolved_per_instant_not_per_date`.

### D-7(e) — all-or-nothing for the partition being filtered (deliberate asymmetry with D-3b)

For the partition being **filtered** (not the neighbour), every raw body (`raw_*`, excluding
`*.meta.json`) must have a validated sidecar, or filtering is disabled for the **entire**
partition — counted (`last_partition_filter_unresolved_count`), logged `WARNING`, never "filter
what we can". This is the opposite scope rule from D-3b's neighbour proof, and deliberately so:
every raw body in the partition being filtered contributes rows to the output frame regardless of
sidecar presence, so an orphan body there means the true window bound cannot be known at all — a
partial filter would silently mis-trim. The neighbour only needs an existence proof for one
covering chunk, so an unrelated orphan elsewhere in that partition is irrelevant. This rule
applies identically to BOTH interval-semantics paths — resolving the CURRENT partition's own
window is unaffected by which application path consumes it afterward.

### D-5 — never empty a non-empty frame (CLOSED path only; amended 2026-08 by the TRIM ruling)

If applying the trim would drop every row (a corrupted or mis-declared window), the CLOSED-interval
filter (`filter_frame_to_window`, Elexon) is refused entirely — the frame is returned unchanged,
logged `ERROR`. Silent unfiltered fallback is preferred to silent data loss.

**This refusal does NOT extend to the HALF_OPEN path** (`exclude_out_of_window`, ENTSO-E) since the
2026-07-26 TRIM ruling (`partition_window.py:604-626`): a 100%-out-of-window drop is PERFORMED, not
refused — every row in the frame genuinely was never part of that partition's own request, so there
is no ownership to preserve by retaining it. The drop is signalled via `all_dropped=True` and logged
`ERROR` (the same loud, never-silent treatment as the CLOSED refusal, achieved by a different
mechanism: proceeding with the drop rather than reverting it).

### D-6 — Elexon source-scoped; ENTSO-E opt-in per transformer

Elexon's filter is a property of the connector's write layout (`PUBLISH_DATETIME` chunks are
uniformly closed-interval), so it is gated by a source-scoped constant
(`_PUBLICATION_WINDOW_FILTER_SOURCES`) — every current and future in-scope Elexon dataset is
covered automatically. ENTSO-E's filter is gated **per transformer**
(`BaseSilverTransformer.EVENT_WINDOW_FILTER: ClassVar[bool]`), because — unlike Elexon — not every
ENTSO-E dataset shares the same window semantics (horizon forecasts, snapshot queries, and
revision streams do not have a single delivery-day window to trim to at all). This scoping
question (which datasets opt in) is orthogonal to D-3's question (which application path a
dataset's own interval semantics selects, once opted in).

### D-8 — entity-key golden map (F-16, R2-A Task 3, unaffected by the amendment)

`ENTITY_KEY_COLUMNS`/`OPTIONAL_ENTITY_KEY_COLUMNS` are declared verbatim from each transformer's
own `unique(subset=...)` dedup key (31 of 33 registered Elexon datasets), with `system_prices` and
`remit` sourced from `LATEST_VIEW_SPECS` (`latest_views.py:94-107`) since neither has an
in-transform `unique()` call. `cli.py`'s duplicate-quality-check now resolves the dataset's actual
grain via these class attributes instead of a hardcoded `(settlement_date, settlement_period)`
pair, closing F-16: the check reports `fuelhh`'s 580 real duplicated keys and none of the 27,959
`fuel_type` false positives. Order is explicitly NOT part of the contract (`unique(subset=...)` is
set semantics) — `tests/fixtures/entity_keys_golden.json` and every consumer compare declared keys
as sets.

### D-9 — F-10 closure is scoped, not repo-wide (amended 2026-08 by B4/N-9)

**This ADR originally closed F-10 for a narrow, probe-proven ENTSO-E opt-in set only**:
`day_ahead_prices` (the probe-proven case), `actual_load`, `load_forecast`, `actual_generation`,
`actual_generation_units`, `wind_solar_forecast`, `generation_forecast`. 13 further datasets were
named-exempt with a recorded reason (the A31/A32/A33 horizon forecasts, `forecast_margin`,
`installed_capacity*`, `water_reservoirs`, all `outages_*`, and `generation_units_master_data`,
which has no `periodStart`/`periodEnd` at all). The remaining 28 registered ENTSO-E datasets
carried a literal `"TODO: unclassified"` reason (`silver/entsoe/_event_window.py`) pending N-9
research (CLAUDE.md: "Do not invent rate limits, endpoints, or schemas... write a TODO and stop").

**B4 (2026-08, milestone v0.18) landed the N-9 research verdict** (`R3-RESEARCH.md` Sec 1.1): 19
of the 28 TODO datasets are evidence-classified FILTER_SAFE and now opted in (26 opted-in total),
4 are evidence-classified EXEMPT with a cited reason, and 5 remain genuinely `TODO`-marked
UNKNOWN (never observed populated-and-window-compared, or unwired — N-22). The full dataset ->
verdict -> citation -> transformer-family map lives in
`silver/entsoe/_event_window.py::EVENT_WINDOW_CLASSIFICATION`, the audit surface F-10's closure
and the N-9 gate check against. **F-10 and the N-9 gate remain OPEN**: they close only when the
5 UNKNOWN entries are resolved by a future pass, or their residual is accepted at a milestone
close (a Bobbo decision, not this ADR's to make).

## Residuals (accepted, not pursued here)

- **Vendor-content residual (D-3c, CLOSED-interval / Elexon only).** The proof establishes
  *durability*, not *vendor content*: if the vendor genuinely omitted a record from the
  neighbour's window, the trim still proceeds and the row is lost from silver. Row-level ownership
  (reading neighbour raw bodies) would close this but is a T3 change, not taken. Detection
  surface: the R2-exit rebuild's count-gated sweep (predicted post-rebuild row/duplicate counts
  must match exactly); repair is ingest-layer only — an unchanged re-transform cannot restore a
  record never durably written. Not applicable to the HALF_OPEN (ENTSO-E) path, which has no
  vendor-content ambiguity to begin with — an out-of-window row is excluded regardless of whether
  the vendor's content was "correct".
- **R-13 — the durability proof trusts the entire pagination interpretation, not merely a
  vendor-declared `total_pages`.** `connectors/elexon/parsers.py`'s `get_pagination_info` silently
  defaults missing or unexpected pagination metadata to `(1, 1)` — a non-dict body, an absent
  `meta`/`metadata` block, or absent `totalPages`/`lastPage` keys all yield "page 1 of 1". What
  `covering_chunk_is_durable` therefore trusts is the whole chain: the parser's defaults, the page
  identity written into the sidecar, and the connector's correct propagation of both. A genuinely
  multi-page response whose metadata the parser could not read presents as a complete single-page
  chunk and the proof passes. **Accepted as a connector-integrity residual requiring no extra
  control here**: it is a bronze-capture defect that would corrupt the raw layer independently of
  this filter, and adding a second interpretation of the same bytes in silver would duplicate the
  trust rather than remove it. Bounded by `get_pagination_info` being the single shared parser and
  by the "pages present must equal `1..max`" rule. CLOSED-interval (Elexon) only; ENTSO-E's
  HALF_OPEN path makes no pagination-durability claim at all.
- **Memoization strategy (R-10).** The lower-bound ownership machinery's actually-implemented
  memoization is per-distinct-instant (a plain dict), not "per covering-chunk identity" as an
  earlier revision of this design specified — the primitive doesn't expose chunk-window bounds
  without full recomputation, so a strict chunk-level cache isn't buildable as a black-box
  consumer. Flagged as a performance note, not a correctness gap, when this design was reviewed;
  now moot on the ENTSO-E path (no ownership resolution happens there at all) and retained only as
  a note for whichever future CLOSED-interval path exercises the lower bound.

## Consequences

- Two adjacent Elexon publication partitions never write the same vendor record twice, without
  ever deleting SP1 of a settlement day. At Elexon's own trailing boundary (no successor exists),
  the unproven row is **retained** — duplication preferred to loss, unchanged by this ADR's
  amendment.
- ENTSO-E's day-ahead-prices (and 6 siblings) silver output is **trimmed unconditionally** to each
  partition's own recorded `[periodStart, periodEnd)` request window, on both bounds, regardless
  of whether any neighbour bronze partition exists. An out-of-window row is never written to
  silver under any circumstances — including at the very edges of a fetch batch, where no adjacent
  bronze was ever requested. Bronze remains the immutable source of truth for that row's data under
  its own correctly-scoped partition.
- The duplicate-quality-check is keyed on each dataset's real grain.
- No data operations were required to land this design; the R2-exit rebuild wipes and re-derives
  affected silver from immutable bronze. Per the plan's A-15b, once ENTSO-E bronze exists on disk,
  the R2-exit sweep additionally asserts every opted-in dataset's silver rows satisfy their own
  recorded window — zero out-of-window rows, by construction of this design.
