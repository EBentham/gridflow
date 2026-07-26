# ADR-026 - Partition-window filters (publication-window + event-window)

**Status:** Proposed
**Date:** 2026-07-26
**Phase:** R2-A (Partition integrity), `.planning/phases/R2-partition-integrity/R2-A-PLAN.md`
**Findings closed:** F-04 (Elexon boundary duplication), F-16 (duplicate-check key), F-10
(ENTSO-E vendor over-span) — scoped, see "Scope" below
**Cross-references:** ADR-025 (temporal vintage / `available_at`, incl. its §3 residual for
`remit`/`fou2t14d`), `_EXACT_PARTITION_ONLY_SOURCES` (`silver/base.py`)

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

### D-1/D-2 — two partition semantics, one primitive

`silver/partition_window.py` is a single, source-agnostic primitive
(`request_window_from_sidecar`, `partition_request_window`, `covering_chunk_is_durable`,
`neighbour_owns`, `filter_frame_to_window`) applied to two different sources/dimensions:

| source | partition means | filter dimension | window params |
|---|---|---|---|
| `elexon` (23 of 24 in-scope datasets) | a **publication** window | `published_at` | `publishDateTimeFrom`/`publishDateTimeTo` |
| `elexon/remit` (D-1b) | same, but `remit` derives `timestamp_utc` deterministically from `published_at` and drops the source column at select | `timestamp_utc` | same |
| `entsoe` (7 opted-in datasets) | a **UTC delivery day**, clamped at overall-range edges | `timestamp_utc` | `periodStart`/`periodEnd` |

For ENTSO-E, `[periodStart, periodEnd)` is DST-invariant because CET/CEST never enters the
computation (D-2) — the window is a UTC instant range, not a calendar-local one.

### D-3/D-3d — asymmetric bound enforcement, both proven by durability

**Elexon enforces its UPPER bound only.** Rows below `publishDateTimeFrom` are counted and logged
`WARNING` but always kept — Elexon's own boundary loss (the *trailing* partition, no successor)
is the only case D-3b's gate needs to cover.

**ENTSO-E enforces BOTH bounds**, because a trimmed row is re-homed in an *adjacent* UTC-day
partition on either side, not just the one following it. The lower bound is resolved **per
distinct below-window instant, never per calendar date** (D-3d): `day_subwindows`
(`utils/time.py:130-131`, `sub_start = max(start, day_start)`, `sub_end = min(end, day_start + 1
day)`) **clamps** sub-windows at the overall fetch's range edges, so a predecessor partition can
legitimately hold only part of a UTC day (e.g. `[D T06:00, D+1 00:00)`). Two below-bound
timestamps on the *same* UTC date can therefore have different ownership outcomes, and collapsing
to a `frozenset[date]` (admit the whole date once one representative instant is proven) would
delete the unproven one. This was Sol pass 4's blocker (S4-1) against an earlier `frozenset[date]`
revision; `test_entsoe_mixed_ownership_within_one_utc_date_trims_only_the_proven_row` pins the
regression, demonstrated RED against both an unconditional trim and the per-date collapse.

Both bounds carry the failing `WindowReason` when unenforced (D-3e — `OwnershipVerdict(owned,
reason)`), threaded into `WindowFilterResult.retained_reasons` and logged, so an
`INCOMPLETE_PAGE_SET` (a torn ingest) is diagnosable from the logs rather than collapsing into an
undifferentiated count.

### D-3b/D-3c — the neighbour-durability proof (why a completed fetch is not enough)

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

### D-7(e) — all-or-nothing for the partition being filtered (deliberate asymmetry with D-3b)

For the partition being **filtered** (not the neighbour), every raw body (`raw_*`, excluding
`*.meta.json`) must have a validated sidecar, or filtering is disabled for the **entire**
partition — counted (`last_partition_filter_unresolved_count`), logged `WARNING`, never "filter
what we can". This is the opposite scope rule from D-3b's neighbour proof, and deliberately so:
every raw body in the partition being filtered contributes rows to the output frame regardless of
sidecar presence, so an orphan body there means the true window bound cannot be known at all — a
partial filter would silently mis-trim. The neighbour only needs an existence proof for one
covering chunk, so an unrelated orphan elsewhere in that partition is irrelevant.

### D-5 — never empty a non-empty frame

If applying the trim would drop every row (a corrupted or mis-declared window), the filter is
refused entirely — the frame is returned unchanged, logged `ERROR`. Silent unfiltered fallback is
preferred to silent data loss.

### D-6 — Elexon source-scoped; ENTSO-E opt-in per transformer

Elexon's filter is a property of the connector's write layout (`PUBLISH_DATETIME` chunks are
uniformly closed-interval), so it is gated by a source-scoped constant
(`_PUBLICATION_WINDOW_FILTER_SOURCES`) — every current and future in-scope Elexon dataset is
covered automatically. ENTSO-E's filter is gated **per transformer**
(`BaseSilverTransformer.EVENT_WINDOW_FILTER: ClassVar[bool]`), because — unlike Elexon — not every
ENTSO-E dataset shares the same window semantics (horizon forecasts, snapshot queries, and
revision streams do not have a single delivery-day window to trim to at all).

### D-8 — entity-key golden map (F-16, R2-A Task 3, unchanged by this revision)

`ENTITY_KEY_COLUMNS`/`OPTIONAL_ENTITY_KEY_COLUMNS` are declared verbatim from each transformer's
own `unique(subset=...)` dedup key (31 of 33 registered Elexon datasets), with `system_prices` and
`remit` sourced from `LATEST_VIEW_SPECS` (`latest_views.py:94-107`) since neither has an
in-transform `unique()` call. `cli.py`'s duplicate-quality-check now resolves the dataset's actual
grain via these class attributes instead of a hardcoded `(settlement_date, settlement_period)`
pair, closing F-16: the check reports `fuelhh`'s 580 real duplicated keys and none of the 27,959
`fuel_type` false positives. Order is explicitly NOT part of the contract (`unique(subset=...)` is
set semantics) — `tests/fixtures/entity_keys_golden.json` and every consumer compare declared keys
as sets.

### D-9 — F-10 closure is scoped, not repo-wide

**This ADR closes F-10 for exactly 7 opted-in ENTSO-E datasets**: `day_ahead_prices` (the
probe-proven case), `actual_load`, `load_forecast`, `actual_generation`,
`actual_generation_units`, `wind_solar_forecast`, `generation_forecast`. 13 further datasets are
named-exempt with a recorded reason (the A31/A32/A33 horizon forecasts, `forecast_margin`,
`installed_capacity*`, `water_reservoirs`, all `outages_*`, and `generation_units_master_data`,
which has no `periodStart`/`periodEnd` at all). The remaining ~28 registered ENTSO-E datasets are
`EVENT_WINDOW_FILTER_EXEMPT` with a literal `"TODO: unclassified"` reason
(`silver/entsoe/_event_window.py`) — this plan does not infer their window semantics
(CLAUDE.md: "Do not invent rate limits, endpoints, or schemas... write a TODO and stop"). Tracked
as **N-9, a v0.18 milestone gate**; `cross_border_flows` is named as the leading candidate for the
next classification pass.

## Residuals (accepted, not pursued here)

- **Vendor-content residual (D-3c).** The proof establishes *durability*, not *vendor content*:
  if the vendor genuinely omitted a record from the neighbour's window, the trim still proceeds
  and the row is lost from silver. Row-level ownership (reading neighbour raw bodies) would close
  this but is a T3 change, not taken. Detection surface: the R2-exit rebuild's count-gated sweep
  (predicted post-rebuild row/duplicate counts must match exactly); repair is ingest-layer only —
  an unchanged re-transform cannot restore a record never durably written.
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
  by the "pages present must equal `1..max`" rule.

## Consequences

- Two adjacent Elexon publication partitions never write the same vendor record twice, without
  ever deleting SP1 of a settlement day.
- ENTSO-E's day-ahead-prices (and 6 siblings) silver output is trimmed to the requested UTC day
  for interior days of a multi-day fetch (both neighbours present and durable); at a fetch's own
  edges (no predecessor/successor bronze exists because it was genuinely never fetched), the
  over-spanning rows are **retained** rather than dropped — this is a deliberate consequence of
  the anti-loss guarantee (D-3b/D-3d), not an oversight; see the "Known limitation" note below.
- The duplicate-quality-check is keyed on each dataset's real grain.
- No data operations were required to land this design; the R2-exit rebuild wipes and re-derives
  affected silver from immutable bronze.

## Known limitation — batch-edge partitions retain their vendor over-span

Because D-3b/D-3d require a **proven durable neighbour** before trimming, a UTC-day partition at
the very edge of a fetch batch (no bronze exists yet for the adjacent day, because it was never
requested) cannot have its over-spanning rows trimmed without violating the anti-loss guarantee —
those rows would be permanently lost, since no other partition durably carries them. This is the
expected, reviewed behaviour of the durability gate, not a defect in it. It does mean that a
freshly-ingested date range's first and last silver partitions can carry more rows than their own
UTC day until the adjacent day is also ingested (at which point a re-transform trims them
correctly, once the neighbour becomes durable). Downstream consumers reading a single in-progress
ingest batch should be aware of this edge effect; it self-resolves once ingestion continues past
the batch boundary and the date is re-transformed.
