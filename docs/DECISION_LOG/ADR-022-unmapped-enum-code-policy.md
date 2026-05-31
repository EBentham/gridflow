# ADR-022 — Unmapped-enum-code policy for ENTSO-E silver transformers + CLI partial-vs-failed status

Status: Accepted

## Context

The v1.5 pre-close review (verdict **FOLLOW_UP**) confirmed a High-severity,
reproducible data-loss defect (H2) and traced it to a pattern shared across the
ENTSO-E silver layer.

### The mapping-without-default pattern

`silver/entsoe/imbalance_volume.py:64-66` maps the raw `flow_direction` code to a
`direction` label with `replace_strict` and **no `default=` argument**:

```python
pl.col("flow_direction")
.replace_strict({"A01": "long", "A02": "short"})
.alias("direction")
```

Polars `replace_strict` raises `InvalidOperationError` on any value absent from
the mapping (reproduced on polars 1.38.1 for both `"A03"` and `""`). The trigger
is code-certain, not hypothetical:

- The parser initialises `flow_direction = ""`
  (`connectors/entsoe/parsers.py:183`) and only overwrites it when a
  `flowDirection.direction` element is present (`:226-229`). An **empty string is
  the common path** for documents without that element.
- `A03` ("up and down") is a **legitimate** ENTSO-E balancing code that the
  current two-key map does not cover.

So the unmapped value is frequently a real, expected input — not garbage.

### This is a cross-transformer pattern, not one call site

A grep of `replace_strict` across `src/gridflow/silver/` finds **eight unguarded
call sites across six ENTSO-E transformers**, every one without a `default=`
(the two `activated_balancing_*` transformers each carry two sites — lines 74 and
77 — so six transformers yield eight sites):

| Transformer | Line | Column → mapping |
|---|---|---|
| `entsoe/imbalance_volume.py` | 65 | `flow_direction` {A01,A02} |
| `entsoe/imbalance_prices.py` | 65 | `business_type` {A19,A20} |
| `entsoe/contracted_reserves.py` | 65 | `business_type` {A95,A96,A97,A98} |
| `entsoe/activated_balancing_prices.py` | 74, 77 | `business_type`, `flow_direction` |
| `entsoe/activated_balancing_qty.py` | 74, 77 | `business_type`, `flow_direction` |
| `entsoe/outages_generation.py` | 71 | `business_type` {A53,A54} |

The safe form already exists in the same codebase:
`silver/elexon/system_prices.py:159` calls
`replace_strict(self.RUN_PRECEDENCE, default=0)`. The pattern is known; it is
simply not applied consistently to the ENTSO-E enum maps. Each of these is a
registered, model-relevant dataset that runs under `transform entsoe --all`.

### How one bad code zeroes (and perpetually re-fails) a date

The `InvalidOperationError` propagates out of `transform()` and is caught by the
CLI's blanket handler. The **live trigger is the `transform` command**
(`cli.py:147-161`); the `ingest` command (`cli.py:74-87`) shares the identical
`except Exception → tracker.fail(...) → raise typer.Exit(1)` shape but is not
where `replace_strict` runs.

In `transform`, the per-date loop is **inside** the `try` (`cli.py:133-153`):

```python
for ds in datasets:
    tracker = PipelineRunTracker(con, source, ds, "transform")
    try:
        transformer = get_transformer(...)
        for target_date in dates:                 # ← inside the try
            rows = transformer.run(target_date, ...)
            total_rows += rows
        tracker.complete(rows_out=total_rows)
    except Exception as e:
        tracker.fail(_safe_error_message(str(e)))  # whole dataset marked failed
        ...
        continue                                   # next dataset; this one aborts
```

Consequences, all confirmed against current source:

1. **Partial silver + FAILED tracking.** `transformer.run` writes per date, so
   dates *before* the offending date are written to silver, but the dataset's
   `pipeline_runs` row is stamped `status='failed'` for the whole run — the run
   record does not reflect that some dates succeeded.
2. **Remaining dates abandoned.** The raise exits the inner date loop, so every
   date *after* the offending one in the range is skipped for that dataset (the
   `continue` advances to the next dataset).
3. **Re-fails forever under unchanged bronze.** The transform loop never consults
   `pipeline_runs` (or `pipeline_watermarks`, which is ingestion-only) to skip a
   previously-failed date. A rerun reprocesses the full range, re-hits the same
   unmapped code in unchanged bronze, and fails identically. The already-succeeded
   dates re-write as **new APPEND_ONLY revisions** (run-suffixed files), so reruns
   accrete duplicate revisions while never making progress past the bad date.

Because the failure is loud (`status='failed'`, `Exit(1)`) it presents as a
*handled* pipeline failure, but the underlying state is silent partial data-loss
that no rerun can clear without a code change. Happy-path fixtures use only mapped
codes (A01/A02), so CI stays green — see the review's "CI green ≠ covered" note.

### Tracking status

This is issue-05 sub-finding **#2**
(`.planning/issues/code-review-2026-05/05-silver-value-unit-mapping-and-schema.md`,
`status: ready-for-agent` at HEAD). The v1.5 milestone closed only issue-05 **#1**
(the gfm currency consumer); #2 is neither fixed nor recorded in the STATE.md
deferral list. The issue's own acceptance criterion #2 and suggested-approach #2
already frame this as a contract decision (map the full code set, or
`default=`-sentinel + warn, or filter-with-WARNING — but never silently zero the
date, and distinguish "partial/unmapped" from "no data").

### Current run-status model (what a new status extends)

`PipelineRunTracker` (`observability.py`) writes to `pipeline_runs` with columns
`(run_id, source, dataset, operation, started_at, status, completed_at, rows_in,
rows_out, rows_skipped, duration_seconds, error_message)`. Today `status` takes
`'running'` → `'success'` (`complete()`) or `'failed'` (`fail()`). Notably
`complete()` **already accepts a `rows_skipped` count** and the table already has
a `rows_skipped` column — so surfacing dropped/sentinel rows needs no schema
change, and a third terminal status is an additive extension of an existing model.

## Decision

**Accepted and implemented in phase F32** (the v1.5 pre-close blocker fix). This
ADR fixes the *policy*; F32 did the wiring, tests, and issue/STATE bookkeeping.
The shipped form matches the recommendation below: `UNMAPPED_SENTINEL = "unmapped"`
applied at all eight sites and the `'completed_with_warnings'` CLI status.

### 1. Unmapped-code mapping contract — sentinel + logged count (recommended)

Adopt, uniformly across all eight ENTSO-E enum-mapping call sites (six transformers):

```python
pl.col("<code_col>")
.replace_strict(MAPPING, default=UNMAPPED_SENTINEL, return_dtype=pl.Utf8)
.alias("<label>")
```

with `UNMAPPED_SENTINEL = "unmapped"` (a single shared constant), plus a
**`logger.warning` carrying the count and the distinct unmapped raw codes** for
that date, and the count fed to `tracker.complete(rows_skipped=...)` /
the partial status below.

The discriminating reason sentinel beats filter is established in Context:
`flow_direction=""` is the *common* path and `A03` is a *legitimate* code.
Filtering would silently drop common, legitimate rows — trading one silent
data-loss for another. The sentinel **preserves every row**, keeps the value
recoverable, and makes the unmapped volume observable. Mapping the full ENTSO-E
code set is rejected as the primary fix because the documented code universe is
open-ended per document type and would re-break on the next unlisted code; the
sentinel degrades gracefully where an exhaustive map fails closed.

Filter-with-WARNING is retained only as the appropriate choice for a future
column whose unmapped value is genuinely garbage (no legitimate codes outside the
map) — decided per transformer when a map is introduced, not as the default.

### 2. CLI run-status: a partial status distinct from FAILED

Add a third terminal `pipeline_runs.status` value — proposed
**`'completed_with_warnings'`** — set when a run finished and wrote rows but
encountered ≥1 unmapped code (i.e. `rows_skipped > 0` / sentinel rows present).
It is distinct from:

- `'success'` — clean, zero unmapped.
- `'failed'` — exited before completion / wrote nothing usable.

This requires a small `PipelineRunTracker` method (e.g.
`complete_with_warnings(...)` or a flag on `complete`) and a CLI echo that reports
the partial outcome without `Exit(1)`. The transformer no longer raises on an
unmapped code (the sentinel handles it), so the CLI’s `except` is no longer the
path for this class of failure — which is precisely what breaks the re-fail loop
at the source.

### 3. Rerun-skip semantics (scoped, not built here)

The sentinel fix alone ends the perpetual re-fail loop: a sentinel-mapped run
reaches `tracker.complete(...)`, so a subsequent run is no longer forced to
re-hit a raising date. The new partial status additionally lets an operator (or a
future orchestrator) see that a date completed-with-warnings rather than cleanly.

**Out of scope for this ADR:** building per-date checkpoint/skip logic into the
transform loop (consulting `pipeline_runs` to skip already-`success` dates on
rerun). That is a separate orchestration concern; this ADR only removes the
*forced* re-fail and defines the status vocabulary a future checkpointer would
key on. Reruns of an unchanged range still re-write already-succeeded dates as new
APPEND_ONLY revisions; that is tolerated because the gridflow_models read path
deduplicates to the latest revision per key (ADR-017/018), so duplicate revisions
do not double-count downstream.

### Scope boundary

This ADR is **H2 only**. The other v1.5 review findings are governed elsewhere and
are explicitly *not* decided here:

- H1 / M2 / L1 (ENTSO-E day-ahead realised-join contract: column name, GB
  bidding-zone filter, per-row currency) → gridflow_models **ADR-051**.
- M3 / L4 / L5 (connector per-unit surface-vs-swallow-after-retries) → the
  separate gridflow connector-failure-contract ADR (**ADR-023**).

The *semantics* of an `""`/`"unmapped"` direction for `imbalance_volume`
specifically (is a directionless imbalance row meaningful, or should it be
filtered at that one site?) is left to the implementing phase; this ADR fixes the
data-loss policy, not that per-dataset domain question.

## Alternatives considered

**Option A — sentinel default + logged count + partial status (chosen above).**
Preserves rows, observable, uniform, and matches the existing
`system_prices.py:159` `default=` precedent. Cost: silver gains a `"unmapped"`
label value that downstream consumers must tolerate (acceptable — it is explicit
and counted, far better than a vanished date).

**Option B — filter unmapped rows with a WARNING (rejected as the default).**
Drop rows whose code is not in the map, log the dropped count. Rejected as the
*default* because, for these specific columns, the unmapped value is commonly the
empty-string parser default and can be a legitimate code (`A03`) — filtering
silently discards real, expected data. Kept as a per-column option where the
unmapped value is provably garbage.

**Option C — exhaustively map the full ENTSO-E code set, keep `replace_strict`
strict (rejected).** Enumerate every documented `business_type`/`flow_direction`
code. Rejected because the code universe is open-ended and version-dependent; the
strict map fails closed (whole-date loss) on the first unlisted or
vendor-extension code, re-creating H2 under a new code. Curating the maps more
completely is still encouraged, but on top of the sentinel safety net, not instead
of it.

**Option D — broaden the CLI `except` to keep going per date but stay
`FAILED` (rejected).** Catching the error inside the date loop and continuing
would stop the whole-range abort, but without a sentinel the offending date still
writes zero rows and the run is still opaque about which dates were lost. Treats
the symptom (loop abort) not the cause (strict map), and leaves the silent-loss
class intact for the failed dates.

**Option E — do nothing / defer (rejected).** The review classifies H2 as a
blocker that must resolve before close, reproducible under plausible live input,
and currently untracked as a deferral. Leaving it relies on the happy-path
fixtures continuing to dodge the real code space.

## Consequences

- **Eight call sites change across six ENTSO-E transformers** (the two
  `activated_balancing_*` transformers carry two sites each); the
  `system_prices.py` site already complies and is the template. A shared sentinel
  constant (`UNMAPPED_SENTINEL = "unmapped"` in `silver/entsoe/_enum_maps.py`)
  keeps the label uniform.
- **No `pipeline_runs` schema change** for the row count (`rows_skipped` already
  exists); the new `status` value is an additive enum extension. Any dashboard or
  query that filters `status='success'` must be updated to also treat
  `'completed_with_warnings'` as a non-failure (otherwise partial-but-fine runs
  read as "not successful").
- **Silver may now contain a `"unmapped"` label** in `direction` / `reserve_type`
  / `outage_type`. Downstream consumers and any silver schema/contract that
  enumerates allowed label values must permit it. This is the intended trade: an
  explicit, counted sentinel instead of a vanished date.
- **The perpetual re-fail loop is broken at the source** — an unmapped code no
  longer aborts the date; the run completes (with warnings) and writes rows.
- **Per-date rerun-skip is not delivered**, so reruns of an unchanged range still
  accrete APPEND_ONLY revisions for already-succeeded dates; this is absorbed by
  the latest-revision read-path dedup (ADR-017/018) and flagged here so a future
  orchestration phase can add checkpointing keyed on the new status vocabulary.
- **A behavioural test is required** (issue-05 AC #2): feed an unmapped
  `flow_direction` (and at least one unmapped `business_type`) and fail if the
  date silently drops to zero rows, asserting the run is distinguished as
  partial/unmapped rather than `failed`. This closes the happy-path-fixture blind
  spot.
- **Issue/STATE bookkeeping:** on landing, flip issue-05 status appropriately and
  record the outcome in gridflow_models `.planning/STATE.md`; if any portion is
  deferred, record it explicitly as a tracked deferral rather than leaving it
  silent.

## Rollback

Revert the eight call sites to `replace_strict(MAPPING)` (no default) from this
phase's diff, drop the `'completed_with_warnings'` status handling and the shared
sentinel constant, and remove the unmapped-code test. Silver written during the
sentinel period retains any `"unmapped"` labels as historical artefacts (no
back-fill needed; the read-path dedup is unaffected). Reverting reinstates the H2
data-loss behaviour, so rollback is only appropriate if the policy itself is
superseded by a fuller mapping strategy.

## References

- v1.5 pre-close review synthesis (finding H2; F32 ADR topic #2; F32 scope
  sketch):
  `C:/Users/Bobbo/OneDrive/Desktop/Python/gridflow_models/.planning/reviews/2026-05-31-v1.5-pre-close/REVIEW.md`
- Source issue (sub-finding #2; acceptance criterion #2; suggested approach #2):
  `C:/Users/Bobbo/OneDrive/Desktop/Python/gridflow_models/.planning/issues/code-review-2026-05/05-silver-value-unit-mapping-and-schema.md`
- Related: ADR-017 / ADR-018 (APPEND_ONLY run-suffixing + latest-revision read
  selection — the mechanism that absorbs rerun revision accretion).
- Companion follow-up ADRs (not decided here): gridflow_models ADR-051
  (realised-join contract, H1/M2/L1); gridflow ADR-023
  connector-per-unit-failure-contract (M3/L4/L5).
