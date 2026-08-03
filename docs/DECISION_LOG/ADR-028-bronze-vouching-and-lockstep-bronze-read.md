# ADR-028 - Bronze vouching and lockstep bronze reads

**Status:** Proposed
**Date:** 2026-08-03
**Phase:** R2-g (Partition integrity), `.planning/phases/R2-partition-integrity/R2-g-PLAN.md`
**Findings closed:** F-05 (ENTSO-G covering-partition read + fabricated reingest vintage) —
scoped, see "What F-05 closure does and does not cover" below
**Residuals filed:** N-15 (the same read/vintage split pre-exists in GIE AGSI — out of scope),
N-16 (`written_at` is captured before the body write completes), N-17 (reference-dataset
`event_time` may be a run-date artefact), N-18 (a non-object sidecar crashes the transform)
**Cross-references:** ADR-025 (temporal vintage / `available_at`; this ADR generalises its
per-file no-`now()`-fallback contract), ADR-027 (single-snapshot threading; this ADR applies
the same mechanism to the filesystem), ADR-026 (owns `_EXACT_PARTITION_ONLY_SOURCES`, which
`entsog` joins here), `silver/base.py::{_resolve_vouched_bronze_set,run}`,
`pipeline/runner.py::run_transform`

## Context

`bronze/writer.py` writes a response as TWO durable artefacts: the body first, then the
`raw_*.meta.json` sidecar carrying its timestamps. Each is written temp-then-`os.replace`, so
neither is ever torn — but a crash BETWEEN them leaves a durable body with no sidecar. Call
that body **unvouched**: nothing on disk can say when it became available.

Three questions had no recorded answer, and each had an intuitive wrong answer that had
already been implemented once.

**1. What should a transformer do with an unvouched body?** The intuitive answer — read it and
stamp `available_at = datetime.now(UTC)` — was the first implementation. It makes a frame's
vintage depend on *when the re-transform happened to run*.

**2. Which files should `available_at` be derived from?** ENTSO-G resolved its read set and its
vintage through two independently-coded filesystem walks (`_bronze_files` /
`_bronze_path_for_date` versus `_bronze_date_dirs` plus a second sidecar glob). Two walks with
different selection rules can and did disagree. Four successive review passes each found a
distinct instance: the wrong partition selected (a vintage measured 26 days off), a sibling
file's stamp borrowed, a mixed partition stamped from only the vouchable subset while the rest
of its rows were still read, and a TOCTOU window between the two scans. Every one is the same
defect wearing a different costume.

**3. What `available_at` should a frame carry when its bronze files have DIFFERENT stamps?**
This is the question that dissolved the first two. Given file A (sidecar 09:00) and file B
(10:00) both contributing rows, `max` stamps A's rows an hour late — hiding them from a
point-in-time query at 09:30 — and `min` stamps B's rows an hour early, leaking them to an
`as_of` before they existed (lookahead bias, the cardinal sin for a backtesting consumer).
Three attempts each fixed *which files the scalar ranged over*, and each relocated the finding
rather than closing it.

## Decision

### D-1 — Exclude until vouched

A bronze body whose OWN sidecar does not yield a parseable tz-aware UTC timestamp is **left out
of the frame**: not read, not stamped, and — equally binding — **not deleted, not repaired, and
never written to**. It is counted, with its path and its reason, and reported.

The `unvouched => now()` alternative is **rejected**, on idempotence:

| Policy | Bronze state B, transform at t1 | Same bronze state B, transform at t2 | Idempotent? |
|---|---|---|---|
| `unvouched => now()` | `available_at = t1` | `available_at = t2` | **NO** — identical bronze, different vintage |
| **exclude until vouched** | each row's fallback = its own vouched file's stamp in B | the same per-row stamps | **YES** |

The invariant is stated over **bronze state**, never over the wall clock. The converse is not a
violation: if a sidecar lands between t1 and t2 the bronze state changed, so a different result
is correct.

**Scope it precisely.** This governs the **ingest-stamp fallback arm** of ADR-025 §3's row-wise
`available_at = coalesce(published_at, ingest_stamp)`, on a `reingest=True` run. A non-null
vendor `published_at` still wins that coalesce, and a live run still stamps `datetime.now(UTC)`.
Neither is changed here, and neither is what this ADR is about.

### D-2 — The lockstep invariant: one scan, one value, both derivations

For a transformer opting into `LOCKSTEP_BRONZE_READ`, per `run()` call:

- `_bronze_candidates(target_date)` performs **exactly one** filesystem scan;
- `_resolve_vouched_bronze_set` reads **exactly one** sidecar per EXAMINED candidate and
  performs **no globbing at all**;
- the resulting `VouchedBronzeSet` is the **single value threaded** into both the frame read and
  the vintage derivation.

There is no second scan for the two to disagree about. This is ADR-027's mechanism — *"exactly
one `read_watermark` call per resolution feeds both the window derivation and the gap detection,
so the two can never disagree"* — applied to the filesystem instead of the catalogue. It closes
all four historical failure modes structurally rather than case by case, including TOCTOU.

Per ADR-027 D-3, the closure is **structural, not topological**: "only one process runs the
pipeline" is explicitly not accepted as a defence in this repo.

The honest residual: one scan makes the two derivations *consistent*, not *atomic with the
filesystem*. A file landing after the scan is absent from BOTH the frame and the stamps and is
picked up next run — the same bounded re-do ADR-027 accepted for `CAS_MISMATCH`.

A body is in `entries` (with its own stamp) or in `unvouched` (excluded from both). **There is
no third state in which a file is read but unstamped**, which is what makes the invariant hold
mechanically rather than by argument.

### D-3 — The vintage is carried PER ROW, not chosen as a frame-level scalar

No frame-level `available_at` can satisfy the invariant once two files with different stamps
both contribute rows (see Context question 3). The scalar itself is the defect, so it is removed
rather than re-selected.

`run()` builds the merged raw frame exactly as before and, **in the same flattening step**,
attaches a transient base-owned column `gf_bronze_vintage` holding each raw row's own bronze
file's sidecar stamp — built from the same per-file `(path, records)` structure that builds the
rows, so rows and stamps cannot desync. `_add_bitemporal_columns` consumes it as the ingest-time
source of an expression that was **already row-wise** (ADR-025 §3), and `_process_frame` drops
it before the write behind a fail-loud guard. **No new column reaches silver.**

This is a net simplification: a row filtered out, dropped by `transform()`, or lost to dedup
takes its stamp with it, and a file that yields no surviving row leaves no stamp anywhere in the
frame. Nothing is reconstructed, so nothing can be reconstructed wrongly.

Two guards are load-bearing:

- **the collision guard tests NORMALISED names, not the literal string.** ENTSO-G's
  `_normalise_column_names` maps `gfBronzeVintage`, `gf-bronze-vintage` and `GF_Bronze_Vintage`
  all onto the reserved name and coalesces them, so a vendor field with a datetime-castable
  value would silently replace the true bronze stamp — the exact corruption this design exists
  to prevent, arriving through the carrier instead of the aggregate. "Vendor fields are
  camelCase" is not a defence; camelCase is precisely the colliding spelling.
- **the generic family's all-columns dedup subset excludes the carrier.** Its failure mode is
  silent: the column is per-file-varying, so leaving it in makes two identical records from two
  files stop comparing equal and lets cross-file duplicates survive to disk.

`VINTAGE_PER_BRONZE_FILE` was evaluated first and does not fit: it issues one `_write_silver`
call per bronze FILE, which only coexists under `APPEND_ONLY`. Neither ENTSO-G family is
`APPEND_ONLY`, so the last file would silently overwrite the rest; it also collapses dedup to
intra-file only, and the reference-dataset write path ignores `available_at` entirely. Recorded
so it is not re-proposed.

### D-4 — Selection operates on the VOUCHED list, never before it

`BronzeReadSelection.NEWEST_VOUCHED` walks candidates in order and stops at the **first vouched**
one. Truncating to "newest" before vouching would let a single orphan newest body empty the frame
for a dataset with dozens of good older files.

This is not stamp-borrowing: the selected file's rows are read AND its own stamp is used; the
stepped-over bodies are counted and named. Candidates BEHIND the selected file are not probed at
all, so the reported count means something precise — *"N newer bodies were stepped over"*, not
*"N bodies exist somewhere in the tree"*.

### D-5 — Three-valued run outcome: how a permanently-unvouched file stays visible

Excluding rows is only acceptable if the exclusion is a real gate rather than a log line nobody
reads. Evaluated per `(source, dataset)` in `run_transform`, ranked so the existing precedence is
untouched:

| Rung | Condition | Outcome |
|---|---|---|
| 1 (existing) | `total_all_dropped` | `failed` — wins the status and message, but must NOT swallow the unvouched count |
| **2 (new)** | at least one date where candidates were examined and NONE vouched | `failed`, `tracker.fail`, ERROR, non-zero exit |
| **3 (new)** | any unvouched files at all | `completed_with_warnings` + `bronze_unvouched=N` + ONE WARNING |
| 4 (existing) | unmapped / validation failures | unchanged |
| 5 | otherwise | `success` |

**Why rung 2 is a hard fail.** Zero rows from bronze that demonstrably EXISTS. A stale
pre-existing Parquet is left on disk untouched, and only a failed dataset-level status stops a
scheduler treating it as current — the argument rung 1 already makes in `runner.py`.

**Why rung 3 is not.** Partial exclusion is the realistic crash-residue shape, and its content
has very likely re-landed via the 72h `incremental_overlap_hours` re-fetch. (We cannot *prove*
that without reading the orphan, which is what we refuse to do — hence still a counted,
non-`success` outcome.) Hard-failing forever on one permanently orphaned body would manufacture
exactly the desensitisation the ruling exists to avoid.

**No age threshold, deliberately.** Any clock-dependent severity would make a transform that
passed yesterday fail today on identical bronze — reintroducing the wall-clock dependence D-1
rejects, one layer up in the signalling stack.

**The visibility chain, stated so it can be audited:**

1. **It never self-clears.** Bronze is never deleted or edited, so the orphan persists; every
   subsequent transform re-examines it, re-counts it, and re-emits the record.
2. **It is structured, not merely logged.** `DatasetResult.bronze_unvouched` is a first-class
   field; `status` is never `success` while it is nonzero; the CLI prints it; and
   `PipelineRunTracker` (`observability.py:63`, insert at `:86`, `complete` at `:112`, `fail` at
   `:185`) records the non-success outcome in `pipeline_runs`, which `gridflow status`
   (`cli.py:575-597`) reads. A scheduler keys off the status, not the log stream.
3. **Total exclusion escalates to a non-zero exit code**, every run, until resolved.
4. **There is NO sanctioned repair path**, and that gap is named rather than invented — writing
   the missing sidecar is a bronze write, which the immutability ruling forbids and `guard.py`
   hard-halts. It needs its own decision and its own unit.

### D-6 — Bounded emission, exact counts

One aggregated record per `(source, dataset)` per `run_transform` invocation, whatever the file
count or the date-range length. Nothing above `DEBUG` is emitted by the classifier, the resolver,
or `run()`.

Counts are accumulated as a **union of `(path, reason)` pairs**, never as running integers and
never as a path set beside a detached reason `Counter`. Both alternatives misreport: the ENTSO-G
reference family rescans the whole tree on every target date, so one orphan over a 30-day range
would be counted 30 times — a 30x overstatement in the very record whose job is to size the
remediation; and once paths are deduplicated across dates a detached counter cannot attribute a
newly-seen path to its reason. Only the EXAMPLE paths are capped (at 5); the distinct-file count
and the per-reason totals are exact.

`rows_skipped` keeps its ROW meaning. Excluded FILES are never summed into it — their row count
is unknown precisely because we refuse to read them, so the sum would be a category error against
an existing contract.

### D-7 — `entsog` joins `_EXACT_PARTITION_ONLY_SOURCES`, and only after D-2 lands

See the amendment to ADR-026. The ordering is load-bearing rather than stylistic: the same
frozenset gates the vintage helper `_bronze_date_dirs`, so flipping it while the vintage path
still ran through that helper makes the walk return `[]` for any date without an exact partition
and fall through to `datetime.now(UTC)` — a fabricated vintage, measured 26 days off on the
earlier attempt. D-2's lockstep branch removes both ENTSO-G families from that method's caller
set entirely, so by the time the flip lands the trigger has no path to fire down. A spy pins the
non-call mechanically, because otherwise this is a claim that can rot.

### D-8 — Rolled out narrowly, though the rule is universal

`LOCKSTEP_BRONZE_READ` is a per-transformer opt-in, taken by exactly the two ENTSO-G families and
pinned in a genuinely fresh process (an in-process registry assertion passes on ambient pytest
imports and can ship as a no-op).

The honest reason for the narrowness: vouching is a property of the **bronze writer's**
body-then-sidecar ordering, which is universal across sources — so the control is universally
correct, not ENTSO-G-specific. It is rolled out narrowly because every other source has bronze on
disk, and changing their read sets is a data-semantics change on live data. `bronze/entsog` and
`silver/entsog` were both verified ABSENT under `C:\gridflow-data` on 2026-08-02, which is what
holds this unit's blast radius at zero.

Generalising the rollout is **N-15**, which must audit every `_bronze_files` override rather than
point-fix known cases. It should cite this ADR rather than re-derive the mechanism.

## What F-05 closure does and does not cover

| Surface | Status |
|---|---|
| `PhysicalFlowsTransformer` READ (the 35-day covering fallback) | **CLOSED** by D-7 |
| Both ENTSO-G families' VINTAGE path (`_bronze_date_dirs` -> `now()`) | **CLOSED** by D-2 |
| Generic family, non-reference branch | Already exact-dir-only; unchanged |
| Generic family, REFERENCE branch (whole-tree scan -> possible `target_date` `event_time`) | **NOT CLOSED — N-17.** Pre-existing, unchanged here |

**N-17 in one line:** a reference dataset selects the newest body in the whole tree regardless of
date, and if that payload carries no recognised timestamp column, `_event_time_expr` falls back to
`target_date` midnight UTC — so rows from an arbitrarily old body are labelled with the date the
transform ran for. Whether any current ENTSO-G reference payload actually lacks such a column is
**UNKNOWN**: it comes from the vendor response and there is no ENTSO-G bronze on this machine to
inspect. Its fix is an event-time contract for date-agnostic reference data, over an expression
shared by every source. **F-05 must not be recorded as covering it.**

## Consequences

- Under `--reingest`, an ENTSO-G partition's `available_at` may now vary row to row. That is the
  point: each row resolves to the recorded stamp of the file it actually came from. A live run
  still produces a constant stamp, so the per-row shape appears only in the mode whose
  reproducibility the ruling is about (which is the mode the R2 exit rebuild uses).
- Rung 2 is a new way for a run to fail that previously "succeeded" with zero rows. A genuinely
  concurrent ingest+transform can trip it transiently on the first file of a new date; it
  self-heals next run and loses nothing. Fail-closed on an ambiguous state is the house rule, and
  a quiet zero-row success over existing bronze is the silent-loss class v0.18 exists to remove.
- A permanently-unvouched body means permanently-missing rows with no in-scope remedy. **Accepted,
  and it is the point of the ruling** — bounded by D-5's gate (never silent) and by bronze
  retention, so the rows remain recoverable if a repair is ever sanctioned.
- `_timestamp_from_sidecar`'s observable behaviour is deliberately UNCHANGED, including its
  uncaught `AttributeError` on syntactically valid non-object JSON (**N-18**). Only the new
  lockstep-only classifier is hardened. Softening the wrapper would let its non-ENTSO-G callers
  stamp a frame from a sibling's timestamp, fall through to `now()`, or silently skip rows — for
  every source. That is a data-semantics change on live datasets, i.e. a different unit.
- **N-16 is unchanged and bounds this ADR's own claims.** `bronze/writer.py` captures `written_at`
  BEFORE the body write it is documented as marking the completion of, so a recorded stamp is
  always `<=` the true durable-write instant (never later), by the duration of the write. Every
  availability claim here is therefore stated over the **recorded** stamp, which is the strongest
  true statement available. Moving that line would silently change the meaning of `written_at` for
  future sidecars while existing ones retain the old meaning — a mixed-vintage corpus with no
  field to distinguish the two, which needs its own decision.
