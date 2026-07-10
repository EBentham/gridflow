# ADR-025 — Temporal vintage & revision capture (one decision for P0.1 / P0.3 / P1.1)

Status: **Accepted** (v0.17 Phase 0; grilled via grill-with-docs + user sign-off, 2026-07-10)

Supersedes the run-collapsing behaviour of `SystemPriceTransformer._resolve_runs`.
Extends [ADR-017](ADR-017-append-only-class-attribute.md) (APPEND_ONLY class
attribute) and [ADR-018](ADR-018-append-only-run-suffixed-files.md) (run-suffixed
files, QUALIFY on read). Feeds gridflow_models' point-in-time barrier and the
P1.5 imbalance-context gold path.

## Context

The 2026-07 full-stack review found the temporal-vintage surface fractured across
three findings that are really **one design decision**:

- **P0.1 / R1-F01 — capture.** `SystemPriceTransformer._resolve_runs`
  (`silver/elexon/system_prices.py:158-168`) groups by
  `(settlement_date, settlement_period)` and keeps `.first()` after sorting on a
  static run-precedence rank. This **collapses every settlement vintage to one row
  per SP** and discards the earlier ones (II → SF → R1 → R2 → R3 → RF). A
  re-transform of an already-settled day cannot reproduce what was knowable at an
  earlier `as_of`: the interim-price history is destroyed at transform time.

- **P0.3 / R1-F02 — read.** The two datasets that already opt into APPEND_ONLY
  (`remit`, `fou2t14d`) coexist as run-suffixed files, but gridflow has **no
  latest-revision read surface**. ADR-018 claimed "the QUALIFY complexity already
  lives in the F3 data layer", but that turned out to be true only in
  *gridflow_models'* consumer layer (`available_at <= as_of`, then latest). On the
  gridflow side, `serving/client.py` has only a bitemporal-*exclude* clause
  (`_present_bitemporal_exclude_clause`), and the quality CLI reads the raw
  multi-file glob — so once system_prices becomes APPEND_ONLY, a naive count sees
  every vintage as a separate row and structural checks false-fail.

- **P1.1 / R1-F07 + R5-F04 — vintage semantics.** `available_at` is currently
  `datetime.now(UTC)` on a live run (or reconstructed from the bronze sidecar on
  `--reingest`) — i.e. **an ingest timestamp, not a publication timestamp.** For
  point-in-time correctness in models, `available_at` must carry the **vendor
  publication vintage** when the vendor supplies one. Without this, a historical
  `as_of` fetch in models silently returns rows stamped with their re-ingest time,
  defeating the leakage barrier.

**Domain fact that shapes everything below** (vault
`30-vendors/elexon/datasets/system_prices.md`, verified against the transformer):
the live system_prices feed — the DISEBSP `DATE_PATH` endpoint
(`/balancing/settlement/system-prices/{date}`) — exposes **neither
`settlementRunType` nor `publishTime`**. Live silver has `run_type = None`;
`run_type` is populated only by legacy/alternate endpoints and older fixtures.
Each fetch returns the vendor's *current-best* value per settlement period, with
no in-band vintage marker. The only vintage signal on the live feed is **when we
fetched it**.

Two capture models were considered:

- **Option A — dedup on `(date, period, run_type)`.** Keep the F0 atomic-replace
  model; change `_resolve_runs` to key the dedup on run_type too. Rejected on two
  grounds. First, it **degenerates on the live feed**: with `run_type = None`
  everywhere, `(date, period, run_type)` collapses to `(date, period)` — exactly
  the broken behaviour under repair. Second, even where run_type exists, an
  overwrite model cannot preserve two vintages *of the same run_type* (a corrected
  R1) and captures no cross-ingest publication history, leaving P1.1's
  point-in-time keystone unsatisfied.

- **Option B — APPEND_ONLY + vintage-bearing `available_at`.** Vintages coexist as
  run-suffixed files keyed off `available_at`; run selection moves to read time.
  Works identically whether or not the vendor exposes a run label, and matches the
  ADR-017/018 precedent already used by `remit`/`fou2t14d`.

**Decision selects Option B** (user, 2026-07-10).

## Decision

### 1. Capture (P0.1 + P2.1) — APPEND_ONLY, no transform-time collapse

- `SystemPriceTransformer` sets `APPEND_ONLY = True`. Delete the
  `_resolve_runs` collapse: the transformer emits **every** row it receives,
  carrying `run_type` (usually `None` on the live feed) and
  `price_derivation_code` verbatim. Vintage selection is a *read-time* concern,
  per ADR-018.
- **Vintage-granularity requirement:** rows originating from different bronze
  fetches must **not** share a single transform-time `available_at`. Each bronze
  response's rows carry that response's sidecar timestamp as their vintage, and
  the writer keys the run-suffixed filename off it — one silver run-file per
  distinct bronze vintage. (Without this, a re-transform that reads a date's
  accumulated bronze would stamp II-era and R1-era rows with one `available_at`
  and the history would collapse inside a single file.) `--reingest` idempotency
  is preserved exactly as in ADR-018: the suffix derives from the bronze sidecar,
  so a second pass overwrites the same files.
- **Scope of the flip: `system_prices` only, this milestone.** The
  settlement-decomposition datasets (BOAV/EBOCF/DISEBSP-stack/ISPSTACK) do not yet
  exist in the repo; when they land they inherit this decision. `remit`/
  `fou2t14d` already comply.
- **Regression guard (P2.1 — same PR).** The run-precedence test becomes
  order-sensitive: an SF-then-II fixture plus a mid-frame R1 row must fail under a
  `keep="last"` / collapse regression, and a two-vintage live-shaped fixture
  (`run_type = None`, distinct bronze sidecars) must prove both vintages survive
  to silver. Landing P0.1 without these leaves the guard degenerate.

### 2. Read (P0.3) — `*_latest` QUALIFY views, one row per key

- Add a **latest-revision read surface** for APPEND_ONLY datasets: a DuckDB view
  `silver_<source>_<dataset>_latest` applying
  `QUALIFY ROW_NUMBER() OVER (PARTITION BY <business_key> ORDER BY available_at DESC, <run_rank> DESC) = 1`.
  - **Ordering is `available_at`-primary.** On the live feed run_type is `None`,
    so publication order is the only universal vintage axis. Run-precedence rank
    (RF > R3 > R2 > R1 > SF > II, `None` ranked lowest) participates only as the
    **secondary** tie-break, for legacy/alternate-endpoint rows that share an
    `available_at`.
  - system_prices business key = `(settlement_date, settlement_period)`;
    remit/fou2t14d reuse the same view factory with their own entity keys.
  - **Naming:** the `_latest` suffix, not `_current` — NESO *datasets* are already
    named `*_current` (`silver_neso_intensity_current`, `…_generation_current`,
    `…_regional_current`), so a `_current` view convention would be ambiguous
    against dataset names. `_latest` collides with nothing in the manifest
    (ADR-024).
- **Point the quality CLI at the `_latest` surface** so structural duplicate/gap
  checks see one row per key, not one per vintage.
- The view is the "current best value as of now" projection. It does **not**
  replace the point-in-time (`available_at <= as_of`) selection in
  gridflow_models — that consumer-side barrier is unchanged and remains the
  authority for training-time correctness.

### 3. Vintage semantics (P1.1) — `available_at = coalesce(published_at, ingest_time)`

- Introduce the derivation **`available_at = coalesce(published_at, ingest_time)`**
  at the silver-write boundary. When a transformer supplies a tz-aware UTC
  `published_at` (the vendor publication instant), it becomes `available_at`;
  otherwise `available_at` falls back to the existing ingest/reingest timestamp.
  Backward-compatible: datasets emitting no `published_at` keep today's behaviour
  byte-for-byte.
- **Real remaining scope** (verified 2026-07-10):
  - **Elexon is largely done** — 22 transformers already map `publishTime` →
    `published_at`. `system_prices` itself **cannot**: the live DATE_PATH feed has
    no `publishTime`, so its `available_at` is honest ingest-time and the
    stamp-fidelity table records it as such.
  - **ENTSO-E is the bulk** — the `with_published_at` helper
    (`silver/entsoe/_published_at.py`) exists but only ~4-5 of 26 transformers
    call it. P1.1 wires all of them (the parser already carries the document
    `createdDateTime`).
- Commit a **per-dataset stamp-fidelity table** documenting, for each dataset,
  whether `available_at` is a true publication vintage or an ingest-time
  fallback, so downstream consumers know which datasets support honest
  point-in-time queries.

## Consequences

- **Re-transform preserves history.** Re-transforming an SF/R1-era day from
  immutable bronze reproduces every vintage as a distinct file; the interim-price
  history survives. (The ask-first re-transform of on-disk system_prices silver
  is Phase-1 unit 1.6.)
- **Consumers must choose a surface.** Ad-hoc / quality reads use
  `silver_*_latest`; models keeps its `available_at <= as_of` barrier. Reading
  the raw multi-file glob now yields all vintages by design.
- **`available_at` changes meaning for vintage-bearing datasets** — from "when we
  ingested" to "when the vendor published". The stamp-fidelity table is the
  contract that makes this legible per dataset.
- **Filename idempotency holds** (ADR-018): re-ingesting the same vintage
  overwrites the same file; a genuine new vintage lands a new file.
- **Partition directories grow.** A future compaction job can roll old
  append-only files into per-month archives without touching the writer.
- **CH2-W1 intersection.** APPEND_ONLY capture is the mechanism the deferred
  `--incremental` revision-capture policy needs; P0.10 (watermark-advance guard +
  non-zero overlap default) is its ingest-side complement (Phase 2). This ADR
  does not itself resolve CH2-W1's scheduling policy.
- **P1.5 gold path.** `gold_uk_imbalance_context` consumes
  `silver_elexon_system_prices`; once APPEND_ONLY it must read the `_latest` view
  (or apply the same QUALIFY) — handled in P1.5 alongside the
  `run_type`/`price_derivation_code` column fix.

## Resolved at grill (2026-07-10)

1. **Tie-break authority** → `available_at DESC` primary, run-precedence rank
   secondary. The draft's precedence-primary leaning was wrong for the live feed
   (`run_type = None` everywhere there).
2. **Scope of the APPEND_ONLY flip** → system_prices only; the other
   settlement-decomposition datasets don't exist in the repo yet.
3. **Elexon `publishTime` truthfulness** → moot for system_prices (live feed has
   no `publishTime`); for the 22 datasets already mapping it, the pattern is
   established and unchanged.
4. **View naming** → `_latest` (a `_current` suffix collides with three NESO
   dataset names).
