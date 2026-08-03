# ADR-029 - Bronze retention and silver rebuildability

**Status:** Proposed
**Date:** 2026-07-26
**Phase:** v0.18 kickoff + Phase R1; migrated to this repo under N-8 (v0.18 R2)
**Cross-references:** ADR-025 (the vintages bronze retention protects), ADR-028 (bronze is
never deleted, repaired, or written to — this ADR's binding half), `.claude/hooks/guard.py`
(mechanical enforcement, local-only), `v0.18-ROADMAP.md` (R2-exit rebuild gates)

## Context

> Provenance: v0.18 kickoff + Phase R1 (2026-07-26). Bobbo twice offered to delete the
> bronze tree ("at most a week's worth of data, trivial to re-extract") when approving the
> data-reset for the R2-exit rebuild. The orchestrator declined bronze deletion both times
> and withdrew the question rather than escalating it. `status: proposed` because Bobbo has
> not explicitly ratified the reasoning — he offered, was answered, and moved on.

v0.18's R2 exit runs a full **silver** wipe + rebuild from bronze, to repair four defects at
once (fuelhh null `published_at`, SP2 boundary duplicates, fou2t14d's collapsed vintages,
ENTSO-E dead-wired vintages) under the fixed transformers. Bobbo's instinct was that bronze
could go too, since the tree is small and the pipeline can re-ingest.

Measured 2026-07-26: bronze is **292 files / 61.3 MB / 2026-07-16 → 07-25**
(`elexon/remit` 64 files, `indo` 60, `fuelhh` 60, `fou2t14d` 60, `system_prices` 6,
`windfor` 4, plus Open-Meteo). Size is genuinely trivial.

The constraint is not size — it is that **re-extraction cannot return what bronze holds**.
Vendors serve their *current* revision of a past period, not the history of what they
published and when:

- **Measured for ENTSO-E** (C-13 probe, same day): two A44 fetches of one historical window
  32 s apart returned an identical payload with a fresh `mRID` and a moving `createdDateTime`
  — documents are generated per request. A re-ingest therefore stamps every recovered row
  with today's clock.
- **Measured for Elexon fou2t14d** (review V1): bronze holds 184,015 rows for ~7,980 keys —
  roughly 23 intraday publications per key, captured only because successive live fetches
  recorded them. A single re-ingest of those dates returns one publication per key;
  ~176,035 vintage rows evaporate.
- Partial exception: Elexon settlement **run types** (II/SF/R1/R2/R3/RF) are vendor-labelled
  and would survive a re-ingest. Successive publications *within* a run would not.

Deleting bronze is also strictly more operational work: the rebuild reads *from* bronze, so
deletion forces an authorized live re-ingest across six vendors before any rebuild can run.

## Decision

**Bronze is immutable and is never deleted, wiped, or edited in place — not for cleanup, not
for disk space, not as a shortcut to a clean rebuild.** Silver (and gold) may be wiped and
rebuilt freely and often; they are pure derivations. Any "reset the data" request resolves to
a **silver-only** wipe plus a rebuild from the retained bronze, announced with per-dataset row
counts before execution.

This makes explicit, with measurements, what the repo's existing "bronze is immutable" rule
already asserted and what `guard.py` already enforces mechanically.

## Alternatives considered

- **Delete both and re-ingest** (Bobbo's offer) — rejected: destroys the only multi-vintage
  data on disk, including the exact 176k rows that v0.18 R1-C shipped a fix to preserve, and
  would make the rebuild's own post-conditions ("fou2t14d vintage count reconciles with
  bronze") vacuous. No offsetting benefit: it costs an extra authorized ingest and returns
  strictly less.
- **Delete bronze for datasets deemed uninteresting** — rejected: the value of a vintage is
  not knowable in advance, and per-dataset carve-outs erode a rule whose whole worth is that
  it is unconditional.
- **Keep bronze but stop capturing vintages** (dedup at ingest) — rejected: it is the same
  information loss moved earlier, and it contradicts ADR-025.

## Consequences

- The bronze tree grows monotonically. At 61 MB / 10 days this is a non-problem for years;
  revisit only if the growth rate changes materially (a compaction/cold-storage scheme, never
  deletion).
- "Rebuild the data" always means silver+gold, and always needs no authorization beyond the
  announcement — no live API calls, no key use.
- Bronze remains the single recovery source, so its backup/mirror status is now load-bearing.
  **Open:** the tree lives on `C:\gridflow-data` (moved off OneDrive in 2026-07 for the DuckDB
  sync-lock); nothing currently mirrors it. Worth a deliberate decision separate from this one.
- If a future rebuild's vintage counts do NOT reconcile with bronze, that is a transformer bug
  to investigate, not an expected loss.

---

*Migrated from the vault (`quant-vault/00-active/decisions/2026-07-26-bronze-retention-silver-rebuild.md`)
to this repo on 2026-08-03 under v0.18 N-8 — bronze retention is gridflow-internal, so this ADR
is its canonical home; the vault copy is reduced to a pointer in the same unit.*
