# ADR-032 — Keyless vendor rows in `bmunits_reference` are dropped, not fatal

**Status:** proposed
**Date:** 2026-09-09
**Phase:** none — raised from `gridflow_models` v1.9 unit S-2's pre-step
**Cross-references:** C-7 (v0.18, fail-hard on a null `bm_unit_id`, ruled by
Bobbo 2026-08-16), D-8 (`ENTITY_KEY_COLUMNS`), ADR-029 (bronze retention and
silver rebuildability).

## Context

C-7 made `BMUnitsTransformer` abort the whole transform when any row carried a
null or empty-string `bm_unit_id`. Its handoff records it as merged with **"zero
current on-disk exposure — a correctness fix ahead of exposure"**.

On 2026-09-09 that exposure arrived, and it is not one row. Every one of the 19
bronze captures of 2026-09-01 holds **3060 rows, exactly 90 of them keyless**,
the same 90 `nationalGridBmUnit` values each time. `gridflow transform elexon
bmunits_reference` therefore refuses outright, and no silver
`bmunits_reference` has ever existed on disk. That blocks a downstream
consumer (`gridflow_models` v1.9 S-2, which needs the FUELHH↔BMUNITS technology
mapping) on a vendor gap it cannot fix.

The keyless rows were verified against the live vendor endpoint before anything
was decided — record at
`gridflow_models/.planning/phases/v1.9-S2-realised-residual-perfect-prog/BMUNITS-NULL-KEY-VERIFICATION.md`.
Findings that bear on this decision:

- **Blank at source.** A live `GET /reference/bmunits/all` on 2026-09-09 returned
  3063 rows with **89** still null. Not a parsing or ingest defect.
- **Transient for at least part of the population.** `DYCEB-1` was null in all 19
  captures and is `E_DYCEB-1` eight days later. A null `elexonBmUnit` is at
  least sometimes pending-registration state, not a permanent semantic.
- **Zero join cost.** Every one of the 2467 distinct `bm_unit_id` values in `pn`
  silver (658k rows) and all 386 in `boal` resolves against a keyed reference
  row. Only 4 of the 90 appear in any time series, and those 4 are already
  present and keyed under a transposed spelling (`SOFOW-1x` / `SOFWO-1x`).
- **Re-keying would be actively destructive.** 7 of the 89 are shadow rows of
  units that ARE keyed. Synthesising `T_<nationalGridBmUnit>` would mint a
  duplicate of a real `elexonBmUnit`, and the `keep="last"` dedup would then
  overwrite Iron Acton, Killingholme and the four Sofia BMUs with rows that are
  null in every attribute — the exact collapse C-7 exists to prevent.
- **Not established:** no vendor documentation explaining why the field is
  blank. Three OpenAPI paths 404'd. The mechanism is a hypothesis; only the
  behaviour is evidence.

## Decision

Ruled by Bobbo in chat, 2026-09-09 (`gridflow_models/.planning/RULINGS.md`
#497): *"just drop the rows for now."*

`BMUnitsTransformer.transform` **drops** rows with a null or empty-string
`bm_unit_id` and logs every dropped `national_grid_bm_unit` at **ERROR** with a
`dropped N of M` count, instead of raising. The drop happens before the dedup,
so the collapse C-7 guards against remains impossible.

C-7's guarantee is unchanged and is not softened: **a null-key row still never
reaches silver.** What changes is the disposition of such a row — excluded and
recorded, rather than fatal to the dataset. C-7 protects the silver key space;
it was never meant to require the vendor to be complete.

There is deliberately **no bypass flag and no caller-facing switch**: no code
path admits a keyless row. And a payload in which *nothing* is keyed still
raises, because that is a broken feed rather than a vendor gap, and writing it
would replace a good reference dataset with an empty one.

`DATASET_VERSION` goes `1.0.0` → `1.1.0`. Under 1.0.0 a unit's absence meant the
vendor did not send it; under 1.1.0 it may also mean the vendor sent it without
a key. The stamp is how a consumer tells the two regimes apart.

## Alternatives considered

- **Keep the fail-hard.** Rejected: it makes a reference dataset that other
  joins depend on unbuildable for as long as the vendor has a gap, and the
  measurement shows the gap costs no join coverage. C-7 was ruled against an
  assumed exposure of one row, not 90.
- **Quarantine to a sibling artifact** — recommended by the verification agent,
  and the strongest alternative. Rejected **for now**, not on the merits: Bobbo
  ruled "drop", bronze already retains every dropped row so nothing is
  destroyed, and a new silver artifact is scope this decision does not need.
  The case for it is real and is carried as the fill-forward follow-up
  (`gridflow_models/.planning/BACKLOG.md`, #498) — notably `WTGRW-1`, whose
  `fuelType = WIND` exists nowhere else in silver once the row is dropped.
- **Re-key on `nationalGridBmUnit`.** Rejected on evidence — see the shadow-row
  finding above. Strictly worse than failing closed.
- **A percentage threshold** (fail if more than N% is keyless). Not adopted: it
  would introduce a magic number nobody has ruled on. The ERROR log with its
  count is the signal that the gap has grown; revisit if it ever does.
- **Leaving C-7's `== ""` predicate untouched.** Rejected. The predicate was
  written before this change and tolerates a whitespace-only key, but that hole
  was unreachable in practice under fail-hard: any null aborted the transform,
  so in the real payload *no* row reached silver. Dropping the nulls and writing
  the rest makes it reachable — a `" "` key would become its own entity key,
  join against nothing, and two such rows would collapse under the `keep="last"`
  dedup, which is the exact hazard C-7 exists to prevent. The predicate is
  therefore widened to `strip_chars() == ""`. Measured as a **no-op on today's
  data: 0 whitespace-only and 0 padded keys in the 3060-row bronze body** — so
  it closes a future hazard without changing any current row. A padded but real
  key is kept **verbatim**; `strip_chars` decides emptiness only and never
  rewrites an entity key.

## Consequences

- `bmunits_reference` becomes buildable, unblocking `gridflow_models` v1.9 S-2.
- **This trades a loud failure for quiet data loss**, so the loss must stay
  visible through **two** channels, not one:
  - `last_excluded_row_count` carries the **count** into the run status. D-40
    folds it into `rows_invalid` (`pipeline/runner.py:271-275`, `:1232-1234`),
    which promotes the transform to `completed_with_warnings` rather than
    `success`. This matters more than it looks: `BMUnitsTransformer` is neither
    `VINTAGE_PER_BRONZE_FILE` nor `PARTITION_DATE_COLUMN`-bearing, so the D-42
    empty-frame net (`silver/base.py:1622-1633`) cannot cover for it — this
    counter is the transformer's **only** structured channel.
  - The ERROR log carries **which units** went missing — full identities rather
    than a sample, because the count alone cannot tell a fill-forward what to
    fill.

  An earlier draft of this ADR claimed the ERROR log was the *only* remaining
  signal. That was wrong, and it was load-bearing: it argued the trade was
  acceptable while declining a designed, tested, runner-consumed channel that
  already existed. Corrected here (diff review REVIEW-DIFF-1 BLOCKER-2) rather
  than left in the record, because a decision log that freezes a false premise
  is worse than no entry.
- A `bmunits_reference` row's absence is now ambiguous without reading
  `dataset_version`.
- Bronze retains every dropped row (ADR-029), so the drop is fully reversible
  and a later fill-forward has a source. It has only one day of captures to
  work from today — that, not the policy, is the binding constraint on
  fill-forward.
- `DYCEB-1` is a ready-made regression fixture for a future promotion path: a
  genuine null→keyed transition in real captured data.
- C-7's original text said "do not soften it, do not add a bypass flag". This
  ADR is the record that the narrowing was ruled deliberately, with
  measurement, by the same person who ruled C-7 — not worked around.
