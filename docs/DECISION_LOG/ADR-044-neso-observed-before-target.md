# ADR-044 — NESO rows observed before their target start: known-at = gridflow's first capture

**Status:** proposed
**Date:** 2026-10-24
**Phase:** v0.22 follow-up (research `neso-known-at-20261024`, Sol review REVIEW-REC-1)
**Amends:** ADR-036 P-8 (a held output may serve a qualified subset of its rows through named
selectors); ADR-033 (a fourth `_`-prefixed registry ledger).
**Cross-references:** ADR-025 (bitemporal columns, the leakage barrier), ADR-034 P-10 (complete
captures, as-of before selection), ADR-035 (`capture_fallback` for dumps), ADR-039 (per-resource
selection), ADR-041 (held gold contracts), RULINGS 466 (as-of is a parameter, never in a view),
RULINGS 529 (forward-target rule).

## Context

gridflow keeps every NESO capture in silver, one output per capture, each row stamped with its
capture's `bronze_capture_id` and `capture_written_at`. `_latest` is the newest complete capture and
the as-of read is capture-level (`available_at <= as_of` on the completion records). Nothing records
when gridflow first saw an individual row.

Capture time is already a clock: `capture_fallback` stamps it on every dump family, and 28 held
outputs use it. A later stamp never breaks the leakage barrier, so those holds are not "we don't know
when this was known". They say "a row whose target precedes its stamp may be a revised or
retrospective value": RULINGS 529's forward-target rule, applied today to a whole capture and
enforced by tests only. History-bearing bodies fail it for the whole file even where some rows were
captured before their target started.

The research classed twelve families as held only on that question (`holds48.md`, class A). The
review removed two of them (below), leaving ten.

## Decision

**P-1 The rule, per row.**

- A **row version** is (family, capture resource from the completion ledger, every non-lineage
  column). Lineage is the record `RESERVED` set (`capture_written_at`, `bronze_capture_id`,
  `available_at`, `published_at`, `dataset_version`, `source_run_id`, `timestamp_utc`, ...). Nulls
  compare equal. Resources stay part of identity, so a rename never matches across resources.
- **`first_seen_at`** is the earliest `capture_written_at` among the **complete** captures holding
  the version (the `_latest` completeness rule, `latest_views.complete_captures`: `populated` with a
  matching row count, or `valid_empty`). Incomplete captures neither date a version nor serve a row.
- A version is **observed before its target** when `timestamp_utc > first_seen_at`, strictly; a
  target equal to first sight is excluded.
- **The claim is narrow:** this record content was observed by gridflow before its evidenced
  target start, so it cannot be a hindsight value. It is not a claim that the content is a
  forecast. Each family keeps its recorded semantics: tariff Draft/Final status, issued limits,
  plan snapshots, forecasts. A value equal to an earlier one (A->B->A) evidences earlier knowledge of
  the content, not of its reissue.

**P-2 Policy outside the schema records.** `registry/_observed_before_target.json` lists each
admitted family with a one-line rationale and an evidence pointer (the `_reconcile_adjudications.json`
pattern). No `SchemaRecord`, `Held` model, record version or `dataset_version` changes, so the base
pins (`tests/fixtures/neso_data_portal/{dem1h,h,sc}/base_pin.json`) and every COVERED fingerprint stay
byte-unchanged. `load_observed_before_target` refuses a missing file (never read as empty), a
repeated family, and any entry `observed_before_target_problems` rejects:

- the family has a record;
- its package is not held, and the record's own eligibility is `Held` (so the effective
  eligibility is that hold);
- its temporal kind has an evidenced target start: `utc_instant`, `sp_pair` or `date_sp1`, never
  `none` (the capture time), `month` or `local_instant`; and a `date_sp1` record anchored on a label
  date column (`LABEL_ANCHOR_DATE_COLUMNS`: the K-AS-1 and K-DFS anchors, where EFA 1 starts 23:00
  local the previous day) is refused;
- it does not take `issue_time_evidenced`, which already dates its rows by another clock.

**P-3 Two selectors, opt-in enforced.** `gridflow.silver.neso_data_portal.first_seen`, pure Polars
over the all-vintage base and the completion ledger. Both take the family key and refuse
(`NotObservedBeforeTargetError`) a family absent from the validated policy:

- `observed_snapshot(family, base, completions, as_of)`: what gridflow held at `as_of`. It chooses
  the newest complete capture with `capture_written_at <= as_of` (capture time, not `available_at`),
  one per resource when the record sets `latest_partition`, **before** touching rows, so a deletion,
  a replacement or a valid-empty capture clears older rows. It then keeps that capture's rows whose
  version was first seen (over complete captures written at or before `as_of`) strictly before its
  target start. Lineage columns are the selected capture's; `first_seen_at` is added.
- `observed_archive(family, base, completions, as_of=None)`: every distinct version observed before
  its target, once, represented by its row in its first complete capture, so every lineage column
  is that capture's. With `as_of`, only captures written at or before it take part. It is "content
  observed", not "content current at `as_of`": a version later revised or deleted is still returned.

Neither selector reads anything written after `as_of`, so a later capture never changes an earlier
result. No view carries `as_of`. `with_first_seen` remains as an annotation only, with no
eligibility claim.

**P-4 Holds stay holds.** A listed family's output is still `Held` for every consumer: gold
contracts stay in `views/held/`, `_latest` and ordinary silver reads are unchanged and do not acquire
the guarantee. Only the two selectors serve the qualified rows. The eligibility report and the vault
skeleton's Holds section append "rows gridflow observed before their target start are readable
through `observed_snapshot` / `observed_archive` (known-at = first capture, ADR-044)", and the
report's totals count such outputs.

**P-5 Scope: ten families.** `bsuos_fixed_tariffs`, `country_ci_forecast`, `national_ci_forecast`,
`wind_forecast_14d_day_ahead_bmu`, `wind_forecast_14d_days_ahead`, `daily_opmr`, `ifa_itl`,
`ifa2_ifa_itl`, `viking_link_ntc`, `system_operating_plan`. Two class A families stay fully held:

- `dc_forecast_4d_history`: its `date_sp1` timestamp is a label anchor; EFA 1 starts 23:00 local on
  the previous day, so `timestamp_utc > first_seen_at` can pass after the row's first block has
  started. The policy loader refuses it until its EFA date and window convention is evidenced and
  qualified per block.
- `ic_requirement_auction`: rows mix requirements with auction outcomes. A capture just after
  clearing would certify an outcome as observed before delivery. Requirements and results need
  separate semantics first.

## Proof sketch

Take a version V observed before its target, first seen in complete capture C at `t = first_seen_at`.

1. **Not early.** gridflow held V's bytes at `t`, so NESO had published V at or before `t`; serving
   V at any `as_of >= t` cannot show what was not yet public.
2. **Not hindsight.** V's target starts after `t`: when gridflow saw V, the target period had not
   begun, whatever revision of earlier content V may be.
3. **No future lineage.** Both selectors drop every row of a capture written after `as_of` before
   computing anything, so neither `first_seen_at` nor any returned lineage depends on a later
   capture. The snapshot's lineage is the capture held at `as_of`; the archive's is V's first.
4. **Deletion honoured.** The snapshot chooses the capture before filtering rows, so content absent
   from the capture held at `as_of` (deleted, replaced, or a valid-empty capture) is not served by
   it.

**Clock assumption.** `capture_written_at` is stamped after the body is published
(`src/gridflow/bronze/writer.py:156-158`, NESO publish path; measured 4-15 ms after fetch). A late write only makes the
bound later (conservative). A clock running behind real time would break it: a download at real
10:05 stamped 10:00 could qualify a 10:03 target. No such skew was found in the sampled captures.

## Measured (live silver, 2026-10-24, read-only)

Two captures exist for nearly every family (2026-10-08 and 2026-10-16). Over the ten families,
`observed_archive` returns 309,984 versions and `observed_snapshot` at 2026-10-24 returns 157,061
rows, both equal per family to the research measurement (`forward-measure.md` §1: national CI 1,942
/ 1,439; country CI 202 / 101; 14-day wind BMU 306,195 / 154,558; 14-day wind aggregate 1,338 / 669;
daily OPMR 213 / 200; Viking 69 / 69; IFA 11 / 11; IFA2 11 / 11; BSUoS fixed tariffs 2 / 2; SOP 1 /
1). Without the fully reissued 14-day BMU forecast, the archive is 3,789 versions.

## Limits

- Nothing dated before 2026-10-08 can ever qualify; between captures a version is only placed
  within the capture window. The qualified slice grows only if captures continue.
- A value that changed and changed back between two captures is invisible.
- `utc_instant` copies a vendor timestamp; it does not by itself establish start-versus-end
  semantics. The admitted families' records evidence it individually; a new entry needs its own.
- **Vendor times are not adopted.** Upload-time columns, voltage `last_updated` and IC auction
  `published_datetime` are consistent with per-row publication over the one 8-day window, which
  cannot show that pre-2026-10-08 history was unchanged since its stamp. SOP creation time, DC
  `forecast_created` and demand-performance `publish_datetime` are disproved as row availability
  times; daily OPMR `publish_date` holds only at end-of-day grain. Revisit after more captures.
