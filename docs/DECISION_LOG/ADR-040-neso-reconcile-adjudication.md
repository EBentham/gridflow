# ADR-040 — NESO reconcile adjudication of vendor-caused gaps

**Status:** proposed
**Date:** 2026-10-09
**Phase:** v0.22 unit GEN-2H (adjudicated gaps; `metered_wind_output_monthly`,
`wind_bmu_boa_volumes`)
**Amends:** ADR-034 P-14 (reconcile categories, exit code, drain), ADR-039 P-5 (overlap
detection).
**Amended by:** v0.22 unit IC-2H (RULINGS 575), §Amendment 1
**Cross-references:** RULINGS 547 (GEN-2H split), RULINGS 551 (H7, the overlap check's memory
bound), ADR-033 P-11 (coverage's `_adjudications.json`, a different ledger), K-GEN-2-FACTS §2
(M-OVERLAP), §4 (B-ROW-GRAIN).

## Context

Two GEN-2 families carry vendor data that yields a reconcile gap no drain can close:

- `metered_wind_output_monthly` (9 financial-year resources): the 2025-2026 and 2026-2027
  archives both publish 2026-04-01 P1 to 2026-04-05 P1 (193 settlement keys) with
  conflicting values. ADR-039 P-5 reports this as `overlap`; NESO states no precedence, so
  both rows are served.
- `wind_bmu_boa_volumes` (9 resources): the 2018/19, 2019/20 and 2024/25 bodies repeat whole
  rows (18, 1 and 2 excess). No vendor-column key is lossless, so those captures fail the
  within-capture duplicate guard (`DuplicateEntityKeyError`), a `failed` gap that a drain
  re-runs into the same failure. The six clean archives are worth loading.

Before this ADR the only outcomes were a permanently red `reconcile --all` that every receipt
learns to read past (the silent-failure class), or leaving whole families unloaded.

Separately, `reconcile --all` segfaulted four times on 2026-10-09 in the overlap check: its
whole-family window (`n_unique` over the grain) over `da_wind_forecast_historic_day_ahead_bmu`
(21,906,814 rows) exhausted memory (RULINGS 551).

## Decision

**P-1 — a separate, committed ledger.** `registry/_reconcile_adjudications.json`, read by
`load_reconcile_adjudications`, holds `ReconcileAdjudication` entries: `family`, `category`,
`cause`, `captures`, `reason`, `question`, `evidence`, `ruling`. It is not a `SchemaRecord`
field (record dumps are COVERED proof inputs, so reconcile policy would enter the transform
contract) and not coverage's `_adjudications.json` (which matches by resource id and excuses
bronze coverage).

- `category` is `Literal["overlap", "failed"]`: this is the one allowlist of adjudicable
  categories. `missing`, `orphaned`, `missing_or_invalid_output`, `duplicated`,
  `stale_covered` (gridflow's own faults) and `stale_adjudication` fail validation.
- `cause` is set exactly for `failed` and names an allowed failure class (§Amendment 1 lists
  them).
- Each capture must full-match one committed capture id (both body-name stamp forms) under a
  real partition date: no wildcard, no `-`, no directory scope. An `overlap` entry names at
  least two captures.
- `reason`, `question` and `evidence` are one non-empty line each; `ruling` is a RULINGS line
  number (never read at runtime). Two entries may not share a `(family, category, capture)`.
- A missing ledger is a `RegistryError`, never "no entries". `reconcile_adjudication_problems`
  checks every entry against the registry: a recorded family, each capture under the family's
  own or a sibling's directory, each resource in the family's package.

**P-3 — scope, covering and staleness.** After the raw gaps are built, `reconcile` splits them:

- An entry is **in scope** when its family is checked and every capture is filed at or before
  the cutoff. An out-of-scope entry neither covers nor goes stale, so any gap it names stays
  open (fail closed).
- An in-scope entry **covers** a gap of its family and category on one of its named captures;
  a `failed` gap only when the gap's cause (the failure record's `error_class`) equals the
  entry's; an `overlap` gap only when it has peers and every peer (each other selected capture
  serving one of its shared keys) is named in the entry. A third resource overlapping a named
  capture therefore reopens it.
- Each in-scope capture that no covered gap names is one `stale_adjudication` gap (never
  drainable, never adjudicable). A superseded *failed* capture stays expected and keeps its
  gap, so its entry stays live; an `overlap` entry goes stale when its captures stop being the
  selected ones.

**P-4 — report, lines and exit.** `ReconcileReport.gaps` holds open gaps (including
`stale_adjudication`); `adjudicated` holds the covered ones. `passed` = no open gap (the CLI's
exit-0 predicate); `clean` = `passed` and nothing adjudicated, so adjudicated is never clean.
Lines: the `GAP` lines, then one `ADJUDICATED <category> <family> <day> <capture> <detail>
[ruling <n>; reason: ...; question: ...]` line per covered gap, then the existing `SUMMARY`
lines, byte-identical in text. **Per-category `SUMMARY` counts are over open gaps**, so a
receipt can read `SUMMARY overlap 0` beside `SUMMARY adjudicated 2`. Only when something is
adjudicated or stale do `SUMMARY adjudicated <n>` and `SUMMARY stale_adjudication <m>` follow;
a run without ledger entries prints exactly what it printed before. The CLI exits 0 with no
open gap, 1 with open gaps, 2 on a usage error or a missing, malformed or unbacked ledger.

**P-5 — drain.** The drain groups the open gaps only, so an adjudicated failure is never
re-run and its failure record is never rewritten (H4). The drain's report carries the
adjudicated bucket of its second reconcile.

**P-6 — the bounded overlap check (H7).** The selection is still `select_latest_vintage`. The
selected rows are split into `ceil(rows / OVERLAP_BUCKET_ROWS)` buckets (1,000,000 rows) by
`hash(seed=0)` of the grain struct; each bucket's shared keys come from a streaming
`group_by(grain).agg(n_unique(resource_id))`; the serving captures come from one streaming
semi-join of the selection on those keys with `nulls_equal=True`. Equal keys, nulls
included, hash equal within one process, so no key spans two buckets and the report does not
depend on the bucket count. No whole-family window remains.

**Committed entries (RULINGS 547).** `metered_wind_output_monthly` `overlap` on the 2025-2026
and 2026-2027 captures of 2026-10-08, and `wind_bmu_boa_volumes` `failed`
(`DuplicateEntityKeyError`) on the 2018/19, 2019/20 and 2024/25 captures of 2026-10-08, each
with the NESO question the close brief carries. The BOA family is also held (E-SEM) on its row
grain; its key carries `boa_volume`, which the duplicate guard cannot use to catch a wrong
volume.

## Consequences

- **R-1.** Each monthly re-capture of the selected 2026-2027 metered resource turns reconcile
  red (an open overlap plus a stale entry) until the ledger is edited in a PR. H3 demands
  this: an adjudication is for exact captures.
- **R-2.** The shared rows of a family are materialised after the semi-join, bounded by the
  overlap's size, not the family's. A pathological family that overlaps almost everywhere
  would materialise most of its key columns.
- **R-3.** The real-data peak of the bounded check was not measured at planning (too little
  free RAM); the slow test `tests/integration/test_neso_overlap_memory.py` or the seat's
  activation receipt measures it.
- No data changes: adjudication touches no silver, completion, failure or `_latest` byte; the
  overlap rows stay from both resources and the failed captures stay unloaded with their
  failure records. No dedup, occurrence index, sum or precedence filter.

## Amendment 1 — invalid-encoding bodies (IC-2H, RULINGS 575)

- **Allowed causes.** `cause` is `Literal["DuplicateEntityKeyError", "UnicodeDecodeError"]`.
  A generic class (`ComputeError`, `Exception`) stays unadjudicable: it names no vendor fault,
  so an entry naming it could cover a gridflow defect.
- **Why an invalid-encoding body is vendor-caused.** The `brit_ned` 20241016 weekly upload is
  one body in an otherwise UTF-8 package: of the 187 BritNed bodies it is the only one that is
  not valid UTF-8 (two standalone `0xA0` bytes, a cp1252 or Latin-1 no-break space).
- **The record's declared encoding stays authoritative.** No per-resource encoding, no
  re-decode, no bronze repair: the capture fails and stays unloaded with its failure record.
- **The reader guarantee.** Every body that is not valid in `record.encoding` fails with
  `UnicodeDecodeError` before Polars sees it, wherever the bad byte sits (data row, header,
  beyond the header pre-parse's reach, a truncated tail, behind a BOM, inside a markup body).
  A UTF-8 body is validated in bounded chunks and passed on uncopied, so a valid body's bytes
  and outputs are unchanged. Before this amendment the class depended on the byte's position
  (`ComputeError` from the header pre-parse, or `NotCsvBodyError`).
- **Committed entry.** `brit_ned` `failed` (`UnicodeDecodeError`) on
  `raw_20261008T085647Z_811bec71-f099-4474-ba5e-2f9932b39cc2_24bb3d9b.csv` of 2026-10-08, with
  the question to NESO: which text encoding the resource uses, and whether it can be
  republished as UTF-8.
- **Migration.** A failure record written before the gate keeps its old class (`ComputeError`)
  until a drain re-runs the capture and rewrites it (atomic replace). Until then reconcile
  reports the gap open and the entry stale (fail closed, exit 1).
  - **R-1.** Between the merge and that drain, `reconcile --all` on the real root exits 1 with
    the B-ENC gap open (`ComputeError`) and one `stale_adjudication`; the drain closes it.
  - **R-2.** About 100 bodies of 13 not-yet-recorded families (mostly `0xA3`, £) are not valid
    UTF-8; under a `utf-8` record they would fail with `UnicodeDecodeError`. When those
    families are recorded, their records must declare the real encoding.
