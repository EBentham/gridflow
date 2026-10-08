# ADR-036 — NESO profiler, record proposals, vault skeletons and the pilot records

**Status:** proposed
**Date:** 2026-10-08
**Phase:** v0.22 unit E (profiler and pilot)
**Amends:** ADR-033 (the frozen-key ledger covers the swept bronze; `key_collisions` counts
only foreign sources), ADR-034 (the first frozen records).
**Cross-references:** ADR-030 (NESO source, `ckan_last_modified`), ADR-033 (registry,
coverage), ADR-034 (generic engine, V-rules, `_latest` modes), ADR-035 (dump vintage,
field-info).

## Context

Units A, B and D left 310 registry families with bronze for most of them (the S sweep:
304 dataset directories, 1,383 bodies) and no frozen record: every non-legacy family was
ingest-only. Writing 266 records by hand from the vendor dictionary alone repeats the
mistakes the research found (a dictionary format the bronze contradicts, a `Z` suffix on a
GMT/BST clock). Unit E measures the bronze offline, proposes a record per family with every
unsettled semantic marked, and freezes the first six through B's engine.

## Decision

**P-1/P-2 — the profiler** (`connectors/neso_data_portal/profile.py`). Offline: no network
(the snapshot verifier is imported lazily), no write under the data root, no registry edit.
Per usable CSV capture: a chunked byte pass (size, BOM, strict UTF-8, CRLF/LF/CR, cp1252
evidence), the header Polars parses (the one `readers.read_csv_body` matches), a sample of
at most `--sample-rows` rows that classifies value shapes **before** the measuring passes,
then two streaming passes over the full body: nulls, blanks, rows and all-blank rows; then
cast failures for the shapes' candidate dtypes, candidate-key and full-row duplicates
(hashed `n_unique`, so a collision can only make a unique key look duplicated) and
settlement-period coverage. Cast failures are counted on each cell exactly as the silver
engine casts it (unstripped; only the all-blank rows its reader drops are skipped), so a
drafted dtype with zero failures casts at transform time; shapes are classified on
stripped values, as evidence only. **I-1:** no body is read whole into Python and `read_csv`
always carries `n_rows`. A ragged or binary body is `parse_error` data, never coerced and
never an abort. Outputs (`families/<key>.json`, then `summary.json`, then the report) go
through `replace_atomically`, are byte-deterministic and carry no host or clock value.

**P-3/P-6 — proposals.** The dtype draft rule and the TODO consequence table are stated
once, in `_draft_column` and `TODO_CONSEQUENCES`. A draft follows the frozen-record shape
but every unsettled field holds `"TODO: <id>"`. `temporal` is **always** a TODO (bronze
alone evidences no zone or period), so no draft is a valid `SchemaRecord` until a human
settles it; `latest` is **always** `whole_capture`, because the profiler cannot know a
vendor-documented identity (the `key_identity` TODO names the best measured candidate).
A `held` TODO sets the draft's eligibility to held under the package's batch. Epochs and
same-package siblings are flagged, never merged or renamed.

**P-7 — outputs and counts.** The batch map (`docs/neso_data_portal/batches.json`) is
extracted from the dataset matrix. The counts are defined once, in the profiler's
docstring: the *measured family count* is registry `tabular` families with at least one
usable CSV capture; a batch's *distinct headers* is the sum of its families' header
epochs. The measured numbers live in `docs/neso_data_portal/v0.22-PROFILE.md`, rendered
from `summary.json` alone; the seat replaces the matrix's 274 with the measured count. On
the swept bronze: 269 measured families, 1,242 CSV captures (141 non-CSV listed), 358
distinct headers, 44 multi-epoch families, 28 sibling-candidate groups, 140 captures not
valid UTF-8, 39 parse failures.

**P-8 — eligibility report** (`eligibility.py`). One rule for an output's effective
eligibility: a held package holds every output; else the record's own eligibility; else
eligible. `docs/neso_data_portal/eligibility.md` is generated from the registry and
`--check`ed like `registry yaml --check`; any registry change regenerates it.

**P-9 — vault skeletons** (`skeleton.py`). One page per package from the registry, the
verified snapshot and optional field-info: files by disposition, schema tables, keys, both
clocks per vintage, cadence, licence and the RULINGS 477 attribution, holds. **I-3:** every
target is checked first; a page without `skeleton: true` is never overwritten.
`evidence.py` holds the snapshot and field-info integrity loaders both tools share.

**Pilot records (R-1..R-5).** Six families, one epoch each, the exact vendor header:

- **R-1** `temporal {kind: none}` for all six: no pilot column has an evidenced zone and
  period definition, and `month`, `date_sp1` and `sp_pair` embed a GB-local conversion.
- **R-2** `whole_capture` for the REG archetype; `key_latest` only where a
  vendor-documented identity is measured unique in every capture. `entity_key` is that
  identifier, else **every column** — a capture-level "no identical rows" guard that fails
  loud (`DuplicateEntityKeyError`) and never merges.
- **R-3** dtypes from field-info physical types (`numeric`→`float64`, `int4`→`int64`,
  `text`→`string`, `date`→`date` with the bronze format, `timestamp`→`string`). The one
  exception, `forecast_month`, is a `text` column with the documented `MMM-YY` format.
  A settlement-period column (`da_demand_fc_performance.settlement_period`) is `int64`
  bounded `min 1`, `max 50`, so an out-of-range period is excluded and counted, never
  written; the profiler drafts the same bounds on any `int64` settlement-period column.
- **R-4** `vintage: ckan_last_modified` (all six are uploads).
- **R-5** `version "1"`, `reader "csv"`, `encoding "utf-8"`, `issue {kind: none}`.

| Family | entity_key · latest | eligibility |
|---|---|---|
| `tec_register` | every column · `whole_capture` | eligible |
| `interconnector_register` | `project_number` · `whole_capture` | eligible |
| `embedded_register` | `project_number` · `whole_capture` | eligible |
| `demand_forecast_2_52w` | every column · `whole_capture` | held, Q-1 |
| `da_demand_fc_performance` | every column · `whole_capture` | held, Q-2 |
| `constraint_cost_fc_24m` | `forecast_month` · `key_latest` | held, Q-3 |

Evidence for the two choices the research did not fix:

- `da_demand_fc_performance` cannot take `sp_pair`: bronze repeats `(Date,
  Settlement_Period)` with identical `Datetime` on 2021-10-31 (50 rows, 48 distinct SP:
  SP4 and SP5 twice) and 2022-10-30 (48 rows, 46 distinct SP: SP2 and SP3 twice). A true
  UTC instant cannot repeat on a fold day, so the trailing `Z` labels a local clock; the
  datetime columns stay strings and `local_instant` stays forbidden.
- `demand_forecast_2_52w` has no label key: `(calendar_year, ESIWK)` repeats at 2027 week
  29 with `CDATE_peak` 2027-07-21 and 2027-07-22.
- TEC's `Project Number` is documented as unique but `PRO-000053` occupies two
  non-identical rows (bronze lines 1062–1063), so TEC takes every column; the
  interconnector and embedded registers' documented identifiers are measured unique.

**Class-3 findings (research §1–3) and E-SEM.** Field-info physical types are usable
evidence; `info.label`/`info.notes` are empty everywhere, and formats and time meanings
need bronze checks (TEC's dictionary says `yyyy-mm-dd`, every bronze date is
`dd/mm/yyyy`). No row identity in the pilot is vendor-evidenced as unique across captures.
The three held outputs wait on **E-SEM**, the research unit the seat dispatches after
merge; its conclusion lifts or keeps each hold through a registry commit that regenerates
the eligibility report:

- **Q-1** ESI week definition (start and end day, numbering, rollover against
  `calendar_year` and `financial_year`) and why 2027 week 29 appears twice.
- **Q-2** `Datetime`/`Publish_Datetime` end in `Z` but the dictionary states GMT/BST;
  whether `Datetime` marks period start or end; the repeated SP4/SP5 and SP2/SP3; whether
  `Publish_Datetime` is the publication instant.
- **Q-3** The currency unit of `Constraint Cost` (vendor metadata shows an undecodable
  symbol before `m`).

The question strings are binding verbatim in the records.

**Folded in.** `_frozen_keys.json` gains the 301 keys the S sweep captured (304 rows,
sorted), so coverage reports 0 unfrozen keys. `key_collisions` flags a key only when
another source registers it: a recorded family registers under `neso_data_portal` by
design (ADR-034 P-15).

## Consequences

- The six families are no longer ingest-only: their generated transformers, `_latest`
  views and the completion relation register (13 catalogue additions), and reconcile's
  drain covers them. **Activation is the seat's** (decision 18): transform every captured
  partition, run coverage and reconcile, before any refresh.
- **Peak memory** is per body, not per family: the real-bronze run peaked at 1.9 GiB
  working set (260 s wall), above the single streaming pass's 0.9 GiB because the second
  pass carries one cast, hash and count expression per column.
- **Parse-failure captures are not header epochs.** The 39 ragged `system_frequency`
  captures are ZIP bodies declared CSV; their first line is binary, so they are reported
  as `parse_error` and excluded from the distinct-header count.
- **Proposals are not records.** A batch unit reads a proposal, settles each TODO with
  evidence, and commits a record; the profiler never writes the registry.
- **Windows path length.** The engine's output names run to ~120 characters; the pilot
  tests use a short temporary root so the longest pilot keys stay under MAX_PATH. The
  production root is short.
