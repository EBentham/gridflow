# ADR-042 — NESO scenario unpivot and edition

**Status:** proposed
**Date:** 2026-10-10
**Phase:** v0.22 unit SC (scenario mechanism; pilot `fes_ed1_electricity_demand`)
**Amends:** ADR-034 P-1 (`UnpivotSpec`, `ValueSpec`, `edition_by_filename`, `epoch_outputs`, V-18,
`RESERVED`), P-4 (unpivot, then cast; exclusion judges long rows), P-5 (the `edition` stamp).
**Cross-references:** ADR-034 (generic engine, records, completion ledger, `_latest`), ADR-036
(records), ADR-039 (per-resource `_latest` via `latest_partition`; the `formats_by_filename`
discriminator precedent; FM-12).

## Context

FES and tRESP tables publish projection years as **wide columns** (one per year), and each FES
**edition** as a separate CKAN resource. ADR-034's engine types one silver row per body row under a
frozen record: it has no reshape step and no edition dimension. Without both, a FES family either
cannot get a record or collapses its editions.

The pilot, `fes_ed1_electricity_demand`, holds four CSV resources captured on 2026-10-08, one per
edition (2023, 2024, 2025, 2026). Measured on that bronze (read-only): six dimension columns then
contiguous year columns in every body (2010–2050, 2020–2050, 2023–2050, 2023–2050); 1,340 wide rows
become **45,985** long rows, of which **5,971** carry a blank year cell; every non-blank cell casts
strictly to Float64; no dimension tuple repeats within a body. The dimensions differ by edition:
`Scenario` (2023, 2026) against `Pathway` (2024, 2025), and `Aggregation Level` (2023–2025) against
`Level` (2026).

## Decision

**P-1 — the record declares the reshape.** `HeaderEpoch` gains `unpivot: UnpivotSpec | None`
(default `None`). An `UnpivotSpec` holds `years`, the exact vendor header labels each mapped to a
declared projection year in header order, and one `ValueSpec` (`dtype` string/int64/float64,
`nullable`, `null_tokens`, `min`/`max`). Labels are never parsed (decision 3): the year is what the
record declares. With `unpivot`, the epoch's `columns` are the index (dimension) columns only, and
the epoch's shape rule is: `columns` sources and year labels are disjoint, together cover the header
exactly, each sequence is in header order, and no index source is named `projection_year` or
`value` (the Polars unpivot would collide). The year columns are declared **once**, as one typed
value (C-1): there are no per-year `ColumnSpec`s and no temporary silver names, so nothing can leak
into `silver_columns`, the manifest or a typed-empty view.

`epoch_outputs(epoch)` is the epoch's post-reshape output specs: `epoch.columns` itself (the same
object) without `unpivot`; otherwise the index specs, then `projection_year` (int64, non-nullable)
and `value` (typed by the `ValueSpec`). `silver_columns`, V-1 and V-3 iterate it.

The generated names are fixed: `projection_year`, `value` and `edition` (C-2). `edition` joins
`RESERVED` (engine-stamped, like `resource_id`). `projection_year` and `value` are not reserved: an
already-long table (tRESP) declares them as ordinary columns, and V-1 enforces one dtype each across
epochs.

**P-1 (edition).** `SchemaRecord` gains `edition_by_filename`: exact `(resource_filename, edition)`
pairs (default `None`; non-empty, non-empty filenames, each filename once; two filenames may share an
edition). The discriminator is the captured resource filename (C-3), following ADR-039's
`formats_by_filename`; each pilot entry is justified by the captured filename and resource name
carrying the same year. The resource id is provenance and the `_latest` partition, not edition
evidence. No fallback, normalisation or regex. V-4 admits `edition` to the key only with the map.

**V-18.** If any epoch unpivots: every epoch's outputs include `projection_year` and `value`, and
`projection_year` is in the entity key. With an edition map: `edition` is in the entity key, and a
`whole_capture` record selects per resource (`latest_partition: resource_id`), since family-scope
selection would keep one edition.

**P-2 — the engine.** `type_child` matches the epoch, resolves formats, then (with `unpivot`) runs a
Polars `unpivot` of the year labels over the index columns and maps the label column with
`replace_strict(years, return_dtype=Int64)` (which also types a zero-height frame); null tokens and
the strict D-41 cast then run over `epoch_outputs`, so an uncastable year cell still fails the
capture. `_exclude` builds its mask from `epoch_outputs`, so exclusion judges **long rows**: a blank or
out-of-range year excludes that year's row only, counted and sampled. `edition_for(record,
resource_filename)` returns the exact map entry, `None` without a map, and raises
`UnmappedResourceEditionError` naming the filename and the mapped filenames. `finish_capture` calls it
first and stamps `edition` (Int64) when mapped; `record_dtypes` places `edition` after the container
columns and before `resource_id`.

**P-3 — the valid-empty path.** A header-only body checks `edition_for` before its completion is
recorded, so an unmapped file fails there too.

**P-4 — equivalence.** `metadata_dependencies` adds `resource_filename` for an edition-mapped record:
the derived `edition` depends on it, so a renamed file voids a COVERED grant.

**P-5 — skeleton pages.** An unpivot epoch's schema table gains two `(unpivot)` rows
(`projection_year`, `value`) and an `Unpivot, epoch n:` line listing each label → year; an
edition-mapped record gains an `- Edition:` bullet. Pages of records without the fields are unchanged.

**P-6 — the pilot record.** `fes_ed1_electricity_demand` gets record version 1: csv, utf-8 (the
reader strips the 2024 BOM), `temporal none`, `whole_capture` per `resource_id`, vintage
`ckan_last_modified`, four epochs (2023, 2024, 2025, 2026) with every dimension a nullable string
under its vendor-derived name (C-5: `scenario` and `pathway`, `aggregation_level` and `level`, are kept
apart; no vendor semantics are equated), the year labels written out literally, and `value` float64
nullable (C-6: a blank cell is kept as null; no null tokens). The entity key is `resource_id`,
`edition`, every dimension, and `projection_year`. The edition map is `fes2023_ed1_v001.csv` → 2023,
`fes2024_ed1_v002.csv` → 2024, `fes2025_ed1_v006.csv` → 2025, `10yo2026_ed1_v001.csv` → 2026. The
output is **held** (`E-SEM`): NESO does not state what period a year header denotes (or whether its
label is the starting or ending year), which ED2 definition applies to each ED1 item including the
2026 Ten Year Outlook, or what a blank projection cell means. `unit` is carried per row exactly as the
vendor states it (`GW`/`GWh`); nothing is converted.

**`latest_views.py` is not edited (C-7).** ADR-039's per-resource whole-capture selection already
keeps every resource, and each pilot resource is exactly one edition, so every edition survives
`_latest`.

**Leakage bound (C-8).** The as-of form bounds `available_at <= as_of` per capture, where
`available_at` is the CKAN `last_modified` publication vintage. Capture time is not the bound (SC-SPEC
leakage line; RULINGS 597): an edition is served at as-of instants after its vintage even when that
precedes its 2026-10-08 capture.

**No `ENGINE_VERSION` bump (C-4).** Every engine change is conditional on a field only the pilot
sets, so no existing completion is invalidated.

## Failure modes

An unmapped filename (a fifth edition, a re-versioned or renamed file, a sidecar without
`resource_filename`) fails the capture before any write, populated or header-only, and reconcile
reports it `failed` until a record commit maps it. A new header fails with `HeaderEpochError`. Two
editions with one grain are both served (edition is in the key; resources partition `_latest`); one
edition republished under a new resource id is served twice and reported as a non-drainable `overlap`.
Duplicate dimension tuples in one body fail with `DuplicateEntityKeyError`.

## Tests

`tests/unit/test_neso_scenario_mechanism.py` (T-SC1 byte-unchanged against the golden written at
`34992b6`; T-SC2 record rules; T-SC3 unpivot typing; T-SC4 edition) and
`tests/unit/test_neso_sc_pilot.py` (T-SC5 pilot over four cut fixtures; T-SC6 leakage matrix across
the four vintages plus a correction row; T-SC7 skeleton; T-SC8 the only generated addition).

## Consequences and residuals

- Every existing record, output, `_latest` relation and golden is byte-unchanged (I-1, T-SC1); the
  only generated addition is `fes_ed1_electricity_demand` (T-SC8).
- **FM-12 (accepted, unchanged).** A vendor edit in place without a `last_modified` bump re-captures
  with the old vintage, so an as-of read between the true and the recorded publication sees the edit
  (ADR-039 FM-12).
- **Mechanisms SCN-1…3 need that SC does not add (C-9)**, each an escalation trigger at its batch's
  pickup: the CSV table boundary for preamble bodies and their unnamed trailing columns (ES1 2021/2022,
  regional storage `>1MW` 2021); two-character regional year labels (`"20"`…`"50"`, expressible as a
  long epoch whose label mapping is that batch's evidence call); building-block `Baseline (…)` columns
  (a retained dimension or a declared year: the batch's call); technology-measure unpivots (LA heat
  model: a different axis, not a year unpivot); the ES2 2026 notes table; GSP-info 2022 decoding; and
  the tRESP edition (no edition evidence in captured names yet).
- Every researched FES/tRESP shape is one of three: wide years (P-1 unpivot), already long (declared
  `projection_year`/`value` columns, no unpivot), or retained measure columns (an ordinary record).
- **Pre-existing gap, not fixed here:** ADR-039's `formats_by_filename` also reads `resource_filename`,
  but `metadata_dependencies` does not list it for that case. It is outside this unit's byte-unchanged
  scope; noted for the seat.
