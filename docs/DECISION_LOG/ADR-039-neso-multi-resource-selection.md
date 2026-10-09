# ADR-039 — NESO multi-resource revision selection

**Status:** proposed
**Date:** 2026-10-09
**Phase:** v0.22 unit DEM-1H (multi-resource selection; `historic_demand`)
**Amends:** ADR-034 P-1 (V-4, V-17, `RESERVED`), P-4 (per-filename date format), P-10
(per-resource whole capture), P-14 (`overlap`).
**Cross-references:** ADR-033 P-10 (resource selection by name; UUID recreation), ADR-034
(generic engine, completion ledger, `_latest`), ADR-036 (records), ADR-038 (a
multi-resource family that escaped this only because it has an issue column).

## Context

`historic_demand` holds 26 yearly CSV resources (2001–2026). Each covers a disjoint
settlement-date range; there is no issue column and no `run_type`. ADR-034's record shape
cannot express it:

1. `whole_capture` keeps the single newest complete capture **per family**, so one year
   would survive in `_latest`.
2. `key_latest` on `(settlement_date, settlement_period)` alone breaks the repo rule that
   settlement data is never deduplicated on the pair without `run_type`, and V-6.
3. One header (epoch 2) carries three date formats by resource year: `%Y-%m-%d` for
   2001–2008 and 2025, `%d-%b-%y` for 2023, `%d-%b-%Y` for 2024. A `ColumnSpec` has one
   `format`.
4. Literal `NA` appears in the numeric columns of the epoch-2 bodies, and no NESO source
   defines it.

The same selection problem recurs in other multi-resource families without an issue column
(BritNed's 187 resources, the monthly BSUoS families, the skip-rate families, ASDP's monthly
dumps). The mechanism is general; this unit records only `historic_demand`.

Measured on the 2026-10-08 bronze (read-only): 26 captures, 450,766 rows (epochs 0–3:
175,296 / 70,128 / 192,864 / 12,478); no settlement pair is shared across resources; every
body is pair-unique; 26 days of 46 periods, 9,340 of 48 and 25 of 50; every 2001–2025 body
spans exactly Jan 1–Dec 31 of its filename year, 2026 runs to 2026-09-17.

## Decision

**P-1 — the record opts in.** `SchemaRecord` gains `latest_partition: "resource_id" | None`
(default `None`, so every existing record's `exclude_none` dump, and with it every COVERED
grant's evidence digest, is unchanged). `resource_id` joins `RESERVED`, because the engine
writes it (V-2 then refuses a vendor column of that name, and the profiler drafts such a
header as `resource_id_vendor`). V-4 admits `resource_id` to `entity_key` only when the
field is set. **V-17**: a partitioned record must be `whole_capture`, and its key holds
`resource_id` and at least one other column (the per-resource grain).

**P-2 — the engine stamps the resource.** For a partitioned record, `record_dtypes` places
`resource_id` (string) immediately before the capture stamps, and `finish_capture` stamps the
capture's sidecar `resource_id` on every row. The output columns, typed-empty base view,
manifest and equivalence projection all derive from `record_dtypes`. The within-capture
duplicate-key check now enforces the per-resource grain, so a body that repeats a settlement
pair fails loudly.

**P-3 — one newest complete capture per resource.** `LatestViewSpec` gains
`completion_partition` (`whole_capture` only; any other mode raises). Unset, the SQL is
the ADR-034 text byte for byte. Set, the eligible-completions subquery's tail
`ORDER BY … LIMIT 1` becomes
`QUALIFY ROW_NUMBER() OVER (PARTITION BY c."resource_id" ORDER BY …) = 1`. DuckDB applies
the `WHERE` (eligibility and the `$as_of` bound) before `QUALIFY`, so each resource's winner
is ranked only among completions available at the as-of instant (B3). The Polars mirror
sorts the eligible completions and keeps the first per partition
(`unique(subset=[partition], keep="first", maintain_order=True)`) instead of `head(1)`.
No registered view carries `$as_of`.

**P-4 — a per-filename date format.** `ColumnSpec` gains `formats_by_filename`: exact
`(resource_filename, format)` pairs, on `date` columns only, exclusive with `format`,
non-empty, filenames unique, compared exactly (no normalisation, no regex, no fallback).
`casting.epoch_formats` resolves each column's format for the capture; an unlisted filename
raises `UnmappedResourceFormatError`, on the populated path and the valid-empty path alike,
so the capture fails with a failure record and no completion. A mapped format goes through
the same strict cast as a scalar one.

**P-5 — overlap is a reconcile gap.** `CATEGORIES` gains `overlap` (not drainable). For each
checked partitioned family, reconcile selects `_latest` through `select_latest_vintage` (the
one Polars renderer, so the report and the view agree by construction) over the family's
outputs and its completions up to the cutoff, and reports every selected capture that serves
an entity key (the key without `resource_id`) also served by another resource. A check that
cannot read an output is one `overlap check failed` gap, never a pass. The CLI exits non-zero
on any gap, as for every category.

**P-6 — the `historic_demand` record.** Four exact header epochs, issue `none`;
`sp_pair (settlement_date, settlement_period)` (UK settlement date, period beginning);
`entity_key [resource_id, settlement_date, settlement_period]`; `whole_capture` per
`resource_id`; vintage `ckan_last_modified`. `SETTLEMENT_DATE` is `%d-%b-%Y` in epochs 0
and 1, `%Y-%m-%d` in epoch 3, and mapped per filename in epoch 2 (2001–2008 and 2025
`%Y-%m-%d`, 2023 `%d-%b-%y`, 2024 `%d-%b-%Y`). `SETTLEMENT_PERIOD` is int64 1..50. ND,
ENGLAND_WALES_DEMAND, NON_BM_STOR, PUMP_STORAGE_PUMPING and IFA_FLOW are float64 (no `NA`
in any body); FORECAST_ACTUAL_INDICATOR and every column that carries `NA` in any body stay
string in every epoch. The family is **held** (unit E-SEM) on the meaning of `NA`.

### Why these choices

- **C-1. A resource is its UUID.** The completion ledger already records the sidecar
  `resource_id` per (capture, family), so the per-resource winner needs no ledger column,
  no migration and no second SQL shape. NESO may recreate a resource under a new UUID with
  the same name (ADR-033 P-10); no source sets a precedence between the two, so none is
  invented: the two UUIDs are two resources and their shared targets surface as `overlap`.
  Partitioning by name would need the name in the ledger, a schema change across every
  family's completions.
- **C-2. The key names the resource.** V-6 requires an `sp_pair` key to strictly contain
  the pair; a key of every column (the K-DEM-1 guard key) would be a value key that also
  weakens the duplicate check. The measured grain is the pair within one resource body.
- **C-3. `NA` columns stay text in every epoch.** V-1 fixes one dtype per silver name across
  epochs, and declaring `NA` a null token is not evidenced. Every non-`NA` value of those
  columns casts to float, so switching them is a one-record edit once NESO defines `NA`.
- **C-4. No `ENGINE_VERSION` bump.** The version enters every generic output's
  `dataset_version` and every completion; a bump would re-transform all 33 recorded
  families. Every engine change here is conditional on a field only `historic_demand` sets.
- **C-5.** The tests that pinned `historic_demand` as ingest-only are updated, not deleted.
- **C-6. Overlap is a gap, not a failure and not a `_latest` filter.** The transform runs
  per capture and cannot see other resources without becoming order-dependent; filtering in
  `_latest` would be the invented precedence H2 forbids. `_latest` serves both captures'
  rows and reconcile reports them.

### Proof

- **Byte-unchanged (H4).** `tests/fixtures/neso_data_portal/dem1h/base_pin.json` was written
  on the untouched base (master `73fde80`): every generated family's `_latest` SQL in both
  as-of modes, every record's dump and output columns, and every DEM-1 fixture's engine
  output (`source_run_id` excluded). `test_neso_multi_resource.py::TestByteUnchanged`
  compares the tree against it; the only addition is `historic_demand`.
- **Selection.** `TestLeakageMatrix` (both renderers, per-resource corrections, a resource's
  first capture after the bound, valid-empty between populated captures, ties, incomplete
  and completion-less captures, `available_at == as_of`, one pair in two resources),
  `TestRandomisedParity` (SQL = Polars = a plain-Python oracle, 40 seeded rounds),
  `TestFamilyScopeUnchanged`, and `TestEngineAndCatalogue` (three resources through `run()`
  and the catalogue).
- **Record rules.** `TestRecordRules` (V-17, V-4, V-2, the committed registry, `None`
  defaults); `TestFormatMapShape`, `TestFormatResolution`.
- **Overlap.** `TestOverlap` (served and reported, UUID recreation, disjoint resources, an
  unpartitioned family never checked, an unreadable output).
- **`historic_demand`.** `test_neso_dem1h_record.py`: every epoch types its fixture with no
  exclusion, every date falls in its filename year, an unlisted filename fails, DST days of
  46 and 50 periods and the UK period start, `NA` kept as text, `_latest` serving every
  resource with no overlap, the held eligibility.

## Failure modes

- A crash after one resource's output and before its completion: that capture is not
  eligible, the resource falls back to its previous complete capture, and the other
  resources are untouched; reconcile reports orphaned (b) and the drain recovers it.
- A missing or partial output: not eligible (count ≠ `row_count`); the same per-resource
  fallback; reconcile reports `missing_or_invalid_output`.
- An unlisted filename, a bad cast, an unknown header or a repeated pair within one resource:
  the capture fails with a failure record and no completion; reconcile reports `failed`.
- A valid-empty newest capture: that resource contributes zero rows; an as-of before it sees
  its earlier rows.
- A resource's first capture after the as-of instant: it contributes nothing; other
  resources' earlier winners survive (the bound is in `WHERE`, before ranking).

## Consequences and residuals

- **FM-7 (accepted).** NESO recreating a resource under a new UUID with the same name gives
  two resources: `_latest` serves both captures and reconcile reports `overlap` for each.
  Resolution waits for a vendor precedence rule.
- **FM-8 (accepted).** NESO deleting a resource leaves no deletion signal in bronze, so its
  newest complete capture stays in `_latest`. Not a loss.
- **FM-9 (accepted).** A `%d-%b-%Y` date (the scalar format of epochs 0 and 1, or the 2024
  map entry) that meets a two-digit-year body parses silently as year `00yy` (Polars
  1.40.1). The per-file map was measured on every 2026-10-08 body, and the tests check that
  every fixture's dates fall in its filename year, but this stays a residual **for any
  future capture of any `%d-%b-%Y` resource**, whatever its filename: no strict parser
  tells the two apart.
- **FM-12 (accepted, unchanged).** For a body whose `ckan_last_modified` predates its
  capture (the 2009 file, captured in 2026), `available_at <= as_of` is not proof the bytes
  were captured by then (ADR-034 C-3). Partitioning does not change it.
- The output is held until NESO defines `NA`.
- Families expected to adopt `latest_partition` in their own batches, not recorded here:
  `brit_ned` (187 resources), the monthly BSUoS families
  (`bsuos_monthly_forecast_actual_sum`, `_fc_summary`, `_fc_summary_pct`, `_summary`,
  `_summary_pct`, whose resources share target months and will report `overlap`), the
  skip-rate families (`skip_rates_exclusion_reasons`, `skip_rates_in_merit_all_bm`,
  `skip_rates_in_merit_psa`), and ASDP's monthly dumps once their issue fields are settled.
