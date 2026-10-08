# ADR-034 — NESO generic silver engine, completion ledger, reconcile and drain

**Status:** proposed
**Date:** 2026-10-07
**Phase:** v0.22 unit B (generic silver engine)
**Amends:** ADR-033 C-7 (closed by P-13 below).
**Cross-references:** ADR-022 (enum sentinels), ADR-024 (schema manifest), ADR-025 (`_latest`
views), ADR-028 (bronze vouching), ADR-029 (bronze retention, silver rebuildability),
ADR-030 (NESO source), ADR-033 (registry, upload leg).

## Context

Unit A put 310 NESO families into a registry and captures every resource to bronze, but only
the three legacy keys (`daily_wind_availability`, `historic_generation_mix`,
`embedded_wind_solar_forecast`) have silver transformers. Hand-writing 272 more is not
viable, and the per-file branch of `BaseSilverTransformer` retained every body's frame until
the run ended, so a many-capture date grew memory linearly (1.16 GB for 2 captures, 2.16 GB
for 5, measured at `d7cf513`).

## Decision

**P-1 — a frozen schema record per family.** `FamilyEntry.record` (`registry/record.py`)
declares, per exact vendor header (an *epoch*), each column's silver name, dtype, format,
null tokens, nullability and bounds; the temporal recipe; the entity key; `key_latest` or
`whole_capture` selection; the run-type column; siblings; and the vintage recipe
(`ckan_last_modified`, `capture_fallback`, `issue_time_evidenced` with evidence). Load-time
rules V-1..V-13 reject a record that cannot be honoured, at the one validation site. A
family with a record is transformed by the generic engine; without one it is ingest-only.

**P-2 / P-6 — one capture, one output, one run id.** A capture's id is its body's
data-root-relative POSIX path; its partition date is the bronze date directory. The engine
transforms each (capture, family) pair on its own into one append-only output named
`<family>_<YYYYMMDD>_run<written-at stamp>_<sha256(capture id)[:32]>.parquet`, and refuses
to replace a file that holds another capture's rows. `run(date)` resolves one run id for the
whole call, so every output it writes carries the same `source_run_id`; the run id is
lineage only, and every clock comes from the sidecar and the record.

**P-4 / P-5 — typing order (I-2).** Each child is typed under its own epoch: null tokens,
casts, then exclusion of non-nullable nulls and bound breaches, counted per child and rule.
Only then, capture-wide: `timestamp_utc` from the temporal recipe, `published_at`,
`bronze_capture_id`, `capture_written_at`, and the entity-key uniqueness check. A row is
judged only by its own epoch's rules. A header-only body is a valid empty capture only when
unit A's `empty_capture` marker is set and the family allows empty; any other empty or
all-excluded body fails loud.

**P-7 — the completion ledger.** One Parquet record per (capture, family) under
`state/neso_data_portal/completion/<family>/`, written after the output (both atomic), then
the pair's failure record (if any) is unlinked; a failure writes only a failure record and
never removes an earlier output or record. One predicate, `is_valid`, decides whether a
record vouches for a complete current output (versions current; valid-empty with no output;
populated with an output of exactly `row_count` rows, every row at the current
`dataset_version` and, for the generic engine, exactly this capture's id). Skip-if-valid,
bespoke adoption and reconcile all use it.

**P-8 — the bespoke three keep their classes.** A post-run hook records each of their
outputs that passes `is_valid`; the drain adopts a valid output or re-transforms exactly as
the per-file branch would. Two captures resolving to one bespoke path are both left
unrecorded (`duplicated`).

**P-9 — frame release.** The per-file branch releases each body's frames before the next
(outputs byte-unchanged; pinned by a golden captured at `d7cf513`).

**P-10 — revision selection.** `key_latest` orders each key by `issue_time` (when declared),
then `available_at`, then the configured run-type rank, then the tie-break
`(capture_written_at, bronze_capture_id)`, all descending with nulls last. `whole_capture`
returns every row of the newest complete capture (a valid-empty capture is complete and
yields zero rows; an output without a valid completion, or whose row count disagrees, is
never eligible). As-of is a bound parameter (`$as_of`) applied before selection, in SQL and
in Polars alike (RULINGS 466); no registered view text carries it.

**P-11 — relation existence (I-1).** For every registered class whose relations exist by
registration (`silver/owned_relations.RegisteredRelationsTransformer`; the generic engine is
the only one today) the catalogue creates the base view, its `_latest` and the support
relation `state_neso_data_portal_completion` unconditionally: over Parquet when any exists,
else a typed-empty view of the same ordered `(name, type)` list. Existence has one input,
registration, so no ledger or silver state can remove a relation; disagreement between silver
and the ledger is reconcile's to report. The catalogue and the quality CLI stay source-free:
the marker class carries the output columns, the support relations and the completion frame.

**P-12 — manifest.** Generated families export `columns = record_columns(record)` with
`columns_source = "frozen_record"`, and take their designated date column and SQL type from
the recipe (`sp_pair`/`date_sp1`/`month` → the date column, `DATE`; instants and `none` →
`timestamp_utc`, `TIMESTAMPTZ`). V-13 keeps every designated date name on one SQL type.

**P-13 — ingest-only families are skipped loud, not failed.** A non-legacy family without a
record is registered ingest-only: `run_transform` skips it with a WARNING and
`completed_with_warnings` (tabular) or quietly with `success` (`files`), and the CLI prints
`skipped (<reason>)`. This closes ADR-033 C-7.

**P-14 — reconcile and drain.** `python -m gridflow.connectors.neso_data_portal.reconcile
(<key>…|--all) --cutoff YYYY-MM-DD [--drain]` reports `missing`, `failed`,
`missing_or_invalid_output`, `orphaned` (a: a completion without an expected capture; b: a
generic output without a completion), `duplicated` and `stale_covered`, exit 0/1/2. The drain
recovers `missing`, attempt-`failed`, `missing_or_invalid_output` and orphaned (b) per
(family, date), isolates every capture and group, refreshes the catalogue once and reconciles
again. A late drain mints the on-time vintage: only `source_run_id` differs (B7).
`stale_covered` compares a COVERED grant's evidence with the newest committed capture of the
covered and covering resources, their record versions, and the SHA-256 of the grant-holding
resource's registry child inventory.

**P-16 — memory gate.** A generic transform of 20 ~75 MiB captures of one date peaks within
10 % of 5. Both runs first transform the same 8 warm-up captures of another date: the
working set after each capture is flat, but the process peak climbs over the first ~6-9
captures while the allocator's cache warms, so without the warm-up the ratio measured
allocator noise (1.078 and 1.114 on the same engine). With it, three consecutive runs gave
1.045, 1.035 and 1.001; a temporary `retained.append(frame)` gives 1.65 GB for 2 captures
and 2.98 GB for 5.

## Consequences and residuals

- **C-3 / FM-12, accepted residual.** A later capture with the same non-null `published_at`
  as an earlier one inherits the earlier `available_at` (`coalesce`). It arises only through
  ADR-033 FM-9 (bytes changed without `last_modified` moving) or a refetch after an unusable
  newest capture. An as-of query between the two captures can therefore see the later bytes
  at the earlier instant. The tie-break still orders them deterministically.
- **FM-5 / FM-6, one residual of I-1.** With outputs wiped and the ledger kept (or the
  reverse), the relations stay registered and serve what survives: whole-capture `_latest`
  serves the newest capture that is still complete, or zero rows, until the drain. Reconcile
  exits 1 naming each pair. Before the next refresh after a ledger wipe the completion view
  errors on its emptied glob (loud).
- **FM-14, accepted residual.** COVERED metadata that changes with no new capture (a
  filename moves, `last_modified` does not) is not detectable from bronze.
- **FM-18, accepted residual.** A typed-empty base view does not see Parquet written after
  it was registered; every writer (transform, build, backfill, pipeline, the drain) refreshes,
  so only a crash between a write and its refresh leaves it stale, cured by the next refresh.
- **Inventory digest is registry-side.** No container reader exists yet, so `stale_covered`'s
  inventory check digests the registry's child inventory, not one observed in bronze.
- **Per-capture footprint.** One ~75 MiB CSV capture peaks near 1.1-1.6 GB, most of it the
  capture-wide pass (broadcast stamp columns over every row and the uniqueness check). Bounded
  per capture, not per date; a larger body is a later unit's concern.
- **Ingest-only warnings are loud by design** until unit E's records land: `pipeline
  neso_data_portal --all` ends `completed_with_warnings` for every tabular family without a
  record.

**Forward pointer:** ADR-036 freezes the first records (the six pilot families) and adds
the offline profiler that proposes the rest.

Forward pointer: ADR-037 supersedes the registry-side inventory digest and the capture-id COVERED pins.
