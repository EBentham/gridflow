# ADR-043 — NESO system frequency: rendering families, alternative formats and overlap groups

**Status:** proposed
**Date:** 2026-10-15
**Phase:** v0.22 unit H (`system-frequency-data`, 1-second GB system frequency)
**Amends:** ADR-034 P-1 (`ColumnSpec.alt_formats`, `SchemaRecord.overlap_group`, V-21), P-4 (the
alternative-format parse and its strictness pass); ADR-039 P-5 (the group overlap check and the
interval pre-check); ADR-040 (overlap adjudication across a group's families).
**Cross-references:** ADR-034 (generic engine, records, `_latest`), ADR-037 (`zip_member`), ADR-039
(per-resource `_latest`, `overlap`), ADR-040 (reconcile adjudication).

## Context

`system-frequency-data` publishes one CSV resource per month, 152 of them on the 2026-10-08 capture
(January 2014 to August 2026), every body with header `dtm,f` (CRLF, no BOM, strict UTF-8), together
**399,686,400** one-second rows. The bodies are not one shape:

- **Two body formats.** 113 bodies are CSV; 39 (January 2019 to March 2022) are deflated ZIPs, each
  holding exactly one CSV member, `f 2019 M.csv` or `fNew YYYY M.csv`. The registry already lists
  each ZIP's member as the resource's one child.
- **Three renderings of `dtm`.**
  - **R1** (2014, 12 months): day-first and offset-less, `31/01/2014 23:59:59`. No NESO statement
    gives its zone. October 2014 repeats the labels `26/10/2014 01:00:00` to `01:59:59` (3,600 extra
    rows), and March to September carry the following month's first hour.
  - **R2** (2015 to 2019, 60 months): ISO with a numeric offset in **every** row (157,766,400 rows
    measured), `2015-03-29 00:59:59 +0000` then `2015-03-29 02:00:00 +0100` at the spring change.
  - **R3** (January 2020 on, 80 months): ISO, naive. NESO's package page states: "Time values are
    all given in Greenwich Mean Time (GMT) starting January 2020"
    (<https://www.neso.energy/data-portal/system-frequency-data>). **August 2026** renders each of
    its 31 midnights date-only (`2026-08-01`); the other cells are full.
- Measured (v0.22-H research (a)): every R2/R3 month covers exactly its UTC month with no repeated or
  missing second; all 139 adjacent edges from January 2015 to August 2026 are contiguous with no
  overlap; CKAN `last_modified` of the 2014 months is 2023-03-24, so NESO re-uploads old months.

One `SchemaRecord` has one `reader` and one format and zone per datetime column, and V-1 fixes one
dtype per silver name, so no single record can read the package. A strict `%Y-%m-%d %H:%M:%S` parse
fails August 2026's date-only cells, and excluding them is forbidden (seat ruling 6). The per-family
overlap check of ADR-039 P-5 cannot see an instant served by two families.

## Decision

**C-1 — five rendering families, existing mechanisms.** The bronze home `system_frequency` stays
record-less (every resource keeps `family: system_frequency`, so ingest, the frozen key and coverage
are unchanged; ingest fetches nothing for the new families). Five sibling-fed families
(`siblings: ["system_frequency"]`, ADR-039's sibling read) split the package by rendering and body:

| Family | Rendering | Reader | Months |
|---|---|---|---|
| `system_frequency_dmy_csv` | R1, raw string, held | `csv` | 2014-01..2014-12 (12) |
| `system_frequency_offset_csv` | R2, `%Y-%m-%d %H:%M:%S %z` | `csv` | 2015-01..2018-12 (48) |
| `system_frequency_offset_zip` | R2 | `zip_member` (`f \d{4} (?:[1-9]\|1[0-2])\.csv`) | 2019-01..2019-12 (12) |
| `system_frequency_gmt_zip` | R3, zone `UTC`, alt `%Y-%m-%d` | `zip_member` (`fNew \d{4} (?:[1-9]\|1[0-2])\.csv`) | 2020-01..2022-03 (27) |
| `system_frequency_gmt_csv` | R3 | `csv` | 2022-04..2026-08 (53) |

R2 and R3 never share a family: one column mixing them would let an offset-less R2 row parse as
UTC, which defeats the every-row offset evidence. Every record: `encoding utf-8`, one epoch `dtm,f`,
`f` float64 nullable with no bounds or null tokens (no vendor range; a blank reading stays null),
key `(resource_id, dtm)`, `whole_capture` with `latest_partition: resource_id`, vintage
`ckan_last_modified`.

**Zone evidence (H4).** R2's instants come from the offset each row carries (converted, never
relabelled). R3 is typed UTC on the package's GMT statement quoted above. R1 has none: `dtm` stays
the raw vendor string, `temporal: none`, and the output is **held** (`unit H-R1`) with the verbatim
`TODO:` question; no local-instant inference from boundary shapes or row counts.

**P-3 — `ColumnSpec.alt_formats`.** `alt_formats: tuple[str, ...] | None = None`, `datetime` only:
further `strptime` formats, tried in order, for a cell `format` does not parse. Shape: the column is
`datetime` with a `format` that has no `%z`; the tuple is non-empty; each entry is non-empty, has no
`%z`, is unique and differs from `format`. The cast becomes the coalesce of non-strict parses, the
primary format first (so a full cell is never read by the shorter `%Y-%m-%d`, whose parse of a full
cell is null, not a truncation), then the unchanged zone tail. Strictness is kept by a pass before
the cast: a non-null cell no format parses raises `UnparsedDatetimeError` (column, count, formats,
up to five sample cells) and the capture fails with its failure record; nothing is coerced or
excluded by the parse. A null cell still meets `nullable`. A column without the field is cast
exactly as before.

**P-4 — `SchemaRecord.overlap_group` and V-21.** `overlap_group: str | None = None` (a silver name)
marks families of one package whose `_latest` relations together form one series. **V-21**, run once
per package at load: every member selects per resource (`latest_partition: resource_id`); a group has
at least two members; every member has the same grain (its `entity_key` without `resource_id`, in
order) and the same silver dtype per grain column. The four typed families form group
`system_frequency`; R1's string `dtm` cannot join it (V-21), so the December 2014 to January 2015
edge is not checked (residual FM-10).

**P-5 — the group overlap check and the interval pre-check.** Reconcile's overlap check generalises
over a group: each member's `select_latest_vintage` selection (its own files, spec and completions
up to the cutoff) is reduced to `(grain, resource_id, bronze_capture_id, family)` and concatenated;
the bucketed `group_by` and the semi-join of ADR-040 run over the union; each capture's gap is filed
under its own family, with peers across the group, and only for the checked families. Reconciling
one member still reads the whole group, so a cross-family overlap is reported on the checked family
with the other family's capture as a peer. An unreadable output gives one `overlap check failed` gap
per checked member. A family without a group is checked exactly as before (one-member call, same
detail text and peers).

Before the buckets, `_proven_disjoint` may short-circuit: only for a one-column grain of dtype
`Utf8`, `Int64`, `Date` or `Datetime`, one streaming `group_by(bronze_capture_id)` gives each
selected capture's `(min, max, null_count)`; any null returns `False`; the spans are sorted by
`(min, max)` and swept, returning `False` as soon as a span's `min` is at or below the running
`max`. **Soundness:** an overlap needs one non-null key value `k` served by two selected captures;
`k` lies in both spans, so after sorting the later span's `min <= k <=` the running max and the
sweep returns `False`; `True` therefore implies the exact path would return no gap. Nulls go to the
exact path, which joins with null equality.

**Memory (H7).** A `whole_capture` selection reads its member's scan twice (row counts per capture,
then the semi-join on the winners). Polars' common-subplan elimination caches a scan read twice, so
a query collected with it materialises every selected row: 8.7 GB peak over the real group's 368M
rows. Every query the check collects (the pre-check, each bucket, the semi-join) therefore runs on
the streaming engine with `comm_subplan_elim` off; each branch streams its own scan (the files are
read twice, results are identical). Measured over the full 2026-10-08 copy: `reconcile` of the five
families 1.86 GB peak, the group pre-check 1.05 GB.

**ADR-040 amendment — group adjudication.** An `overlap` entry still covers a gap of its own family
only when every capture serving the shared keys is named. A capture the entry names that is a peer
of a gap the entry covers, and on which the entry's family has no gap of its own (a capture of
another family of the group), is accounted, not `stale_adjudication`. A cross-family overlap is
therefore adjudicated by one entry per family, each naming both captures; a third capture joining
the overlap leaves every gap open, and an entry whose overlap has gone is stale. Within one family
every peer has its own gap, so the rule changes nothing outside a group.

**C-2 — selection is ADR-039's, unchanged.** `whole_capture` + `latest_partition: resource_id`
serves each month's newest complete capture; a revision supersedes its month whole, so an instant a
revision drops is absent from `_latest` and present at an `as_of` before the revision (seat ruling
7). An instant-keyed `key_latest` recovers a dropped instant and is not used. Measured on a 402M-row
synthetic store: full `_latest` count 7.2 s, one-day count 2.8 s; no new mode. `latest_views.py`,
`generic.py` and the readers are not edited.

**C-3 — no union view.** Consumers union the four typed `_latest` relations; the group check proves
at every reconcile that the union serves no instant twice (a reconcile that finds one fails and names
it). The held R1 relation never joins them.

**C-4 — future months.** `name_regex` stays `null`, so only registered resources are fetched; a new
month is reported missing by coverage until a registry edit adds it under `system_frequency` with
`SILVER(system_frequency_gmt_csv)`, from where `alt_formats` types any date-only midnight.

**C-5 — October 2014.** The member fails `(resource_id, dtm)` with `DuplicateEntityKeyError`, is not
served and is never resolved by row order. Its capture carries an ADR-040 `failed` adjudication
(ruling 861) asking which row of each repeated label is the first (BST) hour.

**Byte-unchanged elsewhere (H8).** Both new fields default to `None`, so every existing record's
`exclude_none` dump (and so every COVERED evidence digest), every generated `_latest` SQL text,
every output column list and every DEM-1 engine output is identical; the golden written on the
untouched base (`tests/fixtures/neso_data_portal/h/base_pin.json`) proves it.

## Proof

- `tests/unit/test_neso_h_mechanism.py`: T-F1 (byte-unchanged against the base golden; the only
  generated additions are the five families), T-F2/T-F3 (`alt_formats` shape and typing, the loud
  `UnparsedDatetimeError`), T-F4 (V-21; the committed group), T-F5 (the pre-check's sweep, and twelve
  seeded reconcile round trips equal with and without it), T-F6 (group overlap: contiguous, served
  and reported, one checked member, adjudication, unreadable output, no query caching a scan).
- `tests/unit/test_neso_h_records.py`: T-F7 (revised month: whole-capture supersession, as-of, SQL
  and Polars parity), T-F8 (month-edge duplicates, a re-upload under a new UUID), T-F9 (every
  rendering through its committed record, fixtures cut byte-exact from the 2026-10-08 bronze; ZIP
  member equal to the same bytes as CSV; October 2014 failed and adjudicated), T-F10 (failure
  accounting), T-F11 (registry, ledger and eligibility pins), T-F12 (the three typed format
  boundaries, clean and duplicated).
- `tests/integration/test_neso_transform_memory_gate.py`: the 5-vs-20 gate through `zip_member`.

## Residuals

- **FM-10:** the December 2014 (R1) to January 2015 (R2) edge cannot be overlap-checked until R1's
  zone is documented; the hold's question names it.
- **FM-14:** a month re-uploaded under a new resource UUID serves the same instants twice; both are
  served and reported `overlap` (no vendor precedence, ADR-039).
- **FM-15:** a new month needs a registry edit before it is fetched or transformed; coverage reports
  it missing meanwhile.
- The `zip_member` reader decompresses each member twice (inventory, then read): CPU only.
- R1's overlap check takes the exact bucketed path: day-first strings do not sort by time.
- Whether a reading is an instantaneous sample or a one-second aggregate is undocumented by NESO;
  the docs wave labels it.
