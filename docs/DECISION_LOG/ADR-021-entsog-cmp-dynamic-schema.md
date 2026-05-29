# ADR-021 — ENTSO-G cmp_* silver schema is genuinely dynamic (no contract pin, no back-fill)

Status: Accepted (2026-05-29)

## Context

An on-disk schema-drift scan flagged two ENTSO-G silver datasets with
non-uniform Parquet schemas across their partition files:

- `entsog/cmp_unavailable_firm_capacity`: 7 files, common 37 cols, one
  outlier (`..._20260501.parquet`) at 45 cols.
- `entsog/cmp_unsuccessful_requests`: 7 files, common 43 cols, one
  outlier (`..._20260501.parquet`) at 51 cols.

The 8 columns that appear only in the outlier file are, in both
datasets:

| column | dtype | population in the outlier file |
|---|---|---|
| `booking_platform_key`        | `Null` (typeless) | 100% null |
| `booking_platform_label`      | `Null` (typeless) | 100% null |
| `booking_platform_url`        | `Null` (typeless) | 100% null |
| `interruption_calculation_remark` | `Null` (typeless) | 100% null |
| `data_set`                    | `Int64`   | ~1–3% non-null |
| `id_point_type`               | `Int64`   | ~1–3% non-null |
| `is_archived`                 | `Boolean` | ~1–3% non-null |
| `point_type`                  | `String`  | ~1–3% non-null |

`cmp_auction_premiums` and every other entsog dataset (~30) are uniform;
only these two drift.

### Mechanism (not corruption — a date-filter coincidence)

Both datasets are window datasets (`requires_dates=True` →
`date_window_dataset=True`), so
`GenericEntsogJsonTransformer.read_bronze` runs
`filter_records_to_target_date`
(`src/gridflow/silver/entsog/datetime.py`). The connector stores the
same multi-day window response under every day's partition; the filter
keeps a record when its first parseable priority timestamp
(`_record_date` walks `periodFrom → publicationDateTime → … →
capacityFrom → … → lastUpdateDateTime`) equals the target date, or when
no timestamp parses at all.

The 8 columns ride in on **archived/historical CMP records**
(`is_archived=true`, `lastUpdateDateTime` in 2026-02, `capacityFrom`
scattered across 2023–2026) that the API returns in *every* window
response. These records carry no `periodFrom`, so their effective
filter-date comes from `capacityFrom`. The number whose `capacityFrom`
lands on a given target date is coincidental: on 2026-05-01 it was
**1 of 1101 rows** (`cmp_unsuccessful_requests`) / **22 of 697 rows**
(`cmp_unavailable_firm_capacity`); on every other day it was zero, so
those days' partitions carry none of the 8 columns.

### Why this is not the indo / `published_at` case

The fix in `src/gridflow/silver/elexon/indo.py` + the back-fill in
`scripts/normalize_published_at.py` pin `published_at` as an
always-present nullable contract column and emit/fill it as typed-null.
That is honest because `published_at` is conceptually present on
**every** INDO row — it is merely absent from some bronze payloads.

The cmp_* envelope fields attach to a **record subset** (1 row in 1101),
not to the dataset. Asserting "every row has this nullable column" would
be *fabrication*, not back-fill. The `GenericEntsogJsonTransformer`
(`src/gridflow/silver/entsog/generic.py`) is schema-flexible by design:
it passes through whatever fields the API returns
(`ordered.extend(col for col in df.columns if col not in ordered)`).

### The drift does not break gridflow, and is latent for consumers

Verified on the installed DuckDB 1.5.2 against the real 7-file glob:

| read path | result |
|---|---|
| raw `read_parquet(glob, hive_partitioning=true)` — *no* `union_by_name` | succeeds (both file orders; modern DuckDB null-fills) |
| DuckDB view `union_by_name=true` (`src/gridflow/storage/duckdb.py`) | succeeds |
| Polars `missing_columns="insert"` (`src/gridflow/storage/parquet.py`) | succeeds |

These datasets are not in the `gridflow_models` read path. The
historically-reported `InvalidInputException: schema mismatch in glob`
only bites an external raw-glob consumer pinned to an older DuckDB.

### Project posture already covers this

`docs/CANONICAL_SCHEMA.yaml` marks both entries
`business_columns: { TODO_HUMAN_FILL_COLUMNS: true }`, so
`tests/integration/test_canonical_schema_alignment.py` skips them. The
canonical contract for these datasets is, by design, awaiting human
curation. CLAUDE.md: "Do not invent rate limits, endpoints, or schemas.
If it's not documented, write a TODO and stop."

## Decision

Treat the cmp_* envelope columns as **genuinely dynamic**. Specifically:

1. The stable silver contract for each dataset is its **common column
   set** (37 / 43 columns). The 8 varying columns are **not** contract
   columns — 4 are typeless all-null write-noise, 4 are sparse envelope
   fields on an archived-record subset.
2. **Do not** pin a column contract, **do not** modify
   `GenericEntsogJsonTransformer`, and **do not** back-fill / normalise
   the 12 drifted files.
3. Rely on gridflow's existing tolerant read paths (`union_by_name=true`
   in DuckDB views; `missing_columns="insert"` in `storage/parquet.py`),
   which already absorb this drift.
4. Defer any contract pin to the `docs/CANONICAL_SCHEMA.yaml` curation
   pass these two entries already await.
5. Add a regression test
   (`tests/integration/test_entsog_cmp_schema_drift.py`) that documents
   the drift as by-design and guards the tolerant-reader contract.

## Alternatives considered

**Option B — indo-style pin + back-fill.** Declare the union schema as a
contract, emit the columns as typed-null when absent, migrate the 12
files (mirroring `scripts/normalize_published_at.py`).
Rejected because:
1. *Semantic fabrication* — the columns attach to a 1/1101 record
   subset, not to every row (see Context). A typed-null fill asserts a
   contract that does not exist.
2. *Not durable* — the `GenericEntsogJsonTransformer` would still emit
   the narrow schema on any future day without a coinciding archived
   record, re-drifting against the back-filled files on the next
   ingest.
3. *Inventing schemas* — typing the 4 all-null `Null` columns
   (`booking_platform_*`, `interruption_calculation_remark`) has no
   observed data or documentation to derive from, and pinning these two
   datasets pre-empts the deliberate `TODO_HUMAN_FILL_COLUMNS` curation
   posture shared by all 155 non-Open-Meteo datasets.

**Option C — drop `pl.Null`-dtype columns in the generic transformer.**
Strip typeless all-null columns before write to remove 4 of the 8.
Rejected because it is *actively harmful*: the transformer serves ~30
datasets, and any normally-typed column that happens to be all-null in a
given day's date-filtered slice infers as `pl.Null` and would be dropped
*that day* — manufacturing drift where none exists today. The ~30
currently-uniform datasets are not guaranteed to stay null-free per
partition.

## Consequences

- The 12 drifted files are left as-is. gridflow reads them correctly
  everywhere today.
- The two datasets remain `TODO_HUMAN_FILL_COLUMNS` in
  `docs/CANONICAL_SCHEMA.yaml`; the alignment test continues to skip
  them. This ADR records *why* they are deferred, for the future
  curator.
- A new regression test pins the intended behaviour (drift expected;
  tolerant readers unify it), so a future drift scan does not get
  "fixed" by fabricating a contract, and the `union_by_name=true` /
  `missing_columns="insert"` reader tolerance cannot silently regress.
- No change to the generic transformer, so the ~30 uniform datasets are
  unaffected.

## Reversal

If an external consumer that needs a strictly uniform schema for these
datasets materialises, the honest pin (not Option B as written) is:

1. Curate `docs/CANONICAL_SCHEMA.yaml` for the two entries: declare the
   common columns plus the **4 typed** envelope columns with their
   observed dtypes (`data_set: Int64`, `id_point_type: Int64`,
   `is_archived: Boolean`, `point_type: String`); **drop** the 4
   typeless `Null` columns (zero data loss — they are 100% null).
2. Add a cmp-specific `GenericEntsogJsonTransformer` subclass that emits
   the declared typed columns as typed-null when a day's slice lacks
   them, and drops the typeless columns.
3. Run a one-time, idempotent, silver-only back-fill modelled on
   `scripts/normalize_published_at.py` (preserve bitemporal lineage;
   atomic writes via `storage/parquet.write_parquet`).
4. Remove `TODO_HUMAN_FILL_COLUMNS` so
   `test_canonical_schema_alignment.py` begins enforcing the contract,
   and supersede this ADR.
