---
phase: F7
slug: stack-model-data-infrastructure
status: ready
created: 2026-05-05
depends_on:
  - F0   # Bitemporal pattern in BaseSilverTransformer
  - F1
  - F3   # GridflowDataSource and Protocol
requirements:
  - F7-BITEMP-01
  - F7-BITEMP-02
  - F7-APPEND-01
  - F7-APPEND-02
  - F7-APPEND-03
  - F7-PARTITION-01
  - F7-PARTITION-02
  - F7-MANUAL-01
  - F7-MANUAL-02
  - F7-MANUAL-03
  - F7-REINGEST-01
---

# F7 — Stack Model Data Infrastructure

## Objective

Phase F7 prepares the data foundation required by the stack model in F8. It
delivers three concrete capabilities. First, the bitemporal column pattern
established in F0 is extended to the four datasets the stack model consumes:
REMIT outage messages, BM unit reference data, ENTSO-E installed capacity per
unit, and Elexon two-to-fourteen-day generation availability. Second, append-
only revision storage is introduced at the silver layer for the two of these
datasets where revisions materially affect modelling — REMIT, where outage
messages are revised as participants update their notifications, and FOU2T14D,
where forward availability forecasts evolve as new information becomes
available. Third, a `ManualCommodityPriceSource` adapter is added to
`gridflow_models` to provide gas, coal, and carbon prices from a hand-curated
CSV. This is the only dataset class the stack model requires that gridflow
does not provide.

After F7 completes, the stack model can be specified entirely in F8 with no
further data infrastructure work. The fundamentals price model in F9 builds
on top of F8 without additional data dependencies.

## Phase Boundary

F7 spans both the gridflow repository and the gridflow_models repository. The
work falls into two independent workstreams that can be executed in parallel
or sequentially as resources allow.

**Workstream A (gridflow):** extends `BaseSilverTransformer` with an
`APPEND_ONLY` flag and run-suffixed filenames; modifies the REMIT transformer
to preserve revisions; sets `DATASET_VERSION` on the four stack datasets; and
re-ingests them from existing bronze. Results in a `summary.md` for F7-A in
the gridflow planning directory.

**Workstream B (gridflow_models):** adds the `ManualCommodityPriceSource`
adapter, extends `GridflowDataSource.fetch()` to accept per-dataset partition
columns for the `QUALIFY` clause, and provides a sample
`data/manual/commodities.csv` with realistic GB market data covering 2022
through 2026. Includes property tests for the new adapter.

F7 does not cover the stack model itself, the dispatch optimization, or any
estimator code. Those begin in F8. F7 also does not introduce a real-time
commodity price connector — that is deferred indefinitely until the manual
workflow proves insufficient.

## Workstream Detail

### Workstream A — gridflow bitemporal extension

The four stack datasets have markedly different revision behaviours, which
informs whether append-only storage is required.

| Dataset | Revision pattern | Treatment |
|---|---|---|
| `elexon/remit` | Heavy revisions: outage messages get updated `revision_number` as participants amend availability windows. Critical to preserve. | Append-only with `mrid` as `QUALIFY` partition. |
| `elexon/fou2t14d` | Forward forecasts: each daily run produces revised availability for D+2 through D+14. Revisions are material for backtesting. | Append-only with `(event_time, fuel_type)` as `QUALIFY` partition. |
| `elexon/bmunits_reference` | Static reference: BM unit ownership and capacity change rarely. Each run replaces the full universe. | Single snapshot; bitemporal columns only. |
| `entsoe/installed_capacity_units` | Annual updates: ENTSO-E publishes installed capacity once per year per unit. | Single snapshot; bitemporal columns only. |

The current REMIT silver transformer in
`src/gridflow/silver/elexon/remit.py` deduplicates with
`df.unique(subset=["mrid"], keep="last")`, which discards revision history.
F7 removes this deduplication entirely — every revision is kept in silver,
and the data adapter applies `QUALIFY` at read time to pick the most recent
revision available at the requested `as_of`.

### Workstream B — ManualCommodityPriceSource

The CSV format is bitemporal-aware and supports multiple commodities in a
single file:

```csv
date,commodity,price,currency,units,available_at,source
2024-01-15,natural_gas_nbp_da,95.50,GBp,therm,2024-01-15T17:30:00Z,ICE settlement
2024-01-15,natural_gas_ttf_da,32.40,EUR,MWh,2024-01-15T17:30:00Z,ICE settlement
2024-01-15,coal_api2_da,110.25,USD,tonne,2024-01-15T17:30:00Z,ICE settlement
2024-01-15,eua_dec_front,67.80,EUR,tonne,2024-01-15T17:30:00Z,ICE settlement
```

The `available_at` column records when each price became publicly available
— typically ICE settlement at 17:30 UK time for end-of-day prices. The
`ManualCommodityPriceSource` adapter implements `BitemporalDataSource` and
treats each commodity as a separate logical dataset addressable as
`"manual/<commodity>"`. The CSV file path is supplied as a constructor
argument with a default of `Path("data/manual/commodities.csv")` resolved
relative to the project root, mirroring the constructor-injection pattern
used by `GridflowDataSource(gridflow_data_dir)`. F7 does not introduce a
new `configs/settings.yaml` field; if a downstream consumer wants
config-driven path injection, they wire it at the call site.

## Canonical References

The following files must be reviewed before execution:

- `src/gridflow/silver/base.py` — `BaseSilverTransformer.run()` to be extended
- `src/gridflow/silver/elexon/remit.py` — REMIT transformer to be modified
- `src/gridflow/silver/elexon/bmunits.py` — reference data pattern (`BMUnitsTransformer`; its `dataset` attribute is `"bmunits_reference"` but the module file is `bmunits.py`)
- `src/gridflow/silver/elexon/fou2t14d.py` — generation availability transformer
- `src/gridflow/silver/entsoe/installed_capacity_units.py` — ENTSO-E capacity
- `.planning/phases/F0-bitemporal-fundamentals-datasets/F0-PLAN.md` — pattern reference
- `src/gridflow_models/data/gridflow_source.py` — `QUALIFY` extension target
- `src/gridflow_models/data/source.py` — `BitemporalDataSource` Protocol
- `docs/endpoints/elexon.md` — REMIT and FOU2T14D schemas
- `docs/ARCHITECTURE.md` §17.4 — append-only design
- `CLAUDE.md` — atomic write convention, parameterized SQL rule

## Requirements

| ID | Description | Acceptance |
|---|---|---|
| F7-BITEMP-01 | The four stack datasets receive `event_time`, `available_at`, `source_run_id`, `dataset_version` columns automatically via `BaseSilverTransformer`. | Property test against silver fixtures for each transformer. |
| F7-BITEMP-02 | `DATASET_VERSION` is set explicitly on each of the four stack transformers. | Static check in test. |
| F7-APPEND-01 | When `APPEND_ONLY = True`, the transformer writes to a run-suffixed filename rather than overwriting. Two consecutive runs produce two distinct files in the same partition directory. | Integration test with two fixture runs. |
| F7-APPEND-02 | The REMIT transformer no longer deduplicates by `mrid`; every revision is preserved in silver. | Unit test with multiple revisions of the same `mrid`. |
| F7-APPEND-03 | Default `APPEND_ONLY = False` preserves existing F0 atomic-replace behaviour for non-revision datasets. | Existing F0 tests continue to pass. |
| F7-PARTITION-01 | `GridflowDataSource.fetch(latest_only=True, partition_columns=["mrid"])` returns the most recent revision per `mrid` available at `as_of`. | Unit test with multi-revision Parquet fixture. |
| F7-PARTITION-02 | When `partition_columns` is omitted with `latest_only=True`, the existing `event_time` partition is used (backward compatibility). | Existing F3 tests continue to pass. |
| F7-MANUAL-01 | `ManualCommodityPriceSource` satisfies `isinstance(source, BitemporalDataSource)`. | Unit test. |
| F7-MANUAL-02 | `ManualCommodityPriceSource.fetch("manual/natural_gas_nbp_da", ...)` returns rows for that commodity only, with `available_at <= as_of` filter applied. | Unit test with mixed-commodity CSV fixture. |
| F7-MANUAL-03 | A property test confirms that for ANY `as_of` value, no row in the returned DataFrame has `available_at > as_of`. | Hypothesis property test. |
| F7-REINGEST-01 | The four stack datasets are re-ingested from existing bronze with accurate historical `available_at` values reconstructed from bronze sidecars. | Integration test confirms row counts and `available_at` ranges. |

## Architectural Decisions

The four ADRs below should be recorded in `docs/DECISION_LOG/` of the
respective repositories before execution begins. Each captures the
rationale and trade-offs of a non-obvious design choice.

**ADR-017 — `APPEND_ONLY` is a class attribute on `BaseSilverTransformer`**

Most datasets in gridflow do not require revision history. The atomic-
replace pattern from F0 is correct for them. Adding append-only behaviour
universally would create file proliferation and complicate read paths
without providing value. A class attribute on each transformer subclass
gives per-dataset opt-in: `APPEND_ONLY: ClassVar[bool] = True` for REMIT
and FOU2T14D, default `False` everywhere else. The `BaseSilverTransformer`
write logic branches on this flag without requiring per-transformer code
changes elsewhere.

**ADR-018 — Append-only uses run-timestamp filenames, not row-level merging**

Two implementations were considered. The first reads existing rows for the
target partition, merges new rows with old based on a business key, and
writes the merged result atomically. The second writes each run to a unique
filename and lets the read path apply deduplication. The second is
substantially simpler. It avoids the read-merge-write race condition that
the first approach introduces, eliminates the need for a per-dataset merge
key configuration in the writer, and produces self-contained Parquet files
that are inspectable in isolation. The cost is read-time complexity — a
`QUALIFY` clause is required for "what was current at as_of" queries — but
that complexity already lives in the data layer from F3.

**ADR-019 — `QUALIFY` partition columns are per-dataset configuration**

The F3 implementation of `latest_only` partitions by `event_time` only,
which is correct for datasets like system prices where each settlement
period has at most one revision lineage. REMIT requires partitioning by
`mrid` because the same outage message is revised across many `event_time`
values. FOU2T14D requires partitioning by `(event_time, fuel_type)` because
each daily run revises forward availability for many delivery periods. The
adapter accepts a `partition_columns: list[str] | None = None` parameter
on `fetch()`. When supplied, it replaces the default `[event_time]`. When
omitted, the F3 behaviour is preserved unchanged.

**ADR-020 — `ManualCommodityPriceSource` is an adapter in gridflow_models, not a gridflow connector**

Commodity prices belong to a fundamentally different domain than power
market data. Vendor licensing, update cadences, and request authentication
patterns all differ from gridflow's existing connectors. Building a real
ICE or EEX connector requires legal and engineering investment that is not
justified before the stack model has demonstrated commercial value. The
manual CSV approach delivers what F8 needs in days rather than months,
keeps the dependency local and auditable, and preserves the option to
upgrade to a real connector later behind the same `BitemporalDataSource`
Protocol without changing any consumer code.

---

## Tasks

The tasks are split into Workstream A (gridflow changes) and Workstream B
(gridflow_models changes). Workstream A tasks are prefixed `A` and execute
in the gridflow repository. Workstream B tasks are prefixed `B` and execute
in gridflow_models. The two workstreams have no execution dependency between
them, though the integration test in B5 requires both to be complete.

### Workstream A — gridflow

<task type="auto" tdd="true">
  <name>Task A1 — RED: tests for bitemporal columns and append-only on stack datasets</name>
  <read_first>
    - src/gridflow/silver/base.py
    - src/gridflow/silver/elexon/remit.py
    - src/gridflow/silver/elexon/bmunits.py
    - src/gridflow/silver/elexon/fou2t14d.py
    - src/gridflow/silver/entsoe/installed_capacity_units.py
    - tests/property/test_bitemporal_columns.py  (F0 reference)
  </read_first>
  <files>
    - tests/property/test_bitemporal_stack_datasets.py
    - tests/integration/test_append_only_writes.py
    - tests/unit/test_remit_revision_preservation.py
  </files>
  <action>
Write three test files that fail with `ImportError` or `AttributeError` until
the corresponding implementation tasks complete.

The first test file extends the F0 bitemporal property tests to cover the
four stack datasets. It instantiates each transformer against a fixture
bronze directory and asserts that the silver output contains
`event_time`, `available_at`, `source_run_id`, and `dataset_version` columns
with correct types. It also asserts the per-transformer `DATASET_VERSION`
class attribute is set to `"1.0.0"` (or higher when intentionally bumped).

The second test file verifies append-only behaviour. The test runs the
REMIT transformer twice against the same bronze fixture but with different
mock run identifiers, then enumerates the resulting Parquet files in the
silver directory. Two distinct files must exist, both with the run
identifier in their filename, and a third assertion verifies that the
union of rows across the two files contains revisions that would have
been discarded under the previous `keep="last"` deduplication.

The third test file covers REMIT revision preservation specifically. It
constructs a bronze fixture with three messages: `MSG-001 revision_number=1`,
`MSG-001 revision_number=2`, and `MSG-002 revision_number=1`. After
transformation, the silver output must contain three rows, not two — the
F0 deduplication that would have discarded `MSG-001 revision=1` must no
longer execute.
  </action>
</task>

<task type="auto" tdd="true">
  <name>Task A2 — GREEN: extend BaseSilverTransformer with APPEND_ONLY flag</name>
  <read_first>
    - src/gridflow/silver/base.py
    - tests/integration/test_append_only_writes.py
  </read_first>
  <files>
    - src/gridflow/silver/base.py
  </files>
  <action>
Modify `BaseSilverTransformer` to accept an `APPEND_ONLY` class attribute
with default `False`. The `run()` method's write logic branches on this
flag.

When `APPEND_ONLY = False`, behaviour matches F0: the silver writer
constructs the path as `<dataset>_<date>.parquet` and uses atomic replace
via `os.replace()`. Existing F0 tests continue to pass without modification.

When `APPEND_ONLY = True`, the writer constructs the path as
`<dataset>_<date>_run<RUN_TIMESTAMP>.parquet` where `RUN_TIMESTAMP` is the
ISO-8601 representation of the `available_at` datetime computed earlier in
the method (with colons and `+` replaced by hyphens for filename safety).
Using `available_at` rather than a fresh `datetime.now(UTC)` is deliberate:
during `--reingest`, `available_at` is reconstructed from bronze sidecar
metadata, so two re-ingest runs over the same date produce identical
filenames and the second pass overwrites cleanly via `os.replace()` (idempotent
re-ingest). For live runs (where `available_at = datetime.now(UTC)`), each
run produces a distinct timestamp and therefore a distinct file.

The writer does not attempt to delete prior files; both old and new files
coexist in the partition directory across distinct `available_at` values.
The existing atomic write to a temporary file followed by `os.replace()`
continues to apply, ensuring readers never see a partial file.

A docstring on `APPEND_ONLY` explains the trade-off: most datasets should
keep the default; only revision-publishing datasets opt in. The docstring
references ADR-018 in `docs/DECISION_LOG/`.
  </action>
</task>

<task type="auto" tdd="true">
  <name>Task A3 — GREEN: modify REMIT transformer to preserve revisions</name>
  <read_first>
    - src/gridflow/silver/elexon/remit.py
    - tests/unit/test_remit_revision_preservation.py
  </read_first>
  <files>
    - src/gridflow/silver/elexon/remit.py
  </files>
  <action>
Make two modifications to `REMITTransformer`. First, set
`APPEND_ONLY: ClassVar[bool] = True` and
`DATASET_VERSION: ClassVar[str] = "2.0.0"`. The version bump from `1.0.0`
to `2.0.0` reflects the schema-affecting change: prior to F7, silver
contained one row per `mrid`; from F7 onward, silver contains one row per
`(mrid, revision_number, available_at)`.

Second, remove the line `df = df.unique(subset=["mrid"], keep="last")`.
Every revision is now preserved. Downstream consumers that want only the
latest revision use `GridflowDataSource.fetch(latest_only=True,
partition_columns=["mrid"])` rather than relying on the silver layer to
deduplicate.

Update the transformer's docstring to document the new semantics: silver
preserves all revisions; latest-revision queries are a read-time concern.
Add a brief reference to the F7 phase summary for context.

**`ingested_at` column policy:** REMIT currently sets an `ingested_at`
column to `datetime.now(UTC)` at transform time. After F7 this overlaps
semantically with the bitemporal `available_at` column (which is sidecar
time during re-ingest, transform time otherwise). Keep `ingested_at` for
backward compatibility with downstream consumers, but add a one-line
docstring note clarifying that `available_at` is the authoritative
publication timestamp and `ingested_at` is the local processing timestamp.
Do not remove the column in F7 — that is a separate cleanup phase.
  </action>
</task>

<task type="auto" tdd="false">
  <name>Task A4 — Set DATASET_VERSION and APPEND_ONLY on the four stack transformers</name>
  <files>
    - src/gridflow/silver/elexon/remit.py  (already done in A3)
    - src/gridflow/silver/elexon/bmunits.py
    - src/gridflow/silver/elexon/fou2t14d.py
    - src/gridflow/silver/entsoe/installed_capacity_units.py
  </files>
  <action>
Set the appropriate class attributes on the three remaining transformers.
For `bmunits.py` (the `BMUnitsTransformer` whose dataset string is
`"bmunits_reference"`) and `installed_capacity_units.py`, set
`DATASET_VERSION: ClassVar[str] = "1.0.0"` only; `APPEND_ONLY` defaults
to `False`. These datasets do not publish revisions in any practical sense
— BM unit reference is a static universe rebuild on each run, and ENTSO-E
installed capacity changes annually with full retransmission.

For `fou2t14d.py`, set both `DATASET_VERSION = "1.0.0"` and
`APPEND_ONLY = True`. FOU2T14D publishes revised forward availability
forecasts daily; preserving revisions enables point-in-time backtesting
of supply margin models. Unlike REMIT, the dataset version does not bump
to `2.0.0` because the existing FOU2T14D transformer does not deduplicate
— it merely now writes to run-suffixed filenames.

**`ingested_at` column policy:** `fou2t14d.py` and
`installed_capacity_units.py` already set an `ingested_at = datetime.now(UTC)`
column at transform time. Keep these for backward compatibility; do not
remove. Add a one-line docstring note clarifying that `available_at` is
the authoritative bitemporal publication timestamp and `ingested_at` is
the local processing timestamp, and the two diverge under `--reingest`.

No transformer body code requires modification beyond setting the class
attributes and the docstring note. The `BaseSilverTransformer` change in
A2 handles the file naming and column injection automatically.
  </action>
</task>

<task type="auto" tdd="false">
  <name>Task A5 — Re-ingest the four stack datasets</name>
  <action>
Run, for the available historical date range:

```
gridflow transform elexon remit --reingest --start 2022-01-01 --end 2026-05-04
gridflow transform elexon bmunits_reference --reingest --start 2022-01-01 --end 2026-05-04
gridflow transform elexon fou2t14d --reingest --start 2022-01-01 --end 2026-05-04
gridflow transform entsoe installed_capacity_units --reingest --start 2022-01-01 --end 2026-05-04
```

(`bmunits_reference` accepts explicit `--start`/`--end` to ensure the full
historical bronze is re-transformed rather than just the recent
`default_lookback_hours` window. `BMUnitsTransformer.read_bronze` rglobs the
dataset directory regardless, but explicit dates make the intent obvious.)

For REMIT and FOU2T14D, each historical date produces a single run-suffixed
file (since each historical date has been ingested only once historically).
Subsequent live runs will produce additional files as new revisions arrive.

For BM units reference and ENTSO-E installed capacity, the existing single-
file pattern continues; only the bitemporal columns are added.

After re-ingest, run the property tests from Task A1 against the live silver
layer and document row counts in
`.planning/phases/F7-stack-model-data-infrastructure/F7-A-RESULTS.md`. For
REMIT specifically, document the row count increase relative to the pre-
F7 silver — the difference represents the historical revisions that were
previously discarded.
  </action>
</task>

### Workstream B — gridflow_models

<task type="auto" tdd="true">
  <name>Task B1 — RED: tests for ManualCommodityPriceSource and partition columns</name>
  <read_first>
    - src/gridflow_models/data/source.py
    - src/gridflow_models/data/gridflow_source.py
    - src/gridflow_models/data/memory_source.py
    - tests/unit/data/test_dataset_filter.py  (F6 reference for adapter test pattern)
    - tests/property/test_data_source_as_of.py  (F3 property test reference)
  </read_first>
  <files>
    - tests/unit/data/test_manual_source.py
    - tests/unit/data/test_partition_columns.py
    - tests/property/test_manual_source_as_of.py
    - tests/conftest.py
  </files>
  <action>
Write three test files. The first contains unit tests for
`ManualCommodityPriceSource`: instantiation against a tmp_path CSV fixture,
filtering by commodity name through the dataset parameter, filtering by
`event_time` range, filtering by `available_at <= as_of`, and protocol
conformance via `isinstance(source, BitemporalDataSource)`.

The second test file covers the `partition_columns` extension to
`GridflowDataSource.fetch()`. The fixture creates a Parquet file with
multiple revisions of the same `mrid` and varying `available_at` values.
The test asserts that calling `fetch(latest_only=True,
partition_columns=["mrid"])` returns one row per `mrid` — the row with
the highest `available_at` not exceeding the requested `as_of`. A
companion test confirms that omitting `partition_columns` while passing
`latest_only=True` still uses `event_time` as the partition (backward
compatibility with F3 behaviour).

The third file is a Hypothesis property test that mirrors the existing
F3 universal as_of invariant: for arbitrary `as_of` values, no row in
the returned DataFrame from `ManualCommodityPriceSource.fetch()` may
have `available_at > as_of`.

Add the fixture to `tests/unit/data/conftest.py` (a scoped conftest, not
the top-level `tests/conftest.py`) so the commodity CSV fixture is loaded
only by data-adapter tests. The fixture writes a small commodity CSV to
tmp_path with three commodities (`natural_gas_nbp_da`, `coal_api2_da`,
`eua_dec_front`) and three dates worth of data, with realistic
`available_at` timestamps at 17:30 UK time on the price date.
  </action>
</task>

<task type="auto" tdd="true">
  <name>Task B2 — GREEN: extend GridflowDataSource and Protocol with partition_columns</name>
  <read_first>
    - src/gridflow_models/data/source.py
    - src/gridflow_models/data/gridflow_source.py
    - src/gridflow_models/data/memory_source.py
    - src/gridflow_models/data/parquet_source.py
    - tests/unit/data/test_partition_columns.py
  </read_first>
  <files>
    - src/gridflow_models/data/source.py
    - src/gridflow_models/data/gridflow_source.py
    - src/gridflow_models/data/memory_source.py
    - src/gridflow_models/data/parquet_source.py
  </files>
  <action>
Add a `partition_columns: list[str] | None = None` parameter to the
`fetch()` method on the `BitemporalDataSource` Protocol and on each
adapter implementation. The parameter is consulted only when
`latest_only=True`. When `partition_columns` is `None` and `latest_only`
is `True`, the existing F3 behaviour applies: partition by `event_time`,
order by `available_at DESC`. When `partition_columns` is provided, it
replaces the partition list while the order clause remains
`available_at DESC`.

**Concrete signature/call-site changes required (all four files):**

1. `src/gridflow_models/data/source.py` — add the
   `partition_columns: list[str] | None = None` parameter to the
   `BitemporalDataSource.fetch` Protocol signature.
2. `src/gridflow_models/data/gridflow_source.py` — add the parameter to
   `GridflowDataSource.fetch`, thread it into `_build_sql` (the helper
   that constructs the QUALIFY clause), and to `_latest_rows` if used.
3. `src/gridflow_models/data/memory_source.py` — add the parameter to
   `InMemoryBitemporalSource.fetch` and update the existing
   `_latest_rows` helper (currently hard-codes `[EVENT_TIME]`) to accept
   `partition_columns: list[str] | None` and substitute it when non-None.
4. `src/gridflow_models/data/parquet_source.py` — add the parameter to
   `ParquetBitemporalSource.fetch` and pass it through the existing
   `_latest_rows(result)` call site so the Polars adapter shares the
   same dedup helper as `memory_source`.

**SQL injection defence (gridflow_source.py).** The `QUALIFY` clause is
constructed from the `partition_columns` list. Because DuckDB does not
support parameter binding for column identifiers in the `PARTITION BY`
expression, every column name must be validated against the existing
`_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")` regex already used
for `dataset_filter`. Reuse that regex (do not duplicate it). Any column
name failing the match raises `ValueError("invalid partition column: ...")`
immediately, before the SQL is constructed.

**Polars dedup correctness (memory_source.py and parquet_source.py).**
The dedup uses
`df.sort("available_at", descending=True)
   .unique(subset=partition_columns, keep="first", maintain_order=True)`.
The `maintain_order=True` flag is mandatory: without it, Polars parallel
execution does not guarantee `keep="first"` returns the first row in the
sorted input order, and small-fixture tests will pass while production
silver scans produce non-deterministic latest-revision selection. The
existing `_latest_rows` helper at `memory_source.py` already passes
`maintain_order=True`; preserve that.

The Polars implementation is naturally safe from injection because no
SQL is involved.

All three adapters maintain backward compatibility: omitting
`partition_columns` produces the F3 behaviour exactly. Existing F3
property tests must continue to pass without modification.
  </action>
</task>

<task type="auto" tdd="true">
  <name>Task B3 — GREEN: implement ManualCommodityPriceSource</name>
  <read_first>
    - tests/unit/data/test_manual_source.py
    - tests/property/test_manual_source_as_of.py
    - src/gridflow_models/data/source.py
    - src/gridflow_models/data/_training_set_builder.py
    - src/gridflow_models/core/bitemporal.py
  </read_first>
  <files>
    - src/gridflow_models/data/manual_source.py
  </files>
  <action>
Implement the adapter following the established pattern of the other three
data sources. The adapter constructor signature is
`ManualCommodityPriceSource(csv_path: Path | str = Path("data/manual/commodities.csv"))`
and reads the file into a Polars DataFrame on instantiation. The CSV must
contain the columns `date`, `commodity`, `price`, `currency`, `units`,
`available_at`, and `source`. The adapter renames `date` to `event_time`
and casts both timestamp columns to UTC-aware `Datetime`.

The `dataset` parameter to `fetch()` follows the format
`"manual/<commodity_name>"`. The adapter splits on `/`, validates that
the prefix is `manual`, and uses the suffix as the commodity filter on
the in-memory DataFrame. An unknown commodity returns an empty DataFrame
rather than raising an error, matching the behaviour of the other
adapters when a dataset is not found.

The `fetch_training_set` method delegates to the same
`_build_training_set` helper used by the in-memory adapter, ensuring
consistent join semantics across all adapters.

Implement the adapter as approximately seventy lines including
docstring and type hints. The CSV reading uses
`pl.read_csv(path, try_parse_dates=True)` followed by explicit casts of
`event_time` and `available_at` to ensure UTC timezone awareness, matching
the schema validation enforced by `TrainingSet.__post_init__`.

Also add `data/manual/commodities.csv` with realistic GB market data
covering 2022-01-01 through 2026-05-04 for the four commodities listed
in the workstream description. The file should contain approximately
6,000 rows (4 commodities × ~1,500 trading days). Use historically plausible
prices: NBP gas roughly 60-300 GBp/therm depending on year, API2 coal
roughly 100-400 USD/tonne, EUA carbon roughly 50-100 EUR/tonne. Document
the data source in a comment header — this CSV is hand-curated for the
project portfolio and is not derived from licensed vendor feeds.
  </action>
</task>

<task type="auto" tdd="false">
  <name>Task B4 — Wire imports and update STATE.md</name>
  <files>
    - src/gridflow_models/data/__init__.py
    - .planning/STATE.md
    - docs/DECISION_LOG/ADR-017-append-only-class-attribute.md
    - docs/DECISION_LOG/ADR-018-append-only-run-suffixed-files.md
    - docs/DECISION_LOG/ADR-019-qualify-partition-columns-configurable.md
    - docs/DECISION_LOG/ADR-020-manual-commodity-source-not-connector.md
  </files>
  <action>
Add `ManualCommodityPriceSource` to the public API exposed by
`data/__init__.py`. The four ADRs documented in the architectural decisions
section above are committed to `docs/DECISION_LOG/`, each approximately
twenty lines covering context, decision, and consequences. Two of the
ADRs (017 and 018) describe gridflow changes and may be cross-referenced
to gridflow's `docs/DECISION_LOG/` rather than duplicated, depending on
the developer's preference for documentation locality.

Update `.planning/STATE.md` to mark F7 complete and identify F8 as the
next active phase:

```markdown
## Active phase
**F8 — Stack model and dispatch optimization**

## Completed phases
- F1 — Project bootstrap
- F2 — Core primitives
- F3 — Data access layer
- F4 — Validation framework
- F5 — Demand forecast model
- F6 — Wind and solar generation forecast models
- F7 — Stack model data infrastructure
```
  </action>
</task>

<task type="auto" tdd="false">
  <name>Task B5 — Integration test against real gridflow stack data</name>
  <read_first>
    - src/gridflow_models/data/gridflow_source.py
    - tests/integration/test_gridflow_source_integration.py  (F3 pattern)
  </read_first>
  <files>
    - tests/integration/test_stack_data_integration.py
  </files>
  <action>
Write an integration test that exercises the full F7 capability set against
real gridflow data. The test is gated on `GRIDFLOW_DATA_DIR` existing and is
marked `@pytest.mark.integration`, excluded from CI.

The test fetches REMIT data for a known historical date with multiple
revisions, calls `fetch(latest_only=True, partition_columns=["mrid"])`,
and asserts that the row count equals the distinct `mrid` count rather
than the total revision count. A second assertion confirms that for each
returned `mrid`, the `available_at` value is the maximum among that
`mrid`'s revisions.

A companion test fetches a date of FOU2T14D data with the same `latest_only`
pattern and `partition_columns=["event_time", "fuel_type"]` to verify the
multi-column partition case.

A third test exercises `ManualCommodityPriceSource` against the committed
`data/manual/commodities.csv`, confirming that prices are returned for
the requested date range and commodity, and that the `available_at`
filter is correctly applied.

Document results in `F7-B-RESULTS.md` alongside row counts and any
observed schema surprises.
  </action>
</task>

---

## Verification

The non-integration suite runs without modification on CI. After Workstream A
completes:

```bash
uv run pytest tests/property/test_bitemporal_stack_datasets.py -v
uv run pytest tests/unit/test_remit_revision_preservation.py -v
uv run pytest tests/integration/test_append_only_writes.py -v
```

After Workstream B completes:

```bash
uv run pytest tests/unit/data/test_manual_source.py \
              tests/unit/data/test_partition_columns.py \
              tests/property/test_manual_source_as_of.py -v
uv run mypy src/gridflow_models/
uv run ruff check src/ tests/
```

Integration tests, gated on real gridflow data:

```bash
uv run pytest tests/integration/test_stack_data_integration.py -v -m integration
```

## Definition of Done

All eleven F7 requirements are covered by passing tests across both
workstreams. The four stack datasets contain bitemporal columns in their
silver output; REMIT preserves revisions; FOU2T14D writes append-only
files; the `partition_columns` extension to `GridflowDataSource` correctly
applies multi-column `QUALIFY` partitions; `ManualCommodityPriceSource`
satisfies the protocol and the universal as_of property; the four ADRs are
committed to `docs/DECISION_LOG/`; and the four stack datasets are
re-ingested with documented row counts.

The F7 commit messages follow the existing convention. Workstream A commits
in the gridflow repository as `feat: F7 stack-dataset bitemporal upgrade and
append-only support`. Workstream B commits in gridflow_models as
`feat: F7 ManualCommodityPriceSource and partition_columns extension`.

## What Comes Next

**F8 — Stack Model and Dispatch Optimization.** F8 builds the merit-order
supply curve from the four datasets prepared in F7 plus the manual
commodity prices, computes plant-level marginal costs, and produces a
`SupplyCurve` value object addressable by `(price, cumulative_capacity)`
pairs. The stack model is constructive rather than statistical — it has
no `fit_one_fold` method — so F8 also introduces a parallel
`ConstructiveModel` Protocol alongside the existing `ProbabilisticEstimator`.
F9 then combines the F5 and F6 demand and renewable forecasts with the F8
stack to produce probabilistic SMP forecasts via residual-demand-and-stack
intersection.
