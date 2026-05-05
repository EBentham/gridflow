# gridflow - Current Milestone Requirements

## Milestone v0.8 Fundamentals Model Silver Foundations

### Silver Bitemporal Contract

- [x] **F0-BITEMP-01**: Every successful silver write adds `event_time`, `available_at`, `source_run_id`, and `dataset_version` before Parquet and CSV outputs are written.
- [x] **F0-BITEMP-02**: `event_time` is derived from the dataset's semantic delivery timestamp, including settlement-date/settlement-period conversion where no direct timestamp column exists, with target-date fallback for static/reference datasets.
- [x] **F0-BITEMP-03**: Normal transforms stamp `available_at` once at silver write time using a UTC-aware timestamp, close to but independent from `pipeline_runs.completed_at`.
- [x] **F0-BITEMP-04**: `dataset_version` comes from a transformer `DATASET_VERSION` class attribute, with the base transformer providing a safe default.
- [x] **F0-ISSUE-01**: In-scope forecast-style datasets (`elexon/ndf` and `elexon/windfor`) expose `issue_time` from publish-time metadata when the bronze payload contains it.

### Run Context And Re-Ingest

- [x] **F0-RUN-01**: CLI and script silver execution paths pass the active `PipelineRunTracker.run_id` into `BaseSilverTransformer.run()` so each silver row can be traced back to the producing run.
- [x] **F0-RUN-02**: Direct transformer calls without an active run still work by using an explicit synthetic ad hoc run id.
- [x] **F0-REINGEST-01**: A `--reingest` silver path reconstructs historical `available_at` from bronze `raw_*.meta.json` sidecars when available, falling back safely and visibly when metadata is missing.
- [x] **F0-REINGEST-02**: The five demand-forecast foundation dataset re-transform commands are documented for `elexon/indo`, `elexon/fuelhh`, `elexon/windfor`, `elexon/ndf`, and `open_meteo/historical`; local historical execution is blocked by absent `data/bronze/` partitions and recorded in `F0-RESULTS.md`.

### Verification And Handoff

- [x] **F0-VERIFY-01**: Unit and integration tests prove bitemporal columns, UTC awareness, run-id traceability, re-ingest sidecar timestamps, and in-scope transformer versioning.
- [x] **F0-VERIFY-02**: DuckDB silver views can query the new bitemporal columns for re-transformed datasets without breaking older silver Parquet files.
- [x] **F0-VERIFY-03**: `F0-RESULTS.md` records test commands, DuckDB checks, caveats, and the exact dataset/source naming choices needed by `gridflow_models`.

## Future Requirements

### Stack Model Data Foundations

- **F7-BITEMP-01**: Stack-model datasets receive the same bitemporal upgrade when the stack model begins consuming them.
- **F7-COMMOD-01**: `gridflow_models` can read manually curated commodity prices with bitemporal `available_at` semantics.
- **F7-REVISION-01**: Datasets with material revisions, starting with imbalance/system-price style data, can use append-only revision storage.

### Models Project

- **F1-MODELS-01**: `gridflow_models` is scaffolded as a separate Python project after F0 proves the silver bitemporal contract.
- **F2-MODELS-01**: `gridflow_models` core primitives enforce point-in-time training-set construction using the F0 columns.

## Out of Scope

| Feature | Reason |
|---------|--------|
| Creating the `gridflow_models` repository | This milestone only prepares `gridflow` silver data for that project. |
| Re-ingesting every silver dataset | F0 is limited to the first five datasets needed for the demand-forecast foundation. |
| Append-only revision storage | Deferred until datasets with material revisions are consumed by stack/imbalance work. |
| DuckDB latest-as-of view helpers | Useful, but not needed until `gridflow_models` data access starts querying revisions. |
| Commodity price connector | The F0 work is source-agnostic silver lineage; manual commodity data is later stack-model scope. |
| Scheduled live endpoint monitoring | Still a cross-source follow-up, not a blocker for bitemporal silver lineage. |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| F0-BITEMP-01 | F0 | Completed |
| F0-BITEMP-02 | F0 | Completed |
| F0-BITEMP-03 | F0 | Completed |
| F0-BITEMP-04 | F0 | Completed |
| F0-ISSUE-01 | F0 | Completed |
| F0-RUN-01 | F0 | Completed |
| F0-RUN-02 | F0 | Completed |
| F0-REINGEST-01 | F0 | Completed |
| F0-REINGEST-02 | F0 | Completed with missing-bronze caveat |
| F0-VERIFY-01 | F0 | Completed |
| F0-VERIFY-02 | F0 | Completed |
| F0-VERIFY-03 | F0 | Completed |

**Coverage:**
- v0.8 requirements: 12 total
- Mapped to phases: 12
- Unmapped: 0

---
*Requirements defined: 2026-05-05*
*Last updated: 2026-05-05 after F0 execution and verification*
