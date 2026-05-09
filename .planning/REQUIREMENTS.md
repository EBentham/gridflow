# gridflow - Requirements History

> v0.11 requirements archived to `.planning/milestones/v0.11-open-meteo-renewable-extension-REQUIREMENTS.md`
> Next milestone requirements will be defined by `/gsd-new-milestone`.

---

## Milestone v0.9 Vault Vendor Validation And Docs

### Vault Documentation

- [x] **V1-VAULT-01**: Each of the six active vendors has a per-dataset page at `quant-vault/30-vendors/<vendor>/datasets/<dataset_key>.md` for every active dataset in `config/sources.yaml`, using the `gridflow-dataset-spec` skill template verbatim (frontmatter, Overview, API endpoint, Working curl example, Bronze layer, Silver layer with full schema table, Implementation delta, Known gotchas). **Completed 2026-05-08:** 156 dataset pages on disk (Elexon 33, ENTSOE 48, ENTSOG 33, GIE 7, NESO 33, Open-Meteo 2).
- [x] **V1-VAULT-02**: Each vendor's `quant-vault/30-vendors/<vendor>/endpoints.md` is a complete quick-summary table covering every active dataset with dataset key, path, parameter style, and one-line description, grouped by parameter style or family. **Completed 2026-05-08.**
- [x] **V1-VAULT-03**: Each vendor's `quant-vault/30-vendors/<vendor>/README.md` resolves all existing `TODO` markers (auth method, rate limit confirmation, status URL, known gotchas) against vendor docs and live API behaviour. **Completed 2026-05-08:** zero TODO markers remain in any of the 6 vendor READMEs.
- [x] **V1-VAULT-04**: NESO existing 33 dataset pages are validated in place against live API and source code. Drift is patched, accurate content is preserved. **Completed 2026-05-08:** 12 pages classified `accurate` (frontmatter-only bump), 21 `needs_minor_patch`, 0 full rewrites.

### Live Validation

- [x] **V1-VALID-01**: Every active endpoint is hit via a live GET (or vendor-equivalent) request with reasonable parameters. Each gets PASS / FAIL / EMPTY status in `<vendor>-VALIDATION.md`. **Completed 2026-05-08:** aggregate 113 PASS / 43 EMPTY / 0 FAIL across 156 datasets.
- [x] **V1-VALID-02**: For every FAIL or EMPTY result, the `<vendor>-VALIDATION.md` records cause (deprecated path, wrong param name, wrong base URL, no data for window, requires filter, etc.) plus the raw curl command and HTTP status used. **Completed 2026-05-08:** all 43 EMPTY results have documented cause + curl evidence.
- [x] **V1-VALID-03**: Every endpoint URL pattern in `src/gridflow/connectors/<vendor>/endpoints.py` matches the official-docs URL pattern verbatim, OR the discrepancy is recorded in the dataset page's `## Implementation delta` section. Authority hierarchy (docs > fixtures > code) is honoured — code is never silently treated as ground truth. **Completed 2026-05-08:** all production-bug deltas (Elexon `freq` param-name mismatch, NESO regional silver level, REMIT/SOSO cap, schema regex, ENTSOE A09 duplication) recorded for follow-up phase work.

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
