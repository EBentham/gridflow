---
phase: F0
slug: bitemporal-fundamentals-datasets
status: complete
created: 2026-05-05
milestone: v0.8
requirements:
  - F0-BITEMP-01
  - F0-BITEMP-02
  - F0-BITEMP-03
  - F0-BITEMP-04
  - F0-ISSUE-01
  - F0-RUN-01
  - F0-RUN-02
  - F0-REINGEST-01
  - F0-REINGEST-02
  - F0-VERIFY-01
  - F0-VERIFY-02
  - F0-VERIFY-03
---

# F0 Context - Bitemporal Upgrade for Fundamentals Datasets

## User Intent

Prepare `gridflow` silver data for the future `gridflow_models` project by adding
row-level point-in-time lineage. The immediate consumer is the first fundamentals
model path: demand forecasting for GB power-market data.

This phase stays in the existing `gridflow` repository. The separate
`gridflow_models` Python project starts after F0 is implemented and verified.

## Source Documents

- `C:\Users\Bobbo\Downloads\FUNDAMENTALS_PROGRAM_AND_F0_SPEC.md`
- `C:\Users\Bobbo\Downloads\gridflow_models_ARCHITECTURE_v3_1.md`

## Why Bitemporal

Current silver writes are atomic replacements. That is good for operational
cleanliness, but it means historical training data can change when a dataset is
re-run. Models need to know:

- `event_time`: when the delivery period or observed event occurred.
- `available_at`: when this row became queryable in our local data system.
- `source_run_id`: which pipeline run created the row.
- `dataset_version`: which transformer schema/version produced the row.
- `issue_time`: for forecast-like datasets, when the forecast was published or issued.

`gridflow_models` will use these fields to construct point-in-time training sets
and prevent leakage in validation and backtests.

## Phase Boundary

F0 covers:

- Base silver transformer changes that inject bitemporal fields before write.
- Transform run-id propagation from CLI/script run trackers into transformers.
- A re-ingest mode that reconstructs historical `available_at` from bronze
  sidecar metadata when available.
- The first five modelling foundation datasets:
  - `elexon/indo`
  - `elexon/fuelhh`
  - `elexon/windfor`
  - `elexon/ndf`
  - `open_meteo/historical`
- Tests, DuckDB sanity checks, and `F0-RESULTS.md`.

F0 does not cover:

- Creating or scaffolding `gridflow_models`.
- Re-ingesting every silver dataset.
- Append-only revision storage.
- DuckDB latest-as-of helper views.
- Commodity price data or stack-model datasets.

## Canonical Local References

- `src/gridflow/silver/base.py` - `BaseSilverTransformer.run()` and write flow.
- `src/gridflow/observability.py` - `PipelineRunTracker.run_id`.
- `src/gridflow/cli.py` - `transform`, `pipeline`, and `backfill` command paths.
- `scripts/run_pipeline.py` - direct Python runner transform path.
- `src/gridflow/storage/parquet.py` - atomic Parquet writes.
- `src/gridflow/storage/duckdb.py` - silver view registration.
- `src/gridflow/silver/elexon/indo.py` - INDO transformer.
- `src/gridflow/silver/elexon/fuelhh.py` - FUELHH transformer.
- `src/gridflow/silver/elexon/wind_forecast.py` - WINDFOR transformer.
- `src/gridflow/silver/elexon/demand_forecast.py` - NDF/NDFD transformer.
- `src/gridflow/silver/openmeteo/historical.py` - Open-Meteo historical transformer.

## Repo-Specific Corrections To The Supplied F0 Spec

1. The source name in this repo is `open_meteo`, not `openmeteo`.
2. `windfor` is implemented in `src/gridflow/silver/elexon/wind_forecast.py`.
3. `ndf` is implemented in `src/gridflow/silver/elexon/demand_forecast.py`.
4. The supplied F0 spec says no new dependencies but also asks for Hypothesis.
   This plan uses parametrized pytest/property-style checks unless we explicitly
   choose to add Hypothesis later.
5. The architecture says forecast datasets need `issue_time`. F0 includes
   `elexon/ndf` and `elexon/windfor`, so this plan treats issue-time handling as
   an explicit discussion point and requirement for those datasets where publish
   metadata exists.

## Decisions

- `available_at` for normal transforms is captured by the silver writer at write
  time using `datetime.now(UTC)`, immediately before output writes.
- `source_run_id` is passed into `BaseSilverTransformer.run()` from the active
  `PipelineRunTracker.run_id`.
- Direct transformer use without tracker context remains supported with an
  ad hoc run id.
- `dataset_version` is a class attribute on transformer classes, with the base
  class defaulting to `"1.0.0"`.
- Historical re-ingest uses bronze `raw_*.meta.json` timestamps as the best
  available conservative proxy for historical `available_at`.
- Append-only storage is deferred until a model needs revising datasets.

## Resolved Execution Decisions

1. `issue_time` is implemented for `elexon/ndf` and `elexon/windfor` when
   publish-time metadata exists.
2. Tests stayed dependency-free with parametrized pytest and integration coverage.
3. Historical re-transform commands are documented, but broad local reingest was
   not run because this workspace has no `data/bronze/` partitions.
