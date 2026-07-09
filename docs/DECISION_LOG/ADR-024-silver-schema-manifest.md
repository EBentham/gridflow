# ADR-024 - Silver schema manifest

**Status:** Accepted
**Date:** 2026-07-09
**Phase:** gridflow silver schema manifest / gridflow_models PR-2

## Context

On 2026-07-09, the sibling `gridflow_models` repo hit notebook
`BinderException` failures after it re-declared gridflow silver schema
facts as frozen literals. The stopgap in gridflow_models repaired the
immediate drift, but the ownership boundary was still wrong: gridflow
creates the silver and gold relations, so gridflow must own and export
the contract that downstream model code consumes.

ADR-003 established the SDK `SELECT * EXCLUDE (...)` pattern and the
private bitemporal exclude tuple used by `GridflowClient`. That solved
the public serving methods, but it left downstream consumers with no
public way to discover the same relation names, date columns, bitemporal
columns, partition columns, or fixed-vs-dynamic schema status.

`docs/CANONICAL_SCHEMA.yaml` is useful documentation, but it is
subordinate and incomplete. It is not a runtime authority: many entries
are intentionally uncurated, dynamic families depend on payload shape,
and the live registry is populated by import side effects.

## Decision

Gridflow owns and exports the silver schema contract via
`gridflow.silver.schema_manifest`.

The module provides:

- `SilverSchemaEntry`, a frozen dataclass describing each registered
  silver relation and selected serving/gold aliases.
- `BITEMPORAL_EXCLUDE`, the public authority for bitemporal and
  partitioning columns hidden by the serving client.
- `DESIGNATED_DATE_COLS`, a reviewed central table keyed by
  `(source, dataset)`.
- `get_silver_schema_manifest()` and `silver_schema_manifest_frame()`.

The manifest is computed from the live transformer registry plus the
reviewed date-column declarations. Fixed columns are read from each
transformer's Pydantic `schema_cls`. Dynamic families remain declared as
dynamic rather than pretending to have a stable fixed list. Gold serving
rows read their SQL view projections from `src/gridflow/gold/views`.

We deliberately chose a central reviewed table for designated date
columns rather than per-transformer attributes. Date-column conventions
cross-cut source families and serving aliases, and the incident was a
review-boundary failure. Keeping the table in one module makes the
contract diff explicit in gridflow PRs.

We are not generating files, writing database tables, or making the
DuckDB catalogue the source of truth. The manifest is runtime Python API
surface backed by the same registry that writes silver.

## Consequences

`gridflow_models` can delete its duplicated literals in PR-3 and consume
the gridflow manifest directly. Until then, `GridflowClient` keeps the
private `_BITEMPORAL_EXCLUDE` alias pointing at the public manifest tuple
so the existing cross-repo sync test remains valid.

The `weather` serving alias is documented as a pre-existing SDK
compatibility misnomer. It maps to `silver_elexon_itsdo`, which is GB
Initial Transmission System Demand Outturn, not meteorological weather.

The decommissioned Elexon `bod` transformer remains excluded. It is not
registered by `src/gridflow/silver/elexon/__init__.py`, but the retained
transformer module still self-registers when imported directly by legacy
tests. The manifest therefore maintains an explicit decommissioned
registry skip for `elexon/bod` while keeping the strict missing-date-column
failure for every other registered transformer.

GIE AGSI `unavailability` is designated by `ingested_at` in this
manifest. Its event-overlap handling remains a follow-up: the transformer
filters records whose event window overlaps the target date, but the
stable query date for downstream consumers is still the ingestion
timestamp until a reviewed event-date contract is added.

`CANONICAL_SCHEMA.yaml` remains subordinate documentation and test
input. It is not the source for the runtime manifest.

## Alternatives considered

- **Keep literals in gridflow_models.** Rejected. This repeats the
  ownership error that caused the 2026-07-09 drift incident.
- **Put date-column attributes on every transformer.** Rejected for this
  PR. It scatters a cross-repo contract across more than 160 classes and
  makes review harder.
- **Generate a YAML or JSON manifest file.** Rejected. Generated files
  add freshness questions without improving the runtime contract.
- **Discover everything from DuckDB.** Rejected. The catalogue may not
  exist in development, and dynamic/payload-dependent schemas cannot be
  represented reliably from an empty or partial local database.

## References

- ADR-003: `GridflowClient.get_* SELECT * EXCLUDE` pattern.
- `src/gridflow/silver/schema_manifest.py`.
- `src/gridflow/serving/client.py`.
- `src/gridflow/gold/views/eu_gas_storage.sql`.
- `src/gridflow/gold/views/uk_imbalance_context.sql`.
