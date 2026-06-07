# ADR-003 (a.k.a. ADR-GF-003) — GridflowClient.get_* SELECT * EXCLUDE pattern

**Status:** Accepted (amended 2026-06-07, CH4-07)
**Date:** 2026-05-10
**Phase:** Companion to gridflow_models F11 / WBH-03

> **Amendment (2026-06-07, CH4-07 — `fix/ch4-architecture-hygiene`):** the
> "Consequences" claim that a column named in EXCLUDE but absent fails loudly
> with `BinderException` as a desirable drift detector held only for the silver
> parquet views (which carry all six bitemporal columns). The two cross-source
> **gold** SQL views (`gold_eu_gas_storage`, `gold_uk_imbalance_context`) are
> explicit-column SELECTs that carry NONE of them, so the unconditional EXCLUDE
> raised `BinderException` on every real catalogue, breaking both public SDK
> methods. The client now EXCLUDEs only the bitemporal columns ACTUALLY present
> in the queried relation (`GridflowClient._present_bitemporal_exclude_clause`,
> introspecting `information_schema.columns` with a bound relation name): silver
> views still exclude all six; the gold views exclude none and bind cleanly. The
> forward-compat "new public column flows through automatically" property is
> retained. Tradeoff: a bitemporal column dropped from a *silver* view is now
> silently not excluded rather than raising — the loud drift detector is traded
> for cross-source correctness.

> Naming note: gridflow_models' F11-E plan refers to this ADR as
> "ADR-GF-003" for cross-reference symmetry with the `GF-` prefix the
> planner used for cross-repo identification. The on-disk filename
> follows gridflow's flat `ADR-NNN-` convention (established by F11-G's
> ADR-001 and ADR-002), so it is `ADR-003-...md`. Both names refer to
> this single document.

## Context

The 2026-05-10 live notebook walkthrough hit
`BinderException: Referenced column "run_type" not found` on
`client.get_system_prices(...)` against a fresh ingest. The hardcoded
SELECT list in `GridflowClient.get_system_prices` named `run_type` —
a column the Elexon Insights system-prices endpoint does not actually
publish today. Live silver instead has `price_derivation_code` (see
`silver/elexon/system_prices.py` line 67, where the API field
`priceDerivationCode` is renamed to `run_type` — but the bronze JSON
no longer carries either field on the live endpoint, so the silver
output omits it).

A follow-up audit of the entire `GridflowClient.get_*` family
(documented in gridflow_models `F11-RESEARCH.md` §6) confirmed that
**every** `get_*` method was at risk of the same drift:

| Method | View (FROM) | Hardcoded columns | Audit finding |
|--------|-------------|-------------------|---------------|
| `get_system_prices` | `silver_system_prices` | timestamp_utc, system_sell_price, system_buy_price, net_imbalance_volume, **run_type** | **CONFIRMED DRIFT** — live silver has `price_derivation_code`, not `run_type`. |
| `get_generation_by_fuel` | `silver_generation_by_fuel` | timestamp_utc, fuel_type, generation_mw | **VIEW DOES NOT EXIST** — `silver_generation_by_fuel` was removed from the silver registry as a duplicate of `silver_fuelhh` (`silver/elexon/__init__.py` line 36). |
| `get_fuel_generation` | `silver_fuelhh` | 6 columns | Plausible against live `silver_fuelhh` schema; rewritten for f-string-SQL hygiene. |
| `get_gas_storage` | `gold_eu_gas_storage` | 9 columns + optional country filter | Unverified against live data (no GIE AGSI+ ingest in the audit environment). |
| `get_weather` | `silver_historical` | 9 columns including `hdd, cdd` | Unverified (no Open-Meteo ingest in the audit environment). |
| `get_imbalance_context` | `gold_uk_imbalance_context` | 10 columns including **run_type** | Same `run_type` risk as `get_system_prices` — propagates through the gold join. |

In addition to the schema-drift hazard, every `get_*` method used
**f-string SQL** with the date-range arguments interpolated into the
WHERE clause as quoted string literals. This violates gridflow's
"parameterised SQL only" coding rule and is incompatible with
gridflow_models' analogous CLAUDE.md hard rule.

## Decision

Adopt **`SELECT * EXCLUDE (...)`** with **parameterised SQL** for every
`GridflowClient.get_*` method. The EXCLUDE clause names the bitemporal
and partitioning internal columns the helper hides from the public
surface; date-range arguments use `?` placeholders.

The shared bitemporal-internal exclude list is captured as a module
constant:

```python
_BITEMPORAL_EXCLUDE = (
    "event_time",
    "available_at",
    "source_run_id",
    "dataset_version",
    "month",
    "year",
)
_BITEMPORAL_EXCLUDE_SQL = ", ".join(_BITEMPORAL_EXCLUDE)
```

Verbatim rewrite shape (illustrated for `get_system_prices`):

```python
def get_system_prices(
    self,
    start: str | date,
    end: str | date,
) -> pl.DataFrame:
    sql = (
        "SELECT * EXCLUDE (" + _BITEMPORAL_EXCLUDE_SQL + ") "
        "FROM silver_system_prices "
        "WHERE settlement_date BETWEEN ? AND ? "
        "ORDER BY timestamp_utc"
    )
    return self._require_con().execute(sql, [str(start), str(end)]).pl()
```

Two changes from the existing body:

1. **`SELECT * EXCLUDE (...)`** for forward-compat with any new column
   the silver transformer adds. New public columns flow through
   automatically; bitemporal-internal additions go in the EXCLUDE
   tuple.
2. **Parameterised SQL** with `?` placeholders. Closes the f-string
   SQL anti-pattern across the file. SQL-injection surface from the
   date-range arguments is eliminated as a side-effect (T-F11E-03 in
   the F11-E threat model).

Methods with optional non-date filters (e.g. `get_gas_storage`'s
`country_code`, `get_weather`'s `location`) build the parameter list
defensively:

```python
params: list[str] = [str(start), str(end)]
country_filter = ""
if country_code:
    country_filter = " AND country_code = ?"
    params.append(country_code)
```

so the optional clause is also parameterised, never f-string-injected.

For `get_generation_by_fuel`, where the named view
(`silver_generation_by_fuel`) does not exist, the body is rewritten
to query the canonical view (`silver_fuelhh`) with a column projection
preserving the original 3-column return shape, plus a one-shot
`DeprecationWarning` pointing callers at `get_fuel_generation`:

```python
warnings.warn(
    "GridflowClient.get_generation_by_fuel() is deprecated; "
    "the underlying silver_generation_by_fuel view was removed "
    "(it duplicated silver_fuelhh). Call get_fuel_generation() "
    "instead. This shim queries silver_fuelhh under the hood.",
    DeprecationWarning,
    stacklevel=2,
)
```

The corresponding paired smoke tests in the gridflow_models repo
(`tests/integration/test_client_get_methods.py`) anchor the public
column-set per method via tiny JSON snapshots under
`tests/integration/_schema_snapshots/`. The snapshots are committed;
regeneration requires the explicit `--regen-schema-snapshots` pytest
flag plus PR review of the resulting diff (gridflow_models
`F11-RESEARCH.md` §11 Pitfall 5).

## Consequences

**Accepted:**

- A schema addition on the silver side flows through to the public API
  without any client-side change. The smoke-test snapshot still
  reflects the old column set, the test fails, the developer
  regenerates the snapshot deliberately and the diff is reviewed.
- A schema removal of a column named in EXCLUDE fails loudly with
  `BinderException` from the EXCLUDE clause. This is the desirable
  schema-drift detector — better than silently returning wrong-shape
  frames.
- The CLAUDE.md "no f-string SQL" hard rule (gridflow_models) and the
  analogous rule in this repo are simultaneously cleared on the file
  in scope (`src/gridflow/serving/client.py`).
- `get_generation_by_fuel` callers continue to work (data still flows
  via `silver_fuelhh`) but now see a one-shot DeprecationWarning
  pointing them at `get_fuel_generation`. This is a strictly
  better-than-status-quo outcome — before this change, callers got an
  obscure `CatalogException` once they hit the now-non-existent view.
- The shared `_BITEMPORAL_EXCLUDE` tuple at module level documents the
  silver-layer's bitemporal contract in one place. Adding a new
  bitemporal-internal column to the silver schema requires a single
  edit here, not six SELECT-list edits.

**Tradeoff:**

- **Callers MUST be defensive about column-set changes.** They cannot
  assume a fixed column set across gridflow versions. The smoke-test
  snapshots in gridflow_models are the source of truth for the
  expected column set per method. Downstream notebook code that
  references columns by position (e.g. `df[df.columns[3]]`) is now
  fragile — new columns inserted on the silver side will shift
  positions. Idiomatic notebook code uses column-name access (e.g.
  `df["price_derivation_code"]`), which is unaffected.
- A future column with a sensitive name (e.g. an internal pipeline
  log) could be exposed if not added to EXCLUDE. The silver layer is
  curated, so this is unlikely in practice; the smoke-test snapshot
  review catches accidental exposure on first encounter.
- One extra catalog round-trip per query (the `* EXCLUDE` planner
  expansion). Negligible for the analytical workloads this client
  serves.

## Alternatives considered

- **Hardcoded SELECT lists (status quo).** Rejected as the WBH-03
  trigger demonstrates: brittle, drift-prone, and silent in failure
  mode unless the missing column happens to be named in the SELECT
  list (which then surfaces as `BinderException`, but the
  remediation requires editing six methods independently).
- **Add the new column (`price_derivation_code`) to the SELECT list,
  remove `run_type`.** Solves the immediate drift but pushes the same
  trap into the next column-set drift event. EXCLUDE is the
  forward-compatible primitive.
- **Maintain `silver_generation_by_fuel` as a view alias for
  `silver_fuelhh`.** Rejected — would re-introduce the duplicate that
  was deliberately removed (`silver/elexon/__init__.py` line 36). The
  DeprecationWarning + transparent shim is the cleaner migration
  path.
- **Change the public API to return a typed model rather than a
  Polars DataFrame.** Out of scope — would be a major caller-side
  break across gridflow_models and any downstream notebooks. The
  forward-compat win of `EXCLUDE` is achievable without that surface
  change.
- **Use `pl.read_parquet(...)` directly bypassing DuckDB.** Out of
  scope — the client API contract is "DuckDB SQL gateway"; gold
  cross-source views (`gold_uk_imbalance_context`,
  `gold_eu_gas_storage`) are SQL views, not parquets, and have no
  parquet representation.

## References

- gridflow_models F11-CONTEXT.md §I-02 / WBH-03.
- gridflow_models F11-RESEARCH.md §6 (audit methodology + recommended
  pattern + smoke-test pattern).
- gridflow_models F11-RESEARCH.md §11 Pitfall 5 (snapshot regeneration
  discipline).
- gridflow_models F11-E plan
  (`.planning/phases/F11-workbench-hardening/F11-E-gridflow-client-get-schema-audit-PLAN.md`).
- gridflow_models F11-E smoke tests
  (`tests/integration/test_client_get_methods.py` +
  `tests/integration/_schema_snapshots/`).
- ADR-001 (project root resolution) and ADR-002 (client
  reopen_readonly) — F11-G's two ADRs in this directory; same flat
  `ADR-NNN-` naming convention.
- DuckDB `SELECT * EXCLUDE` reference:
  https://duckdb.org/docs/current/sql/query_syntax/select
- DuckDB Discussion #16700: EXCLUDE missing-column behaviour confirms
  the desirable fail-loud semantics
  (https://github.com/duckdb/duckdb/discussions/16700).
- This commit's tests in gridflow_models:
  6 smoke tests, 6 JSON schema snapshots, 1 README, 1 conftest hook.
