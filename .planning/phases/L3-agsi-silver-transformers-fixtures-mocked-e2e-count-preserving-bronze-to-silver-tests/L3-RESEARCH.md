# Phase L3 Research: AGSI Silver Transformers, Fixtures, And Mocked E2E

**Researched:** 2026-05-04
**Status:** Complete

## Research Complete

Phase L3 should turn the AGSI bronze coverage from L2 into deterministic
silver-layer confidence. L2 already aligned AGSI active endpoint inventory,
implemented query-scope storage fetching, used `last_page` pagination, and
proved exact mocked bronze request/page counts for aggregate, country, company,
and facility scopes. L3 should not revisit live API behavior; it should make
the local bronze-to-silver contract preserve payload data and prove it with
fixture-backed non-live tests.

## What Matters For Planning

- `src/gridflow/silver/gie/agsi.py` currently registers only
  `gie_agsi/storage` and preserves a compact country-level subset.
- L2 writes new AGSI storage bronze under `gie_agsi/storage_reports` while
  keeping `storage` as a compatibility alias.
- AGSI storage payload rows include live fields that are currently dropped:
  `updatedAt`, `gasDayEnd`, `consumption`, `consumptionFull`,
  `netWithdrawal`, `injectionCapacity`, `withdrawalCapacity`,
  `contractedCapacity`, `availableCapacity`, `coveredCapacity`, `status`,
  `url`, and `info`.
- Storage silver deduplication can no longer rely only on `gas_day` plus
  `country_code`; aggregate, country, company, and facility rows can all share
  date values and need entity-level/entity-key columns.
- `docs/gie_agsi_endpoint_catalog.yaml` marks `about_summary`,
  `about_listing`, `news`, `news_item`, and `unavailability` as active AGSI
  endpoint rows. L3 should either add deterministic silver outputs for these
  families or update catalog/config with explicit deferred reasons; the roadmap
  asks for fixture-backed listing/news/unavailability bronze-to-silver coverage,
  so the preferred plan is to transform them.
- `tests/integration/test_entsog_mocked_e2e.py` and
  `tests/integration/test_neso_mocked_e2e.py` provide the strongest local
  pattern: build `RawResponse` fixtures, write with `BronzeWriter`, run
  registered transformers through `get_transformer`, and assert parquet rows
  and expected columns.

## Recommended Local Shapes

1. Extend `GasStorageTransformer` so the same implementation can serve both
   `storage` and `storage_reports`.
2. Add storage output columns for entity scope and live fields:
   `entity_level`, `entity_code`, `entity_name`, `entity_url`,
   `country_code`, `country_name`, `gas_day`, `gas_day_end`, `updated_at`,
   inventory/flow/capacity/fullness/status/info fields, plus provider and
   ingestion metadata.
3. Add reference-style GIE transformers in `src/gridflow/silver/gie/agsi.py`
   or a small adjacent module for:
   - `about_summary`
   - `about_listing`
   - `news`
   - `news_item`
   - `unavailability`
4. Register each active AGSI dataset id with `gridflow.silver.registry` and
   import it from `src/gridflow/silver/gie/__init__.py`.
5. Add small sanitized fixtures under `tests/fixtures/gie/`:
   - storage reports covering aggregate, country, company, and facility rows
   - listing/about payloads
   - news/news item payloads
   - unavailability payloads
6. Add `tests/integration/test_gie_agsi_mocked_e2e.py` for fixture-backed
   bronze-to-silver coverage across all active AGSI families.
7. Keep `tests/integration/test_gie_agsi_mocked_bronze.py` as the request-shape
   and bronze completeness gate; L3 should add silver checks without weakening
   L2 assertions.

## Validation Architecture

Use non-live pytest coverage only. The fast L3 gate should focus on the GIE
silver transformer and mocked E2E path:

```powershell
uv run --extra dev pytest tests/unit/test_gie.py tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q
```

The full L3 pre-execution gate should include the existing L2 mocked bronze
suite and catalog tests:

```powershell
uv run --extra dev ruff check src/gridflow/silver/gie tests/unit/test_gie.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py
uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q
```

## Risks To Carry Into The Plan

- Silent field loss: storage silver must assert expected live columns, not just
  row counts.
- Scope collapse: aggregate/country/company/facility rows must remain
  distinguishable after silver deduplication.
- Dataset-id mismatch: L2 uses `storage_reports`; L3 must not leave only the
  legacy `storage` transformer registered.
- False active coverage: active listing/news/unavailability families must have
  tests proving transform output or catalog-backed deferral.
- Live leakage: mocked E2E must use fixture `RawResponse` objects and no
  `GIE_API_KEY` or network calls.
