---
phase: I2
slug: elexon-mocked-request-shape-and-fixture-backed-bronze-to-silver-tests
status: complete
created: 2026-05-03
requirements:
  - ELEXON-MOCK-01
  - ELEXON-MOCK-02
  - ELEXON-MOCK-03
---

# I2 Research - Elexon Mocked Request Shape and Fixture-Backed Bronze-to-Silver Tests

## Research Complete

Phase I2 should extend the I1 Elexon inventory contract into two non-live validation layers:

1. Mocked request-shape tests that exercise every active Elexon dataset through `ElexonConnector.fetch()` without network access.
2. Fixture-backed bronze-to-silver tests that write realistic Elexon JSON through `BronzeWriter`, run representative registered silver transformers, and assert rows, metadata, partitions, pagination, and expected columns.

## Source Artifacts Read

- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/I1-elexon-inventory-test-scaffolding/I1-01-PLAN.md`
- `.planning/phases/I1-elexon-inventory-test-scaffolding/I1-01-SUMMARY.md`
- `config/sources.yaml`
- `src/gridflow/connectors/elexon/client.py`
- `src/gridflow/connectors/elexon/endpoints.py`
- `src/gridflow/bronze/writer.py`
- `src/gridflow/silver/base.py`
- `src/gridflow/silver/registry.py`
- `src/gridflow/silver/elexon/__init__.py`
- `tests/unit/test_elexon_endpoints.py`
- `tests/endpoints/test_endpoint_urls.py`
- `tests/integration/test_elexon_connector.py`
- `tests/integration/test_bronze_to_silver.py`
- `tests/unit/test_silver_transforms.py`
- `tests/fixtures/elexon/*.json`

## Key Findings

### Existing Contract

I1 established a registry-driven inventory contract:

- `load_settings().get_source_config("elexon").datasets` is the active configured dataset inventory.
- `ENDPOINTS` is the request-definition source of truth.
- Importing `gridflow.silver.elexon` triggers registered silver transformers.
- `EXCLUDED_ENDPOINTS` names intentionally inactive/decommissioned duplicates.

I2 should keep using those production registries rather than maintaining a second static active dataset list.

### Connector Behavior to Test

`ElexonConnector.fetch()` dispatches by `ParamStyle`:

- `DATE_PATH`: appends `{YYYY-MM-DD}` to the path and sends page query params.
- `PUBLISH_DATETIME`: sends `publishDateTimeFrom` / `publishDateTimeTo`, or endpoint-specific `from` / `to`, and chunks by `max_chunk_hours`.
- `SETTLEMENT_DATE_PERIOD`: iterates settlement periods and pages until an empty `data` array or error stops a period.
- `NO_PARAMS`: sends one request with no query params and no data-date partition.

The mocked tests should prove request URLs and query params through the actual connector, not just `build_params()`.

### Fixture and Bronze-Silver Behavior

Existing Elexon fixtures cover many active datasets, and `tests/unit/test_silver_transforms.py` already proves transformer-level field normalization. I2 should add an integration layer that:

- creates `RawResponse` objects from fixture JSON,
- writes those responses with `BronzeWriter`,
- confirms the bronze data file and `.meta.json` sidecar,
- runs `get_transformer("elexon", dataset, tmp_data_dir).run(target_date)`,
- reads the produced parquet file, and
- asserts expected rows and columns for representative dataset families.

Representative families should include:

- `system_prices` for `DATE_PATH`,
- `boal` or `netbsad` for `from` / `to` publish datetime,
- `freq`, `fuelhh`, or `windfor` for standard publish datetime,
- `pn` for `settlementDate` plus `settlementPeriod`,
- `bmunits_reference` for no-param reference data.

### Fixture Gaps

Some currently active datasets have no fixture file yet. Phase I2 does not need a fixture for every active dataset if representative families are covered, but mocked request-shape tests must cover every active configured dataset. Missing fixtures should be explicitly named in the fixture-backed test module so future expansions are intentional.

## Implementation Approach

Add one new focused integration test module, likely `tests/integration/test_elexon_mocked_e2e.py`, with helper functions for:

- loading the real active Elexon source config,
- creating deterministic test windows per `ParamStyle`,
- registering `respx` routes for expected requests,
- building `RawResponse` fixture objects,
- writing bronze files,
- computing the expected silver parquet path.

Reuse `tmp_data_dir` from `tests/conftest.py`, `BronzeWriter`, `ElexonConnector`, `ENDPOINTS`, `ParamStyle`, and `get_transformer`.

## Validation Architecture

Use fast, local-only pytest and ruff commands:

```powershell
uv run --extra dev ruff check tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py
uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py -m "not live" -q
```

The phase must not require live Elexon network access or an API key.

## Risks and Mitigations

| Risk | Mitigation |
| --- | --- |
| Tests duplicate active dataset inventory and drift from config | Derive active datasets from `load_settings()` and compare against `ENDPOINTS`. |
| Mocked routes accidentally miss requests because query params are too loose | Assert route call counts and exact query params for each `ParamStyle`. |
| Fixture-backed tests become too broad and slow | Use representative datasets across families instead of every transformer. |
| `bmunits_reference` bronze partition uses ingestion date, not target date | Use a deterministic `fetched_at` in the `RawResponse` and choose the matching target date for transformer execution. |
| PN period iteration can make up to 50 period requests | Mock period 1 with data and period 2 with empty `data` so the connector stops early without writing empty bronze data. |
| Pagination metadata is not actually exercised | Include at least one two-page mocked response and assert bronze metadata sidecars contain `page` and `total_pages`. |

## Recommended Plan Shape

One plan is sufficient:

- Wave 1, `I2-01`: create request-shape helpers, add full active dataset mocked fetch coverage, add representative bronze-to-silver fixture coverage, and run non-live verification.

This single plan covers `ELEXON-MOCK-01`, `ELEXON-MOCK-02`, and `ELEXON-MOCK-03`.

