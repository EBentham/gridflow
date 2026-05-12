---
phase: L2
slug: agsi-query-scope-request-builder-last-page-pagination-bronze-completeness-tests
status: complete
mapped: 2026-05-04
---

# Phase L2: Pattern Map

## File Classification

| New/Modified File | Role | Closest Analog | Match Quality |
|-------------------|------|----------------|---------------|
| `config/sources.yaml` | AGSI source inventory and rate limit | Existing source dataset blocks | exact |
| `src/gridflow/connectors/gie/endpoints.py` | AGSI query-plan helpers | L1 implementation plus `src/gridflow/connectors/neso/endpoints.py` | exact |
| `src/gridflow/connectors/gie/client.py` | metadata-driven connector fetch loop | `src/gridflow/connectors/neso/carbon_intensity.py` and `src/gridflow/connectors/entsog/client.py` | strong |
| `tests/integration/test_gie_agsi_mocked_bronze.py` | mocked request and bronze completeness tests | `tests/integration/test_entsog_mocked_e2e.py` and `tests/integration/test_neso_mocked_e2e.py` | exact |
| `tests/unit/test_gie_endpoint_catalog.py` | endpoint/helper regression tests | Existing L1 catalog tests | exact |

## Patterns To Reuse

### Metadata-Driven Connector Dispatch

Source: `src/gridflow/connectors/entsog/client.py`

Use endpoint metadata to validate dataset ids and build request params. Avoid a
parallel hand-maintained dataset list inside the connector.

### Multi-Request Date Iteration

Source: `src/gridflow/connectors/neso/carbon_intensity.py`

Use a small request-spec builder so `fetch()` reads as: validate dataset, build
request specs, execute specs, return `RawResponse` objects with provenance.
For AGSI storage, request specs should come from `build_storage_query_plan()`.

### Mocked HTTP With respx

Source: `tests/integration/test_entsog_mocked_e2e.py` and
`tests/integration/test_neso_mocked_e2e.py`

Use `@respx.mock`, collect `httpx.Request` objects in a handler, and assert the
actual path/query params that reached the mocked API.

### Bronze Writer Provenance

Source: `tests/integration/test_entsog_mocked_e2e.py`

Use `BronzeWriter(tmp_data_dir).write(response)` and inspect the `.meta.json`
sidecar. AGSI assertions should include `source`, `dataset`, `request_params`,
`page`, `total_pages`, and data-date path parts.

## Anti-Patterns To Avoid

- Do not call the live AGSI API in L2 tests.
- Do not use `total` or `pageSize` as the pagination loop condition.
- Do not send `till`; AGSI range requests use `to`.
- Do not discard `storage` compatibility while the existing AGSI silver
  transformer still registers `gie_agsi/storage`.
- Do not duplicate the active endpoint inventory across catalog, config, and
  connector tests without an alignment assertion.
- Do not add silver transformer field preservation in L2; that belongs to L3.

## Verification Commands

```powershell
uv run --extra dev ruff check src/gridflow/connectors/gie tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py
uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py -m "not live" -q
uv run --extra dev pytest -m "not live" -q
```

