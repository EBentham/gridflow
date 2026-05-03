---
phase: I2
slug: elexon-mocked-request-shape-and-fixture-backed-bronze-to-silver-tests
status: complete
created: 2026-05-03
---

# I2 Pattern Map

## Target Files

| Target | Role | Closest Existing Analog |
|--------|------|-------------------------|
| `tests/integration/test_elexon_mocked_e2e.py` | New mocked request-shape and fixture-backed bronze-to-silver integration tests | `tests/integration/test_elexon_connector.py`, `tests/integration/test_bronze_to_silver.py`, `tests/unit/test_silver_transforms.py` |
| `tests/fixtures/elexon/*.json` | Fixture inputs for representative bronze-to-silver checks | Existing Elexon fixture files |
| `tests/unit/test_elexon_endpoints.py` | Inventory helper source of truth if minor helper extraction is needed | Existing I1 inventory tests |

## Existing Patterns to Reuse

### Mocked HTTP

`tests/integration/test_elexon_connector.py` uses `respx.mock`, `httpx.Response`, and the real `ElexonConnector` async context manager. I2 should follow that pattern instead of mocking connector internals.

### Bronze-Silver Flow

`tests/integration/test_bronze_to_silver.py` shows the desired integration shape:

- create or load a `RawResponse`,
- write it with `BronzeWriter(tmp_data_dir).write(response)`,
- assert the raw file and sidecar metadata exist,
- instantiate a transformer with `tmp_data_dir`,
- call `run(target_date)`,
- read the expected parquet output and assert columns/rows.

### Transformer Registry

`tests/unit/test_elexon_endpoints.py` imports `gridflow.silver.elexon` before using `list_transformers("elexon")`. I2 should do the same before calling `get_transformer("elexon", dataset, tmp_data_dir)`.

### Active Dataset Inventory

Use `load_settings().get_source_config("elexon").datasets` and `ENDPOINTS`. Do not maintain a separate hand-written list of active Elexon datasets except for small representative fixture coverage tables.

## Landmines

- `DATE_PATH` endpoints append the date to the request path.
- `PUBLISH_DATETIME` endpoints may use `publishDateTimeFrom` / `publishDateTimeTo` or endpoint-specific `from` / `to`.
- `PN` is `SETTLEMENT_DATE_PERIOD` and loops settlement periods; mocked tests should stop after a small number of mocked period calls.
- `NO_PARAMS` endpoints have no query params and no explicit `data_date` from connector responses.
- `uou2t14d` has `max_chunk_hours=4`, so a 24-hour range creates multiple chunk requests.
- Bronze metadata does not currently write `data_date` into the sidecar; partition assertions must use path structure plus existing sidecar fields.

