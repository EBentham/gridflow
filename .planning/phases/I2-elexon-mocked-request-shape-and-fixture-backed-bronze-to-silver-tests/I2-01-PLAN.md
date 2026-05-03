---
phase: I2
plan: 01
type: execute
wave: 1
depends_on: []
requirements:
  - ELEXON-MOCK-01
  - ELEXON-MOCK-02
  - ELEXON-MOCK-03
files_modified:
  - tests/integration/test_elexon_mocked_e2e.py
  - tests/fixtures/elexon/*.json
  - tests/integration/test_elexon_connector.py
  - tests/unit/test_elexon_endpoints.py
autonomous: true
---

# I2-01 Plan - Elexon Mocked Request Shape and Fixture-Backed Bronze-to-Silver Coverage

## Objective

<objective>

Add a non-live Elexon end-to-end validation layer that proves every active configured Elexon dataset builds the correct mocked request shape, and that representative realistic Elexon JSON fixtures can be written to bronze, transformed to silver parquet, and checked for provenance, partitioning, pagination metadata, rows, and expected columns.

</objective>

## Execution Context

Read these before editing:

- `.planning/phases/I2-elexon-mocked-request-shape-and-fixture-backed-bronze-to-silver-tests/I2-RESEARCH.md`
- `.planning/phases/I2-elexon-mocked-request-shape-and-fixture-backed-bronze-to-silver-tests/I2-VALIDATION.md`
- `.planning/phases/I2-elexon-mocked-request-shape-and-fixture-backed-bronze-to-silver-tests/I2-PATTERNS.md`
- `.planning/phases/I1-elexon-inventory-test-scaffolding/I1-01-SUMMARY.md`
- `.planning/REQUIREMENTS.md`
- `.planning/ROADMAP.md`
- `config/sources.yaml`
- `src/gridflow/connectors/elexon/client.py`
- `src/gridflow/connectors/elexon/endpoints.py`
- `src/gridflow/bronze/writer.py`
- `src/gridflow/silver/base.py`
- `src/gridflow/silver/registry.py`
- `src/gridflow/silver/elexon/__init__.py`
- `tests/conftest.py`
- `tests/integration/test_elexon_connector.py`
- `tests/integration/test_bronze_to_silver.py`
- `tests/unit/test_elexon_endpoints.py`
- `tests/unit/test_silver_transforms.py`
- `tests/fixtures/elexon/*.json`

## Must Haves

- The plan must satisfy `ELEXON-MOCK-01`, `ELEXON-MOCK-02`, and `ELEXON-MOCK-03`.
- Active Elexon mocked request coverage must be registry-driven from `load_settings().get_source_config("elexon").datasets` and `ENDPOINTS`, not a duplicate hard-coded inventory.
- Tests must not hit the live Elexon API, must not require an API key, and must run under `-m "not live"`.
- Mocked request tests must assert exact URL/path/query behavior for every `ParamStyle` used by active Elexon datasets.
- Fixture-backed tests must use `BronzeWriter` and registered silver transformers, not direct calls to transformer `transform()` only.
- Bronze assertions must include data file existence, `.meta.json` sidecar existence, source/dataset/request metadata, `page`, `total_pages`, and date partition path where applicable.
- Silver assertions must read parquet files and check row counts plus expected columns.

## Tasks

<tasks>

### 1. Create the Elexon Mocked E2E Test Module

<read_first>

- `tests/conftest.py`
- `tests/integration/test_elexon_connector.py`
- `tests/integration/test_bronze_to_silver.py`
- `tests/unit/test_elexon_endpoints.py`
- `src/gridflow/connectors/elexon/client.py`
- `src/gridflow/connectors/elexon/endpoints.py`
- `src/gridflow/bronze/writer.py`
- `src/gridflow/silver/registry.py`
- `src/gridflow/silver/elexon/__init__.py`

</read_first>

<action>

Create `tests/integration/test_elexon_mocked_e2e.py` with:

- `from __future__ import annotations`
- imports for `json`, `date`, `datetime`, `UTC`, `Path`, `httpx`, `polars as pl`, `pytest`, and `respx`
- imports for `BronzeWriter`, `RawResponse`, `load_settings`, `ElexonConnector`, `ENDPOINTS`, `ParamStyle`, and `get_transformer`
- `import gridflow.silver.elexon  # noqa: F401` before registry access
- `FIXTURES = Path(__file__).parent.parent / "fixtures" / "elexon"`
- `BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"`
- deterministic constants:
  - `TARGET_DATE = date(2024, 1, 15)`
  - `START = datetime(2024, 1, 15, 0, 0, tzinfo=UTC)`
  - `END = datetime(2024, 1, 16, 0, 0, tzinfo=UTC)`

Add helpers:

- `_active_elexon_config()` returns `load_settings().get_source_config("elexon")`
- `_fixture_body(dataset: str) -> bytes` reads `tests/fixtures/elexon/{dataset}_response.json` and returns bytes
- `_raw_response(dataset: str, body: bytes, request_url: str, request_params: dict, page: int = 1, total_pages: int = 1, data_date: date | None = TARGET_DATE) -> RawResponse` creates a deterministic Elexon `RawResponse` with `fetched_at=datetime(2024, 1, 15, 12, 0, tzinfo=UTC)`, `content_type="application/json"`, `source="elexon"`, `api_version="v1"`, `http_status=200`, `page=page`, and `total_pages=total_pages`
- `_silver_parquet_path(data_dir: Path, dataset: str, target_date: date = TARGET_DATE) -> Path` returns `data_dir / "silver" / "elexon" / dataset / f"year={target_date.year}" / f"month={target_date.month:02d}" / f"{dataset}_{target_date.strftime('%Y%m%d')}.parquet"`
- `_assert_bronze_metadata(bronze_path: Path, dataset: str, page: int = 1, total_pages: int = 1) -> None` checks sidecar path `bronze_path.with_suffix("").with_suffix(".meta.json")`, then asserts `source == "elexon"`, `dataset == dataset`, `http_status == 200`, `content_type == "application/json"`, `page == page`, `total_pages == total_pages`, and `body_size_bytes > 0`

</action>

<acceptance_criteria>

- `tests/integration/test_elexon_mocked_e2e.py` exists.
- The file contains `import gridflow.silver.elexon  # noqa: F401`.
- The file contains `def _active_elexon_config()`.
- The file contains `def _fixture_body(dataset: str) -> bytes`.
- The file contains `def _raw_response(`.
- The file contains `def _silver_parquet_path(`.
- The file contains `def _assert_bronze_metadata(`.
- `uv run --extra dev ruff check tests/integration/test_elexon_mocked_e2e.py` exits 0.

</acceptance_criteria>

### 2. Add Registry-Driven Mocked Request-Shape Coverage

<read_first>

- `tests/integration/test_elexon_mocked_e2e.py`
- `src/gridflow/connectors/elexon/client.py`
- `src/gridflow/connectors/elexon/endpoints.py`
- `tests/endpoints/test_endpoint_urls.py`
- `tests/unit/test_elexon_endpoints.py`

</read_first>

<action>

In `tests/integration/test_elexon_mocked_e2e.py`, add a `TestElexonMockedRequestShape` class.

Add a parametrized async test named `test_active_datasets_fetch_with_expected_mocked_request_shape` that:

- derives datasets from `sorted(_active_elexon_config().datasets)`
- asserts `set(datasets) == set(ENDPOINTS)`
- for each dataset, registers the expected `respx` route against the real `BASE_URL` and endpoint path
- creates a source config from `_active_elexon_config()`
- runs `async with ElexonConnector(config) as connector: responses = await connector.fetch(dataset, START, END)`
- asserts at least one response exists for every active dataset
- asserts every response has `source == "elexon"`, `dataset == dataset`, `http_status == 200`, and `request_url.startswith(BASE_URL)`

Use exact request expectations by `ParamStyle`:

- `DATE_PATH`: route `GET {BASE_URL}{endpoint.path}/2024-01-15`, assert query includes `page=1`, and assert response `data_date == TARGET_DATE`
- standard `PUBLISH_DATETIME`: route `GET {BASE_URL}{endpoint.path}`, assert query includes endpoint `from_param` and `to_param`; for most endpoints values are `2024-01-15T00:00:00Z` and `2024-01-16T00:00:00Z`; for `uou2t14d`, assert multiple 4-hour chunk requests are made and that one route receives `publishDateTimeFrom=2024-01-15T00:00:00Z` and `publishDateTimeTo=2024-01-15T04:00:00Z`
- `SETTLEMENT_DATE_PERIOD`: for `pn`, mock period 1 page 1 with fixture data and period 2 page 1 with `{"data": []}`; assert the connector returns the period 1 response and does not write an empty period response
- `NO_PARAMS`: route `GET {BASE_URL}{endpoint.path}`, assert no query date parameters and no `page` param

For datasets without fixture files, use a minimal JSON body `{"data": [{"id": "{dataset}"}], "metadata": {"totalPages": 1, "currentPage": 1}}` for request-shape tests only. Do not use synthetic bodies for fixture-backed bronze-to-silver tests.

Add one focused pagination test named `test_paginated_dataset_fetches_all_pages` using `system_prices` or another pagination-capable endpoint, with page 1 metadata `totalPages=2` and page 2 metadata `totalPages=2`. Assert two connector responses and `responses[0].page == 1`, `responses[1].page == 2`, and both responses have `total_pages == 2`.

</action>

<acceptance_criteria>

- `tests/integration/test_elexon_mocked_e2e.py` contains `class TestElexonMockedRequestShape`.
- The file contains `test_active_datasets_fetch_with_expected_mocked_request_shape`.
- The file contains `sorted(_active_elexon_config().datasets)`.
- The file contains `assert set(datasets) == set(ENDPOINTS)`.
- The file contains assertions for `ParamStyle.DATE_PATH`, `ParamStyle.PUBLISH_DATETIME`, `ParamStyle.SETTLEMENT_DATE_PERIOD`, and `ParamStyle.NO_PARAMS`.
- The file contains `test_paginated_dataset_fetches_all_pages`.
- `uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py -m "not live" -q` exits 0.

</acceptance_criteria>

### 3. Add Fixture-Backed Bronze-to-Silver Coverage

<read_first>

- `tests/integration/test_elexon_mocked_e2e.py`
- `tests/integration/test_bronze_to_silver.py`
- `tests/unit/test_silver_transforms.py`
- `src/gridflow/bronze/writer.py`
- `src/gridflow/silver/base.py`
- `src/gridflow/silver/registry.py`
- `src/gridflow/silver/elexon/__init__.py`
- `tests/fixtures/elexon/system_prices_response.json`
- `tests/fixtures/elexon/boal_response.json`
- `tests/fixtures/elexon/freq_response.json`
- `tests/fixtures/elexon/pn_response.json`
- `tests/fixtures/elexon/bmunits_response.json`

</read_first>

<action>

In `tests/integration/test_elexon_mocked_e2e.py`, add a `TestElexonFixtureBackedBronzeToSilver` class.

Add a parametrized test named `test_fixture_response_writes_bronze_and_transforms_to_silver` with at least these representative cases:

| dataset | fixture | expected columns |
| --- | --- | --- |
| `system_prices` | `system_prices_response.json` | `timestamp_utc`, `system_sell_price`, `system_buy_price`, `data_provider` |
| `boal` | `boal_response.json` | `timestamp_utc`, `bm_unit_id`, `acceptance_number`, `data_provider` |
| `freq` | `freq_response.json` | `timestamp_utc`, `frequency_hz`, `data_provider` |
| `pn` | `pn_response.json` | `timestamp_utc`, `bm_unit_id`, `level_from`, `level_to`, `data_provider` |
| `bmunits_reference` | `bmunits_response.json` | `bm_unit_id`, `fuel_type`, `registered_capacity_mw`, `data_provider` |

For each case:

- load fixture bytes with `_fixture_body(dataset)` or the provided fixture filename
- create a `RawResponse` using `_raw_response(dataset=dataset, body=body, request_url=f"{BASE_URL}{ENDPOINTS[dataset].path}", request_params={"page": 1}, data_date=TARGET_DATE)`
- write it with `BronzeWriter(tmp_data_dir).write(response)`
- assert `bronze_path.exists()`
- assert the bronze path includes `bronze/elexon/{dataset}/2024/01/15`
- call `_assert_bronze_metadata(bronze_path, dataset)`
- create `transformer = get_transformer("elexon", dataset, tmp_data_dir)`
- run `rows = transformer.run(TARGET_DATE)`
- assert `rows > 0`
- read parquet from `_silver_parquet_path(tmp_data_dir, dataset)`
- assert the parquet exists
- assert the DataFrame length is `rows`
- assert each expected column is in `df.columns`

If `bmunits_reference` cannot be run against `TARGET_DATE` because the connector lacks `data_date` for no-param responses, keep the deterministic `data_date=TARGET_DATE` in the synthetic `RawResponse` and add an assertion that the no-param mocked request test separately verifies connector response `data_date is None`.

</action>

<acceptance_criteria>

- `tests/integration/test_elexon_mocked_e2e.py` contains `class TestElexonFixtureBackedBronzeToSilver`.
- The file contains `test_fixture_response_writes_bronze_and_transforms_to_silver`.
- The parametrized cases include `system_prices`, `boal`, `freq`, `pn`, and `bmunits_reference`.
- The test calls `BronzeWriter(tmp_data_dir).write(response)`.
- The test calls `get_transformer("elexon", dataset, tmp_data_dir)`.
- The test reads parquet with `pl.read_parquet`.
- `uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py -m "not live" -q` exits 0.

</acceptance_criteria>

### 4. Assert Bronze Metadata, Partitioning, and Pagination Behavior

<read_first>

- `tests/integration/test_elexon_mocked_e2e.py`
- `src/gridflow/bronze/writer.py`
- `src/gridflow/connectors/base.py`
- `src/gridflow/connectors/elexon/client.py`
- `src/gridflow/connectors/elexon/parsers.py`

</read_first>

<action>

Strengthen the new test module so `ELEXON-MOCK-03` is explicit:

- in the fixture-backed test, assert the bronze path has `year=2024` only for silver paths and plain `2024/01/15` only for bronze paths
- in `_assert_bronze_metadata`, assert sidecar JSON keys `request_url`, `request_params`, `api_version`, `http_status`, `body_sha256`, `body_size_bytes`, `page`, and `total_pages` are present
- add `test_bronze_metadata_preserves_pagination_fields` that writes two `system_prices` `RawResponse` objects with `page=1`, `page=2`, `total_pages=2`, then asserts each sidecar contains the matching `page` and `total_pages`
- add `test_no_param_mocked_response_has_no_query_params_or_data_date` for `bmunits_reference`, asserting connector response `request_params == {}` and `data_date is None`
- ensure no test uses `@pytest.mark.live`

</action>

<acceptance_criteria>

- `tests/integration/test_elexon_mocked_e2e.py` contains `test_bronze_metadata_preserves_pagination_fields`.
- The file contains `test_no_param_mocked_response_has_no_query_params_or_data_date`.
- The file contains `assert "2024" in bronze_path.parts`.
- The file contains `assert "year=2024" in parquet_path.parts`.
- `_assert_bronze_metadata` asserts keys `request_url`, `request_params`, `api_version`, `body_sha256`, and `body_size_bytes`.
- `Select-String -Path tests/integration/test_elexon_mocked_e2e.py -Pattern '@pytest.mark.live'` returns no matches.

</acceptance_criteria>

### 5. Run Verification and Keep the Phase Non-Live

<read_first>

- `pyproject.toml`
- `tests/integration/test_elexon_mocked_e2e.py`
- `tests/integration/test_elexon_connector.py`
- `tests/unit/test_elexon_endpoints.py`
- `.planning/phases/I2-elexon-mocked-request-shape-and-fixture-backed-bronze-to-silver-tests/I2-VALIDATION.md`

</read_first>

<action>

Run:

```powershell
uv run --extra dev ruff check tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py
uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py -m "not live" -q
```

If `test_elexon_mocked_e2e.py` needs fixture expansion, add only small deterministic fixture JSON files under `tests/fixtures/elexon/` and list the specific new filenames in the phase summary. Do not call the live Elexon API, do not require `ELEXON_API_KEY`, and do not remove the I1 inventory tests.

</action>

<acceptance_criteria>

- Ruff command exits 0.
- Pytest command exits 0.
- The pytest command includes `-m "not live"`.
- No new tests in `tests/integration/test_elexon_mocked_e2e.py` are marked live.
- `ELEXON-MOCK-01`, `ELEXON-MOCK-02`, and `ELEXON-MOCK-03` are all satisfied by automated tests.

</acceptance_criteria>

</tasks>

## Threat Model

<threat_model>

| ID | Threat | Severity | Mitigation |
| --- | --- | --- | --- |
| T-I2-01 | Mocked tests accidentally hit the live Elexon API and become flaky or slow. | High | Use `respx.mock`, exact route registration, and non-live pytest commands only. |
| T-I2-02 | Active dataset coverage drifts because tests use a stale duplicate dataset list. | Medium | Derive active datasets from config and assert equality with `ENDPOINTS`. |
| T-I2-03 | Synthetic request-shape bodies hide transformer fixture regressions. | Medium | Use synthetic bodies only in request-shape tests; use real fixture files for bronze-to-silver tests. |
| T-I2-04 | Bronze provenance is not validated, so metadata or partition regressions slip through. | Medium | Assert data file paths, metadata sidecars, `page`, `total_pages`, request URL, params, and body hashes. |
| T-I2-05 | Representative fixture coverage misses an Elexon family needed for I3 live tests. | Medium | Cover date-path, publish datetime, from/to, settlement date/period, and no-param reference families. |

</threat_model>

## Verification

<verification>

Run:

```powershell
uv run --extra dev ruff check tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py
uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py -m "not live" -q
```

</verification>

## Success Criteria

<success_criteria>

- `ELEXON-MOCK-01`: Every active configured Elexon dataset has mocked request URL and parameter-shape coverage without network access.
- `ELEXON-MOCK-02`: Representative Elexon fixtures write through `BronzeWriter` and transform to silver parquet across date-path, publish datetime, settlement-period, and reference-data families.
- `ELEXON-MOCK-03`: Tests assert bronze metadata, `data_date` partitioning, pagination metadata, and expected silver columns.
- Fast verification passes without live network access or an API key.
- I3 can reuse the mocked and fixture-backed patterns when moving to live API-to-silver coverage.

</success_criteria>
