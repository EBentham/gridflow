---
phase: I3
plan: 01
type: execute
wave: 1
depends_on: []
requirements:
  - ELEXON-LIVE-01
  - ELEXON-LIVE-02
  - ELEXON-LIVE-03
  - ELEXON-LIVE-04
  - ELEXON-LIVE-05
files_modified:
  - tests/integration/test_elexon_live_e2e.py
  - tests/endpoints/test_endpoint_live.py
  - tests/integration/test_elexon_mocked_e2e.py
autonomous: true
---

# I3-01 Plan - Elexon Live API to Silver Test Suite

## Objective

<objective>

Add an opt-in live Elexon API-to-silver integration suite that calls the public
Insights API for a curated representative dataset set, writes successful live
responses through `BronzeWriter`, runs registered silver transformers, verifies
silver rows/columns/provider/schema-relevant output, and classifies empty,
removed, or no-data responses explicitly without affecting normal non-live test
runs.

</objective>

## Execution Context

Read these before editing:

- `.planning/phases/I3-elexon-live-api-to-silver-test-suite/I3-RESEARCH.md`
- `.planning/phases/I3-elexon-live-api-to-silver-test-suite/I3-VALIDATION.md`
- `.planning/phases/I2-elexon-mocked-request-shape-and-fixture-backed-bronze-to-silver-tests/I2-01-SUMMARY.md`
- `.planning/phases/I2-elexon-mocked-request-shape-and-fixture-backed-bronze-to-silver-tests/I2-01-PLAN.md`
- `.planning/phases/I1-elexon-inventory-test-scaffolding/I1-01-SUMMARY.md`
- `.planning/REQUIREMENTS.md`
- `.planning/ROADMAP.md`
- `CLAUDE.md`
- `config/sources.yaml`
- `src/gridflow/connectors/elexon/client.py`
- `src/gridflow/connectors/elexon/endpoints.py`
- `src/gridflow/bronze/writer.py`
- `src/gridflow/silver/base.py`
- `src/gridflow/silver/registry.py`
- `src/gridflow/silver/elexon/__init__.py`
- `tests/conftest.py`
- `tests/endpoints/test_endpoint_live.py`
- `tests/integration/test_elexon_mocked_e2e.py`
- `tests/integration/test_elexon_connector.py`

## Must Haves

- The plan must satisfy `ELEXON-LIVE-01`, `ELEXON-LIVE-02`, `ELEXON-LIVE-03`, `ELEXON-LIVE-04`, and `ELEXON-LIVE-05`.
- New live tests must be marked `@pytest.mark.live` and remain excluded unless the user selects `-m live`.
- The tests must not require or read `ELEXON_API_KEY`; Elexon's public Insights API is no-key for this suite.
- Live data must be written only under pytest temp data roots, never the project `data/` directory.
- At least one representative dataset from each active request-style family must be covered: `DATE_PATH`, `PUBLISH_DATETIME`/`from`-`to`, `SETTLEMENT_DATE_PERIOD`, and `NO_PARAMS`.
- Successful live responses must go through `BronzeWriter` and registered silver transformers; do not validate only direct `transform()` calls.
- Empty/no-data responses and known removed/excluded endpoints must be explicit skip/deferred/documented outcomes with source, dataset, stage, URL/status/body-preview diagnostics.
- Verification output must identify dataset, stage, request URL, HTTP status, response preview, and silver path when assertions fail.

## Tasks

<tasks>

### 1. Create Live Elexon E2E Test Module and Helpers

<read_first>

- `tests/conftest.py`
- `tests/endpoints/test_endpoint_live.py`
- `tests/integration/test_elexon_mocked_e2e.py`
- `src/gridflow/connectors/elexon/client.py`
- `src/gridflow/connectors/elexon/endpoints.py`
- `src/gridflow/bronze/writer.py`
- `src/gridflow/silver/registry.py`
- `src/gridflow/silver/elexon/__init__.py`

</read_first>

<action>

Create `tests/integration/test_elexon_live_e2e.py` with:

- `from __future__ import annotations`
- imports for `json`, `date`, `datetime`, `UTC`, `Path`, `httpx`, `polars as pl`, and `pytest`
- imports for `BronzeWriter`, `load_settings`, `ElexonConnector`, `ENDPOINTS`, `ParamStyle`, and `get_transformer`
- `import gridflow.silver.elexon  # noqa: F401` before transformer registry access
- module-level `pytestmark = pytest.mark.live`
- constants:
  - `LIVE_DATE = date(2026, 2, 1)`
  - `LIVE_START = datetime(2026, 2, 1, 0, 0, tzinfo=UTC)`
  - `LIVE_END = datetime(2026, 2, 2, 0, 0, tzinfo=UTC)`
  - `BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1"`

Add helpers:

- `_active_elexon_config()` returning `load_settings().get_source_config("elexon")`
- `_response_preview(body: bytes, limit: int = 500) -> str`
- `_silver_parquet_path(data_dir: Path, dataset: str, target_date: date) -> Path`, preserving the current `bmunits_reference` non-date-partitioned path
- `_assert_live_response(response, dataset: str, stage: str) -> None` checking non-empty body, source, dataset, HTTP 200, content type containing JSON, request URL, request params, page, and total pages
- `_assert_bronze_sidecar(bronze_path: Path, dataset: str) -> None` checking `.meta.json`, request URL, request params, API version, HTTP status, body hash, page, and total pages
- `_classify_empty_or_skip(response, dataset: str, stage: str) -> None` that parses JSON and calls `pytest.skip()` with source/dataset/stage/URL/status/body-preview if `data` is empty

Do not import or inspect `ELEXON_API_KEY`.

</action>

<verify>

<automated>uv run --extra dev ruff check tests/integration/test_elexon_live_e2e.py</automated>

</verify>

<done>

- `tests/integration/test_elexon_live_e2e.py` exists.
- The module has `pytestmark = pytest.mark.live`.
- The module imports `gridflow.silver.elexon  # noqa: F401`.
- The module does not contain `ELEXON_API_KEY`.
- Helper functions above exist and include dataset/stage diagnostics.

</done>

### 2. Add Live API Fetch and Explicit Empty/Excluded Classification

<read_first>

- `tests/integration/test_elexon_live_e2e.py`
- `tests/endpoints/test_endpoint_live.py`
- `src/gridflow/connectors/elexon/client.py`
- `src/gridflow/connectors/elexon/endpoints.py`
- `src/gridflow/connectors/elexon/parsers.py`

</read_first>

<action>

In `tests/integration/test_elexon_live_e2e.py`, add a parametrized live test named
`test_live_representative_datasets_fetch_successfully_or_classify_empty`.

Use a curated representative matrix:

| dataset | requirement coverage | expected style | expected columns |
| --- | --- | --- | --- |
| `system_prices` | ELEXON-LIVE-01, ELEXON-LIVE-02, ELEXON-LIVE-03 | `ParamStyle.DATE_PATH` | `system_sell_price`, `system_buy_price` |
| `boal` | ELEXON-LIVE-01, ELEXON-LIVE-02, ELEXON-LIVE-03 | `ParamStyle.PUBLISH_DATETIME` with `from` / `to` | `bm_unit_id`, `acceptance_number` |
| `freq` | ELEXON-LIVE-01, ELEXON-LIVE-02, ELEXON-LIVE-03 | `ParamStyle.PUBLISH_DATETIME` | `timestamp_utc`, `frequency_hz` |
| `pn` | ELEXON-LIVE-01, ELEXON-LIVE-02, ELEXON-LIVE-03 | `ParamStyle.SETTLEMENT_DATE_PERIOD` | `bm_unit_id`, `level_from`, `level_to` |
| `bmunits_reference` | ELEXON-LIVE-01, ELEXON-LIVE-02, ELEXON-LIVE-03 | `ParamStyle.NO_PARAMS` | `bm_unit_id`, `fuel_type` |

For each dataset:

- assert the dataset exists in `ENDPOINTS` and active config;
- assert the endpoint `param_style` is the expected style;
- run `async with ElexonConnector(_active_elexon_config()) as connector: responses = await connector.fetch(dataset, LIVE_START, LIVE_END)` using `LIVE_START` as the end for `DATE_PATH` and `SETTLEMENT_DATE_PERIOD` if needed to avoid two-day ranges;
- assert at least one response object exists unless the connector raises an HTTP status error classified with `pytest.skip()` and diagnostics;
- run `_assert_live_response()` on returned responses;
- call `_classify_empty_or_skip()` before writing bronze for datasets where returned JSON has an empty `data` array.

Add a second focused test named `test_live_known_excluded_endpoints_are_documented` that reads `EXCLUDED_ENDPOINTS` and asserts `bod`, `generation_by_fuel`, and `indicative_imbalance_volumes` remain documented. Do not call these endpoints in the bronze-to-silver test path.

</action>

<verify>

<automated>uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m live -q -rs</automated>

</verify>

<done>

- The live test covers `system_prices`, `boal`, `freq`, `pn`, and `bmunits_reference`.
- The live test asserts `ENDPOINTS[dataset].param_style`.
- Failures or skips include source, dataset, stage, URL/status/body preview where available.
- Removed/excluded endpoint outcomes are asserted as documented exclusions, not silent passes.

</done>

### 3. Write Live Responses Through Bronze and Transform to Silver

<read_first>

- `tests/integration/test_elexon_live_e2e.py`
- `tests/integration/test_elexon_mocked_e2e.py`
- `src/gridflow/bronze/writer.py`
- `src/gridflow/silver/base.py`
- `src/gridflow/silver/registry.py`
- `src/gridflow/silver/elexon/__init__.py`
- representative Elexon silver transformer modules for the selected datasets

</read_first>

<action>

Extend `test_live_representative_datasets_fetch_successfully_or_classify_empty` so
each non-empty representative case performs the full live API-to-silver path:

- write the selected non-empty responses with `BronzeWriter(tmp_data_dir).write(response)`;
- assert the bronze file exists under `bronze/elexon/{dataset}/...`;
- assert `.meta.json` sidecar fields using `_assert_bronze_sidecar()`;
- choose the transformer target date from `response.data_date` where present, otherwise `LIVE_DATE`;
- run `transformer = get_transformer("elexon", dataset, tmp_data_dir)`;
- run `rows_written = transformer.run(target_date)`;
- assert `rows_written > 0`;
- read parquet from `_silver_parquet_path(tmp_data_dir, dataset, target_date)`;
- assert the parquet exists, `len(df) == rows_written`, and expected columns are present;
- when `data_provider` is present in `df.columns`, assert `df["data_provider"].unique().to_list() == ["elexon"]`.

For `bmunits_reference`, preserve the current reference-data silver path
`silver/elexon/bmunits_reference/bmunits_reference.parquet` and do not require
`year=YYYY` partitions.

</action>

<verify>

<automated>uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m live -q -rs</automated>

</verify>

<done>

- Live responses are written with `BronzeWriter`.
- The test invokes `get_transformer("elexon", dataset, tmp_data_dir)`.
- The test reads live-generated silver parquet with Polars.
- Silver assertions include row counts, expected columns, and `data_provider == "elexon"` when present.
- `ELEXON-LIVE-02` and `ELEXON-LIVE-03` are covered by automated live assertions.

</done>

### 4. Prove Opt-In Behavior and Run Regression Gates

<read_first>

- `pyproject.toml`
- `tests/conftest.py`
- `tests/integration/test_elexon_live_e2e.py`
- `tests/endpoints/test_endpoint_live.py`
- `tests/integration/test_elexon_mocked_e2e.py`
- `tests/integration/test_elexon_connector.py`
- `tests/unit/test_elexon_endpoints.py`

</read_first>

<action>

Run these verification commands:

```powershell
uv run --extra dev ruff check tests/integration/test_elexon_live_e2e.py tests/endpoints/test_endpoint_live.py tests/integration/test_elexon_mocked_e2e.py
uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m "not live" -q
uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m live -q -rs
uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py -m "not live" -q
```

If an individual live dataset is unavailable or empty, keep the skip reason explicit
and ensure at least one successful live API-to-silver path remains. If all live
representative datasets fail for network/service reasons, stop and report the live
service issue rather than weakening assertions into false passes.

</action>

<verify>

<automated>uv run --extra dev ruff check tests/integration/test_elexon_live_e2e.py tests/endpoints/test_endpoint_live.py tests/integration/test_elexon_mocked_e2e.py</automated>
<automated>uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m "not live" -q</automated>
<automated>uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m live -q -rs</automated>
<automated>uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py -m "not live" -q</automated>

</verify>

<done>

- Ruff exits 0.
- Non-live run proves new live tests are excluded unless selected.
- Live run exercises the public Elexon API without an API key.
- Existing I2 mocked/inventory regression tests still pass.
- The final executor summary records any expected skips/deferred outcomes with dataset names and reasons.

</done>

</tasks>

## Threat Model

<threat_model>

| ID | Threat | Severity | Mitigation |
| --- | --- | --- | --- |
| T-I3-01 | Live tests accidentally run during normal CI/local test runs. | High | Mark the whole module live and verify `-m "not live"` excludes it. |
| T-I3-02 | Empty API responses become false green results. | High | Parse JSON, classify empty `data` explicitly, and skip with source/dataset/stage diagnostics. |
| T-I3-03 | Live responses pollute project data or overwrite user data. | High | Use pytest `tmp_data_dir` only and assert bronze paths are under the temp root. |
| T-I3-04 | API drift causes opaque failures. | Medium | Reuse and extend I1 diagnostics with URL, status, body preview, param style, and dataset stage. |
| T-I3-05 | Reference-data silver path differs from partitioned datasets. | Medium | Preserve the existing `bmunits_reference` single-file path helper from I2. |
| T-I3-06 | The suite silently starts depending on credentials. | Medium | Do not read `ELEXON_API_KEY`; rely on official no-key public API behavior and assert this in review. |

</threat_model>

## Verification

<verification>

Run:

```powershell
uv run --extra dev ruff check tests/integration/test_elexon_live_e2e.py tests/endpoints/test_endpoint_live.py tests/integration/test_elexon_mocked_e2e.py
uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m "not live" -q
uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m live -q -rs
uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py -m "not live" -q
```

</verification>

## Success Criteria

<success_criteria>

- `ELEXON-LIVE-01`: Opt-in live tests call the real public Elexon Insights API for representative active configured datasets with narrow deterministic windows and assert HTTP success, JSON shape, content size, request metadata, and pagination metadata.
- `ELEXON-LIVE-02`: Successful live responses are written through `BronzeWriter` and transformed to silver parquet for representative dataset styles/families.
- `ELEXON-LIVE-03`: Silver output assertions verify row counts, required columns, `data_provider` values where present, and schema-relevant transformer output.
- `ELEXON-LIVE-04`: Empty/no-data responses and known removed/excluded endpoints are explicit skip/deferred/documented outcomes with diagnostics.
- `ELEXON-LIVE-05`: Live tests are marked `@pytest.mark.live`, remain excluded from normal runs, require no API key, and provide source/dataset/stage/URL/status/body-preview diagnostics.
- Existing I2 non-live mocked Elexon E2E and inventory tests remain green.

</success_criteria>
