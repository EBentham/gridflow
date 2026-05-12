---
phase: I3
slug: elexon-live-api-to-silver-test-suite
status: complete
created: 2026-05-04
requirements:
  - ELEXON-LIVE-01
  - ELEXON-LIVE-02
  - ELEXON-LIVE-03
  - ELEXON-LIVE-04
  - ELEXON-LIVE-05
---

# I3 Research - Elexon Live API to Silver Test Suite

## Research Complete

Phase I3 should add opt-in live tests that call the public Elexon Insights API,
write successful responses through `BronzeWriter`, transform them with registered
silver transformers, and assert schema-relevant silver outputs. It should not add
new production datasets or move live checks into normal test runs.

## Source Artifacts Read

- `.planning/ROADMAP.md`
- `.planning/REQUIREMENTS.md`
- `.planning/STATE.md`
- `.planning/phases/I1-elexon-inventory-test-scaffolding/I1-01-SUMMARY.md`
- `.planning/phases/I2-elexon-mocked-request-shape-and-fixture-backed-bronze-to-silver-tests/I2-01-SUMMARY.md`
- `.planning/phases/I2-elexon-mocked-request-shape-and-fixture-backed-bronze-to-silver-tests/I2-01-PLAN.md`
- `CLAUDE.md`
- `config/sources.yaml`
- `src/gridflow/connectors/elexon/client.py`
- `src/gridflow/connectors/elexon/endpoints.py`
- `src/gridflow/bronze/writer.py`
- `src/gridflow/silver/base.py`
- `src/gridflow/silver/registry.py`
- `tests/conftest.py`
- `tests/endpoints/test_endpoint_live.py`
- `tests/integration/test_elexon_mocked_e2e.py`
- `tests/integration/test_elexon_connector.py`
- `tests/fixtures/elexon/*.json`

## Official API Notes

The official Elexon Insights Developer Portal says the REST APIs provide
production data, are public, require no API key, and publish OpenAPI definitions
plus examples and parameter definitions:

- https://developer.data.elexon.co.uk/

The same portal documents the relevant API patterns for this phase:

- dataset endpoints can be queried by published times and can return JSON;
- opinionated endpoints include derived paths such as demand/generation forecasts;
- streaming endpoints are JSON only and are not the target of this phase;
- reference endpoints provide standing data such as `/reference/bmunits/all`.

Elexon's BSC site describes the Insights Solution as the BMRA data service for
GB wholesale electricity data and confirms the service exposes website, REST API,
and IRIS access patterns. I3 should use only the REST API surface already modeled
by `ElexonConnector`.

- https://www.elexon.co.uk/bsc/data/kinnect-insights-solution/

## Key Findings

### Existing Contract

I1 established:

- active Elexon datasets are defined by `load_settings().get_source_config("elexon").datasets`;
- `ENDPOINTS` is the request-style source of truth;
- excluded/decommissioned datasets live in `EXCLUDED_ENDPOINTS`;
- `tests/endpoints/test_endpoint_live.py` already has Elexon live diagnostics with
  source, dataset, stage, parameter style, URL, status, and bounded body preview.

I2 established:

- registry-driven mocked request-shape coverage for every active configured dataset;
- fixture-backed `BronzeWriter` to silver integration tests for representative
  Elexon transformer families;
- helper patterns for bronze metadata, pagination, no-param responses, and silver
  parquet assertions in `tests/integration/test_elexon_mocked_e2e.py`.

I3 should reuse these patterns rather than creating a second live framework.

### Live Dataset Selection

Live API-to-silver coverage should be representative, not exhaustive, because the
phase goal is to prove live responses flow through bronze and silver for each major
parameter style and transformer family while keeping the run short and deterministic.

Recommended representative set:

| Dataset | Style / family | Why |
| --- | --- | --- |
| `system_prices` | `DATE_PATH` settlement data | stable daily endpoint and core settlement price transformer |
| `boal` | `from` / `to` dataset endpoint | validates BOALF replacement path and balancing acceptance transformer |
| `freq` | standard publish datetime | compact system frequency payload and timestamp transformer |
| `pn` | settlement date + period | validates period iteration and physical-notification transformer |
| `bmunits_reference` | no params reference data | validates static reference endpoint and non-date silver output |

If one representative dataset is temporarily empty or unavailable, the tests should
classify it with an explicit skip/deferred reason and keep the outcome visible.

### Live Response Handling

Live tests should:

- use narrow windows such as 2026-02-01 or a recent deterministic fallback only if
  an endpoint returns no data;
- call `ElexonConnector.fetch()` inside its async context manager;
- assert each returned `RawResponse` has `source`, `dataset`, `http_status`, content
  type, request URL, request params, page, total pages, and non-empty body;
- write only selected non-empty responses through `BronzeWriter(tmp_data_dir)`;
- run registered transformers with `get_transformer("elexon", dataset, tmp_data_dir)`;
- read the generated parquet with Polars and assert rows, expected columns, and
  `data_provider == "elexon"` where the transformer currently emits it;
- for empty/no-data responses, call `pytest.skip()` or record an expected empty
  classification with source/dataset/stage diagnostics.

### Marker and Run Behavior

`tests/conftest.py` already makes `@pytest.mark.live` tests opt-in unless the user
selects `-m live`. I3 should keep all new live tests in a live-marked module or
class and verify exclusion with both collection and normal non-live test commands.

No Elexon API key is required. Tests must not reference `ELEXON_API_KEY`.

## Validation Architecture

Fast non-live guard:

```powershell
uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m "not live" -q
```

This should collect the module and skip all live tests through the existing
collection gate.

Live verification:

```powershell
uv run --extra dev pytest tests/integration/test_elexon_live_e2e.py -m live -q -rs
```

Regression guard:

```powershell
uv run --extra dev ruff check tests/integration/test_elexon_live_e2e.py tests/endpoints/test_endpoint_live.py tests/integration/test_elexon_mocked_e2e.py
uv run --extra dev pytest tests/integration/test_elexon_mocked_e2e.py tests/integration/test_elexon_connector.py tests/unit/test_elexon_endpoints.py -m "not live" -q
```

## Risks and Mitigations

| Risk | Mitigation |
| --- | --- |
| Live API temporarily unavailable | Keep tests opt-in, use clear `pytest.skip()` classifications with dataset, URL, status, and body preview. |
| No-data windows create false passes | Require explicit classification for empty responses and assert at least one live bronze-to-silver case per major family when data exists. |
| Live tests pollute local project data | Use `tmp_data_dir` only; never write to configured project `data/`. |
| Tests accidentally require credentials | Assert no `ELEXON_API_KEY` dependency and rely on the official public no-key API behavior. |
| Transformer paths differ for reference data | Preserve I2's `bmunits_reference` non-partitioned silver path handling. |
| Run becomes too slow or noisy | Use a curated representative dataset set and narrow windows. |

## Recommended Plan Shape

One Wave 1 plan is sufficient:

- `I3-01`: create `tests/integration/test_elexon_live_e2e.py`, add live helper
  functions, cover representative live API-to-bronze-to-silver cases, classify
  empty/deferred outcomes, and prove live tests remain opt-in.
