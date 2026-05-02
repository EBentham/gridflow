---
phase: H3
slug: live-entsoe-test-suite
status: complete
created: 2026-05-02
---

# Phase H3 - Pattern Map

## Closest Existing Patterns

| New H3 concern | Existing analog | Pattern to reuse |
|----------------|-----------------|------------------|
| all-dataset ENTSO-E iteration | `tests/integration/test_entsoe_mocked_e2e.py` | drive coverage from `sorted(DOC_TYPES)` and assert config/registry agreement |
| bronze-to-silver E2E | `TestEntsoeBronzeToSilverPipeline` | write `RawResponse` through `BronzeWriter`, run real transformer, read parquet |
| temporary pipeline data | `tests/conftest.py::tmp_data_dir` | isolate `bronze`, `silver`, `gold`, logs, and DuckDB under pytest temp paths |
| CLI dataset alias coverage | `tests/unit/test_cli_resolve_datasets.py` | direct helper tests for pure logic; command tests for integration behavior |
| ENTSO-E transformer registration | `src/gridflow/silver/entsoe/__init__.py` | import provider package to register all 16 transformer classes |
| connector live fetch | `src/gridflow/connectors/entsoe/client.py` | exercise public `EntsoeConnector.fetch()` with real `SourceConfig` |

## Reusable Files

- `tests/integration/test_entsoe_mocked_e2e.py` - helper style and all-dataset assertions.
- `tests/conftest.py` - `tmp_data_dir` fixture.
- `src/gridflow/cli.py` - command behavior and failure propagation target.
- `src/gridflow/config/settings.py` - settings resolution; inspect before deciding whether tests can isolate with temp config alone.
- `src/gridflow/connectors/entsoe/endpoints.py` - `DOC_TYPES`, dataset domain style, document/process types.
- `src/gridflow/silver/entsoe/__init__.py` - import surface for all ENTSO-E transformers.
- `src/gridflow/silver/registry.py` - transformer lookup for dataset-level live transform checks.

## Planning Notes

- Prefer adding a focused live integration module over broad changes to existing mocked tests.
- Keep live tests opt-in and skip without `ENTSOE_API_KEY`.
- Hard-fail semantics require code support in CLI, not just assertions.
- Use temporary config/data roots in command tests to avoid writing to normal `./data`.
- Do not scope Elexon fixes into H3, but preserve the follow-up note.
