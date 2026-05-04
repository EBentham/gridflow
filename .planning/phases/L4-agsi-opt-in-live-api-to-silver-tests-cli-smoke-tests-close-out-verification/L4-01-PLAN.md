---
phase: L4
plan: L4-01
type: execute
wave: 1
depends_on:
  - L3
requirements:
  - AGSI-11
  - AGSI-12
requirements_addressed:
  - AGSI-11
  - AGSI-12
files_modified:
  - tests/integration/test_gie_agsi_live_e2e.py
  - tests/integration/test_gie_agsi_cli_live_smoke.py
  - .planning/phases/L4-agsi-opt-in-live-api-to-silver-tests-cli-smoke-tests-close-out-verification/L4-LIVE-COMMANDS.md
  - .planning/phases/L4-agsi-opt-in-live-api-to-silver-tests-cli-smoke-tests-close-out-verification/L4-01-SUMMARY.md
  - .planning/phases/L4-agsi-opt-in-live-api-to-silver-tests-cli-smoke-tests-close-out-verification/L4-VERIFICATION.md
  - .planning/REQUIREMENTS.md
  - .planning/ROADMAP.md
  - .planning/STATE.md
autonomous: true
user_setup:
  - "Set GIE_API_KEY in the environment before running -m live gates; tests must skip with explicit diagnostics when it is absent."
must_haves:
  truths:
    - "Opt-in live AGSI tests prove aggregate, country, company, and facility storage responses can be written to bronze and transformed to silver."
    - "A slow explicit full-inventory gate can validate listing-derived expected request counts while respecting the documented 60 calls/minute limit."
    - "AGSI CLI pipeline, ingest/transform, and backfill smoke tests use isolated GRIDFLOW_* paths and verify bronze plus silver outputs."
    - "Close-out artifacts record live pass/skip classifications, unavailability documentation ambiguity, and the deferred ALSI follow-up."
  artifacts:
    - path: "tests/integration/test_gie_agsi_live_e2e.py"
      provides: "Credentialed live API-to-silver AGSI coverage."
    - path: "tests/integration/test_gie_agsi_cli_live_smoke.py"
      provides: "Credentialed live CLI smoke coverage under isolated paths."
    - path: ".planning/phases/L4-agsi-opt-in-live-api-to-silver-tests-cli-smoke-tests-close-out-verification/L4-LIVE-COMMANDS.md"
      provides: "Manual command and live outcome reference."
  key_links:
    - from: "GieConnector.fetch('storage_reports')"
      to: "BronzeWriter -> get_transformer('gie_agsi', 'storage_reports') -> Polars parquet assertions"
      via: "tests/integration/test_gie_agsi_live_e2e.py"
    - from: "gridflow CLI commands"
      to: "temp GRIDFLOW_DATA_DIR / GRIDFLOW_DUCKDB_PATH / GRIDFLOW_LOG_DIR outputs"
      via: "tests/integration/test_gie_agsi_cli_live_smoke.py"
---

# L4-01 Plan: AGSI Opt-In Live API-To-Silver Tests, CLI Smoke Tests, And Close-Out Verification

<objective>
Add credentialed, opt-in AGSI live verification that proves representative real
gas storage data flows from the AGSI API through bronze and silver, proves the
public CLI paths can run in isolated temp roots, and closes v0.7 with explicit
live pass/skip classifications and traceability for `AGSI-11` and `AGSI-12`.

Purpose: finish the AGSI validation milestone by testing the real API paths
without polluting local project data or making live network checks part of the
default non-live test suite.

Output: live API-to-silver tests, live CLI smoke tests, live command docs,
summary, verification, and updated planning traceability.
</objective>

<execution_context>
@$HOME/.codex/get-shit-done/workflows/execute-plan.md
@$HOME/.codex/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/PROJECT.md
@.planning/ROADMAP.md
@.planning/REQUIREMENTS.md
@.planning/STATE.md
@.planning/research/GIE-AGSI-API-RESEARCH.md
@.planning/phases/L1-gie-agsi-endpoint-research-catalog-inventory-contract-and-expected-count-model/L1-01-SUMMARY.md
@.planning/phases/L2-agsi-query-scope-request-builder-last-page-pagination-and-bronze-completeness-tests/L2-01-SUMMARY.md
@.planning/phases/L3-agsi-silver-transformers-fixtures-mocked-e2e-count-preserving-bronze-to-silver-tests/L3-01-SUMMARY.md
@.planning/phases/L3-agsi-silver-transformers-fixtures-mocked-e2e-count-preserving-bronze-to-silver-tests/L3-VERIFICATION.md
@CLAUDE.md
@config/sources.yaml
@docs/gie_agsi_endpoint_catalog.yaml
@src/gridflow/cli.py
@src/gridflow/config/settings.py
@src/gridflow/connectors/gie/client.py
@src/gridflow/connectors/gie/endpoints.py
@src/gridflow/bronze/writer.py
@src/gridflow/silver/base.py
@src/gridflow/silver/registry.py
@src/gridflow/silver/gie/__init__.py
@tests/conftest.py
@tests/integration/test_gie_agsi_mocked_bronze.py
@tests/integration/test_gie_agsi_mocked_e2e.py
@tests/integration/test_elexon_live_e2e.py
@tests/integration/test_elexon_cli_live_smoke.py
@tests/integration/test_entsog_live_e2e.py
@tests/integration/test_entsog_cli_live_smoke.py
</context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Add credentialed AGSI live API-to-silver coverage</name>
  <files>tests/integration/test_gie_agsi_live_e2e.py</files>
  <behavior>
    - When `GIE_API_KEY` is absent, the module skips live tests with a source/dataset/stage reason instead of failing from auth setup.
    - When `GIE_API_KEY` is present, representative aggregate, country, company, and facility `storage_reports` responses are fetched from `https://agsi.gie.eu`, written through `BronzeWriter`, transformed through the registered `storage_reports` transformer, and asserted from generated parquet.
    - Company and facility live cases use a trimmed `/api/about?show=listing` payload containing one selected company/facility so representative coverage does not accidentally fan out across the full inventory.
    - The explicit full-inventory gate is marked live and opt-in, derives expected company/facility counts from listing, throttles to respect 60 calls/minute, and may skip with diagnostics when the live API returns no data for a documented window.
  </behavior>
  <action>
Create `tests/integration/test_gie_agsi_live_e2e.py` following the Elexon, ENTSOG, and NESO live E2E style.

Use `from __future__ import annotations`; imports for `json`, `os`, `date`, `datetime`, `UTC`, `TYPE_CHECKING`, `Any`, `httpx`, `polars as pl`, and `pytest`; import `gridflow.silver.gie  # noqa: F401` before registry access; import `BronzeWriter`, `SourceConfig`, `load_settings`, `GieConnector`, `QueryScope`, `build_storage_query_plan`, `expected_records_for_plan`, and `get_transformer`.

Define `LIVE_DATE = date(2026, 5, 1)`, `LIVE_START = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)`, `LIVE_END = datetime(2026, 5, 1, 0, 0, tzinfo=UTC)`, and `BASE_URL = "https://agsi.gie.eu"`. Add `_gie_config()` that loads `gie_agsi`, shortens timeout to 30 seconds, and calls `pytest.skip("source=gie_agsi stage=setup outcome=missing GIE_API_KEY")` if neither the resolved config nor `os.environ["GIE_API_KEY"]` has a key.

Add helpers equivalent to the live analogs:
- `_response_preview(body: bytes, limit: int = 500) -> str`
- `_records_from_response(response) -> list[dict[str, Any]]` reading JSON `data`
- `_silver_path(data_dir, dataset, target_date)` matching the date-partitioned `BaseSilverTransformer` layout under `silver/gie_agsi/{dataset}/year=YYYY/month=MM/{dataset}_YYYYMMDD.parquet`
- `_assert_live_response(response, dataset, stage)` checking source, dataset, HTTP 200, JSON content type, `request_url.startswith(BASE_URL)`, body, page, total pages, and auth-free diagnostics only
- `_assert_bronze_sidecar(bronze_path, dataset)` checking sidecar source, dataset, request URL, request params, api version, HTTP status, body hash/size, page, and total pages
- `_classify_empty_or_skip(response, dataset, stage)` that skips empty `data` responses with source, dataset, stage, URL, status, and body preview
- `_trim_listing_payload(payload)` that selects one company with at least one facility and returns a listing-shaped payload preserving only that company and one facility

Add `@pytest.mark.live` async test `test_live_agsi_storage_scopes_fetch_transform_or_classify_empty`, parametrized over aggregate, country, company, and facility scopes. Fetch `about_listing` once inside the company/facility cases, trim the payload, and pass it as `listing_payload` so the test exercises representative company/facility behavior without a full-inventory burst. For successful non-empty responses, write every selected response with `BronzeWriter(tmp_data_dir)`, assert sidecars, run `get_transformer("gie_agsi", "storage_reports", tmp_data_dir).run(target_date)`, read parquet with Polars, and assert rows, `data_provider == "gie_agsi"`, plus `entity_level` includes the expected scope value.

Add `@pytest.mark.live` async test `test_live_agsi_unavailability_fetches_or_classifies_documented_ambiguity` that calls `unavailability` for the same narrow window, writes/transforms non-empty responses, or skips with diagnostics that explicitly mention the documented unavailability ambiguity from `docs/gie_agsi_endpoint_catalog.yaml`.

Add `@pytest.mark.live` async test `test_live_agsi_full_inventory_expected_counts_gate` that is deliberately slower and explicit: fetch listing, build company and facility query plans from the trimmed-or-full listing depending on a local constant such as `FULL_INVENTORY = False`, assert `expected_records_for_plan(plan)` is greater than zero, and document in the skip/pass reason that true full inventory should stay opt-in because GIE documents 60 calls/minute. Do not make this test call every company/facility unless a local constant or environment flag clearly enables the slow path.
  </action>
  <verify>
    <automated>uv run --extra dev ruff check tests/integration/test_gie_agsi_live_e2e.py</automated>
    <automated>uv run --extra dev pytest tests/integration/test_gie_agsi_live_e2e.py -m "not live" -q</automated>
    <automated>uv run --extra dev pytest tests/integration/test_gie_agsi_live_e2e.py -m live -q -rs</automated>
  </verify>
  <done>
`AGSI-11` has live API-to-silver coverage for aggregate, country, company, and facility storage scopes; live failures include source/dataset/stage diagnostics; the suite remains excluded by `-m "not live"`; and no response is written outside `tmp_data_dir`.
  </done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Add isolated AGSI CLI live smoke tests and command docs</name>
  <files>tests/integration/test_gie_agsi_cli_live_smoke.py, .planning/phases/L4-agsi-opt-in-live-api-to-silver-tests-cli-smoke-tests-close-out-verification/L4-LIVE-COMMANDS.md</files>
  <behavior>
    - CLI smoke tests invoke `gridflow` through `Typer` `CliRunner` with real user-facing command arguments.
    - Every live command sets `GRIDFLOW_DATA_DIR`, `GRIDFLOW_DUCKDB_PATH`, and `GRIDFLOW_LOG_DIR` under pytest `tmp_path` before invoking the CLI.
    - Pipeline, separate ingest/transform, and backfill commands verify bronze and silver outputs for `gie_agsi/storage_reports`.
    - A non-live sentinel proves `L4-LIVE-COMMANDS.md` covers command strings, isolated environment setup, selected windows, expected skips, AGSI-11, AGSI-12, unavailability ambiguity, and ALSI follow-up.
  </behavior>
  <action>
Create `tests/integration/test_gie_agsi_cli_live_smoke.py` using the Elexon/ENTSOG/NESO CLI smoke pattern. Include `from __future__ import annotations`; imports for `dataclass`, `os`, `Path`, `pytest`, and `CliRunner`; import `app` from `gridflow.cli`; define `START = "2026-05-01"`, `END = "2026-05-01"`, `CURATED_DATASET = "storage_reports"`, and `runner = CliRunner()`.

Add a `CliSmokePaths` dataclass with `data_dir`, `duckdb_path`, and `log_dir`. Add `_isolated_env(tmp_path, monkeypatch)` that sets all three `GRIDFLOW_*` paths under `tmp_path`. Add `_require_gie_key()` that skips with `source=gie_agsi stage=setup outcome=missing GIE_API_KEY` if the environment has no key. Add `_invoke_cli(args)` that asserts exit code 0 and includes the full command, output, and exception in failures. Add `_assert_outputs(paths, dataset, tmp_path)` that checks `bronze/gie_agsi/{dataset}/raw_*.json` and `silver/gie_agsi/{dataset}/**/*.parquet`, and asserts all found files live under `tmp_path`.

Add live tests:
- `test_live_pipeline_gie_agsi_storage_reports_creates_bronze_and_silver`: invoke `["pipeline", "gie_agsi", CURATED_DATASET, "--start", START, "--end", END]`; assert output includes `Pipeline: gie_agsi`, `Bronze (ingest)`, `Silver (transform)`, and `Pipeline complete`; verify outputs.
- `test_live_ingest_then_transform_gie_agsi_storage_reports_creates_outputs`: invoke `ingest` then `transform` for the same dataset/window and verify outputs after each stage.
- `test_live_backfill_gie_agsi_storage_reports_creates_outputs`: invoke `["backfill", "gie_agsi", CURATED_DATASET, "--start", START, "--end", END, "--chunk-days", "1"]`; assert output includes `Backfilling gie_agsi/storage_reports` and `Backfill complete`; verify outputs.

Create `L4-LIVE-COMMANDS.md` with PowerShell isolated environment setup, exact `uv run gridflow pipeline`, `ingest`, `transform`, and `backfill` commands for `gie_agsi storage_reports`, exact pytest live/non-live commands, the selected 2026-05-01 gas-day window, expected pass/skip classification rules, GIE API-key setup, the 60 calls/minute rate-limit note, the unavailability documentation ambiguity, and the ALSI LNG follow-up deferral.

Add non-live sentinel `test_l4_live_command_documentation_covers_closeout_requirements` that reads `L4-LIVE-COMMANDS.md` and asserts it contains `pipeline`, `ingest`, `transform`, `backfill`, `GIE_API_KEY`, `GRIDFLOW_DATA_DIR`, `GRIDFLOW_DUCKDB_PATH`, `GRIDFLOW_LOG_DIR`, `AGSI-11`, `AGSI-12`, `unavailability`, and `ALSI`.
  </action>
  <verify>
    <automated>uv run --extra dev ruff check tests/integration/test_gie_agsi_cli_live_smoke.py</automated>
    <automated>uv run --extra dev pytest tests/integration/test_gie_agsi_cli_live_smoke.py -m "not live" -q</automated>
    <automated>uv run --extra dev pytest tests/integration/test_gie_agsi_cli_live_smoke.py -m live -q -rs</automated>
  </verify>
  <done>
`AGSI-12` is covered by isolated live CLI assertions for pipeline, ingest/transform, and backfill; command docs are checked by a non-live sentinel; and no CLI smoke output lands under the normal project `data/`, DuckDB, or `logs/` paths.
  </done>
</task>

<task type="auto">
  <name>Task 3: Run final gates and close out L4 traceability</name>
  <files>.planning/phases/L4-agsi-opt-in-live-api-to-silver-tests-cli-smoke-tests-close-out-verification/L4-01-SUMMARY.md, .planning/phases/L4-agsi-opt-in-live-api-to-silver-tests-cli-smoke-tests-close-out-verification/L4-VERIFICATION.md, .planning/REQUIREMENTS.md, .planning/ROADMAP.md, .planning/STATE.md</files>
  <action>
Run the focused gates:

```powershell
uv run --extra dev ruff check tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py
uv run --extra dev pytest tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py -m "not live" -q
uv run --extra dev pytest tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py -m live -q -rs
uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q
```

If live gates skip because `GIE_API_KEY` is missing, record that as a human-needed live verification outcome and do not mark `AGSI-11` or `AGSI-12` complete. If live gates run and classify individual no-data outcomes, require at least one successful storage API-to-silver path and one successful CLI path before marking the phase complete.

After successful live evidence exists, create `L4-01-SUMMARY.md` with command outcomes, created artifacts, live pass/skip classifications, unavailability ambiguity notes, and ALSI follow-up. Create `L4-VERIFICATION.md` with goal-backward verification for `AGSI-11` and `AGSI-12`. Update `.planning/REQUIREMENTS.md` to mark `AGSI-11` and `AGSI-12` complete only if the live evidence passed. Update `.planning/ROADMAP.md` to mark L4 complete and preserve the close-out notes. Update `.planning/STATE.md` to move v0.7 to complete or ready for milestone audit, following the current state style.
  </action>
  <verify>
    <automated>uv run --extra dev ruff check tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py</automated>
    <automated>uv run --extra dev pytest tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py -m "not live" -q</automated>
    <automated>uv run --extra dev pytest tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py -m live -q -rs</automated>
    <automated>uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q</automated>
  </verify>
  <done>
Summary and verification artifacts exist; `AGSI-11` and `AGSI-12` are marked complete only with successful credentialed evidence; roadmap/state traceability is current; and existing L1-L3 non-live AGSI regression gates remain green.
  </done>
</task>

</tasks>

<threat_model>
## Trust Boundaries

| Boundary | Description |
|----------|-------------|
| Local tests to GIE AGSI API | Credentialed live HTTP requests leave the local machine and depend on provider auth, rate limits, and data availability. |
| Live API response to bronze storage | External JSON bytes are written to local temp bronze files with sidecar metadata. |
| CLI process to filesystem | User-facing commands create bronze, silver, DuckDB, and log artifacts that must stay under isolated temp roots. |

## STRIDE Threat Register

| Threat ID | Category | Component | Disposition | Mitigation Plan |
|-----------|----------|-----------|-------------|-----------------|
| T-L4-01 | Information Disclosure | `GIE_API_KEY` / live diagnostics | mitigate | Never print the key; skip on missing key with generic diagnostics; assert sidecars and docs do not include secret values. |
| T-L4-02 | Denial of Service | GIE AGSI live API | mitigate | Keep representative live tests narrow, keep full inventory explicitly slow/opt-in, and preserve `rate_limit_per_second: 1` / 60 calls per minute guidance. |
| T-L4-03 | Tampering | Project data directory | mitigate | Set and assert `GRIDFLOW_DATA_DIR`, `GRIDFLOW_DUCKDB_PATH`, and `GRIDFLOW_LOG_DIR` under pytest temp paths for CLI smoke tests. |
| T-L4-04 | Repudiation | Live pass/skip outcomes | mitigate | Record source, dataset, stage, URL/status/body preview classifications in test skips/failures and summarize them in `L4-01-SUMMARY.md`. |
| T-L4-05 | Reliability | Empty or ambiguous live responses | mitigate | Treat empty storage responses as explicit skips with diagnostics, require at least one successful storage API-to-silver path before phase completion, and document unavailability ambiguity. |
</threat_model>

<verification>
Run:

```powershell
uv run --extra dev ruff check tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py
uv run --extra dev pytest tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py -m "not live" -q
uv run --extra dev pytest tests/integration/test_gie_agsi_live_e2e.py tests/integration/test_gie_agsi_cli_live_smoke.py -m live -q -rs
uv run --extra dev pytest tests/unit/test_gie.py tests/unit/test_gie_endpoint_catalog.py tests/integration/test_gie_agsi_mocked_bronze.py tests/integration/test_gie_agsi_mocked_e2e.py -m "not live" -q
```
</verification>

<success_criteria>
- `AGSI-11`: Opt-in live AGSI tests use `GIE_API_KEY`, call real AGSI representative aggregate/country/company/facility storage scopes, write live responses through bronze, transform to silver parquet, and classify no-data/error outcomes explicitly.
- `AGSI-11`: A slower explicit full-inventory live gate can validate listing-derived expected request counts while respecting GIE's documented 60 calls/minute limit.
- `AGSI-12`: Opt-in live CLI smoke tests run pipeline, ingest/transform, and backfill commands under isolated `GRIDFLOW_DATA_DIR`, `GRIDFLOW_DUCKDB_PATH`, and `GRIDFLOW_LOG_DIR` paths and verify bronze plus silver outputs.
- Close-out docs record live pass/skip classifications, API documentation ambiguity around unavailability, and the ALSI LNG follow-up.
- L1-L3 non-live AGSI request-shape, pagination, inventory, fixture-backed bronze-to-silver, and transformer regression gates remain green.
</success_criteria>

<output>
After completion, create `.planning/phases/L4-agsi-opt-in-live-api-to-silver-tests-cli-smoke-tests-close-out-verification/L4-01-SUMMARY.md`.
</output>
