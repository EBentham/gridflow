---
phase: H2
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - pyproject.toml
  - tests/integration/test_entsoe_mocked_e2e.py
autonomous: true
requirements:
  - MOCK-01
  - MOCK-02
  - MOCK-03

must_haves:
  truths:
    - "`tests/integration/test_entsoe_mocked_e2e.py` verifies URL/query shape for every dataset in `DOC_TYPES`."
    - "URL coverage asserts the configured ENTSO-E source and `DOC_TYPES` contain the same 16 datasets."
    - "Zone-style datasets send `in_Domain.mRID` and `out_Domain.mRID`, not `controlArea_Domain.mRID`."
    - "Control-area balancing datasets send `controlArea_Domain.mRID`, not `in_Domain.mRID`."
    - "Every request includes `documentType`, `periodStart`, `periodEnd`, and `securityToken`; `processType` appears only when configured."
    - "Bronze-to-silver integration writes realistic XML fixtures through `BronzeWriter` and runs real ENTSO-E transformers."
    - "Representative bronze-to-silver coverage includes `day_ahead_prices`, `actual_load`, `cross_border_flows`, and `imbalance_prices`."
    - "Windows UTC timezone conversion is supported by adding `tzdata` to project dependencies."
  artifacts:
    - path: "pyproject.toml"
      provides: "Windows timezone database dependency for Polars UTC conversion"
      contains: "tzdata"
    - path: "tests/integration/test_entsoe_mocked_e2e.py"
      provides: "Mocked ENTSO-E URL construction and bronze-to-silver integration tests"
      exports: ["TestEntsoeUrlConstructionAllDatasets", "TestEntsoeBronzeToSilverPipeline"]
  key_links:
    - from: "tests/integration/test_entsoe_mocked_e2e.py"
      to: "src/gridflow/connectors/entsoe/client.py"
      via: "EntsoeConnector.fetch"
      pattern: "EntsoeConnector"
    - from: "tests/integration/test_entsoe_mocked_e2e.py"
      to: "src/gridflow/bronze/writer.py"
      via: "BronzeWriter.write"
      pattern: "BronzeWriter"
    - from: "tests/integration/test_entsoe_mocked_e2e.py"
      to: "src/gridflow/silver/entsoe/*.py"
      via: "real transformer run()"
      pattern: "Transformer"
---

<objective>
Add mocked ENTSO-E E2E validation that proves request construction for all 16 ENTSO-E
datasets and proves a representative bronze-to-silver path using realistic XML fixtures.

Purpose: satisfy MOCK-01, MOCK-02, and MOCK-03 without touching the live ENTSO-E API.

Output:
- `pyproject.toml` includes `tzdata` so Windows runners can convert Polars UTC columns.
- `tests/integration/test_entsoe_mocked_e2e.py` covers all 16 URL shapes and representative
  bronze-to-silver fixture runs.
</objective>

<execution_context>
@.planning/PROJECT.md
@.planning/ROADMAP.md
@.planning/REQUIREMENTS.md
@.planning/STATE.md
@.planning/phases/H2-entsoe-mocked-e2e-tests/H2-RESEARCH.md
@.planning/phases/H2-entsoe-mocked-e2e-tests/H2-PATTERNS.md
</execution_context>

<context>
H2 is ENTSO-E-only. Do not add mocked E2E coverage for Elexon, ENTSO-G, GIE, NESO, or
Open-Meteo in this phase.

Known blocker: full-suite collection may still fail because `src/gridflow/silver/elexon/__init__.py`
imports missing Elexon modules. Do not fix that in H2 unless the user explicitly redirects scope.
Run and report the full-suite attempt, but use the targeted H2/ENTSO-E command as the H2 gate.
</context>

<tasks>

<task type="auto">
  <name>Task 1: Add Windows timezone data dependency</name>
  <read_first>
    - pyproject.toml
    - .planning/phases/H2-entsoe-mocked-e2e-tests/H2-RESEARCH.md
  </read_first>
  <files>pyproject.toml</files>
  <action>
Add `tzdata>=2024.1` to the main `[project].dependencies` list. Keep the existing style:
one quoted dependency per line, trailing comma.

Rationale: ENTSO-E transformers use Polars `Datetime("us", "UTC")`; on Windows, Python's
stdlib zoneinfo may not have the IANA timezone database unless `tzdata` is installed.
  </action>
  <verify>
    <automated>Select-String -Path pyproject.toml -Pattern 'tzdata>=2024.1'</automated>
  </verify>
  <acceptance_criteria>
    - `pyproject.toml` contains `tzdata>=2024.1`
    - No unrelated dependencies are added
    - Do not commit `uv.lock` unless it was already tracked; in this repo it is currently untracked
  </acceptance_criteria>
  <done>Timezone dependency added.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Add mocked ENTSO-E URL construction coverage for all 16 datasets</name>
  <read_first>
    - tests/integration/test_entsoe_connector.py
    - src/gridflow/connectors/entsoe/client.py
    - src/gridflow/connectors/entsoe/endpoints.py
    - config/sources.yaml
  </read_first>
  <files>tests/integration/test_entsoe_mocked_e2e.py</files>
  <action>
Create `tests/integration/test_entsoe_mocked_e2e.py`.

Imports must include:
```python
from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import polars as pl
import pytest
import respx

from gridflow.bronze.writer import BronzeWriter
from gridflow.config.settings import SourceConfig, load_settings
from gridflow.connectors.base import RawResponse
from gridflow.connectors.entsoe.client import EntsoeConnector
from gridflow.connectors.entsoe.endpoints import DOC_TYPES
```

Also import the four representative transformer classes directly from their concrete modules:
`DayAheadPricesTransformer`, `ActualLoadTransformer`, `CrossBorderFlowsTransformer`, and
`ImbalancePricesTransformer`.

Use constants:
```python
FIXTURES = Path(__file__).parent.parent / "fixtures" / "entsoe"
ENTSOE_BASE = "https://web-api.tp.entsoe.eu"
TARGET_DATE = date(2024, 1, 15)
START = datetime(2024, 1, 15, tzinfo=UTC)
END = datetime(2024, 1, 16, tzinfo=UTC)
ZONE_PAIR_DATASETS = {"cross_border_flows", "net_transfer_capacity"}
```

Create a fixture:
```python
@pytest.fixture
def entsoe_source_config() -> SourceConfig:
    source = load_settings().get_source_config("entsoe")
    return source.model_copy(update={"api_key": "test-token", "timeout": 5})
```

Create `TestEntsoeUrlConstructionAllDatasets`:

1. `test_config_and_doc_types_cover_same_16_datasets`
   - `configured = set(load_settings().get_source_config("entsoe").datasets)`
   - `registered = set(DOC_TYPES)`
   - assert both lengths are `16`
   - assert `configured == registered`

2. `test_url_shape_for_every_dataset`
   - Parametrize with `sorted(DOC_TYPES)`.
   - Use `@respx.mock` and `@pytest.mark.asyncio`.
   - Mock `respx.get(f"{ENTSOE_BASE}/api")` with a basic XML response:
     `httpx.Response(200, content=b"<root />", headers={"content-type": "text/xml"})`
   - Fetch the dataset with `EntsoeConnector`.
   - Assert at least one response and at least one mocked call.
   - For every call, inspect `dict(call.request.url.params)`.
   - Assert `documentType`, `periodStart`, `periodEnd`, and `securityToken`.
   - Assert `processType` appears only when `DOC_TYPES[dataset].process_type` is not `None`.
   - If `DOC_TYPES[dataset].domain_style == "control_area"`, assert
     `controlArea_Domain.mRID` exists and `in_Domain.mRID` / `out_Domain.mRID` do not.
   - Otherwise assert `in_Domain.mRID` and `out_Domain.mRID` exist and
     `controlArea_Domain.mRID` does not.
   - For `dataset in ZONE_PAIR_DATASETS`, assert at least one call has
     `in_Domain.mRID != out_Domain.mRID`.
  </action>
  <verify>
    <automated>uv run --extra dev pytest tests/integration/test_entsoe_mocked_e2e.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - URL-shape test parametrizes over all 16 datasets in `DOC_TYPES`
    - Test asserts config and connector registry agree on the same 16 datasets
    - No live HTTP requests are made
    - `uv run --extra dev pytest tests/integration/test_entsoe_mocked_e2e.py -x -q` reaches the URL tests
  </acceptance_criteria>
  <done>All 16 ENTSO-E URL shapes are covered by mocked integration tests.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Add representative bronze-to-silver mocked pipeline coverage</name>
  <read_first>
    - tests/integration/test_bronze_to_silver.py
    - src/gridflow/bronze/writer.py
    - src/gridflow/silver/base.py
    - src/gridflow/silver/entsoe/day_ahead_prices.py
    - src/gridflow/silver/entsoe/actual_load.py
    - src/gridflow/silver/entsoe/cross_border_flows.py
    - src/gridflow/silver/entsoe/imbalance_prices.py
  </read_first>
  <files>tests/integration/test_entsoe_mocked_e2e.py</files>
  <action>
In the same test file, add helper functions:

```python
def _silver_path(data_dir: Path, dataset: str, target_date: date) -> Path:
    return (
        data_dir
        / "silver"
        / "entsoe"
        / dataset
        / f"year={target_date.year}"
        / f"month={target_date.month:02d}"
        / f"{dataset}_{target_date:%Y%m%d}.parquet"
    )


def _write_fixture_to_bronze(tmp_data_dir: Path, dataset: str, fixture_name: str) -> Path:
    body = (FIXTURES / fixture_name).read_bytes()
    response = RawResponse(
        body=body,
        content_type="text/xml",
        source="entsoe",
        dataset=dataset,
        fetched_at=datetime(2024, 1, 15, 12, 0, tzinfo=UTC),
        request_url=f"{ENTSOE_BASE}/api",
        request_params={"documentType": DOC_TYPES[dataset].document_type},
        api_version="v1",
        http_status=200,
        data_date=TARGET_DATE,
    )
    return BronzeWriter(tmp_data_dir).write(response)
```

Add `TestEntsoeBronzeToSilverPipeline` with a parametrized test over:

```python
pytest.param("day_ahead_prices", "day_ahead_prices_gb.xml", DayAheadPricesTransformer,
             {"timestamp_utc", "area_code", "price_eur_mwh"}, id="day_ahead_prices"),
pytest.param("actual_load", "actual_load_gb.xml", ActualLoadTransformer,
             {"timestamp_utc", "area_code", "load_mw"}, id="actual_load"),
pytest.param("cross_border_flows", "cross_border_flows_gb_fr.xml", CrossBorderFlowsTransformer,
             {"timestamp_utc", "in_area_code", "out_area_code", "flow_mw"}, id="cross_border_flows"),
pytest.param("imbalance_prices", "imbalance_prices_gb.xml", ImbalancePricesTransformer,
             {"timestamp_utc", "area_code", "direction", "price_eur_mwh"}, id="imbalance_prices"),
```

Test body:
- write the fixture to bronze
- assert the bronze XML path and `.meta.json` sidecar exist
- instantiate `transformer_cls(tmp_data_dir)`
- run `rows = transformer.run(TARGET_DATE)`
- assert `rows > 0`
- assert the expected silver parquet path exists
- read with `pl.read_parquet`
- assert `len(df) == rows`
- assert required columns are present
- assert `data_provider` exists and has only `"entsoe"` when present
  </action>
  <verify>
    <automated>uv run --extra dev pytest tests/integration/test_entsoe_mocked_e2e.py -x -q</automated>
  </verify>
  <acceptance_criteria>
    - Representative pipeline covers price, quantity, zone, zone-pair, and control-area behavior
    - Tests use realistic XML fixtures from `tests/fixtures/entsoe/`
    - Tests use real `BronzeWriter` and real ENTSO-E transformer `run()`
    - No live HTTP requests are made
  </acceptance_criteria>
  <done>Representative bronze-to-silver mocked E2E path is covered.</done>
</task>

<task type="auto">
  <name>Task 4: Run phase verification and record known blocker if present</name>
  <read_first>
    - .planning/phases/H2-entsoe-mocked-e2e-tests/H2-VALIDATION.md
    - .planning/STATE.md
  </read_first>
  <files>.planning/phases/H2-entsoe-mocked-e2e-tests/H2-01-SUMMARY.md</files>
  <action>
Run:

```powershell
uv run --extra dev pytest tests/integration/test_entsoe_mocked_e2e.py -x -q
uv run --extra dev pytest tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_connector.py tests/unit/test_entsoe.py -x -q
uv run --extra dev pytest -x -q
```

If the full suite fails only because of the known Elexon import blocker, record that in the
summary as a pre-existing blocker. Do not edit Elexon files in H2.

Create `H2-01-SUMMARY.md` after implementation using the project summary template.
  </action>
  <verify>
    <automated>Test-Path .planning/phases/H2-entsoe-mocked-e2e-tests/H2-01-SUMMARY.md</automated>
  </verify>
  <acceptance_criteria>
    - Quick H2 command passes
    - Phase ENTSO-E command passes or any failure is a clearly documented pre-existing environment issue
    - Full-suite attempt is recorded honestly
    - Summary lists commits and verification outputs
  </acceptance_criteria>
  <done>Phase verification recorded.</done>
</task>

</tasks>

<verification>

Before H2 is marked complete:

1. `Select-String -Path pyproject.toml -Pattern 'tzdata>=2024.1'` exits 0.
2. `uv run --extra dev pytest tests/integration/test_entsoe_mocked_e2e.py -x -q` passes.
3. `uv run --extra dev pytest tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_connector.py tests/unit/test_entsoe.py -x -q` passes, or any remaining failure is documented as a pre-existing non-H2 environment issue.
4. `uv run --extra dev pytest -x -q` is attempted and result recorded.
5. `git diff --name-only` for implementation is limited to `pyproject.toml`, `tests/integration/test_entsoe_mocked_e2e.py`, and H2 summary/verification docs.

</verification>

<success_criteria>

- MOCK-01: URL construction is validated for every ENTSO-E dataset without live network calls.
- MOCK-02: Bronze-to-silver pipeline runs for representative realistic XML fixtures.
- MOCK-03: URL-shape coverage spans all 16 configured/registered ENTSO-E datasets.
- H2 does not broaden into non-ENTSO-E E2E coverage.
- Known unrelated full-suite blockers are documented, not hidden.

</success_criteria>
