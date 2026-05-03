---
phase: H6
plan: 01
type: execute
wave: 2
depends_on:
  - H5-01
files_modified:
  - src/gridflow/connectors/entsoe/endpoints.py
  - src/gridflow/connectors/entsoe/client.py
  - src/gridflow/connectors/entsoe/parsers.py
  - src/gridflow/schemas/entsoe.py
  - src/gridflow/silver/entsoe/
  - config/sources.yaml
  - docs/entsoe_endpoint_catalog.yaml
  - tests/fixtures/entsoe/
  - tests/unit/test_entsoe.py
  - tests/unit/test_entsoe_endpoint_catalog.py
  - tests/integration/test_entsoe_mocked_e2e.py
  - tests/integration/test_entsoe_live.py
autonomous: true
requirements:
  - SRC-TX-01
  - SRC-TX-02
  - SRC-TX-03
  - SRC-TX-04
  - COVER-03
  - LIVE-05
---

<objective>
Add the H6 ENTSO-E transmission and market source batch while keeping request
construction metadata-driven and parser/schema changes grouped by payload family.
</objective>

<execution_context>
@.planning/phases/H6-entsoe-transmission-market-sources/H6-CONTEXT.md
@docs/entsoe_endpoint_catalog.yaml
@src/gridflow/connectors/entsoe/endpoints.py
@src/gridflow/connectors/entsoe/client.py
@src/gridflow/connectors/entsoe/parsers.py
@src/gridflow/schemas/entsoe.py
@src/gridflow/silver/entsoe/
@config/sources.yaml
@tests/integration/test_entsoe_mocked_e2e.py
@tests/integration/test_entsoe_live.py
</execution_context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Add request metadata primitives for H6 filters</name>
  <action>
Support documented H6 optional filters through endpoint metadata: auction
category/type, contract market agreement type, update date/time, offset, and
business type variants. Preserve exact ENTSO-E parameter casing where the
catalog documents mixed-case names.
  </action>
  <done>Adding an H6 dataset requires metadata, not a new connector branch, unless the payload introduces a new primitive.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Implement quantity and price time-series H6 rows</name>
  <action>
Add DOC_TYPES/config/catalog updates, fixtures, schemas, transformers, and tests
for quantity-like and price-like H6 rows. Prefer shared transformer helpers only
where columns and meaning truly align.
  </action>
  <done>Commercial schedules, capacity, congestion income/cost, auction revenue, and position datasets reach silver from fixtures.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Spike flow-based allocations safely</name>
  <action>
Review fixture shape for `flow_based_allocations` before committing to silver
schema. Implement end-to-end if it fits existing parser patterns; otherwise
update catalog status to deferred with a concrete parser/schema reason.
  </action>
  <done>`flow_based_allocations` is either implemented with tests or deferred without leaving catalog drift.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 4: Expand mocked and live gates</name>
  <action>
Add URL-shape assertions and fixture-backed mocked E2E for every implemented H6
dataset. Add opt-in live request-shape probes for representative H6 request
families.
  </action>
  <done>H6 failures identify the dataset and request family without exposing API credentials.</done>
</task>

</tasks>

<verification>

Required gates:

```powershell
uv run --extra dev ruff check src/gridflow/connectors/entsoe src/gridflow/schemas/entsoe.py src/gridflow/silver/entsoe tests/unit/test_entsoe.py tests/unit/test_entsoe_endpoint_catalog.py tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_live.py
uv run --extra dev pytest tests/unit/test_entsoe.py tests/unit/test_entsoe_endpoint_catalog.py tests/integration/test_entsoe_connector.py tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_live.py -m "not live" -x -q
uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py::TestEntsoeLiveAllDatasets::test_live_request_shape_uses_supported_domain_params -q -rs
```

</verification>

<success_criteria>

- All H6 planned catalog rows are implemented or explicitly reclassified.
- H6 request filters are table-driven and covered by exact URL-shape tests.
- Quantity, price, and allocation-like schemas remain semantically separate.
- Existing ENTSO-E datasets remain passing.

</success_criteria>
