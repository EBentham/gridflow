---
phase: H4
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - src/gridflow/connectors/entsoe/endpoints.py
  - src/gridflow/connectors/entsoe/client.py
  - config/sources.yaml
  - tests/integration/test_entsoe_connector.py
  - tests/integration/test_entsoe_mocked_e2e.py
  - tests/integration/test_entsoe_live.py
  - tests/unit/test_entsoe.py
autonomous: true
requirements:
  - URL-01
  - LIVE-04
---

<objective>
Repair URL construction for the 16 existing ENTSO-E datasets by replacing the
single broad zone-style request model with endpoint-specific request metadata.

Output:
- `DOC_TYPES` can represent each documented area parameter family needed by current
  datasets.
- `EntsoeConnector.fetch()` builds request query parameters from that metadata.
- Mocked and live request-shape tests catch invalid parameter names.
</objective>

<execution_context>
@.planning/phases/H4-entsoe-endpoint-catalog-request-builder/H4-CONTEXT.md
@.planning/phases/H4-entsoe-endpoint-catalog-request-builder/H4-RESEARCH.md
@src/gridflow/connectors/entsoe/endpoints.py
@src/gridflow/connectors/entsoe/client.py
@config/sources.yaml
@tests/integration/test_entsoe_connector.py
@tests/integration/test_entsoe_mocked_e2e.py
@tests/integration/test_entsoe_live.py
@tests/unit/test_entsoe.py
</execution_context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Make request metadata endpoint-specific</name>
  <files>src/gridflow/connectors/entsoe/endpoints.py</files>
  <action>
Extend `EntsoeDocType` with `domain_style` variants and `extra_params`.
Update the 16 existing datasets to match documented ENTSO-E query shapes.
  </action>
  <done>Endpoint metadata describes load, generation, outage, balancing, and zone-pair styles.</done>
</task>

<task type="auto">
  <name>Task 2: Build query params from metadata</name>
  <files>src/gridflow/connectors/entsoe/client.py</files>
  <action>
Replace hard-coded zone/control-area helpers with a metadata-driven document fetch.
Add a small `_domain_params()` helper that validates required in/out domains and emits
the exact documented parameter names.
  </action>
  <done>Connector no longer sends `in_Domain` / `out_Domain` for every non-balancing dataset.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Update URL-shape tests</name>
  <files>tests/integration/test_entsoe_connector.py, tests/integration/test_entsoe_mocked_e2e.py, tests/integration/test_entsoe_live.py, tests/unit/test_entsoe.py</files>
  <action>
Assert exact domain parameter sets, absence of legacy `.mRID` request keys, extra params,
and config-to-registry alignment.
  </action>
  <done>Mocked and opt-in live request-shape gates cover every current request style.</done>
</task>

</tasks>

<verification>

Required gates:

```powershell
uv run --extra dev ruff check src/gridflow/connectors/entsoe/client.py src/gridflow/connectors/entsoe/endpoints.py tests/integration/test_entsoe_connector.py tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_live.py tests/unit/test_entsoe.py
uv run --extra dev pytest tests/integration/test_entsoe_connector.py tests/integration/test_entsoe_mocked_e2e.py tests/unit/test_entsoe.py -x -q
uv run --extra dev pytest tests/integration/test_entsoe_live.py -m "not live" -x -q
uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py::TestEntsoeLiveAllDatasets::test_live_request_shape_uses_supported_domain_params -q -rs
```

</verification>

<success_criteria>

- Existing mocked URL tests fail if a dataset sends the wrong ENTSO-E area parameter family.
- Representative live request-shape probes pass without `Input parameter does not exist`.
- Config and code registry stay aligned for dataset names, document types, and process types.
- No API key appears in assertion output or docs.

</success_criteria>

