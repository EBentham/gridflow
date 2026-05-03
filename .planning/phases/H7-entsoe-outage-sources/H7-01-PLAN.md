---
phase: H7
plan: 01
type: execute
wave: 3
depends_on:
  - H6-01
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
  - SRC-OUT-01
  - SRC-OUT-02
  - SRC-OUT-03
  - COVER-03
  - LIVE-05
---

<objective>
Add the H7 ENTSO-E outage extension batch without regressing the existing
`outages_generation` unit-level schema.
</objective>

<execution_context>
@.planning/phases/H7-entsoe-outage-sources/H7-CONTEXT.md
@docs/entsoe_endpoint_catalog.yaml
@src/gridflow/connectors/entsoe/endpoints.py
@src/gridflow/connectors/entsoe/client.py
@src/gridflow/connectors/entsoe/parsers.py
@src/gridflow/schemas/entsoe.py
@src/gridflow/silver/entsoe/outages_generation.py
@config/sources.yaml
@tests/integration/test_entsoe_mocked_e2e.py
@tests/integration/test_entsoe_live.py
</execution_context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Add outage request metadata</name>
  <action>
Add H7 DOC_TYPES/config/catalog entries with exact outage domain params,
business types, and optional filters. Ensure mixed-case ENTSO-E params such as
`In_Domain`, `Out_Domain`, `DocStatus`, and update windows are emitted exactly.
  </action>
  <done>URL-shape tests distinguish each outage request family.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Extend outage parsing and schemas</name>
  <action>
Add parser support and silver schemas for consumption, transmission,
offshore-grid, and production outages. Preserve document/resource/status fields
needed to diagnose unavailable assets.
  </action>
  <done>Fixture-backed tests prove each outage payload reaches schema-valid silver.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Protect existing generation outage behavior</name>
  <action>
Run or extend focused tests around `outages_generation` so the new outage parser
paths cannot drop unit-level fields introduced in G4.
  </action>
  <done>Existing outages generation tests still pass with the shared outage parser changes.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 4: Reclassify dependent outage variants</name>
  <action>
Review catalog rows for net-position impact, available capacity, and fallbacks.
Keep them deferred with dependency reasons unless primary outage implementation
makes them trivial to promote safely.
  </action>
  <done>No H7 catalog row remains silently planned without implementation or an updated deferral reason.</done>
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

- Primary H7 outage datasets reach silver or are explicitly reclassified.
- Existing `outages_generation` output remains stable.
- Outage request filters and domain params are metadata-driven.
- Deferred outage variants have concrete dependency reasons.

</success_criteria>
