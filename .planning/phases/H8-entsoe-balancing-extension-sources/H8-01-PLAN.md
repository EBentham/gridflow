---
phase: H8
plan: 01
type: execute
wave: 4
depends_on:
  - H7-01
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
  - SRC-BAL-01
  - SRC-BAL-02
  - SRC-BAL-03
  - SRC-BAL-04
  - COVER-03
  - LIVE-05
---

<objective>
Add the H8 ENTSO-E balancing extension source batch and close the near-term
planned catalog coverage while keeping SO GL and implementation-framework rows
explicitly deferred.
</objective>

<execution_context>
@.planning/phases/H8-entsoe-balancing-extension-sources/H8-CONTEXT.md
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
  <name>Task 1: Add H8 request metadata and config</name>
  <action>
Add DOC_TYPES/config/catalog entries for the near-term H8 rows with exact
document/process/business codes, area params, offsets, product filters, market
agreement filters, and direction filters.
  </action>
  <done>Catalog validation and URL-shape tests cover every implemented H8 row.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Implement balancing state and financial datasets</name>
  <action>
Use quantity or price time-series parser paths where semantically correct for
`current_balancing_state` and `balancing_financial_expenses_income`.
  </action>
  <done>Both datasets reach schema-valid silver from fixtures or have documented parser reasons for deferral.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Implement or split bid and capacity parser families</name>
  <action>
Review fixture payloads for `balancing_energy_bids`,
`aggregated_balancing_energy_bids`, `procured_balancing_capacity`, and
`cross_zonal_balancing_capacity`. Add dedicated parser/schema families when bid,
product, direction, or agreement semantics cannot be safely represented by the
generic time-series parser.
  </action>
  <done>Bid and capacity rows are implemented end-to-end or reclassified with precise parser/schema blockers.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 4: Close near-term catalog coverage</name>
  <action>
Update catalog statuses so all H8 near-term rows are implemented or explicitly
deferred, and SO GL / implementation-framework rows remain deferred with owner
batch or backlog reasons.
  </action>
  <done>No planned H8 catalog row remains without runtime coverage or a deferral rationale.</done>
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

- Near-term H8 balancing-extension rows are implemented or deliberately reclassified.
- Existing balancing datasets remain passing.
- Bid and capacity parser families preserve domain semantics.
- The endpoint catalog has no silent remaining planned near-term rows after H8.

</success_criteria>
