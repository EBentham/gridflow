---
phase: H4
plan: 02
type: execute
wave: 2
depends_on:
  - H4-01
files_modified:
  - src/gridflow/connectors/entsoe/endpoints.py
  - src/gridflow/connectors/entsoe/client.py
  - src/gridflow/connectors/entsoe/parsers.py
  - src/gridflow/schemas/entsoe.py
  - src/gridflow/silver/entsoe/
  - config/sources.yaml
  - tests/fixtures/entsoe/
  - tests/integration/test_entsoe_mocked_e2e.py
  - tests/integration/test_entsoe_live.py
autonomous: true
requirements:
  - DOC-01
  - COVER-01
  - COVER-02
  - LIVE-04
---

<objective>
Implement the full ENTSO-E endpoint coverage workflow.

Purpose: convert the official endpoint collection into an auditable catalog, classify
missing data sources, and add the next implementation batches with request, parser,
schema, transformer, mocked E2E, and opt-in live coverage.
</objective>

<execution_context>
@.planning/phases/H4-entsoe-endpoint-catalog-request-builder/H4-CONTEXT.md
@.planning/phases/H4-entsoe-endpoint-catalog-request-builder/H4-RESEARCH.md
@src/gridflow/connectors/entsoe/endpoints.py
@src/gridflow/connectors/entsoe/client.py
@src/gridflow/connectors/entsoe/parsers.py
@src/gridflow/schemas/entsoe.py
@src/gridflow/silver/entsoe/__init__.py
@config/sources.yaml
@tests/integration/test_entsoe_mocked_e2e.py
@tests/integration/test_entsoe_live.py
</execution_context>

<tasks>

<task type="auto" tdd="true">
  <name>Task 1: Add an official endpoint catalog artifact</name>
  <action>
Create a checked-in ENTSO-E endpoint catalog document or structured fixture generated from
the official collection. Each row must capture domain, article/name, document type, process
type, business type, required area parameter style, optional filters, max window, parser
family, implementation status, and deferral reason.
  </action>
  <acceptance_criteria>
    - Every Postman collection item is represented or explicitly excluded.
    - Existing 16 datasets map to catalog rows.
    - Missing endpoint families are grouped by implementation batch.
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Promote high-priority missing sources</name>
  <action>
Add initial missing datasets in domain batches, starting with:
load month/year forecasts and forecast margin; generation unit/reservoir datasets;
transmission commercial schedules and physical/capacity variants; outage production and
transmission datasets; master production/generation units.
  </action>
  <acceptance_criteria>
    - Each promoted dataset has `DOC_TYPES` metadata and `config/sources.yaml` metadata.
    - Each promoted dataset has a parser strategy, schema, transformer, registration, and
      fixture-backed bronze-to-silver test.
    - Dataset names are stable and analysis-friendly.
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Generalize request styles beyond current 16 datasets</name>
  <action>
Extend request metadata to support `area_Domain`, `Connecting_Domain`, `Acquiring_Domain`,
`registeredResource`, update-window parameters, offsets, document status, market agreement
types, auction filters, and PSR/resource filters where documented.
  </action>
  <acceptance_criteria>
    - Request building is table-driven; adding a dataset should not require new connector
      branches unless ENTSO-E introduces a new request primitive.
    - Tests assert exact query params for every promoted dataset.
  </acceptance_criteria>
</task>

<task type="auto" tdd="true">
  <name>Task 4: Add validation workflow for endpoint coverage</name>
  <action>
Add a test or script that compares the official catalog against `DOC_TYPES`, reports
implemented, deferred, and missing rows, and fails only on unclassified gaps.
  </action>
  <acceptance_criteria>
    - Running the coverage check gives a clear gap matrix.
    - Deferred endpoints require an explicit reason and owner phase/backlog item.
  </acceptance_criteria>
</task>

</tasks>

<verification>

Required gates:

```powershell
uv run --extra dev pytest tests/integration/test_entsoe_connector.py tests/integration/test_entsoe_mocked_e2e.py tests/unit/test_entsoe.py -x -q
uv run --extra dev pytest tests/integration/test_entsoe_live.py -m "not live" -x -q
uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py::TestEntsoeLiveAllDatasets::test_live_request_shape_uses_supported_domain_params -q -rs
uv run --extra dev ruff check src tests
```

Full live all-dataset fetch and bronze-to-silver gates remain opt-in and should be run
after each promoted batch when `ENTSOE_API_KEY` is available.

</verification>

<success_criteria>

- The project has an auditable ENTSO-E endpoint catalog with no silent omissions.
- Missing ENTSO-E sources are classified as implemented, planned, or intentionally deferred.
- The connector can express every request shape needed by the official collection.
- New datasets reach silver through the same medallion path as the existing 16 datasets.

</success_criteria>

