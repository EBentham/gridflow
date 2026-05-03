---
phase: H5
plan: 01
type: execute
wave: 1
depends_on:
  - H4-02
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
  - SRC-GEN-01
  - SRC-GEN-02
  - SRC-GEN-03
  - SRC-GEN-04
  - COVER-03
  - LIVE-05
---

<objective>
Add ENTSO-E generation unit, reservoir, and generation-unit reference data
sources from the H5 catalog batch.

Output:
- `installed_capacity_units`, `actual_generation_units`, `water_reservoirs`,
  and `generation_units_master_data` are either implemented end-to-end or
  intentionally reclassified with a documented reason.
- Unit identifiers survive parser, schema, transformer, fixture, mocked E2E,
  and live request-shape coverage.
- H4 bronze `data_date` and backfill partition behavior has a regression test.
</objective>

<execution_context>
@.planning/phases/H5-entsoe-generation-unit-sources/H5-CONTEXT.md
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
  <name>Task 1: Lock the H4 bronze partition regression</name>
  <action>
Add or update focused coverage proving `EntsoeConnector._fetch_document()` sets
`RawResponse.data_date` from `periodStart`, and that a requested historical
ingest/backfill looks under the requested date partition rather than today's
ingestion date.
  </action>
  <done>Regression fails if ENTSO-E bronze files partition by `fetched_at` when a requested data date exists.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 2: Add H5 endpoint metadata and config</name>
  <action>
Add `DOC_TYPES` and `config/sources.yaml` entries for the H5 datasets with exact
document/process/business codes, domain params, optional filters, max-window
notes, and catalog status updates.
  </action>
  <done>Catalog validation confirms every implemented H5 row has matching runtime metadata.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 3: Extend parsers for unit and reference payloads</name>
  <action>
Reuse `parse_timeseries_xml` for unit time-series where possible, preserving
`unit_mrid`, `unit_name`, `production_type`, and domain fields. Add a small
master-data parser only if the A95/B11 payload cannot fit the existing
time-series parser.
  </action>
  <done>Fixtures prove unit and master-data fields are extracted without breaking existing parsers.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 4: Add schemas and silver transformers</name>
  <action>
Create H5 schema classes and silver transformer modules, export them from
`src/gridflow/silver/entsoe/__init__.py`, and keep column naming aligned with
current ENTSO-E conventions.
  </action>
  <done>Each implemented H5 dataset writes schema-valid silver parquet from fixture-backed bronze XML.</done>
</task>

<task type="auto" tdd="true">
  <name>Task 5: Add mocked E2E and live request-shape gates</name>
  <action>
Add realistic H5 fixtures, mocked bronze-to-silver coverage, exact URL-shape
assertions, and opt-in live request-shape probes for the new domain/filter
families.
  </action>
  <done>Mocked gates run without credentials; live gates skip without `ENTSOE_API_KEY` and redact the token in failures.</done>
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

- H5 planned catalog rows are implemented or intentionally deferred with updated reasons.
- Unit-level generation sources preserve resource identity through silver.
- H4 UAT bronze partition/backfill issue has a regression gate.
- Existing 19 ENTSO-E datasets continue to pass request-shape and fixture-backed coverage.

</success_criteria>
