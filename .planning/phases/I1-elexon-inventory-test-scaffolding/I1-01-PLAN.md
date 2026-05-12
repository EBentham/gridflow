---
phase: I1
plan: 01
type: execute
wave: 1
depends_on: []
requirements:
  - ELEXON-INV-01
  - ELEXON-INV-02
  - ELEXON-INV-03
files_modified:
  - config/sources.yaml
  - src/gridflow/connectors/elexon/endpoints.py
  - src/gridflow/silver/elexon/__init__.py
  - src/gridflow/silver/registry.py
  - tests/unit/test_elexon_endpoints.py
  - tests/endpoints/test_endpoint_urls.py
  - tests/endpoints/test_endpoint_live.py
  - tests/integration/test_elexon_connector.py
autonomous: true
---

# I1-01 Plan - Elexon Inventory Contract and Live-Test Scaffolding

## Objective

Make the active Elexon dataset inventory auditable across configuration, request definitions, and silver transformer registration, then leave a clean live-test scaffolding baseline for later mocked and live bronze-to-silver phases.

## Execution Context

Read these before editing:

- `.planning/phases/I1-elexon-inventory-test-scaffolding/I1-CONTEXT.md`
- `.planning/phases/I1-elexon-inventory-test-scaffolding/I1-RESEARCH.md`
- `.planning/REQUIREMENTS.md`
- `.planning/ROADMAP.md`
- `config/sources.yaml`
- `src/gridflow/connectors/elexon/endpoints.py`
- `src/gridflow/connectors/elexon/client.py`
- `src/gridflow/silver/elexon/__init__.py`
- `src/gridflow/silver/registry.py`
- `tests/unit/test_elexon_endpoints.py`
- `tests/endpoints/test_endpoint_urls.py`
- `tests/endpoints/test_endpoint_live.py`
- `tests/integration/test_elexon_connector.py`

## Tasks

### 1. Add Inventory Contract Tests

Add focused tests, preferably in `tests/unit/test_elexon_endpoints.py`, that verify:

- Every active Elexon dataset in `config/sources.yaml` has an entry in `ENDPOINTS`.
- Every active configured Elexon dataset has a registered silver transformer after importing `gridflow.silver.elexon`.
- Every active `ENDPOINTS` dataset is represented in source config unless explicitly named as intentionally excluded.
- Every configured active dataset has a known `ParamStyle`.

Use the real config loader and real registries where practical. Avoid creating a second complete hard-coded list of active datasets in tests.

### 2. Document Intentional Exclusions

Create a small explicit exclusion manifest in the tests or a nearby Elexon test helper for removed, duplicate, or intentionally unsupported Elexon endpoints. At minimum, cover:

- `bod`, retained in code comments but not registered as active.
- `generation_by_fuel`, removed as a duplicate of `fuelinst`.
- `indicative_imbalance_volumes`, removed in favor of the active configured replacement.

Each exclusion should have a short reason. This is the acceptance evidence for `ELEXON-INV-02`.

### 3. Tighten Request-Style Baseline

Review `tests/endpoints/test_endpoint_urls.py` and `tests/unit/test_elexon_endpoints.py` for any stale hard-coded Elexon dataset assumptions. Keep request-style assertions explicit where they protect behavior, but make broad inventory assertions registry-driven.

If the current endpoint registry and config disagree, fix the smallest production source of truth that is clearly wrong. Do not add new Elexon datasets in this phase unless the local source files already show an accidental omission.

### 4. Prepare Live-Test Diagnostics

Review `tests/endpoints/test_endpoint_live.py` and add or refine helper diagnostics so later I3 failures clearly show:

- source and dataset,
- parameter style,
- date or datetime window,
- URL or request parameters,
- HTTP status when available,
- bounded response preview when available.

Keep live tests marked with `pytest.mark.live`. Do not build the full bronze-to-silver live suite in I1.

### 5. Verify

Run fast verification:

```powershell
uv run --extra dev ruff check src/gridflow/connectors/elexon src/gridflow/silver/elexon tests/unit/test_elexon_endpoints.py tests/endpoints/test_endpoint_urls.py tests/endpoints/test_endpoint_live.py tests/integration/test_elexon_connector.py
uv run --extra dev pytest tests/unit/test_elexon_endpoints.py tests/endpoints/test_endpoint_urls.py tests/integration/test_elexon_connector.py -m "not live" -x -q
```

Run live scaffolding verification if network access is available:

```powershell
uv run --extra dev pytest -m live tests/endpoints/test_endpoint_live.py -q -rs
```

## Threat Model

| Threat | Risk | Mitigation |
| --- | --- | --- |
| Live tests create noisy failures because Elexon is temporarily unavailable | Medium | Keep live checks opt-in with `pytest.mark.live` and preserve fast non-live verification. |
| Inventory tests become a duplicate hard-coded registry | Medium | Compare real config, endpoint, and transformer registries; hard-code only explicit exclusions and reason strings. |
| Missing transformer registration is hidden by import order | Medium | Import `gridflow.silver.elexon` before querying `list_transformers("elexon")`. |
| Live failure output logs too much response data | Low | Keep response previews bounded and avoid storing full live payloads in I1. |

## Success Criteria

- `ELEXON-INV-01`: Active Elexon dataset coverage is asserted across config, endpoint registry, and silver transformer registry.
- `ELEXON-INV-02`: Intentionally excluded or decommissioned Elexon datasets are documented with reasons.
- `ELEXON-INV-03`: Parameter styles for active configured Elexon datasets are validated by tests.
- Fast verification passes without live network access.
- Live endpoint smoke tests remain explicitly selectable and have actionable diagnostics for I3.
