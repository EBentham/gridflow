# I1 Research - Elexon Inventory and Test Scaffolding

## Current State

The project already has a substantial Elexon surface:

- `config/sources.yaml` defines the public Elexon base URL and active datasets.
- `src/gridflow/connectors/elexon/endpoints.py` defines `ENDPOINTS`, `ParamStyle`, and request builders for Elexon dataset styles.
- `src/gridflow/connectors/elexon/client.py` implements date-path, date-range, settlement-date, settlement-date-period, publish-datetime, and no-parameter live requests.
- `src/gridflow/silver/elexon/__init__.py` imports the active silver transformers so they register through `src/gridflow/silver/registry.py`.
- Existing tests cover endpoint URL generation, endpoint registry unit behavior, mocked connector behavior, and live endpoint pings.

The official Elexon developer portal describes the Insights API as public API access with endpoint-level documentation. For this milestone, local source-of-truth alignment matters more than discovering new endpoint families, because the user intent is validation of the currently supported GridFlow Elexon path.

## Gaps

The existing tests verify many request shapes, but they do not yet make the active Elexon inventory a single auditable contract across:

- configured source datasets,
- endpoint registry entries,
- silver transformer registrations,
- explicit exclusions for removed, duplicate, or intentionally unsupported endpoints.

That leaves room for future drift: a dataset can be added to configuration without a transformer, an endpoint can exist without being exposed by source config, or a decommissioned dataset can be mistaken for a missing implementation.

## Implementation Strategy

I1 should add a small inventory contract test suite before expanding mocked and live end-to-end coverage:

1. Assert that all configured active Elexon datasets have endpoint definitions.
2. Assert that all configured active Elexon datasets have registered silver transformers after importing `gridflow.silver.elexon`.
3. Assert that intentionally excluded datasets are named with a reason.
4. Keep existing request-shape tests as the param-style baseline, and improve diagnostics where needed for later live testing.

## Validation Architecture

I1 validation should be fast by default and live-aware by marker:

- Default verification runs unit, endpoint URL, and mocked connector tests without live network access.
- Live smoke verification remains separately selectable with `pytest -m live`.
- Diagnostics should identify `source`, `dataset`, request style, URL or parameter window, HTTP status, and bounded response preview when a live ping fails.

## Risks

- Live API availability can be temporarily noisy. Keep live tests marked and narrowly scoped.
- Overly broad hard-coded expected lists can become brittle. Prefer comparing production registries with focused explicit exceptions.
- Import-order dependencies can hide missing transformer registration. Inventory tests should import `gridflow.silver.elexon` before querying `list_transformers("elexon")`.
