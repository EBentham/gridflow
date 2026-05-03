# I1 Context - Elexon Inventory and Test Scaffolding

## User Intent

Build Elexon validation in the same spirit as the completed ENTSO-E validation work, with the milestone ultimately proving that a live Elexon API response can flow all the way through bronze storage and into the silver layer.

## Phase Boundary

I1 is the foundation phase. It should not attempt the full live API to silver proof yet; that belongs to I3. I1 should make the Elexon inventory trustworthy enough that later mocked and live end-to-end tests can build on a stable dataset list, parameter-style registry, and diagnostic scaffolding.

## Canonical Local References

- `config/sources.yaml` is the configured Elexon dataset list.
- `src/gridflow/connectors/elexon/endpoints.py` is the request-shape registry.
- `src/gridflow/connectors/elexon/client.py` is the live request execution path.
- `src/gridflow/connectors/elexon/parsers.py` is the response extraction path.
- `src/gridflow/silver/elexon/__init__.py` imports active Elexon silver transformers.
- `src/gridflow/silver/registry.py` exposes registered transformers by source.
- `tests/endpoints/test_endpoint_urls.py` is the broad endpoint URL and parameter-shape suite.
- `tests/endpoints/test_endpoint_live.py` is the current live endpoint ping suite.
- `tests/integration/test_entsoe_live.py` is the closest live bronze-to-silver pattern for I3.

## Decisions

- Elexon live tests remain opt-in through the existing `live` pytest marker.
- Elexon does not require an API key; live-test skip logic should reflect network availability and API behavior rather than missing credentials.
- Excluded/decommissioned Elexon datasets should be documented explicitly so future coverage audits do not treat them as accidental omissions.
- I1 validation should compare configured datasets, endpoint definitions, and silver transformer registrations, while avoiding hard-coded assumptions that duplicate the production registries too broadly.
