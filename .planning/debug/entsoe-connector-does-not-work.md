---
status: resolved
trigger: "The ENTSOE connector does not work for gridflow backfill entsoe --all --start 2026-04-15 --end 2026-04-25"
created: 2026-05-02
updated: 2026-05-02
---

# Debug Session: ENTSO-E Connector Does Not Work

## Symptoms

- Command: `gridflow backfill entsoe --all --start 2026-04-15 --end 2026-04-25`
- Observed failure starts at `entsoe/day_ahead_prices`.
- User saw ENTSO-E acknowledgement XML and a failed request URL containing `in_Domain.mRID`, `out_Domain.mRID`, and a redacted `securityToken`.

## Current Focus

- hypothesis: ENTSO-E rejects the connector request because domain query parameter names are wrong.
- test: Compare live API response for current parameter names versus canonical ENTSO-E parameter names without printing the token.
- expecting: Current `.mRID` parameter names fail; canonical names are accepted by the API layer.
- next_action: Resolved; monitor broader live backfill for dataset-specific no-data/transform issues.

## Evidence

- Local settings diagnostic:
  - `.env` contains `ENTSOE_API_KEY`.
  - `load_settings().get_source_config("entsoe").api_key` is present.
  - Loaded token length is 36 characters.
- Live API probe using current connector-style parameters:
  - `in_Domain.mRID` / `out_Domain.mRID`
  - HTTP status: 400
  - ENTSO-E reason: `Input parameter does not exist: in_Domain.mRID`
- Live API probe using canonical zone parameters:
  - `in_Domain` / `out_Domain`
  - HTTP status: 200
  - ENTSO-E reason: `No matching data found ...`
  - Interpretation: authentication and parameter names are accepted; the response is now a data availability issue, not malformed request shape.
- Live API probe using current control-area parameter:
  - `controlArea_Domain.mRID`
  - HTTP status: 400
  - ENTSO-E reason: `Input parameter does not exist: controlArea_Domain.mRID`
- Live API probe using canonical control-area parameter:
  - `controlArea_Domain`
  - HTTP status: 200
  - ENTSO-E reason: `No matching data found ...`

## Eliminated

- hypothesis: `.env` key is not loaded at all.
  - result: eliminated for current working tree. `load_settings()` sees the ENTSO-E key and direct live probes include a non-empty token.
- hypothesis: token is necessarily invalid.
  - result: not supported by the live probe. With canonical parameter names, ENTSO-E accepts the request at the API layer and returns a domain/data response rather than an auth rejection.

## Root Cause

The ENTSO-E connector sends invalid query parameter names:

- `in_Domain.mRID`
- `out_Domain.mRID`
- `controlArea_Domain.mRID`

The live ENTSO-E API expects:

- `in_Domain`
- `out_Domain`
- `controlArea_Domain`

The mocked H2 tests encoded the same incorrect `.mRID` names, so the mocked suite passed while the real API rejected the request.

## Correction Plan

1. Update `src/gridflow/connectors/entsoe/client.py`
   - Replace `in_Domain.mRID` with `in_Domain`.
   - Replace `out_Domain.mRID` with `out_Domain`.
   - Replace `controlArea_Domain.mRID` with `controlArea_Domain`.
   - Keep internal variable names and silver XML parsing fields unchanged; this is a request-query fix, not an XML schema fix.

2. Update connector URL-shape tests
   - Update `tests/integration/test_entsoe_connector.py` assertions and test names/comments.
   - Update `tests/integration/test_entsoe_mocked_e2e.py` assertions and zone-pair comparison keys.
   - Add regression assertions that `.mRID` request params are not sent.

3. Add or extend a live diagnostic test
   - Add a small `@pytest.mark.live` request-shape sanity check that exercises one zone-style dataset and one control-area dataset.
   - Assert the failure is not `Input parameter does not exist`.
   - Keep it redacted and opt-in.

4. Improve ENTSO-E error reporting
   - Parse `Acknowledgement_MarketDocument` XML on non-2xx responses.
   - Surface reason code/text in connector exceptions.
   - Keep `securityToken` redacted from any URL or request context.

5. Verify
   - Run `uv run --extra dev pytest tests/integration/test_entsoe_connector.py tests/integration/test_entsoe_mocked_e2e.py tests/unit/test_entsoe.py -x -q`.
   - Run `uv run --extra dev pytest tests/integration/test_entsoe_live.py -m "not live" -x -q`.
   - Run `uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py -x -q` with `ENTSOE_API_KEY`.
   - Retry `gridflow backfill entsoe --all --start 2026-04-15 --end 2026-04-25`.

## Resolution

Implemented.

## Fix

- Updated `src/gridflow/connectors/entsoe/client.py` to send canonical request query parameters:
  - `in_Domain`
  - `out_Domain`
  - `controlArea_Domain`
- Added ENTSO-E acknowledgement XML parsing for non-2xx responses so reason code/text are surfaced.
- Added security-token redaction to ENTSO-E HTTP error messages.
- Updated mocked connector and E2E URL-shape tests to assert legacy `.mRID` query params are not sent.
- Added a live request-shape regression test for one zone-style dataset and one control-area dataset.

## Verification

- `uv run --extra dev pytest tests/integration/test_entsoe_connector.py tests/integration/test_entsoe_mocked_e2e.py tests/unit/test_entsoe.py -x -q` - 227 passed.
- `uv run --extra dev ruff check src/gridflow/connectors/entsoe/client.py src/gridflow/connectors/entsoe/endpoints.py tests/integration/test_entsoe_connector.py tests/integration/test_entsoe_mocked_e2e.py tests/integration/test_entsoe_live.py` - passed.
- `uv run --extra dev pytest tests/integration/test_entsoe_live.py -m "not live" -x -q` - 6 passed, 38 deselected.
- `uv run --extra dev pytest -m live tests/integration/test_entsoe_live.py::TestEntsoeLiveAllDatasets::test_live_request_shape_uses_supported_domain_params -q -rs` - 2 passed.
- Direct live connector probe for `day_ahead_prices` over `2026-04-15` to `2026-04-16` returned six HTTP 200 responses with request params using `in_Domain` and `out_Domain`, and no legacy `.mRID` keys.

## Follow-up

The original invalid-query-parameter failure is fixed. A full `gridflow backfill entsoe --all --start 2026-04-15 --end 2026-04-25` may still reveal dataset-specific issues, such as valid ENTSO-E acknowledgement XML for no matching data or transformer assumptions. Those should be handled as separate follow-up bugs with their own evidence.
