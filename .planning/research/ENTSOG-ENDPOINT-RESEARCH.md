# ENTSOG Endpoint Research

**Milestone:** v0.5-entsog-pipeline-validation
**Researched:** 2026-05-04

## Sources Reviewed

- ENTSOG TP API User Manual v2.1:
  `https://transparency.entsog.eu/api/archiveDirectories/8/api-manual/TP_REG715_Documentation_TP_API%20-%20v2.1.pdf`
- ENTSOG TP API User Manual v3.0:
  `https://transparency.entsog.eu/api/archiveDirectories/8/api-manual/ENTSOG_TP_API_UserManual_v3.0.pdf`
- `entsog-py` reference client:
  `https://github.com/nhcb/entsog-py`
- Live ENTSOG Transparency Platform API metadata probes against
  `https://transparency.entsog.eu/api/v1`.

## Endpoint Inventory

The ENTSOG API groups the implementable surface into four families:

| Family | Official endpoints | Gridflow implementation target |
| --- | --- | --- |
| Point operational data | `/operationalData` / `/operationalDatas` | Indicator-specific datasets such as `physical_flows`, `nominations`, `allocations`, capacities, gas quality, GCV, and Wobbe Index |
| Point event/CMP data | `/interruptions`, `/cmpUnsuccessfulRequests`, `/cmpUnavailables`, `/cmpAuctions` | Dataset per endpoint with date-window request metadata |
| Zone/tariff/UMM data | `/aggregatedData`, `/tariffsFulls`, `/tariffsSimulations`, `/urgentMarketMessages` | Dataset per endpoint, with mandatory zone point-direction filters for aggregated data |
| Referential data | `/connectionPoints`, `/operators`, `/balancingZones`, `/operatorPointDirections`, `/interconnections`, `/aggregateInterconnections` | Dataset per endpoint, usually non-date or slow-changing reference output |

## Live API Findings

- The public API returns JSON with a top-level `meta` object and one data array named after the endpoint, for example `operatorPointDirections` or `cmpAuctions`.
- Default `limit` is 100. Production fetches can use `limit=-1`, but live tests should override to `limit=1` or a similarly small value.
- The v3.0 manual recommends short, well-defined requests and explicitly calls out monthly ranges for large data pulls.
- `operationalData` and `operationalDatas` require a `pointDirection` filter in practice. A date/indicator request without `pointDirection` returned `404 {"message":"No result found"}` in live probes.
- `pointDirection` for point operational data is built as `operatorKey + pointKey + directionKey`, matching the ENTSOG frontend and `entsog-py`.
- `/aggregatedData` also requires `pointDirection`, `from`, `to`, `indicator`, and `periodType`. The zone direction key is built from aggregate interconnection fields.
- Indicator values are case-sensitive. The v3.0 manual corrects the operational nomination indicator to `Nomination`, while the older v2.1 annex also lists `Nominations`.
- v3.0 adds gas quality indicators: `Methane Content`, `Hydrogen Content`, and `Oxygen Content`.
- `timeZone`/`timezone` handling is inconsistent in docs, but the API accepts timezone-style parameters and the existing project uses ENTSOG's `UCT` convention.

## Implementation Implications

1. Replace the single hard-coded `physical_flows` request with an ENTSOG endpoint registry.
2. Keep `physical_flows` as a specialised silver transformer because it normalises gas flow values to GWh/day.
3. Add generic ENTSOG JSON silver transformers for the remaining operational, CMP, tariff, UMM, and reference endpoint datasets.
4. Treat the endpoint catalog, source config, connector registry, and silver transformer registry as one contract.
5. Add mocked request-shape tests for every configured ENTSOG dataset.
6. Add fixture-backed bronze-to-silver tests for every dataset family.
7. Add opt-in live tests that hit the real public API with narrow limits and classify documented no-data responses as explicit skips.

## Risks

- Some documented endpoints can return `404 No result found` for narrow windows even when the route exists; live tests must distinguish no-data from broken request construction.
- `limit=-1` on broad operational or aggregated requests risks API timeouts. Keep config windows at 30 days or less and use CLI/backfill chunking for larger pulls.
- Referential endpoints can be large. Live validation should request only small samples.
- Operational indicators are exact-case strings. Tests should assert exact values so future edits do not silently drift.

## RESEARCH COMPLETE
