# NESO Carbon Intensity Platform Research

**Milestone:** v0.6 NESO Carbon Intensity Platform
**Source:** https://carbon-intensity.github.io/api-definitions/
**Researched:** 2026-05-04

## Summary

NESO's Carbon Intensity API is public, JSON-only, and path-parameter based.
It exposes 33 documented route variants across five response families:

- Carbon Intensity - National
- Statistics - National
- Generation Mix - National beta
- Carbon Intensity - Regional beta
- Static national generation emission factors

No authentication or pagination is required. Windowed endpoints use ISO-like UTC
path timestamps such as `2026-02-01T00:00Z`. Gridflow chunks windowed requests
into 14-day calls and stores all dynamic path values in bronze sidecar metadata.

## Endpoint Families

| Family | Routes | Pipeline shape |
|--------|--------|----------------|
| National intensity | 10 | One row per half-hour with forecast, actual, and index. |
| Statistics | 2 | One row per stats range/block with max, average, min, and index. |
| Generation mix | 3 | One row per half-hour and fuel. |
| Regional intensity | 17 | One row per half-hour, region, and fuel. |
| Factors | 1 | Reference rows by fuel. |

## Implementation Notes

- `src/gridflow/connectors/neso/endpoints.py` is the source of truth for the
  active dataset inventory, path templates, parser families, and defaults.
- `config/sources.yaml` mirrors the endpoint registry so CLI `--all` works for
  every NESO dataset.
- `src/gridflow/silver/neso/carbon_intensity.py` registers all 33 transformers
  from the endpoint catalog.
- Regional payloads have two shapes: period objects with nested `regions`, and
  region objects with nested `data`. The transformer handles both.
- `/generation` returns a `data` object while range generation endpoints return
  `data` arrays; both are normalised to long fuel rows.
- Postcode endpoints use `RG10` by default because it is the official example
  and was live-valid during research.
- `/intensity/date/{date}/{period}` is not a single daily request. Gridflow
  must request every valid GB settlement period for each settlement date:
  normally 48 periods, 46 on spring DST transition dates, and 50 on autumn DST
  transition dates.

## Verification Plan

- Unit inventory and path construction tests compare endpoint registry, config,
  catalog YAML, and transformer registration.
- Mocked E2E tests cover every registered dataset through connector fetch,
  bronze write, and silver transform.
- Opt-in live E2E tests hit every registered public API route and transform the
  live response into isolated temp silver parquet.
- Opt-in live CLI smoke test runs `gridflow pipeline neso carbon_intensity` in
  isolated temp output paths.
